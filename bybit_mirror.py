"""
bybit_mirror.py  —  Bybit execution adapter ("the mirror")
=============================================================
Takes every trade decision the pipeline produces (the `levels` dict that
strategy.py / allocation.py build and run_bot.py finalizes) and
re-executes it on Bybit, translated for the price difference between the two
exchanges.

Why translate at all?
    Binance and Bybit never quote the same price for the same coin. If we sent
    Binance's literal entry/SL/TP to Bybit the stop and target would sit at the
    wrong absolute prices and the Bybit trade's risk:reward / PnL would NOT
    match the Binance one. So we compute the live spread between the exchanges
    at decision time and SHIFT the whole trade by that spread:

        spread          = bybit_last_price - binance_entry
        bybit_entry     = binance_entry + spread      (= bybit's current close)
        bybit_sl        = binance_sl  + spread
        bybit_tp        = binance_tp  + spread

    Because the SAME spread is added to entry, SL and TP, the absolute risk
    distance (entry→SL) and reward distance (entry→TP) are preserved exactly,
    so the R:R the strategy computed for Binance is the R:R Bybit sees, and
    the trade mirrors the Binance one one-for-one in PnL terms.

Flexibility / per-exchange independence:
    - Each exchange has its OWN concurrent-position cap (Binance uses
      MAX_CONCURRENT_POSITIONS, Bybit uses BYBIT_MAX_CONCURRENT_POSITIONS) and
      its OWN per-trade dollar allocation. If Binance is at its 3-trade limit
      the mirror can STILL open the same trade on Bybit (and vice versa).
    - A symbol that isn't listed on Bybit (or whose Bybit close can't be
      fetched) aborts the BYBIT side only — the Binance side proceeds untouched,
      and the next pull/decision gets a fresh chance. This is how the
      "⅔ slots remaining on Bybit" scenario works: Bybit silently skips coins
      it can't trade and keeps its remaining slots for coins it can.

Execution model (mirrors trader.py exactly):
    - LIMIT entry order at the translated (Bybit) entry price.
    - TP/SL are attached natively on the Bybit order itself (ccxt sends them to
      Bybit's trading-stop endpoint), so no separate attach-on-fill step is
      needed — unlike Binance USD-M which carries no TP/SL on a limit order.
    - A private WebSocket (Bybit v5 private stream) pushes order fill events
      instantly. A cancel timer handles unfilled order timeouts.
    - A monitor loop polls open positions, logs PnL rows to
      config.PERFORMANCE_CSV (tagged exchange="bybit"), and records closes.

Run by run_bot.py in parallel with trader.py's Binance loop — never standalone
for trading, though `python bybit_mirror.py` runs just the monitor loop.
"""

import hashlib
import hmac
import json
import threading
import time
from datetime import datetime, timezone

import ccxt

import config
import perfio


class BybitMirror:
    """Execution adapter for Bybit linear (USDT-perp). Shares no state with the
    Binance Trader — Bybit positions, pending entries and capacity are tracked
    completely independently, so the two exchanges never block each other."""

    EXCHANGE_ID = "bybit"

    def __init__(self):
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            raise RuntimeError(
                "BYBIT_API_KEY / BYBIT_API_SECRET are not set. Export them as "
                "environment variables or put them in a local .env file."
            )

        # Bybit linear perpetuals = the USDT perp market the strategy trades.
        self.exchange = ccxt.bybit({
            "apiKey": config.BYBIT_API_KEY.strip(),
            "secret": config.BYBIT_API_SECRET.strip(),
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        })

        # Enable CCXT's native Bybit Demo Trading (the pybit sketch's demo=True
        # — routes to https://api-demo.bybit.com). Public market data (tickers,
        # candles) is served there too, so the spread calc works on demo.
        if config.BYBIT_TESTNET:
            self.exchange.enableDemoTrading(True)

        # One-at-a-time lock for this exchange. ccxt's enableRateLimit already
        # paces requests; this guarantees even the poll loops never overlap on
        # the wire. (Binance's ratelimit.py is Binance-specific and doesn't
        # apply to Bybit.)
        self._http_lock = threading.Lock()

        # Open trades tracked for reporting/PnL, restored from the persistent
        # ledger so a restart never orphans a live Bybit position.
        self._open_trades: dict = {}   # raw symbol -> trade meta dict
        ledger = perfio.load_ledger()
        self._open_trades.update(ledger.get(self.EXCHANGE_ID, {}))

        self._pending_entries: dict = {}   # order_id -> rec (see execute_trade)
        self._pending_lock = threading.Lock()
        self._ws_thread = threading.Thread(
            target=self._user_data_stream_loop, daemon=True)
        self._ws_thread.start()
        self._cancel_thread = threading.Thread(
            target=self._cancel_timer_loop, daemon=True)
        self._cancel_thread.start()

        perfio.ensure_perf_header()
        print(f"  [bybit] BybitMirror ready — demo={config.BYBIT_TESTNET} "
              f"risk/trade=${config.BYBIT_RISK_PER_TRADE_USD:.0f} "
              f"lev={config.BYBIT_LEVERAGE}x "
              f"max_concurrent={config.BYBIT_MAX_CONCURRENT_POSITIONS}")

    # ── Serialized ccxt call ───────────────────────────────────────────────
    def _guarded(self, fn, *args, **kwargs):
        with self._http_lock:
            return fn(*args, **kwargs)

    # ── Symbol / listing helpers ───────────────────────────────────────────
    @staticmethod
    def _to_unified(symbol: str) -> str:
        """'BTCUSDT' (Binance raw id) -> 'BTC/USDT:USDT' (ccxt unified, Bybit
        linear). Everything we send to ccxt.bybit uses the unified form."""
        quote = config.SYMBOL_QUOTE
        base = symbol[: -len(quote)] if symbol.endswith(quote) else symbol
        return f"{base}/{quote}:{quote}"

    def _last_price(self, symbol: str) -> float | None:
        """Bybit's latest close for a symbol, or None if it can't be fetched.
        This doubles as the listing check: a coin that isn't on Bybit (or whose
        demo/feed is down) yields None -> the mirror aborts for THAT symbol and
        waits for the next pull, per the spec."""
        try:
            ticker = self._guarded(self.exchange.fetch_ticker, self._to_unified(symbol))
            last = ticker.get("last") or ticker.get("close")
            return float(last) if last else None
        except Exception as e:
            print(f"  [bybit] {symbol}: no Bybit price available ({type(e).__name__}: {e})")
            return None

    def _is_listed(self, symbol: str) -> bool:
        """Cheap pre-flight: is the coin actually a tradeable Bybit linear
        market? Gives a clean 'not listed' log before any order logic runs."""
        try:
            self.exchange.load_markets()
        except Exception as e:
            print(f"  [bybit] Could not load Bybit markets: {e}")
            return True  # let the real call surface the error
        return self._to_unified(symbol) in self.exchange.markets

    # ── Spread translation ─────────────────────────────────────────────────
    def _translate_levels(self, levels: dict, spread: float) -> dict:
        """Shift a Binance levels dict onto Bybit by adding the live spread to
        entry, SL and TP. Risk/reward distances are preserved exactly, so the
        Bybit trade's PnL mirrors Binance's."""
        t = dict(levels)
        t["exchange"] = self.EXCHANGE_ID
        t["binance_entry"] = levels["entry"]
        t["spread"] = spread
        t["entry"] = levels["entry"] + spread
        t["sl"] = levels["sl"] + spread if levels.get("sl") is not None else None
        t["tp"] = levels["tp"] + spread if levels.get("tp") is not None else None
        return t

    # ── Position / capacity state (Bybit-only) ─────────────────────────────
    def get_open_positions(self):
        """All Bybit positions with non-zero size. Bybit's fetch_positions()
        (defaultType='swap') returns every linear swap position for the
        account; we keep only the ones actually held."""
        try:
            positions = self._guarded(self.exchange.fetch_positions)
            return [
                p for p in positions
                if float(p.get("contracts") or p.get("info", {}).get("size", 0)) != 0
            ]
        except Exception as e:
            print(f"  [bybit] Error fetching positions: {e}")
            return []

    def fetch_account_state(self) -> dict | None:
        """Exchange-reported account state for PnL verification. Bybit's
        fetch_balance() discards the raw wallet fields, so this hits
        /v5/account/wallet-balance directly (UNIFIED, falling back to CONTRACT).
        totalWalletBalance = realized wallet, totalMarginBalance = equity
        (wallet + unrealized), totalPerpUPL = unrealized perp PnL. Best-effort:
        returns None on any failure so the monitor never blocks on it."""
        try:
            raw = None
            for acc_type in ("UNIFIED", "CONTRACT"):
                try:
                    raw = self._guarded(
                        self.exchange.privateGetV5AccountWalletBalance,
                        {"accountType": acc_type},
                    )
                    break
                except Exception as e:
                    if acc_type == "CONTRACT":
                        raise e
            row = (((raw or {}).get("result") or {}).get("list") or [{}])[0]

            def f(*keys):
                for k in keys:
                    v = row.get(k)
                    if v not in (None, ""):
                        try:
                            return float(v)
                        except (TypeError, ValueError):
                            pass
                return None

            wallet = f("totalWalletBalance")
            equity = f("totalMarginBalance", "totalWalletBalance")
            unrealized = f("totalPerpUPL")
            available = f("totalAvailableBalance")
            if wallet is None:
                return None
            return {
                "wallet": wallet,
                "unrealized": unrealized,
                "equity": equity,
                "available": available,
                "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        except Exception as e:
            print(f"  [bybit] Account state fetch failed: {e}")
            return None

    @staticmethod
    def _opened_ms(meta: dict) -> int | None:
        opened = (meta or {}).get("opened_at")
        if not opened:
            return None
        try:
            return int(datetime.fromisoformat(str(opened)).timestamp() * 1000)
        except Exception:
            return None

    def fetch_realized_pnl(self, symbol: str, since_ms: int | None) -> float | None:
        """The exchange's OWN realized PnL for a closed position via
        /v5/position/closed-pnl (real exit price + fees + funding). Demo
        trading rejects this endpoint, so on testnet this returns None and the
        caller falls back to the last mark PnL."""
        try:
            params = {"category": "linear", "symbol": symbol, "limit": 50}
            r = self._guarded(self.exchange.privateGetV5PositionClosedPnl, params)
            rows = ((r or {}).get("result") or {}).get("list") or []
            if not rows:
                return None
            total = 0.0
            for row in rows:
                ct = int(row.get("createdTime") or 0)
                if since_ms is not None and ct < since_ms:
                    continue
                tp = row.get("totalPnl")
                if tp not in (None, ""):
                    total += float(tp)
                else:
                    total += float(row.get("realizedPnl") or 0)
                    total += float(row.get("fundingFee") or 0)
            # Same trust rule as Trader: a real close always books a non-zero
            # PnL, so a zero sum falls back to the last mark PnL.
            return total if total != 0.0 else None
        except Exception as e:
            print(f"  [bybit] Closed-PnL fetch failed for {symbol}: {e}")
            return None

    def _final_realized(self, symbol: str, meta: dict):
        """Same contract as Trader._final_realized: prefer the exchange's
        closed-PnL figure for positions tracked from their real open; adopted
        positions (no trustworthy opened_at) fall back to last mark PnL."""
        if not meta.get("adopted"):
            since = self._opened_ms(meta)
            if since is not None:
                rpnl = self.fetch_realized_pnl(symbol, since)
                if rpnl is not None:
                    return rpnl, "income"
        return meta.get("last_pnl", ""), "mark"

    def pending_entry_count(self) -> int:
        with self._pending_lock:
            return sum(1 for e in self._pending_entries.values() if not e.get("resolved"))

    def open_capacity_used(self) -> int:
        """Bybit slots in use: open positions + resting/unfilled LIMIT entries.
        Pending entries reserve a slot, so stacked orders can never fill past
        BYBIT_MAX_CONCURRENT_POSITIONS."""
        return len(self.get_open_positions()) + self.pending_entry_count()

    def has_open_position(self) -> bool:
        """True = Bybit is at its own concurrent cap. Independent of Binance —
        Binance being full NEVER blocks Bybit and vice versa."""
        return self.open_capacity_used() >= config.BYBIT_MAX_CONCURRENT_POSITIONS

    def open_position_count(self) -> int:
        return len(self.get_open_positions())

    # ── Execution ──────────────────────────────────────────────────────────
    def execute_trade(self, levels: dict, deadline_ts: float | None = None,
                      wait_for_fill: bool = True, candle_open_ts: float | None = None):
        """Mirror a Binance `levels` dict onto Bybit. Same contract as
        Trader.execute_trade so run_bot.py drives both adapters identically:

        - aborts if the signal deadline already passed,
        - aborts (Bybit-only) if Bybit is at capacity or the coin isn't on
          Bybit — Binance is never affected,
        - translates entry/SL/TP by the live Binance↔Bybit spread,
        - sizes with Bybit's OWN dollar allocation + leverage,
        - places a LIMIT entry with native Bybit TP/SL attached.

        wait_for_fill=False returns right after placement — the entry follower
        owns the fill/cancel — used by the pre-calc path so one coin never
        stalls the pipeline."""
        if deadline_ts is not None and time.time() > deadline_ts:
            print(f"  [bybit] Aborting mirror of {levels.get('symbol', 'trade')} — "
                  f"signal deadline expired ({time.time() - deadline_ts:.2f}s late).")
            self._log_trade_event({"event": "deadline_expired", "symbol": levels.get("symbol", ""), **levels})
            return None

        symbol = levels["symbol"]
        if not config.is_traded_symbol(symbol):
            print(f"  [bybit] {symbol} not in traded-symbols allowlist — mirror skipped.")
            self._log_trade_event({"event": "symbol_not_allowed", "symbol": symbol, **levels})
            return None

        if self.has_open_position():
            print(f"  [bybit] Skipping {levels['symbol']} — Bybit capacity full "
                  f"({self.open_capacity_used()}/{config.BYBIT_MAX_CONCURRENT_POSITIONS}).")
            self._log_trade_event({"event": "capacity_full", "symbol": symbol, **levels})
            return None

        if not self._is_listed(symbol):
            print(f"  [bybit] {symbol} not listed on Bybit — mirror aborted, "
                  f"Binance side unaffected.")
            self._log_trade_event({"event": "bybit_not_listed", "symbol": symbol, **levels})
            return None

        # ── Spread: Bybit's close at decision time vs. Binance's entry ──
        bybit_last = self._last_price(symbol)
        if bybit_last is None or bybit_last <= 0:
            print(f"  [bybit] {symbol}: cannot pull Bybit close — mirror aborted, "
                  f"will retry on the next pull.")
            self._log_trade_event({"event": "bybit_no_price", "symbol": symbol, **levels})
            return None

        spread = bybit_last - levels["entry"]
        tlevels = self._translate_levels(levels, spread)
        unified = self._to_unified(symbol)
        direction = levels["direction"]
        side = "buy" if direction == "long" else "sell"

        print(f"  [bybit] {symbol}: spread {spread:+.6f} (bybit close "
              f"{bybit_last:.6f} - binance entry {levels['entry']:.6f}) -> "
              f"entry={tlevels['entry']:.6f} sl={tlevels['sl']:.6f} tp={tlevels['tp']:.6f}")

        # ── Size with Bybit's own allocation ──
        if tlevels["entry"] <= 0:
            print(f"  [bybit] {symbol}: non-positive translated entry, mirror aborted.")
            self._log_trade_event({"event": "invalid_entry", "symbol": symbol, **tlevels})
            return None
        qty = (config.BYBIT_RISK_PER_TRADE_USD * config.BYBIT_LEVERAGE) / tlevels["entry"]

        try:
            balance = self._guarded(self.exchange.fetch_balance)
            free = float(balance.get("USDT", {}).get("free", 0) or 0)
            if config.BYBIT_RISK_PER_TRADE_USD > free:
                print(f"  [bybit] Margin check {symbol}: allocated "
                      f"{config.BYBIT_RISK_PER_TRADE_USD:.2f} USDT margin > free "
                      f"{free:.2f} USDT — mirror skipped, not enough Bybit margin.")
                self._log_trade_event({"event": "margin_insufficient", "symbol": symbol,
                                       "allocated": config.BYBIT_RISK_PER_TRADE_USD,
                                       "free": free, **tlevels})
                return None
        except Exception as e:
            print(f"  [bybit] Could not check Bybit margin for {symbol}: {e}")
            self._log_trade_event({"event": "margin_check_error", "symbol": symbol,
                                   "error": str(e), **tlevels})

        try:
            self._guarded(self.exchange.set_leverage, config.BYBIT_LEVERAGE, unified)
        except Exception as e:
            print(f"  [bybit] Could not set Bybit leverage: {e}")
            self._log_trade_event({"event": "leverage_set_failed", "symbol": symbol,
                                   "error": str(e), **tlevels})

        try:
            entry_price = self.exchange.price_to_precision(unified, tlevels["entry"])
            sl_price = self.exchange.price_to_precision(unified, tlevels["sl"])
            tp_price = self.exchange.price_to_precision(unified, tlevels["tp"])
            qty_precise = self.exchange.amount_to_precision(unified, qty)
        except Exception as e:
            print(f"  [bybit] Could not round prices/qty for {symbol}: {e}")
            self._log_trade_event({"event": "order_failed", "error": str(e), **tlevels})
            return None

        target_notional = config.BYBIT_RISK_PER_TRADE_USD * config.BYBIT_LEVERAGE
        actual_notional = float(qty_precise) * tlevels["entry"]
        if target_notional > 0 and actual_notional / target_notional < config.MIN_FILL_RATIO:
            print(f"  [bybit] {symbol}: precision rounding collapsed notional from "
                  f"${target_notional:.0f} to ${actual_notional:.2f} "
                  f"({actual_notional/target_notional:.0%} of target) — mirror skipped, "
                  f"position too small to be useful.")
            self._log_trade_event({
                "event": "skipped_small_notional", "symbol": symbol,
                "target_notional": round(target_notional, 2),
                "actual_notional": round(actual_notional, 2),
                "qty_raw": round(qty, 8), "qty_precise": qty_precise,
            })
            return None

        # ── Place the LIMIT entry with native Bybit TP/SL. We pass the
        #    stopLoss/takeProfit OBJECT form ({triggerPrice}) so ccxt keeps
        #    the order on Bybit's place-order endpoint (privatePostV5OrderCreate)
        #    with the TP/SL riding on the resting limit order. Using the flat
        #    stopLossPrice/takeProfitPrice form would make ccxt route to the
        #    trading-stop endpoint instead, which errors with "can not set
        #    tp/sl/ts for zero position" because no position exists yet. ──
        try:
            entry_order = self._guarded(
                self.exchange.create_order, unified, "limit", side, qty_precise,
                price=entry_price,
                params={
                    "stopLoss": {"triggerPrice": sl_price},
                    "takeProfit": {"triggerPrice": tp_price},
                },
            )
        except Exception as e:
            print(f"  [bybit] LIMIT entry placement failed for {symbol}: {e}")
            self._log_trade_event({"event": "order_failed", "error": str(e), **tlevels})
            return None

        order_id = entry_order.get("id")
        rec = {
            "order_id": order_id,
            "symbol": symbol,
            "unified": unified,
            "side": side,
            "qty": qty_precise,
            "entry_price": entry_price,
            "sl": sl_price,
            "tp": tp_price,
            "direction": direction,
            "levels": tlevels,
            "deadline_ts": deadline_ts,
            "placed_at": time.time(),
            "cancel_at": (candle_open_ts + config.BYBIT_LIMIT_FILL_WINDOW_SECONDS
                          if candle_open_ts is not None
                          else time.time() + config.BYBIT_LIMIT_FILL_WINDOW_SECONDS),
            "cancel_attempted": False,
            "attached": True,          # TP/SL ride on the order itself
            "resolved": False,
            "filled": False,
            "event": threading.Event(),
        }
        with self._pending_lock:
            self._pending_entries[order_id] = rec

        timeout_at = rec["cancel_at"]
        print(f"  [bybit] LIMIT {side.upper()} {symbol} qty={qty_precise} "
              f"@{entry_price} placed (SL {sl_price} / TP {tp_price} attached) "
              f"{'— not waiting for fill' if not wait_for_fill else ''}")
        self._log_trade_event({"event": "order_placed", "symbol": symbol, "direction": direction,
                               "qty": qty_precise, "entry": entry_price,
                               "sl": sl_price, "tp": tp_price, "side": side})

        if not wait_for_fill:
            return entry_order

        rec["event"].wait(max(timeout_at - time.time(), 0.0))
        if not rec["filled"] and not rec["resolved"]:
            print(f"  [bybit] LIMIT entry {symbol} still open at deadline — the entry "
                  f"follower keeps watching and will cancel it if it never fills.")
            return None
        return entry_order if rec["filled"] else None

    # ── Entry follower (WebSocket-based) ──────────────────────────────────
    def _user_data_stream_loop(self):
        """Daemon: maintain a Bybit v5 private WebSocket for instant order fill
        notifications. Reconnects automatically on disconnects."""
        import websocket

        while True:
            try:
                expires = int((time.time() + 10) * 1000)
                signature_val = hmac.new(
                    config.BYBIT_API_SECRET.strip().encode("utf-8"),
                    f"GET/realtime{expires}".encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()

                if config.BYBIT_TESTNET:
                    ws_url = "wss://stream-testnet.bybit.com/v5/private"
                else:
                    ws_url = "wss://stream.bybit.com/v5/private"

                print(f"  [bybit] Connecting private WebSocket...")

                def on_message(ws, message):
                    try:
                        data = json.loads(message)
                        topic = data.get("topic", "")
                        msg_type = data.get("type", "")
                        if topic == "order" and msg_type == "snapshot":
                            for order in data.get("data", []):
                                self._handle_order_update(order)
                    except Exception as e:
                        print(f"  [bybit] WS message error: {e}")

                def on_error(ws, error):
                    print(f"  [bybit] WS error: {error}")

                def on_close(ws, close_status_code, close_msg):
                    print(f"  [bybit] WS closed ({close_status_code}: {close_msg})")

                def on_open(ws):
                    # Authenticate
                    auth_msg = json.dumps({
                        "op": "auth",
                        "args": [config.BYBIT_API_KEY.strip(), expires, signature_val],
                    })
                    ws.send(auth_msg)
                    # Subscribe to order updates
                    sub_msg = json.dumps({
                        "op": "subscribe",
                        "args": ["order"],
                    })
                    ws.send(sub_msg)
                    print(f"  [bybit] Private WebSocket connected + subscribed")

                ws = websocket.WebSocketApp(
                    ws_url,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_open=on_open,
                )

                ws_thread = threading.Thread(target=ws.run_forever, kwargs={
                    "ping_interval": 20,
                    "ping_timeout": 10,
                }, daemon=True)
                ws_thread.start()

                while ws_thread.is_alive():
                    time.sleep(5)

                ws.close()
                print(f"  [bybit] Private WebSocket disconnected, reconnecting...")

            except Exception as e:
                print(f"  [bybit] WebSocket error: {e}")
                time.sleep(5)

    def _handle_order_update(self, order: dict):
        """Process an order update from the Bybit private WebSocket.
        On fill, finalize. On cancel/expired, finalize as failed."""
        order_id = str(order.get("orderId", ""))
        status = order.get("orderStatus", "")
        cum_exec_qty = float(order.get("cumExecQty", 0))

        if not order_id:
            return

        with self._pending_lock:
            rec = self._pending_entries.get(order_id)
        if not rec or rec.get("resolved"):
            return

        if status == "Filled" or cum_exec_qty > 0:
            if not rec.get("filled"):
                rec["filled"] = True
                rec["meta"] = {
                    **rec["levels"],
                    "qty": cum_exec_qty if cum_exec_qty > 0 else rec["qty"],
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                }
                self._open_trades[rec["symbol"]] = rec["meta"]
                rec["event"].set()
                print(f"  [bybit] Filled {rec['direction'].upper()} {rec['symbol']} "
                      f"qty={rec['meta']['qty']} entry={rec['entry_price']}"
                      f" (WS fill, status={status}, filled={cum_exec_qty})")
            self._finalize_filled(rec)
        elif status in ("Cancelled", "Expired", "Rejected", "Failed"):
            self._finalize_failed(rec, status.lower())

    def _cancel_timer_loop(self):
        """Daemon: cancel unfilled Bybit LIMIT entries when their fill window
        expires. Runs independently of the WebSocket."""
        while True:
            try:
                with self._pending_lock:
                    recs = list(self._pending_entries.values())
                for rec in recs:
                    if rec.get("resolved"):
                        continue
                    if time.time() > rec["cancel_at"] and not rec["cancel_attempted"]:
                        rec["cancel_attempted"] = True
                        try:
                            self._guarded(self.exchange.cancel_order,
                                          rec["order_id"], rec["unified"])
                            self._log_trade_event({"event": "order_cancelled",
                                                   "symbol": rec["symbol"],
                                                   "order_id": rec["order_id"]})
                        except Exception as e:
                            print(f"  [bybit] Could not cancel unfilled LIMIT entry for "
                                  f"{rec['symbol']}: {e}")
                            self._log_trade_event({"event": "cancel_failed",
                                                   "symbol": rec["symbol"],
                                                   "order_id": rec["order_id"],
                                                   "error": str(e)})
            except Exception as e:
                print(f"  [bybit] Cancel timer error: {e}")
            time.sleep(1)

    def _finalize_filled(self, rec):
        with self._pending_lock:
            if rec.get("resolved"):
                return
            rec["resolved"] = True
            self._pending_entries.pop(rec["order_id"], None)
        self._open_trades[rec["symbol"]] = rec["meta"]
        perfio.update_ledger(self.EXCHANGE_ID, self._open_trades)
        perfio.save_signal(self.EXCHANGE_ID, rec["symbol"], rec["meta"])
        self._log_trade_event({"event": "opened", **rec["meta"]})
        print(f"  [bybit] Opened {rec['direction'].upper()} {rec['symbol']} "
              f"qty={rec['qty']} entry={rec['entry_price']} sl={rec['sl']} "
              f"tp={rec['tp']} — Bybit TP/SL attached")
        rec["event"].set()

    def _finalize_failed(self, rec, status):
        with self._pending_lock:
            if rec.get("resolved"):
                return
            rec["resolved"] = True
            self._pending_entries.pop(rec["order_id"], None)
        self._log_trade_event({"event": "entry_not_filled", "status": status, **rec["levels"]})
        print(f"  [bybit] LIMIT entry {status} for {rec['symbol']}, abandoning.")
        rec["event"].set()

    # ── Monitoring / performance logging ───────────────────────────────────
    # All CSV/log/state writes go through perfio (single-writer, serialized
    # with the Binance monitor in the same process).

    def _log_performance_row(self, symbol, direction, qty, entry, sl, tp,
                             mark_price, pnl, status):
        perfio.append_perf_row([
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            symbol, direction, qty, entry, sl, tp, mark_price, pnl, status,
            self.EXCHANGE_ID,
        ])

    def _log_trade_event(self, record: dict):
        record["exchange"] = self.EXCHANGE_ID
        record["time"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        perfio.append_log_event(record)

    def _adopt_position(self, symbol, pos, contracts, side) -> dict:
        """A position exists on Bybit that isn't in the ledger (opened before a
        restart, or never tracked). Check the signals file first — if the system
        placed this trade, use the stored entry/tp/sl instead of adopting with
        blanks. Only truly external positions get adopted."""
        stored = perfio.find_signal(self.EXCHANGE_ID, symbol)
        if stored is not None:
            meta = {
                "symbol": symbol,
                "direction": stored.get("direction", "long" if str(side).lower() == "buy" else "short"),
                "qty": stored.get("qty", abs(contracts)),
                "entry": stored.get("entry", float(pos.get("entryPrice") or 0)),
                "sl": stored.get("sl", ""),
                "tp": stored.get("tp", ""),
                "opened_at": stored.get("opened_at", datetime.now(timezone.utc).isoformat()),
            }
            self._open_trades[symbol] = meta
            self._log_trade_event({"event": "opened", **meta})
            perfio.update_ledger(self.EXCHANGE_ID, self._open_trades)
            print(f"  [bybit] Restored from signals file: {symbol} "
                  f"{meta['direction']} qty={meta['qty']} sl={meta['sl']} tp={meta['tp']}")
            return meta
        meta = {
            "symbol": symbol,
            "direction": "long" if str(side).lower() == "buy" else "short",
            "qty": abs(contracts),
            "entry": float(pos.get("entryPrice") or 0),
            "sl": "",
            "tp": "",
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "adopted": True,
        }
        self._open_trades[symbol] = meta
        self._log_trade_event({"event": "adopted_position", **meta})
        perfio.update_ledger(self.EXCHANGE_ID, self._open_trades)
        print(f"  [bybit] Adopted untracked position: {symbol} "
              f"{meta['direction']} qty={meta['qty']}")
        return meta

    def monitor_loop(self):
        """Run forever: poll Bybit open positions, log PnL, detect closes,
        snapshot. Same exchange-truth design as trader.py — every position is
        adopted so it gets a close row, closed rows carry final mark/PnL, and
        snapshot.json is rewritten every poll for the dashboard."""
        print("  [bybit] Monitor loop started.")
        absent_streaks: dict[str, int] = {}
        while True:
            try:
                positions = self.get_open_positions()
                account = self.fetch_account_state()
                positions_dict = {}
                open_symbols = set()
                for pos in positions:
                    raw_symbol = (pos.get("info") or {}).get("symbol")
                    symbol = (raw_symbol or pos.get("symbol") or "").upper()
                    if "/" in symbol:   # unified 'BTC/USDT:USDT' -> raw 'BTCUSDT'
                        symbol = symbol.replace("/", "").split(":")[0]
                    open_symbols.add(symbol)
                    contracts = float(pos.get("contracts") or 0)
                    mark = float(pos.get("markPrice") or 0)
                    pnl = float(pos.get("unrealizedPnl") or 0)
                    side = pos.get("side")

                    meta = self._open_trades.get(symbol)
                    if meta is None:
                        meta = self._adopt_position(symbol, pos, contracts, side)
                    else:
                        absent_streaks[symbol] = 0
                    meta["last_mark"] = mark
                    meta["last_pnl"] = pnl
                    self._open_trades[symbol] = meta

                    self._log_performance_row(
                        meta["symbol"], meta["direction"], meta["qty"], meta["entry"],
                        meta["sl"], meta["tp"], mark, pnl, "open",
                    )
                    positions_dict[symbol] = {
                        "symbol": meta["symbol"], "direction": meta["direction"],
                        "qty": meta["qty"], "entry": meta["entry"],
                        "sl": meta["sl"], "tp": meta["tp"],
                        "mark": mark, "pnl": pnl, "opened_at": meta.get("opened_at", ""),
                    }

                changed = False
                for symbol in list(self._open_trades.keys()):
                    if symbol not in open_symbols:
                        absent_streaks[symbol] = absent_streaks.get(symbol, 0) + 1
                        if absent_streaks[symbol] >= config.BYBIT_POSITION_CLOSED_CONFIRM_POLLS:
                            meta = self._open_trades.pop(symbol)
                            rpnl, source = self._final_realized(symbol, meta)
                            self._log_performance_row(
                                meta["symbol"], meta["direction"], meta["qty"], meta["entry"],
                                meta["sl"], meta["tp"],
                                meta.get("last_mark", ""), rpnl, "closed",
                            )
                            self._log_trade_event({
                                "event": "closed", "realized_pnl": rpnl,
                                "pnl_source": source, **meta,
                            })
                            print(f"  [bybit] Position closed: {meta['symbol']} "
                                  f"(realized {rpnl} via {source})")
                            absent_streaks.pop(symbol, None)
                            changed = True
                            perfio.remove_signal(self.EXCHANGE_ID, symbol)
                if changed:
                    perfio.update_ledger(self.EXCHANGE_ID, self._open_trades)

                perfio.update_snapshot(self.EXCHANGE_ID, positions_dict, account)

            except Exception as e:
                print(f"  [bybit] Monitor loop error: {e}")

            time.sleep(config.BYBIT_POSITION_POLL_SECONDS)


if __name__ == "__main__":
    BybitMirror().monitor_loop()
