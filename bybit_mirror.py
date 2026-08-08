"""
bybit_mirror.py  —  Bybit execution adapter ("the mirror")
=============================================================
Takes every trade decision the pipeline produces (the `levels` dict that
strategy.py / allocation.py / hratmap.py build and run_bot.py finalizes) and
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
    - An entry follower thread polls every resting LIMIT until it fills (then
      tracks the position) or its fill window expires (then cancels it).
    - A monitor loop polls open positions, logs PnL rows to
      config.PERFORMANCE_CSV (tagged exchange="bybit"), and records closes.

Run by run_bot.py in parallel with trader.py's Binance loop — never standalone
for trading, though `python bybit_mirror.py` runs just the monitor loop.
"""

import csv
import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import ccxt

import config


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

        self._open_trades: dict = {}   # raw symbol -> trade meta dict
        self._pending_entries: dict = {}   # order_id -> rec (see execute_trade)
        self._pending_lock = threading.Lock()
        self._follower_thread = threading.Thread(
            target=self._entry_follower_loop, daemon=True)
        self._follower_thread.start()

        self._init_performance_csv()
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
                      wait_for_fill: bool = True):
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
            return None

        if self.has_open_position():
            print(f"  [bybit] Skipping {levels['symbol']} — Bybit capacity full "
                  f"({self.open_capacity_used()}/{config.BYBIT_MAX_CONCURRENT_POSITIONS}).")
            return None

        rr = levels.get("risk_reward")
        if rr is None or rr < config.MIN_RISK_REWARD:
            print(f"  [bybit] Skipping {levels['symbol']} — R:R {rr} below "
                  f"MIN_RISK_REWARD ({config.MIN_RISK_REWARD}).")
            return None

        symbol = levels["symbol"]
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
            return None
        qty = (config.BYBIT_RISK_PER_TRADE_USD * config.BYBIT_LEVERAGE) / tlevels["entry"]

        try:
            balance = self._guarded(self.exchange.fetch_balance)
            free = float(balance.get("USDT", {}).get("free", 0) or 0)
            if config.BYBIT_RISK_PER_TRADE_USD > free:
                print(f"  [bybit] Margin check {symbol}: allocated "
                      f"{config.BYBIT_RISK_PER_TRADE_USD:.2f} USDT margin > free "
                      f"{free:.2f} USDT — mirror skipped, not enough Bybit margin.")
                return None
        except Exception as e:
            print(f"  [bybit] Could not check Bybit margin for {symbol}: {e}")

        try:
            self._guarded(self.exchange.set_leverage, config.BYBIT_LEVERAGE, unified)
        except Exception as e:
            print(f"  [bybit] Could not set Bybit leverage: {e}")

        try:
            entry_price = self.exchange.price_to_precision(unified, tlevels["entry"])
            sl_price = self.exchange.price_to_precision(unified, tlevels["sl"])
            tp_price = self.exchange.price_to_precision(unified, tlevels["tp"])
            qty_precise = self.exchange.amount_to_precision(unified, qty)
        except Exception as e:
            print(f"  [bybit] Could not round prices/qty for {symbol}: {e}")
            self._log_trade_event({"event": "order_failed", "error": str(e), **tlevels})
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
            "cancel_at": time.time() + config.BYBIT_LIMIT_FILL_WINDOW_SECONDS,
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

        if not wait_for_fill:
            return entry_order

        rec["event"].wait(max(timeout_at - time.time(), 0.0))
        if not rec["filled"] and not rec["resolved"]:
            print(f"  [bybit] LIMIT entry {symbol} still open at deadline — the entry "
                  f"follower keeps watching and will cancel it if it never fills.")
            return None
        return entry_order if rec["filled"] else None

    # ── Entry follower ─────────────────────────────────────────────────────
    def _entry_follower_loop(self):
        """Daemon: watch every resting Bybit LIMIT entry until it fills (then
        track the position) or its fill window expires (then cancel it)."""
        while True:
            try:
                with self._pending_lock:
                    recs = list(self._pending_entries.values())
                for rec in recs:
                    self._poll_pending_entry(rec)
            except Exception as e:
                print(f"  [bybit] Entry follower error: {e}")
            time.sleep(config.BYBIT_ORDER_POLL_SECONDS)

    def _poll_pending_entry(self, rec):
        if rec.get("resolved"):
            return
        if time.time() > rec["cancel_at"] and not rec["cancel_attempted"]:
            rec["cancel_attempted"] = True
            try:
                self._guarded(self.exchange.cancel_order, rec["order_id"], rec["unified"])
            except Exception as e:
                print(f"  [bybit] Could not cancel unfilled LIMIT entry for "
                      f"{rec['symbol']}: {e}")

        try:
            order = self._guarded(
                self.exchange.fetch_order, rec["order_id"], rec["unified"],
                params={"acknowledged": True},
            )
        except Exception as e:
            rec["poll_failures"] = rec.get("poll_failures", 0) + 1
            # Without params["acknowledged"]=True, a Unified-account fetchOrder
            # ALWAYS raises this "last 500 orders" error — so it MUST be passed.
            # If polling still fails after the fill window has expired (the order
            # id is genuinely unreachable), abandon instead of reprinting the
            # same error every poll forever.
            if rec["poll_failures"] >= config.BYBIT_MAX_POLL_FAILURES and time.time() > rec["cancel_at"]:
                print(f"  [bybit] Could not poll fill status for {rec['symbol']} after "
                      f"{rec['poll_failures']} attempts ({type(e).__name__}); fill window "
                      f"expired — abandoning.")
                self._finalize_failed(rec, "unresolvable")
            else:
                print(f"  [bybit] Could not poll fill status for {rec['symbol']}: {e}")
            return

        status = order.get("status")
        filled_qty = float(order.get("filled") or 0)
        if status == "closed" or filled_qty > 0:
            # 'filled_qty > 0' on a canceled/expired status = the cancel raced a
            # fill (or a partial fill happened before the rest was canceled):
            # Bybit is ALREADY holding the position, so track + manage it —
            # never orphan it. Bybit's TP/SL ride on the order (trading-stop
            # endpoint), so they carry over to the filled position automatically.
            if not rec.get("filled"):
                rec["filled"] = True
                rec["meta"] = {
                    **rec["levels"],
                    "qty": filled_qty if filled_qty > 0 else rec["qty"],
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                }
                self._open_trades[rec["symbol"]] = rec["meta"]
                rec["event"].set()
                print(f"  [bybit] Filled {rec['direction'].upper()} {rec['symbol']} "
                      f"qty={rec['meta']['qty']} entry={rec['entry_price']}"
                      f" (status={status}, filled={filled_qty})")
            self._finalize_filled(rec)
        elif status in ("canceled", "expired", "rejected", "failed"):
            self._finalize_failed(rec, status)

    def _finalize_filled(self, rec):
        with self._pending_lock:
            if rec.get("resolved"):
                return
            rec["resolved"] = True
            self._pending_entries.pop(rec["order_id"], None)
        self._open_trades[rec["symbol"]] = rec["meta"]
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
    def _init_performance_csv(self):
        csv_path = Path(config.PERFORMANCE_CSV)
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        # Same file + schema as trader.py so performance_plot.py sees both
        # exchanges in one equity view. If a legacy file without the exchange
        # column exists, rewrite the header so columns never misalign.
        needs_header = not csv_path.exists()
        if not needs_header:
            with open(csv_path, "r", encoding="utf-8") as f:
                header = f.readline().strip()
            if header and "exchange" not in header:
                needs_header = True
        if needs_header:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow([
                    "timestamp_utc", "symbol", "direction", "qty", "entry",
                    "sl", "tp", "mark_price", "unrealized_pnl", "status", "exchange",
                ])

    def _log_performance_row(self, symbol, direction, qty, entry, sl, tp,
                             mark_price, pnl, status):
        csv_path = Path(config.PERFORMANCE_CSV)
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow([
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                symbol, direction, qty, entry, sl, tp, mark_price, pnl, status,
                self.EXCHANGE_ID,
            ])

    def _log_trade_event(self, record: dict):
        record["exchange"] = self.EXCHANGE_ID
        record["time"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        log_path = Path(config.TRADE_LOG_FILE)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")

    def monitor_loop(self):
        """Run forever: poll Bybit open positions, log PnL, detect closes.
        Positions are only declared closed after they've been absent for
        BYBIT_POSITION_CLOSED_CONFIRM_POLLS consecutive polls — the same
        confirm-streak logic trader.py uses on Binance."""
        print("  [bybit] Monitor loop started.")
        absent_streaks: dict[str, int] = {}
        while True:
            try:
                positions = self.get_open_positions()
                open_symbols = set()
                for pos in positions:
                    raw_symbol = (pos.get("info") or {}).get("symbol")
                    symbol = (raw_symbol or pos.get("symbol") or "").upper()
                    if "/" in symbol:   # unified 'BTC/USDT:USDT' -> raw 'BTCUSDT'
                        symbol = symbol.replace("/", "").split(":")[0]
                    open_symbols.add(symbol)
                    contracts = float(pos.get("contracts") or 0)
                    meta = self._open_trades.get(symbol)
                    if meta:
                        absent_streaks[symbol] = 0
                        self._log_performance_row(
                            meta["symbol"], meta["direction"], meta["qty"], meta["entry"],
                            meta["sl"], meta["tp"],
                            float(pos.get("markPrice") or 0),
                            float(pos.get("unrealizedPnl") or 0), "open",
                        )
                    else:
                        side = pos.get("side")
                        direction = "long" if str(side).lower() == "buy" or contracts > 0 else "short"
                        self._log_performance_row(
                            symbol, direction, abs(contracts),
                            float(pos.get("entryPrice") or 0),
                            "", "", float(pos.get("markPrice") or 0),
                            float(pos.get("unrealizedPnl") or 0), "open",
                        )

                for symbol in list(self._open_trades.keys()):
                    if symbol not in open_symbols:
                        absent_streaks[symbol] = absent_streaks.get(symbol, 0) + 1
                        if absent_streaks[symbol] >= config.BYBIT_POSITION_CLOSED_CONFIRM_POLLS:
                            meta = self._open_trades.pop(symbol)
                            self._log_performance_row(
                                meta["symbol"], meta["direction"], meta["qty"], meta["entry"],
                                meta["sl"], meta["tp"], "", "", "closed",
                            )
                            self._log_trade_event({"event": "closed", **meta})
                            print(f"  [bybit] Position closed: {meta['symbol']}")
                            absent_streaks.pop(symbol, None)

            except Exception as e:
                print(f"  [bybit] Monitor loop error: {e}")

            time.sleep(config.BYBIT_POSITION_POLL_SECONDS)


if __name__ == "__main__":
    BybitMirror().monitor_loop()
