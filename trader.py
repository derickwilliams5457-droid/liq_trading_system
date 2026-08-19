"""
trader.py  —  Stage 5 of the pipeline
=======================================
Takes a direction + SL + TP from allocation.py and places the trade on
Binance Futures via ccxt. Before opening anything it checks for any existing
open position across the whole account and refuses to open a second one.
A background loop polls open-position PnL and appends a row to
config.PERFORMANCE_CSV on every poll, plus a final row when a position closes.

Entry orders use the Binance user data stream (WebSocket) for instant fill
notifications instead of REST polling. A cancel timer thread handles unfilled
order timeouts. On fill, the STOP_MARKET / TAKE_PROFIT_MARKET (closePosition)
pair is attached and the position is tracked, so no fill is ever left unmanaged.

⚠️  This places real orders against a real exchange account. config.TESTNET
defaults to True — you must explicitly set LIQ_TESTNET=false to trade live.
Never hard-code API keys; set BINANCE_API_KEY / BINANCE_API_SECRET as
environment variables (or in a local, gitignored .env file).

Usage (usually driven by run_bot.py, but runnable standalone for the
monitor loop):
    python trader.py
"""

import json
import re
import threading
import time
from datetime import datetime, timezone

import ccxt

import config
import perfio
import ratelimit


class Trader:
    EXCHANGE_ID = "binance"
    def __init__(self, mirror=None):
        """mirror: optional BybitMirror adapter. When attached, every trade this
        Trader places on Binance is ALSO dispatched to the mirror, which
        translates entry/SL/TP by the live Binance↔Bybit spread and executes on
        Bybit. Each exchange enforces its OWN concurrent-position cap, so a
        trade Binance declines (account full) can still go to Bybit and vice
        versa — run_bot.py wires the two together."""
        if not config.BINANCE_API_KEY or not config.BINANCE_API_SECRET:
            raise RuntimeError(
                "BINANCE_API_KEY / BINANCE_API_SECRET are not set. "
                "Export them as environment variables or put them in a local .env file."
            )

        self.mirror = mirror

        # Initialize CCXT Binance USD-M exchange with stripped keys
        self.exchange = ccxt.binanceusdm({
            "apiKey": config.BINANCE_API_KEY.strip(),
            "secret": config.BINANCE_API_SECRET.strip(),
            "enableRateLimit": True,
        })

        # Enable CCXT's native Binance Demo Trading mode
        if config.TESTNET:
            self.exchange.enableDemoTrading(True)

        # Open trades tracked for reporting/PnL. Restored from the persistent
        # ledger (trades.json) so a restart never orphans a live position —
        # the monitor re-adopts anything it finds on the exchange anyway.
        self._open_trades: dict = {}   # symbol -> meta dict for each open position
        ledger = perfio.load_ledger()
        self._open_trades.update(ledger.get(self.EXCHANGE_ID, {}))

        # Entry follower: owns every resting LIMIT entry order until it fills
        # (then attaches TP/SL + tracks the position) or is cancelled.
        self._pending_entries: dict = {}
        self._pending_lock = threading.Lock()
        self._listen_key: str | None = None
        self._ws_thread = threading.Thread(target=self._user_data_stream_loop, daemon=True)
        self._ws_thread.start()
        self._cancel_thread = threading.Thread(target=self._cancel_timer_loop, daemon=True)
        self._cancel_thread.start()

        perfio.ensure_perf_header()

    # ── Serialized, ban-aware ccxt call ────────────────────────────────
    # Every Binance REST call in the process shares one at-a-time lock + the
    # global fail-fast ban state (ratelimit), so the trader never polls
    # concurrently with data fetches and never pokes an active IP ban.
    def _guarded(self, fn, *args, **kwargs):
        with ratelimit.serialized():
            ratelimit.fail_fast_if_banned()
            return fn(*args, **kwargs)

    # ── Position state ───────────────────────────────────────────────────
    def get_open_positions(self):
        try:
            positions = self._guarded(self.exchange.fetch_positions)
            return [
                p for p in positions 
                if float(p.get("contracts") or p.get("info", {}).get("positionAmt", 0)) != 0
            ]
        except ratelimit.BinanceBanned:
            raise  # caller decides to sleep out the ban — never fake "no position"
        except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
            print(f"  [trader] Network error fetching positions: {e}")
            # Return empty or handle gracefully depending on safety requirements
            return []
        except Exception as e:
            print(f"  [trader] Unexpected error fetching positions: {e}")
            return []

    def fetch_account_state(self) -> dict | None:
        """Exchange-reported account state for PnL verification. Binance's
        fapi account exposes totalWalletBalance (realized wallet), totalUnrealizedProfit
        and totalMarginBalance (= wallet + unrealized = equity). Best-effort:
        returns None on any failure so the monitor never blocks on it."""
        try:
            bal = self._guarded(self.exchange.fetch_balance)
            info = bal.get("info") or {}

            def f(*keys):
                for k in keys:
                    v = info.get(k)
                    if v not in (None, ""):
                        try:
                            return float(v)
                        except (TypeError, ValueError):
                            pass
                return None

            wallet = f("totalWalletBalance")
            unrealized = f("totalUnrealizedProfit")
            equity = f("totalMarginBalance")
            available = f("totalAvailableBalance")
            if available is None:
                try:
                    available = float(bal.get("USDT", {}).get("free") or 0)
                except (TypeError, ValueError):
                    available = None
            if equity is None and wallet is not None and unrealized is not None:
                equity = wallet + unrealized
            if wallet is None:
                return None
            return {
                "wallet": wallet,
                "unrealized": unrealized,
                "equity": equity,
                "available": available,
                "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        except ratelimit.BinanceBanned:
            return None   # the positions poll already sleeps out the ban
        except Exception as e:
            print(f"  [trader] Account state fetch failed: {e}")
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
        """The exchange's OWN realized PnL for a position: income history for
        the symbol since it opened, summing REALIZED_PNL + COMMISSION +
        FUNDING_FEE. This includes the real exit price plus fees/funding — vs
        our last-mark estimate which is what was never matched the exchange.
        Returns None on any failure so the caller falls back to last mark PnL."""
        try:
            params = {"symbol": symbol, "limit": 1000}
            if since_ms is not None:
                params["startTime"] = since_ms
            inc = self._guarded(self.exchange.fapiprivateGetIncome, params)
            total = 0.0
            for r in inc:
                if r.get("incomeType") in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE"):
                    total += float(r.get("income") or 0)
            # A genuinely closed position always books income (at least a
            # commission + a realized pnl), so an empty/zero sum means the
            # data isn't trustworthy yet — caller falls back to last mark PnL.
            return total if total != 0.0 else None
        except Exception as e:
            print(f"  [trader] Income fetch failed for {symbol}: {e}")
            return None

    def _final_realized(self, symbol: str, meta: dict):
        """Realized PnL to book for a closed position. Prefer the exchange's
        income-derived figure, but ONLY when the position was tracked from its
        real open (adopted positions have no trustworthy opened_at — an income
        window starting 'now' would wrongly book 0). Fall back to the last
        observed mark PnL otherwise."""
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
        """Slots in use: filled positions + resting/unfilled LIMIT entries.
        Pending entries reserve a slot, so stacked orders can never fill past
        MAX_CONCURRENT_POSITIONS — a 4th trade only becomes possible once one
        of the current trades closes (TP/SL) and frees its slot."""
        return len(self.get_open_positions()) + self.pending_entry_count()

    def has_open_position(self) -> bool:
        return self.open_capacity_used() >= config.MAX_CONCURRENT_POSITIONS

    def open_position_count(self) -> int:
        return len(self.get_open_positions())

    def _valid_symbol(self, symbol: str) -> bool:
        """Check the symbol is tradeable on this exchange. A liquidation feed
        can reference symbols that are no longer tradeable on USD-M (delisted,
        renamed), so any symbol that isn't in the loaded markets must be
        rejected BEFORE calling price_to_precision / set_leverage — those raise
        an uncaught ccxt.BadSymbol and would otherwise kill the whole bot."""
        try:
            markets = self.exchange.markets or self.exchange.load_markets()
            return symbol in self.exchange.markets_by_id
        except Exception:
            # Can't load markets for some reason — let the guarded calls below
            # surface the real error rather than rejecting valid trades.
            return True

    # ── Sizing ───────────────────────────────────────────────────────────
    def _position_size(self, symbol: str, entry: float, sl: float) -> float:
        # Fixed allocation — hardcoded, never scaled. The trade commits exactly
        # RISK_PER_TRADE_USD of margin at LEVERAGEx, so notional = USD * LEVERAGE
        # and qty = notional / entry. SL is not part of sizing.
        if entry <= 0:
            return 0.0
        qty = (config.RISK_PER_TRADE_USD * config.LEVERAGE) / entry

        # Pre-trade margin check: if the allocated margin isn't available in the
        # free balance, skip the trade entirely — do NOT scale down. The user's
        # env value (LIQ_RISK_USD) is the hard cap per trade.
        try:
            balance = self._guarded(self.exchange.fetch_balance)
            free = float(balance.get("USDT", {}).get("free", 0) or 0)
            if config.RISK_PER_TRADE_USD > free:
                print(f"  [trader] Margin check {symbol}: allocated "
                      f"{config.RISK_PER_TRADE_USD:.2f} USDT margin > free "
                      f"{free:.2f} USDT — skipping, not enough margin.")
                self._log_trade_event({"event": "margin_insufficient", "symbol": symbol,
                                       "allocated": config.RISK_PER_TRADE_USD, "free": free})
                return 0.0
        except ratelimit.BinanceBanned:
            raise
        except Exception as e:
            print(f"  [trader] Could not check margin for {symbol}: {e}")
            self._log_trade_event({"event": "margin_check_error", "symbol": symbol, "error": str(e)})

        try:
            precise_qty = float(self.exchange.amount_to_precision(symbol, qty))
        except Exception:
            precise_qty = qty

        target_notional = config.RISK_PER_TRADE_USD * config.LEVERAGE
        actual_notional = precise_qty * entry
        if target_notional > 0 and actual_notional / target_notional < config.MIN_FILL_RATIO:
            print(f"  [trader] {symbol}: precision rounding collapsed notional from "
                  f"${target_notional:.0f} to ${actual_notional:.2f} "
                  f"({actual_notional/target_notional:.0%} of target) — skipping, "
                  f"position too small to be useful.")
            self._log_trade_event({
                "event": "skipped_small_notional", "symbol": symbol,
                "target_notional": round(target_notional, 2),
                "actual_notional": round(actual_notional, 2),
                "qty_raw": round(qty, 8), "qty_precise": precise_qty,
            })
            return 0.0

        return precise_qty

    # ── Execution ────────────────────────────────────────────────────────
    def execute_trade(self, levels: dict, deadline_ts: float | None = None,
                      wait_for_fill: bool = True, candle_open_ts: float | None = None):
        """levels is allocation.compute_levels()'s return dict.

        deadline_ts: Optional Unix timestamp cutoff. Trade aborts if current time exceeds this.
        wait_for_fill: False = place the LIMIT order and return immediately without
            blocking on the fill outcome — the entry follower owns the fill/reject
            and TP/SL attach in the background. Used by the pre-calc path so a
            coin at close never stalls the pipeline waiting for a fill; the next
            pending coin / signal is processed right away.
        candle_open_ts: Unix timestamp of the candle open (same as candle_close_time
            of the previous candle). Used to set the LIMIT fill deadline at exactly
            candle_open + LIMIT_FILL_WINDOW_SECONDS, independent of placement time.
        """
        # 1. Deadline Check: Prevent executing stale signals
        if deadline_ts is not None and time.time() > deadline_ts:
            print(
                f"  [trader] Aborting {levels.get('symbol', 'trade')} — signal deadline expired "
                f"({time.time() - deadline_ts:.2f}s late)."
            )
            self._log_trade_event({"event": "deadline_expired", "symbol": levels.get("symbol", ""), **levels})
            return None

        if self.has_open_position():
            print(f"  [trader] Skipping {levels['symbol']} — "
                  f"{self.open_position_count()} open + {self.pending_entry_count()} pending "
                  f"= {self.open_capacity_used()} slot(s) used, max {config.MAX_CONCURRENT_POSITIONS}.")
            self._log_trade_event({"event": "capacity_full", "symbol": levels["symbol"], **levels})
            return None

        symbol = levels["symbol"]
        direction = levels["direction"]
        entry_hint = levels["entry"]
        sl, tp = levels["sl"], levels["tp"]

        # Symbol validation: a liquidation feed can carry delisted/renamed
        # symbols that are not tradeable on USD-M. Reject early instead of
        # letting price_to_precision raise an uncaught BadSymbol that kills
        # the whole bot.
        if not self._valid_symbol(symbol):
            print(f"  [trader] Skipping {symbol} — not a tradeable market on {self.exchange.id}.")
            self._log_trade_event({"event": "bad_symbol", "symbol": symbol, **levels})
            return None

        with self._pending_lock:
            already_pending = any(
                e["symbol"] == symbol and not e.get("resolved")
                for e in self._pending_entries.values()
            )
        if already_pending:
            print(f"  [trader] Skipping {symbol} — a LIMIT entry is already pending.")
            self._log_trade_event({"event": "already_pending", "symbol": symbol, **levels})
            return None

        side = "buy" if direction == "long" else "sell"

        qty = self._position_size(symbol, entry_hint, sl)
        if qty <= 0:
            print(f"  [trader] Zero/invalid position size for {symbol}, skipping.")
            self._log_trade_event({"event": "zero_position_size", "symbol": symbol, **levels})
            return None

        # 2. Re-check deadline right before API order placement
        if deadline_ts is not None and time.time() > deadline_ts:
            print(f"  [trader] Aborting {symbol} right before order placement — deadline exceeded.")
            self._log_trade_event({"event": "deadline_expired", "symbol": symbol, **levels})
            return None

        try:
            self._guarded(self.exchange.set_leverage, config.LEVERAGE, symbol)
        except Exception as e:
            print(f"  [trader] Could not set leverage: {e}")
            self._log_trade_event({"event": "leverage_set_failed", "symbol": symbol, "error": str(e)})

        # 3. LIMIT entry at the interest candle's exact close price — precise
        #    fill price, never a market order. The order waits at `entry` and
        #    is polled until it fills or the deadline passes (then cancelled).
        try:
            entry_price = self.exchange.price_to_precision(symbol, entry_hint)
        except ccxt.BadSymbol as e:
            print(f"  [trader] Symbol {symbol} not found on {self.exchange.id}: {e}")
            self._log_trade_event({"event": "bad_symbol", "error": str(e), **levels})
            return None
        except Exception as e:
            print(f"  [trader] Could not price entry for {symbol}: {e}")
            self._log_trade_event({"event": "order_failed", "error": str(e), **levels})
            return None
        try:
            entry_order = self._guarded(
                self.exchange.create_order, symbol, "limit", side, qty, price=entry_price)
        except ratelimit.BinanceBanned:
            raise  # run_bot sleeps the ban out — the pending entry is never placed
        except Exception as e:
            print(f"  [trader] LIMIT entry placement failed for {symbol}: {e}")
            self._log_trade_event({"event": "order_failed", "error": str(e), **levels})
            return None

        # 4. Hand the resting entry to the follower thread — it owns the fill
        #    watch, the TP/SL attach on fill (Binance USD-M carries no TP/SL
        #    on a limit order), and the deadline cancel. execute_trade only
        #    waits for the outcome so run_bot stays serialized per signal.
        order_id = entry_order.get("id")
        rec = {
            "order_id": order_id,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "entry_price": entry_price,
            "sl": sl,
            "tp": tp,
            "direction": direction,
            "levels": levels,
            "deadline_ts": deadline_ts,
            "placed_at": time.time(),
            "cancel_at": (candle_open_ts + config.LIMIT_FILL_WINDOW_SECONDS
                          if candle_open_ts is not None
                          else time.time() + config.LIMIT_FILL_WINDOW_SECONDS),
            "cancel_attempted": False,
            "attached": False,
            "attached_sl": False,
            "attached_tp": False,
            "attach_failures": 0,
            "resolved": False,
            "filled": False,
            "event": threading.Event(),
        }
        with self._pending_lock:
            self._pending_entries[order_id] = rec

        timeout_at = rec["cancel_at"]

        if not wait_for_fill:
            # Order placed — return now. The entry follower watches for the
            # fill, attaches TP/SL, or cancels at deadline, all in background.
            print(f"  [trader] LIMIT {side.upper()} {symbol} qty={qty} @ {entry_price} placed — "
                  f"not waiting for fill, continuing pipeline.")
            self._log_trade_event({"event": "order_placed", "symbol": symbol, "direction": direction,
                                   "qty": qty, "entry": entry_price, "sl": sl, "tp": tp, "side": side})
            return entry_order

        rec["event"].wait(max(timeout_at - time.time(), 0.0))

        if not rec["filled"] and not rec["resolved"]:
            print(f"  [trader] LIMIT entry {symbol} still open at deadline — the entry follower "
                  f"keeps watching and will attach TP/SL if it fills, or cancel it.")
            return None
        return entry_order if rec["filled"] else None

    # ── Multi-exchange dispatch ──────────────────────────────────────────
    def dispatch_trade(self, levels: dict, deadline_ts: float | None = None,
                       wait_for_fill: bool = True, candle_open_ts: float | None = None):
        """Execute `levels` on Binance, then mirror it to the attached Bybit
        mirror (if any). The mirror runs under its own try/except and its own
        capacity/listing checks, so a Bybit failure NEVER cancels or alters the
        Binance trade — the worst case is the Binance trade proceeds alone."""
        result = self.execute_trade(levels, deadline_ts=deadline_ts,
                                    wait_for_fill=wait_for_fill,
                                    candle_open_ts=candle_open_ts)

        mirror = getattr(self, "mirror", None)
        if mirror is not None:
            try:
                mirror.execute_trade(levels, deadline_ts=deadline_ts,
                                     wait_for_fill=wait_for_fill,
                                     candle_open_ts=candle_open_ts)
            except Exception as e:
                print(f"  [trader] Bybit mirror failed for {levels.get('symbol')}: {e}")
        return result

    # ── Entry follower (WebSocket-based) ────────────────────────────────
    def _user_data_stream_loop(self):
        """Daemon: maintain a Binance user data stream (WebSocket) for instant
        order fill notifications. Reconnects automatically on disconnects.
        The listenKey is kept alive via REST PUT every 30 minutes."""
        import websocket

        while True:
            try:
                # Create a fresh listenKey
                self._listen_key = self._create_listen_key()
                if not self._listen_key:
                    print(f"  [trader] Could not create listenKey, retrying in 30s...")
                    time.sleep(30)
                    continue

                if config.TESTNET:
                    ws_url = f"wss://stream.binancefuture.com/ws/{self._listen_key}"
                else:
                    ws_url = f"wss://fstream.binance.com/ws/{self._listen_key}"
                print(f"  [trader] Connecting user data stream...")

                def on_message(ws, message):
                    try:
                        data = json.loads(message)
                        event_type = data.get("e")
                        if event_type == "ORDER_TRADE_UPDATE":
                            self._handle_order_update(data)
                    except Exception as e:
                        print(f"  [trader] WS message error: {e}")

                def on_error(ws, error):
                    print(f"  [trader] WS error: {error}")

                def on_close(ws, close_status_code, close_msg):
                    print(f"  [trader] WS closed ({close_status_code}: {close_msg})")

                def on_open(ws):
                    print(f"  [trader] User data stream connected")

                ws = websocket.WebSocketApp(
                    ws_url,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_open=on_open,
                )

                # Run with a 30-minute keepalive ping
                ws_thread = threading.Thread(target=ws.run_forever, kwargs={
                    "ping_interval": 1800,
                    "ping_timeout": 10,
                }, daemon=True)
                ws_thread.start()

                # Keep the listenKey alive while the WS is running
                while ws_thread.is_alive():
                    time.sleep(60)
                    try:
                        self._guarded(
                            self.exchange.fapiPrivatePutListenKey,
                        )
                    except Exception as e:
                        print(f"  [trader] ListenKey keepalive failed: {e}")
                        break

                ws.close()
                self._delete_listen_key()
                print(f"  [trader] User data stream disconnected, reconnecting...")

            except Exception as e:
                print(f"  [trader] User data stream error: {e}")
                time.sleep(5)

    def _create_listen_key(self) -> str | None:
        """Create a new Binance USD-M listenKey for the user data stream."""
        try:
            result = self._guarded(self.exchange.fapiPrivatePostListenKey)
            return result.get("listenKey")
        except Exception as e:
            print(f"  [trader] Failed to create listenKey: {e}")
            return None

    def _delete_listen_key(self):
        """Delete the current listenKey to cleanly close the user data stream."""
        if self._listen_key:
            try:
                self._guarded(self.exchange.fapiPrivateDeleteListenKey,
                              {"listenKey": self._listen_key})
            except Exception:
                pass  # best-effort cleanup
            self._listen_key = None

    def _handle_order_update(self, data: dict):
        """Process an ORDER_TRADE_UPDATE event from the user data stream.
        On fill, attach TP/SL and track the position. On cancel/expired,
        finalize as failed."""
        order = data.get("o", {})
        order_id = str(order.get("i", ""))
        status = order.get("X", "")
        filled_qty = float(order.get("z", 0))

        if not order_id:
            return

        with self._pending_lock:
            rec = self._pending_entries.get(order_id)
        if not rec or rec.get("resolved"):
            return

        if status == "FILLED" or filled_qty > 0:
            if not rec.get("filled"):
                rec["filled"] = True
                rec["meta"] = {
                    **rec["levels"],
                    "qty": filled_qty if filled_qty > 0 else rec["qty"],
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                }
                self._open_trades[rec["symbol"]] = rec["meta"]
                rec["event"].set()
                print(f"  [trader] Filled {rec['direction'].upper()} {rec['symbol']} "
                      f"qty={rec['meta']['qty']} entry={rec['entry_price']}"
                      f" (WS fill, status={status}, filled={filled_qty})")
            result = self._attach_tp_sl(rec)
            if result is True:
                self._finalize_filled(rec)
            elif result == "abandon":
                self._finalize_failed(rec, "attach_abandoned")
        elif status in ("CANCELED", "EXPIRED", "REJECTED"):
            self._finalize_failed(rec, status.lower())

    def _cancel_timer_loop(self):
        """Daemon: check every pending LIMIT entry and cancel it when the fill
        window expires. Runs independently of the WebSocket — the WS handles
        fill events, this handles timeouts."""
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
                            self._guarded(self.exchange.cancel_order, rec["order_id"], rec["symbol"])
                            self._log_trade_event({"event": "order_cancelled",
                                                   "symbol": rec["symbol"],
                                                   "order_id": rec["order_id"]})
                        except ratelimit.BinanceBanned:
                            continue
                        except Exception as e:
                            print(f"  [trader] Could not cancel unfilled LIMIT entry for {rec['symbol']}: {e}")
                            self._log_trade_event({"event": "cancel_failed",
                                                   "symbol": rec["symbol"],
                                                   "order_id": rec["order_id"],
                                                   "error": str(e)})
            except Exception as e:
                print(f"  [trader] Cancel timer error: {e}")
            time.sleep(1)

    def _attach_tp_sl(self, rec):
        """Idempotent TP/SL attach after a LIMIT fill. Each half (STOP_MARKET /
        TAKE_PROFIT_MARKET, closePosition) is placed at most once — a Binance
        -4130 ("a closePosition order with GTE already exists") is treated as
        success for that half, so a half-completed attach from an earlier attempt
        recovers instead of re-submitting the order forever. -4509 ("no open
        position behind GTE closePosition") is permanent — the position is gone,
        so the entry is abandoned rather than retried. Any other error retries
        up to MAX_TP_SL_ATTACH_ATTEMPTS, then abandons so a filled-but-unmanageable
        entry can never reprint oco_failed every second forever.

        Returns True (attach complete), False (retry later), or "abandon"
        (permanent failure — caller finalizes the rec)."""
        if rec.get("attached"):
            return True
        close_side = "sell" if rec["direction"] == "long" else "buy"
        halves = [
            ("sl", "STOP_MARKET", rec["sl"]),
            ("tp", "TAKE_PROFIT_MARKET", rec["tp"]),
        ]
        for key, order_type, stop_price in halves:
            if rec.get(f"attached_{key}"):
                continue
            try:
                self._guarded(
                    self.exchange.create_order, rec["symbol"], order_type, close_side, rec["qty"],
                    params={"stopPrice": stop_price, "closePosition": True},
                )
                rec[f"attached_{key}"] = True
            except ratelimit.BinanceBanned:
                return False  # never poke an active ban — follower retries on a later cycle
            except Exception as e:
                code = self._binance_error_code(e)
                if code == -4130:
                    # The closePosition order for this half already exists (left
                    # over from a previous half-completed attach). Count it done.
                    rec[f"attached_{key}"] = True
                elif code == -4509:
                    # No position behind the fill — GTE closePosition can never be
                    # placed. Permanent: abandon this entry so it stops retrying.
                    print(f"  [trader] TP/SL attach aborted for {rec['symbol']} — no open "
                          f"position behind the fill ({str(e)[:160]}).")
                    self._log_trade_event({
                        "event": "oco_failed",
                        "reason": "no_position",
                        "error": str(e),
                        **rec["levels"],
                    })
                    return "abandon"
                else:
                    rec["attach_failures"] = rec.get("attach_failures", 0) + 1
                    if rec["attach_failures"] >= config.MAX_TP_SL_ATTACH_ATTEMPTS:
                        print(f"  [trader] TP/SL attach failed {rec['attach_failures']} "
                              f"times for {rec['symbol']} — abandoning.")
                        self._log_trade_event({
                            "event": "oco_failed",
                            "reason": "max_attempts",
                            "attempts": rec["attach_failures"],
                            "error": str(e),
                            **rec["levels"],
                        })
                        return "abandon"
                    print(f"  [trader] TP/SL attach failed after LIMIT fill for {rec['symbol']}: {e}")
                    self._log_trade_event({"event": "oco_failed", "error": str(e), **rec["levels"]})
                    return False

        rec["attached"] = True
        return True

    @staticmethod
    def _binance_error_code(e) -> int | None:
        """Extract the numeric `code` from a ccxt ExchangeError that embeds the
        raw Binance body, e.g. `binance {"code":-4130,"msg":"..."}`."""
        m = re.search(r'"code"\s*:\s*(-?\d+)', str(e))
        return int(m.group(1)) if m else None

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
        print(f"  [trader] Opened {rec['direction'].upper()} {rec['symbol']} "
              f"qty={rec['qty']} entry={rec['entry_price']} (LIMIT filled) "
              f"sl={rec['sl']} tp={rec['tp']} — TP/SL attached")
        rec["event"].set()

    def _finalize_failed(self, rec, status):
        with self._pending_lock:
            if rec.get("resolved"):
                return
            rec["resolved"] = True
            self._pending_entries.pop(rec["order_id"], None)
        self._log_trade_event({"event": "entry_not_filled", "status": status, **rec["levels"]})
        print(f"  [trader] LIMIT entry {status} for {rec['symbol']}, abandoning.")
        rec["event"].set()

    # ── Monitoring / performance logging ────────────────────────────────
    # The EXCHANGE is the source of truth for what is open. Every position
    # found on the exchange is adopted into the ledger (tracked or untracked)
    # so it always gets a close row; anything in the ledger that vanishes is
    # declared closed after POSITION_CLOSED_CONFIRM_POLLS consecutive absent
    # polls, and the closed row carries the final mark/PnL so realized PnL is
    # self-contained. A live snapshot is written every poll — the dashboard
    # reads snapshot.json for "open", never CSV history.

    def _log_performance_row(self, symbol, direction, qty, entry, sl, tp, mark_price, pnl, status):
        perfio.append_perf_row([
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            symbol, direction, qty, entry, sl, tp, mark_price, pnl, status,
            self.EXCHANGE_ID,
        ])

    def _log_trade_event(self, record: dict):
        record["exchange"] = self.EXCHANGE_ID
        record["time"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        perfio.append_log_event(record)

    def _adopt_position(self, symbol, pos, contracts) -> dict:
        """A position exists on the exchange that isn't in the ledger (opened
        before a restart, or never tracked). Check the signals file first — if
        the system placed this trade, use the stored entry/tp/sl instead of
        adopting with blanks. Only truly external positions get adopted."""
        stored = perfio.find_signal(self.EXCHANGE_ID, symbol)
        if stored is not None:
            meta = {
                "symbol": symbol,
                "direction": stored.get("direction", "long" if contracts > 0 else "short"),
                "qty": stored.get("qty", abs(contracts)),
                "entry": stored.get("entry", float(pos.get("entryPrice") or 0)),
                "sl": stored.get("sl", ""),
                "tp": stored.get("tp", ""),
                "opened_at": stored.get("opened_at", datetime.now(timezone.utc).isoformat()),
            }
            self._open_trades[symbol] = meta
            self._log_trade_event({"event": "opened", **meta})
            perfio.update_ledger(self.EXCHANGE_ID, self._open_trades)
            print(f"  [trader] Restored from signals file: {symbol} "
                  f"{meta['direction']} qty={meta['qty']} sl={meta['sl']} tp={meta['tp']}")
            return meta
        meta = {
            "symbol": symbol,
            "direction": "long" if contracts > 0 else "short",
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
        print(f"  [trader] Adopted untracked position: {symbol} "
              f"{meta['direction']} qty={meta['qty']}")
        return meta

    def _rectify_naked_positions(self):
        """Startup pass: find every open position on Binance and ensure it has
        TP/SL orders attached. If a position is naked (missing one or both),
        fetch the stored TP/SL from signals.json and place the missing orders.
        This catches positions where the attach failed silently, the exchange
        cancelled the orders, or the system restarted mid-attach."""
        try:
            positions = self.get_open_positions()
        except Exception as e:
            print(f"  [trader] Cannot rectify — failed to fetch positions: {e}")
            return

        for pos in positions:
            raw_symbol = (pos.get("info") or {}).get("symbol")
            symbol = (raw_symbol or pos.get("symbol") or "").upper()
            contracts = float(
                pos.get("contracts") or pos.get("info", {}).get("positionAmt", 0) or 0
            )
            if contracts == 0:
                continue

            meta = self._open_trades.get(symbol)
            sl = meta.get("sl", "") if meta else ""
            tp = meta.get("tp", "") if meta else ""

            if not sl and not tp:
                stored = perfio.find_signal(self.EXCHANGE_ID, symbol)
                if stored:
                    sl = stored.get("sl", "")
                    tp = stored.get("tp", "")

            if not sl and not tp:
                continue

            try:
                open_orders = self._guarded(self.exchange.fetch_open_orders, symbol)
            except Exception as e:
                print(f"  [trader] Rectify {symbol}: could not fetch open orders: {e}")
                continue

            has_sl = any(o.get("type") == "STOP_MARKET" for o in open_orders)
            has_tp = any(o.get("type") == "TAKE_PROFIT_MARKET" for o in open_orders)

            if has_sl and has_tp:
                continue

            close_side = "sell" if (meta.get("direction") if meta else contracts > 0) else "buy"
            qty = abs(contracts)

            if sl and not has_sl:
                try:
                    self._guarded(
                        self.exchange.create_order, symbol, "STOP_MARKET", close_side, qty,
                        params={"stopPrice": sl, "closePosition": True},
                    )
                    print(f"  [trader] Rectified SL for {symbol} @ {sl}")
                    self._log_trade_event({"event": "rectified_sl", "symbol": symbol,
                                           "sl": sl, "qty": qty, "direction": close_side})
                except Exception as e:
                    code = self._binance_error_code(e)
                    if code == -4130:
                        print(f"  [trader] Rectify {symbol}: SL order already exists (-4130)")
                    else:
                        print(f"  [trader] Rectify {symbol}: SL attach failed: {e}")
                        self._log_trade_event({"event": "rectify_failed", "symbol": symbol,
                                               "side": "sl", "error": str(e)})

            if tp and not has_tp:
                try:
                    self._guarded(
                        self.exchange.create_order, symbol, "TAKE_PROFIT_MARKET", close_side, qty,
                        params={"stopPrice": tp, "closePosition": True},
                    )
                    print(f"  [trader] Rectified TP for {symbol} @ {tp}")
                    self._log_trade_event({"event": "rectified_tp", "symbol": symbol,
                                           "tp": tp, "qty": qty, "direction": close_side})
                except Exception as e:
                    code = self._binance_error_code(e)
                    if code == -4130:
                        print(f"  [trader] Rectify {symbol}: TP order already exists (-4130)")
                    else:
                        print(f"  [trader] Rectify {symbol}: TP attach failed: {e}")
                        self._log_trade_event({"event": "rectify_failed", "symbol": symbol,
                                               "side": "tp", "error": str(e)})

    def monitor_loop(self):
        """Run forever: poll open positions, log PnL, detect closes, snapshot."""
        print("  [trader] Monitor loop started.")
        absent_streaks: dict[str, int] = {}  # symbol -> consecutive absent polls
        self._rectify_naked_positions()
        while True:
            try:
                try:
                    positions = self.get_open_positions()
                except ratelimit.BinanceBanned as e:
                    print(f"  [trader] IP banned by Binance ({e.remaining:.0f}s) — monitor paused, no polling.")
                    time.sleep(min(e.remaining, 60.0))
                    continue
                except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
                    print(f"  [trader] Network error polling positions: {e} — skipping poll cycle.")
                    time.sleep(config.POSITION_POLL_SECONDS)
                    continue

                account = self.fetch_account_state()
                positions_dict = {}
                open_symbols = set()

                for pos in positions:
                    # ccxt returns the UNIFIED symbol (e.g. "BEAT/USDT:USDT") on a
                    # position, but _open_trades is keyed by the raw exchange id
                    # (e.g. "BEATUSDT"). Use the raw id from pos.info so tracked
                    # trades match their open position.
                    raw_symbol = (pos.get("info") or {}).get("symbol")
                    symbol = (raw_symbol or pos.get("symbol") or "").upper()
                    open_symbols.add(symbol)

                    contracts = float(
                        pos.get("contracts") or pos.get("info", {}).get("positionAmt", 0) or 0
                    )
                    mark = float(pos.get("markPrice") or 0)
                    pnl = float(pos.get("unrealizedPnl") or 0)

                    meta = self._open_trades.get(symbol)
                    if meta is None:
                        meta = self._adopt_position(symbol, pos, contracts)
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

                # Tracked/adopted but no longer on the exchange — confirm-streak
                # before declaring closed so a transient poll hiccup never books
                # a fake close.
                changed = False
                for symbol in list(self._open_trades.keys()):
                    if symbol not in open_symbols:
                        absent_streaks[symbol] = absent_streaks.get(symbol, 0) + 1
                        if absent_streaks[symbol] >= config.POSITION_CLOSED_CONFIRM_POLLS:
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
                            print(f"  [trader] Position closed: {meta['symbol']} "
                                  f"(realized {rpnl} via {source})")
                            absent_streaks.pop(symbol, None)
                            changed = True
                            perfio.remove_signal(self.EXCHANGE_ID, symbol)
                if changed:
                    perfio.update_ledger(self.EXCHANGE_ID, self._open_trades)

                perfio.update_snapshot(self.EXCHANGE_ID, positions_dict, account)

            except Exception as e:
                print(f"  [trader] Monitor loop error: {e}")

            time.sleep(config.POSITION_POLL_SECONDS)


if __name__ == "__main__":
    Trader().monitor_loop()