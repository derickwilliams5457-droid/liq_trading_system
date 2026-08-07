"""
trader.py  —  Stage 5 of the pipeline
=======================================
Takes a direction + SL + TP from allocation.py and places the trade on
Binance Futures via ccxt. Before opening anything it checks for any existing
open position across the whole account and refuses to open a second one.
A background loop polls open-position PnL and appends a row to
config.PERFORMANCE_CSV on every poll, plus a final row when a position closes.
A second background loop — the "entry follower" — watches every resting LIMIT
entry order until it fills or dies. Binance USD-M does not carry TP/SL on a
limit order, so the follower attaches the STOP_MARKET / TAKE_PROFIT_MARKET
(closePosition) pair the moment the fill is seen and tracks the position, so
no fill is ever left unmanaged.

⚠️  This places real orders against a real exchange account. config.TESTNET
defaults to True — you must explicitly set LIQ_TESTNET=false to trade live.
Never hard-code API keys; set BINANCE_API_KEY / BINANCE_API_SECRET as
environment variables (or in a local, gitignored .env file).

Usage (usually driven by run_bot.py, but runnable standalone for the
monitor loop):
    python trader.py
"""

import csv
import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import ccxt

import config
import ratelimit


class Trader:
    def __init__(self):
        if not config.BINANCE_API_KEY or not config.BINANCE_API_SECRET:
            raise RuntimeError(
                "BINANCE_API_KEY / BINANCE_API_SECRET are not set. "
                "Export them as environment variables or put them in a local .env file."
            )

        # Initialize CCXT Binance USD-M exchange with stripped keys
        self.exchange = ccxt.binanceusdm({
            "apiKey": config.BINANCE_API_KEY.strip(),
            "secret": config.BINANCE_API_SECRET.strip(),
            "enableRateLimit": True,
        })

        # Enable CCXT's native Binance Demo Trading mode
        if config.TESTNET:
            self.exchange.enableDemoTrading(True)

        self._open_trades: dict = {}   # symbol -> meta dict for each open position

        # Entry follower: owns every resting LIMIT entry order until it fills
        # (then attaches TP/SL + tracks the position) or is cancelled.
        self._pending_entries: dict = {}
        self._pending_lock = threading.Lock()
        self._follower_thread = threading.Thread(target=self._entry_follower_loop, daemon=True)
        self._follower_thread.start()

        self._init_performance_csv()

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

    def has_open_position(self) -> bool:
        return len(self.get_open_positions()) >= config.MAX_CONCURRENT_POSITIONS

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
            return symbol in markets
        except Exception:
            # Can't load markets for some reason — let the guarded calls below
            # surface the real error rather than rejecting valid trades.
            return True

    # ── Sizing ───────────────────────────────────────────────────────────
    def _position_size(self, symbol: str, entry: float, sl: float) -> float:
        balance = self._guarded(self.exchange.fetch_balance)
        equity = float(balance.get("USDT", {}).get("total", 0) or 0)
        risk_amount = equity * (config.RISK_PER_TRADE_PCT / 100)
        per_unit_risk = abs(entry - sl)
        if per_unit_risk <= 0:
            return 0.0
        qty = risk_amount / per_unit_risk

        try:
            return float(self.exchange.amount_to_precision(symbol, qty))
        except Exception:
            return qty

    # ── Execution ────────────────────────────────────────────────────────
    def execute_trade(self, levels: dict, deadline_ts: float | None = None,
                      wait_for_fill: bool = True):
        """levels is allocation.compute_levels()'s return dict.

        deadline_ts: Optional Unix timestamp cutoff. Trade aborts if current time exceeds this.
        wait_for_fill: False = place the LIMIT order and return immediately without
            blocking on the fill outcome — the entry follower owns the fill/reject
            and TP/SL attach in the background. Used by the pre-calc path so a
            coin at close never stalls the pipeline waiting for a fill; the next
            pending coin / signal is processed right away.
        """
        # 1. Deadline Check: Prevent executing stale signals
        if deadline_ts is not None and time.time() > deadline_ts:
            print(
                f"  [trader] Aborting {levels.get('symbol', 'trade')} — signal deadline expired "
                f"({time.time() - deadline_ts:.2f}s late)."
            )
            return None

        if self.has_open_position():
            print(f"  [trader] Skipping {levels['symbol']} — a position is already open.")
            return None

        if levels.get("risk_reward") is None or levels["risk_reward"] < config.MIN_RISK_REWARD:
            print(
                f"  [trader] Skipping {levels['symbol']} — R:R {levels.get('risk_reward')} "
                f"below MIN_RISK_REWARD ({config.MIN_RISK_REWARD})."
            )
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
            return None

        side = "buy" if direction == "long" else "sell"

        qty = self._position_size(symbol, entry_hint, sl)
        if qty <= 0:
            print(f"  [trader] Zero/invalid position size for {symbol}, skipping.")
            return None

        # 2. Re-check deadline right before API order placement
        if deadline_ts is not None and time.time() > deadline_ts:
            print(f"  [trader] Aborting {symbol} right before order placement — deadline exceeded.")
            return None

        try:
            self._guarded(self.exchange.set_leverage, config.LEVERAGE, symbol)
        except Exception as e:
            print(f"  [trader] Could not set leverage: {e}")

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
            "cancel_attempted": False,
            "attached": False,
            "resolved": False,
            "filled": False,
            "event": threading.Event(),
        }
        with self._pending_lock:
            self._pending_entries[order_id] = rec

        timeout_at = deadline_ts if deadline_ts is not None else time.time() + config.ENTRY_TIMEOUT_SECONDS

        if not wait_for_fill:
            # Order placed — return now. The entry follower watches for the
            # fill, attaches TP/SL, or cancels at deadline, all in background.
            print(f"  [trader] LIMIT {side.upper()} {symbol} qty={qty} @ {entry_price} placed — "
                  f"not waiting for fill, continuing pipeline.")
            return entry_order

        rec["event"].wait(max(timeout_at - time.time(), 0.0))

        if not rec["filled"] and not rec["resolved"]:
            print(f"  [trader] LIMIT entry {symbol} still open at deadline — the entry follower "
                  f"keeps watching and will attach TP/SL if it fills, or cancel it.")
            return None
        return entry_order if rec["filled"] else None

    # ── Entry follower ──────────────────────────────────────────────────
    def _entry_follower_loop(self):
        """Daemon: watch every resting LIMIT entry until it fills or dies.
        Binance USD-M doesn't accept TP/SL on a limit order, so the STOP_MARKET
        / TAKE_PROFIT_MARKET pair is attached here the moment the fill shows up,
        and the position is tracked so it is never left unmanaged."""
        while True:
            try:
                with self._pending_lock:
                    recs = list(self._pending_entries.values())
                for rec in recs:
                    self._poll_pending_entry(rec)
            except Exception as e:
                print(f"  [trader] Entry follower error: {e}")
            time.sleep(config.ORDER_POLL_SECONDS)

    def _poll_pending_entry(self, rec):
        if rec.get("resolved"):
            return
        # Deadline reached: cancel the resting order once. A fill racing the
        # cancel is caught by the status poll right below — never abandoned blind.
        if rec["deadline_ts"] is not None and time.time() > rec["deadline_ts"] and not rec["cancel_attempted"]:
            rec["cancel_attempted"] = True
            try:
                self._guarded(self.exchange.cancel_order, rec["order_id"], rec["symbol"])
            except ratelimit.BinanceBanned:
                return  # never poke an active ban — retry next poll cycle
            except Exception as e:
                print(f"  [trader] Could not cancel unfilled LIMIT entry for {rec['symbol']}: {e}")

        try:
            order = self._guarded(self.exchange.fetch_order, rec["order_id"], rec["symbol"])
        except ratelimit.BinanceBanned:
            return  # never poke an active ban — retry next poll cycle
        except Exception as e:
            print(f"  [trader] Could not poll fill status for {rec['symbol']}: {e}")
            return

        status = order.get("status")
        if status == "closed":
            if not rec.get("filled"):
                rec["filled"] = True
                # Track the trade IMMEDIATELY on fill — before the TP/SL attach —
                # so the monitor loop can log it even if the attach retries or fails.
                rec["meta"] = {
                    **rec["levels"],
                    "qty": rec["qty"],
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                }
                self._open_trades[rec["symbol"]] = rec["meta"]
                rec["event"].set()          # release execute_trade's wait now
                print(f"  [trader] Filled {rec['direction'].upper()} {rec['symbol']} "
                      f"qty={rec['qty']} entry={rec['entry_price']}")
            if self._attach_tp_sl(rec):
                self._finalize_filled(rec)  # TP/SL on, trade fully opened
        elif status in ("canceled", "expired", "rejected"):
            self._finalize_failed(rec, status)

    def _attach_tp_sl(self, rec):
        if rec.get("attached"):
            return True
        close_side = "sell" if rec["direction"] == "long" else "buy"
        try:
            self._guarded(
                self.exchange.create_order, rec["symbol"], "STOP_MARKET", close_side, rec["qty"],
                params={"stopPrice": rec["sl"], "closePosition": True},
            )
            self._guarded(
                self.exchange.create_order, rec["symbol"], "TAKE_PROFIT_MARKET", close_side, rec["qty"],
                params={"stopPrice": rec["tp"], "closePosition": True},
            )
            rec["attached"] = True
            return True
        except ratelimit.BinanceBanned:
            return False  # never poke an active ban — follower retries on a later cycle
        except Exception as e:
            print(f"  [trader] TP/SL attach failed after LIMIT fill for {rec['symbol']}: {e}")
            self._log_trade_event({"event": "oco_failed", "error": str(e), **rec["levels"]})
            return False

    def _finalize_filled(self, rec):
        with self._pending_lock:
            if rec.get("resolved"):
                return
            rec["resolved"] = True
            self._pending_entries.pop(rec["order_id"], None)
        self._open_trades[rec["symbol"]] = rec["meta"]
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
    def _init_performance_csv(self):
        csv_path = Path(config.PERFORMANCE_CSV)
        # Ensure target directory exists before opening file
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        if not csv_path.exists():
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow([
                    "timestamp_utc", "symbol", "direction", "qty", "entry",
                    "sl", "tp", "mark_price", "unrealized_pnl", "status",
                ])

    def _log_performance_row(self, symbol, direction, qty, entry, sl, tp, mark_price, pnl, status):
        csv_path = Path(config.PERFORMANCE_CSV)
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow([
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                symbol, direction, qty, entry, sl, tp, mark_price, pnl, status,
            ])

    def _log_trade_event(self, record: dict):
        record["time"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        log_path = Path(config.TRADE_LOG_FILE)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")

    def monitor_loop(self):
        """Run forever: poll open positions, log PnL, detect closes.
        Supports multiple concurrent positions — each tracked trade is only
        declared closed after its symbol has been absent for
        POSITION_CLOSED_CONFIRM_POLLS consecutive polls. A position seen
        without any trade meta is logged best-effort so it is never
        invisible to the CSV."""
        print("  [trader] Monitor loop started.")
        absent_streaks: dict[str, int] = {}  # symbol -> consecutive absent polls
        while True:
            try:
                try:
                    positions = self.get_open_positions()
                except ratelimit.BinanceBanned as e:
                    print(f"  [trader] IP banned by Binance ({e.remaining:.0f}s) — monitor paused, no polling.")
                    time.sleep(min(e.remaining, 60.0))
                    continue

                open_symbols = set()
                for pos in positions:
                    symbol = (pos.get("symbol") or "").upper()
                    open_symbols.add(symbol)
                    contracts = float(
                        pos.get("contracts") or pos.get("info", {}).get("positionAmt", 0) or 0
                    )
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
                        # Position open but no tracked trade — log best-effort
                        qty = abs(contracts)
                        direction = "long" if contracts > 0 else "short"
                        self._log_performance_row(
                            symbol, direction, qty, float(pos.get("entryPrice") or 0),
                            "", "", float(pos.get("markPrice") or 0),
                            float(pos.get("unrealizedPnl") or 0), "open",
                        )

                # Check for closed positions (tracked but no longer on exchange)
                for symbol in list(self._open_trades.keys()):
                    if symbol not in open_symbols:
                        absent_streaks[symbol] = absent_streaks.get(symbol, 0) + 1
                        if absent_streaks[symbol] >= config.POSITION_CLOSED_CONFIRM_POLLS:
                            meta = self._open_trades.pop(symbol)
                            self._log_performance_row(
                                meta["symbol"], meta["direction"], meta["qty"], meta["entry"],
                                meta["sl"], meta["tp"], "", "", "closed",
                            )
                            self._log_trade_event({"event": "closed", **meta})
                            print(f"  [trader] Position closed: {meta['symbol']}")
                            absent_streaks.pop(symbol, None)

            except Exception as e:
                print(f"  [trader] Monitor loop error: {e}")

            time.sleep(config.POSITION_POLL_SECONDS)


if __name__ == "__main__":
    Trader().monitor_loop()