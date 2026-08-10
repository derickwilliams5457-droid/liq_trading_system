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
        self._follower_thread = threading.Thread(target=self._entry_follower_loop, daemon=True)
        self._follower_thread.start()

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
                return 0.0
        except ratelimit.BinanceBanned:
            raise
        except Exception as e:
            print(f"  [trader] Could not check margin for {symbol}: {e}")

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
            print(f"  [trader] Skipping {levels['symbol']} — "
                  f"{self.open_position_count()} open + {self.pending_entry_count()} pending "
                  f"= {self.open_capacity_used()} slot(s) used, max {config.MAX_CONCURRENT_POSITIONS}.")
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
            "cancel_at": time.time() + config.LIMIT_FILL_WINDOW_SECONDS,
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
            return entry_order

        rec["event"].wait(max(timeout_at - time.time(), 0.0))

        if not rec["filled"] and not rec["resolved"]:
            print(f"  [trader] LIMIT entry {symbol} still open at deadline — the entry follower "
                  f"keeps watching and will attach TP/SL if it fills, or cancel it.")
            return None
        return entry_order if rec["filled"] else None

    # ── Multi-exchange dispatch ──────────────────────────────────────────
    def dispatch_trade(self, levels: dict, deadline_ts: float | None = None,
                       wait_for_fill: bool = True):
        """Execute `levels` on Binance, then mirror it to the attached Bybit
        mirror (if any). The mirror runs under its own try/except and its own
        capacity/listing checks, so a Bybit failure NEVER cancels or alters the
        Binance trade — the worst case is the Binance trade proceeds alone."""
        result = self.execute_trade(levels, deadline_ts=deadline_ts, wait_for_fill=wait_for_fill)

        mirror = getattr(self, "mirror", None)
        if mirror is not None:
            try:
                mirror.execute_trade(levels, deadline_ts=deadline_ts, wait_for_fill=wait_for_fill)
            except Exception as e:
                print(f"  [trader] Bybit mirror failed for {levels.get('symbol')}: {e}")
        return result

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
        # Fill window expired (LIMIT_FILL_WINDOW_SECONDS after placement): cancel
        # the resting order once. A fill racing the cancel is caught by the status
        # poll right below — never abandoned blind.
        if time.time() > rec["cancel_at"] and not rec["cancel_attempted"]:
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
            rec["poll_failures"] = rec.get("poll_failures", 0) + 1
            # If polling keeps failing after the fill window expired, the order
            # id is effectively unreachable — abandon instead of reprinting the
            # same error every poll forever.
            if rec["poll_failures"] >= config.MAX_POLL_FAILURES and time.time() > rec["cancel_at"]:
                print(f"  [trader] Could not poll fill status for {rec['symbol']} after "
                      f"{rec['poll_failures']} attempts ({type(e).__name__}); fill window "
                      f"expired — abandoning.")
                self._finalize_failed(rec, "unresolvable")
            else:
                print(f"  [trader] Could not poll fill status for {rec['symbol']}: {e}")
            return

        status = order.get("status")
        filled_qty = float(order.get("filled") or 0)
        if status == "closed" or filled_qty > 0:
            # 'closed' = fully filled. 'filled_qty > 0' = the cancel raced a fill
            # (or the order partially filled before the rest was canceled): the
            # exchange is ALREADY holding a position, so it MUST be tracked and
            # managed — never orphaned. Treating every 'canceled' as 'never
            # filled' is exactly how an untracked, TP/SL-less position (a
            # "ghost") ends up live on the account.
            if not rec.get("filled"):
                rec["filled"] = True
                # Track the trade IMMEDIATELY on fill — before the TP/SL attach —
                # so the monitor loop can log it even if the attach retries or fails.
                rec["meta"] = {
                    **rec["levels"],
                    "qty": filled_qty if filled_qty > 0 else rec["qty"],
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                }
                self._open_trades[rec["symbol"]] = rec["meta"]
                rec["event"].set()          # release execute_trade's wait now
                print(f"  [trader] Filled {rec['direction'].upper()} {rec['symbol']} "
                      f"qty={rec['meta']['qty']} entry={rec['entry_price']}"
                      f" (status={status}, filled={filled_qty})")
            result = self._attach_tp_sl(rec)
            if result is True:
                self._finalize_filled(rec)  # TP/SL on, trade fully opened
            elif result == "abandon":
                # Permanent attach failure (no position behind the fill, or the
                # retry cap ran out) — stop retrying. The trade stays tracked in
                # _open_trades so the monitor still closes it out when the
                # exchange-side position disappears; it just never gets TP/SL.
                self._finalize_failed(rec, "attach_abandoned")
        elif status in ("canceled", "expired", "rejected"):
            self._finalize_failed(rec, status)

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
        before a restart, or never tracked). Build best-effort meta and adopt
        it so it gets a real close path instead of a forever-"open" ghost row."""
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

    def monitor_loop(self):
        """Run forever: poll open positions, log PnL, detect closes, snapshot."""
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
                if changed:
                    perfio.update_ledger(self.EXCHANGE_ID, self._open_trades)

                perfio.update_snapshot(self.EXCHANGE_ID, positions_dict, account)

            except Exception as e:
                print(f"  [trader] Monitor loop error: {e}")

            time.sleep(config.POSITION_POLL_SECONDS)


if __name__ == "__main__":
    Trader().monitor_loop()