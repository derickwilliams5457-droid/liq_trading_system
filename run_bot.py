"""
run_bot.py  —  Orchestrator (ties Stages 3, 4, 5 together)
=============================================================
Long-running process. Tails config.LIQ_SIGNALS_FILE (written by
liq_bucket.py). Handles two signal types:

     1. PRE_SPIKE_SIGNAL ("pre_activity_spike"):
     Emitted 15s BEFORE the interest candle closes. Starts pre-calculations
     (votes, SL/TP via Dynamic ATR on 6 candles) while the candle is still
     forming. Stores results in pending_pre_calcs. At candle close,
     _process_due_pendings fetches the actual entry price and re-derives
     SL/TP, then executes ALL qualifying coins with priority over any new
     signal.

  2. TRIGGER_SIGNAL ("activity_spike"):
     Fallback — emitted AFTER the candle closes (when liq_bucket.py misses
     the 15s pre-close window). Uses the original post-close flow.

Both flows use the same strategy/trader infrastructure. SL and TP are both
derived from the Dynamic ATR (~1:1 R:R by construction).

trader.monitor_loop() runs concurrently in a background thread, continuously
polling PnL into config.PERFORMANCE_CSV regardless of whether new signals
are arriving.

liq_stream.py and liq_bucket.py are SEPARATE processes — start them first.

Usage:
    python run_bot.py
"""

import json
import threading
import time
import traceback

import pandas as pd

import config
import data_cache
import ratelimit
import strategy
from trader import Trader

# The Bybit mirror is imported lazily in main() — if the package/creds are
# missing the bot degrades to Binance-only instead of crashing at import time.
from bybit_mirror import BybitMirror

# Pre-calculated trades awaiting candle close. Keyed by (symbol, close_time_iso):
# { "direction", "dynamic", "tp", "tp_method", "precision", "rect_high", "rect_low",
#   "zone_20", "zone_80", "atr", "close_time", "deadline", "t0" }
_pending_pre_calcs: dict = {}
_PENDING_LOCK = threading.Lock()


def _all_exchanges_full(trader: Trader, mirror) -> bool:
    """True iff EVERY enabled exchange is at its own concurrent-position cap.

    Binance and Bybit are independent: each has its own MAX_CONCURRENT_* and
    its own open-position count. A signal is only skipped here when NO exchange
    can take it — e.g. Binance at 3/3 but Bybit at 2/3 still proceeds so the
    mirror can open the trade on Bybit.

    Raises ratelimit.BinanceBanned if Binance capacity can't be read because
    the IP is banned — the caller sleeps the ban out (never poll into a ban).
    A Bybit capacity-read error is NOT fatal: Bybit is treated as available and
    its own execute_trade() will re-check before placing anything.
    """
    binance_full = trader is None or trader.has_open_position()
    if mirror is None:
        return binance_full
    try:
        bybit_full = mirror.has_open_position()
    except Exception as e:
        print(f"[run_bot] Bybit capacity check failed ({e}) — treating Bybit as available.")
        bybit_full = False
    return binance_full and bybit_full



def handle_pre_spike(trader: Trader, mirror, record: dict):
    """Handle a pre_activity_spike signal: pre-calculate SL/TP while the
    interest candle is still forming, store in pending_pre_calcs. At candle
    close, _process_due_pendings fetches the actual entry price, re-derives
    SL/TP from ATR, and executes ALL qualifying coins with priority over
    new signals."""
    symbol = record["symbol"]
    t0 = time.time()

    bucket_start = pd.Timestamp(record["bucket_start"])
    if bucket_start.tzinfo is None:
        bucket_start = bucket_start.tz_localize("UTC")

    candle_close_time = bucket_start + pd.Timedelta(minutes=config.BUCKET_MINUTES)
    deadline = candle_close_time.timestamp() + config.PRE_CALC_EXECUTE_SECONDS

    print(f"\n[run_bot] PRE-SPIKE: {symbol} {record['signal']} "
          f"(candle_closed={record.get('candle_closed', True)}, prior streak {record['prior_streak']})  "
          f"candle_close={candle_close_time}  budget={deadline - time.time():.1f}s")

    banned_rem = ratelimit.current_ban_remaining()
    if banned_rem > 0:
        print(f"[run_bot] {symbol}: Binance IP ban active ({banned_rem:.0f}s left) — "
              f"sleeping it out, no polling.")
        time.sleep(banned_rem)
        return

    try:
        if _all_exchanges_full(trader, mirror):
            print(f"[run_bot] Skipping {symbol} — all exchanges at max concurrent "
                  f"positions.  [pre-calc +{time.time() - t0:.1f}s]")
            return
    except ratelimit.BinanceBanned as e:
        print(f"[run_bot] {symbol}: Binance IP ban detected ({e.remaining:.0f}s left) — "
              f"sleeping it out.  [pre-calc +{time.time() - t0:.1f}s]")
        time.sleep(max(e.remaining, 0.0))
        return

    close_time_iso = candle_close_time.isoformat()
    with _PENDING_LOCK:
        if (symbol, close_time_iso) in _pending_pre_calcs:
            print(f"[run_bot] {symbol}: already pre-calculating for this close time, skipping.  "
                  f"[pre-calc +{time.time() - t0:.1f}s]")
            return

    try:
        try:
            candles = strategy.get_recent_candles(symbol, end_time=bucket_start - pd.Timedelta(milliseconds=1))
            direction, candles, levels = strategy.decide_direction(
                symbol, candles=candles, end_time=bucket_start - pd.Timedelta(milliseconds=1),
                pre_close=True,
            )
        except ratelimit.BinanceBanned as e:
            print(f"[run_bot] {symbol}: Binance IP banned ({e.remaining:.0f}s left) — "
                  f"abandoning pre-spike.  [pre-calc +{time.time() - t0:.1f}s]")
            time.sleep(max(e.remaining, 0.0))
            return
        except Exception as e:
            print(f"[run_bot] {symbol}: strategy failed ({e}), abandoning pre-spike.  "
                  f"[pre-calc +{time.time() - t0:.1f}s]")
            return

        if candles is None or candles.empty:
            print(f"[run_bot] {symbol}: no candle data, abandoning pre-spike.  "
                  f"[pre-calc +{time.time() - t0:.1f}s]")
            return

        if direction is None or levels is None:
            print(f"[run_bot] {symbol}: rejected at vote/rect stage, no trade.  "
                  f"[pre-calc +{time.time() - t0:.1f}s]")
            return

        t_vote = time.time()
        print(f"[run_bot] {symbol}: vote + rect done (pre_close)  [+{t_vote - t0:.1f}s]")

        dynamic = levels["dynamic"]
        tp_price = levels["tp"]

        print(f"[run_bot] {symbol}: PRE-CALC OK — direction={direction}  "
              f"dynamic={dynamic}  tp={tp_price}  "
              f"[+{time.time() - t0:.1f}s]")

        pre_calc = {
            "symbol": symbol,
            "direction": direction,
            "dynamic": dynamic,
            "tp": tp_price,
            "tp_method": "atr",
            "precision": 6,
            "rect_high": levels["rect_high"],
            "rect_low": levels["rect_low"],
            "zone_20": levels["zone_20"],
            "zone_80": levels["zone_80"],
            "atr": levels["atr"],
            "close_time": candle_close_time,
            "deadline": deadline,
            "t0": t0,
        }
        with _PENDING_LOCK:
            _pending_pre_calcs[(symbol, close_time_iso)] = pre_calc
            n_waiting = len(_pending_pre_calcs)

        secs_to_close = candle_close_time.timestamp() - time.time()
        print(f"[run_bot] {symbol} PENDING — pre-calc done, candle NOT closed yet  "
              f"(close at {candle_close_time}, {secs_to_close:.0f}s away)  "
              f"DIRECTION={direction}  SL(dynamic)={dynamic}  TP={tp_price}  "
              f"| {n_waiting} coin(s) pending close")

    except Exception as e:
        print(f"[run_bot] {symbol}: unexpected error in pre-spike ({e}).  "
              f"[pre-calc +{time.time() - t0:.1f}s]")


def _dispatch(levels: dict, deadline_ts: float | None, wait_for_fill: bool,
              trader: Trader, mirror, candle_open_ts: float | None = None):
    """Execute a finalized trade across every enabled exchange. When the
    Binance trader exists it dispatches internally to the attached mirror
    (Trader.dispatch_trade); when Binance is not configured the mirror is
    called directly. Every adapter re-checks its OWN capacity and symbol
    listing before touching its exchange."""
    if trader is not None:
        try:
            trader.dispatch_trade(levels, deadline_ts=deadline_ts,
                                  wait_for_fill=wait_for_fill,
                                  candle_open_ts=candle_open_ts)
        except Exception as e:
            print(f"[run_bot] {levels.get('symbol')}: trade execution failed ({e}), "
                  f"skipping.  [verdict]")
    elif mirror is not None:
        try:
            mirror.execute_trade(levels, deadline_ts=deadline_ts,
                                 wait_for_fill=wait_for_fill,
                                 candle_open_ts=candle_open_ts)
        except Exception as e:
            print(f"[run_bot] {levels.get('symbol')}: Bybit execution failed ({e}), "
                  f"skipping.  [verdict]")


def _process_due_pendings(trader: Trader, mirror):
    """Process EVERY pending pre-calc whose close time has arrived, inline and
    in priority order. Called at the top of the tail loop — so before any new
    signal for another coin is handled, all due pre-calculated coins are
    finished first (the new coin is held back until they're done).

    Runs at the exact second the next candle opens (no 2s buffer) and must
    take at most ~3s per coin: fetch close price -> recompute R:R -> place the
    limit order. execute_trade is called with wait_for_fill=False so a coin
    never blocks the pipeline waiting for a fill — the entry follower owns
    the fill/reject outcome in the background. After the order is placed,
    processing moves on to the next pending coin / signal.
    """
    now = time.time()
    ready = []

    with _PENDING_LOCK:
        for key, pre in list(_pending_pre_calcs.items()):
            close_ts = pre["close_time"].timestamp()
            # Allowlist gate (defense-in-depth): a stale pre-calc for a symbol
            # outside the allowlist is dropped, never executed.
            if not config.is_traded_symbol(pre["symbol"]):
                _pending_pre_calcs.pop(key, None)
                continue
            # Fetch the close price the exact second the candle opens.
            if now >= close_ts:
                ready.append((key, pre))

    for key, pre in ready:
        symbol = pre["symbol"]
        direction = pre["direction"]
        dynamic = pre["dynamic"]
        tp = pre["tp"]
        deadline = pre["deadline"]
        t0 = pre["t0"]
        precision = pre["precision"]

        # Already past deadline? Skip.
        if time.time() > deadline:
            print(f"[run_bot] {symbol}: pending close — deadline expired, skipping.")
            with _PENDING_LOCK:
                _pending_pre_calcs.pop(key, None)
            continue

        # Fetch actual entry price (interest candle close). If the candle is
        # not visible the very second it opens, one quick retry covers the
        # Binance finalization lag while staying inside the 3s budget.
        try:
            candles = strategy.get_recent_candles(
                symbol, end_time=pre["close_time"],
                hours=config.LOOKBACK_HOURS,
            )
            if candles.empty:
                time.sleep(0.5)
                candles = strategy.get_recent_candles(
                    symbol, end_time=pre["close_time"],
                    hours=config.LOOKBACK_HOURS,
                )
            if candles.empty:
                print(f"[run_bot] {symbol}: pending close — no candle data at close, skipping.")
                with _PENDING_LOCK:
                    _pending_pre_calcs.pop(key, None)
                continue
            entry = float(candles["close"].iloc[-1])
        except ratelimit.BinanceBanned as e:
            print(f"[run_bot] {symbol}: pending close — Binance banned ({e.remaining:.0f}s), skipping.")
            with _PENDING_LOCK:
                _pending_pre_calcs.pop(key, None)
            time.sleep(max(e.remaining, 0.0))
            continue
        except Exception as e:
            print(f"[run_bot] {symbol}: pending close — failed to fetch entry ({e}), skipping.")
            with _PENDING_LOCK:
                _pending_pre_calcs.pop(key, None)
            continue

        # Re-derive SL and TP from ATR with the actual entry price
        atr = pre["atr"]
        if direction == "short":
            sl = round(entry + atr, 6)
            tp = round(entry - atr, 6)
        else:
            sl = round(entry - atr, 6)
            tp = round(entry + atr, 6)

        levels = {
            "symbol": symbol,
            "direction": direction,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "sl_method": "localized_atr",
            "tp_method": "atr",
            "atr": atr,
            "dynamic": dynamic,
            "rect_high": pre["rect_high"],
            "rect_low": pre["rect_low"],
            "zone_20": pre["zone_20"],
            "zone_80": pre["zone_80"],
        }

        print(f"[run_bot] {symbol}: CLOSE — entry fetched={entry}  "
              f"SL={sl}  TP={tp}  "
              f"[verdict +{time.time() - t0:.1f}s]")

        # Execute trade — place the order on Binance AND mirror it to Bybit
        # (each exchange re-checks its own capacity/listing), then move on
        # immediately. Whether it fills or is rejected is owned by each
        # exchange's entry follower in the background.
        try:
            _dispatch(levels, deadline, wait_for_fill=False, trader=trader, mirror=mirror,
                      candle_open_ts=pre["close_time"].timestamp())
        except ratelimit.BinanceBanned as e:
            print(f"[run_bot] {symbol}: pending close — Binance ban during execution "
                  f"({e.remaining:.0f}s) — trade not placed.  "
                  f"[verdict +{time.time() - t0:.1f}s]")
            time.sleep(max(e.remaining, 0.0))
        except Exception as e:
            # Never let a single bad trade (bad symbol, bad precision,
            # etc.) kill the pipeline.
            print(f"[run_bot] {symbol}: pending close — trade execution failed ({e}), "
                  f"skipping.  [verdict +{time.time() - t0:.1f}s]")

        # Clean up
        with _PENDING_LOCK:
            _pending_pre_calcs.pop(key, None)

    if ready:
        with _PENDING_LOCK:
            n_left = len(_pending_pre_calcs)
        if n_left:
            waiting = ", ".join(sorted(f"{k[0]}" for k in _pending_pre_calcs))
            print(f"[run_bot] {len(ready)} coin(s) processed at close — "
                  f"{n_left} still waiting for their close: {waiting}")
        else:
            print(f"[run_bot] {len(ready)} coin(s) processed at close — "
                  f"nothing left waiting.")
        time.sleep(1)  # check every second


def handle_signal(trader: Trader, mirror, record: dict):
    symbol = record["symbol"]

    t0 = time.time()  # internal clock: detection -> final verdict

    bucket_start = pd.Timestamp(record["bucket_start"])
    if bucket_start.tzinfo is None:
        bucket_start = bucket_start.tz_localize("UTC")

    candle_close_time = bucket_start + pd.Timedelta(minutes=config.BUCKET_MINUTES)
    deadline = candle_close_time.timestamp() + config.ENTRY_TIMEOUT_SECONDS

    print(f"\n[run_bot] Trigger: {symbol} {record['signal']} (prior streak {record['prior_streak']})  "
          f"candle_close={candle_close_time}  budget={deadline - time.time():.1f}s")

    # If the IP is already banned, don't even start — sleep the ban out and
    # make NO requests. This is the guard that stops "polling into a ban":
    # churning through every queued signal while banned only re-arms the 418
    # and makes Binance extend the ban.
    banned_rem = ratelimit.current_ban_remaining()
    if banned_rem > 0:
        print(f"[run_bot] {symbol}: Binance IP ban active ({banned_rem:.0f}s left) — "
              f"sleeping it out, no polling.")
        time.sleep(banned_rem)
        return

    try:
        blocked = _all_exchanges_full(trader, mirror)
    except ratelimit.BinanceBanned as e:
        print(f"[run_bot] {symbol}: Binance IP ban detected ({e.remaining:.0f}s left) — "
              f"sleeping it out, no polling.")
        time.sleep(max(e.remaining, 0.0))
        return
    if blocked:
        print(f"[run_bot] Skipping {symbol} — every enabled exchange is at max "
              f"concurrent positions.  [verdict +{time.time() - t0:.1f}s]")
        return

    try:
        try:
            direction, candles, levels = strategy.decide_direction(symbol, end_time=candle_close_time)
        except ratelimit.BinanceBanned as e:
            print(f"[run_bot] {symbol}: Binance IP banned ({e.remaining:.0f}s left) — "
                  f"abandoning signal, sleeping the ban out.  [verdict +{time.time() - t0:.1f}s]")
            time.sleep(max(e.remaining, 0.0))
            return
        except Exception as e:
            print(f"[run_bot] {symbol}: strategy failed ({e}), abandoning.  [verdict +{time.time() - t0:.1f}s]")
            return

        if candles is None or candles.empty:
            print(f"[run_bot] {symbol}: no candle data, abandoning.  [verdict +{time.time() - t0:.1f}s]")
            return

        if direction is None or levels is None:
            print(f"[run_bot] {symbol}: rejected at vote/rect stage, no trade.  [verdict +{time.time() - t0:.1f}s]")
            return

        t_vote = time.time()
        print(f"[run_bot] {symbol}: vote + rect done  [+{t_vote - t0:.1f}s]")

        entry = levels["entry"]

        # Final combined levels print: entry / SL / TP in one line
        print(f"[run_bot] {symbol} LEVELS -> ENTRY={entry}  SL={levels['sl']}  "
              f"TP={levels['tp']}  "
              f"(SL: {levels['sl_method']}, TP: {levels['tp_method']})  "
              f"[verdict +{time.time() - t0:.1f}s]")

        if time.time() >= deadline:
            print(f"[run_bot] {symbol}: time budget expired, abandoning.  [verdict +{time.time() - t0:.1f}s]")
            return

        try:
            _dispatch(levels, deadline, wait_for_fill=True, trader=trader, mirror=mirror,
                      candle_open_ts=candle_close_time.timestamp())
        except ratelimit.BinanceBanned as e:
            print(f"[run_bot] {symbol}: Binance IP ban during execution "
                  f"({e.remaining:.0f}s left) — trade not placed, sleeping the ban out.  "
                  f"[verdict +{time.time() - t0:.1f}s]")
            time.sleep(max(e.remaining, 0.0))
        except Exception as e:
            print(f"[run_bot] {symbol}: trade execution failed ({e}), abandoning.  "
                  f"[verdict +{time.time() - t0:.1f}s]")
    finally:
        data_cache.evict_symbol(symbol)
        print(f"[run_bot] {symbol}: cache purged after verdict.")


def tail_signals(trader: Trader, mirror):
    """Follow config.LIQ_SIGNALS_FILE from its current end, like `tail -f`."""
    config.LIQ_SIGNALS_FILE.touch(exist_ok=True)
    last_heartbeat = time.time()
    seen_since_heartbeat = 0

    with open(config.LIQ_SIGNALS_FILE, "r") as f:
        f.seek(0, 2)   # jump to end — only react to NEW signals from here on
        while True:
            # PRIORITY: any pre-calculated coin whose candle has closed is
            # processed FIRST, before a new liquidation signal for another
            # coin is even looked at. Pending trades are never queued behind
            # new-coin work.
            try:
                _process_due_pendings(trader, mirror)
            except Exception as e:
                print(f"[run_bot] pending-processing error: {e}")
                traceback.print_exc()

            line = f.readline()
            if not line:
                if time.time() - last_heartbeat > 60:
                    print(f"[run_bot] heartbeat: still tailing, {seen_since_heartbeat} "
                          f"signal line(s) seen in the last minute")
                    last_heartbeat = time.time()
                    seen_since_heartbeat = 0
                time.sleep(1)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            seen_since_heartbeat += 1
            sig = record.get("signal")
            # Allowlist gate: never run strategy/metrics for a symbol outside
            # config.TRADED_SYMBOLS_FILE. This is defense-in-depth for stale
            # signals already in the file or a bucket process running the old
            # (pre-filter) code — the signal itself was dropped at the bucket
            # level, but we don't rely on that here.
            if not config.is_traded_symbol(record.get("symbol")):
                print(f"[run_bot] ignoring {record.get('symbol')} — not in allowlist")
                continue
            try:
                if sig == config.PRE_SPIKE_SIGNAL:
                    handle_pre_spike(trader, mirror, record)
                elif sig == config.TRIGGER_SIGNAL:
                    handle_signal(trader, mirror, record)
                else:
                    print(f"[run_bot] ignoring {record.get('symbol')} "
                          f"{sig} (unknown signal type)")
            except Exception as e:
                # THE safety net: no exception from any handler may ever crash
                # the tail loop. Log it and keep tailing — a single bad signal
                # must never stop the whole bot.
                print(f"[run_bot] ERROR processing {record.get('symbol')} {sig}: {e}")
                traceback.print_exc()


def main():
    print("\n  Trading Bot Orchestrator")
    print(f"  Pre-spike signal : {config.PRE_SPIKE_SIGNAL} ({config.PRE_CLOSE_SECONDS}s before close)")
    print(f"  Fallback signal  : {config.TRIGGER_SIGNAL} (post-close)")
    print(f"  Strategy         : rectangle (extreme zones 0-20% / 80-100%, {config.RECTANGLE_CANDLES} candles)")
    print(f"  SL ATR window    : {config.SL_ATR_WINDOW} candles (interest excluded)")
    print(f"  Max positions    : {config.MAX_CONCURRENT_POSITIONS} (per exchange)")
    print(f"  Binance testnet  : {config.TESTNET}")
    print(f"  Tailing          : {config.LIQ_SIGNALS_FILE}\n")

    trader = None
    try:
        trader = Trader()
    except Exception as e:
        print(f"  Binance trader unavailable ({e}) — will trade on Bybit only if its mirror is up.")

    mirror = None
    if config.BYBIT_ENABLED:
        try:
            mirror = BybitMirror()
        except Exception as e:
            print(f"  Bybit mirror unavailable ({e}) — continuing Binance-only.")

    if trader is not None:
        trader.mirror = mirror   # interconnect: Binance trades mirror to Bybit
        monitor_thread = threading.Thread(target=trader.monitor_loop, daemon=True)
        monitor_thread.start()
    if mirror is not None:
        mirror_thread = threading.Thread(target=mirror.monitor_loop, daemon=True)
        mirror_thread.start()

    if trader is None and mirror is None:
        raise RuntimeError("No exchange adapter is configured (need Binance and/or Bybit keys).")

    print(f"  Pending closes    : processed inline with priority before new signals "
          f"(max {config.PRE_CALC_EXECUTE_SECONDS}s window into the new candle)")

    tail_signals(trader, mirror)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n  Stopped.")