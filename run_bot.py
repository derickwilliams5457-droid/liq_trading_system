"""
run_bot.py  —  Orchestrator (ties Stages 3, 4, 5 together)
=============================================================
Long-running process. Tails config.LIQ_SIGNALS_FILE (written by
liq_bucket.py). Handles two signal types:

     1. PRE_SPIKE_SIGNAL ("pre_activity_spike"):
     Emitted 15s BEFORE the interest candle closes. Starts pre-calculations
     (votes, SL via Dynamic ATR on 5 candles, uncleared zones for TP) while
     the candle is still forming. Stores results in pending_pre_calcs.
     At candle close, _process_due_pendings fetches the actual entry price
     immediately (the exact second the next candle opens — no buffer),
     re-checks R:R, and executes ALL qualifying coins with priority over any
     new signal.

  2. TRIGGER_SIGNAL ("activity_spike"):
     Fallback — emitted AFTER the candle closes (when liq_bucket.py misses
     the 15s pre-close window). Uses the original post-close flow.

Both flows share hratmap zone fetches (warmed in background) and the same
strategy/trader infrastructure.

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
import hratmap
import ratelimit
import strategy
from trader import Trader

# The Bybit mirror is imported lazily in main() — if the package/creds are
# missing the bot degrades to Binance-only instead of crashing at import time.
from bybit_mirror import BybitMirror

_zone_fetches: dict = {}  # (symbol, end_time_ms) -> {"thread", "symbol", "end_time", "tag", "result"}
_ZONE_FETCH_LOCK = threading.Lock()

# Pre-calculated trades awaiting candle close. Keyed by (symbol, close_time_iso):
# { "direction", "dynamic", "tp", "tp_method", "precision", "rect_high", "rect_low",
#   "zone_20", "zone_80", "atr", "mad", "direction", "close_time", "deadline", "t0",
#   "tp_zone" }
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


def _ensure_zone_fetch(symbol: str, end_time_ms: int, force: bool = True) -> dict | None:
    """Return a FULL zone computation for (symbol, end_time_ms), started only
    if needed. The ENTIRE hratmap job — klines + precision + aggTrades fetch +
    ray calculation — runs in this one background thread, so by the time the
    strategy voting finishes the uncleared zones are already done. An identical
    job (running or already completed) is reused — hratmap is never run twice
    for the same data. `force=False` (the warmer) skips when the
    concurrent-fetch cap is reached; the trigger path always forces.
    """
    key = (symbol, end_time_ms)
    with _ZONE_FETCH_LOCK:
        now_ms = int(time.time() * 1000)
        stale = [k for k, v in _zone_fetches.items() if v["end_time"] < now_ms - 45 * 60 * 1000]
        for k in stale:
            del _zone_fetches[k]

        existing = _zone_fetches.get(key)
        if existing is not None:
            # Reuse a warm/recent identical computation — hratmap runs once
            # per (symbol, candle-close); the tag identifies the cached data.
            print(f"[run_bot] zones tag={existing['tag']} reuse for {symbol} "
                  f"({'still running' if existing['thread'].is_alive() else 'already done'})")
            return existing  # reuse (still running, or already computed)

        running = [v for v in _zone_fetches.values() if v["thread"].is_alive()]
        if not force and len(running) >= config.MAX_CONCURRENT_ZONE_FETCHES:
            return None

        result: dict = {}

        def fetch_zones():
            try:
                zones, precision = hratmap.get_uncleared_zones(
                    symbol, end_time=end_time_ms,
                    lookback=config.HRATMAP_LOOKBACK_CANDLES,
                    slice_minutes=config.SLICE_MINUTES, max_workers=config.MAX_WORKERS,
                )
                result["zones"] = zones
                result["precision"] = precision
            except ratelimit.BinanceBanned as e:
                result["error"] = e
            except Exception as e:
                result["error"] = e

        thread = threading.Thread(target=fetch_zones, daemon=True)
        thread.start()
        fetch = {
            "thread": thread, "symbol": symbol, "end_time": end_time_ms,
            "tag": f"zones:hratmap:{symbol}", "result": result,
        }
        _zone_fetches[key] = fetch
        return fetch


def _warm_loop():
    """Speculative precompute: a symbol that is SILENCE_THRESHOLD+ silent
    buckets deep is one active bucket away from firing activity_spike. Keep its
    3m zone window warm in the background so the slow aggTrades pull is already
    done when the trigger lands and _ensure_zone_fetch() reuses it — the
    verdict then no longer waits on a cold fetch."""
    while True:
        try:
            # Never keep precomputing into an active ban — the fetch threads
            # would only fail-fast anyway, so just pause until it clears.
            if ratelimit.current_ban_remaining() > 0:
                print("[run_bot] warmer paused while Binance IP ban is active.")
                time.sleep(15)
                continue

            state = {}
            if config.LIQ_BUCKET_STATE.exists():
                state = json.loads(config.LIQ_BUCKET_STATE.read_text())

            now_bucket = pd.Timestamp.now(tz="UTC").floor(f"{config.BUCKET_MINUTES}min")
            end_ms = int(now_bucket.timestamp() * 1000)

            on_deck = sorted(
                ((sym, st.get("silent", 0)) for sym, st in state.items()
                 if config.is_traded_symbol(sym)
                 and st.get("silent", 0) >= config.SILENCE_THRESHOLD),
                key=lambda kv: -kv[1],
            )[:config.WARM_SYMBOLS]

            for sym, _streak in on_deck:
                _ensure_zone_fetch(sym, end_ms, force=False)
        except Exception:
            pass
        time.sleep(config.WARM_POLL_SECONDS)


def handle_pre_spike(trader: Trader, mirror, record: dict):
    """Handle a pre_activity_spike signal: pre-calculate SL/TP while the
    interest candle is still forming, store in pending_pre_calcs. R:R is NOT
    gated here — the entry price isn't known until close, so the only
    pre-close requirement is that a TP zone exists inside the rect. The tail
    loop's _process_due_pendings drains these at the exact second each candle
    closes — priority over new signals — fetches the actual entry price,
    computes R:R against the real SL/TP, and executes ALL qualifying coins
    (not just the best one)."""
    symbol = record["symbol"]
    t0 = time.time()

    bucket_start = pd.Timestamp(record["bucket_start"])
    if bucket_start.tzinfo is None:
        bucket_start = bucket_start.tz_localize("UTC")

    candle_close_time = bucket_start + pd.Timedelta(minutes=config.BUCKET_MINUTES)
    # Pre-calculated coins get a tight window: everything (votes, SL, TP) is
    # already done before close. At close we only recompute R:R with the actual
    # close price and fill the limit order — that must finish within
    # PRE_CALC_EXECUTE_SECONDS into the new candle, then execute IMMEDIATELY.
    # No waiting for the 60s fallback budget.
    deadline = candle_close_time.timestamp() + config.PRE_CALC_EXECUTE_SECONDS
    close_time_ms = int(candle_close_time.timestamp() * 1000)
    # For pre-close, exclude the forming interest candle: end at bucket_start - 1ms
    pre_close_end_ms = int(bucket_start.timestamp() * 1000) - 1

    print(f"\n[run_bot] PRE-SPIKE: {symbol} {record['signal']} "
          f"(candle_closed={record.get('candle_closed', True)}, prior streak {record['prior_streak']})  "
          f"candle_close={candle_close_time}  budget={deadline - time.time():.1f}s")

    # Ban check
    banned_rem = ratelimit.current_ban_remaining()
    if banned_rem > 0:
        print(f"[run_bot] {symbol}: Binance IP ban active ({banned_rem:.0f}s left) — "
              f"sleeping it out, no polling.")
        time.sleep(banned_rem)
        return

    # Position check — skip only if EVERY enabled exchange is at max concurrent
    # positions. Binance full + Bybit having slots still proceeds, so the
    # mirror can take the trade on Bybit.
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

    # Check if already pre-calculating for this symbol+close_time
    close_time_iso = candle_close_time.isoformat()
    with _PENDING_LOCK:
        if (symbol, close_time_iso) in _pending_pre_calcs:
            print(f"[run_bot] {symbol}: already pre-calculating for this close time, skipping.  "
                  f"[pre-calc +{time.time() - t0:.1f}s]")
            return

    try:
        # ── Strategy voting with pre_close mode (excludes forming interest candle) ──
        try:
            # Fetch candles up to the previous completed candle (exclude forming interest)
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

        # hratmap is EXPENSIVE (aggTrades pull + ray calc) — only call it for
        # coins that passed the 3-vote system. A coin rejected at the vote/rect
        # stage above never touches hratmap.
        zone_fetch = _ensure_zone_fetch(symbol, pre_close_end_ms)

        # ── Wait for hratmap zones ──
        zone_fetch["thread"].join(timeout=max(deadline - time.time(), 0.0))
        if zone_fetch["thread"].is_alive():
            print(f"[run_bot] {symbol}: time budget expired waiting for hratmap, "
                  f"abandoning pre-spike.  [pre-calc +{time.time() - t0:.1f}s]")
            return

        zone_result = zone_fetch["result"]
        if "error" in zone_result:
            err = zone_result["error"]
            if isinstance(err, ratelimit.BinanceBanned):
                print(f"[run_bot] {symbol}: hratmap zone fetch banned ({err.remaining:.0f}s left) — "
                      f"abandoning.  [pre-calc +{time.time() - t0:.1f}s]")
                time.sleep(max(err.remaining, 0.0))
            else:
                print(f"[run_bot] {symbol}: hratmap zone computation failed ({err}), "
                      f"abandoning.  [pre-calc +{time.time() - t0:.1f}s]")
            return

        zones, precision = zone_result["zones"], zone_result["precision"]
        print(f"[run_bot] {symbol}: {len(zones)} uncleared zone(s) ready (pre_close)  "
              f"[+{time.time() - t0:.1f}s]  (zones awaited {time.time() - t_vote:.1f}s)")

        # ── Identify best TP zone (using rectangle bounds from pre-calc) ──
        rect_low, rect_high = levels["rect_low"], levels["rect_high"]
        dynamic = levels["dynamic"]

        # At pre-close time, we don't know the exact entry yet. Use the last
        # completed candle's close as an estimate for zone filtering.
        est_entry = float(candles["close"].iloc[-1])

        tp_zone = next(
            (z for z in zones
             if (direction == "long" and est_entry < z["level_price"] <= rect_high)
             or (direction == "short" and rect_low <= z["level_price"] < est_entry)),
            None,
        )

        if tp_zone is None:
            print(f"[run_bot] {symbol}: no uncleared zone inside rect "
                  f"[{rect_low:.6f}, {rect_high:.6f}] on the {direction} side, "
                  f"aborting pre-spike.  [pre-calc +{time.time() - t0:.1f}s]")
            return

        tp_price = round(tp_zone["level_price"], precision)

        # R:R is NOT checked here anymore — the estimated entry is unreliable
        # (the price keeps moving until close). We only pin the TP zone and SL
        # distance now; the real R:R gate runs at close in _process_due_pendings
        # with the actual close price as entry.

        print(f"[run_bot] {symbol}: PRE-CALC OK — direction={direction}  "
              f"dynamic={dynamic}  tp={tp_price}  "
              f"[+{time.time() - t0:.1f}s]")

        # ── Store in pending dict for _process_due_pendings to pick up at close ──
        pre_calc = {
            "symbol": symbol,
            "direction": direction,
            "dynamic": dynamic,
            "tp": tp_price,
            "tp_method": f"hratmap_{tp_zone['side']}_max_notional",
            "tp_zone": tp_zone,
            "precision": precision,
            "rect_high": rect_high,
            "rect_low": rect_low,
            "zone_20": levels["zone_20"],
            "zone_80": levels["zone_80"],
            "atr": levels["atr"],
            "mad": levels["mad"],
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
    finally:
        data_cache.evict_symbol(symbol)
        with _ZONE_FETCH_LOCK:
            _zone_fetches.pop((symbol, pre_close_end_ms), None)


def _dispatch(levels: dict, deadline_ts: float | None, wait_for_fill: bool,
              trader: Trader, mirror):
    """Execute a finalized trade across every enabled exchange. When the
    Binance trader exists it dispatches internally to the attached mirror
    (Trader.dispatch_trade); when Binance is not configured the mirror is
    called directly. Every adapter re-checks its OWN capacity and symbol
    listing before touching its exchange."""
    if trader is not None:
        try:
            trader.dispatch_trade(levels, deadline_ts=deadline_ts, wait_for_fill=wait_for_fill)
        except Exception as e:
            print(f"[run_bot] {levels.get('symbol')}: trade execution failed ({e}), "
                  f"skipping.  [verdict]")
    elif mirror is not None:
        try:
            mirror.execute_trade(levels, deadline_ts=deadline_ts, wait_for_fill=wait_for_fill)
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

        # Compute SL with actual entry
        sl = strategy._compute_entry_sl(entry, direction, dynamic)

        # Re-check R:R with actual entry
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        risk_reward = round(reward / risk, 2) if risk > 0 else 0.0

        if risk_reward < config.MIN_RR:
            print(f"[run_bot] {symbol}: pending close — R:R {risk_reward} < MIN_RR "
                  f"({config.MIN_RR}) at actual entry {entry}, skipping.  "
                  f"[verdict +{time.time() - t0:.1f}s]")
            with _PENDING_LOCK:
                _pending_pre_calcs.pop(key, None)
            continue

        # Build levels dict for execute_trade
        levels = {
            "symbol": symbol,
            "direction": direction,
            "entry": entry,
            "sl": sl,
            "sl_method": "localized_atr",
            "tp": tp,
            "tp_method": pre["tp_method"],
            "risk_reward": risk_reward,
            "atr": pre["atr"],
            "mad": pre["mad"],
            "dynamic": dynamic,
            "rect_high": pre["rect_high"],
            "rect_low": pre["rect_low"],
            "zone_20": pre["zone_20"],
            "zone_80": pre["zone_80"],
        }

        print(f"[run_bot] {symbol}: CLOSE — entry fetched={entry}  "
              f"SL={sl}  TP={tp}  R:R={risk_reward}  "
              f"[verdict +{time.time() - t0:.1f}s]")

        # Execute trade — place the order on Binance AND mirror it to Bybit
        # (each exchange re-checks its own capacity/listing), then move on
        # immediately. Whether it fills or is rejected is owned by each
        # exchange's entry follower in the background.
        try:
            _dispatch(levels, deadline, wait_for_fill=False, trader=trader, mirror=mirror)
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
    end_time_ms = candle_close_time.timestamp() * 1000

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

    # ── Kick off the hratmap aggTrades pull in the BACKGROUND now, so the
    #    slow part runs WHILE the 3-vote rectangle decision is being made
    #    below instead of serially after it. A warmed/recent identical pull
    #    (speculative precompute or a rejected prior signal) is reused — 
    #    hratmap is never called twice for the same data. ────────────────
    zone_fetch = _ensure_zone_fetch(symbol, end_time_ms)

    try:
        # ── Single-shot: vote → rectangle filter → R:R system ──────────────
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

        # ── Wait for the parallel hratmap job (fetch + calculate) ───────────
        # The background thread already computed the zones WHILE voting ran, so
        # this join should return almost immediately.
        zone_fetch["thread"].join(timeout=max(deadline - time.time(), 0.0))
        if zone_fetch["thread"].is_alive():
            print(f"[run_bot] {symbol}: time budget expired while waiting for hratmap, "
                  f"abandoning.  [verdict +{time.time() - t0:.1f}s]")
            return

        zone_result = zone_fetch["result"]
        if "error" in zone_result:
            err = zone_result["error"]
            if isinstance(err, ratelimit.BinanceBanned):
                print(f"[run_bot] {symbol}: hratmap zone fetch banned ({err.remaining:.0f}s left) — "
                      f"abandoning, sleeping the ban out.  [verdict +{time.time() - t0:.1f}s]")
                time.sleep(max(err.remaining, 0.0))
            else:
                print(f"[run_bot] {symbol}: hratmap zone computation failed ({err}), "
                      f"abandoning.  [verdict +{time.time() - t0:.1f}s]")
            return

        zones, precision = zone_result["zones"], zone_result["precision"]
        print(f"[run_bot] {symbol}: {len(zones)} uncleared zone(s) ready  [+{time.time() - t0:.1f}s]  "
              f"(zones awaited {time.time() - t_vote:.1f}s)")

        entry = levels["entry"]
        rect_low, rect_high = levels["rect_low"], levels["rect_high"]

        # zones come back sorted by notional (largest first) — the first one
        # inside the rect on the trade side is the TP.
        tp_zone = next(
            (z for z in zones
             if (direction == "long" and entry < z["level_price"] <= rect_high)
             or (direction == "short" and rect_low <= z["level_price"] < entry)),
            None,
        )

        if tp_zone is None:
            print(f"[run_bot] {symbol}: no uncleared zone inside rect "
                  f"[{rect_low:.6f}, {rect_high:.6f}] on the {direction} side, "
                  f"aborting trade.  [verdict +{time.time() - t0:.1f}s]")
            return

        levels["tp"] = round(tp_zone["level_price"], precision)
        levels["tp_method"] = f"hratmap_{tp_zone['side']}_max_notional"

        risk = abs(entry - levels["sl"])
        reward = abs(levels["tp"] - entry)
        levels["risk_reward"] = round(reward / risk, 2) if risk > 0 else 0.0

        if levels["risk_reward"] < config.MIN_RR:
            print(f"[run_bot] {symbol}: R:R {levels['risk_reward']} < MIN_RR "
                  f"({config.MIN_RR}), aborting.  [verdict +{time.time() - t0:.1f}s]")
            return

        print(f"[run_bot] {symbol}: TP={levels['tp']} from uncleared "
              f"{tp_zone['side']}x{tp_zone['leverage']} zone notional="
              f"{tp_zone['notional']:,.0f} USDT  R:R={levels['risk_reward']}")

        # ── Final combined levels print: entry / SL / TP in one line ────────
        print(f"[run_bot] {symbol} LEVELS -> ENTRY={entry}  SL={levels['sl']}  "
              f"TP={levels['tp']}  R:R={levels['risk_reward']}  "
              f"(SL: {levels['sl_method']}, TP: {levels['tp_method']})  "
              f"[verdict +{time.time() - t0:.1f}s]")

        if time.time() >= deadline:
            print(f"[run_bot] {symbol}: time budget expired, abandoning.  [verdict +{time.time() - t0:.1f}s]")
            return

        try:
            _dispatch(levels, deadline, wait_for_fill=True, trader=trader, mirror=mirror)
        except ratelimit.BinanceBanned as e:
            print(f"[run_bot] {symbol}: Binance IP ban during execution "
                  f"({e.remaining:.0f}s left) — trade not placed, sleeping the ban out.  "
                  f"[verdict +{time.time() - t0:.1f}s]")
            time.sleep(max(e.remaining, 0.0))
        except Exception as e:
            # Never let a single bad trade (bad symbol, bad precision, etc.)
            # crash the tail loop — the bot must keep processing signals.
            print(f"[run_bot] {symbol}: trade execution failed ({e}), abandoning.  "
                  f"[verdict +{time.time() - t0:.1f}s]")
    finally:
        # Final verdict reached (any outcome): purge this symbol's shared
        # cached data + its zone fetch so the cache never clogs up with stale
        # candles/zones from processed signals.
        data_cache.evict_symbol(symbol)
        with _ZONE_FETCH_LOCK:
            _zone_fetches.pop((symbol, end_time_ms), None)
        print(f"[run_bot] {symbol}: cache purged after verdict [tagged entries evicted].")


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

    warm_thread = threading.Thread(target=_warm_loop, daemon=True)
    warm_thread.start()
    print(f"  Warming {config.WARM_SYMBOLS} on-deck symbol(s) every "
          f"{config.WARM_POLL_SECONDS}s (speculative aggTrades precompute)")

    # Pending pre-calcs are processed INLINE in the tail loop, at the top of
    # every iteration — at the exact second each coin's candle opens and with
    # priority over any new signal. No separate thread needed.
    print(f"  Pending closes    : processed inline with priority before new signals "
          f"(max {config.PRE_CALC_EXECUTE_SECONDS}s window into the new candle)")

    tail_signals(trader, mirror)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n  Stopped.")