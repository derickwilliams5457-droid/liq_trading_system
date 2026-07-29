"""
run_bot.py  —  Orchestrator (ties Stages 3, 4, 5 together)
=============================================================
Long-running process. Tails config.LIQ_SIGNALS_FILE (written by
liq_bucket.py). On every line whose "signal" matches config.TRIGGER_SIGNAL:

    T = the interest (silence/spike) candle's CLOSE time
        (bucket_start + BUCKET_MINUTES — bucket_start is the candle's OPEN)
    deadline = T + config.ENTRY_TIMEOUT_SECONDS  (default 30s total budget)

    Within that budget:
      strategy.decide_direction()   -> direction ('long'/'short'/None),
                                        retried on fetch failure
      anomaly.get_uncleared_zones() -> current zones for that symbol
      allocation.compute_levels()   -> sl / tp (entry is STRICTLY that
                                        candle's close — never re-fetched)
      trader.execute_trade()        -> places a LIMIT order at that exact
                                        close price, polls for a fill until
                                        `deadline`, cancels and abandons the
                                        trade if it never fills

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

import pandas as pd

import config
import strategy
from trader import Trader


def handle_signal(trader: Trader, record: dict):
    symbol = record["symbol"]

    bucket_start = pd.Timestamp(record["bucket_start"])
    if bucket_start.tzinfo is None:
        bucket_start = bucket_start.tz_localize("UTC")

    candle_close_time = bucket_start + pd.Timedelta(minutes=config.BUCKET_MINUTES)
    deadline = candle_close_time.timestamp() + config.ENTRY_TIMEOUT_SECONDS

    print(f"\n[run_bot] Trigger: {symbol} {record['signal']} (prior streak {record['prior_streak']})  "
          f"candle_close={candle_close_time}  budget={deadline - time.time():.1f}s")

    if trader.has_open_position():
        print(f"[run_bot] Skipping {symbol} — a position is already open.")
        return

    # ── Single-shot: vote → rectangle filter → R:R system ──────────────
    try:
        direction, candles, levels = strategy.decide_direction(symbol, end_time=candle_close_time)
    except Exception as e:
        print(f"[run_bot] {symbol}: strategy failed ({e}), abandoning.")
        return

    if candles is None or candles.empty:
        print(f"[run_bot] {symbol}: no candle data, abandoning.")
        return

    if direction is None or levels is None:
        print(f"[run_bot] {symbol}: rejected at vote/rect/RR stage, no trade.")
        return

    if time.time() >= deadline:
        print(f"[run_bot] {symbol}: time budget expired, abandoning.")
        return

    trader.execute_trade(levels, deadline_ts=deadline)


def tail_signals(trader: Trader):
    """Follow config.LIQ_SIGNALS_FILE from its current end, like `tail -f`."""
    config.LIQ_SIGNALS_FILE.touch(exist_ok=True)
    last_heartbeat = time.time()
    seen_since_heartbeat = 0

    with open(config.LIQ_SIGNALS_FILE, "r") as f:
        f.seek(0, 2)   # jump to end — only react to NEW signals from here on
        while True:
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
            if record.get("signal") == config.TRIGGER_SIGNAL:
                handle_signal(trader, record)
            else:
                print(f"[run_bot] ignoring {record.get('symbol')} "
                      f"{record.get('signal')} (not the trigger signal: {config.TRIGGER_SIGNAL})")


def main():
    print("\n  Trading Bot Orchestrator")
    print(f"  Acting on signal : {config.TRIGGER_SIGNAL}")
    print(f"  Strategy         : rectangle (extreme zones 0-20% / 80-100%)")
    print(f"  Testnet          : {config.TESTNET}")
    print(f"  Tailing          : {config.LIQ_SIGNALS_FILE}\n")

    trader = Trader()

    monitor_thread = threading.Thread(target=trader.monitor_loop, daemon=True)
    monitor_thread.start()

    tail_signals(trader)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n  Stopped.")