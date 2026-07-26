"""
run_bot.py  —  Orchestrator (ties Stages 3, 4, 5 together)
=============================================================
Long-running process. Tails config.LIQ_SIGNALS_FILE (written by
liq_bucket.py). On every line whose "signal" matches config.TRIGGER_SIGNAL:

    strategy.decide_direction()  -> direction ('long'/'short'/None)
    anomaly.get_uncleared_zones() -> current zones for that symbol
    allocation.compute_levels()  -> sl / tp
    trader.execute_trade()       -> places the order (skips if a position
                                     is already open — enforced inside Trader)

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

import config
import strategy
import allocation
import anomaly
from trader import Trader


def handle_signal(trader: Trader, record: dict):
    symbol = record["symbol"]
    print(f"\n[run_bot] Trigger: {symbol} {record['signal']} (prior streak {record['prior_streak']})")

    if trader.has_open_position():
        print(f"[run_bot] Skipping {symbol} — a position is already open.")
        return

    try:
        direction, candles = strategy.decide_direction(symbol, end_time=record["bucket_start"])
    except Exception as e:
        print(f"[run_bot] strategy.decide_direction failed for {symbol}: {e}")
        return

    if direction is None:
        print(f"[run_bot] {symbol}: flat trend, no trade.")
        return

    if candles.empty:
        print(f"[run_bot] {symbol}: no candle closed at/before {record['bucket_start']} yet, skipping.")
        return

    # entry = the CLOSE of the exact silence/spike candle the signal fired on
    price = float(candles["close"].iloc[-1])

    try:
        zones = anomaly.get_uncleared_zones(symbol, chart_tf=config.ANOMALY_CHART_TF)
    except Exception as e:
        print(f"[run_bot] anomaly.get_uncleared_zones failed for {symbol}: {e}")
        zones = []

    levels = allocation.compute_levels(symbol, direction, price, candles, zones, candles)
    print(f"[run_bot] {symbol} {direction.upper()}  entry~{price}  "
          f"sl={levels['sl']} ({levels['sl_method']})  tp={levels['tp']} ({levels['tp_method']})  "
          f"R:R={levels['risk_reward']}")

    if levels["risk_reward"] is None or levels["risk_reward"] < config.MIN_RISK_REWARD:
        print(f"[run_bot] {symbol}: R:R {levels['risk_reward']} below MIN_RISK_REWARD "
              f"({config.MIN_RISK_REWARD}), skipping.")
        return

    trader.execute_trade(levels)


def tail_signals(trader: Trader):
    """Follow config.LIQ_SIGNALS_FILE from its current end, like `tail -f`."""
    config.LIQ_SIGNALS_FILE.touch(exist_ok=True)
    with open(config.LIQ_SIGNALS_FILE, "r") as f:
        f.seek(0, 2)   # jump to end — only react to NEW signals from here on
        while True:
            line = f.readline()
            if not line:
                time.sleep(1)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("signal") == config.TRIGGER_SIGNAL:
                handle_signal(trader, record)


def main():
    print("\n  Trading Bot Orchestrator")
    print(f"  Acting on signal : {config.TRIGGER_SIGNAL}")
    print(f"  Contrarian mode  : {config.CONTRARIAN_MODE}")
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
