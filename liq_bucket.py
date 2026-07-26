"""
liq_bucket.py  —  Stage 2 of the pipeline
===========================================
Long-running, standalone process. Every config.BUCKET_POLL_SECONDS it:
  1. Re-reads config.LIQ_CSV (written by liq_stream.py)
  2. Buckets each symbol's events into config.BUCKET_MINUTES windows
  3. Advances a per-symbol consecutive-silent / consecutive-active counter
  4. Fires a signal the moment a streak breaks:
       - "activity_spike" on the bucket where activity resumes after
         >= config.SILENCE_THRESHOLD silent buckets
       - "collapse"       on the bucket where activity dies after
         >= config.ACTIVITY_THRESHOLD active buckets

Signals are appended as JSON lines to config.LIQ_SIGNALS_FILE, which
run_bot.py tails. Counter state is persisted to config.LIQ_BUCKET_STATE so a
restart doesn't lose streak history.

Usage:
    python liq_bucket.py
"""

import json
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

import config

BUCKET_DELTA = timedelta(minutes=config.BUCKET_MINUTES)


def load_state() -> dict:
    if config.LIQ_BUCKET_STATE.exists():
        try:
            return json.loads(config.LIQ_BUCKET_STATE.read_text())
        except Exception:
            pass
    return {}   # symbol -> {"last_bucket": iso_str, "silent": int, "active": int}


def save_state(state: dict):
    config.LIQ_BUCKET_STATE.write_text(json.dumps(state, indent=2))


def load_events() -> pd.DataFrame:
    if not config.LIQ_CSV.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(config.LIQ_CSV)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df

    if "price" not in df.columns and "avg_price" in df.columns:
        df["price"] = df["avg_price"]
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_utc", "symbol"])
    df["bucket"] = df["timestamp_utc"].dt.floor(f"{config.BUCKET_MINUTES}min")
    return df


def emit_signal(f_out, symbol: str, bucket_start: datetime, signal: str, count: int, prior_streak: int):
    record = {
        "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "bucket_start": bucket_start.isoformat(),
        "symbol": symbol,
        "signal": signal,
        "count_in_bucket": int(count),
        "prior_streak": int(prior_streak),
    }
    f_out.write(json.dumps(record) + "\n")
    f_out.flush()
    print(f"  [{record['time']}] {symbol:<12} {signal:<15} "
          f"(prior streak: {prior_streak}, this bucket had {count} liq{'s' if count != 1 else ''})")


def process_tick(df: pd.DataFrame, state: dict, f_out) -> dict:
    """Advance every symbol's bucket state up to (but not including) the
    currently-forming bucket, emitting signals for any streak break."""
    now_bucket = pd.Timestamp.now(tz="UTC").floor(f"{config.BUCKET_MINUTES}min")

    if df.empty:
        return state

    counts = df.groupby(["symbol", "bucket"]).size()
    symbols = sorted(df["symbol"].unique())

    for sym in symbols:
        sym_state = state.get(sym)
        first_bucket = df.loc[df["symbol"] == sym, "bucket"].min()

        if sym_state is None:
            # first time we've ever seen this symbol — start the clock at its
            # first bucket instead of retroactively treating history as silence
            cursor = first_bucket
            silent, active = 0, 0
        else:
            cursor = pd.Timestamp(sym_state["last_bucket"]) + BUCKET_DELTA
            silent, active = sym_state["silent"], sym_state["active"]

        while cursor < now_bucket:
            count = int(counts.get((sym, cursor), 0))
            is_active = count > 0
            signal = None

            if is_active:
                if silent >= config.SILENCE_THRESHOLD:
                    signal = "activity_spike"
                    emit_signal(f_out, sym, cursor, signal, count, silent)
                active += 1
                silent = 0
            else:
                if active >= config.ACTIVITY_THRESHOLD:
                    signal = "collapse"
                    emit_signal(f_out, sym, cursor, signal, count, active)
                silent += 1
                active = 0

            cursor += BUCKET_DELTA

        state[sym] = {
            "last_bucket": (cursor - BUCKET_DELTA).isoformat() if cursor > first_bucket else first_bucket.isoformat(),
            "silent": silent,
            "active": active,
        }

    return state


def main():
    print(f"\n  Liquidation Bucket Monitor")
    print(f"  Bucket width      : {config.BUCKET_MINUTES}min")
    print(f"  Silence threshold : {config.SILENCE_THRESHOLD} buckets -> activity_spike")
    print(f"  Activity threshold: {config.ACTIVITY_THRESHOLD} buckets -> collapse")
    print(f"  Reading           : {config.LIQ_CSV}")
    print(f"  Writing signals to: {config.LIQ_SIGNALS_FILE}\n")

    state = load_state()

    with open(config.LIQ_SIGNALS_FILE, "a") as f_out:
        while True:
            df = load_events()
            state = process_tick(df, state, f_out)
            save_state(state)
            time.sleep(config.BUCKET_POLL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n  Stopped.")
