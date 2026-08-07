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

CSV_FIELDS = [
    "timestamp_utc", "event_time_ms", "symbol", "side", "side_label",
    "qty", "avg_price", "usd_value", "delta_vs_last_pct",
]

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

    if "timestamp_utc" not in df.columns:
        try:
            df = pd.read_csv(config.LIQ_CSV, names=CSV_FIELDS, header=None)
        except Exception:
            return pd.DataFrame()

    if "price" not in df.columns and "avg_price" in df.columns:
        df["price"] = df["avg_price"]
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_utc", "symbol"])
    df["bucket"] = df["timestamp_utc"].dt.floor(f"{config.BUCKET_MINUTES}min")
    return df


def emit_signal(f_out, symbol: str, bucket_start: datetime, signal: str, count: int,
                prior_streak: int, usd, max_single_usd, candle_closed: bool = True):
    """Append a signal line. `candle_closed` tells run_bot whether the bucket
    that triggered this signal has already finished closing:
      True  -> activity_spike / collapse (completed bucket, post-close flow)
      False -> pre_activity_spike (bucket still forming, run_bot pre-calculates
               then waits for the close price before executing)
    """
    record = {
        "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "bucket_start": bucket_start.isoformat(),
        "symbol": symbol,
        "signal": signal,
        "count_in_bucket": int(count),
        "usd_in_bucket": round(float(usd), 2),
        "max_single_usd": round(float(max_single_usd), 2),
        "prior_streak": int(prior_streak),
        "candle_closed": candle_closed,
    }
    f_out.write(json.dumps(record) + "\n")
    f_out.flush()
    print(f"  [{record['time']}] {symbol:<12} {signal:<18} "
          f"(prior streak: {prior_streak}, this bucket had {count} liq{'s' if count != 1 else ''}, "
          f"largest single ${max_single_usd:,.2f}, candle_closed={candle_closed})")


def process_tick(df: pd.DataFrame, state: dict, f_out) -> dict:
    """Advance every symbol's bucket state up to (but not including) the
    currently-forming bucket, emitting signals for any streak break.

    Phase 1 — completed buckets: emit activity_spike / collapse with
    candle_closed=True (post-close fallback flow in run_bot.py).

    Phase 2 — current (still-forming) bucket: emit PRE_SPIKE_SIGNAL as soon as
    a qualifying liquidation (>= MIN_ACTIVITY_USD single event) is observed for
    a symbol in a silent streak. NO waiting for the last PRE_CLOSE_SECONDS —
    the signal fires on the very next poll after the threshold is crossed, so
    run_bot has the whole rest of the bucket to pre-calculate SL/TP/R:R. The
    signal carries candle_closed=False so run_bot knows to wait for the close
    price before executing.

    Uses pre_spike_bucket (ISO timestamp) per symbol to prevent duplicate
    signals for the same bucket — activity_spike is suppressed if
    pre_activity_spike was already emitted for that bucket.
    """
    now_bucket = pd.Timestamp.now(tz="UTC").floor(f"{config.BUCKET_MINUTES}min")

    if df.empty:
        return state

    # Precompute per (symbol, bucket) once: event count, total USD, and the
    # LARGEST SINGLE liquidation USD. "Active" is decided by the biggest
    # individual event, not the bucket total — ten $100 liquidations adding
    # up to $1,000 do NOT count as activity; one $1,000+ liquidation does.
    counts = df.groupby(["symbol", "bucket"]).size()
    usd_sums = df.groupby(["symbol", "bucket"])["usd_value"].sum()
    usd_max = df.groupby(["symbol", "bucket"])["usd_value"].max()
    symbols = sorted(df["symbol"].unique())

    # ── Phase 1: completed buckets (per-symbol) ─────────────────────────
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
            usd = float(usd_sums.get((sym, cursor), 0.0))
            max_single_usd = float(usd_max.get((sym, cursor), 0.0))
            is_active = max_single_usd >= config.MIN_ACTIVITY_USD

            if is_active:
                if silent >= config.SILENCE_THRESHOLD:
                    # Suppress activity_spike if pre_activity_spike was already
                    # emitted for this same bucket (avoid duplicate signals).
                    pre_bucket = (sym_state or {}).get("pre_spike_bucket")
                    already_pre = pre_bucket is not None and pre_bucket == cursor.isoformat()
                    if not already_pre:
                        emit_signal(f_out, sym, cursor, "activity_spike", count, silent,
                                    usd, max_single_usd, candle_closed=True)
                active += 1
                silent = 0
            else:
                if active >= config.ACTIVITY_THRESHOLD:
                    emit_signal(f_out, sym, cursor, "collapse", count, active,
                                usd, max_single_usd, candle_closed=True)
                silent += 1
                active = 0

            cursor += BUCKET_DELTA

        state[sym] = {
            "last_bucket": (cursor - BUCKET_DELTA).isoformat() if cursor > first_bucket else first_bucket.isoformat(),
            "silent": silent,
            "active": active,
            "pre_spike_bucket": state.get(sym, {}).get("pre_spike_bucket"),
        }

    # ── Phase 2: pre-spike on the current (still-forming) bucket ────────
    # Fires IMMEDIATELY on the next poll after a >= $1000 liquidation is seen
    # in the current bucket for a symbol in a silent streak — no waiting for
    # the last PRE_CLOSE_SECONDS. run_bot gets the whole rest of the bucket
    # to pre-calculate, then just waits for the close price.
    for sym in symbols:
        sym_st = state.get(sym, {})
        if sym_st.get("silent", 0) >= config.SILENCE_THRESHOLD:
            cur_max_single = float(usd_max.get((sym, now_bucket), 0.0))
            if cur_max_single >= config.MIN_ACTIVITY_USD:
                # Only emit once per bucket — pre_spike_bucket tracks which
                # bucket already had a pre-signal emitted.
                if sym_st.get("pre_spike_bucket") != now_bucket.isoformat():
                    cur_count = int(counts.get((sym, now_bucket), 0))
                    emit_signal(f_out, sym, now_bucket, config.PRE_SPIKE_SIGNAL,
                                cur_count, sym_st.get("silent", 0), 0.0, cur_max_single,
                                candle_closed=False)
                    sym_st["pre_spike_bucket"] = now_bucket.isoformat()

    return state


def main():
    print(f"\n  Liquidation Bucket Monitor")
    print(f"  Bucket width      : {config.BUCKET_MINUTES}min")
    print(f"  Active bucket min : a single liquidation >= ${config.MIN_ACTIVITY_USD:,.0f}")
    print(f"  Silence threshold : {config.SILENCE_THRESHOLD} buckets -> activity_spike")
    print(f"  Activity threshold: {config.ACTIVITY_THRESHOLD} buckets -> collapse")
    print(f"  Pre-spike         : immediate {config.PRE_SPIKE_SIGNAL} the moment ${config.MIN_ACTIVITY_USD:,.0f} "
          f"is crossed in a forming bucket (candle_closed=False)")
    print(f"  Reading           : {config.LIQ_CSV}")
    print(f"  Writing signals to: {config.LIQ_SIGNALS_FILE}\n")

    state = load_state()
    tick_count = 0
    heartbeat_every = max(1, round(60 / config.BUCKET_POLL_SECONDS))

    with open(config.LIQ_SIGNALS_FILE, "a") as f_out:
        while True:
            df = load_events()
            n_events = len(df) if not df.empty else 0
            state = process_tick(df, state, f_out)
            save_state(state)
            tick_count += 1

            if tick_count % heartbeat_every == 0:
                now = datetime.now(timezone.utc).isoformat(timespec="seconds")
                n_symbols = len(state)
                closest = sorted(
                    state.items(),
                    key=lambda kv: -max(kv[1].get("silent", 0), kv[1].get("active", 0)),
                )[:3]
                closest_str = ", ".join(
                    f"{sym}(silent={s.get('silent', 0)},active={s.get('active', 0)})"
                    for sym, s in closest
                )or "none yet"
                print(f" [{now}] heartbeat: {n_events} rows in CSV, {n_symbols} symbols tracked, ")
                print(f"closest to firing : {closest_str}")

            time.sleep(config.BUCKET_POLL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n  Stopped.")
