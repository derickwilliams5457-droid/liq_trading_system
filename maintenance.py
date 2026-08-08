"""
maintenance.py  —  unbounded-file-growth guard (optional 5th stage)
=====================================================================
Three of the pipeline's files grow without bound and on a small cloud disk
they eventually fill it up and kill the bot:

    data/liquidations.csv    one row per liquidation event (the busiest file)
    data/performance.csv     one row per PnL poll while a position is open
    data/trade_log.jsonl     one line per opened/closed/order event

This stage periodically trims each back to a bounded number of rows using an
atomic write-temp + os.replace, so a consumer never sees a half-written file.
It is SAFE because every consumer resumes from its own persisted cursor:
liq_bucket.py starts from the last bucket in liq_bucket_state.json and
run_bot.py tails liq_signals.jsonl from its current position — neither needs
old rows to keep working. run_bot.py/dash.py only re-read performance.csv and
trade_log.jsonl incrementally from their current offsets.

liq_signals.jsonl is deliberately NOT trimmed: run_bot.py holds it open and
tails it live, so replacing the inode would silently stop the tail until the
bot restarts — and a truncation racing a just-written signal could drop it.

There is a tiny, accepted race: a writer appending at the exact moment of the
replace loses that one row (appends land on the old inode). Files are only
trimmed when they exceed the cap (not on every pass), so this happens at most
once a day in practice. Nothing depends on zero loss in these files.

Usage:
    python maintenance.py     # trims once immediately, then every interval
"""

import os
import tempfile
import threading
import time

import config

_trim_lock = threading.Lock()

HEADER_LIQ = [
    "timestamp_utc", "event_time_ms", "symbol", "side", "side_label",
    "qty", "avg_price", "usd_value", "delta_vs_last_pct",
]
HEADER_PERF = [
    "timestamp_utc", "symbol", "direction", "qty", "entry",
    "sl", "tp", "mark_price", "unrealized_pnl", "status", "exchange",
]


def _trim_tail(path, max_rows: int, header: list | None):
    """Keep the CSV header + the last (max_rows - 1) rows when the file holds
    more than max_rows. Atomic: write a temp file, then os.replace over it.
    Never touches files smaller than the cap."""
    if not path.exists():
        return 0
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        rows = f.readlines()
    keep_extra = 1 if header is not None else 0
    if len(rows) <= max_rows + keep_extra:
        return 0

    keep = [rows[0]] if (header is not None and rows and rows[0].strip() == ",".join(header)) else rows[:1]
    keep = keep + rows[-(max_rows - len(keep)):]

    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".maintenance-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.writelines(keep)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    print(f"[maintenance] trimmed {path.name}: {len(rows)} -> {len(keep)} rows")
    return len(rows) - len(keep)


def trim_once():
    """Trim every unbounded file down to its configured cap."""
    with _trim_lock:
        for path, cap, header in (
            (config.LIQ_CSV,          config.MAX_LIQ_CSV_ROWS,      HEADER_LIQ),
            (config.PERFORMANCE_CSV,  config.MAX_PERFORMANCE_ROWS,  HEADER_PERF),
            (config.TRADE_LOG_FILE,   config.MAX_TRADE_LOG_LINES,   None),
        ):
            try:
                _trim_tail(path, cap, header)
            except Exception as e:
                print(f"[maintenance] failed to trim {path.name}: {e}")


def main():
    print(f"[maintenance] started — trimming every "
          f"{config.MAINTENANCE_INTERVAL_SECONDS}s (caps: "
          f"liq={config.MAX_LIQ_CSV_ROWS}, perf={config.MAX_PERFORMANCE_ROWS}, "
          f"log={config.MAX_TRADE_LOG_LINES})")
    trim_once()
    while True:
        time.sleep(config.MAINTENANCE_INTERVAL_SECONDS)
        trim_once()


if __name__ == "__main__":
    main()
