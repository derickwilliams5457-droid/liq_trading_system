"""
perfio.py  —  single-writer persistence for reporting state
=================================================================
The Binance monitor (trader.py) and the Bybit monitor (bybit_mirror.py)
run in the SAME process, two threads. All performance/log appends and all
state-file writes go through the locks in this module so two threads can
never interleave a CSV row, mangle a JSONL line, or clobber a state file
with a half-written buffer.

Three artifacts live here:

  - performance.csv   append-only PnL history (rows carry status open/closed)
  - trade_log.jsonl   append-only audit trail
  - trades.json       open-trade LEDGER (per exchange) — survives restarts so
                      a position is never orphaned; written atomically
  - snapshot.json     live SNAPSHOT of open positions + PnL plus each
                      exchange's account state (equity / wallet / unrealized),
                      rewritten every monitor poll; the dashboard's source of
                      truth for what is actually open AND for verifying its
                      own PnL math against the exchange

Every file write is atomic (temp file + os.replace) except the two append
logs, which are single-writer serialized + flushed per line. Readers (dash,
maintenance) can therefore never observe a torn write.
"""

import csv
import json
import os
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path

import config

_WRITE_LOCK = threading.Lock()

PERF_HEADER = [
    "timestamp_utc", "symbol", "direction", "qty", "entry",
    "sl", "tp", "mark_price", "unrealized_pnl", "status", "exchange",
]


def _open_append(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    return open(path, "a", encoding="utf-8", newline="")


def append_perf_row(row: list):
    """Append one performance.csv row. Serialized + flushed so concurrent
    monitor threads can never interleave a partial line."""
    with _WRITE_LOCK:
        with _open_append(config.PERFORMANCE_CSV) as f:
            csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow([str(v) for v in row])
            f.flush()


def append_log_event(record: dict):
    """Append one trade_log.jsonl event. Serialized + flushed like the CSV."""
    with _WRITE_LOCK:
        with _open_append(config.TRADE_LOG_FILE) as f:
            f.write(json.dumps(record, default=str) + "\n")
            f.flush()


def ensure_perf_header():
    """Write the CSV header (and migrate a legacy header that lacks the
    trailing `exchange` column) exactly once. Rewriting the whole file is safe
    here: this only runs at startup, before any monitor thread begins."""
    path = config.PERFORMANCE_CSV
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_header = not path.exists()
    if not needs_header:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            header = f.readline().strip()
        if header and "exchange" not in header:
            needs_header = True
    if needs_header:
        with _WRITE_LOCK:
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow(PERF_HEADER)


def _atomic_write(path: Path, obj):
    """Write JSON via temp + os.replace so readers never see a torn file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".perfio-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, default=str, indent=2)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def load_ledger() -> dict:
    """trades.json -> {exchange: {symbol: meta}}. Corrupt/missing -> {}."""
    with _WRITE_LOCK:
        return _load_ledger_locked()


def _load_ledger_locked() -> dict:
    path = config.TRADES_LEDGER_FILE
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def update_ledger(exchange: str, section: dict):
    """Atomically replace one exchange's ledger section. The load-merge-save is
    one lock hold, so the Binance and Bybit monitors (same process) can never
    clobber each other's section."""
    with _WRITE_LOCK:
        ledger = _load_ledger_locked()
        ledger[exchange] = dict(section)
        _atomic_write(config.TRADES_LEDGER_FILE, ledger)


def save_ledger(ledger: dict):
    with _WRITE_LOCK:
        _atomic_write(config.TRADES_LEDGER_FILE, ledger)


def _load_snapshot_locked() -> dict:
    path = config.SNAPSHOT_FILE
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def update_snapshot(exchange: str, positions: dict, account: dict | None = None):
    """MERGE one exchange's position set + (optional) account state into the
    shared snapshot.json. Both monitors call this every poll; because each
    exchange only touches its own section, binance and bybit positions (and
    account states) all survive in the same file instead of clobbering each
    other."""
    with _WRITE_LOCK:
        cur = _load_snapshot_locked()
        cur[exchange] = dict(positions)
        if account is not None:
            acc = cur.setdefault("account", {})
            acc[exchange] = account
        cur["generated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        _atomic_write(config.SNAPSHOT_FILE, cur)


def read_snapshot() -> dict | None:
    path = config.SNAPSHOT_FILE
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _load_signals_locked() -> dict:
    path = config.SIGNALS_FILE
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def load_signals() -> dict:
    with _WRITE_LOCK:
        return _load_signals_locked()


def save_signal(exchange: str, symbol: str, meta: dict):
    """Write a trade signal to the combined signals file. Called on fill so
    the monitor can distinguish system-initiated positions from external ones
    across both exchanges."""
    with _WRITE_LOCK:
        signals = _load_signals_locked()
        ex = signals.setdefault(exchange, {})
        ex[symbol] = {
            "symbol": meta.get("symbol", symbol),
            "direction": meta.get("direction", ""),
            "qty": meta.get("qty", 0),
            "entry": meta.get("entry", 0),
            "sl": meta.get("sl", ""),
            "tp": meta.get("tp", ""),
            "opened_at": meta.get("opened_at", ""),
        }
        _atomic_write(config.SIGNALS_FILE, signals)


def remove_signal(exchange: str, symbol: str):
    """Remove a signal from the combined file when a position closes."""
    with _WRITE_LOCK:
        signals = _load_signals_locked()
        ex = signals.get(exchange)
        if ex and symbol in ex:
            del ex[symbol]
            if not ex:
                del signals[exchange]
            _atomic_write(config.SIGNALS_FILE, signals)


def find_signal(exchange: str, symbol: str) -> dict | None:
    """Look up a signal by exchange + symbol. Returns the stored meta dict
    or None if the system didn't place this trade."""
    with _WRITE_LOCK:
        signals = _load_signals_locked()
        ex = signals.get(exchange)
        if ex:
            return ex.get(symbol)
        return None
