"""
liq_stream.py  —  Stage 1 of the pipeline
==========================================
Streams real-time liquidation orders from Binance Futures (!forceOrder@arr),
prints them to the terminal, and logs EVERY event (regardless of size) to
config.LIQ_CSV. This file is a long-running, standalone process — run it by
itself, independent of every other stage.

Requirements:
    pip install websockets

Usage:
    python liq_stream.py
"""

import asyncio
import csv
import json
import time
from collections import defaultdict
from datetime import datetime, timezone

import websockets

import config

# ── ANSI color codes ──────────────────────────────────────────────────────────
R, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
RED, GREEN, YELLOW, CYAN, WHITE, MAGENTA = (
    "\033[91m", "\033[92m", "\033[93m", "\033[96m", "\033[97m", "\033[95m"
)
BG_RED, BG_BLUE = "\033[41m", "\033[44m"

CSV_FIELDS = [
    "timestamp_utc", "event_time_ms", "symbol", "side", "side_label",
    "qty", "avg_price", "usd_value", "delta_vs_last_pct",
]

# ── State ─────────────────────────────────────────────────────────────────────
entry_cache: dict = {}
liq_count: dict = defaultdict(int)
total_longs = 0
total_shorts = 0
session_start = time.time()
event_total = 0


def fmt_usd(v: float) -> str:
    if v >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:.0f}"


def fmt_change(pct: float) -> str:
    if pct > 0:
        return f"{GREEN}▲+{pct:.2f}%{R}"
    if pct < 0:
        return f"{RED}▼{pct:.2f}%{R}"
    return f"{DIM}±0.00%{R}"


def side_label(side: str) -> str:
    if side.upper() == "SELL":
        return f"{BG_RED}{BOLD} LONG LIQ {R}"
    return f"{BG_BLUE}{BOLD}SHORT LIQ {R}"


def init_csv():
    if not config.LIQ_CSV.exists():
        with open(config.LIQ_CSV, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()


def log_liq_to_csv(o: dict, sym: str, side: str, qty: float, ap: float, usd: float, delta_pct):
    row = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "event_time_ms": o.get("T", ""),
        "symbol": sym,
        "side": side,
        "side_label": "LONG_LIQ" if side.upper() == "SELL" else "SHORT_LIQ",
        "qty": qty,
        "avg_price": ap,
        "usd_value": round(usd, 2),
        "delta_vs_last_pct": (round(delta_pct, 4) if delta_pct is not None else ""),
    }
    try:
        with open(config.LIQ_CSV, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(row)
    except Exception as e:
        print(f"{YELLOW}[csv write error] {e}{R}")


def print_header():
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    up = int(time.time() - session_start)
    hms = f"{up//3600:02d}:{(up%3600)//60:02d}:{up%60:02d}"
    print(f"\n{BOLD}{CYAN}━━━  BINANCE FUTURES LIQUIDATION FEED  ━━━{R}  {DIM}uptime {hms} | {ts}{R}")
    print(f"  {'SYMBOL':<13} {'SIDE':<13} {'QTY':>10}  {'AVG PRICE':>12}  {'Δ vs LAST LIQ':>15}  {'USD SIZE':>11}")
    print(f"  {DIM}{'─'*13} {'─'*13} {'─'*10}  {'─'*12}  {'─'*15}  {'─'*11}{R}")


def print_liq(data: dict):
    global total_longs, total_shorts, event_total

    o = data.get("o", {})
    sym = o.get("s", "???")
    side = o.get("S", "?")
    qty = float(o.get("q", 0))
    ap = float(o.get("ap", 0) or o.get("p", 0))
    usd = qty * ap

    last = entry_cache.get(sym)
    delta_pct = (ap - last) / last * 100 if last else None

    log_liq_to_csv(o, sym, side, qty, ap, usd, delta_pct)

    if usd < config.MIN_USD_SIZE:
        return

    event_total += 1
    liq_count[sym] += 1
    if side.upper() == "SELL":
        total_longs += 1
    else:
        total_shorts += 1

    delta_str = fmt_change(delta_pct) if delta_pct is not None else f"{DIM}   first hit   {R}"
    entry_cache[sym] = ap

    if usd >= 1_000_000:
        size_str, bell = f"{MAGENTA}{BOLD}{fmt_usd(usd)}{R}", "\a"
    elif usd >= 100_000:
        size_str, bell = f"{YELLOW}{BOLD}{fmt_usd(usd)}{R}", ""
    else:
        size_str, bell = f"{WHITE}{fmt_usd(usd)}{R}", ""

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(
        f"  {BOLD}{CYAN}{sym:<13}{R} {side_label(side)}  "
        f"{qty:>10.4f}  {ap:>12.4f}  {delta_str:>27}  {size_str:>19}  {DIM}{ts}{R}{bell}"
    )

    if event_total % 40 == 0:
        print_stats()
        print_header()


def print_stats():
    total = total_longs + total_shorts
    if not total:
        return
    top5 = sorted(liq_count.items(), key=lambda x: -x[1])[:5]
    top5_str = "  ".join(f"{CYAN}{s}{R}×{n}" for s, n in top5)
    ratio = total_longs / total * 100 if total else 0
    print(
        f"\n  {DIM}▸ Stats{R}  Long liq'd: {RED}{total_longs}{R}  Short liq'd: {GREEN}{total_shorts}{R}  "
        f"Long bias: {YELLOW}{ratio:.0f}%{R}  │  Hot: {top5_str}"
    )


async def main():
    print(f"\n{BOLD}{GREEN}  Binance Futures Liquidation Monitor{R}")
    print(f"  Stream : {CYAN}{config.WS_URL}{R}")
    print(f"  Filter : {YELLOW}>= {fmt_usd(config.MIN_USD_SIZE)}{R} (terminal only — CSV logs everything)")

    init_csv()
    print(f"  CSV    : {CYAN}{config.LIQ_CSV.resolve()}{R}\n")

    reconnect_delay = 3
    while True:
        try:
            async with websockets.connect(
                config.WS_URL, ping_interval=20, ping_timeout=30, close_timeout=10,
            ) as ws:
                print(f"  {GREEN}✓ Connected{R}  — watching for liquidations...\n")
                print_header()
                reconnect_delay = 3

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                        if "data" in data:
                            data = data["data"]
                        if data.get("e") == "forceOrder":
                            print_liq(data)
                    except Exception as e:
                        print(f"{YELLOW}[parse error] {e}  raw={raw[:120]}{R}")

        except websockets.exceptions.ConnectionClosedError as e:
            print(f"\n{YELLOW}  Connection closed: {e} — reconnecting in {reconnect_delay}s…{R}")
        except OSError as e:
            print(f"\n{RED}  Network error: {e} — reconnecting in {reconnect_delay}s…{R}")
        except Exception as e:
            print(f"\n{RED}  Unexpected error: {e} — reconnecting in {reconnect_delay}s…{R}")

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n\n  {DIM}Session ended.{R}")
        print_stats()
        print()
