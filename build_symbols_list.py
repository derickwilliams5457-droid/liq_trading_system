#!/usr/bin/env python3
"""Read Binance futures symbols (.txt) and Bybit USDT linear futures (.csv),
write the intersection to symbols.txt — one symbol per line, sorted."""

import csv
from pathlib import Path

PROJECT  = Path(__file__).parent
BINANCE_F  = PROJECT / "binance_futures_symbols.txt"
BYBIT_F    = PROJECT / "bybit_usdt_linear_futures.csv"
OUT        = PROJECT / "symbols.txt"


def load_binance() -> set[str]:
    if not BINANCE_F.exists():
        return set()
    return {line.strip() for line in BINANCE_F.read_text().splitlines() if line.strip()}


def load_bybit() -> set[str]:
    if not BYBIT_F.exists():
        return set()
    symbols = set()
    with BYBIT_F.open(newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            sym = row.get("symbol", "").strip()
            status = row.get("status", "").strip()
            if sym and status == "Trading":
                symbols.add(sym)
    return symbols


def main():
    binance = load_binance()
    bybit   = load_bybit()
    common  = sorted(binance & bybit)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(common) + "\n")
    print(f"Binance: {len(binance)}  Bybit: {len(bybit)}  Common: {len(common)}")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
