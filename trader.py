"""
trader.py  —  Stage 5 of the pipeline
=======================================
Takes a direction + SL + TP from allocation.py and places the trade on
Binance Futures via ccxt. Before opening anything it checks for any existing
open position across the whole account and refuses to open a second one.
A background loop polls open-position PnL and appends a row to
config.PERFORMANCE_CSV on every poll, plus a final row when a position closes.

⚠️  This places real orders against a real exchange account. config.TESTNET
defaults to True — you must explicitly set LIQ_TESTNET=false to trade live.
Never hard-code API keys; set BINANCE_API_KEY / BINANCE_API_SECRET as
environment variables (or in a local, gitignored .env file).

Usage (usually driven by run_bot.py, but runnable standalone for the
monitor loop):
    python trader.py
"""

import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import ccxt

import config


class Trader:
    def __init__(self):
        if not config.BINANCE_API_KEY or not config.BINANCE_API_SECRET:
            raise RuntimeError(
                "BINANCE_API_KEY / BINANCE_API_SECRET are not set. "
                "Export them as environment variables or put them in a local .env file."
            )

        # Initialize CCXT Binance USD-M exchange with stripped keys
        self.exchange = ccxt.binanceusdm({
            "apiKey": config.BINANCE_API_KEY.strip(),
            "secret": config.BINANCE_API_SECRET.strip(),
            "enableRateLimit": True,
        })

        # Enable CCXT's native Binance Demo Trading mode
        if config.TESTNET:
            self.exchange.enableDemoTrading(True)

        self._open_trade_meta = None   # tracks sl/tp/entry for the currently open position
        self._init_performance_csv()

    # ── Position state ───────────────────────────────────────────────────
    def get_open_positions(self):
        try:
            positions = self.exchange.fetch_positions()
            return [
                p for p in positions 
                if float(p.get("contracts") or p.get("info", {}).get("positionAmt", 0)) != 0
            ]
        except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
            print(f"  [trader] Network error fetching positions: {e}")
            # Return empty or handle gracefully depending on safety requirements
            return []
        except Exception as e:
            print(f"  [trader] Unexpected error fetching positions: {e}")
            return []

    def has_open_position(self) -> bool:
        return len(self.get_open_positions()) >= config.MAX_CONCURRENT_POSITIONS

    # ── Sizing ───────────────────────────────────────────────────────────
    def _position_size(self, symbol: str, entry: float, sl: float) -> float:
        balance = self.exchange.fetch_balance()
        equity = float(balance.get("USDT", {}).get("total", 0) or 0)
        risk_amount = equity * (config.RISK_PER_TRADE_PCT / 100)
        per_unit_risk = abs(entry - sl)
        if per_unit_risk <= 0:
            return 0.0
        qty = risk_amount / per_unit_risk

        try:
            return float(self.exchange.amount_to_precision(symbol, qty))
        except Exception:
            return qty

    # ── Execution ────────────────────────────────────────────────────────
    def execute_trade(self, levels: dict, deadline_ts: float | None = None):
        """levels is allocation.compute_levels()'s return dict.

        deadline_ts: Optional Unix timestamp cutoff. Trade aborts if current time exceeds this.
        """
        # 1. Deadline Check: Prevent executing stale signals
        if deadline_ts is not None and time.time() > deadline_ts:
            print(
                f"  [trader] Aborting {levels.get('symbol', 'trade')} — signal deadline expired "
                f"({time.time() - deadline_ts:.2f}s late)."
            )
            return None

        if self.has_open_position():
            print(f"  [trader] Skipping {levels['symbol']} — a position is already open.")
            return None

        if levels.get("risk_reward") is None or levels["risk_reward"] < config.MIN_RISK_REWARD:
            print(
                f"  [trader] Skipping {levels['symbol']} — R:R {levels.get('risk_reward')} "
                f"below MIN_RISK_REWARD ({config.MIN_RISK_REWARD})."
            )
            return None

        symbol = levels["symbol"]
        direction = levels["direction"]
        entry_hint = levels["entry"]
        sl, tp = levels["sl"], levels["tp"]

        side = "buy" if direction == "long" else "sell"
        close_side = "sell" if direction == "long" else "buy"

        qty = self._position_size(symbol, entry_hint, sl)
        if qty <= 0:
            print(f"  [trader] Zero/invalid position size for {symbol}, skipping.")
            return None

        # 2. Re-check deadline right before API order placement
        if deadline_ts is not None and time.time() > deadline_ts:
            print(f"  [trader] Aborting {symbol} right before order placement — deadline exceeded.")
            return None

        try:
            self.exchange.set_leverage(config.LEVERAGE, symbol)
        except Exception as e:
            print(f"  [trader] Could not set leverage: {e}")

        try:
            entry_order = self.exchange.create_order(symbol, "market", side, qty)
            self.exchange.create_order(
                symbol, "STOP_MARKET", close_side, qty,
                params={"stopPrice": sl, "closePosition": True},
            )
            self.exchange.create_order(
                symbol, "TAKE_PROFIT_MARKET", close_side, qty,
                params={"stopPrice": tp, "closePosition": True},
            )
        except Exception as e:
            print(f"  [trader] Order placement failed for {symbol}: {e}")
            self._log_trade_event({"event": "order_failed", "error": str(e), **levels})
            return None

        self._open_trade_meta = {
            **levels, 
            "qty": qty, 
            "opened_at": datetime.now(timezone.utc).isoformat()
        }
        self._log_trade_event({"event": "opened", **self._open_trade_meta})
        print(f"  [trader] Opened {direction.upper()} {symbol} qty={qty} entry~{entry_hint} sl={sl} tp={tp}")
        return entry_order

    # ── Monitoring / performance logging ────────────────────────────────
    def _init_performance_csv(self):
        csv_path = Path(config.PERFORMANCE_CSV)
        # Ensure target directory exists before opening file
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        if not csv_path.exists():
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow([
                    "timestamp_utc", "symbol", "direction", "qty", "entry",
                    "sl", "tp", "mark_price", "unrealized_pnl", "status",
                ])

    def _log_performance_row(self, symbol, direction, qty, entry, sl, tp, mark_price, pnl, status):
        csv_path = Path(config.PERFORMANCE_CSV)
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow([
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                symbol, direction, qty, entry, sl, tp, mark_price, pnl, status,
            ])

    def _log_trade_event(self, record: dict):
        record["time"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        log_path = Path(config.TRADE_LOG_FILE)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")

    def monitor_loop(self):
        """Run forever: poll open positions, log PnL, detect closes."""
        print("  [trader] Monitor loop started.")
        while True:
            try:
                positions = self.get_open_positions()
                if positions and self._open_trade_meta:
                    p = positions[0]
                    mark = float(p.get("markPrice") or 0)
                    pnl = float(p.get("unrealizedPnl") or 0)
                    meta = self._open_trade_meta
                    self._log_performance_row(
                        meta["symbol"], meta["direction"], meta["qty"], meta["entry"],
                        meta["sl"], meta["tp"], mark, pnl, "open",
                    )
                elif self._open_trade_meta:
                    # position that was open has since closed
                    meta = self._open_trade_meta
                    self._log_performance_row(
                        meta["symbol"], meta["direction"], meta["qty"], meta["entry"],
                        meta["sl"], meta["tp"], "", "", "closed",
                    )
                    self._log_trade_event({"event": "closed", **meta})
                    print(f"  [trader] Position closed: {meta['symbol']}")
                    self._open_trade_meta = None
            except Exception as e:
                print(f"  [trader] Monitor loop error: {e}")

            time.sleep(config.POSITION_POLL_SECONDS)


if __name__ == "__main__":
    Trader().monitor_loop()