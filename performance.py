"""
performance_plot.py  —  optional, standalone
===============================================
Reads config.PERFORMANCE_CSV (written continuously by trader.py) and renders
an equity-curve-style chart: unrealized PnL over time per position, plus
markers where positions closed. Run any time — it doesn't touch the live
pipeline.

Usage:
    python performance_plot.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import config


def load_performance() -> pd.DataFrame:
    df = pd.read_csv(config.PERFORMANCE_CSV, on_bad_lines="skip")
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df["unrealized_pnl"] = pd.to_numeric(df["unrealized_pnl"], errors="coerce")
    return df


def plot(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(13, 6))
    fig.patch.set_facecolor("#0f1115")
    ax.set_facecolor("#0f1115")

    open_rows = df[df["status"] == "open"]
    closed_rows = df[df["status"] == "closed"]

    ax.plot(open_rows["timestamp_utc"], open_rows["unrealized_pnl"],
            color="#00b4ff", linewidth=1.3, label="unrealized PnL")
    ax.axhline(0, color="#555555", linewidth=0.8, linestyle="--")

    if not closed_rows.empty:
        ax.scatter(closed_rows["timestamp_utc"], [0] * len(closed_rows),
                   marker="x", color="#ffb703", s=80, zorder=5, label="position closed")

    ax.set_title("Trading Performance — Unrealized PnL Over Time", color="white", fontsize=13, fontweight="bold")
    ax.set_ylabel("PnL (USDT)", color="#cccccc")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    ax.tick_params(colors="#cccccc")
    for spine in ax.spines.values():
        spine.set_color("#333333")
    ax.grid(alpha=0.15, color="#888888")
    legend = ax.legend(loc="upper left", framealpha=0.85, facecolor="#1a1d24")
    for text in legend.get_texts():
        text.set_color("#dddddd")
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    if not config.PERFORMANCE_CSV.exists():
        print(f"No performance data yet at {config.PERFORMANCE_CSV}")
    else:
        plot(load_performance())
