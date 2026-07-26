"""
anomaly.py  —  used by allocation.py (Stage 4)
================================================
Fetches OHLCV, flags anomaly candles (price-change + volume-surge intensity),
and reduces them to a list of "uncleared" price zones: anomaly candles whose
[Low, High] range has not been re-entered by price since they formed. These
act as unfilled liquidity/imbalance zones that allocation.py targets for TP
and references for SL.

This module does NOT plot anything — it returns plain data. If you want a
one-off visual, run it directly (see __main__ below) for a printed list.

Requirements:
    pip install ccxt pandas numpy scipy
"""

from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import ccxt
from scipy import stats


def _normalize_pandas_freq(tf: str) -> str:
    """Accepts either a ccxt/Binance-style timeframe ('3m', '15m', '1h', '1d')
    or an already-valid pandas offset alias ('3min', '1h', '1D') and returns
    something pandas' resample()/floor() will accept. Binance-style minute
    notation ('Nm') is not a valid pandas alias on its own — pandas reads
    bare 'm' as month-end — so it must become 'Nmin'."""
    tf = tf.strip()
    if tf[-1] == "m" and tf[:-1].isdigit():
        return tf[:-1] + "min"
    return tf


class BinanceFuturesAnalyzer:
    def __init__(self, symbol="XAUTUSDT", chart_tf="3min"):
        self.symbol = symbol
        self.chart_tf = _normalize_pandas_freq(chart_tf)
        self.exchange = ccxt.binance({
            "enableRateLimit": True,
            "options": {"defaultType": "future"},
        })

    def fetch_anomaly_data(self, fetch_mode="today"):
        now = datetime.now(timezone.utc)

        if fetch_mode == "yesterday":
            start_dt = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)

        start_ms = int(start_dt.timestamp() * 1000)
        now_ms = int(now.timestamp() * 1000)

        ohlcv = []
        since = start_ms
        while since < now_ms:
            batch = self.exchange.fetch_ohlcv(self.symbol, "1m", since=since, limit=1000)
            if not batch:
                break
            ohlcv.extend(batch)
            since = batch[-1][0] + 1
            if len(batch) < 1000:
                break

        if not ohlcv:
            return pd.DataFrame(), None

        df_1m = pd.DataFrame(ohlcv, columns=["Time", "Open", "High", "Low", "Close", "Volume"])
        df_1m["Time"] = pd.to_datetime(df_1m["Time"], unit="ms")
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df_1m[col] = df_1m[col].astype(float)
        df_1m.set_index("Time", inplace=True)
        df_1m = df_1m[~df_1m.index.duplicated(keep="last")]
        df_1m.sort_index(inplace=True)

        df = df_1m.resample(self.chart_tf, origin="start").agg({
            "Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum",
        }).dropna()

        def norm(s):
            return (s - s.min()) / (s.max() - s.min()) if (s.max() - s.min()) != 0 else s * 0

        df["price_chg"] = df["Close"].pct_change().abs()
        df["vol_surge"] = df["Volume"] / df["Volume"].rolling(10).mean()
        df["nv"] = norm(df["vol_surge"].fillna(0))
        df["np"] = norm(df["price_chg"].fillna(0))
        df["intensity"] = (df["np"] * 0.6) + (df["nv"] * 0.4)
        df["is_anomaly"] = df["intensity"] > 0.7   # absolute cutoff — can legitimately be empty on quiet windows
        df["is_anomaly_relative"] = df["intensity"] > df["intensity"].quantile(0.8)   # top 20% of THIS window
        df["intensity_zscore"] = stats.zscore(df["intensity"].fillna(0))

        daily_open = df_1m.iloc[0]["Open"]
        return df.reset_index(), daily_open


def compute_uncleared_zones(df: pd.DataFrame, use_relative: bool = True, clearing: str = "close") -> list[dict]:
    """
    An anomaly candle at time t defines a price zone [Low, High]. Zones that
    stay "uncleared" act as the liquidity/imbalance levels allocation.py
    targets.

    clearing="close" (default): a zone clears only when a LATER candle's
        CLOSE settles back inside [Low, High] — price actually returned and
        held there, not just wicked through. This is deliberately strict:
        ordinary intrabar volatility wicks through nearby levels constantly,
        so a raw range-overlap test ("wick" mode) clears almost every zone
        within a bar or two and leaves nothing uncleared in real data.
    clearing="wick": any later candle's range overlapping the zone at all
        counts as cleared — much more aggressive, mostly of historical/
        comparison interest.

    use_relative=True (default) selects candidate anomaly candles the same
    way the original script's chart did — top 20% of intensity WITHIN this
    fetch window — rather than the absolute `intensity > 0.7` cutoff, which
    can legitimately flag zero candles on a quiet window since intensity is
    min-max normalized per call. Set False to use the strict absolute cutoff.

    Returns a list of dicts, ordered oldest -> newest:
        {time, low, high, mid, direction, intensity}
    direction: "bullish" if the anomaly candle closed up, else "bearish".
    """
    if df.empty:
        return []

    flag_col = "is_anomaly_relative" if use_relative else "is_anomaly"
    if flag_col not in df.columns:
        return []

    zones = []
    anomaly_rows = df[df[flag_col]]

    for idx, row in anomaly_rows.iterrows():
        zone_low, zone_high = row["Low"], row["High"]
        after = df.loc[df.index > idx]
        if clearing == "wick":
            # any later candle's range touches the zone at all — very easily
            # satisfied by ordinary volatility, clears almost everything fast
            cleared = ((after["Low"] <= zone_high) & (after["High"] >= zone_low)).any()
        else:
            # a later candle actually CLOSED back inside the zone — price
            # settled there again, not just wicked through it
            cleared = ((after["Close"] >= zone_low) & (after["Close"] <= zone_high)).any()
        if not cleared:
            zones.append({
                "time": row["Time"],
                "low": float(zone_low),
                "high": float(zone_high),
                "mid": float((zone_low + zone_high) / 2),
                "direction": "bullish" if row["Close"] >= row["Open"] else "bearish",
                "intensity": float(row["intensity"]),
            })

    return zones


def get_uncleared_zones(symbol: str, chart_tf: str = "3min", fetch_mode: str = "today",
                          use_relative: bool = True, clearing: str = "close") -> list[dict]:
    """Convenience one-shot: fetch + compute in a single call. This is the
    function allocation.py imports."""
    analyzer = BinanceFuturesAnalyzer(symbol=symbol, chart_tf=chart_tf)
    df, _ = analyzer.fetch_anomaly_data(fetch_mode=fetch_mode)
    return compute_uncleared_zones(df, use_relative=use_relative, clearing=clearing)


def print_anomalies(df: pd.DataFrame, use_relative: bool = True):
    """Print every detected anomaly candle (not just the uncleared ones) —
    time, price point, bullish/bearish, and intensity — for a quick manual
    read of what the detector is seeing."""
    flag_col = "is_anomaly_relative" if use_relative else "is_anomaly"
    if df.empty or flag_col not in df.columns:
        print("No data.")
        return

    rows = df[df[flag_col]]
    print(f"{len(rows)} anomaly candle(s):\n")
    for _, row in rows.iterrows():
        direction = "bullish" if row["Close"] >= row["Open"] else "bearish"
        print(f"  {row['Time']}  price={row['Close']:.4f}  {direction:<8}  intensity={row['intensity']:.2f}")


if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "XAUTUSDT"

    analyzer = BinanceFuturesAnalyzer(symbol=sym, chart_tf="3min")
    df, _ = analyzer.fetch_anomaly_data(fetch_mode="today")   # always since today 00:00 UTC

    print_anomalies(df)
    print()

    zones = compute_uncleared_zones(df)
    print(f"{len(zones)} uncleared zone(s) for {sym}:\n")
    for z in zones:
        print(f"  {z['time']}  {z['direction']:<8}  [{z['low']:.4f} - {z['high']:.4f}]  "
              f"mid={z['mid']:.4f}  intensity={z['intensity']:.2f}")
