"""
strategy.py  —  Stage 3 of the pipeline
=========================================
Called by run_bot.py whenever liq_bucket.py fires config.TRIGGER_SIGNAL for a
symbol. Pulls the previous config.TREND_CANDLES candles plus the "interest"
candle (the one the signal fired on) at config.STRATEGY_TIMEFRAME, reads the
net trend across that window, and returns the direction to bet.

config.CONTRARIAN_MODE (default True) bets OPPOSITE the trend — this matches
the "liquidation silence" reversal logic: a burst of activity breaking a long
silence tends to mark exhaustion, not continuation. Flip it to False for a
continuation/momentum read instead.

This module doesn't place trades — it only returns a direction string.
"""

import ccxt
import pandas as pd

import config

_exchange = None


def get_exchange():
    global _exchange
    if _exchange is None:
        _exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})
    return _exchange


def get_recent_candles(symbol: str, n: int = None, end_time=None) -> pd.DataFrame:
    """Fetch the last n candles at config.STRATEGY_TIMEFRAME (default:
    TREND_CANDLES + INTEREST_CANDLES, i.e. 5 prior + the trigger candle).

    end_time, if given, pins the window so it ends at (and includes) that
    exact candle instead of whatever is most recently closed when this runs.
    This matters because run_bot.py may process a signal a little while
    after it actually fired (poll interval, network latency) — without
    pinning, the "interest candle" could silently drift to a later candle
    than the one the signal was about, and the entry price would follow it.
    """
    n = n or (config.TREND_CANDLES + config.INTEREST_CANDLES)
    ex = get_exchange()

    if end_time is not None:
        end_ts = pd.Timestamp(end_time)
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize("UTC")
        # fetch extra so trimming down to end_time still leaves n candles
        ohlcv = ex.fetch_ohlcv(symbol, config.STRATEGY_TIMEFRAME, limit=n + 10)
        df = pd.DataFrame(ohlcv, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df = df[df["time"] <= end_ts].tail(n).reset_index(drop=True)
    else:
        ohlcv = ex.fetch_ohlcv(symbol, config.STRATEGY_TIMEFRAME, limit=n)
        df = pd.DataFrame(ohlcv, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)

    return df


def read_trend(candles: pd.DataFrame) -> str:
    """
    Net direction across the window: the 5 prior candles vote by their own
    open->close sign, the interest (most recent/trigger) candle votes too.
    Returns 'up', 'down', or 'flat' (tie -> no trade).
    """
    votes = 0
    for _, row in candles.iterrows():
        if row["close"] > row["open"]:
            votes += 1
        elif row["close"] < row["open"]:
            votes -= 1
    if votes > 0:
        return "up"
    if votes < 0:
        return "down"
    return "flat"


def decide_direction(symbol: str, candles: pd.DataFrame = None, end_time=None) -> tuple[str | None, pd.DataFrame]:
    """
    Returns (direction, candles) where direction is 'long', 'short', or None
    (flat trend -> skip the trade). candles is returned so allocation.py can
    reuse the same fetch instead of hitting the API again — its LAST row is
    the interest/signal candle, and its close is the entry price.

    end_time should be the signal's bucket_start timestamp — it pins the
    fetch so the interest candle is exactly the one the signal fired on,
    not whatever is most recently closed by the time this runs.
    """
    if candles is None:
        candles = get_recent_candles(symbol, end_time=end_time)

    if len(candles) < 2:
        return None, candles

    trend = read_trend(candles)
    if trend == "flat":
        return None, candles

    if config.CONTRARIAN_MODE:
        direction = "short" if trend == "up" else "long"
    else:
        direction = "long" if trend == "up" else "short"

    return direction, candles


if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    direction, candles = decide_direction(sym)
    print(candles[["time", "open", "close"]])
    print(f"\nDirection for {sym}: {direction}  (contrarian_mode={config.CONTRARIAN_MODE})")
