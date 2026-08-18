"""
strategy.py  —  Stage 3 of the pipeline
=========================================
Direction is determined by the 3-vote weighted-majority system (same as
before).  The rectangle is an *additional filter*: once the votes decide a
direction, the last RECTANGLE_CANDLES (5) candles are examined to build a
rectangle from highest high to lowest low, divided into 5 equal zones
(0% = high → 100% = low).

A trade is only allowed when the entry close lands in the *extreme* zones
matching the voted direction:
    short  → entry must be in the top    0-20%  zone
    long   → entry must be in the bottom 80-100% zone
If entry sits in the middle 20-80% zone the trade is skipped regardless of
what the votes say.

SL and TP are both derived from the Dynamic ATR over a 6-candle window:

    ATR  = mean True Range over the 6-candle window
    long -> SL = entry - ATR,  TP = entry + ATR
    short -> SL = entry + ATR,  TP = entry - ATR

R:R is always ~1:1 by construction.

The pre_close flag allows pre-calculating all of this 15 seconds before the
interest candle closes. The entry price is unknown at that point — run_bot.py
plugs it in at close time via _compute_entry_sl().

Returns (direction, candles, levels):
    direction: 'long', 'short', or None (rejected at vote/rect stage)
    levels:    dict with entry, sl, tp, sl_method, tp_method, atr, dynamic,
               rect bounds (rect_high, rect_low, zone_20, zone_80), or None
               if rejected.
"""

import math

import ccxt
import numpy as np
import pandas as pd

import config
import ratelimit

# Module-level singleton for public market data (no API keys needed).
# Avoids creating a fresh ccxt.binanceusdm() + load_markets() on every
# candle fetch — public OHLCV endpoints share one lightweight instance.
_public_exchange = None


def _get_public_exchange():
    global _public_exchange
    if _public_exchange is None:
        _public_exchange = ccxt.binanceusdm({"enableRateLimit": True})
    return _public_exchange


def get_recent_candles(symbol: str, end_time=None, hours: float = None) -> pd.DataFrame:
    """Fetch config.LOOKBACK_HOURS of config.STRATEGY_TIMEFRAME candles.

    Uses a cached ccxt Binance USD-M instance (public market data only,
    no API keys needed). All Binance I/O is serialized + fail-fast on a
    ban via ratelimit, so an IP ban aborts this fetch instead of retry-looping
    into it.
    """
    hours = hours if hours is not None else config.LOOKBACK_HOURS
    n = int(hours * 60 / config.BUCKET_MINUTES) + 10

    end_ms = None
    end_ts = None
    if end_time is not None:
        end_ts = pd.Timestamp(end_time)
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize("UTC")
        end_ms = int(end_ts.timestamp() * 1000)
        print(f"[strategy] fetching ~{n} candles for {symbol} @ {config.STRATEGY_TIMEFRAME}, "
              f"pinned to <= {end_ts}")
    else:
        print(f"[strategy] fetching ~{n} candles for {symbol} @ {config.STRATEGY_TIMEFRAME}")

    exchange = _get_public_exchange()
    params = {}
    if end_ms is not None:
        params["endTime"] = end_ms

    with ratelimit.serialized():
        ratelimit.fail_fast_if_banned()
        raw = exchange.fetch_ohlcv(symbol, config.STRATEGY_TIMEFRAME, limit=n, params=params)

    candles = [
        {
            "open_time": int(c[0]),
            "close_time": int(c[0]) + int(pd.Timedelta(config.STRATEGY_TIMEFRAME).total_seconds() * 1000) - 1,
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[5]),
            "quote_volume": 0.0,
        }
        for c in raw
    ]

    print(f"[strategy] got {len(candles)} candle(s) for {symbol}, "
          f"latest close={candles[-1]['close'] if candles else 'n/a'}")

    df = pd.DataFrame(candles, columns=[
        "open_time", "close_time", "open", "high", "low", "close", "volume", "quote_volume",
    ])
    df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df[["time", "open", "high", "low", "close", "volume"]]

    if end_ts is not None:
        df = df[df["time"] <= end_ts].reset_index(drop=True)
    return df


def _atr(candles: pd.DataFrame, period: int) -> float:
    """Wilder ATR over the trailing `period` bars of the given candle set."""
    high, low, close = candles["high"], candles["low"], candles["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low, (high - prev_close).abs(), (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    n = min(period, len(tr.dropna()))
    if n == 0:
        return float(tr.iloc[-1]) if len(tr) else 0.0
    return float(tr.tail(n).mean())


def efficiency_ratio_vote(candles: pd.DataFrame) -> tuple:
    """Vote 1: Kaufman's ER over config.ER_PERIOD bars. Returns (vote, value)."""
    n = config.ER_PERIOD
    if len(candles) < n + 1:
        return None, 0.0
    window = candles["close"].tail(n + 1).reset_index(drop=True)
    net = window.iloc[-1] - window.iloc[0]
    path = window.diff().abs().sum()
    er = abs(net) / path if path > 0 else 0.0
    if er < config.ER_THRESHOLD:
        return None, er
    return ("up" if net > 0 else "down"), er


def regression_slope_vote(candles: pd.DataFrame) -> tuple:
    """Vote 2: least-squares slope of Close over config.SLOPE_PERIOD bars,
    normalized by ATR over the same window. Returns (vote, normalized_slope)."""
    n = config.SLOPE_PERIOD
    if len(candles) < n:
        return None, 0.0
    window = candles["close"].tail(n).reset_index(drop=True)
    x = np.arange(n)
    slope, _ = np.polyfit(x, window.values, 1)
    atr = _atr(candles, n)
    if atr <= 0:
        return None, 0.0
    slope_norm = slope / atr
    if slope_norm >= config.SLOPE_THRESHOLD:
        return "up", slope_norm
    if slope_norm <= -config.SLOPE_THRESHOLD:
        return "down", slope_norm
    return None, slope_norm


def displacement_persistence_vote(candles: pd.DataFrame) -> tuple:
    """Vote 3: displacement + magnitude-weighted persistence over
    config.DISPLACEMENT_PERSISTENCE_PERIOD bars, both /ATR_n, summed.
    Returns (vote, score)."""
    n = config.DISPLACEMENT_PERSISTENCE_PERIOD
    if len(candles) < n + 1:
        return None, 0.0
    window = candles["close"].tail(n + 1).reset_index(drop=True)
    atr = _atr(candles, n)
    if atr <= 0:
        return None, 0.0

    displacement = (window.iloc[-1] - window.iloc[0]) / atr

    diffs = window.diff().dropna().reset_index(drop=True)
    streak_sum = 0.0
    last_sign = None
    for d in reversed(diffs.tolist()):
        sign = 1 if d > 0 else (-1 if d < 0 else 0)
        if last_sign is None:
            last_sign = sign
        if sign != last_sign or sign == 0:
            break
        streak_sum += d
        last_sign = sign
    persistence = streak_sum / atr

    score = displacement + persistence
    if score >= config.DISPLACEMENT_PERSISTENCE_THRESHOLD:
        return "up", score
    if score <= -config.DISPLACEMENT_PERSISTENCE_THRESHOLD:
        return "down", score
    return None, score


def _compute_rect_info(candles: pd.DataFrame) -> dict:
    """Build rectangle metadata from the last RECTANGLE_CANDLES rows.

    Returns dict with rect_high, rect_low, zone boundaries, and flags
    indicating whether the interest candle created the extremes, or None
    if not enough candles.
    """
    n = config.RECTANGLE_CANDLES
    if candles.empty or len(candles) < n:
        return None

    window = candles.tail(n).reset_index(drop=True)
    rect_high = float(window["high"].max())
    rect_low = float(window["low"].min())
    zone_size = (rect_high - rect_low) / 5
    zone_20 = rect_high - zone_size
    zone_80 = rect_high - 4 * zone_size

    interest = window.iloc[-1]
    entry = float(interest["close"])

    return {
        "rect_high": rect_high,
        "rect_low": rect_low,
        "zone_20": zone_20,
        "zone_80": zone_80,
        "entry": entry,
        "interest_created_high": bool(interest["high"] == rect_high),
        "interest_created_low": bool(interest["low"] == rect_low),
    }


def _dynamic_atr(candles: pd.DataFrame, period: int = None) -> tuple:
    """Dynamic ATR over the trailing `period` candles (the 5 previous candles,
    interest candle excluded). Returns (atr, mad, dynamic):

      TR_t     = max(H-L, |H-C_{t-1}|, |L-C_{t-1}|)
      ATR      = mean(TR over the window)
      dynamic  = ATR                 # MAD no longer added to the dynamic

    MAD (mean(|TR - ATR|)) is still computed and returned as informational
    only — it no longer feeds the dynamic stop distance.

    Never raises: missing/empty/too-short input or non-finite values degrade
    to (0.0, 0.0, 0.0) instead of throwing.
    """
    n = period or config.SL_ATR_WINDOW
    if candles is None or candles.empty or not {"high", "low", "close"}.issubset(candles.columns):
        return 0.0, 0.0, 0.0
    window = candles.tail(n + 1).reset_index(drop=True)   # row 0 = prior close baseline
    prev_close = window["close"].shift(1)
    tr = pd.concat([
        window["high"] - window["low"],
        (window["high"] - prev_close).abs(),
        (window["low"] - prev_close).abs(),
    ], axis=1).max(axis=1).iloc[1:]   # drop baseline row (no prior close inside the window)
    tr = pd.to_numeric(tr, errors="coerce").dropna()
    if tr.empty:
        return 0.0, 0.0, 0.0
    atr_val = float(tr.mean())
    mad = float((tr - atr_val).abs().mean())
    if not (math.isfinite(atr_val) and math.isfinite(mad)):
        return 0.0, 0.0, 0.0
    return atr_val, mad, atr_val


def _compute_entry_sl(entry: float, direction: str, dynamic: float) -> float:
    """Compute SL from an actual entry price and pre-computed dynamic ATR.

    Called at close time when the real entry price is known.
    """
    if direction == "short":
        return round(entry + dynamic, 6)
    return round(entry - dynamic, 6)


def _compute_sl(symbol: str, direction: str, rect_info: dict, candles: pd.DataFrame) -> dict:
    """
    Compute SL and TP from the Dynamic ATR over the SL_ATR_WINDOW candles
    (interest candle excluded in pre_close, included at close):

      atr  = mean TR over the window
      long -> SL = entry - atr,  TP = entry + atr
      short -> SL = entry + atr,  TP = entry - atr
    """
    atr_value, mad, dynamic = _dynamic_atr(candles, config.SL_ATR_WINDOW)
    entry = rect_info["entry"]

    if direction == "short":
        sl_price = entry + dynamic
        tp_price = entry - dynamic
    else:
        sl_price = entry - dynamic
        tp_price = entry + dynamic

    return {
        "entry": entry,
        "atr": round(atr_value, 6),
        "mad": round(mad, 6),
        "dynamic": round(dynamic, 6),
        "sl": round(sl_price, 6),
        "tp": round(tp_price, 6),
        "sl_method": "localized_atr",
        "tp_method": "atr",
        "rect_high": rect_info["rect_high"],
        "rect_low": rect_info["rect_low"],
        "zone_20": rect_info["zone_20"],
        "zone_80": rect_info["zone_80"],
    }


def decide_direction(symbol: str, candles: pd.DataFrame = None, end_time=None,
                     pre_close: bool = False) -> tuple:
    """
    Runs the 3-vote system to determine direction, then applies the rectangle
    zone filter. If it passes, computes SL and TP from the localized Dynamic
    ATR (R:R always ~1:1).

    When pre_close=True, the forming interest candle is excluded from the candle
    data (end_time is shifted to bucket_start - 1ms). This allows pre-calculating
    SL/TP 15 seconds before the candle closes. The entry price is not yet
    known — run_bot.py plugs it in at close time.

    Returns (direction, candles, levels):
        direction: 'long', 'short', or None (rejected at vote/rect stage)
        levels:    dict with entry, sl, tp, sl_method, tp_method, atr, dynamic,
                   rect bounds (rect_high/rect_low/zone_20/zone_80), or None
                   if direction is None.
    """
    # In pre_close mode, the caller passes end_time = bucket_start - 1ms to
    # exclude the forming interest candle. If no end_time given and pre_close,
    # use current time (candles up to now, excluding the forming bucket).
    if candles is None:
        candles = get_recent_candles(symbol, end_time=end_time)

    min_needed = max(config.ER_PERIOD, config.SLOPE_PERIOD,
                      config.DISPLACEMENT_PERSISTENCE_PERIOD) + 1
    if candles.empty or len(candles) < min_needed:
        print(f"[strategy] {symbol}: not enough candle history ({len(candles)}/{min_needed}) for the vote calcs.")
        return None, candles, None

    er_vote, er_val = efficiency_ratio_vote(candles)
    slope_vote, slope_val = regression_slope_vote(candles)
    disp_vote, disp_val = displacement_persistence_vote(candles)

    votes = [er_vote, slope_vote, disp_vote]
    up_count, down_count = votes.count("up"), votes.count("down")

    print(f"[strategy] {symbol} votes — ER_{config.ER_PERIOD}={er_val:.3f}({er_vote}), "
          f"slope={slope_val:.3f}({slope_vote}), disp+persist={disp_val:.3f}({disp_vote}) "
          f"-> up={up_count} down={down_count}")

    if up_count >= config.MIN_VOTES_AGREE:
        majority = "up"
    elif down_count >= config.MIN_VOTES_AGREE:
        majority = "down"
    else:
        print(f"[strategy] {symbol}: no {config.MIN_VOTES_AGREE}-vote majority, skipping.")
        return None, candles, None

    if config.CONTRARIAN_MODE:
        direction = "short" if majority == "up" else "long"
    else:
        direction = "long" if majority == "up" else "short"

    # ── Rectangle zone filter ─────────────────────────────────────────
    rect_info = _compute_rect_info(candles)
    if rect_info is None:
        print(f"[strategy] {symbol}: not enough candles for rectangle check, skipping.")
        return None, candles, None

    entry = rect_info["entry"]
    print(f"[strategy] {symbol} rect_h={rect_info['rect_high']:.6f} rect_l={rect_info['rect_low']:.6f} "
          f"entry={entry:.6f}  z20={rect_info['zone_20']:.6f} z80={rect_info['zone_80']:.6f}")

    if direction == "short" and entry < rect_info["zone_20"]:
        print(f"[strategy] {symbol}: voted short but entry {entry:.6f} outside top 0-20% zone, skipping.")
        return None, candles, None

    if direction == "long" and entry > rect_info["zone_80"]:
        print(f"[strategy] {symbol}: voted long but entry {entry:.6f} outside bottom 80-100% zone, skipping.")
        return None, candles, None

    print(f"[strategy] {symbol}: majority={majority} -> direction={direction} "
          f"(contrarian={config.CONTRARIAN_MODE}) — entry {entry:.6f} passes rectangle filter")

    # ── SL + TP from the localized Dynamic ATR ──
    levels = _compute_sl(symbol, direction, rect_info, candles)

    levels["symbol"] = symbol
    levels["direction"] = direction
    levels["pre_close"] = pre_close
    print(f"[strategy] {symbol}: {direction.upper()}  entry={entry}  "
          f"sl={levels['sl']}  tp={levels['tp']}  "
          f"(atr={levels['atr']} dynamic={levels['dynamic']})  "
          f"rect=[{levels['rect_low']:.6f}, {levels['rect_high']:.6f}]")
    return direction, candles, levels


if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    direction, candles, levels = decide_direction(sym)
    print(candles[["time", "open", "close"]].tail(10))
    print(f"\nDirection for {sym}: {direction}")
    if levels:
        print(f"Levels: {levels}")
