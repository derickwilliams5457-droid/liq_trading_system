"""
strategy.py  —  Stage 3 of the pipeline
=========================================
Direction is determined by the 3-vote weighted-majority system (same as
before).  The rectangle is an *additional filter*: once the votes decide a
direction, the last 6 candles are examined to build a rectangle from highest
high to lowest low, divided into 5 equal zones (0% = high → 100% = low).

A trade is only allowed when the entry close lands in the *extreme* zones
matching the voted direction:
    short  → entry must be in the top    0-20%  zone
    long   → entry must be in the bottom 80-100% zone
If entry sits in the middle 20-80% zone the trade is skipped regardless of
what the votes say.

If the rectangle filter passes, the R:R system computes SL/TP using the
rectangle framework and enforces config.MIN_RR. Everything is returned in
a single `levels` dict so there's no redundant computation in allocation.py.

Returns (direction, candles, levels):
    direction: 'long', 'short', or None (rejected at vote/rect/RR stage)
    levels:    dict with entry, sl, tp, sl_method, tp_method, risk_reward, atr
              or None if direction is None.
"""

import ccxt
import numpy as np
import pandas as pd

import config

_exchange = None


def get_exchange():
    global _exchange
    if _exchange is None:
        _exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})
    return _exchange


def get_recent_candles(symbol: str, end_time=None, hours: float = None) -> pd.DataFrame:
    """Fetch config.LOOKBACK_HOURS of config.STRATEGY_TIMEFRAME candles."""
    hours = hours if hours is not None else config.LOOKBACK_HOURS
    n = int(hours * 60 / config.BUCKET_MINUTES) + 10

    ex = get_exchange()
    ohlcv = ex.fetch_ohlcv(symbol, config.STRATEGY_TIMEFRAME, limit=n)
    df = pd.DataFrame(ohlcv, columns=["time", "open", "high", "low", "close", "volume"])
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)

    if end_time is not None:
        end_ts = pd.Timestamp(end_time)
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize("UTC")
        print(f"[strategy] fetching ~{n} candles for {symbol} @ {config.STRATEGY_TIMEFRAME}, "
              f"pinned to <= {end_ts}")
        df = df[df["time"] <= end_ts].reset_index(drop=True)
    else:
        print(f"[strategy] fetching ~{n} candles for {symbol} @ {config.STRATEGY_TIMEFRAME}")

    print(f"[strategy] got {len(df)} candle(s) for {symbol}, "
          f"latest close={df['close'].iloc[-1] if not df.empty else 'n/a'}")
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


def _vwap(candles: pd.DataFrame) -> float:
    """VWAP over the given candle set."""
    typical = (candles["high"] + candles["low"] + candles["close"]) / 3
    vol = candles["volume"]
    if vol.sum() == 0:
        return float(candles["close"].mean())
    return float((typical * vol).sum() / vol.sum())


def _compute_rr(symbol: str, direction: str, rect_info: dict, candles: pd.DataFrame) -> dict | None:
    """
    Compute SL, TP, and R:R from the rectangle framework.
    Returns levels dict if R:R >= MIN_RR, or None if skipped.
    """
    atr_value = _atr(candles, config.ATR_PERIOD)
    entry = rect_info["entry"]

    # ── Stop loss ────────────────────────────────────────────────────
    if direction == "short":
        if not rect_info["interest_created_high"]:
            sl_price = rect_info["rect_high"] + config.SL_BUFFER_POINTS
            sl_method = "rect_buffer"
        else:
            sl_price = entry + config.ATR_SL_MULT_RECT * atr_value
            sl_method = "rect_atr"
    else:
        if not rect_info["interest_created_low"]:
            sl_price = rect_info["rect_low"] - config.SL_BUFFER_POINTS
            sl_method = "rect_buffer"
        else:
            sl_price = entry - config.ATR_SL_MULT_RECT * atr_value
            sl_method = "rect_atr"

    # ── Take profit candidates ───────────────────────────────────────
    window = candles.tail(config.RECTANGLE_CANDLES) if len(candles) >= config.RECTANGLE_CANDLES else candles
    vwap_price = _vwap(window)

    if direction == "short":
        atr_tp = entry - config.ATR_TP_MULT_RECT * atr_value
        candidates = []
        if vwap_price < entry and vwap_price >= rect_info["rect_low"]:
            candidates.append(("vwap", vwap_price))
        if atr_tp < entry and atr_tp >= rect_info["rect_low"]:
            candidates.append(("atr", atr_tp))
    else:
        atr_tp = entry + config.ATR_TP_MULT_RECT * atr_value
        candidates = []
        if vwap_price > entry and vwap_price <= rect_info["rect_high"]:
            candidates.append(("vwap", vwap_price))
        if atr_tp > entry and atr_tp <= rect_info["rect_high"]:
            candidates.append(("atr", atr_tp))

    if not candidates:
        print(f"  [strategy] {symbol}: no TP candidate inside rectangle, skipping.")
        return None

    # Pick best R:R
    best_rr = -1
    best_tp = None
    best_method = None
    for method, tp_candidate in candidates:
        risk = abs(entry - sl_price)
        reward = abs(tp_candidate - entry)
        rr = reward / risk if risk > 0 else 0
        if rr > best_rr:
            best_rr = rr
            best_tp = tp_candidate
            best_method = method

    rr = round(best_rr, 2)

    if rr < config.MIN_RR:
        print(f"  [strategy] {symbol}: R:R {rr} < MIN_RR ({config.MIN_RR}), skipping.")
        return None

    return {
        "entry": entry,
        "atr": round(atr_value, 6),
        "sl": round(sl_price, 6),
        "sl_method": sl_method,
        "tp": round(best_tp, 6),
        "tp_method": best_method,
        "risk_reward": rr,
    }


def decide_direction(symbol: str, candles: pd.DataFrame = None, end_time=None) -> tuple:
    """
    Runs the 3-vote system to determine direction, then applies the rectangle
    zone filter. If it passes, computes R:R and enforces MIN_RR.

    Returns (direction, candles, levels):
        direction: 'long', 'short', or None (rejected at vote/rect/RR stage)
        levels:    dict with entry, sl, tp, sl_method, tp_method, risk_reward, atr
                  or None if direction is None.
    """
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
    created_h = rect_info["interest_created_high"]
    created_l = rect_info["interest_created_low"]
    print(f"[strategy] {symbol} rect_h={rect_info['rect_high']:.6f} rect_l={rect_info['rect_low']:.6f} "
          f"entry={entry:.6f}  z20={rect_info['zone_20']:.6f} z80={rect_info['zone_80']:.6f}  "
          f"interest_created_high={created_h}  interest_created_low={created_l}")

    if created_h or created_l:
        print(f"[strategy] {symbol}: interest candle created rectangle extreme, skipping.")
        return None, candles, None

    if direction == "short" and entry < rect_info["zone_20"]:
        print(f"[strategy] {symbol}: voted short but entry {entry:.6f} outside top 0-20% zone, skipping.")
        return None, candles, None

    if direction == "long" and entry > rect_info["zone_80"]:
        print(f"[strategy] {symbol}: voted long but entry {entry:.6f} outside bottom 80-100% zone, skipping.")
        return None, candles, None

    print(f"[strategy] {symbol}: majority={majority} -> direction={direction} "
          f"(contrarian={config.CONTRARIAN_MODE}) — entry {entry:.6f} passes rectangle filter")

    # ── R:R system ──────────────────────────────────────────────────
    levels = _compute_rr(symbol, direction, rect_info, candles)
    if levels is None:
        return None, candles, None

    levels["symbol"] = symbol
    levels["direction"] = direction
    print(f"[strategy] {symbol}: {direction.upper()}  entry={entry}  "
          f"sl={levels['sl']} ({levels['sl_method']})  "
          f"tp={levels['tp']} ({levels['tp_method']})  R:R={levels['risk_reward']}")
    return direction, candles, levels


if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    direction, candles, levels = decide_direction(sym)
    print(candles[["time", "open", "close"]].tail(10))
    print(f"\nDirection for {sym}: {direction}")
    if levels:
        print(f"Levels: {levels}")
