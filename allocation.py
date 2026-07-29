"""
allocation.py  —  Stage 4 of the pipeline (rectangle-based)
=============================================================
Given a direction + rect_info from strategy.py, computes SL and TP using the
rectangle framework:

  TP candidates:
    - VWAP of the 6-candle rectangle window
    - ATR-based extension (entry ± ATR_TP_MULT_RECT * ATR)
    Only candidates inside the rectangle are considered; the one with the
    best R:R is chosen.

  SL:
    - If the interest candle DID NOT create the rectangle extreme (high for
      shorts, low for longs): SL = rect extreme ± SL_BUFFER_POINTS
    - If the interest candle DID create the extreme: SL = entry ±
      ATR_SL_MULT_RECT * ATR (3min-ATR-based)

  Minimum R:R of config.MIN_RR (1.0) is enforced.
"""

import pandas as pd

import config


def atr(candles: pd.DataFrame, period: int = None) -> float:
    """Classic Wilder ATR from OHLC candles."""
    period = period or config.ATR_PERIOD
    high, low, close = candles["high"], candles["low"], candles["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    n = min(period, len(tr.dropna()))
    if n == 0:
        return float(tr.iloc[-1]) if len(tr) else 0.0
    return float(tr.tail(n).mean())


def _vwap(candles: pd.DataFrame) -> float:
    """VWAP over the given candle set."""
    typical = (candles["high"] + candles["low"] + candles["close"]) / 3
    vol = candles["volume"]
    if vol.sum() == 0:
        return float(candles["close"].mean())
    return float((typical * vol).sum() / vol.sum())


def compute_levels(symbol: str, direction: str, price: float, candles: pd.DataFrame,
                    zones: list[dict], prev_trend_candles: pd.DataFrame = None,
                    rect_info: dict = None) -> dict:
    """
    direction: 'long' or 'short'
    candles: OHLC DataFrame (used for ATR; tail(6) used for VWAP)
    rect_info: output of strategy.decide_direction() — rectangle bounds and
               interest-candle extremity flags.
    """
    atr_value = atr(candles)

    # ── Stop loss ────────────────────────────────────────────────────────
    if rect_info is None:
        return {
            "symbol": symbol, "direction": direction, "entry": price,
            "atr": round(atr_value, 6), "sl": None, "tp": None,
            "sl_method": "no_rect_info", "tp_method": "no_rect_info",
            "risk_reward": None,
        }

    rect_high = rect_info["rect_high"]
    rect_low = rect_info["rect_low"]

    if direction == "short":
        if not rect_info["interest_created_high"]:
            sl_price = rect_high + config.SL_BUFFER_POINTS
            sl_method = "rect_buffer"
        else:
            sl_price = price + config.ATR_SL_MULT_RECT * atr_value
            sl_method = "rect_atr"
    else:
        if not rect_info["interest_created_low"]:
            sl_price = rect_low - config.SL_BUFFER_POINTS
            sl_method = "rect_buffer"
        else:
            sl_price = price - config.ATR_SL_MULT_RECT * atr_value
            sl_method = "rect_atr"

    # ── Take profit candidates ───────────────────────────────────────────
    window = candles.tail(config.RECTANGLE_CANDLES) if len(candles) >= config.RECTANGLE_CANDLES else candles
    vwap_price = _vwap(window)

    if direction == "short":
        atr_tp = price - config.ATR_TP_MULT_RECT * atr_value
        candidates = []
        if vwap_price < price and vwap_price >= rect_low:
            candidates.append(("vwap", vwap_price))
        if atr_tp < price and atr_tp >= rect_low:
            candidates.append(("atr", atr_tp))
    else:
        atr_tp = price + config.ATR_TP_MULT_RECT * atr_value
        candidates = []
        if vwap_price > price and vwap_price <= rect_high:
            candidates.append(("vwap", vwap_price))
        if atr_tp > price and atr_tp <= rect_high:
            candidates.append(("atr", atr_tp))

    if not candidates:
        print(f"  [allocation] {symbol}: no valid TP inside rectangle, skipping.")
        return {
            "symbol": symbol, "direction": direction, "entry": price,
            "atr": round(atr_value, 6), "sl": round(sl_price, 6),
            "tp": None, "sl_method": sl_method, "tp_method": "none_inside_rect",
            "risk_reward": None,
        }

    # Pick the candidate with the best R:R
    best_rr = -1
    best_tp = None
    best_method = None
    for method, tp_candidate in candidates:
        risk = abs(price - sl_price)
        reward = abs(tp_candidate - price)
        rr = reward / risk if risk > 0 else 0
        if rr > best_rr:
            best_rr = rr
            best_tp = tp_candidate
            best_method = method

    # ── Enforce minimum R:R ──────────────────────────────────────────────
    if best_rr < config.MIN_RR:
        print(f"  [allocation] {symbol}: best R:R {best_rr:.2f} < {config.MIN_RR}, skipping.")
        return {
            "symbol": symbol, "direction": direction, "entry": price,
            "atr": round(atr_value, 6), "sl": round(sl_price, 6),
            "tp": round(best_tp, 6), "sl_method": sl_method, "tp_method": best_method,
            "risk_reward": round(best_rr, 2),
        }

    return {
        "symbol": symbol,
        "direction": direction,
        "entry": price,
        "atr": round(atr_value, 6),
        "sl": round(sl_price, 6),
        "tp": round(best_tp, 6),
        "sl_method": sl_method,
        "tp_method": best_method,
        "risk_reward": round(best_rr, 2),
    }
