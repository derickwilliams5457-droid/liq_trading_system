"""
allocation.py  —  Stage 4 of the pipeline
===========================================
Given a direction from strategy.py, computes the stop-loss and take-profit.

TP: the nearest uncleared anomaly zone in the direction of the bet (above
    price for a long, below price for a short) — price only needs to touch
    the near edge of the zone, so that edge is the TP trigger. If no zone
    exists on that side, fall back to an ATR multiple.

SL: the nearest uncleared zone on the OPPOSITE side of price. A zone closer
    than config.MIN_SL_ZONE_ATR_MULT * ATR is too tight to be a usable stop,
    so it's ignored. If no usable opposite-side zone exists at all, fall back
    to the high/low of the previous config.PREV_TREND_CANDLES candles,
    pushed out by an ATR buffer.
"""

import pandas as pd

import config


def atr(candles: pd.DataFrame, period: int = None) -> float:
    """Classic Wilder ATR from OHLC candles (needs high/low/close columns)."""
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


def _nearest_zone(zones: list[dict], price: float, side: str) -> dict | None:
    """side='above' -> zones with mid > price, side='below' -> zones with mid < price.
    Returns the zone whose near edge is closest to price, or None."""
    candidates = [z for z in zones if (z["mid"] > price if side == "above" else z["mid"] < price)]
    if not candidates:
        return None
    if side == "above":
        return min(candidates, key=lambda z: z["low"] - price)
    return min(candidates, key=lambda z: price - z["high"])


def compute_levels(symbol: str, direction: str, price: float, candles: pd.DataFrame,
                    zones: list[dict], prev_trend_candles: pd.DataFrame = None) -> dict:
    """
    direction: 'long' or 'short'
    candles: recent OHLC candles (used for ATR); should include at least
             config.ATR_PERIOD + 1 bars for a real ATR — pad the fetch in
             run_bot.py if needed.
    zones: output of anomaly.get_uncleared_zones()
    prev_trend_candles: the config.PREV_TREND_CANDLES candles before the
             signal, used only as the SL fallback. Defaults to `candles`.
    """
    prev_trend_candles = prev_trend_candles if prev_trend_candles is not None else candles
    atr_value = atr(candles)

    tp_side = "above" if direction == "long" else "below"

    tp_zone = _nearest_zone(zones, price, tp_side)

    # SL: only consider opposite-side zones that clear the minimum distance —
    # a too-tight nearest zone shouldn't block a further-out usable one.
    def sl_edge_dist(z):
        edge = z["high"] if direction == "long" else z["low"]
        return abs(price - edge)

    sl_candidates = [z for z in zones if (z["mid"] < price if direction == "long" else z["mid"] > price)]
    sl_candidates = [z for z in sl_candidates if sl_edge_dist(z) >= config.MIN_SL_ZONE_ATR_MULT * atr_value]
    sl_zone = min(sl_candidates, key=sl_edge_dist) if sl_candidates else None

    # ── Take profit ──────────────────────────────────────────────────────
    if tp_zone:
        tp_price = tp_zone["low"] if direction == "long" else tp_zone["high"]
        tp_method = "uncleared_zone"
    else:
        tp_price = (price + config.ATR_TP_FALLBACK_MULT * atr_value if direction == "long"
                    else price - config.ATR_TP_FALLBACK_MULT * atr_value)
        tp_method = "atr_fallback"

    # ── Stop loss ────────────────────────────────────────────────────────
    if sl_zone:
        sl_price = sl_zone["high"] if direction == "long" else sl_zone["low"]
        sl_method = "uncleared_zone"
    else:
        trend_low = prev_trend_candles["low"].min()
        trend_high = prev_trend_candles["high"].max()
        buffer = config.ATR_SL_FALLBACK_MULT * atr_value
        sl_price = (trend_low - buffer) if direction == "long" else (trend_high + buffer)
        sl_method = "atr_trend_fallback"

    risk = abs(price - sl_price)
    reward = abs(tp_price - price)
    rr = round(reward / risk, 2) if risk > 0 else None

    return {
        "symbol": symbol,
        "direction": direction,
        "entry": price,
        "atr": round(atr_value, 6),
        "sl": round(sl_price, 6),
        "tp": round(tp_price, 6),
        "sl_method": sl_method,
        "tp_method": tp_method,
        "risk_reward": rr,
    }
