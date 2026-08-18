"""
allocation.py  —  Stage 4 of the pipeline (rectangle-based)
=============================================================
Given a direction + rect_info from strategy.py, computes the SL using the
LOCALIZED Dynamic ATR — the only stop-loss logic in the system:

  SL:
    - ATR  = mean True Range over the 6 previous candles (interest excluded)
    - dynamic = ATR   (MAD no longer added)
    - long  -> SL = entry - dynamic
    - short -> SL = entry + dynamic

  TP:
    - long  -> TP = entry + ATR
    - short -> TP = entry - ATR
    - R:R is always ~1:1 by construction.

NOTE: this module is not imported by the live pipeline (run_bot.py calls
strategy.decide_direction() directly); it is kept as the documented Stage 4
reference and mirrors strategy._compute_sl() exactly.
"""

import math

import pandas as pd

import config


def dynamic_atr(candles: pd.DataFrame, period: int = None) -> tuple:
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


def compute_levels(symbol: str, direction: str, price: float, candles: pd.DataFrame,
                    zones: list[dict] = None, prev_trend_candles: pd.DataFrame = None,
                    rect_info: dict = None) -> dict:
    """
    direction: 'long' or 'short'
    candles: OHLC DataFrame (used for the localized Dynamic ATR)
    rect_info: output of strategy.decide_direction() — rect bounds; only used
               here as a pass-through signal that a rectangle trade is live.
    """
    # ── Stop loss ────────────────────────────────────────────────────────
    if rect_info is None:
        return {
            "symbol": symbol, "direction": direction, "entry": price,
            "atr": None, "mad": None, "sl": None, "tp": None,
            "sl_method": "no_rect_info", "tp_method": "atr",
        }

    atr_value, mad, dynamic = dynamic_atr(candles, config.SL_ATR_WINDOW)

    if direction == "short":
        sl_price = price + dynamic
        tp_price = price - atr_value
    else:
        sl_price = price - dynamic
        tp_price = price + atr_value

    return {
        "symbol": symbol,
        "direction": direction,
        "entry": price,
        "atr": round(atr_value, 6),
        "mad": round(mad, 6),
        "sl": round(sl_price, 6),
        "tp": round(tp_price, 6),
        "sl_method": "localized_atr",
        "tp_method": "atr",
    }
