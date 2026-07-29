"""
config.py
=========
Single source of truth for every module in the pipeline. Nothing else in
this project should hard-code a path, threshold, or credential — import it
from here so tuning the system means editing one file.

API keys are read from environment variables (or a local .env file loaded
below), never hard-coded — don't commit real keys.
"""

import os
from pathlib import Path

# Optional .env support (pip install python-dotenv). Silently skipped if
# the package or file isn't present.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("LIQ_DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

LIQ_CSV           = DATA_DIR / "liquidations.csv"          # written by liq_stream.py
LIQ_BUCKET_STATE  = DATA_DIR / "liq_bucket_state.json"      # per-symbol streak counters, survives restarts
LIQ_SIGNALS_FILE  = DATA_DIR / "liq_signals.jsonl"          # written by liq_bucket.py, tailed by run_bot.py
TRADE_LOG_FILE    = DATA_DIR / "trade_log.jsonl"            # every decision + execution, for audit/debug
PERFORMANCE_CSV   = DATA_DIR / "performance.csv"            # written by trader.py, read by performance_plot.py

# ── Liquidation stream (liq_stream.py) ───────────────────────────────────────
WS_URL       = "wss://fstream.binance.com/market/ws/!forceOrder@arr"
MIN_USD_SIZE = 10   # filter out dust liquidations from the terminal print (CSV logs everything regardless)

# ── Bucketing / signal detection (liq_bucket.py) ─────────────────────────────
BUCKET_MINUTES      = 3     # width of each time bucket
SILENCE_THRESHOLD    = 5     # consecutive silent buckets -> next active bucket fires "activity_spike"
ACTIVITY_THRESHOLD   = 5     # consecutive active buckets  -> next silent bucket fires "collapse"
BUCKET_POLL_SECONDS  = 15    # how often liq_bucket.py re-scans the CSV for the current bucket
TRIGGER_SIGNAL       = "activity_spike"   # which signal type the strategy engine acts on
MIN_ACTIVITY_USD     = 1000  # a bucket only counts as "active" if its total liquidation $ >= this —
                              # a bucket with liquidations under this stays "silent" for streak purposes

# Two different notations for the same "3 minutes", used in different places:
#   EXCHANGE_TIMEFRAME — Binance/ccxt notation ("3m") — for fetch_ohlcv() calls
#   PANDAS_FREQ        — pandas offset alias ("3min") — for .resample() / .floor()
# Mixing these up raises "Invalid frequency" (pandas) or a timeframe error (ccxt).
EXCHANGE_TIMEFRAME  = f"{BUCKET_MINUTES}m"
PANDAS_FREQ          = f"{BUCKET_MINUTES}min"

# ── Strategy (strategy.py) — 3-vote weighted-majority direction ─────────────
STRATEGY_TIMEFRAME  = EXCHANGE_TIMEFRAME   # ccxt notation — passed straight to fetch_ohlcv
LOOKBACK_HOURS       = 4       # how much history to pull for the vote calcs
                                # (4h @ 3min = 80 candles)
CONTRARIAN_MODE      = True    # True = bet OPPOSITE the 2-of-3 majority (liquidation-silence
                                # reversal logic). False = bet WITH the majority.

# Vote 1 — Kaufman's Efficiency Ratio over n=6 bars: |Close[t]-Close[t-6]| /
# sum(|bar-to-bar changes|) over those 6 bars. 1.0 = a straight-line move,
# near 0 = pure chop. Direction = sign of Close[t]-Close[t-6].
ER_PERIOD           = 6
ER_THRESHOLD         = 0.65    # below this -> no-signal (chop, not a real trend)

# Vote 2 — least-squares regression slope of Close over the last n=6 bars,
# normalized by ATR_6 (slope / ATR) so it's comparable across symbols/prices.
SLOPE_PERIOD         = 6
SLOPE_THRESHOLD      = 0.25    # >= +0.25 -> uptrend, <= -0.25 -> downtrend, else no-signal

# Vote 3 — displacement + persistence composite over n=6 bars, both ATR-
# normalized, summed:
#   displacement = (Close[t] - Close[t-6]) / ATR_6
#   persistence  = sum of the signed bar-to-bar moves within the CURRENT
#                  same-direction streak (magnitude-weighted, not just a
#                  streak count), / ATR_6
#   score = displacement + persistence
DISPLACEMENT_PERSISTENCE_PERIOD    = 6
DISPLACEMENT_PERSISTENCE_THRESHOLD = 4.0   # >= +4 -> uptrend, <= -4 -> downtrend, else no-signal

MIN_VOTES_AGREE      = 2       # need at least 2 of the 3 votes agreeing to call a direction —
                                # otherwise (no majority, or majority is "no-signal") -> skip

# ── Timing — limit order priced at the interest candle's exact close ───────
# T = the interest (silence/spike) candle's CLOSE time = bucket_start + BUCKET_MINUTES.
# From T, there is ENTRY_TIMEOUT_SECONDS total to: compute all 3 votes (with
# retries on fetch failure), compute allocation levels, place a LIMIT order at
# that exact close price, and get it filled. The instant it fills, everything
# stops. If the deadline (T + ENTRY_TIMEOUT_SECONDS) passes unfilled, the
# order is cancelled and the trade is abandoned — no trade taken.
ENTRY_TIMEOUT_SECONDS   = 60
ORDER_POLL_SECONDS      = 1     # how often to check fill status within that window

# ── Rectangle strategy (strategy.py + allocation.py) ──────────────────────────
RECTANGLE_CANDLES = 6           # 5 previous 3min candles + 1 interest candle
SL_BUFFER_POINTS = 0.0005       # points buffer beyond rectangle high/low when interest candle didn't create the extreme
ATR_SL_MULT_RECT = 1.5          # ATR multiplier for SL when interest candle created the extreme
ATR_TP_MULT_RECT = 2.5          # ATR multiplier for TP candidate
MIN_RR = 1.0                    # minimum acceptable risk:reward (overrides MIN_RISK_REWARD for rectangle trades)

# ── Allocation / risk (allocation.py) ────────────────────────────────────────
ANOMALY_CHART_TF        = EXCHANGE_TIMEFRAME   # ccxt notation — anomaly.py normalizes internally for resample
ATR_PERIOD               = 14
ATR_TP_FALLBACK_MULT     = 2.5   # TP distance in ATRs when no uncleared zone exists in the TP direction
ATR_SL_FALLBACK_MULT     = 1.5   # SL buffer in ATRs added beyond the 6-candle trend high/low fallback
MIN_SL_ZONE_ATR_MULT     = 0.5   # an opposite-side zone closer than this many ATRs is too tight to use as SL
PREV_TREND_CANDLES       = 6     # "trend of the 6 candles before" used for the SL fallback
MIN_RISK_REWARD          = 1.0   # trades below this R:R are skipped entirely — never executed

# ── Trader / execution (trader.py) ───────────────────────────────────────────
SYMBOL_QUOTE           = "USDT"
TESTNET                = os.getenv("LIQ_TESTNET", "true").lower() == "true"   # default to testnet — flip explicitly for live
RISK_PER_TRADE_PCT     = float(os.getenv("LIQ_RISK_PCT", "0.5"))   # % of account equity risked per trade
LEVERAGE                = int(os.getenv("LIQ_LEVERAGE", "5"))
POSITION_POLL_SECONDS   = 5     # how often trader.py polls open-position PnL
MAX_CONCURRENT_POSITIONS = 1     # "one position open at a time"

BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")