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
TRIGGER_SIGNAL       = "collapse"   # which signal type the strategy engine acts on

# Two different notations for the same "3 minutes", used in different places:
#   EXCHANGE_TIMEFRAME — Binance/ccxt notation ("3m") — for fetch_ohlcv() calls
#   PANDAS_FREQ        — pandas offset alias ("3min") — for .resample() / .floor()
# Mixing these up raises "Invalid frequency" (pandas) or a timeframe error (ccxt).
EXCHANGE_TIMEFRAME  = f"{BUCKET_MINUTES}m"
PANDAS_FREQ          = f"{BUCKET_MINUTES}min"

# ── Strategy (strategy.py) ───────────────────────────────────────────────────
STRATEGY_TIMEFRAME  = EXCHANGE_TIMEFRAME   # ccxt notation — passed straight to fetch_ohlcv
TREND_CANDLES        = 5      # "previous 5 candles"
INTEREST_CANDLES     = 1      # "+1 interest candle" (the candle the signal fired on)
CONTRARIAN_MODE      = True   # True = bet OPPOSITE the recent trend (liquidation-silence reversal logic)
                                # False = bet WITH the trend (continuation logic)

# ── Allocation / risk (allocation.py) ────────────────────────────────────────
ANOMALY_CHART_TF        = EXCHANGE_TIMEFRAME   # ccxt notation — anomaly.py normalizes internally for resample
ATR_PERIOD               = 14
ATR_TP_FALLBACK_MULT     = 2.5   # TP distance in ATRs when no uncleared zone exists in the TP direction
ATR_SL_FALLBACK_MULT     = 1.5   # SL buffer in ATRs added beyond the 6-candle trend high/low fallback
MIN_SL_ZONE_ATR_MULT     = 0.5   # an opposite-side zone closer than this many ATRs is too tight to use as SL
PREV_TREND_CANDLES       = 6     # "trend of the 6 candles before" used for the SL fallback
MIN_RISK_REWARD         = 1.0

# ── Trader / execution (trader.py) ───────────────────────────────────────────
SYMBOL_QUOTE           = "USDT"
TESTNET                = os.getenv("LIQ_TESTNET", "true").lower() == "true"   # default to testnet — flip explicitly for live
RISK_PER_TRADE_PCT     = float(os.getenv("LIQ_RISK_PCT", "1.5"))   # % of account equity risked per trade
LEVERAGE                = int(os.getenv("LIQ_LEVERAGE", "5"))
POSITION_POLL_SECONDS   = 5     # how often trader.py polls open-position PnL
MAX_CONCURRENT_POSITIONS = 1     # "one position open at a time"

BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

