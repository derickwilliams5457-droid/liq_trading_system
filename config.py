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
TRIGGER_SIGNAL       = "activity_spike"   # which signal type the strategy engine acts on (post-close fallback)
PRE_SPIKE_SIGNAL     = "pre_activity_spike"   # emitted 15s before candle close for pre-calculation
MIN_ACTIVITY_USD     = 1000  # a bucket only counts as "active" if its LARGEST SINGLE liquidation >= this —
                              # cumulative volume doesn't matter; ten $100 liqs adding to $1,000 stay "silent"

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
#
# FALLBACK flow (post-close, TRIGGER_SIGNAL): from T there is
# ENTRY_TIMEOUT_SECONDS total to: compute all 3 votes (with retries on fetch
# failure), compute allocation levels, place a LIMIT order at that exact close
# price, and get it filled. The instant it fills, everything stops. If the
# deadline passes unfilled, the order is cancelled and the trade is abandoned.
#
# PRE-CALC flow (PRE_SPIKE_SIGNAL): votes, SL and TP are computed BEFORE close
# (while the interest candle is forming). At close only the actual close price
# + R:R recompute + limit order placement remain, so the window into the new
# candle is much tighter — PRE_CALC_EXECUTE_SECONDS. Once R:R passes, execute
# immediately; never sit on the 60s budget.
ENTRY_TIMEOUT_SECONDS   = 60
PRE_CALC_EXECUTE_SECONDS = 10   # max seconds INTO the new candle to recompute R:R and fill the limit order
PRE_CLOSE_SECONDS       = 15    # the pre-spike signal is designed to arrive ~15s before close
ORDER_POLL_SECONDS      = 1     # how often to check fill status within that window

# ── Rectangle strategy (strategy.py + allocation.py) ──────────────────────────
RECTANGLE_CANDLES = 5           # 5 previous 3min candles (interest candle excluded)
MIN_RR = 1.0                    # minimum acceptable risk:reward (overrides MIN_RISK_REWARD for rectangle trades)

# ── Localized Dynamic-ATR stop loss (strategy.py + allocation.py) ─────────────
# The ONLY stop-loss logic in the system. SL distance = Dynamic ATR over the
# 5+1 candle window: ATR = mean(TR), dynamic = ATR (MAD is computed/reported
# for reference only and is no longer added). Applied as entry - dynamic
# (long) / entry + dynamic (short).
SL_ATR_WINDOW = RECTANGLE_CANDLES   # 5 previous candles (interest candle excluded)

# ── Allocation / risk (allocation.py) ────────────────────────────────────────
ANOMALY_CHART_TF        = EXCHANGE_TIMEFRAME   # ccxt notation — anomaly.py normalizes internally for resample
ATR_TP_FALLBACK_MULT     = 2.5   # TP distance in ATRs when no uncleared zone exists in the TP direction
MIN_RISK_REWARD          = 1.0   # trades below this R:R are skipped entirely — never executed

# ── Performance / speculative precompute (run_bot.py + hratmap.py) ──────────
# The 3m aggTrades window is fetched in slices (each slice < 1h is the Binance
# limit for startTime+endTime together). Slices are fetched with a single
# worker AND serialized by the global one-at-a-time lock in ratelimit.py, so
# the bot always polls Binance strictly one request at a time.
SLICE_MINUTES                = 6     # aggTrades slice size in minutes
MAX_WORKERS                  = 1     # concurrent slice fetchers (serialized by the global rate limiter)
# hratmap pulls aggTrades for the TP-zone window only — the prior 9 completed
# candles (9 x 3m = 27min). Smaller than the full lookback, so the expensive
# aggTrades pull finishes well inside the pre-close budget.
HRATMAP_LOOKBACK_CANDLES     = 9
# A symbol SILENCE_THRESHOLD+ silent buckets deep is one active bucket away
# from firing "activity_spike" — keep its zone window warm in the background
# so the trigger finds the slow aggTrades pull already done.
WARM_SYMBOLS                 = 1     # how many on-deck symbols to precompute for
WARM_POLL_SECONDS            = 15    # how often the warmer re-checks bucket state
MAX_CONCURRENT_ZONE_FETCHES  = 1     # cap on simultaneous hratmap aggTrades pulls

# ── Shared market-data cache (data_cache.py) ────────────────────────────────
# Strategy's 3m candles and hratmap's zone-window candles are the SAME stream —
# cached once (tag "klines:3m") and shared, so neither re-polls Binance for
# data the other already fetched. run_bot.py also evicts the per-symbol
# entries after each verdict; the TTL below is just a safety net.
CANDLE_CACHE_TTL             = 300    # seconds a cached candle batch stays fresh

# ── Global Binance rate limiting (ratelimit.py) ──────────────────────────────
# Binance USD-M caps the whole IP at 2400 request-weight/min and 418-bans the
# IP when exceeded. This token bucket is the single guard for every REST call
# in the process — keep the cap comfortably below 2400 so we can never earn a
# ban, no matter how many modules ask at once.
RATE_LIMIT_WEIGHT_PER_MIN = 1800    # global budget: aggTrades=20, klines=2, exchangeInfo=1

# ── Trader / execution (trader.py) ───────────────────────────────────────────
SYMBOL_QUOTE           = "USDT"
TESTNET                = os.getenv("LIQ_TESTNET", "true").lower() == "true"   # default to testnet — flip explicitly for live
RISK_PER_TRADE_PCT     = float(os.getenv("LIQ_RISK_PCT", "0.5"))   # % of account equity risked per trade
LEVERAGE                = int(os.getenv("LIQ_LEVERAGE", "5"))
POSITION_POLL_SECONDS   = 5     # how often trader.py polls open-position PnL
POSITION_CLOSED_CONFIRM_POLLS = 2  # consecutive polls with no position before a trade is declared closed
MAX_CONCURRENT_POSITIONS = 3     # multiple positions open at a time (multi-coin execution)

BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")