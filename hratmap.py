#!/usr/bin/env python3
"""
liq_heatmap_chart.py

Builds an interactive HTML chart (Plotly, dark theme) showing:

  1. Candlesticks for the chosen symbol/interval/lookback.
  2. Synthetic liquidation levels as horizontal rays: each candle in the
     window is used as an anchor (hypothetical leveraged entry at that
     candle's close), levels are computed across a set of leverage tiers,
     and each ray runs from its anchor candle forward until a LATER candle
     actually trades through that price (the level "breaks"/liquidates) --
     or to the edge of the chart if it never breaks.
  3. Ray color + thickness both driven by the REAL notional traded in that
     ray's time/price box, pulled from Binance aggTrades (not a proxy).
     Rays below a notional floor (default 10,000 USDT) are dropped entirely.
  4. A synced subplot below showing futures vs spot notional volume per
     candle across the same window.

This is still a synthetic/illustrative liquidation model, not real position
data -- see liq_levels.py's docstring for the same caveat. Maintenance-margin
ratios come from the built-in approximation table (MMR_APPROX); no signed
exchange call is made for leverage brackets, so no API keys are needed.

Usage:
    python3 liq_heatmap_chart.py              # prompt for symbol -> print uncleared zones (3m / 75x / 12 / 1000 USDT)
    python3 liq_heatmap_chart.py BTCUSDT      # print uncleared zones for BTCUSDT (same fixed settings)
    python3 liq_heatmap_chart.py BTCUSDT --chart --interval 15m --lookback 200 --notional-floor 25000  # HTML chart

Requires: pip install requests plotly
"""
import argparse
import bisect
import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

import config
import data_cache
import ratelimit

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.colors as pcolors

BASE = "https://fapi.binance.com"
SPOT_BASE = "https://api.binance.com"
TIMEOUT = 10


def print_progress(prefix, pct, bar_len=30):
    pct = max(0.0, min(100.0, pct))
    filled = int(bar_len * pct / 100)
    bar = "#" * filled + "-" * (bar_len - filled)
    print(f"\r{prefix} [{bar}] {pct:5.1f}%", end="", flush=True)
    if pct >= 100:
        print()


LEVERAGE_TIERS = [5, 10, 20, 25, 50, 75, 100]
MMR_APPROX = {5: 0.010, 10: 0.005, 20: 0.005, 25: 0.005, 50: 0.004, 75: 0.0065, 100: 0.005}

VALID_INTERVALS = {
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
}


# --------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------

def get_json(base, path, params=None, weight=ratelimit.WEIGHT_KLINES):
    """Rate-limited, serialized GET. Every request goes through the global
    one-at-a-time lock + weight bucket (ratelimit.request), so the process can
    never exceed the Binance IP budget; on 418/429 the request records the ban
    and raises BinanceBanned instead of retry-looping into it."""
    return ratelimit.request("GET", base + path, params=params, weight=weight)


def get_price_precision(symbol):
    """Cached per-process via the shared data cache (tag "pricePrecision"):
    exchangeInfo is a huge payload, no need to re-fetch it on every signal —
    and if strategy/hratmap both need it, only one of them fetches it."""
    key = f"{symbol}\0pricePrecision"

    def producer():
        info = get_json(BASE, "/fapi/v1/exchangeInfo", weight=ratelimit.WEIGHT_EXCHANGE_INFO)
        for s in info.get("symbols", []):
            if s.get("symbol") == symbol:
                return int(s.get("pricePrecision", 4))
        return 4

    precision, _tag, _hit = data_cache.get_or_fetch(
        key, config.CANDLE_CACHE_TTL, "pricePrecision", producer)
    return precision


# --------------------------------------------------------------------------
# Klines
# --------------------------------------------------------------------------

def fetch_klines(base, path, symbol, interval, limit):
    raw = get_json(base, path, {"symbol": symbol, "interval": interval, "limit": limit})
    candles = []
    for k in raw:
        candles.append(
            {
                "open_time": int(k[0]),
                "close_time": int(k[6]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "quote_volume": float(k[7]),
            }
        )
    return candles


def _interval_ms(interval):
    """'3m' -> 180000, '1h' -> 3600000, etc. Used to normalize cache keys so
    strategy.py and hratmap.py requesting the same candle window share one
    cached fetch instead of each hitting Binance."""
    unit = interval[-1]
    n = int(interval[:-1])
    return n * {"m": 60_000, "h": 3_600_000, "d": 86_400_000}.get(unit, 60_000)


# The klines fetch is made big enough to serve BOTH consumers: strategy's 4h
# lookback (~90 candles) and hratmap's zone window (config.HRATMAP_LOOKBACK_CANDLES,
# default 9 candles) are the same 3m stream, so one cached fetch covers both
# and neither does its own call.
CANDLE_MIN_FETCH = 90


def get_cached_candles(symbol, end_time_ms=None, interval="3m", limit=12,
                       fetch_limit=None):
    """Raw klines (list of dicts, see fetch_klines) SHARED by strategy.py and
    hratmap.py, cached in data_cache with the tag "klines:<interval>".

    Both modules request the same 3m stream pinned to the same candle-close;
    the key is normalized to the interval bucket so they always hit the same
    entry. get_or_fetch() is single-flight: if strategy and the hratmap zone
    thread ask at the same moment, only ONE of them calls Binance and the
    other waits and reuses — never a duplicate fetch per signal.

    Returns (candles, tag, was_cache_hit).
    """
    fetch_limit = fetch_limit or max(limit, CANDLE_MIN_FETCH)

    if end_time_ms is None:
        end_time_ms = int(time.time() * 1000)
    bucket = (end_time_ms // _interval_ms(interval)) * _interval_ms(interval)
    key = f"{symbol}\0klines:{interval}\0{bucket}"
    tag = f"klines:{interval}"

    def producer():
        return fetch_klines(BASE, "/fapi/v1/klines", symbol, interval, fetch_limit)

    candles, stored_tag, hit = data_cache.get_or_fetch(
        key, config.CANDLE_CACHE_TTL, tag, producer)
    return candles, stored_tag, hit


# --------------------------------------------------------------------------
# AggTrades (paginated, fetched ONCE per market for the whole window)
# --------------------------------------------------------------------------

def fetch_agg_trades_slice(base, path, symbol, start_ms, end_ms, max_requests=400, request_delay=0.05):
    """
    Exhaustively pages ONE slice [start_ms, end_ms]. The span must stay under
    1h because startTime+endTime together beyond that triggers Binance error
    -1127; the first request uses both, then pagination continues via fromId
    (which has no range restriction). Every request is routed through
    ratelimit.request() — the global one-at-a-time lock + weight bucket — so
    even parallel slice threads serialize and can never exceed the IP budget
    or earn a 418 ban. A 418/429 ban raises BinanceBanned (no retry into it).
    """
    trades = []
    last_id = None
    for _ in range(max_requests):
        if last_id is None:
            params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
        else:
            params = {"symbol": symbol, "fromId": last_id + 1, "limit": 1000}

        batch = ratelimit.request(
            "GET", base + path, params=params, weight=ratelimit.WEIGHT_AGGTRADES)

        if not batch:
            break
        trades.extend(batch)
        last_id = batch[-1]["a"]
        last_time = batch[-1]["T"]
        if last_time >= end_ms or len(batch) < 1000:
            break
        time.sleep(request_delay)
    return [t for t in trades if int(t["T"]) <= end_ms]


def fetch_agg_trades_full_window(base, path, symbol, start_ms, end_ms, slice_minutes=10,
                                 max_workers=6, label="futures", verbose=True):
    """
    Splits [start_ms, end_ms] into fixed slices (each < 1h so startTime+endTime
    is legal) and fetches them CONCURRENTLY with up to max_workers threads,
    merging + sorting the results. Parallel slices cut the cold-fetch wall time
    by roughly max_workers — the biggest lever on the live-trading path.
    """
    slice_ms = int(slice_minutes * 60 * 1000)
    if verbose:
        start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
        print(f"  {label} aggTrades: {start_dt:%Y-%m-%d %H:%M} UTC -> {end_dt:%Y-%m-%d %H:%M} UTC "
              f"in {slice_minutes}m parallel slices")

    slices = []
    cur = start_ms
    while cur <= end_ms:
        slice_end = min(cur + slice_ms - 1, end_ms)
        slices.append((cur, slice_end))
        cur = slice_end + 1

    all_trades = []
    done = 0
    with ThreadPoolExecutor(max_workers=min(max_workers, len(slices))) as ex:
        futures = {ex.submit(fetch_agg_trades_slice, base, path, symbol, s, e): (s, e) for s, e in slices}
        for fut in as_completed(futures):
            all_trades.extend(fut.result())
            done += 1
            if verbose:
                print_progress(f"  {label} fetch progress", done / len(slices) * 100)

    parsed = [{"price": float(t["p"]), "qty": float(t["q"]), "time": int(t["T"])} for t in all_trades]
    parsed.sort(key=lambda t: t["time"])
    return parsed


def notional_in_box(trades_sorted, time_lo, time_hi, price_lo, price_hi):
    """trades_sorted: list of dicts sorted by 'time' ascending."""
    if not trades_sorted:
        return 0.0
    times = [t["time"] for t in trades_sorted]
    i0 = bisect.bisect_left(times, time_lo)
    i1 = bisect.bisect_right(times, time_hi)
    total = 0.0
    for t in trades_sorted[i0:i1]:
        if price_lo <= t["price"] <= price_hi:
            total += t["price"] * t["qty"]
    return total


# --------------------------------------------------------------------------
# Liquidation math (MMR comes from the built-in approximation table only)
# --------------------------------------------------------------------------

def mmr_for_leverage(lev):
    return MMR_APPROX.get(lev, 0.005)


def liq_price_long(entry, leverage, mmr):
    return entry * (1 - 1 / leverage + mmr)


def liq_price_short(entry, leverage, mmr):
    return entry * (1 + 1 / leverage - mmr)


# --------------------------------------------------------------------------
# Build liquidation rays (walk forward, anchor per candle)
# --------------------------------------------------------------------------

def find_break_index_long(candles, start_idx, level_price):
    for j in range(start_idx + 1, len(candles)):
        if candles[j]["low"] <= level_price:
            return j
    return None  # never broke


def find_break_index_short(candles, start_idx, level_price):
    for j in range(start_idx + 1, len(candles)):
        if candles[j]["high"] >= level_price:
            return j
    return None


def build_rays(candles, fut_trades, spot_trades, zone_tol_pct, notional_floor, leverage_tiers,
               verbose=True):
    """
    Side separation is already structural here, not cosmetic: a "long"
    liquidation only fires when a later candle's LOW trades down through
    the long liq price (find_break_index_long), and a "short" liquidation
    only fires when a later candle's HIGH trades up through the short liq
    price (find_break_index_short). So on a trending-up window you will
    correctly see short rays marked broke=True (they got squeezed) and
    long rays mostly broke=False (they were never actually endangered) --
    that's the real mechanic, not an artifact of the notional calc.
    """
    n = len(candles)
    last_time = candles[-1]["close_time"]
    total_anchors = max(n - 1, 1)
    rays = []
    for i in range(n - 1):  # last candle can't anchor a forward ray
        entry = candles[i]["close"]
        anchor_time = candles[i]["open_time"]
        for lev in leverage_tiers:
            mmr = mmr_for_leverage(lev)
            for side, level_price, finder in (
                ("long", liq_price_long(entry, lev, mmr), find_break_index_long),
                ("short", liq_price_short(entry, lev, mmr), find_break_index_short),
            ):
                break_idx = finder(candles, i, level_price)
                end_time = candles[break_idx]["open_time"] if break_idx is not None else last_time
                broke = break_idx is not None

                lo = level_price * (1 - zone_tol_pct / 100)
                hi = level_price * (1 + zone_tol_pct / 100)
                fut_notional = notional_in_box(fut_trades, anchor_time, end_time, lo, hi)

                if fut_notional < notional_floor:
                    continue

                spot_notional = notional_in_box(spot_trades, anchor_time, end_time, lo, hi)
                fs_ratio = (fut_notional / spot_notional) if spot_notional > 0 else None

                rays.append(
                    {
                        "side": side,
                        "leverage": lev,
                        "level_price": level_price,
                        "anchor_time": anchor_time,
                        "end_time": end_time,
                        "notional": fut_notional,
                        "spot_notional": spot_notional,
                        "fs_ratio": fs_ratio,
                        "broke": broke,
                    }
                )
        if verbose:
            print_progress("  Calculating liquidation levels", (i + 1) / total_anchors * 100)
    return rays


def fetch_zone_inputs(symbol, end_time_ms, lookback=12, leverage=75, notional_floor=1000.0,
                      slice_minutes=6, max_workers=6):
    """
    Background-safe half of the zone computation: everything that does NOT
    depend on candle data. The 3m window is derived straight from end_time
    (lookback * 3min), so this can run concurrently with
    strategy.decide_direction() — the strategy's candles are handed to
    compute_uncleared_zones() once voting finishes.
    """
    print(f"[hratmap] fetching {symbol} liquidation data "
          f"(3m x{lookback} candles, {leverage}x, floor {notional_floor:,.0f} USDT)...")
    precision = get_price_precision(symbol)
    start_ms = end_time_ms - lookback * 3 * 60 * 1000
    fut_trades = fetch_agg_trades_full_window(
        BASE, "/fapi/v1/aggTrades", symbol, start_ms, end_time_ms,
        slice_minutes=slice_minutes, max_workers=max_workers, label="futures", verbose=False,
    )
    return precision, fut_trades


def compute_uncleared_zones(symbol, candles, precision, fut_trades, lookback=12,
                            leverage=75, notional_floor=1000.0, zone_tol=0.15):
    """
    Fast, candle-dependent half: builds rays from the SHARED strategy candles
    + the pre-fetched aggTrades (see fetch_zone_inputs). Zones come back
    sorted by notional (largest first) so the top candidate is ready to use
    directly as the TP.
    """
    print(f"[hratmap] calculating liquidation levels...")
    candles = candles[-lookback:] if len(candles) >= lookback else candles
    if not candles:
        print(f"[hratmap] {symbol}: no candles in window, no zones.")
        return [], precision

    rays = build_rays(candles, fut_trades, [], zone_tol, notional_floor, [leverage], verbose=False)
    zones = [r for r in rays if not r["broke"]]
    zones.sort(key=lambda r: r["notional"], reverse=True)
    print(f"[hratmap] {len(zones)} uncleared zone(s) (sorted by notional, top = TP candidate).")
    return zones, precision


def get_uncleared_zones(symbol, end_time=None, lookback=12, leverage=75,
                        notional_floor=1000.0, zone_tol=0.15, slice_minutes=6, max_workers=6):
    """
    Fetch liquidation rays for `symbol` (3m x lookback candles, a single
    leverage tier) and return ONLY the uncleared (still-open) zones.

    Returns (zones, precision):
        zones:     list of ray dicts {side, leverage, level_price, notional,
                   anchor_time, end_time, broke=False, ...} sorted by
                   notional (largest first) so the top candidate is ready to
                   use directly as the TP.
        precision: symbol's price precision from the exchange.

    This is the single entry point used both by the CLI and by run_bot.py
    (which runs it whole in a background thread so fetch + calculate overlap
    the strategy voting). fetch_zone_inputs() + compute_uncleared_zones() are
    its split internals, available for callers that want to stage the work.
    """
    print(f"[hratmap] fetching {symbol} liquidation data "
          f"(3m x{lookback} candles, {leverage}x, floor {notional_floor:,.0f} USDT)...")
    precision = get_price_precision(symbol)

    # Shared candle source: the same 3m stream strategy.py fetches, so this
    # never re-polls Binance for candles it already has (tag identifies the
    # data to readers, and `hit` shows whether it was a cache reuse).
    candles, tag, hit = get_cached_candles(symbol, end_time_ms=end_time, interval="3m", limit=lookback)
    print(f"[hratmap] {symbol}: candles tag={tag} "
          f"({'cache hit, reuse' if hit else 'fetched fresh'})")

    if end_time is not None:
        candles = [c for c in candles if c["open_time"] <= end_time]
        candles = candles[-lookback:] if len(candles) >= lookback else candles
    if not candles:
        print(f"[hratmap] {symbol}: no candles in window, no zones.")
        return [], precision

    start_ms = candles[0]["open_time"]
    now_ms = int(time.time() * 1000)
    end_ms = end_time if end_time is not None else max(candles[-1]["close_time"], now_ms)

    fut_trades = fetch_agg_trades_full_window(
        BASE, "/fapi/v1/aggTrades", symbol, start_ms, end_ms,
        slice_minutes=slice_minutes, max_workers=max_workers, label="futures", verbose=False,
    )
    return compute_uncleared_zones(symbol, candles, precision, fut_trades,
                                   lookback=lookback, leverage=leverage,
                                   notional_floor=notional_floor, zone_tol=zone_tol)


def print_uncleared_zones(rays, precision, notional_floor):
    """Print still-open (uncleared) liquidation zones to the terminal.

    A ray is "cleared" when a later candle actually traded through its liq
    price (broke=True). Uncleared zones are the pending clusters still sitting
    in price space -- the levels worth watching.
    """
    uncleared = [r for r in rays if not r["broke"]]
    if not uncleared:
        print(f"\n  No uncleared zones at/above {notional_floor:,.0f} USDT in this window.")
        return
    uncleared.sort(key=lambda r: r["level_price"])
    print(f"\n  Uncleared liquidation zones "
          f"(floor >= {notional_floor:,.0f} USDT, still open):")
    print(f"    {'SIDE':<5} {'LEV':>3}  {'PRICE':>12}  {'FLOW (USDT)':>14}")
    for r in uncleared:
        print(f"    {r['side']:<5} {r['leverage']:>3}x  {r['level_price']:>12.{precision}f}  "
              f"{r['notional']:>14,.0f}")
    print()


# --------------------------------------------------------------------------
# Plotting
# --------------------------------------------------------------------------

def to_dt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


LONG_COLORSCALE = "Reds"    # long liquidations = forced selling if triggered
SHORT_COLORSCALE = "Blues"  # short liquidations = forced buying/squeeze if triggered


def color_and_width_for_notional(notional, min_n, max_n, colorscale):
    if max_n <= min_n:
        norm = 0.5
    else:
        log_n = math.log10(max(notional, 1))
        log_min = math.log10(max(min_n, 1))
        log_max = math.log10(max(max_n, 1))
        norm = (log_n - log_min) / (log_max - log_min) if log_max > log_min else 0.5
        norm = min(max(norm, 0.0), 1.0)
    color = pcolors.sample_colorscale(colorscale, norm)[0]
    width = 1.5 + 5.0 * norm
    return color, width, norm


def build_figure(symbol, interval, precision, candles, spot_candles, rays, notional_floor):
    times = [to_dt(c["open_time"]) for c in candles]

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.72, 0.28], vertical_spacing=0.04,
        subplot_titles=(
            f"{symbol} -- {interval} candles with liquidation-level rays (notional >= {notional_floor:,.0f} USDT)",
            "Futures vs Spot notional volume per candle",
        ),
    )

    fig.add_trace(
        go.Candlestick(
            x=times,
            open=[c["open"] for c in candles],
            high=[c["high"] for c in candles],
            low=[c["low"] for c in candles],
            close=[c["close"] for c in candles],
            name="Price",
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
        ),
        row=1, col=1,
    )

    long_rays = [r for r in rays if r["side"] == "long"]
    short_rays = [r for r in rays if r["side"] == "short"]

    long_notionals = [r["notional"] for r in long_rays]
    short_notionals = [r["notional"] for r in short_rays]
    long_min, long_max = (min(long_notionals), max(long_notionals)) if long_notionals else (notional_floor, notional_floor)
    short_min, short_max = (min(short_notionals), max(short_notionals)) if short_notionals else (notional_floor, notional_floor)

    def add_ray_traces(ray_list, colorscale, legend_label, legendgroup, min_n, max_n):
        for idx, r in enumerate(ray_list):
            color, width, norm = color_and_width_for_notional(r["notional"], min_n, max_n, colorscale)
            dash = "solid" if r["broke"] else "dot"
            fs_txt = f"{r['fs_ratio']:.2f}" if r["fs_ratio"] is not None else "n/a"
            hover = (
                f"{legend_label} | {r['leverage']}x<br>"
                f"Price: {r['level_price']:.{precision}f}<br>"
                f"Futures notional in zone: {r['notional']:,.0f} USDT<br>"
                f"Spot notional in zone: {r['spot_notional']:,.0f} USDT<br>"
                f"Fut/Spot ratio: {fs_txt}<br>"
                f"Anchor: {to_dt(r['anchor_time']):%m-%d %H:%M}<br>"
                f"{'Liquidated at' if r['broke'] else 'Still open, chart end'}: {to_dt(r['end_time']):%m-%d %H:%M}"
            )
            fig.add_trace(
                go.Scatter(
                    x=[to_dt(r["anchor_time"]), to_dt(r["end_time"])],
                    y=[r["level_price"], r["level_price"]],
                    mode="lines",
                    line=dict(color=color, width=width, dash=dash),
                    legendgroup=legendgroup,
                    name=legend_label,
                    showlegend=(idx == 0),  # one legend entry toggles the whole group
                    hovertemplate=hover + "<extra></extra>",
                ),
                row=1, col=1,
            )

    # Longs first (so shorts draw on top, matching typical up-move visibility),
    # each side in its own legend group -- click either legend entry to hide/show
    # every ray of that side at once, since only one side actually liquidates on
    # a given directional move.
    add_ray_traces(long_rays, LONG_COLORSCALE, "Long liquidations", "long_liqs", long_min, long_max)
    add_ray_traces(short_rays, SHORT_COLORSCALE, "Short liquidations", "short_liqs", short_min, short_max)

    # Colorbar keys (dummy invisible traces), one per side
    fig.add_trace(
        go.Scatter(
            x=[None], y=[None], mode="markers",
            marker=dict(
                colorscale=LONG_COLORSCALE, cmin=long_min, cmax=long_max,
                color=[long_min, long_max], showscale=True,
                colorbar=dict(title="Long liq notional<br>(USDT)", x=1.02, len=0.35, y=0.95,
                               tickformat=",.0f"),
                size=0.0001,
            ),
            showlegend=False,
            hoverinfo="skip",
        ),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=[None], y=[None], mode="markers",
            marker=dict(
                colorscale=SHORT_COLORSCALE, cmin=short_min, cmax=short_max,
                color=[short_min, short_max], showscale=True,
                colorbar=dict(title="Short liq notional<br>(USDT)", x=1.16, len=0.35, y=0.95,
                               tickformat=",.0f"),
                size=0.0001,
            ),
            showlegend=False,
            hoverinfo="skip",
        ),
        row=1, col=1,
    )

    # Bottom subplot: futures vs spot notional per candle
    fut_notional = [c["quote_volume"] for c in candles]
    spot_by_time = {c["open_time"]: c["quote_volume"] for c in spot_candles}
    spot_notional = [spot_by_time.get(c["open_time"], None) for c in candles]

    fig.add_trace(
        go.Scatter(x=times, y=fut_notional, mode="lines", name="Futures notional",
                    line=dict(color="#4fc3f7", width=1.6)),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(x=times, y=spot_notional, mode="lines", name="Spot notional",
                    line=dict(color="#ffb74d", width=1.6)),
        row=2, col=1,
    )

    fig.update_layout(
        template="plotly_dark",
        height=850,
        hovermode="closest",
        dragmode="zoom",
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_yaxes(title_text="Price (USDT)", row=1, col=1)
    fig.update_yaxes(title_text="Notional (USDT)", row=2, col=1)
    fig.update_xaxes(title_text="Time (UTC)", row=2, col=1)

    return fig


# --------------------------------------------------------------------------
# CLI / interactive input
# --------------------------------------------------------------------------

def prompt_for_inputs():
    symbol = input("Symbol (e.g. BTCUSDT, ETHUSDT, APTUSDT): ").strip().upper()
    while not symbol:
        symbol = input("Symbol can't be empty. Enter symbol: ").strip().upper()

    interval = input(f"Candle interval [default 1h] (options: {', '.join(sorted(VALID_INTERVALS))}): ").strip()
    interval = interval if interval else "1h"
    while interval not in VALID_INTERVALS:
        interval = input(f"Invalid interval. Choose from {', '.join(sorted(VALID_INTERVALS))}: ").strip()

    lookback_raw = input("Lookback candles [default 100]: ").strip()
    lookback = int(lookback_raw) if lookback_raw.isdigit() else 100

    floor_raw = input("Notional floor to plot a level [default 10000]: ").strip()
    notional_floor = float(floor_raw) if floor_raw else 10000.0

    lev_raw = input(f"Max leverage this coin actually allows [default: use all of {LEVERAGE_TIERS}]: ").strip()
    max_leverage = int(lev_raw) if lev_raw.isdigit() else None

    return symbol, interval, lookback, notional_floor, max_leverage


def main():
    ap = argparse.ArgumentParser(description="Interactive liquidation-ray chart")
    ap.add_argument("symbol", nargs="?", default=None)
    ap.add_argument("--interval", default=None)
    ap.add_argument("--lookback", type=int, default=None)
    ap.add_argument("--notional-floor", type=float, default=None, help="minimum zone notional (USDT) to plot a level")
    ap.add_argument("--zone-tol", type=float, default=0.15, help="+/- %% price band per level for notional calc")
    ap.add_argument("--slice-minutes", type=float, default=10, help="aggTrades fetch slice size in minutes (each slice < 1h)")
    ap.add_argument("--max-workers", type=int, default=6, help="concurrent aggTrades slice fetchers")
    ap.add_argument("--max-leverage", type=int, default=None,
                     help="cap leverage tiers at this coin's real max (e.g. 20 if the pair caps at 20x), "
                          "so no phantom levels are drawn for leverage the exchange never actually offers")
    ap.add_argument("--output", default=None, help="output HTML path (default: liq_chart_<symbol>.html)")
    ap.add_argument("--chart", action="store_true",
                    help="build the interactive HTML chart instead of printing uncleared zones "
                         "(prompts for interval/lookback/floor/leverage when no symbol given)")
    cli_args = ap.parse_args()

    uncleared = not cli_args.chart

    if cli_args.symbol is None:
        if uncleared:
            symbol = input("Symbol (e.g. BTCUSDT, ETHUSDT, APTUSDT): ").strip().upper()
            while not symbol:
                symbol = input("Symbol can't be empty. Enter symbol: ").strip().upper()
        else:
            symbol, interval, lookback, notional_floor, max_leverage = prompt_for_inputs()
    else:
        symbol = cli_args.symbol.upper()
        interval = cli_args.interval or "1h"
        lookback = cli_args.lookback or 100
        notional_floor = cli_args.notional_floor if cli_args.notional_floor is not None else 10000.0
        max_leverage = cli_args.max_leverage

    if uncleared:
        interval = "3m"
        lookback = 12
        notional_floor = 1000.0
        max_leverage = 75
        print("Uncleared-zone mode: 3m candles / 75x leverage / 12-candle lookback / "
              "min flow 1000 USDT")

    leverage_tiers = [t for t in LEVERAGE_TIERS if max_leverage is None or t <= max_leverage]
    if max_leverage is not None:
        if not leverage_tiers:
            # user's cap is below our lowest tier (5x) -- still include the cap itself
            leverage_tiers = [max_leverage]
        elif leverage_tiers[-1] != max_leverage:
            # include the exact cap even if it doesn't match one of our preset tiers
            leverage_tiers = leverage_tiers + [max_leverage]
        print(f"Max leverage capped at {max_leverage}x -> using tiers {leverage_tiers}")

    if uncleared:
        leverage_tiers = [75]
        print(f"Using leverage tiers {leverage_tiers}")

    zone_tol = cli_args.zone_tol
    slice_minutes = cli_args.slice_minutes
    max_workers = cli_args.max_workers
    output = cli_args.output or f"liq_chart_{symbol}.html"

    if uncleared:
        zones, precision = get_uncleared_zones(symbol)
        print_uncleared_zones(zones, precision, notional_floor)
        return

    print(f"Fetching {symbol} {interval} x{lookback} candles...")
    try:
        precision = get_price_precision(symbol)
        candles = fetch_klines(BASE, "/fapi/v1/klines", symbol, interval, lookback)
        spot_candles = []
        try:
            spot_candles = fetch_klines(SPOT_BASE, "/api/v3/klines", symbol, interval, lookback)
        except requests.HTTPError:
            spot_candles = []

        start_ms = candles[0]["open_time"]
        now_ms = int(time.time() * 1000)
        end_ms = max(candles[-1]["close_time"], now_ms)  # cover all the way to "now"

        fut_trades = fetch_agg_trades_full_window(
            BASE, "/fapi/v1/aggTrades", symbol, start_ms, end_ms,
            slice_minutes=slice_minutes, max_workers=max_workers, label="futures",
        )
        print(f"  Collected {len(fut_trades):,} futures trades covering the full window.")

        if spot_candles:
            spot_trades = fetch_agg_trades_full_window(
                SPOT_BASE, "/api/v3/aggTrades", symbol, start_ms, end_ms,
                slice_minutes=slice_minutes, max_workers=max_workers, label="spot",
            )
            print(f"  Collected {len(spot_trades):,} spot trades covering the full window.")
        else:
            spot_trades = []
            print("  Skipping spot aggTrades: symbol isn't listed on spot.")
    except requests.HTTPError as e:
        print(f"Binance API error: {e}")
        sys.exit(1)
    except requests.RequestException as e:
        print(f"Network error reaching Binance: {e}")
        sys.exit(1)

    print("Computing liquidation rays and zone notional "
          "(MMR from the built-in approximation table, no signed exchange call)...")
    rays = build_rays(candles, fut_trades, spot_trades, zone_tol, notional_floor, leverage_tiers)
    long_n = sum(1 for r in rays if r["side"] == "long")
    short_n = sum(1 for r in rays if r["side"] == "short")
    print(f"  {len(rays)} rays at or above the {notional_floor:,.0f} USDT floor "
          f"({long_n} long, {short_n} short).")

    fig = build_figure(symbol, interval, precision, candles, spot_candles, rays, notional_floor)
    fig.write_html(output, include_plotlyjs="cdn")
    print(f"Saved interactive chart to {output}")


if __name__ == "__main__":
    main()