"""
ratelimit.py  —  Global one-at-a-time gate + weight bucket + ban state
========================================================================
Binance futures caps the ENTIRE IP at 2400 request-weight per minute (and bans
the IP with a 418 "I'm a teapot" when exceeded — see the "Way too many
requests" error). Multiple modules hit the same IP from this process:
strategy's OHLCV fetches, the trader's position/order polls, and balance
checks. Left unthrottled they burst past the cap in seconds, get the bot
418-banned, and then — worst of all — keep RETRYING into the active ban,
which makes Binance extend the ban for many more minutes (that is what turned
a normal ban into a 25+ minute outage).

This module fixes all three failure modes with ONE shared choke point:

1. SERIALIZED — every Binance REST call in the process goes through the
   global `_HTTP_LOCK`, so the bot polls strictly ONE request at a time.
   Strategy candle fetches, trigger path and the trader's monitor/follower
   all queue on the same lock instead of hammering Binance concurrently.

2. WEIGHT BUCKET — `acquire(weight)` refills continuously (capacity =
   RATE_LIMIT_WEIGHT_PER_MIN tokens, refill = capacity/60 per second) so the
   process can never exceed the configured budget even if callers queue up.
   Callers that miss their deadline just abandon the TRADE (not the process).

3. FAIL-FAST BAN — the moment a 418 (or 429 carrying a "banned until"
   timestamp) is seen, its expiry is recorded globally in `_BANNED_UNTIL`.
   Until that time every request raises `BinanceBanned` WITHOUT touching the
   network, so the IP is never re-polled into an active ban and the ban is
   never extended. No module ever sleep-then-retry-loops through a ban.

All three guarantees are exported through a single helper:

    data = ratelimit.request("GET", url, params=..., weight=...)

ccxt callers (strategy, trader) must instead wrap their call in
`with ratelimit.serialized():` and call `ratelimit.fail_fast_if_banned()`
first so they share the same lock and ban state.
"""

import re
import threading
import time

import requests

import config

# ── Global serialization: strictly one Binance request in flight at a time ──
_HTTP_LOCK = threading.Lock()

# ── Weight token bucket ──────────────────────────────────────────────────────
_LOCK = threading.Lock()
_CAPACITY = float(config.RATE_LIMIT_WEIGHT_PER_MIN)
_TOKENS = _CAPACITY
_LAST = time.monotonic()

# weights (Binance USD-M): aggTrades=20, klines<=100=2, exchangeInfo=1
WEIGHT_AGGTRADES = 20
WEIGHT_KLINES = 2
WEIGHT_EXCHANGE_INFO = 1

# ── Global IP-ban state (fail-fast guard, never poll into an active ban) ────
_BANNED_UNTIL: float | None = None   # epoch seconds the ban expires
_BAN_LOCK = threading.Lock()


class BinanceBanned(Exception):
    """The whole IP is banned by Binance (418, or 429 with a ban timestamp).
    Raised BEFORE a request is sent when the ban is already active, or right
    after one is received. Callers must NOT retry the request — retrying into
    an active ban is what makes Binance extend the ban."""
    def __init__(self, until_epoch_s: float, message: str):
        self.until = until_epoch_s
        self.remaining = max(until_epoch_s - time.time(), 0.0)
        super().__init__(message)


# ── One-at-a-time polling ───────────────────────────────────────────────────

def serialized():
    """Context manager: holds the global one-request-at-a-time lock. Every
    Binance REST call — raw requests OR ccxt — must run inside this, so the
    whole process polls the API one-by-one and never bursts."""
    return _HTTP_LOCK


# ── Token bucket ────────────────────────────────────────────────────────────

def _refill():
    global _TOKENS, _LAST
    now = time.monotonic()
    _TOKENS = min(_CAPACITY, _TOKENS + (now - _LAST) * (_CAPACITY / 60.0))
    _LAST = now


def acquire(weight: float, timeout: float = None) -> bool:
    """Block until `weight` tokens are available, then deduct and return True.

    Blocks indefinitely unless `timeout` (seconds) is given, in which case it
    returns False if the budget isn't available in time. A caller that gets
    False must NOT send the request.
    """
    global _TOKENS
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        with _LOCK:
            _refill()
            if _TOKENS >= weight:
                _TOKENS -= weight
                return True
        if deadline is not None and time.monotonic() >= deadline:
            return False
        time.sleep(0.01)


# ── Ban parsing / global ban state ──────────────────────────────────────────

def ban_until_epoch(text: str) -> float | None:
    """Parse a Binance 418/429 body like '...banned until 1785679007891...'
    and return the unban time as epoch seconds (or None if unparseable)."""
    m = re.search(r"banned until (\d{12,})", text or "")
    if m:
        return int(m.group(1)) / 1000.0
    return None


def ban_sleep_seconds(text: str) -> float | None:
    """How many seconds until the ban in `text` expires (or None)."""
    until = ban_until_epoch(text)
    if until is not None:
        return max(until - time.time() + 2.0, 1.0)
    return None


def mark_banned(until_epoch_s: float):
    """Record a global ban expiry. Only ever EXTENDS the ban (never shrinks
    it) — a shorter timestamp arriving late is ignored."""
    global _BANNED_UNTIL
    with _BAN_LOCK:
        if _BANNED_UNTIL is None or until_epoch_s > _BANNED_UNTIL:
            _BANNED_UNTIL = until_epoch_s


def current_ban_remaining() -> float:
    """Seconds until the current IP ban clears (0.0 = not banned)."""
    global _BANNED_UNTIL
    with _BAN_LOCK:
        if _BANNED_UNTIL is None:
            return 0.0
        rem = _BANNED_UNTIL - time.time()
        if rem <= 0:
            _BANNED_UNTIL = None
            return 0.0
        return rem


def fail_fast_if_banned():
    """Raise BinanceBanned immediately if the IP is still banned — WITHOUT
    touching the network. This is the anti-extension guard: while a ban is
    active every request aborts here instead of re-polling Binance."""
    rem = current_ban_remaining()
    if rem > 0:
        raise BinanceBanned(
            time.time() + rem,
            f"IP still banned by Binance for {rem:.0f}s — not polling the API",
        )


# ── Unified request helper (raw REST / requests) ────────────────────────────

def request(method: str, url: str, params=None, json=None, timeout: float = 10,
            weight: float = WEIGHT_KLINES, retries: int = 2):
    """Single, fully-serialized, token-budgeted Binance HTTP call.

    - Runs inside the global one-request-at-a-time lock.
    - Fails fast (BinanceBanned) if the IP is already banned.
    - On 418: records the ban and raises BinanceBanned immediately — NO
      sleep-and-retry loop that would make Binance extend the ban.
    - On 429 without a ban timestamp: sleeps Retry-After and retries (bounded
      by `retries`), otherwise raises.
    - Consumes `weight` tokens from the global bucket before sending.
    """
    with _HTTP_LOCK:
        fail_fast_if_banned()
        for attempt in range(retries + 1):
            if not acquire(weight):
                raise RuntimeError(
                    "rate-limit token budget exhausted, not sending request")
            r = requests.request(method, url, params=params, json=json, timeout=timeout)

            if r.status_code == 418:
                until = ban_until_epoch(r.text)
                if until is not None:
                    mark_banned(until)
                raise BinanceBanned(
                    until if until is not None else time.time() + 60,
                    f"Binance 418 IP ban: {r.text[:300]}",
                )
            if r.status_code == 429:
                until = ban_until_epoch(r.text)
                if until is not None:
                    mark_banned(until)
                    raise BinanceBanned(
                        until, f"Binance 429 with ban timestamp: {r.text[:300]}")
                try:
                    retry_after = int(r.headers.get("Retry-After", "2"))
                except ValueError:
                    retry_after = 2
                time.sleep(retry_after)
                continue

            r.raise_for_status()
            return r.json()

        raise RuntimeError(f"rate-limited out after {retries} retries: {method} {url}")


def wait_out_ban(text: str):
    """DEPRECATED — use fail_fast_if_banned() + BinanceBanned instead.
    Kept only so old call sites don't break; never sleep-retry into a ban."""
    raise BinanceBanned(
        ban_until_epoch(text) or (time.time() + 60),
        f"IP banned by Binance: {text[:200]}",
    )
