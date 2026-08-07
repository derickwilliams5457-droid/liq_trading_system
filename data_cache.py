"""
data_cache.py  —  Small TTL cache with provenance tags + single-flight dedup
=============================================================================
The same market data is needed by several modules at once (strategy's 3m
candles, hratmap's 3m candles for the zone rays, price precision, zone
results). Without a shared cache every consumer does its own Binance call for
the same data — which is exactly what blew past the IP weight budget and got
the bot 418-banned during the signal influx.

Every entry carries a `tag` (e.g. "klines:3m", "pricePrecision",
"zones:hratmap") so any log/reader can see at a glance which piece of data a
hit came from and which module produced it. Entries expire on a TTL as a
safety net; run_bot.py additionally evicts the per-symbol entries after the
final verdict so the cache never clogs up with stale candles/zones.

get_or_fetch() is "single-flight": if two threads need the same key at once
(which is the normal case here — the hratmap zone thread and the strategy
vote run in parallel), only one of them performs the network fetch and the
other waits and reuses the result. The waiting thread NEVER issues its own
duplicate Binance call.
"""

import threading
import time

_LOCK = threading.Lock()
_ENTRIES: dict = {}     # key -> {"tag", "data", "created", "expires"}
_IN_FLIGHT: dict = {}   # key -> threading.Event() (a producer is working on it)


def get(key):
    """Return (data, tag) or (None, None) if absent/expired."""
    with _LOCK:
        e = _ENTRIES.get(key)
        if e is None:
            return None, None
        if e["expires"] < time.time():
            del _ENTRIES[key]
            return None, None
        return e["data"], e["tag"]


def set(key, data, tag, ttl_seconds):
    """Store `data` under `key` with a human-readable provenance `tag`."""
    with _LOCK:
        _ENTRIES[key] = {
            "tag": tag,
            "data": data,
            "created": time.time(),
            "expires": time.time() + ttl_seconds,
        }
        evt = _IN_FLIGHT.pop(key, None)
        if evt is not None:
            evt.set()


def get_or_fetch(key, ttl_seconds, tag, producer, wait_timeout: float = 10.0):
    """Return (data, tag, was_hit).

    If the key is cached, return it. Otherwise one thread becomes the
    producer (calling `producer()`), caches the result under `tag`, and any
    concurrent caller of the same key waits and reuses that single fetch —
    never a duplicate Binance call. If the producer raises, the exception
    propagates to every waiter after a small wait, and the key stays clear.
    """
    data, stored_tag = get(key)
    if data is not None:
        return data, stored_tag, True

    while True:
        with _LOCK:
            evt = _IN_FLIGHT.get(key)
            if evt is None:
                evt = threading.Event()
                _IN_FLIGHT[key] = evt
                break
        evt.wait(wait_timeout)
        data, stored_tag = get(key)
        if data is not None:
            return data, stored_tag, True
        # producer failed without caching — fall through and try to produce
        # ourselves on the next pass.

    try:
        data = producer()
        set(key, data, tag, ttl_seconds)
        return data, tag, False
    finally:
        with _LOCK:
            evt = _IN_FLIGHT.pop(key, None)
            if evt is not None:
                evt.set()


def evict(key):
    """Remove a single key (e.g. a resolved zone fetch)."""
    with _LOCK:
        _ENTRIES.pop(key, None)


def evict_symbol(symbol):
    """Remove every cached datum whose key belongs to `symbol` — called after
    the final verdict so one signal's data never lingers and clogs the cache."""
    with _LOCK:
        for k in [k for k in _ENTRIES if k.startswith(symbol + "\0")]:
            del _ENTRIES[k]


def clear():
    with _LOCK:
        _ENTRIES.clear()


def stats():
    """Return (entry_count, keys) for diagnostics — every key embeds the tag
    data so a reader can identify what is cached."""
    with _LOCK:
        return len(_ENTRIES), sorted(_ENTRIES.keys())
