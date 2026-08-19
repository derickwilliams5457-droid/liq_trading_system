"""
dash.py  —  lightweight read-only dashboard
=================================================
Serves a tiny interactive page over HTTP showing, per exchange (binance /
bybit): equity curve, realized / unrealized PnL, open positions, closed
positions and recent activity. It is strictly observational:

  - reads data/performance.csv, data/trade_log.jsonl and data/snapshot.json
    READ-ONLY,
  - tails only appended bytes (no full re-reads per poll), and detects when
    maintenance.py rewrites performance.csv via os.replace (new inode) so it
    re-reads from the top instead of trusting a stale offset,
  - a single background thread refreshes the snapshot; HTTP requests just
    serve the cached snapshot (no file I/O on the request path),
  - writes nothing, opens no sockets to exchanges.

Reporting design:
  - Open positions + unrealized PnL come from data/snapshot.json, written by
    the monitors every poll from their exchange fetch_positions(). dash never
    guesses "open" from CSV history, so a restart, an untracked position, or a
    maintenance trim can never leave a phantom open trade on the page.
  - Realized PnL comes from self-contained "closed" rows in performance.csv
    (each carries its own final PnL), so it does not depend on dash having
    seen the matching open rows.
  - The equity curve replays performance.csv; "open" rows track unrealized PnL
    and "closed" rows convert it to realized. An "open" row not updated within
    OPEN_STALE_SECONDS is force-closed at its last PnL so a dead ghost cannot
    inflate the curve forever — and the booking is rolled back if the position
    reappears.
  - Staleness is explicit: if snapshot.json is older than
    SNAPSHOT_STALE_SECONDS the page shows a STALE banner.

Usage:
    python dash.py            # then open http://127.0.0.1:8765

Env (see config.py): DASH_HOST, DASH_PORT, DASH_REFRESH_SECONDS,
DASH_START_BALANCE_BINANCE, DASH_START_BALANCE_BYBIT.
"""

import csv
import json
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import config
import perfio

PERF_HEADER = [
    "timestamp_utc", "symbol", "direction", "qty", "entry", "sl", "tp",
    "mark_price", "unrealized_pnl", "status", "exchange",
]

LOG_TAIL_BYTES = 256 * 1024      # trade_log.jsonl: only tail this much per refresh
LOG_TAIL_EVENTS = 60             # keep this many recent events in the payload
SERIES_MAX = 1800                # downsample equity series to at most this many points
CLOSED_KEEP = 300                # keep this many closed trades in memory


class DataSource:
    """Tails the data files incrementally and builds a JSON snapshot."""

    def __init__(self):
        self._perf_pos = 0
        self._perf_ino = None
        self._perf_header = False
        self._state = self._fresh_state()
        self._snap_open = []            # open positions from snapshot.json
        self._snap_unrealized = {"binance": 0.0, "bybit": 0.0}
        self._snap_account = {}         # exchange -> account state from snapshot.json
        self._snap_generated = None
        self._snap_stale = True
        self._snap_seen = False
        self._payload = b"{}"
        self._error = None
        self._lock = threading.Lock()

    def _fresh_state(self):
        return {
            "realized": {"binance": 0.0, "bybit": 0.0},
            "open_by_exch": {"binance": 0.0, "bybit": 0.0},
            "open_trades": {},      # (ex, symbol, direction) -> trade dict
            "undo_pnl": {},         # key -> amount force-closed; roll back on reopen
            "closed": [],           # recent closed trades
            "series": [],           # [epoch, binance_pnl, bybit_pnl]
        }

    # ------------------------------------------------------------------
    # Incremental tailing
    # ------------------------------------------------------------------
    def _read_new_perf_lines(self):
        """Return complete, newly-appended CSV lines (without the header)."""
        path = config.PERFORMANCE_CSV
        if not path.exists():
            return []
        size = os.path.getsize(path)
        try:
            ino = os.stat(path).st_ino
        except OSError:
            return []
        if self._perf_ino is None:
            self._perf_ino = ino
        if ino != self._perf_ino or size < self._perf_pos:
            # maintenance.py rewrites the file via os.replace (new inode) and
            # may trim rows, so byte offsets are meaningless: re-read from the
            # top into a fresh state for a consistent equity curve.
            print("[dash] performance.csv replaced/trimmed — re-reading from top.")
            self._perf_ino = ino
            self._perf_pos = 0
            self._perf_header = False
            self._state = self._fresh_state()
        with open(path, "rb") as f:
            f.seek(self._perf_pos)
            data = f.read()
        if not data:
            return []
        text = data.decode("utf-8", errors="replace")
        if text.endswith("\n"):
            complete, self._perf_pos = text, size
        else:
            cut = text.rfind("\n")
            if cut == -1:
                return []             # only a partial first line so far
            complete = text[: cut + 1]
            self._perf_pos += len(complete.encode("utf-8"))
        lines = complete.splitlines()
        if not self._perf_header:
            lines = lines[1:]          # drop the CSV header row
            self._perf_header = True
        return lines

    def _read_log_tail(self):
        """Return the most recent trade_log.jsonl events."""
        path = config.TRADE_LOG_FILE
        if not path.exists():
            return []
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            if size > LOG_TAIL_BYTES:
                f.seek(size - LOG_TAIL_BYTES)
                data = f.read()
                cut = data.find(b"\n")
                if cut != -1:
                    data = data[cut + 1:]
            else:
                data = f.read()
        events = []
        for line in data.splitlines():
            if not line.strip():
                continue
            try:
                events.append(json.loads(line))
            except Exception:
                continue
        return events[-LOG_TAIL_EVENTS:]

    # ------------------------------------------------------------------
    # Row -> state
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_epoch(iso: str) -> float | None:
        try:
            return datetime.fromisoformat(iso).timestamp()
        except Exception:
            return None

    def _process_row(self, d: dict):
        ts = self._parse_epoch((d.get("timestamp_utc") or "").strip())
        if ts is None:
            return
        st = self._state
        ex = (d.get("exchange") or "binance").strip().lower()
        if ex not in st["open_by_exch"]:
            ex = "binance"
        try:
            qty = float(d.get("qty") or 0)
            entry = float(d.get("entry") or 0)
            pnl = float(d.get("unrealized_pnl") or 0)
        except (TypeError, ValueError):
            return
        symbol = (d.get("symbol") or "").strip()
        direction = (d.get("direction") or "").strip()
        mark = (d.get("mark_price") or "").strip()
        status = (d.get("status") or "open").strip()
        key = (ex, symbol, direction, round(entry, 8))

        if status == "closed":
            forced = st["undo_pnl"].pop(key, None)
            t = st["open_trades"].pop(key, None)
            # A closed row is SELF-CONTAINED: prefer its own explicit PnL over
            # the last tracked open PnL, so realized PnL does not depend on dash
            # having seen the matching open rows.
            explicit = (d.get("unrealized_pnl") or "").strip()
            if explicit:
                rpnl = float(explicit or 0)
            elif t is not None:
                rpnl = t["pnl"]
            else:
                rpnl = 0.0
            if t is not None:
                st["open_by_exch"][ex] -= t["pnl"]
            if forced is not None:
                st["realized"][ex] -= forced["pnl"]
            st["realized"][ex] += rpnl
            st["closed"].append({
                "exchange": ex, "symbol": symbol, "direction": direction,
                "qty": qty, "entry": entry, "mark": mark,
                "pnl": rpnl, "realized": rpnl,
                "opened_at": t.get("opened_at", "") if t is not None
                else (d.get("timestamp_utc") or "").strip(),
                "closed_at": (d.get("timestamp_utc") or "").strip(),
                "stale_closed": bool(forced),
            })
            if len(st["closed"]) > CLOSED_KEEP:
                st["closed"] = st["closed"][-CLOSED_KEEP:]
        else:
            forced = st["undo_pnl"].pop(key, None)
            if forced is not None:
                # A trade we force-closed as stale is actually still open:
                # undo the forced booking so realized is not inflated.
                st["realized"][ex] -= forced["pnl"]
            t = st["open_trades"].get(key)
            if t is None:
                t = {
                    "exchange": ex, "symbol": symbol, "direction": direction,
                    "qty": qty, "entry": entry, "mark": mark, "pnl": 0.0,
                    "opened_at": (d.get("timestamp_utc") or "").strip(),
                    "first_ts": ts, "last_ts": ts,
                }
                st["open_trades"][key] = t
            delta = pnl - t["pnl"]
            t["pnl"] = pnl
            t["mark"] = mark
            t["last_ts"] = ts
            st["open_by_exch"][ex] += delta

        st["series"].append([
            ts,
            st["realized"]["binance"] + st["open_by_exch"]["binance"],
            st["realized"]["bybit"] + st["open_by_exch"]["bybit"],
        ])

    def _apply_staleness(self):
        """Force-close CSV "open" trades not updated within OPEN_STALE_SECONDS
        so a dead ghost (position closed while the monitor was down, no closed
        row ever written) cannot inflate the equity curve forever. The booking
        is kept in undo_pnl: if the position reappears it is rolled back, so
        realized is never double-counted."""
        st = self._state
        now = time.time()
        # Positions the exchange reports as genuinely open right now (snapshot)
        # are never force-closed — a maintenance trim of old open rows must not
        # look like a close and double-count their unrealized PnL as realized.
        live = {(t.get("exchange"), t.get("symbol")) for t in self._snap_open}
        for key, t in list(st["open_trades"].items()):
            if now - t["last_ts"] <= config.OPEN_STALE_SECONDS:
                continue
            if (t["exchange"], t["symbol"]) in live:
                continue
            ex = t["exchange"]
            st["realized"][ex] += t["pnl"]
            st["open_by_exch"][ex] -= t["pnl"]
            st["undo_pnl"][key] = {"pnl": t["pnl"]}
            st["open_trades"].pop(key, None)
            st["closed"].append({
                "exchange": ex, "symbol": t["symbol"], "direction": t["direction"],
                "qty": t["qty"], "entry": t["entry"], "mark": t["mark"],
                "pnl": t["pnl"], "realized": t["pnl"],
                "opened_at": t["opened_at"],
                "closed_at": datetime.fromtimestamp(t["last_ts"], tz=timezone.utc
                                                    ).isoformat(timespec="seconds"),
                "stale_closed": True,
            })
            if len(st["closed"]) > CLOSED_KEEP:
                st["closed"] = st["closed"][-CLOSED_KEEP:]
        if len(st["undo_pnl"]) > 100:   # bound it
            st["undo_pnl"] = dict(list(st["undo_pnl"].items())[-100:])

    def _load_snapshot(self):
        """Read snapshot.json (exchange truth for open positions + unrealized)."""
        snap = perfio.read_snapshot()
        now = time.time()
        if not snap:
            self._snap_seen = False
            self._snap_stale = True
            self._snap_generated = None
            return
        self._snap_seen = True
        gen = self._parse_epoch((snap.get("generated") or "").strip())
        self._snap_stale = gen is None or (now - gen) > config.SNAPSHOT_STALE_SECONDS
        self._snap_generated = snap.get("generated")
        self._snap_account = snap.get("account") or {}
        open_list = []
        unreal = {"binance": 0.0, "bybit": 0.0}
        for ex in ("binance", "bybit"):
            for sym, t in (snap.get(ex) or {}).items():
                open_list.append({
                    "exchange": ex, "symbol": t.get("symbol", sym),
                    "direction": t.get("direction", ""),
                    "qty": t.get("qty", ""), "entry": t.get("entry", ""),
                    "sl": t.get("sl", ""), "tp": t.get("tp", ""),
                    "mark": t.get("mark", ""), "pnl": t.get("pnl", 0),
                    "opened_at": t.get("opened_at", ""),
                })
                unreal[ex] += float(t.get("pnl") or 0)
        self._snap_open = open_list
        self._snap_unrealized = unreal

    def refresh(self):
        """Incremental update: read new CSV lines + log tail, rebuild payload."""
        try:
            with self._lock:
                for line in self._read_new_perf_lines():
                    try:
                        row = next(csv.reader([line]))
                    except Exception:
                        continue
                    if len(row) < 10:
                        continue
                    d = {h: v for h, v in zip(PERF_HEADER, row)}
                    if len(row) < 11:
                        d["exchange"] = "binance"   # legacy rows: no exchange column
                    self._process_row(d)
                self._load_snapshot()
                self._apply_staleness()
                self._payload = json.dumps(
                    self._build_payload(), default=str
                ).encode("utf-8")
            self._error = None
        except Exception as e:  # never kill the refresh loop
            self._error = f"{type(e).__name__}: {e}"

    # ------------------------------------------------------------------
    # Payload
    # ------------------------------------------------------------------
    @staticmethod
    def _downsample(pts, max_n=SERIES_MAX):
        n = len(pts)
        if n <= max_n:
            return pts
        idx = sorted(
            {0, n - 1} | {int(i * n / max_n) for i in range(max_n)}
        )
        return [pts[i] for i in idx]

    def _build_payload(self):
        st = self._state
        start = {
            "binance": config.DASH_START_BALANCE_BINANCE,
            "bybit": config.DASH_START_BALANCE_BYBIT,
        }
        unreal = dict(self._snap_unrealized if self._snap_seen else st["open_by_exch"])
        equity = {}
        for ex in ("binance", "bybit"):
            equity[ex] = start[ex] + st["realized"][ex] + unreal[ex]
        series = self._downsample(st["series"])
        open_list = list(self._snap_open)
        closed_list = list(reversed(st["closed"][-200:]))
        verify = self._build_verify(start, st, unreal)
        metrics = self._compute_metrics(st, series, start, equity)
        return {
            "generated": datetime.now(timezone.utc).isoformat(),
            "start": start,
            "equity": equity,
            "realized": st["realized"],
            "unrealized": unreal,
            "verify": verify,
            "verify_tolerance": config.PNL_VERIFY_TOLERANCE_USD,
            "stale": self._snap_stale,
            "snapshot_generated": self._snap_generated or "",
            "first_pt": series[0] if series else None,
            "series": series,
            "open": open_list,
            "closed": closed_list,
            "metrics": metrics,
            "activity": self._read_log_tail(),
            "error": self._error,
        }

    def _compute_metrics(self, st, series, start, equity):
        """Compute win rate, avg win/loss, max drawdown, profit factor from
        closed trades and equity series."""
        closed = st["closed"]
        total = len(closed)
        if total == 0:
            return {
                "total_trades": 0, "wins": 0, "losses": 0,
                "win_rate": 0, "avg_win": 0, "avg_loss": 0,
                "profit_factor": 0, "max_drawdown": 0, "max_dd_pct": 0,
                "best_trade": 0, "worst_trade": 0, "expectancy": 0,
                "total_pnl": 0,
            }
        pnls = [t["pnl"] for t in closed]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        total_pnl = sum(pnls)
        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        gross_profit = sum(wins) if wins else 0
        gross_loss = abs(sum(losses)) if losses else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0)
        win_rate = len(wins) / total * 100 if total else 0
        # Expectancy: avg $ gained per trade
        expectancy = total_pnl / total if total else 0

        # Max drawdown from equity series
        max_dd = 0.0
        max_dd_pct = 0.0
        peak = 0.0
        for pt in series:
            val = pt[1] + pt[2]  # combined equity
            total_start = (start.get("binance") or 0) + (start.get("bybit") or 0)
            eq = val + total_start
            if eq > peak:
                peak = eq
            dd = peak - eq
            if dd > max_dd:
                max_dd = dd
                max_dd_pct = (dd / peak * 100) if peak > 0 else 0

        return {
            "total_trades": total,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(win_rate, 1),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "profit_factor": round(profit_factor, 2),
            "max_drawdown": round(max_dd, 2),
            "max_dd_pct": round(max_dd_pct, 1),
            "best_trade": round(max(pnls), 2) if pnls else 0,
            "worst_trade": round(min(pnls), 2) if pnls else 0,
            "expectancy": round(expectancy, 2),
            "total_pnl": round(total_pnl, 2),
        }

    def _build_verify(self, start: dict, st: dict, unreal: dict) -> dict:
        """Compare dash's computed PnL against the exchange's own account
        state (snapshotted by the monitors every poll):

            exchange PnL  = exchange equity (margin balance) - starting balance
            dash PnL      = realized (CSV closed rows) + unrealized (positions)

        The delta is surfaced so a start-balance misconfig, an untracked
        position, or a gap in the CSV never hides the mismatch. Only reported
        for exchanges whose account state has actually been snapshotted."""
        verify = {}
        for ex in ("binance", "bybit"):
            acc = self._snap_account.get(ex)
            if not acc or not isinstance(acc, dict):
                continue
            exch_equity = acc.get("equity")
            if exch_equity is None:
                continue
            exch_pnl = exch_equity - start[ex]
            dash_pnl = st["realized"][ex] + unreal[ex]
            delta = exch_pnl - dash_pnl
            verify[ex] = {
                "exchange_pnl": round(exch_pnl, 2),
                "dash_pnl": round(dash_pnl, 2),
                "delta": round(delta, 2),
                "start_unset": start[ex] == 0,
                "exchange_equity": round(exch_equity, 2),
                "exchange_wallet": (round(acc["wallet"], 2)
                                    if acc.get("wallet") is not None else None),
                "exchange_unrealized": (round(acc["unrealized"], 2)
                                        if acc.get("unrealized") is not None else None),
                "dash_unrealized": round(unreal[ex], 2),
                "dash_realized": round(st["realized"][ex], 2),
                "start": start[ex],
                "ok": abs(delta) <= config.PNL_VERIFY_TOLERANCE_USD,
                "fetched_at": acc.get("fetched_at", ""),
            }
        return verify

    def payload(self):
        return self._payload


# ---------------------------------------------------------------------------
# HTTP layer — serves only the cached snapshot, never touches the files
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    ds = None

    def _send(self, body: bytes, ctype: str):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self._send(PAGE.encode("utf-8"), "text/html; charset=utf-8")
        elif self.path == "/api/data":
            self._send(Handler.ds.payload(), "application/json")
        elif self.path == "/healthz":
            self._send(b"ok", "text/plain")
        else:
            self.send_error(404)

    def log_message(self, fmt, *args):
        pass  # keep the console quiet


# ---------------------------------------------------------------------------
# Page (dark theme, zero-dependency canvas chart)
# ---------------------------------------------------------------------------

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>liq — exchange dashboard</title>
<style>
  :root{--bg:#0a0d12;--panel:#12151c;--card:#161a24;--line:#1e2330;--txt:#e2e4e9;
        --dim:#6b7394;--accent:#3b82f6;--bin:#00b4ff;--byb:#ff8c1a;
        --pos:#22c55e;--neg:#ef4444;--warn:#eab308;}
  *{box-sizing:border-box;margin:0;padding:0;}
  body{background:var(--bg);color:var(--txt);font:13px/1.5 'SF Mono','Cascadia Code','Consolas',monospace;
       padding:16px 20px;max-width:1400px;margin:0 auto;}
  h1{font-size:15px;margin-bottom:14px;font-weight:600;letter-spacing:.03em;}
  h1 span{color:var(--dim);font-weight:400;font-size:12px;}
  .row{display:flex;gap:10px;margin-bottom:10px;flex-wrap:wrap;}
  .card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px 14px;
        flex:1;min-width:160px;}
  .card .label{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:var(--dim);margin-bottom:2px;}
  .card .value{font-size:22px;font-weight:700;line-height:1.2;}
  .card .sub{font-size:11px;color:var(--dim);margin-top:2px;}
  .card .sub b{color:var(--txt);font-weight:600;}
  .up{color:var(--pos);} .down{color:var(--neg);} .flat{color:var(--dim);}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:8px;
         padding:12px 14px;margin-bottom:10px;}
  .panel h2{font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:.1em;
            margin-bottom:8px;font-weight:600;}
  canvas{width:100%;height:240px;display:block;}
  table{width:100%;border-collapse:collapse;font-size:12px;}
  th{color:var(--dim);font-weight:600;text-align:left;padding:3px 6px;border-bottom:1px solid var(--line);
     font-size:10px;text-transform:uppercase;letter-spacing:.06em;}
  td{padding:3px 6px;border-bottom:1px solid #151923;}
  .tag{display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;font-weight:700;
       letter-spacing:.03em;}
  .tag.bin{background:#00b4ff18;color:var(--bin);} .tag.byb{background:#ff8c1a18;color:var(--byb);}
  .tag.long{background:#22c55e18;color:var(--pos);} .tag.short{background:#ef444418;color:var(--neg);}
  .tag.ok{background:#22c55e18;color:var(--pos);} .tag.warn{background:#ef444418;color:var(--neg);}
  .r{text-align:right;} .muted{color:var(--dim);}
  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px;}
  .metrics{display:grid;grid-template-columns:repeat(auto-fit, minmax(120px, 1fr));gap:8px;margin-bottom:10px;}
  .metric{background:var(--card);border:1px solid var(--line);border-radius:6px;padding:8px 10px;text-align:center;}
  .metric .label{font-size:9px;text-transform:uppercase;letter-spacing:.1em;color:var(--dim);}
  .metric .val{font-size:18px;font-weight:700;margin:2px 0;}
  .metric .val.sm{font-size:14px;}
  .foot{margin-top:8px;color:var(--dim);font-size:11px;}
  @media(max-width:820px){.grid2{grid-template-columns:1fr;}.row{flex-direction:column;}}
</style>
</head>
<body>
<h1>liq <span>· exchange performance dashboard</span></h1>

<div id="stale" style="display:none;background:#ef444412;border:1px solid #ef4444;color:#ef4444;
     border-radius:6px;padding:8px 12px;margin-bottom:10px;font-size:12px;">
  STALE — snapshot older than <span id="stale-sec"></span>s. Bot offline or exchange unreachable.
</div>

<!-- Equity cards -->
<div class="row">
  <div class="card"><div class="label" style="color:var(--bin)">Binance</div>
    <div class="value" id="eq-bin">—</div>
    <div class="sub" id="sub-bin">—</div></div>
  <div class="card"><div class="label" style="color:var(--byb)">Bybit</div>
    <div class="value" id="eq-byb">—</div>
    <div class="sub" id="sub-byb">—</div></div>
  <div class="card"><div class="label">Combined</div>
    <div class="value" id="eq-tot">—</div>
    <div class="sub" id="sub-tot">—</div></div>
</div>

<!-- Performance metrics -->
<div class="metrics" id="metrics">
  <div class="metric"><div class="label">Win Rate</div><div class="val" id="m-wr">—</div></div>
  <div class="metric"><div class="label">Profit Factor</div><div class="val" id="m-pf">—</div></div>
  <div class="metric"><div class="label">Avg Win</div><div class="val sm up" id="m-aw">—</div></div>
  <div class="metric"><div class="label">Avg Loss</div><div class="val sm down" id="m-al">—</div></div>
  <div class="metric"><div class="label">Expectancy</div><div class="val sm" id="m-exp">—</div></div>
  <div class="metric"><div class="label">Max Drawdown</div><div class="val sm down" id="m-dd">—</div></div>
  <div class="metric"><div class="label">Best Trade</div><div class="val sm up" id="m-bt">—</div></div>
  <div class="metric"><div class="label">Worst Trade</div><div class="val sm down" id="m-wt">—</div></div>
  <div class="metric"><div class="label">Total Trades</div><div class="val sm" id="m-tt">—</div></div>
  <div class="metric"><div class="label">Total PnL</div><div class="val sm" id="m-tpnl">—</div></div>
</div>

<!-- PnL verification -->
<div class="panel">
  <h2>PnL Verification <span style="font-weight:400;font-size:10px;color:var(--dim)">
    dash vs exchange account</span></h2>
  <div id="verify"><span class="muted">waiting for snapshot…</span></div>
</div>

<!-- Equity curve -->
<div class="panel"><h2>Equity Curve</h2><canvas id="chart"></canvas></div>

<!-- Positions -->
<div class="grid2">
  <div class="panel"><h2>Open Positions</h2><div id="open">—</div></div>
  <div class="panel"><h2>Recently Closed</h2><div id="closed">—</div></div>
</div>

<div class="panel"><h2>Activity</h2><div id="activity" style="font-size:11px;max-height:200px;overflow:auto;">—</div></div>

<div class="foot" id="foot">—</div>

<script>
const REFRESH_MS = __REFRESH__;
const E = (id)=>document.getElementById(id);
const fmt = (v,d=2)=> v==null||isNaN(v) ? "—" : Number(v).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const cls = (v)=> v>0?"up":(v<0?"down":"flat");
const sym = (v)=> v>0?"+":"";
const exlbl = (ex)=>`<span class="tag ${ex==="bybit"?"byb":"bin"}">${ex}</span>`;
const dirlbl = (d)=>`<span class="tag ${d==="long"?"long":"short"}">${d.toUpperCase()}</span>`;
const csv_short = (t)=>{ if(!t) return "—"; const d=new Date(t); if(isNaN(d)) return t; return d.toTimeString().slice(0,8); };

function drawChart(series, startBal){
  const cv=E("chart"), dpr=window.devicePixelRatio||1, r=cv.getBoundingClientRect();
  const W=r.width, H=240;
  cv.width=W*dpr; cv.height=H*dpr;
  const ctx=cv.getContext("2d"); ctx.scale(dpr,dpr); ctx.clearRect(0,0,W,H);
  if(!series||series.length<2){ ctx.fillStyle="#6b7394"; ctx.font="12px monospace"; ctx.fillText("no data yet",12,20); return; }

  const xs=series.map(p=>p[0]);
  const vals0=series.map(p=>p[1]), vals1=series.map(p=>p[2]);
  const combined=series.map((p,i)=>vals0[i]+vals1[i]);
  const total_start = (startBal.binance||0) + (startBal.bybit||0);
  const absCombined = combined.map(v=>v+total_start);

  // Fixed start: y-axis always includes 0 (starting balance)
  const allVals = [total_start, ...absCombined];
  const lo = Math.min(...allVals);
  const hi = Math.max(...allVals);
  const pad = (hi-lo)*0.1 || 1;
  const yLo = lo - pad;
  const yHi = hi + pad;

  const t0=xs[0], t1=xs[xs.length-1];
  const X = (i) => i/(series.length-1||1)*W;
  const Y = v => H - (v-yLo)/(yHi-yLo)*H;

  // Grid lines
  ctx.strokeStyle="#1a1e2a"; ctx.lineWidth=1;
  for(let i=0;i<=5;i++){
    const y=H*i/5;
    ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(W,y); ctx.stroke();
    const v = yHi - (yHi-yLo)*(i/5);
    ctx.fillStyle="#4a5068"; ctx.font="10px monospace"; ctx.fillText(fmt(v,0),4,y-3);
  }

  // Zero/start line
  if(yLo < total_start && yHi > total_start){
    ctx.strokeStyle="#3b82f640"; ctx.lineWidth=1; ctx.setLineDash([4,4]);
    const y=Y(total_start);
    ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(W,y); ctx.stroke();
    ctx.setLineDash([]);
  }

  // Combined equity (filled area)
  ctx.beginPath();
  ctx.moveTo(X(0), Y(absCombined[0]));
  for(let i=1;i<series.length;i++) ctx.lineTo(X(i), Y(absCombined[i]));
  ctx.lineTo(X(series.length-1), H);
  ctx.lineTo(X(0), H);
  ctx.closePath();
  const grad = ctx.createLinearGradient(0,0,0,H);
  grad.addColorStop(0, "rgba(59,130,246,0.15)");
  grad.addColorStop(1, "rgba(59,130,246,0.01)");
  ctx.fillStyle = grad;
  ctx.fill();

  // Combined line
  ctx.strokeStyle="#3b82f6"; ctx.lineWidth=1.8; ctx.beginPath();
  series.forEach((p,i)=>{ const x=X(i),y=Y(absCombined[i]); i?ctx.lineTo(x,y):ctx.moveTo(x,y); });
  ctx.stroke();

  // Binance line
  ctx.strokeStyle="#00b4ff"; ctx.lineWidth=1.2; ctx.beginPath();
  const binAbs = vals0.map(v=>v+(startBal.binance||0));
  series.forEach((p,i)=>{ const x=X(i),y=Y(binAbs[i]); i?ctx.lineTo(x,y):ctx.moveTo(x,y); });
  ctx.stroke();

  // Bybit line
  ctx.strokeStyle="#ff8c1a"; ctx.lineWidth=1.2; ctx.beginPath();
  const bybAbs = vals1.map(v=>v+(startBal.bybit||0));
  series.forEach((p,i)=>{ const x=X(i),y=Y(bybAbs[i]); i?ctx.lineTo(x,y):ctx.moveTo(x,y); });
  ctx.stroke();

  // Legend
  const lx = W - 180;
  ctx.font="10px monospace";
  ctx.fillStyle="#3b82f6"; ctx.fillRect(lx,6,12,2); ctx.fillText("combined",lx+16,10);
  ctx.fillStyle="#00b4ff"; ctx.fillRect(lx+80,6,12,2); ctx.fillText("binance",lx+96,10);
  ctx.fillStyle="#ff8c1a"; ctx.fillRect(lx+140,6,12,2); ctx.fillText("bybit",lx+156,10);

  // Current value dot
  const lastY = Y(absCombined[absCombined.length-1]);
  ctx.beginPath(); ctx.arc(W-2, lastY, 3, 0, Math.PI*2);
  ctx.fillStyle="#3b82f6"; ctx.fill();
}

function render(d){
  const st=d.start||{}, eq=d.equity||{}, rz=d.realized||{}, un=d.unrealized||{};
  const m=d.metrics||{};

  // Stale banner
  if(d.stale){ E("stale").style.display="block";
    E("stale-sec").textContent=Math.round((Date.now()-new Date(d.snapshot_generated||Date.now()))/1000)||"?";
  } else { E("stale").style.display="none"; }

  // Equity cards
  for(const k of ["binance","bybit"]){
    const v=eq[k]??0, s= k==="bybit"?"byb":"bin";
    const pnl = v - (st[k]||0);
    E("eq-"+s).textContent="$"+fmt(v,2);
    E("eq-"+s).className="value "+cls(pnl);
    E("sub-"+s).innerHTML=`realized <b>${sym(rz[k]||0)}$${fmt(rz[k],2)}</b> · unreal <b>${sym(un[k]||0)}$${fmt(un[k],2)}</b>`;
  }
  const tot=eq.binance+eq.bybit;
  const totStart=(st.binance||0)+(st.bybit||0);
  const totPnl=tot-totStart;
  E("eq-tot").textContent="$"+fmt(tot,2);
  E("eq-tot").className="value "+cls(totPnl);
  E("sub-tot").innerHTML=`start <b>$${fmt(totStart,0)}</b> · pnl <b class="${cls(totPnl)}">${sym(totPnl)}$${fmt(totPnl,2)}</b>`;

  // Performance metrics
  if(m.total_trades > 0){
    E("m-wr").textContent=m.win_rate+"%";
    E("m-wr").className="val"+(m.win_rate>=50?" up":" down");
    E("m-pf").textContent=m.profit_factor+"x";
    E("m-pf").className="val"+(m.profit_factor>=1?" up":" down");
    E("m-aw").textContent="$"+fmt(m.avg_win);
    E("m-al").textContent="$"+fmt(m.avg_loss);
    E("m-exp").textContent="$"+fmt(m.expectancy);
    E("m-exp").className="val sm "+cls(m.expectancy);
    E("m-dd").textContent="$"+fmt(m.max_drawdown)+" ("+m.max_dd_pct+"%)";
    E("m-bt").textContent="$"+fmt(m.best_trade);
    E("m-wt").textContent="$"+fmt(m.worst_trade);
    E("m-tt").textContent=m.total_trades+" ("+m.wins+"W / "+m.losses+"L)";
    E("m-tpnl").textContent="$"+fmt(m.total_pnl);
    E("m-tpnl").className="val sm "+cls(m.total_pnl);
  }

  // Verify
  const vf=d.verify||{};
  const vkeys=Object.keys(vf);
  E("verify").innerHTML = vkeys.length?`<table>
    <tr><th></th><th>Dash</th><th>Exchange</th><th class="r">Delta</th></tr>
    ${vkeys.map(k=>{
      const v=vf[k], ok=v.ok;
      const badge=`<span class="tag ${ok?"ok":"warn"}">${ok?"OK":"WARN"}</span>`;
      const exPnlCell = v.start_unset
        ? `<span class="muted">n/a — set DASH_START_BALANCE_*</span>`
        : `${sym(v.exchange_pnl)}$${fmt(v.exchange_pnl,2)}`;
      const deltaCell = v.start_unset
        ? `<span class="muted">n/a</span>`
        : `<span class="${cls(v.delta)}">${sym(v.delta)}$${fmt(v.delta,2)}</span>
           <div class="muted" style="font-size:9px">tol ±$${fmt(d.verify_tolerance,0)}</div>`;
      return `<tr><td>${exlbl(k)} ${badge}</td>
        <td>${sym(v.dash_pnl)}$${fmt(v.dash_pnl,2)} <span class="muted" style="font-size:10px">real ${sym(v.dash_realized)}$${fmt(v.dash_realized,2)}</span></td>
        <td>${exPnlCell} <span class="muted" style="font-size:10px">eq $${fmt(v.exchange_equity,2)}</span></td>
        <td class="r">${deltaCell}</td></tr>`;
    }).join("")}</table>`
    :"<span class='muted'>waiting for exchange account snapshot…</span>";

  // Chart
  drawChart(d.series, st);

  // Open positions
  const rows_o=(d.open||[]).map(t=>`<tr><td>${exlbl(t.exchange)}</td><td>${t.symbol}</td><td>${dirlbl(t.direction)}</td>
    <td class="r">${fmt(t.qty,4)}</td><td class="r">${fmt(t.entry,6)}</td><td class="r">${fmt(t.sl,6)}</td>
    <td class="r">${fmt(t.tp,6)}</td><td class="r">${fmt(t.mark,6)}</td>
    <td class="r ${cls(t.pnl)}">${sym(t.pnl)}$${fmt(t.pnl,2)}</td></tr>`).join("");
  E("open").innerHTML= d.open&&d.open.length?`<table><tr><th>Ex</th><th>Symbol</th><th>Dir</th><th class="r">Qty</th><th class="r">Entry</th><th class="r">SL</th><th class="r">TP</th><th class="r">Mark</th><th class="r">PnL</th></tr>${rows_o}</table>`:"<span class='muted'>none open</span>";

  // Closed trades
  const rows_c=(d.closed||[]).slice(0,50).map(t=>`<tr><td>${exlbl(t.exchange)}</td><td>${t.symbol}</td><td>${dirlbl(t.direction)}</td>
    <td class="r">${fmt(t.entry,6)}</td><td>${csv_short(t.closed_at)}</td>
    <td class="r ${cls(t.realized)}">${sym(t.realized)}$${fmt(t.realized,2)}</td></tr>`).join("");
  E("closed").innerHTML= d.closed&&d.closed.length?`<table><tr><th>Ex</th><th>Symbol</th><th>Dir</th><th class="r">Entry</th><th>Closed</th><th class="r">PnL</th></tr>${rows_c}</table>`:"<span class='muted'>none closed</span>";

  // Activity
  const rows_a=(d.activity||[]).slice(-40).reverse().map(e=>{
    const ev=e.event||"", ex=e.exchange||"", t=e.time||e.closed_at||e.opened_at||"";
    let dt=e.symbol||""; if(e.direction) dt+=` ${e.direction}`;
    let det=""; if(e.sl!=null) det+=` sl=${fmt(e.sl,6)}`; if(e.tp!=null) det+=` tp=${fmt(e.tp,6)}`;
    if(e.error) det+=` <span style="color:var(--neg)">${e.error}</span>`;
    return `<div style="padding:2px 0;border-bottom:1px solid #151923;"><span class="muted">${csv_short(t)}</span> ${ev} ${dt} ${ex?exlbl(ex):""} <span class="muted">${det}</span></div>`;
  }).join("");
  E("activity").innerHTML=rows_a||"<span class='muted'>no activity</span>";

  E("foot").textContent=`snapshot ${d.generated}${d.error?" · error: "+d.error:""} · open ${(d.open||[]).length} · closed ${(d.closed||[]).length} · series ${(d.series||[]).length}pts${d.stale?" · STALE":""}`;
}

async function poll(){
  try{ const r=await fetch("/api/data"); render(await r.json()); }
  catch(e){ E("foot").textContent="update error: "+e; }
}
poll(); setInterval(poll, REFRESH_MS);
</script>
</body>
</html>
""".replace("__REFRESH__", str(int(config.DASH_REFRESH_SECONDS * 1000)))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ds = DataSource()
    Handler.ds = ds

    def _refresh_loop():
        while True:
            ds.refresh()
            time.sleep(config.DASH_REFRESH_SECONDS)

    ds.refresh()  # initial snapshot before serving
    threading.Thread(target=_refresh_loop, daemon=True).start()

    httpd = ThreadingHTTPServer((config.DASH_HOST, config.DASH_PORT), Handler)
    print(f"  [dash] http://{config.DASH_HOST}:{config.DASH_PORT}")
    print(f"  [dash] watching {config.PERFORMANCE_CSV} + {config.TRADE_LOG_FILE}")
    print(f"  [dash] start balances: binance={config.DASH_START_BALANCE_BINANCE:.0f} bybit={config.DASH_START_BALANCE_BYBIT:.0f}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
