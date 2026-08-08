"""
dash.py  —  lightweight read-only dashboard
=================================================
Serves a tiny interactive page over HTTP showing, per exchange (binance /
bybit): equity curve, realized / unrealized PnL, open positions, closed
positions and recent activity. It is strictly observational:

  - reads data/performance.csv and data/trade_log.jsonl READ-ONLY,
  - tails only appended bytes (no full re-reads per poll),
  - a single background thread refreshes the snapshot; HTTP requests just
    serve the cached snapshot (no file I/O on the request path),
  - writes nothing, opens no sockets to exchanges.

It therefore cannot clog or interfere with the pipeline.

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
        self._perf_header = False
        self._state = self._fresh_state()
        self._payload = b"{}"
        self._error = None
        self._lock = threading.Lock()

    def _fresh_state(self):
        return {
            "realized": {"binance": 0.0, "bybit": 0.0},
            "open_by_exch": {"binance": 0.0, "bybit": 0.0},
            "open_trades": {},      # (ex, symbol, direction) -> trade dict
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
        if size < self._perf_pos:
            # File was rewritten/truncated (e.g. legacy-header migration):
            # reset and re-read from the top.
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
            t = st["open_trades"].pop(key, None)
            if t is not None:
                st["realized"][ex] += t["pnl"]
                st["open_by_exch"][ex] -= t["pnl"]
                t["realized"] = t["pnl"]
                t["closed_at"] = (d.get("timestamp_utc") or "").strip()
                st["closed"].append(t)
                if len(st["closed"]) > CLOSED_KEEP:
                    st["closed"] = st["closed"][-CLOSED_KEEP:]
        else:
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
        equity = {}
        for ex in ("binance", "bybit"):
            equity[ex] = start[ex] + st["realized"][ex] + st["open_by_exch"][ex]
        series = self._downsample(st["series"])
        open_list = sorted(
            st["open_trades"].values(), key=lambda t: t["first_ts"], reverse=True
        )
        closed_list = list(reversed(st["closed"][-200:]))
        return {
            "generated": datetime.now(timezone.utc).isoformat(),
            "start": start,
            "equity": equity,
            "realized": st["realized"],
            "unrealized": st["open_by_exch"],
            "first_pt": series[0] if series else None,
            "series": series,
            "open": open_list,
            "closed": closed_list,
            "activity": self._read_log_tail(),
            "error": self._error,
        }

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
  :root{--bg:#0f1115;--panel:#171a21;--line:#2a2e38;--txt:#e6e6e6;--dim:#8b93a5;
        --bin:#00b4ff;--byb:#ff8c1a;--pos:#26a69a;--neg:#ef5350;}
  *{box-sizing:border-box;margin:0;padding:0;}
  body{background:var(--bg);color:var(--txt);font:14px/1.45 system-ui,sans-serif;
       padding:18px;max-width:1200px;margin:0 auto;}
  h1{font-size:18px;margin-bottom:14px;} h1 span{color:var(--dim);font-weight:400;}
  .cards{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:14px;}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px;}
  .card .ex{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--dim);}
  .card .val{font-size:26px;font-weight:700;margin:4px 0 2px;}
  .card .sub{font-size:12px;color:var(--dim);}
  .card .sub b{color:var(--txt);font-weight:600;}
  .up{color:var(--pos);} .down{color:var(--neg);}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;
         padding:12px 14px;margin-bottom:14px;}
  .panel h2{font-size:13px;color:var(--dim);text-transform:uppercase;letter-spacing:.08em;
            margin-bottom:8px;}
  canvas{width:100%;height:260px;display:block;}
  table{width:100%;border-collapse:collapse;font-size:13px;}
  th{color:var(--dim);font-weight:600;text-align:left;padding:4px 8px;border-bottom:1px solid var(--line);}
  td{padding:4px 8px;border-bottom:1px solid #1d212b;}
  .lbl{display:inline-block;padding:1px 7px;border-radius:8px;font-size:11px;font-weight:700;}
  .lbl.bin{background:#00b4ff22;color:var(--bin);} .lbl.byb{background:#ff8c1a22;color:var(--byb);}
  .lbl.long{background:#26a69a22;color:var(--pos);} .lbl.short{background:#ef535022;color:var(--neg);}
  .muted{color:var(--dim);} .right{text-align:right;}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;}
  .foot{margin-top:10px;color:var(--dim);font-size:12px;}
  @media(max-width:820px){.cards{grid-template-columns:1fr;}.grid{grid-template-columns:1fr;}}
</style>
</head>
<body>
<h1>liq <span>· per-exchange equity &amp; performance</span></h1>

<div class="cards">
  <div class="card"><div class="ex" style="color:var(--bin)">Binance</div>
    <div class="val" id="eq-bin">—</div>
    <div class="sub" id="sub-bin">realized — · unrealized —</div></div>
  <div class="card"><div class="ex" style="color:var(--byb)">Bybit</div>
    <div class="val" id="eq-byb">—</div>
    <div class="sub" id="sub-byb">realized — · unrealized —</div></div>
  <div class="card"><div class="ex">Combined</div>
    <div class="val" id="eq-tot">—</div>
    <div class="sub" id="sub-tot">binance + bybit</div></div>
</div>

<div class="panel"><h2>Equity curve</h2><canvas id="chart"></canvas></div>

<div class="grid">
  <div class="panel"><h2>Open positions</h2><div id="open">—</div></div>
  <div class="panel"><h2>Recently closed</h2><div id="closed">—</div></div>
</div>

<div class="panel"><h2>Activity</h2><div id="activity" style="font-size:12px;max-height:220px;overflow:auto;">—</div></div>

<div class="foot" id="foot">—</div>

<script>
const REFRESH_MS = __REFRESH__;
const E = (id)=>document.getElementById(id);
const fmt = (v,d=2)=> v==null||isNaN(v) ? "—" : Number(v).toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const cls = (v)=> v>0?"up":(v<0?"down":"");
const sym = (v)=> v>0?"+":"";
const exlbl = (ex)=>`<span class="lbl ${ex==="bybit"?"byb":"bin"}">${ex}</span>`;
const dirlbl = (d)=>`<span class="lbl ${d==="long"?"long":"short"}">${d}</span>`;
const csv_short = (t)=>{ if(!t) return "—"; const d=new Date(t); if(isNaN(d)) return t; return d.toTimeString().slice(0,8); };

function drawChart(series){
  const cv=E("chart"), dpr=window.devicePixelRatio||1, r=cv.getBoundingClientRect();
  cv.width=r.width*dpr; cv.height=260*dpr;
  const ctx=cv.getContext("2d"); ctx.scale(dpr,dpr); ctx.clearRect(0,0,r.width,260);
  if(!series||series.length<2){ ctx.fillStyle="#8b93a5"; ctx.font="13px system-ui"; ctx.fillText("no data yet",12,24); return; }
  const xs=series.map(p=>p[0]), vals0=series.map(p=>p[1]), vals1=series.map(p=>p[2]);
  const all=[0,...vals0,...vals1];
  const t0=xs[0], t1=xs[xs.length-1], lo=Math.min(...all), hi=Math.max(...all);
  const pad=(hi-lo)*0.08||1, y0=lo-pad, y1=hi+pad;
  const X=t=> (t-t0)/(t1-t0||1)*r.width, Y=v=> 260-(v-y0)/(y1-y0)*260;
  ctx.strokeStyle="#212530"; ctx.fillStyle="#8b93a5"; ctx.font="11px system-ui";
  for(let i=1;i<4;i++){
    const y=260*i/4; ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(r.width,y); ctx.stroke();
    const v=y0+(y1-y0)*(1-i/4); ctx.fillText(fmt(v,1),4,y-4);
  }
  if(y0<0&&y1>0){ ctx.strokeStyle="#555"; ctx.setLineDash([4,4]); ctx.beginPath(); const y=Y(0); ctx.moveTo(0,y); ctx.lineTo(r.width,y); ctx.stroke(); ctx.setLineDash([]); }
  const line=(vals,col)=>{ ctx.strokeStyle=col; ctx.lineWidth=1.6; ctx.beginPath();
    series.forEach((p,i)=>{ const x=X(p[0]),y=Y(vals[i]); i?ctx.lineTo(x,y):ctx.moveTo(x,y); }); ctx.stroke(); };
  line(vals0,"#00b4ff"); line(vals1,"#ff8c1a");
  ctx.fillStyle="#00b4ff"; ctx.fillText("binance",r.width-72,14);
  ctx.fillStyle="#ff8c1a"; ctx.fillText("bybit",r.width-30,14);
}

function render(d){
  const st=d.start||{}, eq=d.equity||{}, rz=d.realized||{}, un=d.unrealized||{};
  const tot=eq.binance+eq.bybit;
  for(const k of ["binance","bybit"]){
    const v=eq[k]??0, s= k==="bybit"?"byb":"bin";
    E("eq-"+s).textContent=fmt(v,2);
    E("eq-"+s).className="val "+cls(v-st[k]||0);
    E("sub-"+s).innerHTML=`realized <b>${sym(rz[k]||0)}${fmt(rz[k],2)}</b> · unrealized <b>${sym(un[k]||0)}${fmt(un[k],2)}</b>`;
  }
  E("eq-tot").textContent=fmt(tot,2); E("eq-tot").className="val "+cls(tot-(st.binance||0)-(st.bybit||0));
  E("sub-tot").innerHTML=`start <b>${fmt((st.binance||0)+(st.bybit||0),0)}</b> · realized <b>${sym((rz.binance||0)+(rz.bybit||0))}${fmt((rz.binance||0)+(rz.bybit||0),2)}</b>`;

  const fp=d.first_pt;
  if(fp){ const db=eq.binance-(st.binance||0)-fp[1], dy=eq.bybit-(st.bybit||0)-fp[2];
    E("sub-bin").innerHTML+=` · Δ <b class="${cls(db)}">${sym(db)}${fmt(db,2)}</b>`;
    E("sub-byb").innerHTML+=` · Δ <b class="${cls(dy)}">${sym(dy)}${fmt(dy,2)}</b>`; }

  drawChart(d.series);

  const rows_o=(d.open||[]).map(t=>`<tr><td>${exlbl(t.exchange)}</td><td>${t.symbol}</td><td>${dirlbl(t.direction)}</td>
    <td class="right">${fmt(t.qty,4)}</td><td class="right">${fmt(t.entry,6)}</td><td class="right">${fmt(t.mark,6)}</td>
    <td class="right ${cls(t.pnl)}">${sym(t.pnl)}${fmt(t.pnl,2)}</td></tr>`).join("");
  E("open").innerHTML= d.open&&d.open.length?`<table><tr><th>Ex</th><th>Symbol</th><th>Dir</th><th class="right">Qty</th><th class="right">Entry</th><th class="right">Mark</th><th class="right">PnL</th></tr>${rows_o}</table>`:"<span class='muted'>none</span>";

  const rows_c=(d.closed||[]).map(t=>`<tr><td>${exlbl(t.exchange)}</td><td>${t.symbol}</td><td>${dirlbl(t.direction)}</td>
    <td class="right">${fmt(t.entry,6)}</td><td>${csv_short(t.closed_at)}</td>
    <td class="right ${cls(t.realized)}">${sym(t.realized)}${fmt(t.realized,2)}</td></tr>`).join("");
  E("closed").innerHTML= d.closed&&d.closed.length?`<table><tr><th>Ex</th><th>Symbol</th><th>Dir</th><th class="right">Entry</th><th>Closed</th><th class="right">PnL</th></tr>${rows_c}</table>`:"<span class='muted'>none</span>";

  const rows_a=(d.activity||[]).map(e=>{
    const ev=e.event||"", ex=e.exchange||"", t=e.time||e.closed_at||e.opened_at||"";
    let dt=e.symbol||""; if(e.direction) dt+=` ${e.direction}`;
    let det=""; if(e.sl!=null) det+=` sl=${fmt(e.sl,6)}`; if(e.tp!=null) det+=` tp=${fmt(e.tp,6)}`; if(e.risk_reward!=null) det+=` rr=${fmt(e.risk_reward,2)}`;
    if(e.error) det+=` <span style="color:var(--neg)">${e.error}</span>`;
    return `<div style="padding:2px 0;border-bottom:1px solid #1d212b;"><span class="muted">${csv_short(t)}</span> ${ev} ${dt} ${ex?exlbl(ex):""} <span class="muted">${det}</span></div>`;
  }).join("");
  E("activity").innerHTML=rows_a||"<span class='muted'>none</span>";

  E("foot").textContent=`snapshot ${d.generated}${d.error?" · refresh error: "+d.error:""} · open ${(d.open||[]).length} · closed ${(d.closed||[]).length} · series ${(d.series||[]).length}pts`;
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
