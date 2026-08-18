# Deploying on Railway

This project deploys as a **single container** that runs every stage in one
process: `entrypoint.py` supervises `dash`, `liq_stream`, `liq_bucket`,
`run_bot`, and `maintenance` as subprocesses (starts them, restarts any that
crash, and shuts them down cleanly on SIGTERM). Railway's default `CMD`
(`python entrypoint.py`) does this with no extra config.

> The Dockerfile already sets `LIQ_DATA_DIR=/data` and runs as a non-root
> user, so the only things you must configure on Railway are the **volume**
> (for persistence) and the **variables** (API keys).

---

## 1. Push the repo

```bash
git add -A
git commit -m "cloud deploy"
git push origin main
```

Make sure `bybit_mirror.py`, `dash.py`, and `maintenance.py` are committed —
`run_bot.py` imports `bybit_mirror.py` at the top level, so a clone missing it
crashes on startup.

## 2. Create the Railway service

1. Railway → **New Project** → **Deploy from GitHub repo** → pick this repo.
2. Railway detects the `Dockerfile` and builds it. The container starts
   `entrypoint.py`, which brings all five stages up.
3. On the service, open the **Deployments** tab and watch the logs — you
   should see `[supervisor] starting 5 stages in one container` and each
   `[stage]` line.

## 3. Add the persistent volume (critical)

Without a volume, everything in `/data` lives in the container's writable
layer, which Railway **replaces on every restart/redeploy** — your
`liquidations.csv`, `liq_bucket_state.json` streak counters, trade log, and
performance history would reset constantly.

1. Service → **Settings** → **Volumes** → **New Volume**.
2. Mount path: **`/data`** (matches `LIQ_DATA_DIR=/data` set in the Dockerfile).
   The image already pre-creates `/data` owned by the app user, so the mount
   is writable by the non-root user out of the box.
3. Attach the volume and redeploy.

`maintenance.py` trims the unbounded files (`liquidations.csv`,
`performance.csv`, `trade_log.jsonl`) back to their caps hourly, so a small
volume won't fill up.

## 4. Set the variables (secrets go here, not in the repo)

Service → **Variables**. Add (all except keys are optional — defaults are in
`config.py` / `.env.example`):

| Variable | Value / note |
|---|---|
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | your keys |
| `LIQ_TESTNET` | `true` (default) until you're sure |
| `LIQ_RISK_USD`, `LIQ_LEVERAGE` | per-trade risk |
| `BYBIT_ENABLED`, `BYBIT_API_KEY`, `BYBIT_API_SECRET` | set only if mirroring to Bybit |
| `BYBIT_TESTNET` | `true` (demo) |
| `BYBIT_RISK_USD`, `BYBIT_LEVERAGE`, `BYBIT_MAX_CONCURRENT_POSITIONS` | Bybit per-trade risk |

`PORT` is injected by Railway automatically — do **not** set it. Never commit
a real `.env`; the repo only ships the `.env.example` template.

## 5. Give the dashboard a domain

`dash.py` is already running inside the container (it's a `entrypoint.py`
stage) and binds `0.0.0.0:$PORT` when `PORT` is present, so Railway's proxy
can reach it.

1. Service → **Settings** → **Networking** → **Generate Domain**.
   Railway gives you a `*.up.railway.app` URL and routes traffic to the
   container's `$PORT`.
2. For a custom domain: **Custom Domain** → add e.g. `dash.yourdomain.com`
   and point its DNS CNAME/ALIAS at your `*.up.railway.app` URL.

   ```
   https://liq-production.up.railway.app    (auto-generated)
   https://dash.example.com                 (custom domain → CNAME to the above)
   ```

3. Optional: **HTTP Healthcheck** → path **`/healthz`**. Railway pings
   `http://localhost:$PORT/healthz` (`dash.py` returns 200 `ok`) and only
   marks the deployment healthy when it responds.

⚠️ The dashboard is **read-only and unauthenticated** — anyone with the URL
can see your positions and PnL. Treat the URL as private; if you need it
public, put it behind a Railway-provided service or an auth proxy.

## 6. How data is stored on Railway

| Concern | Answer |
|---|---|
| Where state lives | In the volume mounted at `/data` (`liquidations.csv`, `liq_bucket_state.json`, `liq_signals.jsonl`, `trade_log.jsonl`, `performance.csv`, `trades.json`, `snapshot.json`) |
| What persists | The volume survives deploys, restarts, and rebuilds. Bucket streak counters, the trade log, performance history, and the open-trade ledger all carry over |
| What does NOT persist | The container's own filesystem outside `/data` (source, pyc caches, temp files) — rebuilt each deploy |
| Disk usage | `maintenance.py` trims the three unbounded files to `MAX_LIQ_CSV_ROWS` / `MAX_PERFORMANCE_ROWS` / `MAX_TRADE_LOG_LINES` (defaults 250k / 50k / 50k rows) hourly |
| Not persisted (by design) | `liq_signals.jsonl` is never trimmed (run_bot tails it live); it grows ~1 MB/day — delete it manually if it ever matters |
| Backup | A Railway volume is **not a backup** — take periodic copies (see below) |
| Limit | One volume = one container; our single-container design is already the right shape. Free/Hobby volume sizes are limited — if you hit the cap, lower the `MAX_*_ROWS` caps |

**Downloading state** (manual backup):

```bash
# easiest: open the volume in the Railway dashboard and download, or
# copy out with a one-off job that mounts the volume at /data and streams it
```

**Wiping state** (fresh start): detach the volume (or set a new mount path)
and redeploy — the bot starts clean with empty bucket counters.

## 7. Restarts, bans, and resilience

- Railway restarts the container if `entrypoint.py` exits; `entrypoint.py`
  restarts any individual stage that dies. So a single stage crash never
  takes the whole bot down.
- `liq_bucket.py` resumes from `liq_bucket_state.json`; `run_bot.py` tails
  `liq_signals.jsonl` from its current position — mid-stream restarts are
  safe.
- Binance IP bans are handled in-process by `ratelimit.py` (fail-fast, no
  polling into a ban) — a ban pauses that process only.
- Entry orders use **Binance user data WebSocket** for instant fill
  notifications (no REST polling). A cancel timer handles unfilled order
  timeouts. The same pattern applies to Bybit via its private v5 WebSocket.
- Time: Binance rejects requests with a skewed client clock. Railway's NTP
  is normally fine; if you ever see timestamp errors, that's the first thing
  to check.

## 8. Architecture overview

```
liq_stream.py  ──→  liquidations.csv
                         ↓
liq_bucket.py   ──→  liq_signals.jsonl
                         ↓
run_bot.py      ──→  strategy.decide_direction()
                         ↓
              ┌─────────────────────┐
              │  trader.py (Binance)│  ←── Binance user data WebSocket
              │  bybit_mirror.py    │  ←── Bybit v5 private WebSocket
              └─────────────────────┘
                         ↓
              performance.csv / trade_log.jsonl / trades.json / snapshot.json
                         ↓
              dash.py (read-only dashboard)
```

All entry orders are LIMIT orders priced at the interest candle's exact close.
Both exchanges use private WebSockets for instant fill notifications — no
REST polling. SL and TP are both derived from the Dynamic ATR (6-candle
window), giving a ~1:1 R:R by construction.
