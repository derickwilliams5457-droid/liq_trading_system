"""
entrypoint.py  —  single-container supervisor (Railway / one-service hosts)
=============================================================================
Docker Compose runs liq_stream.py, liq_bucket.py, and run_bot.py as three
separate containers sharing a ./data volume. Platforms like Railway don't
give you that for free per-service (each service is its own isolated
filesystem, and free/hobby tiers often mean "one service" anyway) — so this
runs all three as subprocesses of ONE process, inside ONE container, sharing
the same local data/ directory the way they do locally.

- Starts liq_stream.py, then waits a bit, then liq_bucket.py, then run_bot.py
  (same staggered order recommended for local/manual startup).
- Streams every subprocess's stdout/stderr to this process's stdout, prefixed
  with the stage name, so Railway's single log view shows all three.
- If any subprocess dies, it's restarted automatically after a short delay
  (this process itself is what Railway's restart policy watches — individual
  stage crashes shouldn't need a full container restart).
- SIGTERM/SIGINT (Railway redeploys, manual stop) cleanly terminates every
  child before exiting.

Usage:
    python entrypoint.py
"""

import signal
import subprocess
import sys
import threading
import time

STAGES = [
    ("dash",        ["python", "-u", "dash.py"],          0),
    ("liq_stream",  ["python", "-u", "liq_stream.py"],    0),
    ("liq_bucket",  ["python", "-u", "liq_bucket.py"],    5),
    ("run_bot",     ["python", "-u", "run_bot.py"],      10),
    ("maintenance", ["python", "-u", "maintenance.py"],  15),
]

RESTART_DELAY_SECONDS = 5

_processes: dict = {}
_shutting_down = threading.Event()


def _stream_output(name: str, proc: subprocess.Popen):
    for line in iter(proc.stdout.readline, ""):
        if not line:
            break
        print(f"[{name}] {line.rstrip()}", flush=True)


def _run_stage(name: str, cmd: list, initial_delay: int):
    if initial_delay:
        print(f"[supervisor] {name} starting in {initial_delay}s...", flush=True)
        time.sleep(initial_delay)

    while not _shutting_down.is_set():
        print(f"[supervisor] launching {name}: {' '.join(cmd)}", flush=True)
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        _processes[name] = proc

        _stream_output(name, proc)
        exit_code = proc.wait()

        if _shutting_down.is_set():
            break

        print(f"[supervisor] {name} exited with code {exit_code} — "
              f"restarting in {RESTART_DELAY_SECONDS}s", flush=True)
        time.sleep(RESTART_DELAY_SECONDS)


def _shutdown(signum, frame):
    print(f"\n[supervisor] received signal {signum}, shutting down all stages...", flush=True)
    _shutting_down.set()
    for name, proc in _processes.items():
        if proc.poll() is None:
            print(f"[supervisor] terminating {name}", flush=True)
            proc.terminate()
    time.sleep(3)
    for name, proc in _processes.items():
        if proc.poll() is None:
            print(f"[supervisor] force-killing {name}", flush=True)
            proc.kill()
    sys.exit(0)


def main():
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    print(f"[supervisor] starting {len(STAGES)} stages in one container: "
          f"{', '.join(s[0] for s in STAGES)}", flush=True)

    threads = []
    for name, cmd, delay in STAGES:
        t = threading.Thread(target=_run_stage, args=(name, cmd, delay), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()