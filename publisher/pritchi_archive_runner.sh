#!/bin/zsh
set -euo pipefail

ROOT="/Users/d.yudin/apps/Kindlysupport_posting"
STATE_DIR="$ROOT/tmp/pritchi_archive"
PID_FILE="$STATE_DIR/runner.pid"
OUT_FILE="$STATE_DIR/runner.out"
SCRIPT="$ROOT/publisher/pritchi_archive_scraper.py"

mkdir -p "$STATE_DIR"

start_runner() {
  if [[ -f "$PID_FILE" ]]; then
    local existing
    existing="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$existing" ]] && kill -0 "$existing" 2>/dev/null; then
      echo "already running pid=$existing"
      exit 0
    fi
    rm -f "$PID_FILE"
  fi

  nohup /opt/homebrew/bin/python3 -u - <<'PY' >>"$OUT_FILE" 2>&1 &
import subprocess
import time

ROOT = "/Users/d.yudin/apps/Kindlysupport_posting"
SCRIPT = ROOT + "/publisher/pritchi_archive_scraper.py"

while True:
    subprocess.run(
        ["/opt/homebrew/bin/python3", SCRIPT, "discover", "--source", "archive", "--limit", "200"],
        cwd=ROOT,
        check=False,
    )
    subprocess.run(
        [
            "/opt/homebrew/bin/python3",
            SCRIPT,
            "import",
            "--limit",
            "20",
            "--delay-min",
            "5",
            "--delay-max",
            "15",
        ],
        cwd=ROOT,
        check=False,
    )
    time.sleep(60)
PY
  echo $! >"$PID_FILE"
  echo "started pid=$(cat "$PID_FILE")"
}

stop_runner() {
  if [[ ! -f "$PID_FILE" ]]; then
    echo "not running"
    exit 0
  fi
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    echo "stopped pid=$pid"
  else
    echo "stale pid file removed"
  fi
  rm -f "$PID_FILE"
}

logs_runner() {
  touch "$OUT_FILE"
  tail -f "$OUT_FILE"
}

status_runner() {
  /opt/homebrew/bin/python3 "$SCRIPT" status
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "runner_pid=$pid alive=yes"
      exit 0
    fi
    echo "runner_pid=$pid alive=no"
    exit 1
  fi
  echo "runner_pid=- alive=no"
}

case "${1:-}" in
  start) start_runner ;;
  stop) stop_runner ;;
  logs) logs_runner ;;
  status) status_runner ;;
  *)
    echo "usage: $0 {start|stop|logs|status}"
    exit 1
    ;;
esac
