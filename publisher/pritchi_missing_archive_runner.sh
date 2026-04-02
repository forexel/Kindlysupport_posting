#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/d.yudin/apps/Kindlysupport_posting"
REC="$ROOT/recovered_pritchi_local_run"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SCRIPT="$ROOT/publisher/pritchi_missing_archive_fill.py"
PID_FILE="$REC/archive_fill.pid"
LOG_FILE="$REC/archive_fill.out"

start() {
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "already_running pid=$(cat "$PID_FILE")"
    return 0
  fi
  cd "$ROOT"
  nohup "$PYTHON_BIN" "$SCRIPT" --limit "${LIMIT:-100}" >"$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  echo "started pid=$(cat "$PID_FILE")"
}

stop() {
  if [[ -f "$PID_FILE" ]]; then
    kill "$(cat "$PID_FILE")" 2>/dev/null || true
    rm -f "$PID_FILE"
    echo "stopped"
  else
    echo "not_running"
  fi
}

status() {
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "alive=yes pid=$(cat "$PID_FILE")"
  else
    echo "alive=no"
  fi
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  restart) stop; start ;;
  *)
    echo "usage: $0 {start|stop|status|restart}"
    exit 1
    ;;
esac
