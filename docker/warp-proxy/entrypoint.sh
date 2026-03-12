#!/usr/bin/env bash
set -euo pipefail

WARP_PROXY_PORT="${WARP_PROXY_PORT:-40000}"
WARP_STATE_MARKER="/var/lib/cloudflare-warp/.registered"
WARP_RETRIES="${WARP_RETRIES:-20}"
WARP_RUNTIME_DIR="/var/lib/cloudflare-warp"

log() { printf '[warp-proxy] %s\n' "$*"; }

retry() {
  local attempts="$1"
  shift
  local n=1
  until "$@"; do
    if [[ "$n" -ge "$attempts" ]]; then
      return 1
    fi
    sleep 2
    n=$((n + 1))
  done
}

start_daemon() {
  log "starting warp-svc"
  warp-svc > /tmp/warp-svc.log 2>&1 &
}

wait_daemon() {
  for _ in $(seq 1 40); do
    if warp-cli --accept-tos status >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  log "warp-svc did not start"
  tail -n 200 /tmp/warp-svc.log || true
  return 1
}

registration_missing() {
  local status_out
  status_out="$(warp-cli --accept-tos status 2>&1 || true)"
  if echo "$status_out" | grep -Eqi "RegistrationInfo:[[:space:]]*None|Registration Missing|ApiMismatch|Invalidated"; then
    return 0
  fi
  return 1
}

reset_registration() {
  log "resetting WARP registration state"
  warp-cli --accept-tos disconnect >/dev/null 2>&1 || true
  warp-cli --accept-tos registration delete >/dev/null 2>&1 || true
  rm -f "${WARP_RUNTIME_DIR}/reg.json" "${WARP_RUNTIME_DIR}/metadata.json" "${WARP_RUNTIME_DIR}/settings.json" "${WARP_STATE_MARKER}" || true
  sleep 1
}

register_new() {
  local out
  out="$(warp-cli --accept-tos registration new 2>&1 || true)"
  echo "$out"
  if echo "$out" | grep -Eqi "success|complete|registered"; then
    return 0
  fi
  if echo "$out" | grep -Eqi "Old registration is still around|ApiMismatch|Invalidated|RegistrationInfo:[[:space:]]*None"; then
    return 2
  fi
  return 1
}

register_if_needed() {
  if registration_missing; then
    log "registration missing in daemon status; forcing re-registration"
    reset_registration
  fi

  if [[ ! -f "$WARP_STATE_MARKER" ]]; then
    log "registering WARP client"
    local attempt=1
    while [[ "$attempt" -le "$WARP_RETRIES" ]]; do
      if register_new >/tmp/warp-registration.log 2>&1; then
        break
      fi
      if [[ "$?" -eq 2 ]]; then
        log "registration state mismatch detected, resetting and retrying (${attempt}/${WARP_RETRIES})"
        reset_registration
      else
        log "registration attempt failed (${attempt}/${WARP_RETRIES})"
      fi
      attempt=$((attempt + 1))
      sleep 2
    done
    if [[ "$attempt" -gt "$WARP_RETRIES" ]]; then
      log "registration new failed after retries"
      cat /tmp/warp-registration.log || true
      return 1
    fi
    if [[ -n "${WARP_LICENSE_KEY:-}" ]]; then
      log "applying WARP license"
      retry "$WARP_RETRIES" warp-cli --accept-tos registration license "${WARP_LICENSE_KEY}" || true
    fi
    if registration_missing; then
      log "registration still missing after registration new"
      return 1
    fi
    touch "$WARP_STATE_MARKER"
    log "registration complete"
  else
    log "registration marker exists and daemon reports registration"
  fi
}

configure_proxy_mode() {
  retry "$WARP_RETRIES" warp-cli --accept-tos mode proxy
  retry "$WARP_RETRIES" warp-cli --accept-tos proxy port "$WARP_PROXY_PORT"
  retry "$WARP_RETRIES" warp-cli --accept-tos connect
}

wait_connected() {
  for _ in $(seq 1 60); do
    if warp-cli --accept-tos status 2>/dev/null | grep -qi "Connected"; then
      return 0
    fi
    sleep 1
  done
  log "WARP not connected yet"
  warp-cli --accept-tos status || true
  return 1
}

main() {
  start_daemon
  wait_daemon
  if ! register_if_needed; then
    log "registration failed after retries"
    tail -n 200 /tmp/warp-svc.log || true
  fi
  if ! configure_proxy_mode; then
    log "proxy mode setup failed after retries"
    tail -n 200 /tmp/warp-svc.log || true
  fi
  wait_connected || log "continuing without connected status"
  warp-cli --accept-tos status || true
  log "proxy listening on :${WARP_PROXY_PORT}"
  exec tail -F /tmp/warp-svc.log
}

main "$@"
