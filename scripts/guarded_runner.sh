#!/usr/bin/env bash
set -euo pipefail

NAME="${1:?missing service name}"
LOG_FILE="${2:?missing log file}"
shift 2

mkdir -p "$(dirname "${LOG_FILE}")"

child_pid=""

stop_children() {
  if [[ -n "${child_pid}" ]] && kill -0 "${child_pid}" >/dev/null 2>&1; then
    kill "${child_pid}" >/dev/null 2>&1 || true
    wait "${child_pid}" >/dev/null 2>&1 || true
  fi
  exit 0
}

trap stop_children TERM INT HUP

while true; do
  printf '[%s] starting %s\n' "$(date --iso-8601=seconds)" "${NAME}" >>"${LOG_FILE}"
  "$@" >>"${LOG_FILE}" 2>&1 &
  child_pid=$!
  if wait "${child_pid}"; then
    status=0
  else
    status=$?
  fi
  child_pid=""
  printf '[%s] %s exited with status %s; restarting in 5 seconds\n' "$(date --iso-8601=seconds)" "${NAME}" "${status}" >>"${LOG_FILE}"
  sleep 5
done
