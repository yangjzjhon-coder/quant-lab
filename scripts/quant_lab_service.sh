#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_DIR="${PROJECT_DIR}/data"
PYTHON_BIN="${PROJECT_DIR}/.venv/bin/python"
CONFIG_PATH="config/settings.yaml"
RUNNER_SCRIPT="${SCRIPT_DIR}/guarded_runner.sh"

SERVICE_LOG_FILE="${DATA_DIR}/service-api.log"
SERVICE_PID_FILE="${DATA_DIR}/service-api.pid"
SERVICE_HEALTH_URL="http://127.0.0.1:18080/health"
SERVICE_PATTERN="python -m quant_lab service-api --config ${CONFIG_PATH}"

DEMO_LOG_FILE="${DATA_DIR}/demo-loop.log"
DEMO_PID_FILE="${DATA_DIR}/demo-loop.pid"
DEMO_PATTERN="python -m quant_lab demo-.*loop --config ${CONFIG_PATH}"

mkdir -p "${DATA_DIR}"

stop_systemd_unit() {
  local uid
  uid="$(id -u)"
  export XDG_RUNTIME_DIR="/run/user/${uid}"
  export DBUS_SESSION_BUS_ADDRESS="unix:path=${XDG_RUNTIME_DIR}/bus"
  systemctl --user stop quant-lab.service >/dev/null 2>&1 || true
}

health_check() {
  curl -fsS "${SERVICE_HEALTH_URL}" >/dev/null 2>&1
}

pid_from_file() {
  local pid_file="$1"
  if [[ -f "${pid_file}" ]]; then
    tr -d '[:space:]' <"${pid_file}"
  fi
}

pid_is_running() {
  local pid="${1:-}"
  [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

running_supervisor() {
  local pid_file="$1"
  local pid
  pid="$(pid_from_file "${pid_file}")"
  pid_is_running "${pid}"
}

running_pids() {
  local pattern="$1"
  pgrep -f "${pattern}" || true
}

pid_cmdline() {
  local pid="${1:-}"
  ps -o args= -p "${pid}" 2>/dev/null || true
}

pid_cwd() {
  local pid="${1:-}"
  readlink -f "/proc/${pid}/cwd" 2>/dev/null || true
}

kill_ancestry_if_foreign() {
  local pid="${1:-}"
  while [[ -n "${pid}" && "${pid}" != "1" ]]; do
    local cmd
    cmd="$(pid_cmdline "${pid}")"
    if [[ "${cmd}" == *"quant_lab"* || "${cmd}" == *"guarded_runner.sh"* ]]; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
    pid="$(ps -o ppid= -p "${pid}" 2>/dev/null | tr -d '[:space:]')"
  done
}

stop_foreign_runtime_processes() {
  local pattern
  for pattern in "${SERVICE_PATTERN}" "${DEMO_PATTERN}"; do
    local pid
    for pid in $(running_pids "${pattern}"); do
      local cwd
      cwd="$(pid_cwd "${pid}")"
      if [[ -n "${cwd}" && "${cwd}" != "${PROJECT_DIR}" ]]; then
        echo "stopping foreign quant-lab runtime pid=${pid} cwd=${cwd}"
        kill_ancestry_if_foreign "${pid}"
      fi
    done
  done
}

ensure_database() {
  (
    cd "${PROJECT_DIR}"
    PYTHONPATH=src PYTHONUNBUFFERED=1 "${PYTHON_BIN}" -m quant_lab service-init-db --config "${CONFIG_PATH}" >/dev/null
  )
}

demo_submit_enabled() {
  (
    cd "${PROJECT_DIR}"
    PYTHONPATH=src "${PYTHON_BIN}" - <<'PY'
from pathlib import Path

from quant_lab.config import load_config

cfg = load_config(Path("config/settings.yaml"))
enabled = (
    cfg.okx.use_demo
    and cfg.trading.allow_order_placement
    and bool(cfg.okx.api_key)
    and bool(cfg.okx.secret_key)
    and bool(cfg.okx.passphrase)
)
raise SystemExit(0 if enabled else 1)
PY
  )
}

demo_loop_command_name() {
  (
    cd "${PROJECT_DIR}"
    PYTHONPATH=src "${PYTHON_BIN}" - <<'PY'
from pathlib import Path

from quant_lab.config import configured_symbols, load_config

cfg = load_config(Path("config/settings.yaml"))
print("demo-portfolio-loop" if len(configured_symbols(cfg)) > 1 else "demo-loop")
PY
  )
}

runtime_env_prefix() {
  local prefix=""
  if [[ -n "${QUANT_LAB_ALLOW_ORDER_PLACEMENT:-}" ]]; then
    prefix+="export QUANT_LAB_ALLOW_ORDER_PLACEMENT='${QUANT_LAB_ALLOW_ORDER_PLACEMENT}'; "
  fi
  printf '%s' "${prefix}"
}

start_guarded_process() {
  local name="$1"
  local pid_file="$2"
  local log_file="$3"
  shift 3

  if running_supervisor "${pid_file}"; then
    echo "${name} supervisor is already running."
    return 0
  fi

  : >"${log_file}"
  chmod +x "${RUNNER_SCRIPT}"
  nohup "${RUNNER_SCRIPT}" "${name}" "${log_file}" "$@" >/dev/null 2>&1 < /dev/null &
  echo $! >"${pid_file}"
}

stop_guarded_process() {
  local name="$1"
  local pid_file="$2"
  local pattern="$3"
  local stopped="false"

  if running_supervisor "${pid_file}"; then
    local pid
    pid="$(pid_from_file "${pid_file}")"
    kill "${pid}" >/dev/null 2>&1 || true
    stopped="true"
    for _ in $(seq 1 20); do
      if ! pid_is_running "${pid}"; then
        break
      fi
      sleep 1
    done
  fi
  rm -f "${pid_file}"

  if [[ -n "$(running_pids "${pattern}")" ]]; then
    pkill -f "${pattern}" >/dev/null 2>&1 || true
    stopped="true"
  fi

  if [[ "${stopped}" == "true" ]]; then
    echo "${name} stopped."
  else
    echo "${name} was not running."
  fi
}

service_status() {
  if health_check; then
    echo "service-api: healthy"
  else
    echo "service-api: unhealthy"
  fi
  echo "service-api supervisor: $(running_supervisor "${SERVICE_PID_FILE}" && echo running || echo stopped)"
  echo "service-api pids: $(running_pids "${SERVICE_PATTERN}" | tr '\n' ' ')"
}

demo_status() {
  local mode="plan_only"
  local command_name
  local loop_kind="single"
  command_name="$(demo_loop_command_name)"
  if demo_submit_enabled; then
    mode="submit"
  fi
  if [[ "${command_name}" == "demo-portfolio-loop" ]]; then
    loop_kind="portfolio"
  fi
  echo "demo-loop mode: ${loop_kind}/${mode}"
  echo "demo-loop supervisor: $(running_supervisor "${DEMO_PID_FILE}" && echo running || echo stopped)"
  echo "demo-loop pids: $(running_pids "${DEMO_PATTERN}" | tr '\n' ' ')"
}

start_service() {
  ensure_database
  stop_systemd_unit
  stop_foreign_runtime_processes
  local env_prefix
  env_prefix="$(runtime_env_prefix)"

  start_guarded_process \
    "service-api" \
    "${SERVICE_PID_FILE}" \
    "${SERVICE_LOG_FILE}" \
    bash -lc "${env_prefix}cd '${PROJECT_DIR}' && PYTHONPATH=src PYTHONUNBUFFERED=1 '${PYTHON_BIN}' -m quant_lab service-api --config '${CONFIG_PATH}'"

  local demo_command
  demo_command="$(demo_loop_command_name)"
  local demo_args=(bash -lc "${env_prefix}cd '${PROJECT_DIR}' && PYTHONPATH=src PYTHONUNBUFFERED=1 '${PYTHON_BIN}' -m quant_lab ${demo_command} --config '${CONFIG_PATH}'")
  if demo_submit_enabled; then
    demo_args=(bash -lc "${env_prefix}cd '${PROJECT_DIR}' && PYTHONPATH=src PYTHONUNBUFFERED=1 '${PYTHON_BIN}' -m quant_lab ${demo_command} --config '${CONFIG_PATH}' --submit --confirm OKX_DEMO")
  fi

  start_guarded_process \
    "demo-loop" \
    "${DEMO_PID_FILE}" \
    "${DEMO_LOG_FILE}" \
    "${demo_args[@]}"

  for _ in $(seq 1 60); do
    if health_check && running_supervisor "${DEMO_PID_FILE}"; then
      echo "quant-lab runtime started."
      echo "health: ${SERVICE_HEALTH_URL}"
      demo_status
      return 0
    fi
    sleep 1
  done

  echo "quant-lab runtime failed to become ready."
  echo "--- service log ---"
  tail -n 80 "${SERVICE_LOG_FILE}" || true
  echo "--- demo-loop log ---"
  tail -n 80 "${DEMO_LOG_FILE}" || true
  return 1
}

stop_service() {
  stop_systemd_unit
  stop_guarded_process "demo-loop" "${DEMO_PID_FILE}" "${DEMO_PATTERN}"
  stop_guarded_process "service-api" "${SERVICE_PID_FILE}" "${SERVICE_PATTERN}"

  for _ in $(seq 1 10); do
    if ! health_check; then
      break
    fi
    sleep 1
  done

  if health_check; then
    echo "service-api is still listening on ${SERVICE_HEALTH_URL}."
    return 1
  fi
}

status_service() {
  service_status
  demo_status
  if health_check && running_supervisor "${DEMO_PID_FILE}"; then
    return 0
  fi
  return 1
}

logs_service() {
  echo "=== service-api log ==="
  if [[ -f "${SERVICE_LOG_FILE}" ]]; then
    tail -n 80 "${SERVICE_LOG_FILE}"
  else
    echo "log file not found: ${SERVICE_LOG_FILE}"
  fi
  echo
  echo "=== demo-loop log ==="
  if [[ -f "${DEMO_LOG_FILE}" ]]; then
    tail -n 80 "${DEMO_LOG_FILE}"
  else
    echo "log file not found: ${DEMO_LOG_FILE}"
  fi
}

restart_service() {
  stop_service || true
  start_service
}

usage() {
  cat <<'EOF'
Usage: quant_lab_service.sh {start|stop|restart|status|logs}
EOF
}

case "${1:-}" in
  start)
    start_service
    ;;
  stop)
    stop_service
    ;;
  restart)
    restart_service
    ;;
  status)
    status_service
    ;;
  logs)
    logs_service
    ;;
  *)
    usage
    exit 1
    ;;
esac
