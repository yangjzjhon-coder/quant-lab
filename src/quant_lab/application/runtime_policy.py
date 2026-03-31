from __future__ import annotations

from typing import Any

from quant_lab.config import AppConfig, configured_symbols

SHARED_RUNTIME_DECISION_SOURCE = "quant_lab.application.runtime_policy"
HARD_RUNTIME_CONSTRAINTS = (
    "ready/blocked/halt/duplicate/reconcile must be computed from one shared decision source",
    "new live trading capabilities must land in shared runtime/helper modules before any CLI/service/worker adapter",
    "single/portfolio and demo/live are parameterized modes, not parallel implementations",
)
EXECUTION_LOOP_STATUS_PRIORITY = (
    "submitted",
    "warning",
    "duplicate",
    "plan_only",
    "idle",
)


def execution_mode(*, config: AppConfig) -> str:
    return "demo" if bool(config.okx.use_demo) else "live"


def required_execution_scope(*, config: AppConfig) -> str:
    return "demo" if execution_mode(config=config) == "demo" else "live"


def symbol_mode(*, config: AppConfig) -> str:
    return "portfolio" if len(configured_symbols(config)) > 1 else "single"


def configured_candidate_binding(*, config: AppConfig) -> dict[str, Any]:
    if config.trading.strategy_router_enabled:
        candidate_ids = sorted({int(value) for value in (config.trading.execution_candidate_map or {}).values()})
        return {
            "binding_mode": "router",
            "candidate_count": len(candidate_ids),
            "candidate_ids": candidate_ids,
            "candidate_id": None,
            "candidate_name": None,
            "route_count": len(config.trading.execution_candidate_map or {}),
            "route_keys": sorted(str(key) for key in (config.trading.execution_candidate_map or {}).keys()),
        }

    candidate_id = int(config.trading.execution_candidate_id) if config.trading.execution_candidate_id else None
    return {
        "binding_mode": "single_candidate",
        "candidate_count": 1 if candidate_id is not None else 0,
        "candidate_ids": [candidate_id] if candidate_id is not None else [],
        "candidate_id": candidate_id,
        "candidate_name": str(config.trading.execution_candidate_name or "").strip() or None,
        "route_count": 0,
        "route_keys": [],
    }


def aggregate_execution_loop_status(statuses: list[str] | tuple[str, ...], *, default: str = "ok") -> str:
    normalized = [str(status or "").strip() for status in statuses if str(status or "").strip()]
    if not normalized:
        return default
    for status in EXECUTION_LOOP_STATUS_PRIORITY:
        if any(item == status for item in normalized):
            return status
    return normalized[0]


def build_rollout_policy_payload(*, config: AppConfig) -> dict[str, Any]:
    resolved_symbols = configured_symbols(config)
    resolved_execution_mode = execution_mode(config=config)
    resolved_symbol_mode = symbol_mode(config=config)
    binding = configured_candidate_binding(config=config)
    rollout = config.rollout

    checks: dict[str, bool] = {
        "shared_decision_source": True,
        "shared_runtime_first": True,
        "parameterized_modes_only": True,
    }
    reasons: list[str] = []

    if rollout.phase == "live_single":
        account_profile_ready = bool(config.okx.profile) and config.okx.profile == rollout.account_profile
        checks.update(
            {
                "phase_live_single": True,
                "execution_mode_live": resolved_execution_mode == "live",
                "single_symbol_mode": resolved_symbol_mode == "single",
                "allowed_symbol_bound": len(resolved_symbols) == 1 and resolved_symbols[0] == rollout.allowed_symbol,
                "approved_candidate_required": bool(config.trading.require_approved_candidate),
                "router_disabled": not bool(config.trading.strategy_router_enabled),
                "single_candidate_binding": binding["binding_mode"] == "single_candidate"
                and binding["candidate_count"] == 1,
                "required_candidate_bound": binding["candidate_id"] == rollout.required_candidate_id,
                "required_candidate_name_bound": (
                    True
                    if rollout.required_candidate_name is None
                    else binding["candidate_name"] == rollout.required_candidate_name
                ),
                "signal_bar_locked": config.strategy.signal_bar == rollout.required_signal_bar,
                "execution_bar_locked": config.strategy.execution_bar == rollout.required_execution_bar,
                "account_profile_locked": account_profile_ready,
            }
        )
        if not checks["execution_mode_live"]:
            reasons.append("rollout.phase=live_single requires okx.use_demo=false")
        if not checks["single_symbol_mode"]:
            reasons.append("stage0 live rollout only allows single-symbol mode")
        if not checks["allowed_symbol_bound"]:
            reasons.append(
                f"stage0 live rollout is pinned to {rollout.allowed_symbol}, current={','.join(resolved_symbols) or '--'}"
            )
        if not checks["approved_candidate_required"]:
            reasons.append("stage0 live rollout requires trading.require_approved_candidate=true")
        if not checks["router_disabled"]:
            reasons.append("stage0 live rollout forbids trading.strategy_router_enabled=true")
        if not checks["single_candidate_binding"]:
            reasons.append("stage0 live rollout requires exactly one bound execution candidate")
        if not checks["required_candidate_bound"]:
            reasons.append(
                f"stage0 live rollout requires trading.execution_candidate_id={rollout.required_candidate_id}"
            )
        if not checks["required_candidate_name_bound"]:
            reasons.append(
                f"stage0 live rollout requires trading.execution_candidate_name={rollout.required_candidate_name}"
            )
        if not checks["signal_bar_locked"]:
            reasons.append(f"stage0 live rollout requires strategy.signal_bar={rollout.required_signal_bar}")
        if not checks["execution_bar_locked"]:
            reasons.append(
                f"stage0 live rollout requires strategy.execution_bar={rollout.required_execution_bar}"
            )
        if not checks["account_profile_locked"]:
            reasons.append(f"stage0 live rollout requires okx.profile={rollout.account_profile}")
        active = True
        ready = all(checks.values())
        status = "ready" if ready else "blocked"
    else:
        active = False
        ready = False
        status = "inactive"

    return {
        "decision_source": SHARED_RUNTIME_DECISION_SOURCE,
        "status": status,
        "active": active,
        "ready": ready,
        "phase": rollout.phase,
        "execution_mode": resolved_execution_mode,
        "symbol_mode": resolved_symbol_mode,
        "hard_constraints": list(HARD_RUNTIME_CONSTRAINTS),
        "checks": checks,
        "reasons": reasons,
        "binding": {
            "account_profile": config.okx.profile,
            "required_account_profile": rollout.account_profile,
            "symbols": resolved_symbols,
            "allowed_symbol": rollout.allowed_symbol,
            "candidate_binding": binding,
            "required_candidate_id": rollout.required_candidate_id,
            "required_candidate_name": rollout.required_candidate_name,
            "signal_bar": config.strategy.signal_bar,
            "required_signal_bar": rollout.required_signal_bar,
            "execution_bar": config.strategy.execution_bar,
            "required_execution_bar": rollout.required_execution_bar,
        },
    }


def build_submit_gate_payload(
    *,
    config: AppConfig,
    execution_approval: dict[str, Any],
    executor_state_info: dict[str, Any],
    rollout_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_rollout_policy = rollout_policy or build_rollout_policy_payload(config=config)
    current_execution_mode = execution_mode(config=config)
    executor_state_ready = executor_state_info.get("status") != "invalid_json"
    rollout_policy_gate = not bool(resolved_rollout_policy.get("active")) or bool(resolved_rollout_policy.get("ready"))

    checks = {
        "use_demo": bool(config.okx.use_demo),
        "allow_order_placement": bool(config.trading.allow_order_placement),
        "api_key": bool(config.okx.api_key),
        "secret_key": bool(config.okx.secret_key),
        "passphrase": bool(config.okx.passphrase),
        "approved_candidate_gate": bool(execution_approval.get("ready")),
        "executor_state_ready": executor_state_ready,
        "rollout_policy_gate": rollout_policy_gate,
    }
    reasons: list[str] = []
    if not checks["use_demo"]:
        reasons.append("okx.use_demo=false")
    if not checks["allow_order_placement"]:
        reasons.append("trading.allow_order_placement=false")
    if not checks["api_key"]:
        reasons.append("missing OKX_API_KEY")
    if not checks["secret_key"]:
        reasons.append("missing OKX_SECRET_KEY")
    if not checks["passphrase"]:
        reasons.append("missing OKX_PASSPHRASE")
    if not checks["approved_candidate_gate"]:
        for reason in execution_approval.get("reasons") or []:
            reasons.append(f"execution approval: {reason}")
    if not checks["executor_state_ready"]:
        reasons.append(f"executor state: invalid JSON at {executor_state_info.get('path')}")
    if not checks["rollout_policy_gate"]:
        rollout_reasons = [str(item) for item in (resolved_rollout_policy.get("reasons") or []) if str(item).strip()]
        if rollout_reasons:
            reasons.extend(f"rollout policy: {reason}" for reason in rollout_reasons)
        else:
            reasons.append("rollout policy: rollout gate is not ready")

    ready = all(checks.values())
    mode = "submit_ready" if ready else "plan_only"
    if not ready and (
        checks["allow_order_placement"]
        or bool(resolved_rollout_policy.get("active"))
        or current_execution_mode == "live"
    ):
        mode = "submit_blocked"

    return {
        "decision_source": SHARED_RUNTIME_DECISION_SOURCE,
        "execution_mode": current_execution_mode,
        "required_scope": required_execution_scope(config=config),
        "mode": mode,
        "ready": ready,
        "checks": checks,
        "reasons": reasons,
        "rollout_policy_status": str(resolved_rollout_policy.get("status") or "inactive"),
    }
