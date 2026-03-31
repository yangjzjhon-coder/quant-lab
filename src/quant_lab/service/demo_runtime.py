from __future__ import annotations

import json
import inspect
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import httpx
import pandas as pd
from sqlalchemy import desc, func, select

from quant_lab.application.demo_support import (
    align_demo_leverage,
    align_demo_leverage_with_stop_rearm,
    build_demo_portfolio_payload,
    build_demo_reconcile_payload,
    extract_rearmable_stop_orders,
    fetch_live_market_data_for_symbol,
    load_demo_portfolio_state,
    load_demo_state,
    require_private_credentials,
    run_demo_align_leverage_action as application_run_demo_align_leverage_action,
    validate_demo_account_mutation,
)
from quant_lab.application.runtime_policy import (
    build_rollout_policy_payload,
    build_submit_gate_payload as build_shared_submit_gate_payload,
    required_execution_scope,
)
from quant_lab.config import AppConfig, configured_symbols
from quant_lab.execution.strategy_router import resolve_strategy_route, serialize_strategy_route_decision
from quant_lab.logging_utils import get_logger
from quant_lab.service.database import AlertEvent, ServiceHeartbeat, session_scope
from quant_lab.service.research_ops import resolve_execution_approval
from quant_lab.service.serialization import serialize_utc_datetime
from quant_lab.utils.files import atomic_write_json

LEGACY_DEMO_HEARTBEAT_SERVICE = "quant-lab-demo-loop"
DEMO_HEARTBEAT_SERVICES = {
    "single": "quant-lab-demo-loop-single",
    "portfolio": "quant-lab-demo-loop-portfolio",
}
PUBLIC_SINGLE_HEARTBEAT_COMPAT_FIELDS = (
    "mode",
    "cycle",
    "status",
    "submitted",
    "response_count",
    "warning_count",
    "action",
    "current_contracts",
    "target_contracts",
    "total_equity",
    "available_equity",
)
PUBLIC_PORTFOLIO_HEARTBEAT_COMPAT_FIELDS = (
    "mode",
    "cycle",
    "status",
    "symbol_count",
    "submitted_symbol_count",
    "actionable_symbol_count",
    "active_position_symbol_count",
    "response_count",
    "warning_count",
    "action",
    "total_equity",
    "available_equity",
)
LEGACY_EXECUTOR_STATE_FILE = "demo_executor_state.json"
DEMO_EXECUTOR_STATE_FILES = {
    "single": "demo_executor_state.single.json",
    "portfolio": "demo_executor_state.portfolio.json",
}
LOGGER = get_logger(__name__)
PROXY_EGRESS_IP_CACHE: dict[str, tuple[float, str | None]] = {}
PROXY_EGRESS_IP_TTL_SECONDS = 600.0


def demo_mode(*, config: AppConfig | None = None, symbols: list[str] | None = None, force_mode: str | None = None) -> str:
    if force_mode in {"single", "portfolio"}:
        return force_mode
    resolved_symbols = list(symbols or (configured_symbols(config) if config is not None else []))
    return "portfolio" if len(resolved_symbols) > 1 else "single"


def heartbeat_service_name(*, mode: str) -> str:
    return DEMO_HEARTBEAT_SERVICES[mode]


def executor_state_path(*, config: AppConfig, project_root: Path | None = None, mode: str) -> Path:
    storage = config.storage if project_root is None else config.storage.resolved(project_root.resolve())
    return storage.data_dir / DEMO_EXECUTOR_STATE_FILES[mode]


def load_executor_state_info(
    *,
    config: AppConfig,
    project_root: Path | None,
    mode: str,
) -> dict[str, Any]:
    storage = config.storage if project_root is None else config.storage.resolved(project_root.resolve())
    primary_path = storage.data_dir / DEMO_EXECUTOR_STATE_FILES[mode]
    legacy_path = storage.data_dir / LEGACY_EXECUTOR_STATE_FILE
    candidates = [(primary_path, False)]
    if legacy_path != primary_path:
        candidates.append((legacy_path, True))

    for candidate, legacy_fallback_used in candidates:
        state_info = _load_executor_state_path_info(
            candidate,
            mode=mode,
            legacy_fallback_used=legacy_fallback_used,
        )
        if state_info.get("status") == "missing":
            continue
        return state_info

    return {
        "status": "missing",
        "path": str(primary_path),
        "legacy_fallback_used": False,
        "mode": mode,
        "payload": None,
    }


def executor_state_payload(state_info: dict[str, Any]) -> dict[str, object]:
    if state_info.get("status") != "ok":
        return {}
    payload = state_info.get("payload")
    return dict(payload) if isinstance(payload, dict) else {}


def load_executor_state_file_payload(path: Path) -> dict[str, object]:
    return executor_state_payload(_load_executor_state_path_info(path))


def save_executor_state(*, path: Path, payload: dict[str, object]) -> None:
    atomic_write_json(path, payload)
    LOGGER.debug("executor state saved path=%s keys=%s", path, sorted(payload.keys()))


def reset_executor_state(*, path: Path) -> None:
    atomic_write_json(path, {})
    LOGGER.info("executor state reset path=%s", path)


def run_align_leverage_action(
    *,
    config: AppConfig,
    session_factory,
    project_root: Path,
    apply: bool,
    confirm: str,
    rearm_protective_stop: bool,
    refresh_snapshot: Callable[[], dict[str, Any]] | None = None,
    load_demo_state_fn=load_demo_state,
    load_demo_portfolio_state_fn=load_demo_portfolio_state,
    build_demo_reconcile_payload_fn=build_demo_reconcile_payload,
    build_demo_portfolio_payload_fn=build_demo_portfolio_payload,
    require_private_credentials_fn=require_private_credentials,
    validate_mutation_fn=validate_demo_account_mutation,
    align_demo_leverage_fn=align_demo_leverage,
    align_demo_leverage_with_stop_rearm_fn=align_demo_leverage_with_stop_rearm,
    extract_rearmable_stop_orders_fn=extract_rearmable_stop_orders,
    load_executor_state_fn=load_executor_state_file_payload,
) -> tuple[dict[str, object], bool]:
    resolved_root = project_root.resolve()
    return application_run_demo_align_leverage_action(
        config,
        apply=apply,
        confirm=confirm,
        rearm_protective_stop=rearm_protective_stop,
        refresh_snapshot=refresh_snapshot,
        executor_state_path_fn=lambda current_cfg, *, mode: executor_state_path(
            config=current_cfg,
            project_root=resolved_root,
            mode=mode,
        ),
        load_executor_state_fn=load_executor_state_fn,
        load_demo_state_fn=lambda current_cfg: _invoke_demo_loader(
            load_demo_state_fn,
            current_cfg,
            session_factory=session_factory,
            project_root=resolved_root,
        ),
        load_demo_portfolio_state_fn=lambda current_cfg, symbols: _invoke_demo_loader(
            load_demo_portfolio_state_fn,
            current_cfg,
            symbols,
            session_factory=session_factory,
            project_root=resolved_root,
        ),
        build_demo_reconcile_payload_fn=build_demo_reconcile_payload_fn,
        build_demo_portfolio_payload_fn=build_demo_portfolio_payload_fn,
        require_private_credentials_fn=require_private_credentials_fn,
        validate_mutation_fn=validate_mutation_fn,
        align_demo_leverage_fn=align_demo_leverage_fn,
        align_demo_leverage_with_stop_rearm_fn=align_demo_leverage_with_stop_rearm_fn,
        extract_rearmable_stop_orders_fn=extract_rearmable_stop_orders_fn,
    )


def latest_demo_heartbeat(*, session_factory, mode: str) -> tuple[ServiceHeartbeat | None, str, bool]:
    service_names = [heartbeat_service_name(mode=mode), LEGACY_DEMO_HEARTBEAT_SERVICE]
    for index, service_name in enumerate(service_names):
        with session_scope(session_factory) as session:
            heartbeat = session.execute(
                select(ServiceHeartbeat)
                .where(ServiceHeartbeat.service_name == service_name)
                .order_by(desc(ServiceHeartbeat.created_at))
                .limit(1)
            ).scalar_one_or_none()
        if heartbeat is not None:
            return heartbeat, service_name, index > 0
    return None, heartbeat_service_name(mode=mode), False


def recent_demo_heartbeats(*, session_factory, mode: str, limit: int) -> tuple[list[ServiceHeartbeat], str, bool]:
    service_names = [heartbeat_service_name(mode=mode), LEGACY_DEMO_HEARTBEAT_SERVICE]
    for index, service_name in enumerate(service_names):
        with session_scope(session_factory) as session:
            heartbeats = list(
                session.execute(
                    select(ServiceHeartbeat)
                    .where(ServiceHeartbeat.service_name == service_name)
                    .order_by(desc(ServiceHeartbeat.created_at))
                    .limit(limit)
                ).scalars()
            )
        if heartbeats:
            return heartbeats, service_name, index > 0
    return [], heartbeat_service_name(mode=mode), False


def build_runtime_route_decisions(
    *,
    config: AppConfig,
    session_factory,
    project_root: Path,
    symbols: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    resolved_symbols = list(symbols or configured_symbols(config))
    if not config.trading.strategy_router_enabled:
        return {}
    required_scope = required_execution_scope(config=config)

    decisions: dict[str, dict[str, Any]] = {}

    for symbol in resolved_symbols:
        try:
            signal_bars, _ = fetch_live_market_data_for_symbol(config, symbol)
            decision = resolve_strategy_route(
                session_factory=session_factory,
                config=config,
                project_root=project_root,
                symbol=symbol,
                signal_bars=signal_bars,
                required_scope=required_scope,
            )
            decisions[symbol] = serialize_strategy_route_decision(
                decision,
                default_strategy=config.strategy,
                symbol=symbol,
                required_scope=required_scope,
            )
        except Exception as exc:
            LOGGER.warning(
                "runtime route resolution failed symbol=%s error=%s:%s",
                symbol,
                type(exc).__name__,
                exc,
            )
            decisions[symbol] = serialize_strategy_route_decision(
                {
                    "enabled": True,
                    "ready": False,
                    "symbol": symbol,
                    "regime": None,
                    "route_key": None,
                    "required_scope": required_scope,
                    "fallback_used": bool(config.trading.strategy_router_fallback_to_config),
                    "selected_strategy_source": "unresolved",
                    "selected_strategy_name": config.strategy.name,
                    "selected_variant": config.strategy.variant,
                    "selected_signal_bar": config.strategy.signal_bar,
                    "selected_execution_bar": config.strategy.execution_bar,
                    "candidate": None,
                    "reasons": [f"live route resolution failed: {type(exc).__name__}: {exc}"],
                    "regime_metrics": {},
                },
                default_strategy=config.strategy,
                symbol=symbol,
                required_scope=required_scope,
            )
    return decisions


def build_execution_approval_payload(
    *,
    config: AppConfig,
    session_factory,
    project_root: Path,
    route_decisions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    resolved_required_scope = required_execution_scope(config=config)
    payload = resolve_execution_approval(
        session_factory=session_factory,
        config=config,
        required_scope=resolved_required_scope,
    )
    payload = dict(payload)
    payload["route_decisions"] = {}

    if not config.trading.strategy_router_enabled:
        return payload

    resolved_symbols = configured_symbols(config)
    decisions = route_decisions if route_decisions is not None else build_runtime_route_decisions(
        config=config,
        session_factory=session_factory,
        project_root=project_root,
        symbols=resolved_symbols,
    )
    reasons = [str(item) for item in (payload.get("reasons") or [])]
    route_reasons: list[str] = []
    route_decision_payload: dict[str, dict[str, Any]] = {}
    for symbol in resolved_symbols:
        decision = decisions.get(symbol)
        if not isinstance(decision, dict):
            route_reasons.append(f"{symbol}: 路由决策缺失")
            continue
        normalized_decision = serialize_strategy_route_decision(
            decision,
            default_strategy=config.strategy,
            symbol=symbol,
            required_scope=resolved_required_scope,
        )
        route_decision_payload[symbol] = normalized_decision
        if normalized_decision.get("ready"):
            continue
        decision_reasons = normalized_decision.get("reasons") or ["路由未就绪"]
        route_reasons.extend(f"{symbol}: {item}" for item in decision_reasons)

    payload["route_decisions"] = route_decision_payload
    payload["reasons"] = reasons + route_reasons
    payload["ready"] = not payload["reasons"]
    return payload


def build_submit_gate_payload(
    *,
    config: AppConfig,
    session_factory,
    project_root: Path,
    mode: str | None = None,
    route_decisions: dict[str, dict[str, Any]] | None = None,
    execution_approval: dict[str, Any] | None = None,
    executor_state_info: dict[str, Any] | None = None,
    rollout_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_mode = demo_mode(config=config, force_mode=mode)
    resolved_executor_state_info = executor_state_info or load_executor_state_info(
        config=config,
        project_root=project_root,
        mode=resolved_mode,
    )
    resolved_execution_approval = execution_approval or build_execution_approval_payload(
        config=config,
        session_factory=session_factory,
        project_root=project_root,
        route_decisions=route_decisions,
    )
    resolved_rollout_policy = rollout_policy or build_rollout_policy_payload(config=config)
    return build_shared_submit_gate_payload(
        config=config,
        execution_approval=resolved_execution_approval,
        executor_state_info=resolved_executor_state_info,
        rollout_policy=resolved_rollout_policy,
    )


def build_demo_trading_payload(
    *,
    config: AppConfig,
    execution_approval: dict[str, Any],
    executor_state_info: dict[str, Any],
    rollout_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_shared_submit_gate_payload(
        config=config,
        execution_approval=execution_approval,
        executor_state_info=executor_state_info,
        rollout_policy=rollout_policy,
    )


def build_single_demo_heartbeat_details(
    *,
    cycle: int,
    symbol: str,
    status: str,
    account: Any = None,
    position: Any = None,
    signal: Any = None,
    plan: Any = None,
    submitted: bool = False,
    responses: list[dict[str, Any]] | None = None,
    warnings: list[str] | None = None,
    already_submitted: bool = False,
    execution_approval: dict[str, Any] | None = None,
    route_decision: dict[str, Any] | None = None,
    executor_state_path: str | None = None,
    executor_state_status: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    response_payload = list(responses or [])
    warning_payload = [str(item) for item in (warnings or []) if str(item).strip()]
    signal_payload = _normalize_signal_payload(_json_mapping(signal))
    plan_payload = _plan_mapping(plan)
    position_payload = _position_mapping(position, fallback=plan_payload)
    account_payload = _account_mapping(account)
    route_payload = _route_decision_payload(route_decision, symbol=symbol)
    executor_state_payload = _executor_state_metadata(
        executor_state=None,
        path=executor_state_path,
        status=executor_state_status,
    )

    return compact_demo_heartbeat_details(
        {
            "mode": "single",
            "cycle": cycle,
            "status": status,
            "symbol": symbol,
            "submitted": bool(submitted),
            "responses": response_payload,
            "warnings": warning_payload,
            "already_submitted": bool(already_submitted),
            "execution_approval": dict(execution_approval or {}),
            "error": error,
            "summary": {
                "mode": "single",
                "cycle": cycle,
                "symbol": symbol,
                "status": status,
                "submitted": bool(submitted),
                "response_count": len(response_payload),
                "warning_count": len(warning_payload),
                "already_submitted": bool(already_submitted),
            },
            "account": account_payload,
            "position": position_payload,
            "signal": signal_payload,
            "plan": plan_payload,
            "route_decision": route_payload,
            "executor_state": executor_state_payload,
        },
        status=status,
    )


def build_portfolio_demo_heartbeat_details(
    *,
    cycle: int,
    status: str,
    symbols: list[str],
    account: Any = None,
    symbol_states: dict[str, Any] | None = None,
    symbol_payloads: dict[str, Any] | None = None,
    submitted_symbols: list[str] | None = None,
    execution_approval: dict[str, Any] | None = None,
    strategy_router_enabled: bool = False,
    executor_state_path: str | None = None,
    executor_state_status: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    submitted_symbol_list = [str(item) for item in (submitted_symbols or []) if str(item).strip()]
    raw_symbol_states = symbol_states if isinstance(symbol_states, dict) else {}
    raw_symbol_payloads = symbol_payloads if isinstance(symbol_payloads, dict) else {}
    normalized_symbol_states: dict[str, Any] = {}

    for symbol in symbols:
        raw_state = raw_symbol_states.get(symbol)
        raw_state = raw_state if isinstance(raw_state, dict) else {}
        raw_payload = raw_symbol_payloads.get(symbol)
        raw_payload = raw_payload if isinstance(raw_payload, dict) else {}
        merged_payload = dict(raw_payload)

        if "position" not in merged_payload:
            merged_payload["position"] = _position_mapping(raw_state.get("position"))
        if "signal" not in merged_payload:
            merged_payload["signal"] = _normalize_signal_payload(_json_mapping(raw_state.get("signal")))
        if "plan" not in merged_payload:
            merged_payload["plan"] = _plan_mapping(raw_state.get("plan"))
        if "planning_account" not in merged_payload:
            merged_payload["planning_account"] = _account_mapping(raw_state.get("planning_account") or raw_state.get("account"))
        if "route_decision" not in merged_payload and "router_decision" not in merged_payload:
            merged_payload["route_decision"] = raw_state.get("router_decision")
        if "public_factor_score" not in merged_payload and raw_state.get("public_factor_snapshot") is not None:
            public_factor = raw_state["public_factor_snapshot"]
            merged_payload["public_factor_score"] = getattr(public_factor, "score", None)
            merged_payload["public_factor_confidence"] = getattr(public_factor, "confidence", None)
        if "portfolio_risk" not in merged_payload:
            portfolio_risk = raw_state.get("portfolio_risk")
            if portfolio_risk is not None and hasattr(portfolio_risk, "to_dict"):
                merged_payload["portfolio_risk"] = portfolio_risk.to_dict()
        normalized_symbol_states[symbol] = _normalize_demo_symbol_heartbeat_state(merged_payload, symbol=symbol)

    summary = {
        "mode": "portfolio",
        "cycle": cycle,
        "status": status,
        "symbol_count": len(symbols),
        "submitted_symbol_count": len(submitted_symbol_list),
        "actionable_symbol_count": sum(
            1
            for payload in normalized_symbol_states.values()
            if isinstance((payload.get("plan") or {}).get("instructions"), list)
            and bool((payload.get("plan") or {}).get("instructions"))
        ),
        "active_position_symbol_count": sum(
            1
            for payload in normalized_symbol_states.values()
            if _coerce_int((payload.get("position") or {}).get("side")) not in {None, 0}
            and (_coerce_float((payload.get("position") or {}).get("contracts")) or 0.0) > 0
        ),
        "response_count": sum(_coerce_int(payload.get("response_count")) or 0 for payload in normalized_symbol_states.values()),
        "warning_count": sum(_coerce_int(payload.get("warning_count")) or 0 for payload in normalized_symbol_states.values()),
        "strategy_router_enabled": bool(strategy_router_enabled),
    }

    return compact_demo_heartbeat_details(
        {
            "mode": "portfolio",
            "cycle": cycle,
            "status": status,
            "symbols": list(symbols),
            "submitted_symbols": submitted_symbol_list,
            "execution_approval": dict(execution_approval or {}),
            "strategy_router_enabled": bool(strategy_router_enabled),
            "error": error,
            "summary": summary,
            "account": _account_mapping(account),
            "symbol_states": normalized_symbol_states,
            "executor_state": _executor_state_metadata(
                executor_state=None,
                path=executor_state_path,
                status=executor_state_status,
            ),
        },
        status=status,
    )


def _normalize_demo_heartbeat_model(details: Any, *, status: str | None = None) -> dict[str, Any]:
    payload = dict(details) if isinstance(details, dict) else {}
    mode = str(payload.get("mode") or "").strip().lower()
    if mode not in {"single", "portfolio"}:
        mode = "portfolio" if isinstance(payload.get("symbol_states"), dict) else "single"
    if mode == "portfolio":
        return _normalize_portfolio_demo_heartbeat_details(payload, status=status)
    return _normalize_single_demo_heartbeat_details(payload, status=status)


def normalize_demo_heartbeat_contract(details: Any, *, status: str | None = None) -> dict[str, Any]:
    normalized = _normalize_demo_heartbeat_model(details, status=status)
    mode = str(normalized.get("mode") or "").strip().lower()
    if mode == "portfolio":
        return _contract_portfolio_demo_heartbeat_details(normalized)
    return _contract_single_demo_heartbeat_details(normalized)


def normalize_demo_heartbeat_details(details: Any, *, status: str | None = None) -> dict[str, Any]:
    normalized = _normalize_demo_heartbeat_model(details, status=status)
    mode = str(normalized.get("mode") or "").strip().lower()
    if mode == "portfolio":
        return _public_portfolio_demo_heartbeat_details(normalized)
    return _public_single_demo_heartbeat_details(normalized)


def compact_demo_heartbeat_details(details: Any, *, status: str | None = None) -> dict[str, Any]:
    normalized = normalize_demo_heartbeat_contract(details, status=status)
    mode = str(normalized.get("mode") or "").strip().lower()
    if mode == "portfolio":
        return _compact_portfolio_demo_heartbeat_details(normalized)
    return _compact_single_demo_heartbeat_details(normalized)


def _contract_single_demo_heartbeat_details(payload: dict[str, Any]) -> dict[str, Any]:
    return _compact_mapping(
        {
            "mode": "single",
            "summary": _json_mapping(payload.get("summary")),
            "account": _json_mapping(payload.get("account")),
            "position": _json_mapping(payload.get("position")),
            "planning_account": _json_mapping(payload.get("planning_account")),
            "signal": _json_mapping(payload.get("signal")),
            "plan": _json_mapping(payload.get("plan")),
            "route_decision": _json_mapping(payload.get("route_decision") or payload.get("router_decision")),
            "executor_state": _json_mapping(payload.get("executor_state")),
            "execution_approval": _json_mapping(payload.get("execution_approval")),
            "responses": _compact_sequence(payload.get("responses")),
            "warnings": _compact_sequence(payload.get("warnings")),
            "error": payload.get("error"),
        }
    )


def _contract_portfolio_demo_heartbeat_details(payload: dict[str, Any]) -> dict[str, Any]:
    compact_symbol_states: dict[str, Any] = {}
    symbol_states = payload.get("symbol_states") if isinstance(payload.get("symbol_states"), dict) else {}
    for symbol, symbol_payload in symbol_states.items():
        compact_symbol_states[str(symbol)] = _contract_symbol_demo_heartbeat_state(symbol_payload)
    return _compact_mapping(
        {
            "mode": "portfolio",
            "summary": _json_mapping(payload.get("summary")),
            "account": _json_mapping(payload.get("account")),
            "symbols": _compact_sequence(payload.get("symbols")),
            "submitted_symbols": _compact_sequence(payload.get("submitted_symbols")),
            "symbol_states": compact_symbol_states,
            "executor_state": _json_mapping(payload.get("executor_state")),
            "execution_approval": _json_mapping(payload.get("execution_approval")),
            "strategy_router_enabled": payload.get("strategy_router_enabled"),
            "error": payload.get("error"),
        }
    )


def _contract_symbol_demo_heartbeat_state(payload: Any) -> dict[str, Any]:
    raw = _json_mapping(payload)
    return _compact_mapping(
        {
            "summary": _json_mapping(raw.get("summary")),
            "position": _json_mapping(raw.get("position")),
            "signal": _json_mapping(raw.get("signal")),
            "plan": _json_mapping(raw.get("plan")),
            "planning_account": _json_mapping(raw.get("planning_account")),
            "route_decision": _json_mapping(raw.get("route_decision") or raw.get("router_decision")),
            "portfolio_risk": _json_mapping(raw.get("portfolio_risk")),
            "public_factor_score": raw.get("public_factor_score"),
            "public_factor_confidence": raw.get("public_factor_confidence"),
            "responses": _compact_sequence(raw.get("responses")),
            "warnings": _compact_sequence(raw.get("warnings")),
        }
    )


def _select_public_heartbeat_compat_fields(values: dict[str, Any], *, allowed: tuple[str, ...]) -> dict[str, Any]:
    return {key: values.get(key) for key in allowed}


def _public_single_demo_heartbeat_details(payload: dict[str, Any]) -> dict[str, Any]:
    contract = _contract_single_demo_heartbeat_details(payload)
    summary = _json_mapping(contract.get("summary"))
    account = _json_mapping(contract.get("account"))
    position = _json_mapping(contract.get("position"))
    plan = _json_mapping(contract.get("plan"))
    public = dict(contract)
    public.update(
        _select_public_heartbeat_compat_fields(
            {
                "mode": "single",
                "cycle": summary.get("cycle"),
                "status": summary.get("status"),
                "submitted": summary.get("submitted"),
                "response_count": summary.get("response_count"),
                "warning_count": summary.get("warning_count"),
                "action": plan.get("action"),
                "current_contracts": position.get("contracts"),
                "target_contracts": plan.get("target_contracts"),
                "total_equity": account.get("total_equity"),
                "available_equity": account.get("available_equity"),
            },
            allowed=PUBLIC_SINGLE_HEARTBEAT_COMPAT_FIELDS,
        )
    )
    return _compact_mapping(public)


def _public_portfolio_demo_heartbeat_details(payload: dict[str, Any]) -> dict[str, Any]:
    contract = _contract_portfolio_demo_heartbeat_details(payload)
    summary = _json_mapping(contract.get("summary"))
    account = _json_mapping(contract.get("account"))
    public = dict(contract)
    public.pop("strategy_router_enabled", None)
    public.update(
        _select_public_heartbeat_compat_fields(
            {
                "mode": "portfolio",
                "cycle": summary.get("cycle"),
                "status": summary.get("status"),
                "symbol_count": summary.get("symbol_count"),
                "submitted_symbol_count": summary.get("submitted_symbol_count"),
                "actionable_symbol_count": summary.get("actionable_symbol_count"),
                "active_position_symbol_count": summary.get("active_position_symbol_count"),
                "response_count": summary.get("response_count"),
                "warning_count": summary.get("warning_count"),
                "action": payload.get("action") or f"{summary.get('submitted_symbol_count') or 0}/{summary.get('symbol_count') or 0} submitted",
                "total_equity": account.get("total_equity"),
                "available_equity": account.get("available_equity"),
            },
            allowed=PUBLIC_PORTFOLIO_HEARTBEAT_COMPAT_FIELDS,
        )
    )
    return _compact_mapping(public)


def _normalize_single_demo_heartbeat_details(payload: dict[str, Any], *, status: str | None) -> dict[str, Any]:
    normalized = _normalize_demo_symbol_heartbeat_state(payload, symbol=str(payload.get("symbol") or "").strip() or None)
    normalized["mode"] = "single"
    current_status = str(status or payload.get("status") or "").strip() or None

    summary = _json_mapping(payload.get("summary"))
    summary.setdefault("mode", "single")
    summary.setdefault("cycle", _coerce_int(normalized.get("cycle")))
    summary.setdefault("status", current_status)
    summary.setdefault("symbol", normalized.get("symbol"))
    summary.setdefault("submitted", bool(normalized.get("submitted")))
    summary.setdefault("response_count", _coerce_int(normalized.get("response_count")) or 0)
    summary.setdefault("warning_count", _coerce_int(normalized.get("warning_count")) or 0)
    summary.setdefault("already_submitted", bool(normalized.get("already_submitted")))
    normalized["summary"] = summary

    executor_state = _executor_state_metadata(
        executor_state=payload.get("executor_state"),
        path=normalized.get("executor_state_path"),
        status=normalized.get("executor_state_status"),
    )
    normalized["executor_state"] = executor_state
    normalized["executor_state_path"] = executor_state.get("path")
    normalized["executor_state_status"] = executor_state.get("status")
    account_payload = normalized.get("account") if isinstance(normalized.get("account"), dict) else {}
    if "total_equity" not in normalized and account_payload.get("total_equity") is not None:
        normalized["total_equity"] = account_payload.get("total_equity")
    if "available_equity" not in normalized and account_payload.get("available_equity") is not None:
        normalized["available_equity"] = account_payload.get("available_equity")
    return normalized


def _compact_single_demo_heartbeat_details(payload: dict[str, Any]) -> dict[str, Any]:
    compact = {
        "mode": "single",
        "summary": _compact_mapping(payload.get("summary")),
        "account": _compact_mapping(payload.get("account")),
        "position": _compact_mapping(payload.get("position")),
        "signal": _compact_mapping(payload.get("signal")),
        "plan": _compact_mapping(payload.get("plan")),
        "route_decision": _compact_mapping(payload.get("route_decision")),
        "executor_state": _compact_mapping(payload.get("executor_state")),
        "execution_approval": _compact_mapping(payload.get("execution_approval")),
        "responses": _compact_sequence(payload.get("responses")),
        "warnings": _compact_sequence(payload.get("warnings")),
        "error": payload.get("error"),
    }
    return _compact_mapping(compact)


def _normalize_portfolio_demo_heartbeat_details(payload: dict[str, Any], *, status: str | None) -> dict[str, Any]:
    normalized = dict(payload)
    normalized["mode"] = "portfolio"
    current_status = str(status or payload.get("status") or "").strip() or None
    raw_symbol_states = payload.get("symbol_states") if isinstance(payload.get("symbol_states"), dict) else {}
    normalized_symbol_states: dict[str, Any] = {}
    for symbol, symbol_payload in raw_symbol_states.items():
        normalized_symbol_states[str(symbol)] = _normalize_demo_symbol_heartbeat_state(symbol_payload, symbol=str(symbol))
    normalized["symbol_states"] = normalized_symbol_states

    symbol_list = payload.get("symbols")
    if isinstance(symbol_list, list):
        normalized["symbols"] = [str(item) for item in symbol_list if str(item).strip()]
    else:
        normalized["symbols"] = sorted(normalized_symbol_states)

    account_payload = _account_mapping(payload.get("account"))
    if not account_payload:
        account_payload = {
            "total_equity": _coerce_float(payload.get("total_equity")),
            "available_equity": _coerce_float(payload.get("available_equity")),
            "currency": payload.get("currency"),
        }
    normalized["account"] = {key: value for key, value in account_payload.items() if value is not None}
    if "total_equity" not in normalized and normalized["account"].get("total_equity") is not None:
        normalized["total_equity"] = normalized["account"].get("total_equity")
    if "available_equity" not in normalized and normalized["account"].get("available_equity") is not None:
        normalized["available_equity"] = normalized["account"].get("available_equity")

    summary = _json_mapping(payload.get("summary"))
    summary.setdefault("mode", "portfolio")
    summary.setdefault("cycle", _coerce_int(payload.get("cycle")))
    summary.setdefault("status", current_status)
    summary.setdefault("symbol_count", _coerce_int(payload.get("symbol_count")) or len(normalized["symbols"]))
    submitted_symbols = payload.get("submitted_symbols")
    submitted_symbol_list = [str(item) for item in submitted_symbols] if isinstance(submitted_symbols, list) else []
    summary.setdefault("submitted_symbol_count", _coerce_int(payload.get("submitted_symbol_count")) or len(submitted_symbol_list))
    summary.setdefault("actionable_symbol_count", _coerce_int(payload.get("actionable_symbol_count")) or 0)
    summary.setdefault("active_position_symbol_count", _coerce_int(payload.get("active_position_symbol_count")) or 0)
    summary.setdefault("response_count", _coerce_int(payload.get("response_count")) or 0)
    summary.setdefault("warning_count", _coerce_int(payload.get("warning_count")) or 0)
    summary.setdefault("strategy_router_enabled", bool(payload.get("strategy_router_enabled")))
    normalized["summary"] = summary

    normalized["cycle"] = summary.get("cycle")
    normalized["symbol_count"] = summary.get("symbol_count")
    normalized["submitted_symbol_count"] = summary.get("submitted_symbol_count")
    normalized["actionable_symbol_count"] = summary.get("actionable_symbol_count")
    normalized["active_position_symbol_count"] = summary.get("active_position_symbol_count")
    normalized["response_count"] = summary.get("response_count")
    normalized["warning_count"] = summary.get("warning_count")
    normalized["submitted_symbols"] = submitted_symbol_list
    normalized["strategy_router_enabled"] = summary.get("strategy_router_enabled")
    normalized["action"] = (
        payload.get("action")
        or f"{summary.get('submitted_symbol_count') or 0}/{summary.get('symbol_count') or 0} submitted"
    )

    executor_state = _executor_state_metadata(
        executor_state=payload.get("executor_state"),
        path=payload.get("executor_state_path"),
        status=payload.get("executor_state_status"),
    )
    normalized["executor_state"] = executor_state
    normalized["executor_state_path"] = executor_state.get("path")
    normalized["executor_state_status"] = executor_state.get("status")
    return normalized


def _compact_portfolio_demo_heartbeat_details(payload: dict[str, Any]) -> dict[str, Any]:
    compact_symbol_states: dict[str, Any] = {}
    symbol_states = payload.get("symbol_states") if isinstance(payload.get("symbol_states"), dict) else {}
    for symbol, symbol_payload in symbol_states.items():
        compact_symbol_states[str(symbol)] = _compact_symbol_demo_heartbeat_state(symbol_payload)
    compact = {
        "mode": "portfolio",
        "summary": _compact_mapping(payload.get("summary")),
        "account": _compact_mapping(payload.get("account")),
        "symbols": _compact_sequence(payload.get("symbols")),
        "submitted_symbols": _compact_sequence(payload.get("submitted_symbols")),
        "symbol_states": compact_symbol_states,
        "executor_state": _compact_mapping(payload.get("executor_state")),
        "execution_approval": _compact_mapping(payload.get("execution_approval")),
        "strategy_router_enabled": payload.get("strategy_router_enabled"),
        "error": payload.get("error"),
    }
    return _compact_mapping(compact)


def _compact_symbol_demo_heartbeat_state(payload: Any) -> dict[str, Any]:
    raw = _json_mapping(payload)
    compact = {
        "summary": _compact_mapping(raw.get("summary")),
        "position": _compact_mapping(raw.get("position")),
        "signal": _compact_mapping(raw.get("signal")),
        "plan": _compact_mapping(raw.get("plan")),
        "planning_account": _compact_mapping(raw.get("planning_account")),
        "route_decision": _compact_mapping(raw.get("route_decision") or raw.get("router_decision")),
        "portfolio_risk": _compact_mapping(raw.get("portfolio_risk")),
        "public_factor_score": raw.get("public_factor_score"),
        "public_factor_confidence": raw.get("public_factor_confidence"),
        "responses": _compact_sequence(raw.get("responses")),
        "warnings": _compact_sequence(raw.get("warnings")),
    }
    return _compact_mapping(compact)


def _normalize_demo_symbol_heartbeat_state(payload: Any, *, symbol: str | None = None) -> dict[str, Any]:
    raw = _json_mapping(payload)
    normalized = dict(raw)

    plan_payload = _plan_mapping(raw.get("plan"))
    if plan_payload.get("action") in {None, ""} and raw.get("action") not in {None, ""}:
        plan_payload["action"] = raw.get("action")
    if plan_payload.get("reason") in {None, ""} and raw.get("reason") not in {None, ""}:
        plan_payload["reason"] = raw.get("reason")
    if plan_payload.get("desired_side") is None:
        plan_payload["desired_side"] = _coerce_int(raw.get("desired_side"))
    if plan_payload.get("current_side") is None:
        plan_payload["current_side"] = _coerce_int(raw.get("current_side"))
    if plan_payload.get("current_contracts") is None:
        plan_payload["current_contracts"] = _coerce_float(raw.get("current_contracts"))
    if plan_payload.get("target_contracts") is None:
        plan_payload["target_contracts"] = _coerce_float(raw.get("target_contracts"))
    if plan_payload.get("latest_price") is None:
        plan_payload["latest_price"] = _coerce_float(raw.get("latest_price"))
    if plan_payload.get("signal_time") in {None, ""} and raw.get("signal_time") not in {None, ""}:
        plan_payload["signal_time"] = raw.get("signal_time")
    if plan_payload.get("effective_time") in {None, ""} and raw.get("effective_time") not in {None, ""}:
        plan_payload["effective_time"] = raw.get("effective_time")
    position_payload = _position_mapping(raw.get("position"), fallback=plan_payload)
    account_payload = _account_mapping(raw.get("account"))
    planning_account_payload = _account_mapping(raw.get("planning_account"))
    if not planning_account_payload:
        planning_account_payload = dict(account_payload)
    if not planning_account_payload:
        planning_account_payload = {
            "total_equity": _coerce_float(raw.get("total_equity")),
            "available_equity": _coerce_float(raw.get("available_equity")),
            "currency": raw.get("currency"),
        }
        planning_account_payload = {
            key: value for key, value in planning_account_payload.items() if value is not None
        }
    if not account_payload:
        account_payload = dict(planning_account_payload)
    signal_payload = _normalize_signal_payload(
        raw.get("signal"),
        signal_time=raw.get("signal_time") or plan_payload.get("signal_time"),
        effective_time=raw.get("effective_time") or plan_payload.get("effective_time"),
        latest_price=_coerce_float(raw.get("latest_price")) or _coerce_float(plan_payload.get("latest_price")),
        desired_side=_coerce_int(raw.get("desired_side")),
        ready=raw.get("submitted"),
    )
    route_payload = _route_decision_payload(
        raw.get("route_decision") or raw.get("router_decision"),
        symbol=symbol or str(raw.get("symbol") or "").strip() or None,
    )
    summary = _json_mapping(raw.get("summary"))
    summary.setdefault("symbol", symbol or raw.get("symbol"))
    summary.setdefault("status", raw.get("status"))
    summary.setdefault("submitted", bool(raw.get("submitted")))
    summary.setdefault("response_count", _coerce_int(raw.get("response_count")) or 0)
    summary.setdefault("warning_count", _coerce_int(raw.get("warning_count")) or 0)
    summary.setdefault("already_submitted", bool(raw.get("already_submitted")))

    normalized["summary"] = summary
    normalized["plan"] = plan_payload
    normalized["position"] = position_payload
    normalized["account"] = account_payload
    normalized["planning_account"] = planning_account_payload
    normalized["signal"] = signal_payload
    if route_payload is not None:
        normalized["route_decision"] = route_payload
        normalized["router_decision"] = route_payload

    resolved_symbol = symbol or str(raw.get("symbol") or raw.get("instrument") or "").strip() or None
    if resolved_symbol is not None:
        normalized["symbol"] = resolved_symbol
    normalized["instrument"] = resolved_symbol or raw.get("instrument")
    normalized["status"] = summary.get("status")
    normalized["submitted"] = summary.get("submitted")
    normalized["response_count"] = summary.get("response_count")
    normalized["warning_count"] = summary.get("warning_count")
    normalized["already_submitted"] = summary.get("already_submitted")
    normalized["action"] = raw.get("action") or plan_payload.get("action")
    normalized["reason"] = raw.get("reason") or plan_payload.get("reason")
    normalized["desired_side"] = _coerce_int(raw.get("desired_side"))
    if normalized["desired_side"] is None:
        normalized["desired_side"] = _coerce_int(signal_payload.get("desired_side"))
    normalized["current_side"] = _coerce_int(raw.get("current_side"))
    if normalized["current_side"] is None:
        normalized["current_side"] = _coerce_int(position_payload.get("side"))
    normalized["current_contracts"] = _coerce_float(raw.get("current_contracts"))
    if normalized["current_contracts"] is None:
        normalized["current_contracts"] = _coerce_float(position_payload.get("contracts"))
    normalized["target_contracts"] = _coerce_float(raw.get("target_contracts"))
    if normalized["target_contracts"] is None:
        normalized["target_contracts"] = _coerce_float(plan_payload.get("target_contracts"))
    normalized["latest_price"] = _coerce_float(raw.get("latest_price"))
    if normalized["latest_price"] is None:
        normalized["latest_price"] = _coerce_float(signal_payload.get("latest_price"))
    if normalized["latest_price"] is None:
        normalized["latest_price"] = _coerce_float(plan_payload.get("latest_price"))
    normalized["signal_time"] = raw.get("signal_time") or signal_payload.get("signal_time")
    normalized["effective_time"] = raw.get("effective_time") or signal_payload.get("effective_time")
    if planning_account_payload:
        planning_equity = _coerce_float(planning_account_payload.get("available_equity"))
        if planning_equity is None:
            planning_equity = _coerce_float(planning_account_payload.get("total_equity"))
        if planning_equity is not None and "planning_equity" not in normalized:
            normalized["planning_equity"] = planning_equity
    return normalized


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        payload = to_dict()
        if isinstance(payload, dict):
            return dict(payload)
    return {}


def _compact_mapping(value: Any) -> dict[str, Any]:
    payload = _json_mapping(value)
    compact: dict[str, Any] = {}
    for key, item in payload.items():
        if item is None:
            continue
        if isinstance(item, dict):
            nested = _compact_mapping(item)
            if nested:
                compact[key] = nested
            continue
        if isinstance(item, list):
            nested_list = _compact_sequence(item)
            if nested_list:
                compact[key] = nested_list
            continue
        compact[key] = item
    return compact


def _compact_sequence(value: Any) -> list[Any]:
    if not isinstance(value, list):
        return []
    compact: list[Any] = []
    for item in value:
        if item is None:
            continue
        if isinstance(item, dict):
            nested = _compact_mapping(item)
            if nested:
                compact.append(nested)
            continue
        if isinstance(item, list):
            nested_list = _compact_sequence(item)
            if nested_list:
                compact.append(nested_list)
            continue
        compact.append(item)
    return compact


def _plan_mapping(value: Any) -> dict[str, Any]:
    payload = _json_mapping(value)
    if payload:
        return payload
    return {
        "action": None,
        "reason": None,
        "desired_side": None,
        "current_side": None,
        "current_contracts": None,
        "target_contracts": None,
        "latest_price": None,
        "signal_time": None,
        "effective_time": None,
        "position_mode": None,
        "instructions": [],
        "warnings": [],
    }


def _position_mapping(value: Any, *, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = _json_mapping(value)
    if payload:
        return payload
    fallback = fallback or {}
    result = {
        "side": _coerce_int(fallback.get("current_side")),
        "contracts": _coerce_float(fallback.get("current_contracts")),
        "position_mode": fallback.get("position_mode"),
    }
    return {key: value for key, value in result.items() if value is not None}


def _account_mapping(value: Any) -> dict[str, Any]:
    payload = _json_mapping(value)
    return {key: value for key, value in payload.items() if value is not None}


def _route_decision_payload(value: Any, *, symbol: str | None = None) -> dict[str, Any] | None:
    if value is None:
        return None
    payload = serialize_strategy_route_decision(value, symbol=symbol, required_scope="demo")
    return payload if isinstance(payload, dict) else None


def _executor_state_metadata(
    *,
    executor_state: Any,
    path: Any = None,
    status: Any = None,
) -> dict[str, Any]:
    payload = _json_mapping(executor_state)
    resolved = dict(payload)
    if path is not None and resolved.get("path") in {None, ""}:
        resolved["path"] = path
    if status is not None and resolved.get("status") in {None, ""}:
        resolved["status"] = status
    return resolved


def public_executor_state_payload(
    *,
    executor_state_info: dict[str, Any],
    latest_demo_heartbeat: ServiceHeartbeat | None,
) -> dict[str, Any]:
    status = str(executor_state_info.get("status") or "missing")
    payload = executor_state_payload(executor_state_info)
    result = {
        "status": status,
        "path": executor_state_info.get("path"),
        "legacy_fallback_used": bool(executor_state_info.get("legacy_fallback_used")),
    }
    if status != "ok":
        return result

    last_error = payload.get("last_error")
    if isinstance(last_error, dict) and latest_demo_heartbeat is not None and latest_demo_heartbeat.status != "error":
        recovered = _normalize_recovered_error(last_error=last_error, latest_demo_heartbeat=latest_demo_heartbeat)
        if recovered is not None:
            payload = dict(payload)
            payload["recovered_error"] = recovered
            payload["last_error"] = None

    result.update(
        {
            "last_submitted_at": payload.get("last_submitted_at"),
            "last_submitted_signature": payload.get("last_submitted_signature"),
            "last_error": payload.get("last_error"),
            "last_plan": payload.get("last_plan"),
            "last_signal": _normalize_signal_payload(payload.get("last_signal")) if payload.get("last_signal") else None,
            "symbols": _normalize_executor_symbol_payloads(payload.get("symbols")),
            "recovered_error": payload.get("recovered_error"),
        }
    )
    return result


def build_preflight_payload(
    config: AppConfig,
    session_factory,
    project_root: Path,
    *,
    resolve_proxy_egress_ip_fn: Callable[[str | None], str | None] | None = None,
) -> dict[str, Any]:
    from quant_lab.execution.strategy_router import build_strategy_router_status

    current_mode = demo_mode(config=config)
    current_required_scope = required_execution_scope(config=config)
    latest_heartbeat_row, latest_namespace, heartbeat_legacy_fallback = latest_demo_heartbeat(
        session_factory=session_factory,
        mode=current_mode,
    )
    executor_state_info = load_executor_state_info(
        config=config,
        project_root=project_root,
        mode=current_mode,
    )
    executor_state = public_executor_state_payload(
        executor_state_info=executor_state_info,
        latest_demo_heartbeat=latest_heartbeat_row,
    )
    execution_approval = build_execution_approval_payload(
        config=config,
        session_factory=session_factory,
        project_root=project_root,
    )
    runtime_policy = build_rollout_policy_payload(config=config)
    strategy_router = build_strategy_router_status(
        session_factory=session_factory,
        config=config,
        required_scope=current_required_scope,
    )
    demo_trading = build_demo_trading_payload(
        config=config,
        execution_approval=execution_approval,
        executor_state_info=executor_state_info,
        rollout_policy=runtime_policy,
    )

    telegram_ready = (
        config.alerts.telegram_enabled
        and bool(config.alerts.telegram_bot_token)
        and bool(config.alerts.telegram_chat_id)
    )
    email_ready = (
        config.alerts.email_enabled
        and bool(config.alerts.email_from)
        and bool(config.alerts.email_to)
        and bool(config.alerts.smtp_host)
        and (not config.alerts.smtp_username or bool(config.alerts.smtp_password))
    )

    payload = {
        "demo_trading": demo_trading,
        "runtime_policy": runtime_policy,
        "rollout_policy": runtime_policy,
        "alerts": {
            "any_ready": telegram_ready or email_ready,
            "channels": {
                "telegram": {
                    "enabled": bool(config.alerts.telegram_enabled),
                    "ready": telegram_ready,
                },
                "email": {
                    "enabled": bool(config.alerts.email_enabled),
                    "ready": email_ready,
                },
            },
        },
        "okx_connectivity": _build_okx_connectivity_payload(
            config=config,
            latest_demo_heartbeat=latest_heartbeat_row,
            resolve_proxy_egress_ip_fn=resolve_proxy_egress_ip_fn or resolve_proxy_egress_ip,
        ),
        "execution_loop": {
            "namespace": latest_namespace,
            "legacy_fallback_used": heartbeat_legacy_fallback,
            "latest_heartbeat": serialize_service_heartbeat(latest_heartbeat_row, include_status_label=True) if latest_heartbeat_row is not None else None,
            "executor_state": executor_state,
        },
        "execution_approval": execution_approval,
        "strategy_router": strategy_router,
        "route_decisions": execution_approval.get("route_decisions") or {},
    }
    payload["dashboard_summary"] = build_runtime_dashboard_summary(preflight=payload)
    return payload


def load_live_reconcile(
    *,
    config: AppConfig,
    session_factory,
    project_root: Path,
    mode: str,
) -> dict[str, Any]:
    executor_state_info = load_executor_state_info(config=config, project_root=project_root, mode=mode)
    executor_state = executor_state_payload(executor_state_info)
    state_warning = None
    if executor_state_info.get("status") == "invalid_json":
        state_warning = f"Executor state is invalid JSON at {executor_state_info.get('path')}."

    if mode == "portfolio":
        symbols = configured_symbols(config)
        account, symbol_states = _invoke_demo_loader(
            load_demo_portfolio_state,
            config,
            symbols,
            session_factory=session_factory,
            project_root=project_root,
        )
        reconcile = build_demo_portfolio_payload(
            cfg=config,
            account=account,
            symbol_states=symbol_states,
            include_exchange_checks=True,
            executor_state=executor_state,
        )
        if state_warning:
            warnings = reconcile.get("warnings")
            if isinstance(warnings, list):
                warnings.append(state_warning)
        return reconcile

    account, position, state = _invoke_demo_loader(
        load_demo_state,
        config,
        session_factory=session_factory,
        project_root=project_root,
    )
    reconcile = build_demo_reconcile_payload(
        cfg=config,
        account=account,
        position=position,
        signal=state["signal"],
        plan=state["plan"],
        state=state,
        executor_state=executor_state,
    )
    if state_warning:
        warnings = reconcile.get("warnings")
        if isinstance(warnings, list):
            warnings.append(state_warning)
    return reconcile


def build_runtime_snapshot(
    *,
    config: AppConfig,
    session_factory,
    project_root: Path,
) -> dict[str, Any]:
    symbols = configured_symbols(config)
    preflight = build_preflight_payload(
        config=config,
        session_factory=session_factory,
        project_root=project_root,
    )
    live_error: str | None = None
    snapshot_source = "live_okx"
    current_mode = demo_mode(config=config, symbols=symbols)

    try:
        reconcile = load_live_reconcile(
            config=config,
            session_factory=session_factory,
            project_root=project_root,
            mode=current_mode,
        )
    except Exception as exc:
        live_error = f"{type(exc).__name__}: {exc}"
        snapshot_source = "cached_local_state"
        if current_mode == "portfolio":
            reconcile = _build_cached_portfolio_reconcile(
                config=config,
                preflight=preflight,
                live_error=live_error,
                symbols=symbols,
            )
        else:
            reconcile = _build_cached_reconcile(
                config=config,
                preflight=preflight,
                live_error=live_error,
            )

    payload = {
        "preflight": preflight,
        "reconcile": reconcile,
        "snapshot_source": snapshot_source,
    }
    if live_error is not None:
        payload["live_error"] = live_error
    return payload


def build_demo_visuals_payload(
    *,
    session_factory,
    reconcile: dict[str, Any],
    history_limit: int = 120,
    alert_limit: int = 8,
) -> dict[str, Any]:
    current_mode = "portfolio" if reconcile.get("mode") == "portfolio" else "single"
    heartbeats, namespace, legacy_fallback = recent_demo_heartbeats(
        session_factory=session_factory,
        mode=current_mode,
        limit=history_limit,
    )
    with session_scope(session_factory) as session:
        status_rows = session.execute(
            select(ServiceHeartbeat.status, func.count())
            .where(ServiceHeartbeat.service_name == namespace)
            .group_by(ServiceHeartbeat.status)
        ).all()
        last_submitted_row = session.execute(
            select(ServiceHeartbeat)
            .where(
                ServiceHeartbeat.service_name == namespace,
                ServiceHeartbeat.status == "submitted",
            )
            .order_by(desc(ServiceHeartbeat.created_at))
            .limit(1)
        ).scalar_one_or_none()
        last_error_row = session.execute(
            select(ServiceHeartbeat)
            .where(
                ServiceHeartbeat.service_name == namespace,
                ServiceHeartbeat.status == "error",
            )
            .order_by(desc(ServiceHeartbeat.created_at))
            .limit(1)
        ).scalar_one_or_none()
        alerts = list(
            session.execute(
                select(AlertEvent)
                .where(AlertEvent.event_key.in_(("demo_order_submitted", "demo_loop_error")))
                .order_by(desc(AlertEvent.created_at))
                .limit(alert_limit)
            ).scalars()
        )

    heartbeats.reverse()
    chart_points = [_demo_visual_heartbeat_point(row) for row in heartbeats]
    recent_events = list(reversed(chart_points[-12:]))

    status_counts = {str(status): int(count) for status, count in status_rows}
    submitted_count = status_counts.get("submitted", 0)
    duplicate_count = status_counts.get("duplicate", 0)
    warning_count = status_counts.get("warning", 0)
    error_count = status_counts.get("error", 0)
    idle_count = status_counts.get("idle", 0)
    total_cycles = sum(status_counts.values())
    last_point = chart_points[-1] if chart_points else None
    last_submitted = _demo_visual_heartbeat_point(last_submitted_row) if last_submitted_row is not None else None
    last_error = _demo_visual_heartbeat_point(last_error_row) if last_error_row is not None else None
    portfolio_mode = reconcile.get("mode") == "portfolio" or any(
        point.get("mode") == "portfolio" for point in chart_points
    )

    alert_feed = [
        {
            "event_key": item.event_key,
            "channel": item.channel,
            "status": item.status,
            "title": item.title,
            "message": item.message,
            "created_at": serialize_utc_datetime(item.created_at),
        }
        for item in alerts
    ]

    if portfolio_mode:
        symbol_states = reconcile.get("symbol_states") or {}
        symbol_states = symbol_states if isinstance(symbol_states, dict) else {}
        per_symbol_states = []
        total_current_contracts = 0.0
        total_target_contracts = 0.0
        for symbol in sorted(symbol_states):
            payload = symbol_states[symbol]
            position = payload.get("position") or {}
            plan = payload.get("plan") or {}
            checks = payload.get("checks") or {}
            current_contracts = _coerce_float(position.get("contracts")) or 0.0
            target_contracts = _coerce_float(plan.get("target_contracts")) or 0.0
            total_current_contracts += current_contracts
            total_target_contracts += target_contracts
            per_symbol_states.append(
                {
                    "symbol": symbol,
                    "action": plan.get("action"),
                    "current_side": _coerce_int(position.get("side")),
                    "current_side_label": client_side_label(position.get("side")),
                    "desired_side": _coerce_int((payload.get("signal") or {}).get("desired_side")),
                    "desired_side_label": client_side_label((payload.get("signal") or {}).get("desired_side")),
                    "current_contracts": current_contracts,
                    "target_contracts": target_contracts,
                    "contract_gap": round(current_contracts - target_contracts, 4),
                    "leverage_match": checks.get("leverage_match"),
                    "size_match": checks.get("size_match"),
                    "protective_stop_ready": checks.get("protective_stop_ready"),
                }
            )

        return {
            "summary": {
                "mode": "portfolio",
                "namespace": namespace,
                "legacy_fallback_used": legacy_fallback,
                "total_cycles": total_cycles,
                "submitted_count": submitted_count,
                "duplicate_count": duplicate_count,
                "warning_count": warning_count,
                "error_count": error_count,
                "idle_count": idle_count,
                "submission_rate_pct": round((submitted_count / total_cycles) * 100, 2) if total_cycles else 0.0,
                "last_cycle": last_point["cycle"] if last_point else None,
                "last_status": last_point["status"] if last_point else None,
                "last_status_label": demo_history_status_label(last_point),
                "last_event_time": last_point["created_at"] if last_point else None,
                "last_submitted_at": last_submitted["created_at"] if last_submitted else None,
                "last_error_at": last_error["created_at"] if last_error else None,
                "symbol_count": len(symbol_states),
                "submitted_symbol_count": _coerce_int(last_point.get("submitted_symbol_count")) if last_point else None,
                "actionable_symbol_count": _coerce_int(last_point.get("actionable_symbol_count")) if last_point else None,
                "active_position_symbol_count": _coerce_int(last_point.get("active_position_symbol_count")) if last_point else None,
                "current_contracts": round(total_current_contracts, 4),
                "target_contracts": round(total_target_contracts, 4),
                "contract_gap": round(total_current_contracts - total_target_contracts, 4),
            },
            "chart": {
                "mode": "portfolio",
                "namespace": namespace,
                "points": chart_points,
                "latest_target_contracts": _coerce_int(last_point.get("actionable_symbol_count")) if last_point else None,
                "latest_live_contracts": _coerce_int(last_point.get("active_position_symbol_count")) if last_point else None,
            },
            "recent_events": recent_events,
            "recent_alerts": alert_feed,
            "status_counts": [
                {"status": status, "label": autotrade_status_label(status), "count": count}
                for status, count in sorted(status_counts.items())
            ],
            "per_symbol_states": per_symbol_states,
        }

    current_contracts = _coerce_float((reconcile.get("position") or {}).get("contracts"))
    if current_contracts is None and last_point is not None:
        current_contracts = _coerce_float(last_point.get("current_contracts"))
    target_contracts = _coerce_float((reconcile.get("plan") or {}).get("target_contracts"))
    if target_contracts is None and last_point is not None:
        target_contracts = _coerce_float(last_point.get("target_contracts"))
    current_side = _coerce_int((reconcile.get("position") or {}).get("side"))
    if current_side is None and last_point is not None:
        current_side = _coerce_int(last_point.get("current_side"))
    desired_side = _coerce_int((reconcile.get("signal") or {}).get("desired_side"))
    if desired_side is None and last_point is not None:
        desired_side = _coerce_int(last_point.get("desired_side"))
    contract_gap = None
    if current_contracts is not None and target_contracts is not None:
        contract_gap = round(current_contracts - target_contracts, 4)

    return {
        "summary": {
            "mode": "single",
            "namespace": namespace,
            "legacy_fallback_used": legacy_fallback,
            "total_cycles": total_cycles,
            "submitted_count": submitted_count,
            "duplicate_count": duplicate_count,
            "warning_count": warning_count,
            "error_count": error_count,
            "idle_count": idle_count,
            "submission_rate_pct": round((submitted_count / total_cycles) * 100, 2) if total_cycles else 0.0,
            "last_cycle": last_point["cycle"] if last_point else None,
            "last_status": last_point["status"] if last_point else None,
            "last_status_label": demo_history_status_label(last_point),
            "last_event_time": last_point["created_at"] if last_point else None,
            "last_submitted_at": last_submitted["created_at"] if last_submitted else None,
            "last_error_at": last_error["created_at"] if last_error else None,
            "current_contracts": current_contracts,
            "target_contracts": target_contracts,
            "contract_gap": contract_gap,
            "current_side": current_side,
            "current_side_label": client_side_label(current_side),
            "desired_side": desired_side,
            "desired_side_label": client_side_label(desired_side),
        },
        "chart": {
            "namespace": namespace,
            "points": chart_points,
            "latest_target_contracts": target_contracts,
            "latest_live_contracts": current_contracts,
        },
        "recent_events": recent_events,
        "recent_alerts": alert_feed,
        "status_counts": [
            {"status": status, "label": autotrade_status_label(status), "count": count}
            for status, count in sorted(status_counts.items())
        ],
    }


def build_autotrade_status(
    *,
    preflight: dict[str, Any],
    reconcile: dict[str, Any],
    demo_visuals: dict[str, Any],
    snapshot_source: str,
    live_error: str | None,
) -> dict[str, Any]:
    demo_trading = preflight.get("demo_trading") or {}
    execution_loop = preflight.get("execution_loop") or {}
    latest_heartbeat = execution_loop.get("latest_heartbeat") or {}
    executor_state = execution_loop.get("executor_state") or {}
    latest_loop_status_label = str(
        latest_heartbeat.get("status_label")
        or autotrade_status_label(latest_heartbeat.get("status"))
    )

    mode = "portfolio" if reconcile.get("mode") == "portfolio" else "single"
    can_submit = bool(demo_trading.get("ready"))
    reasons: list[str] = []
    blocking_reasons: list[str] = []
    actionable_symbols: list[str] = []
    active_symbols: list[str] = []
    state_code = "idle"
    headline = "当前没有可执行信号"
    next_hint = "保持服务运行，等待下一次策略周期确认。"
    will_submit_now = False
    level = "idle"

    if snapshot_source != "live_okx":
        reasons.append("当前页面未直接拿到 OKX 实时状态，展示的是本地缓存快照。")
        if live_error:
            reasons.append(f"实时抓取失败：{live_error}")

    recovered_error = executor_state.get("recovered_error") if isinstance(executor_state, dict) else None
    if isinstance(recovered_error, dict):
        recovered_message = str(recovered_error.get("message") or "").strip()
        if recovered_message:
            reasons.append(f"最近一次循环错误已恢复：{recovered_message}")

    if not can_submit:
        state_code = "blocked_config"
        headline = "自动下单未就绪"
        level = "blocked"
        blocking_reasons.extend(_translate_demo_trading_reasons(demo_trading.get("reasons")))
        if not blocking_reasons:
            blocking_reasons.append("当前运行配置不允许向 OKX Demo 提交订单。")
        reasons.extend(blocking_reasons)
        next_hint = "补齐 Demo API 密钥并确认已开启自动下单开关。"
        return {
            "mode": mode,
            "state_code": state_code,
            "level": level,
            "headline": headline,
            "can_submit": can_submit,
            "will_submit_now": False,
            "submit_mode": str(demo_trading.get("mode") or "unknown"),
            "reasons": reasons,
            "blocking_reasons": blocking_reasons,
            "actionable_symbols": actionable_symbols,
            "active_symbols": active_symbols,
            "latest_loop_status": latest_heartbeat.get("status"),
            "latest_loop_status_label": latest_loop_status_label,
            "latest_event_time": latest_heartbeat.get("created_at") or (demo_visuals.get("summary") or {}).get("last_event_time"),
            "next_hint": next_hint,
        }

    if mode == "portfolio":
        blocking_reasons, actionable_symbols, active_symbols = _portfolio_autotrade_details(reconcile)
    else:
        blocking_reasons, actionable_symbols, active_symbols = _single_autotrade_details(reconcile)

    if blocking_reasons:
        state_code = "blocked_exchange"
        headline = "自动下单被账户或交易所状态阻塞"
        level = "blocked"
        reasons.extend(blocking_reasons)
        next_hint = _pick_blocked_hint(blocking_reasons)
    elif snapshot_source != "live_okx":
        state_code = "stale_snapshot"
        headline = "客户端快照已回退到缓存状态"
        level = "warning"
        next_hint = "先恢复 OKX 实时抓取，再依据页面状态做操作判断。"
    elif actionable_symbols:
        state_code = "ready_actionable"
        headline = f"当前有 {len(actionable_symbols)} 个可执行信号，下一轮会尝试提交 Demo 订单"
        level = "ok"
        will_submit_now = True
        reasons.append(f"可执行标的：{', '.join(actionable_symbols)}")
        next_hint = "保持服务运行即可，系统会在下一轮循环按计划下单。"
    elif active_symbols:
        state_code = "holding"
        headline = "当前已有持仓，策略没有新的调仓动作"
        level = "idle"
        reasons.extend(_collect_idle_reasons(reconcile, mode=mode))
        next_hint = "继续观察持仓与保护止损是否保持正常。"
    else:
        state_code = "idle"
        headline = "当前没有可执行信号"
        level = "idle"
        reasons.extend(_collect_idle_reasons(reconcile, mode=mode))
        next_hint = "等待下一次信号刷新。"

    return {
        "mode": mode,
        "state_code": state_code,
        "level": level,
        "headline": headline,
        "can_submit": can_submit,
        "will_submit_now": will_submit_now,
        "submit_mode": str(demo_trading.get("mode") or "unknown"),
        "reasons": reasons,
        "blocking_reasons": blocking_reasons,
        "actionable_symbols": actionable_symbols,
        "active_symbols": active_symbols,
        "latest_loop_status": latest_heartbeat.get("status"),
        "latest_loop_status_label": latest_loop_status_label,
        "latest_event_time": latest_heartbeat.get("created_at") or (demo_visuals.get("summary") or {}).get("last_event_time"),
        "next_hint": next_hint,
    }


def build_client_headline_summary(
    *,
    preflight: dict[str, Any],
    autotrade_status: dict[str, Any],
    demo_visuals: dict[str, Any],
    snapshot_source: str,
) -> dict[str, Any]:
    demo_trading = preflight.get("demo_trading") or {}
    execution_loop = preflight.get("execution_loop") or {}
    latest_heartbeat = execution_loop.get("latest_heartbeat") or {}
    can_submit = bool(demo_trading.get("ready"))
    will_submit_now = bool(autotrade_status.get("will_submit_now"))
    latest_loop_status = str(
        latest_heartbeat.get("status")
        or autotrade_status.get("latest_loop_status")
        or "missing"
    )
    latest_loop_status_label = str(
        autotrade_status.get("latest_loop_status_label")
        or latest_heartbeat.get("status_label")
        or autotrade_status_label(latest_loop_status)
    )
    latest_event_time = latest_heartbeat.get("created_at") or (demo_visuals.get("summary") or {}).get("last_event_time")
    level = "ok" if can_submit and will_submit_now else "warn" if can_submit else "danger"

    if can_submit:
        pill_text = "可自动执行" if will_submit_now else "通道已开，等待信号"
        submit_value = "允许"
        submit_level = "ok"
        submit_note = "当前配置允许向 OKX Demo 提交订单"
    else:
        pill_text = "当前不可提交"
        submit_value = "不允许"
        submit_level = "danger"
        submit_note = "当前配置或门禁仍阻止提交"

    if will_submit_now:
        actionable_value = "有动作"
        actionable_level = "ok"
        actionable_note = "本轮存在可执行指令"
    else:
        actionable_value = "无动作"
        actionable_level = "warn"
        actionable_note = "当前没有可执行动作或仍被阻塞"

    loop_note = f"最近循环时间：{latest_event_time}" if latest_event_time else "当前还没有演示执行循环心跳"
    return {
        "level": level,
        "pill_text": pill_text,
        "title": str(autotrade_status.get("headline") or ("自动交易已就绪" if can_submit else "自动交易未就绪")),
        "mode_label": _client_demo_mode_label(demo_trading.get("mode")),
        "source_label": _client_snapshot_source_label(snapshot_source),
        "latest_event_time": latest_event_time,
        "submit": {
            "value": submit_value,
            "level": submit_level,
            "note": submit_note,
        },
        "actionable": {
            "value": actionable_value,
            "level": actionable_level,
            "note": actionable_note,
        },
        "loop": {
            "value": latest_loop_status_label,
            "level": "danger" if latest_loop_status == "error" else "ok",
            "note": loop_note,
            "status": latest_loop_status,
        },
    }


def build_client_warning_summary(
    *,
    preflight: dict[str, Any],
    reconcile: dict[str, Any],
    autotrade_status: dict[str, Any],
    live_error: str | None,
) -> dict[str, Any]:
    demo_trading = preflight.get("demo_trading") or {}
    warning_items: list[dict[str, str]] = []
    warning_items.extend(
        _warning_summary_entries(
            source="preflight",
            messages=_translate_demo_trading_reasons(demo_trading.get("reasons")),
        )
    )
    warning_items.extend(
        _warning_summary_entries(
            source="reconcile",
            messages=reconcile.get("warnings"),
        )
    )
    warning_items.extend(
        _warning_summary_entries(
            source="autotrade",
            messages=autotrade_status.get("blocking_reasons"),
        )
    )
    if live_error:
        warning_items.append(
            {
                "source": "live_error",
                "text": f"实时抓取失败：{live_error}",
            }
        )

    deduped_items: list[dict[str, str]] = []
    seen_messages: set[str] = set()
    for item in warning_items:
        text = str(item.get("text") or "").strip()
        if not text or text in seen_messages:
            continue
        seen_messages.add(text)
        deduped_items.append(
            {
                "source": str(item.get("source") or "unknown"),
                "text": text,
            }
        )

    return {
        "has_warnings": bool(deduped_items),
        "count": len(deduped_items),
        "items": deduped_items,
        "messages": [item["text"] for item in deduped_items],
    }


def build_client_checks_summary(
    *,
    preflight: dict[str, Any],
    reconcile: dict[str, Any],
) -> dict[str, Any]:
    demo_trading = preflight.get("demo_trading") or {}
    mode = "portfolio" if reconcile.get("mode") == "portfolio" else "single"
    demo_ready = bool(demo_trading.get("ready"))
    demo_value = "已打通" if demo_ready else _client_demo_mode_label(demo_trading.get("mode"))
    demo_note = (
        "可以向 OKX Demo 提交组合计划"
        if mode == "portfolio" and demo_ready
        else "组合计划暂时还不能提交"
        if mode == "portfolio"
        else "可以提交到 OKX Demo"
        if demo_ready
        else "当前仍是仅规划或受阻状态"
    )
    cards = {
        "demo": {
            "value": demo_value,
            "level": "ok" if demo_ready else "danger",
            "note": demo_note,
        }
    }

    if mode == "portfolio":
        counts = _portfolio_check_counts(reconcile)
        cards["leverage"] = {
            "value": f"{counts['leverage_ready']}/{counts['total']}",
            "level": "ok" if counts["total"] > 0 and counts["leverage_ready"] == counts["total"] else "warn",
            "note": "已对齐目标杠杆的标的数量",
        }
        cards["size"] = {
            "value": f"{counts['size_ready']}/{counts['total']}",
            "level": "ok" if counts["total"] > 0 and counts["size_ready"] == counts["total"] else "warn",
            "note": "已贴合目标仓位的标的数量",
        }
        cards["stop"] = {
            "value": "无持仓" if counts["active"] == 0 else f"{counts['stop_ready']}/{counts['active']}",
            "level": "ok" if counts["active"] == 0 or counts["stop_ready"] == counts["active"] else "warn",
            "note": "已挂保护止损的持仓标的数量",
        }
        return {
            "mode": mode,
            "cards": cards,
            "counts": counts,
        }

    checks = reconcile.get("checks") or {}
    cards["leverage"] = _client_single_check_card(
        present="leverage_match" in checks,
        ready=checks.get("leverage_match"),
        ready_value="已对齐",
        blocked_value="未对齐",
        ready_note="交易所杠杆与策略目标一致",
        blocked_note="交易所杠杆还未对齐",
        blocked_level="warn",
    )
    cards["size"] = _client_single_check_card(
        present="size_match" in checks,
        ready=checks.get("size_match"),
        ready_value="已对齐",
        blocked_value="未对齐",
        ready_note="当前仓位已跟随目标仓位",
        blocked_note="当前仓位与目标仓位仍有偏差",
        blocked_level="warn",
    )
    cards["stop"] = _client_single_check_card(
        present="protective_stop_ready" in checks,
        ready=checks.get("protective_stop_ready"),
        ready_value="已就绪",
        blocked_value="缺失",
        ready_note="保护止损已存在",
        blocked_note="当前保护止损未就绪",
        blocked_level="danger",
    )
    return {
        "mode": mode,
        "cards": cards,
    }


def build_client_exchange_summary(
    *,
    preflight: dict[str, Any],
    reconcile: dict[str, Any],
    snapshot_source: str,
    live_error: str | None,
) -> dict[str, Any]:
    exchange = reconcile.get("exchange") if isinstance(reconcile.get("exchange"), dict) else {}
    account = reconcile.get("account") if isinstance(reconcile.get("account"), dict) else {}
    position = reconcile.get("position") if isinstance(reconcile.get("position"), dict) else {}
    leverage = exchange.get("leverage") if isinstance(exchange.get("leverage"), dict) else {}
    protection_stop = exchange.get("protection_stop") if isinstance(exchange.get("protection_stop"), dict) else {}
    pending_orders = exchange.get("pending_orders") if isinstance(exchange.get("pending_orders"), dict) else {}
    pending_algo_orders = exchange.get("pending_algo_orders") if isinstance(exchange.get("pending_algo_orders"), dict) else {}
    okx = preflight.get("okx_connectivity") if isinstance(preflight.get("okx_connectivity"), dict) else {}

    leverage_values = leverage.get("values") if isinstance(leverage.get("values"), list) else []
    items = [
        {"label": "数据来源", "value": _client_snapshot_source_label(snapshot_source)},
        {
            "label": "账户模式",
            "value": str(account.get("account_mode") or position.get("position_mode") or "--"),
        },
        {"label": "普通挂单数", "value": _format_summary_number(pending_orders.get("count"), digits=0)},
        {"label": "条件单数", "value": _format_summary_number(pending_algo_orders.get("count"), digits=0)},
        {
            "label": "杠杆值",
            "value": ", ".join(str(item) for item in leverage_values if str(item).strip()) or "--",
        },
        {
            "label": "保护止损",
            "value": _client_bool_label(
                protection_stop.get("ready"),
                true_label="已就绪",
                false_label="未就绪",
            ),
        },
        {"label": "OKX Profile", "value": str(okx.get("profile") or "--")},
        {"label": "代理", "value": str(okx.get("proxy_url") or "未配置")},
        {"label": "出口 IP", "value": str(okx.get("egress_ip") or "--")},
    ]

    for note in okx.get("notes") if isinstance(okx.get("notes"), list) else []:
        text = str(note).strip()
        if text:
            items.append({"label": "连接提示", "value": text})
    if live_error:
        items.append({"label": "实时错误", "value": f"{live_error}"})

    return {
        "items": items,
    }


def build_client_plan_summary(*, reconcile: dict[str, Any]) -> dict[str, Any]:
    mode = "portfolio" if reconcile.get("mode") == "portfolio" else "single"
    if mode == "portfolio":
        summary = reconcile.get("summary") if isinstance(reconcile.get("summary"), dict) else {}
        account = reconcile.get("account") if isinstance(reconcile.get("account"), dict) else {}
        currency = str(account.get("currency") or "USDT")
        items = [
            f"账户权益：{_format_summary_amount(account.get('total_equity'), currency=currency)}",
            f"可用权益：{_format_summary_amount(account.get('available_equity'), currency=currency)}",
            f"allocation_mode：{summary.get('allocation_mode') or '--'}",
            f"组合标的数：{_format_summary_number(summary.get('symbol_count'), digits=0)}",
            f"可执行标的数：{_format_summary_number(summary.get('actionable_symbol_count'), digits=0)}",
            f"当前有持仓标的数：{_format_summary_number(summary.get('active_position_symbol_count'), digits=0)}",
            f"requested_total_risk_pct：{_format_summary_percent(summary.get('requested_total_risk_pct'))}",
            f"allocated_total_risk_pct：{_format_summary_percent(summary.get('allocated_total_risk_pct'))}",
            f"portfolio_total_risk_cap_pct：{_format_summary_percent(summary.get('portfolio_total_risk_cap_pct'))}",
            f"same_direction_risk_cap_pct：{_format_summary_percent(summary.get('same_direction_risk_cap_pct'))}",
            f"budgeted_equity_total：{_format_summary_amount(summary.get('budgeted_equity_total'), currency=currency)}",
            f"budgeted_symbol_count：{_format_summary_number(summary.get('budgeted_symbol_count'), digits=0)}",
            f"per_symbol_planning_equity：{_format_summary_amount(summary.get('per_symbol_planning_equity'), currency=currency)}",
            f"planning_equity_reference：{_format_summary_amount(summary.get('planning_equity_reference'), currency=currency)}",
            "最近循环模式：组合模式",
        ]
        return {
            "mode": mode,
            "items": items,
        }

    account = reconcile.get("account") if isinstance(reconcile.get("account"), dict) else {}
    position = reconcile.get("position") if isinstance(reconcile.get("position"), dict) else {}
    signal = reconcile.get("signal") if isinstance(reconcile.get("signal"), dict) else {}
    plan = reconcile.get("plan") if isinstance(reconcile.get("plan"), dict) else {}
    currency = str(account.get("currency") or "USDT")
    items = [
        f"账户权益：{_format_summary_amount(account.get('total_equity'), currency=currency)}",
        f"可用权益：{_format_summary_amount(account.get('available_equity'), currency=currency)}",
        f"当前持仓：{client_side_label(position.get('side'))} | {_format_contracts(position.get('contracts'))}",
        f"策略方向：{client_side_label(signal.get('desired_side'))}",
        f"计划动作：{str(plan.get('action') or '无')}",
        f"目标仓位：{_format_contracts(plan.get('target_contracts'))}",
        f"计划原因：{_client_reason_text(plan.get('reason') or '未提供原因')}",
        f"最新价格：{_format_summary_number(signal.get('latest_price') if signal.get('latest_price') is not None else plan.get('latest_price'), digits=2)}",
    ]
    return {
        "mode": mode,
        "items": items,
    }


def build_client_symbol_summary(*, reconcile: dict[str, Any]) -> dict[str, Any]:
    mode = "portfolio" if reconcile.get("mode") == "portfolio" else "single"
    if mode == "portfolio":
        symbol_states = reconcile.get("symbol_states") if isinstance(reconcile.get("symbol_states"), dict) else {}
        cards: list[dict[str, Any]] = []
        for symbol, payload in sorted(symbol_states.items()):
            state = payload if isinstance(payload, dict) else {}
            position = state.get("position") if isinstance(state.get("position"), dict) else {}
            signal = state.get("signal") if isinstance(state.get("signal"), dict) else {}
            plan = state.get("plan") if isinstance(state.get("plan"), dict) else {}
            checks = state.get("checks") if isinstance(state.get("checks"), dict) else {}
            planning_account = state.get("planning_account") if isinstance(state.get("planning_account"), dict) else {}
            portfolio_risk = state.get("portfolio_risk") if isinstance(state.get("portfolio_risk"), dict) else {}
            router = state.get("router_decision") if isinstance(state.get("router_decision"), dict) else {}
            display = router.get("display") if isinstance(router.get("display"), dict) else {}
            route_meta = router.get("route") if isinstance(router.get("route"), dict) else {}
            route_label = str(
                display.get("route_label")
                or route_meta.get("label")
                or router.get("route_key")
                or router.get("regime")
                or "--"
            )
            planning_equity = (
                planning_account.get("available_equity")
                if planning_account.get("available_equity") is not None
                else planning_account.get("total_equity")
            )
            lines = [
                f"当前方向：{client_side_label(position.get('side'))} | 策略方向：{client_side_label(signal.get('desired_side'))}",
                f"当前仓位：{_format_contracts(position.get('contracts'))} | 目标仓位：{_format_contracts(plan.get('target_contracts'))}",
                f"计划动作：{client_action_label(plan.get('action'))}",
                f"规划权益：{_format_summary_amount(planning_equity, currency=str(planning_account.get('currency') or 'USDT'))} | 路由：{route_label}（{_client_route_ready_label(router.get('ready'))}）",
                f"申请风险：{_format_fraction_percent(portfolio_risk.get('base_risk_fraction'))} | 分配风险：{_format_fraction_percent(portfolio_risk.get('scaled_risk_fraction'))}",
                f"申请目标仓位：{_format_summary_number(portfolio_risk.get('requested_target_contracts'), digits=2)} | 最终目标仓位：{_format_summary_number(portfolio_risk.get('final_target_contracts') if portfolio_risk.get('final_target_contracts') is not None else plan.get('target_contracts'), digits=2)}",
                f"缩放系数：{_format_fraction_percent(portfolio_risk.get('applied_scale'))}",
                f"杠杆：{_client_bool_label(checks.get('leverage_match'), true_label='已对齐', false_label='未对齐', unknown_label='未知')} | 仓位：{_client_bool_label(checks.get('size_match'), true_label='已对齐', false_label='未对齐', unknown_label='未知')} | 止损：{_client_bool_label(checks.get('protective_stop_ready'), true_label='已就绪', false_label='未就绪', unknown_label='未知')}",
                f"组合风控：{_client_portfolio_risk_text(state)}",
            ]
            cards.append({"title": str(symbol), "lines": lines})
        return {
            "mode": mode,
            "title": "组合标的状态",
            "note": "逐个标的展示持仓、目标仓位、route 与 portfolio risk 决策。",
            "cards": cards,
            "empty_text": "组合模式下暂无标的状态",
        }

    position = reconcile.get("position") if isinstance(reconcile.get("position"), dict) else {}
    signal = reconcile.get("signal") if isinstance(reconcile.get("signal"), dict) else {}
    plan = reconcile.get("plan") if isinstance(reconcile.get("plan"), dict) else {}
    return {
        "mode": mode,
        "title": "当前标的状态",
        "note": "展示当前标的的持仓方向、策略目标方向和目标仓位。",
        "cards": [
            {
                "title": str(reconcile.get("instrument") or "--"),
                "lines": [
                    f"当前方向：{client_side_label(position.get('side'))} | 策略方向：{client_side_label(signal.get('desired_side'))}",
                    f"当前仓位：{_format_contracts(position.get('contracts'))} | 目标仓位：{_format_contracts(plan.get('target_contracts'))}",
                    f"计划动作：{str(plan.get('action') or '无')}",
                    f"执行原因：{_client_reason_text(plan.get('reason') or '未提供原因')}",
                ],
            }
        ],
        "empty_text": "当前没有标的状态",
    }


def build_runtime_dashboard_summary(*, preflight: dict[str, Any]) -> dict[str, Any]:
    demo = _json_mapping(preflight.get("demo_trading"))
    alerts = _json_mapping(preflight.get("alerts"))
    okx = _json_mapping(preflight.get("okx_connectivity"))
    execution_loop = _json_mapping(preflight.get("execution_loop"))
    loop_summary = _runtime_dashboard_loop_summary(execution_loop.get("latest_heartbeat"))

    demo_reasons = _translate_demo_trading_reasons(demo.get("reasons"))
    okx_notes = [
        str(item).strip()
        for item in (okx.get("notes") if isinstance(okx.get("notes"), list) else [])
        if str(item).strip()
    ]
    channel_entries = alerts.get("channels") if isinstance(alerts.get("channels"), dict) else {}
    ready_channels = [
        str(name)
        for name, payload in channel_entries.items()
        if isinstance(payload, dict) and payload.get("ready")
    ]
    enabled_channels = [
        str(name)
        for name, payload in channel_entries.items()
        if isinstance(payload, dict) and payload.get("enabled")
    ]

    demo_mode_value = str(demo.get("mode") or "--")
    if demo.get("ready"):
        demo_mode_note = "当前配置允许向 OKX Demo 自动提交订单。"
    else:
        demo_mode_note = okx_notes[-1] if okx_notes else demo_reasons[0] if demo_reasons else "当前运行仍处于安全规划模式。"

    alerts_value = ", ".join(ready_channels) if ready_channels else "disabled"
    alerts_label = (
        ", ".join(ready_channels)
        if ready_channels
        else "未就绪"
        if enabled_channels
        else ui_code_label("disabled")
    )
    alerts_note = (
        f"已就绪：{', '.join(ready_channels)}"
        if ready_channels
        else f"待补齐：{', '.join(enabled_channels)}"
        if enabled_channels
        else "当前未配置告警通道。"
    )

    loop_status = str(loop_summary.get("status") or "missing")
    demo_base_label = (
        "submit ready"
        if demo.get("ready")
        else "loop error"
        if loop_status == "error"
        else str(demo.get("mode") or "plan_only")
    )
    loop_status_label = str(loop_summary.get("status_label") or ui_code_label(loop_status))
    loop_mode = str(loop_summary.get("mode") or "single")
    demo_status_label = f"portfolio {demo_base_label}" if loop_mode == "portfolio" else demo_base_label
    demo_status_ok = bool(demo.get("ready")) or loop_status not in {"error", "missing"}

    return {
        "demo_mode": {
            "value": demo_mode_value,
            "label": ui_code_label(demo_mode_value),
            "note": demo_mode_note,
        },
        "alerts": {
            "value": alerts_value,
            "label": alerts_label,
            "note": alerts_note,
        },
        "loop": {
            "value": loop_status,
            "label": loop_status_label,
            "note": str(loop_summary.get("card_note") or "当前还没有演示执行循环心跳"),
            "mode": loop_mode,
        },
        "status": {
            "label": demo_status_label,
            "display_label": runtime_dashboard_status_label(demo_status_label),
            "ok": demo_status_ok,
            "note": str(loop_summary.get("status_note") or "当前还没有演示执行循环心跳"),
        },
    }


def ui_code_label(value: Any) -> str:
    mapping = {
        "submit_ready": "可提交",
        "submit_blocked": "已阻塞",
        "plan_only": "仅演练",
        "unknown": "未知",
        "single": "单标的",
        "portfolio": "组合",
        "backtest": "回测",
        "report": "生成报表",
        "sweep": "参数扫描",
        "research": "研究",
        "submitted": "已提交",
        "duplicate": "已跳过重复计划",
        "idle": "无动作",
        "warning": "警告",
        "error": "错误",
        "ok": "正常",
        "stale": "过期",
        "queued": "排队中",
        "running": "运行中",
        "completed": "已完成",
        "failed": "失败",
        "missing": "缺失",
        "sent": "已发送",
        "skipped": "已跳过",
        "disabled": "未启用",
    }
    raw = str(value or "").strip()
    if not raw:
        return "--"
    return mapping.get(raw, raw)


def runtime_dashboard_status_label(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "--"
    if raw.startswith("portfolio "):
        return f"组合 | {runtime_dashboard_status_label(raw.split(' ', 1)[1])}"

    normalized = raw.replace(" ", "_")
    if normalized == "loop_error":
        return "循环错误"
    return ui_code_label(normalized)


def _client_demo_mode_label(mode: Any) -> str:
    return ui_code_label(mode)


def _client_snapshot_source_label(source: Any) -> str:
    mapping = {
        "live_okx": "OKX 实时",
        "cached_local_state": "本地缓存",
    }
    return mapping.get(str(source or "").strip(), str(source or "--"))


def _invoke_demo_loader(loader, *args, session_factory, project_root: Path):
    signature = inspect.signature(loader)
    kwargs: dict[str, Any] = {}
    if "session_factory" in signature.parameters:
        kwargs["session_factory"] = session_factory
    if "project_root" in signature.parameters:
        kwargs["project_root"] = project_root
    return loader(*args, **kwargs)


def _normalize_recovered_error(*, last_error: dict[str, Any], latest_demo_heartbeat: ServiceHeartbeat) -> dict[str, Any] | None:
    error_timestamp_raw = last_error.get("timestamp")
    if not error_timestamp_raw:
        return None
    try:
        error_timestamp = datetime.fromisoformat(str(error_timestamp_raw).replace("Z", "+00:00"))
    except ValueError:
        return None

    heartbeat_created_at = latest_demo_heartbeat.created_at
    if heartbeat_created_at is None:
        return None
    if heartbeat_created_at.tzinfo is None:
        heartbeat_created_at = heartbeat_created_at.replace(tzinfo=timezone.utc)
    if error_timestamp.tzinfo is None:
        error_timestamp = error_timestamp.replace(tzinfo=timezone.utc)
    if heartbeat_created_at < error_timestamp:
        return None
    return last_error


def _load_executor_state_path_info(
    path: Path,
    *,
    mode: str | None = None,
    legacy_fallback_used: bool = False,
) -> dict[str, Any]:
    if not path.exists():
        return {
            "status": "missing",
            "status_label": ui_code_label("missing"),
            "path": str(path),
            "legacy_fallback_used": legacy_fallback_used,
            "mode": mode,
            "payload": None,
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning(
            "executor state invalid_json mode=%s path=%s legacy_fallback=%s",
            mode or "unknown",
            path,
            legacy_fallback_used,
        )
        return {
            "status": "invalid_json",
            "path": str(path),
            "legacy_fallback_used": legacy_fallback_used,
            "mode": mode,
            "payload": None,
        }
    if not isinstance(payload, dict):
        payload = {}
    if legacy_fallback_used:
        LOGGER.info("executor state legacy fallback mode=%s path=%s", mode or "unknown", path)
    return {
        "status": "ok",
        "path": str(path),
        "legacy_fallback_used": legacy_fallback_used,
        "mode": mode,
        "payload": payload,
    }


def _build_okx_connectivity_payload(
    *,
    config: AppConfig,
    latest_demo_heartbeat: ServiceHeartbeat | None,
    resolve_proxy_egress_ip_fn: Callable[[str | None], str | None],
) -> dict[str, Any]:
    heartbeat_details = (
        normalize_demo_heartbeat_contract(latest_demo_heartbeat.details, status=latest_demo_heartbeat.status)
        if latest_demo_heartbeat is not None
        else {}
    )
    latest_auth_error = heartbeat_details.get("error")
    egress_ip = resolve_proxy_egress_ip_fn(config.okx.proxy_url)
    notes: list[str] = []
    if config.okx.proxy_url:
        notes.append(f"OKX private API currently uses proxy {config.okx.proxy_url}.")
    if latest_auth_error:
        notes.append("The latest OKX private API attempt failed.")
        if "401 Unauthorized" in str(latest_auth_error):
            notes.append("OKX returned 401 Unauthorized for the current credentials.")
            if egress_ip:
                notes.append(
                    f"If the API key is IP-restricted, whitelist the current proxy egress IP: {egress_ip}."
                )
    elif config.okx.api_key and config.okx.secret_key and config.okx.passphrase:
        notes.append("Credentials are present, but no recent private-auth error is recorded.")

    return {
        "profile": config.okx.profile,
        "config_file": str(config.okx.config_file) if config.okx.config_file else None,
        "use_demo": bool(config.okx.use_demo),
        "proxy_url": config.okx.proxy_url,
        "egress_ip": egress_ip,
        "latest_auth_error": latest_auth_error,
        "notes": notes,
    }


def resolve_proxy_egress_ip(proxy_url: str | None) -> str | None:
    if not proxy_url:
        return None
    now = datetime.now(timezone.utc).timestamp()
    cached = PROXY_EGRESS_IP_CACHE.get(proxy_url)
    if cached and cached[0] > now:
        return cached[1]

    egress_ip: str | None = None
    try:
        with httpx.Client(proxy=proxy_url, timeout=8.0) as client:
            response = client.get("https://api.ipify.org")
            response.raise_for_status()
        egress_ip = response.text.strip() or None
    except Exception as exc:
        LOGGER.warning("proxy egress ip lookup failed proxy=%s error=%s:%s", proxy_url, type(exc).__name__, exc)

    PROXY_EGRESS_IP_CACHE[proxy_url] = (now + PROXY_EGRESS_IP_TTL_SECONDS, egress_ip)
    return egress_ip


def serialize_service_heartbeat(
    heartbeat: ServiceHeartbeat,
    *,
    include_status_label: bool = False,
) -> dict[str, Any]:
    payload = {
        "id": heartbeat.id,
        "service_name": heartbeat.service_name,
        "status": heartbeat.status,
        "details": normalize_demo_heartbeat_details(heartbeat.details, status=heartbeat.status),
        "created_at": serialize_utc_datetime(heartbeat.created_at),
    }
    if include_status_label:
        payload["status_label"] = ui_code_label(heartbeat.status)
    return payload


def serialize_alert_event(alert: AlertEvent) -> dict[str, Any]:
    return {
        "id": alert.id,
        "event_key": alert.event_key,
        "channel": alert.channel,
        "level": alert.level,
        "title": alert.title,
        "message": alert.message,
        "status": alert.status,
        "status_label": ui_code_label(alert.status),
        "delivered_at": serialize_utc_datetime(alert.delivered_at),
        "created_at": serialize_utc_datetime(alert.created_at),
    }


def _build_cached_portfolio_reconcile(
    *,
    config: AppConfig,
    preflight: dict[str, Any],
    live_error: str,
    symbols: list[str],
) -> dict[str, Any]:
    execution_loop = preflight.get("execution_loop") or {}
    latest_heartbeat = execution_loop.get("latest_heartbeat") or {}
    heartbeat_details = normalize_demo_heartbeat_contract(latest_heartbeat.get("details"), status=latest_heartbeat.get("status"))
    executor_state = execution_loop.get("executor_state") or {}
    heartbeat_symbol_states = heartbeat_details.get("symbol_states") if isinstance(heartbeat_details, dict) else {}
    heartbeat_symbol_states = heartbeat_symbol_states if isinstance(heartbeat_symbol_states, dict) else {}
    executor_symbol_states = executor_state.get("symbols") if isinstance(executor_state, dict) else {}
    executor_symbol_states = executor_symbol_states if isinstance(executor_symbol_states, dict) else {}

    symbol_payloads: dict[str, Any] = {}
    warnings = [
        "OKX 实时行情暂时不可用，客户端已回退到最近一次本地缓存的组合状态。",
        live_error,
    ]
    size_match_count = 0

    for symbol in symbols:
        cached_symbol_state = executor_symbol_states.get(symbol)
        cached_symbol_state = cached_symbol_state if isinstance(cached_symbol_state, dict) else {}
        heartbeat_symbol_state = heartbeat_symbol_states.get(symbol)
        heartbeat_symbol_state = heartbeat_symbol_state if isinstance(heartbeat_symbol_state, dict) else {}
        heartbeat_plan = heartbeat_symbol_state.get("plan") if isinstance(heartbeat_symbol_state.get("plan"), dict) else {}
        heartbeat_signal = heartbeat_symbol_state.get("signal") if isinstance(heartbeat_symbol_state.get("signal"), dict) else {}
        heartbeat_position = heartbeat_symbol_state.get("position") if isinstance(heartbeat_symbol_state.get("position"), dict) else {}

        plan = dict(cached_symbol_state.get("last_plan") or {})
        signal = dict(cached_symbol_state.get("last_signal") or {})
        if not plan:
            plan = {
                "action": heartbeat_plan.get("action") or "cached",
                "reason": "实时 OKX 行情不可用，已回退到最近一次本地执行器状态。",
                "desired_side": _coerce_int(heartbeat_plan.get("desired_side")),
                "current_side": _coerce_int(heartbeat_plan.get("current_side")),
                "current_contracts": _coerce_float(heartbeat_plan.get("current_contracts")),
                "target_contracts": _coerce_float(heartbeat_plan.get("target_contracts")),
                "latest_price": _coerce_float(heartbeat_plan.get("latest_price")),
                "signal_time": heartbeat_plan.get("signal_time") or heartbeat_signal.get("signal_time"),
                "effective_time": heartbeat_plan.get("effective_time") or heartbeat_signal.get("effective_time"),
                "position_mode": heartbeat_plan.get("position_mode") or heartbeat_position.get("position_mode") or config.trading.position_mode,
                "instructions": [],
                "warnings": [],
            }
        if not signal:
            signal = _normalize_signal_payload(
                heartbeat_signal,
                signal_time=heartbeat_signal.get("signal_time"),
                effective_time=heartbeat_signal.get("effective_time"),
                latest_price=_coerce_float(heartbeat_signal.get("latest_price")),
                desired_side=_coerce_int(heartbeat_signal.get("desired_side")),
                ready=False,
            )
        else:
            signal = _normalize_signal_payload(signal)

        current_contracts = _coerce_float(plan.get("current_contracts"))
        target_contracts = _coerce_float(plan.get("target_contracts"))
        size_match = None
        if current_contracts is not None and target_contracts is not None:
            tolerance = max(config.instrument.lot_size, 1e-9)
            size_match = abs(current_contracts - target_contracts) <= tolerance
            if size_match:
                size_match_count += 1

        symbol_payloads[symbol] = {
            "instrument": symbol,
            "position": {
                "side": _coerce_int(plan.get("current_side")),
                "contracts": current_contracts,
                "position_mode": plan.get("position_mode"),
            },
            "signal": signal,
            "plan": plan,
            "warnings": [f"[缓存回退] {item}" for item in plan.get("warnings", []) if item],
            "checks": {
                "trade_permission": None,
                "position_mode_match": None,
                "leverage_match": None,
                "size_match": size_match,
                "protective_stop_ready": None,
                "open_orders_idle": None,
                "executor_state_present": bool(cached_symbol_state),
            },
            "exchange": {
                "data_source": "cached_local_state",
                "pending_orders": {"count": None},
                "pending_algo_orders": {"count": None},
                "leverage": {"values": [], "match": None},
                "protection_stop": {"ready": None},
                "error": live_error,
            },
        }

    active_positions = sum(
        1
        for payload in symbol_payloads.values()
        if _coerce_int((payload.get("position") or {}).get("side")) not in {None, 0}
        and (_coerce_float((payload.get("position") or {}).get("contracts")) or 0.0) > 0
    )
    actionable_symbols = sum(
        1
        for payload in symbol_payloads.values()
        if (_coerce_float((payload.get("plan") or {}).get("target_contracts")) or 0.0) > 0
    )

    return {
        "mode": "portfolio",
        "symbols": symbols,
        "account": {
            "source": "cached_local_state",
            "can_trade": (preflight.get("demo_trading") or {}).get("ready"),
        },
        "summary": {
            "symbol_count": len(symbols),
            "ready_symbol_count": 0,
            "actionable_symbol_count": actionable_symbols,
            "active_position_symbol_count": active_positions,
            "allocation_mode": "priority_risk_budget",
            "leverage_ready_symbol_count": 0,
            "protective_stop_ready_symbol_count": 0,
            "size_match_symbol_count": size_match_count,
        },
        "warnings": warnings,
        "symbol_states": symbol_payloads,
        "snapshot_source": "cached_local_state",
        "live_error": live_error,
    }


def _build_cached_reconcile(
    *,
    config: AppConfig,
    preflight: dict[str, Any],
    live_error: str,
) -> dict[str, Any]:
    execution_loop = preflight.get("execution_loop") or {}
    latest_heartbeat = execution_loop.get("latest_heartbeat") or {}
    heartbeat_details = normalize_demo_heartbeat_contract(latest_heartbeat.get("details"), status=latest_heartbeat.get("status"))
    executor_state = execution_loop.get("executor_state") or {}
    heartbeat_plan = heartbeat_details.get("plan") if isinstance(heartbeat_details.get("plan"), dict) else {}
    heartbeat_signal = heartbeat_details.get("signal") if isinstance(heartbeat_details.get("signal"), dict) else {}
    heartbeat_position = heartbeat_details.get("position") if isinstance(heartbeat_details.get("position"), dict) else {}
    heartbeat_account = heartbeat_details.get("account") if isinstance(heartbeat_details.get("account"), dict) else {}

    plan = dict(executor_state.get("last_plan") or {})
    signal = dict(executor_state.get("last_signal") or {})
    current_contracts = _coerce_float(heartbeat_position.get("contracts"))
    if current_contracts is None:
        current_contracts = _coerce_float(plan.get("current_contracts"))
    target_contracts = _coerce_float(plan.get("target_contracts"))
    if target_contracts is None:
        target_contracts = _coerce_float(heartbeat_plan.get("target_contracts"))
    current_side = _coerce_int(heartbeat_position.get("side"))
    if current_side is None:
        current_side = _coerce_int(plan.get("current_side"))
    desired_side = _coerce_int(heartbeat_signal.get("desired_side"))
    if desired_side is None:
        desired_side = _coerce_int(signal.get("desired_side"))
    if desired_side is None:
        desired_side = _coerce_int(plan.get("desired_side"))
    latest_price = _coerce_float(heartbeat_signal.get("latest_price"))
    if latest_price is None:
        latest_price = _coerce_float(heartbeat_plan.get("latest_price"))
    if latest_price is None:
        latest_price = _coerce_float(signal.get("latest_price"))
    if latest_price is None:
        latest_price = _coerce_float(plan.get("latest_price"))

    if not signal:
        signal = _normalize_signal_payload(
            heartbeat_signal,
            signal_time=heartbeat_signal.get("signal_time"),
            effective_time=heartbeat_signal.get("effective_time"),
            latest_price=latest_price,
            desired_side=desired_side,
            ready=False,
        )
    else:
        signal = _normalize_signal_payload(signal)

    if not plan:
        plan = {
            "action": heartbeat_plan.get("action") or "cached",
            "reason": "实时 OKX 行情不可用，已回退到最近一次本地执行器状态。",
            "desired_side": desired_side,
            "current_side": current_side,
            "current_contracts": current_contracts,
            "target_contracts": target_contracts,
            "latest_price": latest_price,
            "signal_time": heartbeat_plan.get("signal_time") or signal.get("signal_time"),
            "effective_time": heartbeat_plan.get("effective_time") or signal.get("effective_time"),
            "position_mode": heartbeat_plan.get("position_mode") or heartbeat_position.get("position_mode") or config.trading.position_mode,
            "instructions": [],
            "warnings": [],
        }

    size_match = None
    if current_contracts is not None and target_contracts is not None:
        tolerance = max(config.instrument.lot_size, 1e-9)
        size_match = abs(current_contracts - target_contracts) <= tolerance

    warnings = [
        "OKX 实时行情暂时不可用，客户端已回退到最近一次本地缓存状态。",
        live_error,
    ]
    cached_warnings = plan.get("warnings")
    if isinstance(cached_warnings, list):
        warnings.extend(str(item) for item in cached_warnings if item)

    return {
        "instrument": config.instrument.symbol,
        "okx_use_demo": bool(config.okx.use_demo),
        "account": {
            "source": "cached_local_state",
            "can_trade": (preflight.get("demo_trading") or {}).get("ready"),
            "total_equity": _coerce_float(heartbeat_account.get("total_equity")),
            "available_equity": _coerce_float(heartbeat_account.get("available_equity")),
            "currency": heartbeat_account.get("currency"),
        },
        "position": {
            "side": current_side,
            "contracts": current_contracts,
            "position_mode": plan.get("position_mode"),
        },
        "signal": signal,
        "plan": plan,
        "warnings": warnings,
        "checks": {
            "trade_permission": None,
            "position_mode_match": None,
            "leverage_match": None,
            "size_match": size_match,
            "protective_stop_ready": None,
            "open_orders_idle": None,
            "executor_state_present": bool(executor_state),
        },
        "exchange": {
            "data_source": "cached_local_state",
            "pending_orders": {"count": None},
            "pending_algo_orders": {"count": None},
            "leverage": {"values": [], "match": None},
            "protection_stop": {"ready": None},
            "error": live_error,
        },
        "snapshot_source": "cached_local_state",
        "live_error": live_error,
    }


def _portfolio_autotrade_details(reconcile: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    symbol_states = reconcile.get("symbol_states") or {}
    symbol_states = symbol_states if isinstance(symbol_states, dict) else {}
    blocking_reasons: list[str] = []
    actionable_symbols: list[str] = []
    active_symbols: list[str] = []

    for symbol in sorted(symbol_states):
        payload = symbol_states[symbol]
        checks = payload.get("checks") or {}
        plan = payload.get("plan") or {}
        position = payload.get("position") or {}

        if _coerce_int(position.get("side")) not in {None, 0} and (_coerce_float(position.get("contracts")) or 0.0) > 0:
            active_symbols.append(symbol)

        symbol_blockers: list[str] = []
        if checks.get("trade_permission") is False:
            symbol_blockers.append("账户无交易权限")
        if checks.get("position_mode_match") is False:
            symbol_blockers.append("持仓模式不匹配")
        if checks.get("leverage_match") is False:
            symbol_blockers.append("杠杆未对齐")
        if checks.get("open_orders_idle") is False:
            symbol_blockers.append("存在未完成普通挂单")
        if checks.get("protective_stop_ready") is False:
            symbol_blockers.append("缺少保护止损")

        if symbol_blockers:
            blocking_reasons.append(f"{symbol}：{'；'.join(symbol_blockers)}")
            continue

        if _plan_is_actionable(plan):
            actionable_symbols.append(symbol)

    return blocking_reasons, actionable_symbols, active_symbols

def _single_autotrade_details(reconcile: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    checks = reconcile.get("checks") or {}
    plan = reconcile.get("plan") or {}
    position = reconcile.get("position") or {}
    symbol = str(reconcile.get("instrument") or "--")
    blocking_reasons: list[str] = []
    active_symbols: list[str] = []
    actionable_symbols: list[str] = []

    if _coerce_int(position.get("side")) not in {None, 0} and (_coerce_float(position.get("contracts")) or 0.0) > 0:
        active_symbols.append(symbol)

    if checks.get("trade_permission") is False:
        blocking_reasons.append("账户无交易权限")
    if checks.get("position_mode_match") is False:
        blocking_reasons.append("持仓模式不匹配")
    if checks.get("leverage_match") is False:
        blocking_reasons.append("杠杆未对齐")
    if checks.get("open_orders_idle") is False:
        blocking_reasons.append("存在未完成普通挂单")
    if checks.get("protective_stop_ready") is False:
        blocking_reasons.append("缺少保护止损")

    if not blocking_reasons and _plan_is_actionable(plan):
        actionable_symbols.append(symbol)

    return blocking_reasons, actionable_symbols, active_symbols

def _collect_idle_reasons(reconcile: dict[str, Any], *, mode: str) -> list[str]:
    if mode == "portfolio":
        symbol_states = reconcile.get("symbol_states") or {}
        symbol_states = symbol_states if isinstance(symbol_states, dict) else {}
        reasons: list[str] = []
        for symbol in sorted(symbol_states):
            payload = symbol_states[symbol]
            plan = payload.get("plan") or {}
            reason = str(plan.get("reason") or "").strip()
            if reason:
                reasons.append(f"{symbol}：{reason}")
        return reasons[:4]

    plan = reconcile.get("plan") or {}
    reason = str(plan.get("reason") or "").strip()
    return [reason] if reason else []

def _warning_summary_entries(*, source: str, messages: Any) -> list[dict[str, str]]:
    if not isinstance(messages, list):
        return []
    entries: list[dict[str, str]] = []
    for item in messages:
        text = str(item or "").strip()
        if not text:
            continue
        entries.append({"source": source, "text": text})
    return entries


def _portfolio_check_counts(reconcile: dict[str, Any]) -> dict[str, int]:
    symbol_states = reconcile.get("symbol_states") if isinstance(reconcile.get("symbol_states"), dict) else {}
    total = 0
    active = 0
    leverage_ready = 0
    size_ready = 0
    stop_ready = 0
    for payload in symbol_states.values():
        state = payload if isinstance(payload, dict) else {}
        total += 1
        checks = state.get("checks") if isinstance(state.get("checks"), dict) else {}
        position = state.get("position") if isinstance(state.get("position"), dict) else {}
        side = _coerce_int(position.get("side"))
        contracts = _coerce_float(position.get("contracts")) or 0.0
        if side not in {None, 0} and contracts > 0:
            active += 1
        if checks.get("leverage_match") is True:
            leverage_ready += 1
        if checks.get("size_match") is True:
            size_ready += 1
        if checks.get("protective_stop_ready") is True:
            stop_ready += 1
    return {
        "total": total,
        "active": active,
        "leverage_ready": leverage_ready,
        "size_ready": size_ready,
        "stop_ready": stop_ready,
    }


def _client_single_check_card(
    *,
    present: bool,
    ready: Any,
    ready_value: str,
    blocked_value: str,
    ready_note: str,
    blocked_note: str,
    blocked_level: str,
) -> dict[str, str]:
    if not present:
        return {
            "value": "未知",
            "level": "warn",
            "note": blocked_note,
        }
    if ready is True:
        return {
            "value": ready_value,
            "level": "ok",
            "note": ready_note,
        }
    return {
        "value": blocked_value,
        "level": blocked_level,
        "note": blocked_note,
    }


def _client_bool_label(value: Any, *, true_label: str, false_label: str, unknown_label: str = "--") -> str:
    if value is True:
        return true_label
    if value is False:
        return false_label
    return unknown_label


def client_side_label(value: Any) -> str:
    parsed = _coerce_float(value)
    if parsed is None:
        return "--"
    if parsed > 0:
        return "做多"
    if parsed < 0:
        return "做空"
    return "空仓"


def _client_route_ready_label(value: Any) -> str:
    if value is True:
        return "已就绪"
    if value is False:
        return "已阻塞"
    return "--"


def client_action_label(value: Any) -> str:
    mapping = {
        "open": "开仓",
        "close": "平仓",
        "flip": "反手",
        "hold": "持有",
        "rebalance": "再平衡",
        "trim": "减仓",
        "increase": "加仓",
        "decrease": "降仓",
        "cached": "缓存回退",
        "n/a": "无",
    }
    raw = str(value or "").strip()
    if not raw:
        return "无"
    return mapping.get(raw, raw)


def _client_reason_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "未提供原因"
    translated = _translate_demo_trading_reasons([text])
    if translated:
        return translated[0]
    return text


def _client_portfolio_risk_text(payload: dict[str, Any]) -> str:
    portfolio_risk = payload.get("portfolio_risk") if isinstance(payload.get("portfolio_risk"), dict) else {}
    router = payload.get("router_decision") if isinstance(payload.get("router_decision"), dict) else {}
    route_meta = router.get("route") if isinstance(router.get("route"), dict) else {}
    display = router.get("display") if isinstance(router.get("display"), dict) else {}
    reasons = [_client_reason_text(item) for item in (portfolio_risk.get("reasons") if isinstance(portfolio_risk.get("reasons"), list) else []) if str(item).strip()]
    if router.get("ready") is False:
        route_reasons = [_client_reason_text(item) for item in (router.get("reasons") if isinstance(router.get("reasons"), list) else []) if str(item).strip()]
        route_label = str(
            display.get("route_label")
            or route_meta.get("label")
            or router.get("route_key")
            or router.get("regime")
            or "--"
        )
        reasons.append(f"路由阻塞：{route_label} | {' | '.join(route_reasons) if route_reasons else '路由未就绪'}")
    return " | ".join(reasons) if reasons else "--"


def _format_contracts(value: Any) -> str:
    return f"{_format_summary_number(value, digits=2)} 张"


def _format_summary_amount(value: Any, *, currency: str) -> str:
    return f"{_format_summary_number(value, digits=2)} {currency}"


def _format_summary_number(value: Any, *, digits: int) -> str:
    parsed = _coerce_float(value)
    if parsed is None:
        return "--"
    if digits == 0:
        return str(int(round(parsed)))
    return f"{parsed:.{digits}f}"


def _format_summary_percent(value: Any) -> str:
    parsed = _coerce_float(value)
    if parsed is None:
        return "--"
    return f"{parsed:.2f}%"


def _format_fraction_percent(value: Any) -> str:
    parsed = _coerce_float(value)
    if parsed is None:
        return "--"
    return f"{parsed * 100:.2f}%"


def _runtime_dashboard_loop_summary(latest_heartbeat: Any) -> dict[str, Any]:
    heartbeat = _json_mapping(latest_heartbeat)
    if not heartbeat:
        return {
            "status": "missing",
            "mode": "single",
            "card_note": "当前还没有演示执行循环心跳",
            "status_note": "当前还没有演示执行循环心跳",
        }

    status = str(heartbeat.get("status") or "missing")
    status_label = str(heartbeat.get("status_label") or ui_code_label(status))
    details = normalize_demo_heartbeat_contract(heartbeat.get("details"), status=status)
    mode = str(details.get("mode") or "").strip().lower()
    summary = details.get("summary") if isinstance(details.get("summary"), dict) else {}
    cycle = summary.get("cycle")
    cycle_label = cycle if cycle not in {None, ""} else "--"

    if mode == "portfolio":
        symbol_count = summary.get("symbol_count")
        actionable_count = summary.get("actionable_symbol_count")
        active_count = summary.get("active_position_symbol_count")
        return {
            "status": status,
            "status_label": status_label,
            "mode": "portfolio",
            "card_note": (
                f"循环 {cycle_label} | 组合 {symbol_count if symbol_count not in {None, ''} else '--'} 标的"
                f" | 可执行 {actionable_count if actionable_count not in {None, ''} else '--'}"
                f" | 持仓中 {active_count if active_count not in {None, ''} else '--'}"
            ),
            "status_note": "组合模式执行循环心跳已连接。",
        }

    plan = details.get("plan") if isinstance(details.get("plan"), dict) else {}
    action = str(plan.get("action") or "n/a")
    return {
        "status": status,
        "status_label": status_label,
        "mode": "single",
        "card_note": f"循环 {cycle_label} | {client_action_label(action)}",
        "status_note": "单标的执行循环心跳已连接。",
    }


def _demo_visual_heartbeat_point(row: ServiceHeartbeat) -> dict[str, Any]:
    details = normalize_demo_heartbeat_contract(row.details, status=row.status)
    mode = str(details.get("mode") or "single")
    summary = details.get("summary") if isinstance(details.get("summary"), dict) else {}
    account = details.get("account") if isinstance(details.get("account"), dict) else {}
    position = details.get("position") if isinstance(details.get("position"), dict) else {}
    signal = details.get("signal") if isinstance(details.get("signal"), dict) else {}
    plan = details.get("plan") if isinstance(details.get("plan"), dict) else {}

    target_contracts = _coerce_float(plan.get("target_contracts"))
    current_contracts = _coerce_float(position.get("contracts"))
    action = str(plan.get("action") or details.get("action") or "--")
    desired_side = _coerce_int(signal.get("desired_side"))
    current_side = _coerce_int(position.get("side"))
    submitted = bool(summary.get("submitted"))
    response_count = _coerce_int(summary.get("response_count")) or 0
    warning_count = _coerce_int(summary.get("warning_count")) or 0
    latest_price = _coerce_float(signal.get("latest_price"))
    total_equity = _coerce_float(account.get("total_equity"))
    available_equity = _coerce_float(account.get("available_equity"))
    signal_time = signal.get("signal_time")
    effective_time = signal.get("effective_time")

    if mode == "portfolio":
        target_contracts = _coerce_float(summary.get("actionable_symbol_count"))
        current_contracts = _coerce_float(summary.get("active_position_symbol_count"))
    point = {
        "id": row.id,
        "mode": mode,
        "cycle": _coerce_int(summary.get("cycle")),
        "status": row.status,
        "status_label": autotrade_status_label(row.status),
        "action": action,
        "desired_side": desired_side,
        "desired_side_label": client_side_label(desired_side),
        "current_side": current_side,
        "current_side_label": client_side_label(current_side),
        "target_contracts": target_contracts,
        "current_contracts": current_contracts,
        "submitted": submitted,
        "response_count": response_count,
        "warning_count": warning_count,
        "latest_price": latest_price,
        "total_equity": total_equity,
        "available_equity": available_equity,
        "signal_time": signal_time,
        "effective_time": effective_time,
        "created_at": serialize_utc_datetime(row.created_at),
    }
    if mode == "portfolio":
        point["symbol_count"] = _coerce_int(summary.get("symbol_count"))
        point["submitted_symbol_count"] = _coerce_int(summary.get("submitted_symbol_count"))
        point["actionable_symbol_count"] = _coerce_int(summary.get("actionable_symbol_count"))
        point["active_position_symbol_count"] = _coerce_int(summary.get("active_position_symbol_count"))
        point["submitted_symbols"] = details.get("submitted_symbols") if isinstance(details.get("submitted_symbols"), list) else []
        point["symbol_states"] = details.get("symbol_states") if isinstance(details.get("symbol_states"), dict) else {}
        if point["action"] == "--":
            point["action"] = f"{point['submitted_symbol_count'] or 0}/{point['symbol_count'] or 0} submitted"
    point["action_label"] = client_action_label(point["action"])
    if target_contracts is not None and current_contracts is not None:
        point["contract_gap"] = round(current_contracts - target_contracts, 4)
    else:
        point["contract_gap"] = None
    return point


def _plan_is_actionable(plan: dict[str, Any]) -> bool:
    instructions = plan.get("instructions")
    if isinstance(instructions, list) and instructions:
        return True
    action = str(plan.get("action") or "").strip().lower()
    return action in {"open", "close", "flip", "rebalance", "trim", "increase", "decrease"}

def _translate_demo_trading_reasons(raw_reasons: Any) -> list[str]:
    mapping = {
        "okx.use_demo=false": "当前配置不是 OKX Demo 模式。",
        "trading.allow_order_placement=false": "当前运行未开启自动下单。",
        "missing OKX_API_KEY": "缺少 OKX_API_KEY。",
        "missing OKX_SECRET_KEY": "缺少 OKX_SECRET_KEY。",
        "missing OKX_PASSPHRASE": "缺少 OKX_PASSPHRASE。",
        "route not ready": "路由未就绪。",
        "路由未就绪": "路由未就绪。",
    }
    reasons: list[str] = []
    if not isinstance(raw_reasons, list):
        return reasons
    for item in raw_reasons:
        text = str(item or "").strip()
        if not text:
            continue
        if text.startswith("execution approval: "):
            reasons.append("Approved candidate gate 未通过: " + text.replace("execution approval: ", "", 1))
            continue
        if text.endswith(": route decision is missing"):
            symbol = text.split(":", 1)[0].strip()
            reasons.append(f"{symbol}：路由决策缺失。")
            continue
        if text.startswith("live route resolution failed: "):
            reasons.append("实时路由解析失败：" + text.replace("live route resolution failed: ", "", 1))
            continue
        reasons.append(mapping.get(text, text))
    return reasons

def _pick_blocked_hint(blocking_reasons: list[str]) -> str:
    joined = " ".join(blocking_reasons)
    if "杠杆未对齐" in joined:
        return "先执行一次杠杆对齐，再看是否恢复为可提交状态。"
    if "保护止损" in joined:
        return "先补齐保护止损，再继续让系统自动运行。"
    if "普通挂单" in joined:
        return "先清理残留挂单，避免新单被旧状态干扰。"
    if "持仓模式" in joined:
        return "先把 OKX 账户持仓模式改到与系统一致。"
    if "交易权限" in joined:
        return "检查 OKX API 权限，确认当前 key 允许交易。"
    return "先排除上面的阻塞项，恢复后自动下单会继续执行。"

def autotrade_status_label(status: str | None) -> str:
    return ui_code_label(status)


def demo_history_status_label(point: dict[str, Any] | None) -> str:
    if not point:
        return "--"
    label = point.get("status_label")
    if isinstance(label, str) and label.strip():
        return label
    return autotrade_status_label(point.get("status"))

def _coerce_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_executor_symbol_payloads(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    normalized: dict[str, Any] = {}
    for symbol, payload in value.items():
        if not isinstance(payload, dict):
            normalized[str(symbol)] = payload
            continue
        normalized[str(symbol)] = {
            **payload,
            "last_signal": _normalize_signal_payload(payload.get("last_signal")),
        }
    return normalized


def _normalize_signal_payload(
    signal: Any,
    *,
    signal_time: Any = None,
    effective_time: Any = None,
    latest_price: float | None = None,
    desired_side: int | None = None,
    ready: bool | None = None,
) -> dict[str, Any]:
    payload = dict(signal) if isinstance(signal, dict) else {}
    resolved_desired_side = _coerce_int(payload.get("desired_side"))
    if resolved_desired_side is None:
        resolved_desired_side = desired_side if desired_side is not None else 0

    resolved_signal_time = payload.get("signal_time") or signal_time
    resolved_effective_time = payload.get("effective_time") or effective_time
    resolved_latest_price = _coerce_float(payload.get("latest_price"))
    if resolved_latest_price is None:
        resolved_latest_price = latest_price

    alpha_payload = payload.get("alpha_signal")
    alpha_payload = dict(alpha_payload) if isinstance(alpha_payload, dict) else {}
    alpha_score = _coerce_float(alpha_payload.get("score"))
    if alpha_score is None:
        alpha_score = _coerce_float(payload.get("strategy_score"))
    alpha_regime = alpha_payload.get("regime")
    if alpha_regime in {None, ""}:
        alpha_regime = payload.get("regime")
    alpha_strategy_name = alpha_payload.get("strategy_name")
    if alpha_strategy_name in {None, ""}:
        alpha_strategy_name = payload.get("contract_strategy_name")
    alpha_strategy_variant = alpha_payload.get("strategy_variant")
    if alpha_strategy_variant in {None, ""}:
        alpha_strategy_variant = payload.get("contract_strategy_variant")
    alpha_side = _coerce_int(alpha_payload.get("side"))
    if alpha_side is None:
        alpha_side = _coerce_int(payload.get("alpha_side"))
    if alpha_side is None:
        alpha_side = resolved_desired_side

    risk_payload = payload.get("risk_signal")
    risk_payload = dict(risk_payload) if isinstance(risk_payload, dict) else {}
    risk_stop_distance = _coerce_float(risk_payload.get("stop_distance"))
    if risk_stop_distance is None:
        risk_stop_distance = _coerce_float(payload.get("stop_distance"))
    if risk_stop_distance is None:
        risk_stop_distance = 0.0
    risk_stop_price = _coerce_float(risk_payload.get("stop_price"))
    if risk_stop_price is None:
        risk_stop_price = _coerce_float(payload.get("signal_stop_price"))
    if risk_stop_price is None:
        risk_stop_price = _coerce_float(payload.get("stop_price"))
    risk_multiplier = _coerce_float(risk_payload.get("risk_multiplier"))
    if risk_multiplier is None:
        risk_multiplier = _coerce_float(payload.get("strategy_risk_multiplier"))
    if risk_multiplier is None:
        risk_multiplier = 1.0
    risk_multiplier = max(0.0, risk_multiplier)

    resolved_ready = payload.get("ready")
    if resolved_ready is None:
        resolved_ready = bool(ready) if ready is not None else False
    else:
        resolved_ready = bool(resolved_ready)

    payload["signal_time"] = resolved_signal_time
    payload["effective_time"] = resolved_effective_time
    payload["latest_price"] = resolved_latest_price
    payload["desired_side"] = resolved_desired_side
    payload["strategy_score"] = alpha_score
    payload["strategy_risk_multiplier"] = risk_multiplier
    payload["regime"] = alpha_regime
    payload["route_key"] = payload.get("route_key")
    payload["ready"] = resolved_ready
    payload["alpha_signal"] = {
        "side": alpha_side,
        "score": alpha_score,
        "regime": alpha_regime,
        "strategy_name": alpha_strategy_name,
        "strategy_variant": alpha_strategy_variant,
    }
    payload["risk_signal"] = {
        "stop_distance": risk_stop_distance,
        "stop_price": risk_stop_price,
        "risk_multiplier": risk_multiplier,
    }
    return payload
