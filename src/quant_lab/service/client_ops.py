from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import desc, func, select

from quant_lab.config import AppConfig, configured_symbols
from quant_lab.service.database import AlertEvent, ServiceHeartbeat, session_scope
from quant_lab.service.monitor import build_preflight_payload


def build_client_snapshot(config: AppConfig, session_factory, project_root: Path) -> dict[str, Any]:
    from quant_lab.cli import (
        _build_demo_portfolio_payload,
        _build_demo_reconcile_payload,
        _executor_state_path,
        _load_demo_portfolio_state,
        _load_demo_state,
        _load_executor_state,
    )

    symbols = configured_symbols(config)
    preflight = build_preflight_payload(config=config, session_factory=session_factory, project_root=project_root)
    live_error: str | None = None
    snapshot_source = "live_okx"
    portfolio_mode = len(symbols) > 1

    try:
        executor_state = _load_executor_state(_executor_state_path(config))
        if portfolio_mode:
            account, symbol_states = _load_demo_portfolio_state(config, symbols)
            reconcile = _build_demo_portfolio_payload(
                cfg=config,
                account=account,
                symbol_states=symbol_states,
                include_exchange_checks=True,
                executor_state=executor_state,
            )
        else:
            account, position, state = _load_demo_state(config)
            reconcile = _build_demo_reconcile_payload(
                cfg=config,
                account=account,
                position=position,
                signal=state["signal"],
                plan=state["plan"],
                state=state,
                executor_state=executor_state,
            )
    except Exception as exc:
        live_error = f"{type(exc).__name__}: {exc}"
        snapshot_source = "cached_local_state"
        if portfolio_mode:
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

    demo_visuals = build_demo_visuals_payload(
        session_factory=session_factory,
        reconcile=reconcile,
    )
    autotrade_status = _build_autotrade_status(
        preflight=preflight,
        reconcile=reconcile,
        demo_visuals=demo_visuals,
        snapshot_source=snapshot_source,
        live_error=live_error,
    )
    payload = {
        "preflight": preflight,
        "reconcile": reconcile,
        "demo_visuals": demo_visuals,
        "autotrade_status": autotrade_status,
        "snapshot_source": snapshot_source,
    }
    if live_error is not None:
        payload["live_error"] = live_error
    return payload


def _build_autotrade_status(
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
            "latest_loop_status_label": _status_label(latest_heartbeat.get("status")),
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
        "latest_loop_status_label": _status_label(latest_heartbeat.get("status")),
        "latest_event_time": latest_heartbeat.get("created_at") or (demo_visuals.get("summary") or {}).get("last_event_time"),
        "next_hint": next_hint,
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


def _build_cached_portfolio_reconcile(
    *,
    config: AppConfig,
    preflight: dict[str, Any],
    live_error: str,
    symbols: list[str],
) -> dict[str, Any]:
    execution_loop = preflight.get("execution_loop") or {}
    latest_heartbeat = execution_loop.get("latest_heartbeat") or {}
    heartbeat_details = latest_heartbeat.get("details") or {}
    executor_state = execution_loop.get("executor_state") or {}
    heartbeat_symbol_states = heartbeat_details.get("symbol_states") if isinstance(heartbeat_details, dict) else {}
    heartbeat_symbol_states = heartbeat_symbol_states if isinstance(heartbeat_symbol_states, dict) else {}
    executor_symbol_states = executor_state.get("symbols") if isinstance(executor_state, dict) else {}
    executor_symbol_states = executor_symbol_states if isinstance(executor_symbol_states, dict) else {}

    symbol_payloads: dict[str, Any] = {}
    warnings = [
        "OKX real-time market data is temporarily unavailable. The client is showing the latest cached portfolio state.",
        live_error,
    ]
    size_match_count = 0

    for symbol in symbols:
        cached_symbol_state = executor_symbol_states.get(symbol)
        cached_symbol_state = cached_symbol_state if isinstance(cached_symbol_state, dict) else {}
        heartbeat_symbol_state = heartbeat_symbol_states.get(symbol)
        heartbeat_symbol_state = heartbeat_symbol_state if isinstance(heartbeat_symbol_state, dict) else {}

        plan = dict(cached_symbol_state.get("last_plan") or {})
        signal = dict(cached_symbol_state.get("last_signal") or {})
        if not plan:
            plan = {
                "action": heartbeat_symbol_state.get("action") or "cached",
                "reason": "Live OKX market data was unavailable. Falling back to the latest local executor state.",
                "desired_side": _coerce_int(heartbeat_symbol_state.get("desired_side")),
                "current_side": _coerce_int(heartbeat_symbol_state.get("current_side")),
                "current_contracts": _coerce_float(heartbeat_symbol_state.get("current_contracts")),
                "target_contracts": _coerce_float(heartbeat_symbol_state.get("target_contracts")),
                "latest_price": _coerce_float(heartbeat_symbol_state.get("latest_price")),
                "signal_time": heartbeat_symbol_state.get("signal_time"),
                "effective_time": heartbeat_symbol_state.get("effective_time"),
                "position_mode": config.trading.position_mode,
                "instructions": [],
                "warnings": [],
            }
        if not signal:
            signal = {
                "signal_time": heartbeat_symbol_state.get("signal_time"),
                "effective_time": heartbeat_symbol_state.get("effective_time"),
                "latest_price": _coerce_float(heartbeat_symbol_state.get("latest_price")),
                "desired_side": _coerce_int(heartbeat_symbol_state.get("desired_side")),
                "ready": False,
            }

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
            "warnings": [f"[cached] {item}" for item in plan.get("warnings", []) if item],
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
            "allocation_mode": "equal_weight",
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
    heartbeat_details = latest_heartbeat.get("details") or {}
    executor_state = execution_loop.get("executor_state") or {}

    plan = dict(executor_state.get("last_plan") or {})
    signal = dict(executor_state.get("last_signal") or {})
    current_contracts = _coerce_float(heartbeat_details.get("current_contracts"))
    if current_contracts is None:
        current_contracts = _coerce_float(plan.get("current_contracts"))
    target_contracts = _coerce_float(plan.get("target_contracts"))
    current_side = _coerce_int(heartbeat_details.get("current_side"))
    if current_side is None:
        current_side = _coerce_int(plan.get("current_side"))
    desired_side = _coerce_int(heartbeat_details.get("desired_side"))
    if desired_side is None:
        desired_side = _coerce_int(signal.get("desired_side"))
    if desired_side is None:
        desired_side = _coerce_int(plan.get("desired_side"))
    latest_price = _coerce_float(heartbeat_details.get("latest_price"))
    if latest_price is None:
        latest_price = _coerce_float(signal.get("latest_price"))
    if latest_price is None:
        latest_price = _coerce_float(plan.get("latest_price"))

    if not signal:
        signal = {
            "signal_time": heartbeat_details.get("signal_time"),
            "effective_time": heartbeat_details.get("effective_time"),
            "latest_price": latest_price,
            "desired_side": desired_side,
            "ready": False,
        }

    if not plan:
        plan = {
            "action": heartbeat_details.get("action") or "cached",
            "reason": "Live OKX market data was unavailable. Falling back to the latest local executor state.",
            "desired_side": desired_side,
            "current_side": current_side,
            "current_contracts": current_contracts,
            "target_contracts": target_contracts,
            "latest_price": latest_price,
            "signal_time": signal.get("signal_time"),
            "effective_time": signal.get("effective_time"),
            "position_mode": heartbeat_details.get("position_mode"),
            "instructions": [],
            "warnings": [],
        }

    size_match = None
    if current_contracts is not None and target_contracts is not None:
        tolerance = max(config.instrument.lot_size, 1e-9)
        size_match = abs(current_contracts - target_contracts) <= tolerance

    warnings = [
        "OKX real-time market data is temporarily unavailable. The client is showing the latest cached local state.",
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


def build_demo_visuals_payload(
    *,
    session_factory,
    reconcile: dict[str, Any],
    history_limit: int = 120,
    alert_limit: int = 8,
) -> dict[str, Any]:
    with session_scope(session_factory) as session:
        heartbeats = list(
            session.execute(
                select(ServiceHeartbeat)
                .where(ServiceHeartbeat.service_name == "quant-lab-demo-loop")
                .order_by(desc(ServiceHeartbeat.created_at))
                .limit(history_limit)
            ).scalars()
        )
        status_rows = session.execute(
            select(ServiceHeartbeat.status, func.count())
            .where(ServiceHeartbeat.service_name == "quant-lab-demo-loop")
            .group_by(ServiceHeartbeat.status)
        ).all()
        last_submitted_row = session.execute(
            select(ServiceHeartbeat)
            .where(
                ServiceHeartbeat.service_name == "quant-lab-demo-loop",
                ServiceHeartbeat.status == "submitted",
            )
            .order_by(desc(ServiceHeartbeat.created_at))
            .limit(1)
        ).scalar_one_or_none()
        last_error_row = session.execute(
            select(ServiceHeartbeat)
            .where(
                ServiceHeartbeat.service_name == "quant-lab-demo-loop",
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
    chart_points = [_heartbeat_point(row) for row in heartbeats]
    recent_events = list(reversed(chart_points[-12:]))

    status_counts = {str(status): int(count) for status, count in status_rows}
    submitted_count = status_counts.get("submitted", 0)
    duplicate_count = status_counts.get("duplicate", 0)
    warning_count = status_counts.get("warning", 0)
    error_count = status_counts.get("error", 0)
    idle_count = status_counts.get("idle", 0)
    total_cycles = sum(status_counts.values())
    last_point = chart_points[-1] if chart_points else None
    last_submitted = _heartbeat_point(last_submitted_row) if last_submitted_row is not None else None
    last_error = _heartbeat_point(last_error_row) if last_error_row is not None else None
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
            "created_at": _serialize_datetime(item.created_at),
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
                    "current_side_label": _side_label(position.get("side")),
                    "desired_side": _coerce_int((payload.get("signal") or {}).get("desired_side")),
                    "desired_side_label": _side_label((payload.get("signal") or {}).get("desired_side")),
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
                "total_cycles": total_cycles,
                "submitted_count": submitted_count,
                "duplicate_count": duplicate_count,
                "warning_count": warning_count,
                "error_count": error_count,
                "idle_count": idle_count,
                "submission_rate_pct": round((submitted_count / total_cycles) * 100, 2) if total_cycles else 0.0,
                "last_cycle": last_point["cycle"] if last_point else None,
                "last_status": last_point["status"] if last_point else None,
                "last_status_label": _status_label(last_point["status"]) if last_point else "--",
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
                "points": chart_points,
                "latest_target_contracts": _coerce_int(last_point.get("actionable_symbol_count")) if last_point else None,
                "latest_live_contracts": _coerce_int(last_point.get("active_position_symbol_count")) if last_point else None,
            },
            "recent_events": recent_events,
            "recent_alerts": alert_feed,
            "status_counts": [
                {"status": status, "label": _status_label(status), "count": count}
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
            "total_cycles": total_cycles,
            "submitted_count": submitted_count,
            "duplicate_count": duplicate_count,
            "warning_count": warning_count,
            "error_count": error_count,
            "idle_count": idle_count,
            "submission_rate_pct": round((submitted_count / total_cycles) * 100, 2) if total_cycles else 0.0,
            "last_cycle": last_point["cycle"] if last_point else None,
            "last_status": last_point["status"] if last_point else None,
            "last_status_label": _status_label(last_point["status"]) if last_point else "--",
            "last_event_time": last_point["created_at"] if last_point else None,
            "last_submitted_at": last_submitted["created_at"] if last_submitted else None,
            "last_error_at": last_error["created_at"] if last_error else None,
            "current_contracts": current_contracts,
            "target_contracts": target_contracts,
            "contract_gap": contract_gap,
            "current_side": current_side,
            "current_side_label": _side_label(current_side),
            "desired_side": desired_side,
            "desired_side_label": _side_label(desired_side),
        },
        "chart": {
            "points": chart_points,
            "latest_target_contracts": target_contracts,
            "latest_live_contracts": current_contracts,
        },
        "recent_events": recent_events,
        "recent_alerts": alert_feed,
        "status_counts": [
            {"status": status, "label": _status_label(status), "count": count}
            for status, count in sorted(status_counts.items())
        ],
    }


def run_client_alert_test(
    config: AppConfig,
    session_factory,
    *,
    message: str,
) -> dict[str, Any]:
    from quant_lab.cli import _persist_alert_results

    sent_channels = _persist_alert_results(
        session_factory,
        cfg=config,
        event_key="manual_test",
        level="info",
        title="Manual test alert",
        message=f"quant-lab client alert\n{message}",
    )
    return {
        "sent": bool(sent_channels),
        "channels": sent_channels,
        "message": message,
    }


def run_client_align_leverage(
    config: AppConfig,
    session_factory,
    project_root: Path,
    *,
    apply: bool,
    confirm: str = "",
    rearm_protective_stop: bool = False,
) -> dict[str, Any]:
    from quant_lab.cli import (
        _run_demo_align_leverage_action,
    )

    payload, _ = _run_demo_align_leverage_action(
        config,
        apply=apply,
        confirm=confirm,
        rearm_protective_stop=rearm_protective_stop,
        refresh_snapshot=lambda: build_client_snapshot(
            config=config,
            session_factory=session_factory,
            project_root=project_root,
        ),
    )
    return payload


def _heartbeat_point(row: ServiceHeartbeat) -> dict[str, Any]:
    details = row.details if isinstance(row.details, dict) else {}
    mode = str(details.get("mode") or "single")
    target_contracts = _coerce_float(details.get("target_contracts"))
    current_contracts = _coerce_float(details.get("current_contracts"))
    if mode == "portfolio":
        target_contracts = _coerce_float(details.get("actionable_symbol_count"))
        current_contracts = _coerce_float(details.get("active_position_symbol_count"))
    point = {
        "id": row.id,
        "mode": mode,
        "cycle": _coerce_int(details.get("cycle")),
        "status": row.status,
        "status_label": _status_label(row.status),
        "action": str(details.get("action") or "--"),
        "desired_side": _coerce_int(details.get("desired_side")),
        "desired_side_label": _side_label(details.get("desired_side")),
        "current_side": _coerce_int(details.get("current_side")),
        "current_side_label": _side_label(details.get("current_side")),
        "target_contracts": target_contracts,
        "current_contracts": current_contracts,
        "submitted": bool(details.get("submitted")),
        "response_count": _coerce_int(details.get("response_count")) or 0,
        "warning_count": _coerce_int(details.get("warning_count")) or 0,
        "latest_price": _coerce_float(details.get("latest_price")),
        "total_equity": _coerce_float(details.get("total_equity")),
        "available_equity": _coerce_float(details.get("available_equity")),
        "signal_time": details.get("signal_time"),
        "effective_time": details.get("effective_time"),
        "created_at": _serialize_datetime(row.created_at),
    }
    if mode == "portfolio":
        point["symbol_count"] = _coerce_int(details.get("symbol_count"))
        point["submitted_symbol_count"] = _coerce_int(details.get("submitted_symbol_count"))
        point["actionable_symbol_count"] = _coerce_int(details.get("actionable_symbol_count"))
        point["active_position_symbol_count"] = _coerce_int(details.get("active_position_symbol_count"))
        point["submitted_symbols"] = details.get("submitted_symbols") if isinstance(details.get("submitted_symbols"), list) else []
        point["symbol_states"] = details.get("symbol_states") if isinstance(details.get("symbol_states"), dict) else {}
        if point["action"] == "--":
            point["action"] = f"{point['submitted_symbol_count'] or 0}/{point['symbol_count'] or 0} submitted"
    if target_contracts is not None and current_contracts is not None:
        point["contract_gap"] = round(current_contracts - target_contracts, 4)
    else:
        point["contract_gap"] = None
    return point


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


def _status_label(status: str | None) -> str:
    mapping = {
        "submitted": "已提交",
        "duplicate": "已跳过重复计划",
        "idle": "无动作",
        "warning": "警告",
        "error": "错误",
        "plan_only": "仅演练",
        "ok": "正常",
    }
    return mapping.get(str(status or ""), str(status or "--"))


def _side_label(side: Any) -> str:
    side_value = _coerce_int(side)
    if side_value is None:
        return "--"
    if side_value > 0:
        return "做多"
    if side_value < 0:
        return "做空"
    return "空仓"


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _status_label(status: str | None) -> str:
    mapping = {
        "submitted": "已提交",
        "duplicate": "已跳过重复计划",
        "idle": "无动作",
        "warning": "警告",
        "error": "错误",
        "plan_only": "仅演练",
        "ok": "正常",
    }
    return mapping.get(str(status or ""), str(status or "--"))


def _side_label(side: Any) -> str:
    side_value = _coerce_int(side)
    if side_value is None:
        return "--"
    if side_value > 0:
        return "做多"
    if side_value < 0:
        return "做空"
    return "空仓"
