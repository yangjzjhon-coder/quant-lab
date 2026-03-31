from __future__ import annotations

from pathlib import Path
from typing import Any

from quant_lab.application.demo_support import persist_alert_results
from quant_lab.config import AppConfig
from quant_lab.service.demo_runtime import (
    build_autotrade_status,
    build_client_checks_summary,
    build_client_exchange_summary,
    build_client_headline_summary,
    build_client_plan_summary,
    build_client_symbol_summary,
    build_client_warning_summary,
    build_demo_visuals_payload,
    build_runtime_snapshot,
    run_align_leverage_action,
)


def build_client_snapshot(config: AppConfig, session_factory, project_root: Path) -> dict[str, Any]:
    runtime_snapshot = build_runtime_snapshot(
        config=config,
        session_factory=session_factory,
        project_root=project_root,
    )
    preflight = runtime_snapshot["preflight"]
    reconcile = runtime_snapshot["reconcile"]
    snapshot_source = str(runtime_snapshot.get("snapshot_source") or "live_okx")
    live_error = runtime_snapshot.get("live_error")
    live_error = str(live_error) if live_error is not None else None

    demo_visuals = build_demo_visuals_payload(
        session_factory=session_factory,
        reconcile=reconcile,
    )
    autotrade_status = build_autotrade_status(
        preflight=preflight,
        reconcile=reconcile,
        demo_visuals=demo_visuals,
        snapshot_source=snapshot_source,
        live_error=live_error,
    )
    headline_summary = build_client_headline_summary(
        preflight=preflight,
        autotrade_status=autotrade_status,
        demo_visuals=demo_visuals,
        snapshot_source=snapshot_source,
    )
    warning_summary = build_client_warning_summary(
        preflight=preflight,
        reconcile=reconcile,
        autotrade_status=autotrade_status,
        live_error=live_error,
    )
    checks_summary = build_client_checks_summary(
        preflight=preflight,
        reconcile=reconcile,
    )
    exchange_summary = build_client_exchange_summary(
        preflight=preflight,
        reconcile=reconcile,
        snapshot_source=snapshot_source,
        live_error=live_error,
    )
    plan_summary = build_client_plan_summary(reconcile=reconcile)
    symbol_summary = build_client_symbol_summary(reconcile=reconcile)
    payload = {
        "preflight": preflight,
        "reconcile": reconcile,
        "demo_visuals": demo_visuals,
        "autotrade_status": autotrade_status,
        "headline_summary": headline_summary,
        "checks_summary": checks_summary,
        "exchange_summary": exchange_summary,
        "plan_summary": plan_summary,
        "symbol_summary": symbol_summary,
        "warning_summary": warning_summary,
        "snapshot_source": snapshot_source,
    }
    if live_error is not None:
        payload["live_error"] = live_error
    return payload


def run_client_alert_test(
    config: AppConfig,
    session_factory,
    *,
    message: str,
) -> dict[str, Any]:
    sent_channels = persist_alert_results(
        session_factory,
        alerts_config=config.alerts,
        event_key="manual_test",
        level="info",
        title="手动测试告警",
        message=f"quant-lab 客户端告警\n{message}",
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
    payload, _ = run_align_leverage_action(
        config=config,
        session_factory=session_factory,
        project_root=project_root,
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
