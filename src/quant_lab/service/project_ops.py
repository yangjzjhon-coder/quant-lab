from __future__ import annotations

from pathlib import Path
import threading
from typing import Any

import pandas as pd
from sqlalchemy import select

from quant_lab.application.report_runtime import (
    backtest_artifact_paths,
    backtest_legacy_artifact_paths,
    load_symbol_report_inputs,
    portfolio_report_prefix,
    resolve_instrument_config,
    symbol_slug,
    trades_frame,
    write_backtest_artifacts,
)
from quant_lab.application.project_tasks import (
    SUPPORTED_PROJECT_TASKS,
    default_project_research_report_prefix,
    project_research_defaults,
    project_sweep_defaults,
    project_task_identity,
)
from quant_lab.artifacts import (
    artifact_resolution_path,
    backtest_artifact_identity,
    backtest_artifact_resolution as resolve_backtest_artifact_group,
    backtest_sleeve_artifact_identity,
    canonical_artifact_paths,
    register_artifact_group,
    sleeve_backtest_artifact_resolution as resolve_sleeve_backtest_artifact_group,
    sweep_artifact_identity,
    sweep_artifact_resolution as resolve_sweep_artifact_group,
    trend_research_artifact_identity,
)
from quant_lab.backtest.engine import run_backtest
from quant_lab.backtest.metrics import build_summary
from quant_lab.backtest.portfolio import (
    attach_equal_weight_portfolio_construction,
    attach_portfolio_risk_budget_overlay,
    build_portfolio_summary,
    build_portfolio_risk_budget_overlay,
    build_portfolio_trade_frame,
    combine_portfolio_equity_curves,
)
from quant_lab.backtest.sweep import run_parameter_sweep
from quant_lab.backtest.trend_research import run_trend_research
from quant_lab.config import AppConfig, configured_symbols
from quant_lab.errors import ConflictError, InvalidRequestError, ServiceOperationError
from quant_lab.logging_utils import get_logger
from quant_lab.models import BacktestArtifacts, TradeRecord
from quant_lab.reporting.dashboard import render_dashboard
from quant_lab.reporting.sweep_dashboard import render_sweep_dashboard
from quant_lab.reporting.trend_research_dashboard import render_trend_research_dashboard
from quant_lab.service.database import ProjectTaskRun, session_scope
from quant_lab.service.demo_runtime import ui_code_label
from quant_lab.service.serialization import serialize_utc_datetime

LOGGER = get_logger(__name__)


def _artifact_resolution_path(resolution: dict[str, Any], key: str, fallback: Path) -> Path:
    return artifact_resolution_path(resolution, key, fallback)


def _backtest_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    symbols: list[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    return resolve_backtest_artifact_group(
        config=config,
        project_root=project_root,
        symbols=symbols,
    )


def _sleeve_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    portfolio_symbols: list[str],
    symbol: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    return resolve_sleeve_backtest_artifact_group(
        config=config,
        project_root=project_root,
        portfolio_symbols=portfolio_symbols,
        symbol=symbol,
    )


def _sweep_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    extra: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    return resolve_sweep_artifact_group(
        config=config,
        project_root=project_root,
        extra=extra,
    )


def _project_task_identity_payload(*, config: AppConfig, project_root: Path, task: str) -> dict[str, Any]:
    try:
        identity = project_task_identity(config=config, project_root=project_root, task=task)
    except ValueError as exc:
        raise InvalidRequestError(
            f"Unsupported project task: {task}",
            error_code="unsupported_project_task",
        ) from exc
    return {
        "task": task,
        "logical_prefix": identity["logical_prefix"],
        "artifact_fingerprint": identity["artifact_fingerprint"],
        "symbols": list(identity["symbols"]),
        "mode": identity["mode"],
    }


def run_project_task(*, config: AppConfig, project_root: Path, task: str) -> dict[str, Any]:
    normalized = _normalize_project_task(task)
    LOGGER.info("project task start task=%s project_root=%s", normalized, project_root)
    _ensure_project_task_ready(config=config, project_root=project_root, task=normalized)
    if normalized == "backtest":
        result = _run_backtest_task(config=config, project_root=project_root)
        LOGGER.info("project task completed task=%s", normalized)
        return result
    if normalized == "report":
        result = _run_report_task(config=config, project_root=project_root)
        LOGGER.info("project task completed task=%s", normalized)
        return result
    if normalized == "sweep":
        result = _run_sweep_task(config=config, project_root=project_root)
        LOGGER.info("project task completed task=%s", normalized)
        return result
    if normalized == "research":
        result = _run_research_task(config=config, project_root=project_root)
        LOGGER.info("project task completed task=%s", normalized)
        return result
    raise InvalidRequestError(
        f"Unsupported project task: {task}",
        error_code="unsupported_project_task",
    )


def execute_project_task(
    *,
    config: AppConfig,
    session_factory,
    project_root: Path,
    task: str,
) -> tuple[ProjectTaskRun, dict[str, Any]]:
    normalized = _normalize_project_task(task)
    request_payload = _project_task_identity_payload(config=config, project_root=project_root, task=normalized)
    run = _create_project_task_run(
        session_factory=session_factory,
        task=normalized,
        status="running",
        request_payload=request_payload,
    )
    result = _execute_project_task_run(
        config=config,
        session_factory=session_factory,
        project_root=project_root,
        run_id=run.id,
        task=normalized,
        raise_on_error=True,
    )
    with session_scope(session_factory) as session:
        persisted = session.get(ProjectTaskRun, run.id)
        if persisted is None:
            raise ServiceOperationError(
                "Project task run record disappeared before completion.",
                error_code="project_task_run_missing",
            )
        session.refresh(persisted)
        return persisted, result


def submit_project_task(
    *,
    config: AppConfig,
    session_factory,
    project_root: Path,
    task: str,
) -> ProjectTaskRun:
    normalized = _normalize_project_task(task)
    request_payload = _project_task_identity_payload(config=config, project_root=project_root, task=normalized)
    run = _create_project_task_run(
        session_factory=session_factory,
        task=normalized,
        status="queued",
        request_payload=request_payload,
    )
    LOGGER.info(
        "project task queued task=%s run_id=%s logical_prefix=%s",
        normalized,
        run.id,
        request_payload.get("logical_prefix"),
    )
    worker = threading.Thread(
        target=_run_project_task_in_background,
        kwargs={
            "config": config,
            "session_factory": session_factory,
            "project_root": project_root,
            "run_id": run.id,
            "task": normalized,
        },
        name=f"quant-lab-project-task-{run.id}",
        daemon=True,
    )
    worker.start()
    return run


def serialize_project_task_run(run: ProjectTaskRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "task_name": run.task_name,
        "task_label": ui_code_label(run.task_name),
        "status": run.status,
        "status_label": ui_code_label(run.status),
        "request_payload": run.request_payload,
        "result_payload": run.result_payload,
        "artifact_payload": run.artifact_payload,
        "error_message": run.error_message,
        "started_at": serialize_utc_datetime(run.started_at),
        "finished_at": serialize_utc_datetime(run.finished_at),
        "created_at": serialize_utc_datetime(run.created_at),
    }


def build_project_task_preflight(*, config: AppConfig, project_root: Path) -> dict[str, Any]:
    storage = config.storage.resolved(project_root.resolve())
    resolved_symbols = configured_symbols(config)
    tasks = {
        task_name: _project_task_readiness(
            config=config,
            project_root=project_root.resolve(),
            storage=storage,
            task=task_name,
            resolved_symbols=resolved_symbols,
        )
        for task_name in sorted(SUPPORTED_PROJECT_TASKS)
    }
    return {
        "ready": all(item["ready"] for item in tasks.values()),
        "symbols": resolved_symbols,
        "signal_bar": config.strategy.signal_bar,
        "execution_bar": config.strategy.execution_bar,
        "raw_dir": str(storage.raw_dir),
        "report_dir": str(storage.report_dir),
        "tasks": tasks,
    }


def _normalize_project_task(task: str) -> str:
    normalized = task.strip().lower()
    if normalized not in SUPPORTED_PROJECT_TASKS:
        raise InvalidRequestError(
            f"Unsupported project task: {task}",
            error_code="unsupported_project_task",
        )
    return normalized


def _ensure_project_task_ready(*, config: AppConfig, project_root: Path, task: str) -> None:
    payload = build_project_task_preflight(config=config, project_root=project_root)
    task_payload = payload["tasks"][task]
    if task_payload["ready"]:
        return
    missing_lines = "\n".join(f"- {item}" for item in task_payload["missing"])
    raise ConflictError(
        f"Project task '{task}' is not ready.\n"
        f"Hint: {task_payload['hint']}\n"
        f"Missing artifacts ({len(task_payload['missing'])}):\n{missing_lines}",
        error_code="project_task_not_ready",
    )


def _create_project_task_run(*, session_factory, task: str, status: str, request_payload: dict[str, Any]) -> ProjectTaskRun:
    with session_scope(session_factory) as session:
        active_runs = list(
            session.execute(
                select(ProjectTaskRun).where(
                    ProjectTaskRun.task_name == task,
                    ProjectTaskRun.status.in_(("queued", "running")),
                )
            ).scalars()
        )
        logical_prefix = request_payload.get("logical_prefix")
        for candidate in active_runs:
            payload = candidate.request_payload if isinstance(candidate.request_payload, dict) else {}
            if logical_prefix and payload.get("logical_prefix") == logical_prefix:
                raise ConflictError(
                    f"Project task '{task}' is already active for logical prefix '{logical_prefix}' (run {candidate.id}).",
                    error_code="project_task_already_active",
                )
        run = ProjectTaskRun(
            task_name=task,
            status=status,
            request_payload=request_payload,
        )
        session.add(run)
        session.flush()
        session.refresh(run)
        return run


def _project_task_readiness(*, config: AppConfig, project_root: Path, storage, task: str, resolved_symbols: list[str]) -> dict[str, Any]:
    missing: list[str] = []
    required: list[str] = []
    hint = ""

    if task == "backtest":
        required = _backtest_required_paths(storage=storage, symbols=resolved_symbols, config=config)
        hint = "Run the download step for all configured symbols before starting the portfolio backtest."
    elif task == "report":
        required = _report_required_paths(
            storage=storage,
            symbols=resolved_symbols,
            config=config,
            project_root=project_root,
        )
        hint = "Run backtest first so the required summary, equity curve, and trades artifacts exist."
    elif task == "sweep":
        required = _backtest_required_paths(storage=storage, symbols=[config.instrument.symbol], config=config)
        hint = "Run the download step for the primary instrument before starting the parameter sweep."
    elif task == "research":
        required = _backtest_required_paths(storage=storage, symbols=[config.instrument.symbol], config=config)
        hint = "Run the download step for the primary instrument before starting the research scan."
    else:
        raise InvalidRequestError(
            f"Unsupported project task: {task}",
            error_code="unsupported_project_task",
        )

    for path in required:
        if not path.exists():
            missing.append(str(path))

    return {
        "task": task,
        "ready": not missing,
        "required_count": len(required),
        "present_count": len(required) - len(missing),
        "missing": missing,
        "hint": hint,
    }


def _backtest_required_paths(*, storage, symbols: list[str], config: AppConfig) -> list[Path]:
    required: list[Path] = []
    for symbol in symbols:
        symbol_slug_value = symbol_slug(symbol)
        required.extend(
            [
                storage.raw_dir / f"{symbol_slug_value}_{config.strategy.signal_bar}.parquet",
                storage.raw_dir / f"{symbol_slug_value}_{config.strategy.execution_bar}.parquet",
                storage.raw_dir / f"{symbol_slug_value}_funding.parquet",
            ]
        )
    return required


def _report_required_paths(*, storage, symbols: list[str], config: AppConfig, project_root: Path) -> list[Path]:
    if len(symbols) == 1:
        identity, resolution = _backtest_artifact_resolution(
            config=config,
            project_root=project_root,
            symbols=symbols,
        )
        logical_prefix = str(identity["logical_prefix"])
        return [
            _artifact_resolution_path(resolution, "summary", storage.report_dir / f"{logical_prefix}_summary.json"),
            _artifact_resolution_path(
                resolution,
                "equity_curve",
                storage.report_dir / f"{logical_prefix}_equity_curve.csv",
            ),
            _artifact_resolution_path(resolution, "trades", storage.report_dir / f"{logical_prefix}_trades.csv"),
        ]

    required: list[Path] = []
    for symbol in symbols:
        identity, resolution = _sleeve_artifact_resolution(
            config=config,
            project_root=project_root,
            portfolio_symbols=symbols,
            symbol=symbol,
        )
        report_prefix = str(identity["logical_prefix"])
        required.extend(
            [
                _artifact_resolution_path(resolution, "summary", storage.report_dir / f"{report_prefix}_summary.json"),
                _artifact_resolution_path(
                    resolution,
                    "equity_curve",
                    storage.report_dir / f"{report_prefix}_equity_curve.csv",
                ),
                _artifact_resolution_path(resolution, "trades", storage.report_dir / f"{report_prefix}_trades.csv"),
            ]
        )

    identity, resolution = _backtest_artifact_resolution(
        config=config,
        project_root=project_root,
        symbols=symbols,
    )
    portfolio_prefix = str(identity["logical_prefix"])
    required.extend(
        [
            _artifact_resolution_path(resolution, "summary", storage.report_dir / f"{portfolio_prefix}_summary.json"),
            _artifact_resolution_path(
                resolution,
                "equity_curve",
                storage.report_dir / f"{portfolio_prefix}_equity_curve.csv",
            ),
            _artifact_resolution_path(resolution, "trades", storage.report_dir / f"{portfolio_prefix}_trades.csv"),
        ]
    )
    return required


def _run_project_task_in_background(
    *,
    config: AppConfig,
    session_factory,
    project_root: Path,
    run_id: int,
    task: str,
) -> None:
    _execute_project_task_run(
        config=config,
        session_factory=session_factory,
        project_root=project_root,
        run_id=run_id,
        task=task,
        raise_on_error=False,
        mark_running=True,
    )


def _execute_project_task_run(
    *,
    config: AppConfig,
    session_factory,
    project_root: Path,
    run_id: int,
    task: str,
    raise_on_error: bool,
    mark_running: bool = False,
) -> dict[str, Any]:
    if mark_running:
        with session_scope(session_factory) as session:
            run = session.get(ProjectTaskRun, run_id)
            if run is None:
                raise ServiceOperationError(
                    f"Project task run {run_id} disappeared before execution.",
                    error_code="project_task_run_missing",
                )
            run.status = "running"

    try:
        result = run_project_task(config=config, project_root=project_root, task=task)
    except Exception as exc:
        LOGGER.exception("project task failed task=%s run_id=%s", task, run_id)
        with session_scope(session_factory) as session:
            run = session.get(ProjectTaskRun, run_id)
            if run is not None:
                run.status = "failed"
                run.error_message = f"{type(exc).__name__}: {exc}"
                run.finished_at = pd.Timestamp.now(tz="UTC").to_pydatetime()
        if raise_on_error:
            raise
        return {}

    with session_scope(session_factory) as session:
        run = session.get(ProjectTaskRun, run_id)
        if run is None:
            raise ServiceOperationError(
                "Project task run record disappeared before completion.",
                error_code="project_task_run_missing",
            )
        run.status = "completed"
        run.result_payload = result
        run.artifact_payload = result.get("artifacts") if isinstance(result, dict) else None
        run.error_message = None
        run.finished_at = pd.Timestamp.now(tz="UTC").to_pydatetime()
    LOGGER.info("project task persisted completed task=%s run_id=%s", task, run_id)
    return result


def _run_backtest_task(*, config: AppConfig, project_root: Path) -> dict[str, Any]:
    resolved_symbols = configured_symbols(config)
    storage = config.storage

    if len(resolved_symbols) == 1:
        symbol = resolved_symbols[0]
        cfg, storage, signal_bars, execution_bars, funding, symbol_slug_value = load_symbol_report_inputs(
            cfg=config,
            project_root=project_root,
            symbol=symbol,
        )
        instrument_config = resolve_instrument_config(cfg, storage, symbol)
        artifacts = run_backtest(
            signal_bars=signal_bars,
            execution_bars=execution_bars,
            funding_rates=funding,
            strategy_config=cfg.strategy,
            execution_config=cfg.execution,
            risk_config=cfg.risk,
            instrument_config=instrument_config,
        )
        summary = build_summary(
            equity_curve=artifacts.equity_curve,
            trades=artifacts.trades,
            initial_equity=cfg.execution.initial_equity,
        )
        report_prefix = f"{symbol_slug_value}_{cfg.strategy.name}"
        artifact_identity = backtest_artifact_identity(
            config=cfg,
            project_root=project_root.resolve(),
            symbols=resolved_symbols,
        )
        trades_path, equity_path, summary_path = write_backtest_artifacts(
            storage=storage,
            report_prefix=report_prefix,
            trades_frame=trades_frame(artifacts.trades),
            equity_curve=artifacts.equity_curve,
            summary=summary,
            signal_frame=artifacts.signal_frame,
            execution_bars=execution_bars,
            artifact_identity=artifact_identity,
        )
        return {
            "task": "backtest",
            "mode": "single",
            "symbols": resolved_symbols,
            "summary": summary,
            "artifacts": {
                "trades": str(trades_path),
                "equity_curve": str(equity_path),
                "summary": str(summary_path),
            },
        }

    per_symbol_initial_equity = config.execution.initial_equity / len(resolved_symbols)
    equity_curves_by_symbol: dict[str, pd.DataFrame] = {}
    artifacts_by_symbol: dict[str, BacktestArtifacts] = {}
    trades_by_symbol: dict[str, list[TradeRecord]] = {}
    all_trades: list[TradeRecord] = []
    sleeve_summaries: list[dict[str, object]] = []
    sleeve_artifacts: list[dict[str, str]] = []

    for symbol in resolved_symbols:
        cfg, storage, signal_bars, execution_bars, funding, symbol_slug_value = load_symbol_report_inputs(
            cfg=config,
            project_root=project_root,
            symbol=symbol,
        )
        instrument_config = resolve_instrument_config(cfg, storage, symbol)
        execution_config = cfg.execution.model_copy(update={"initial_equity": per_symbol_initial_equity})
        artifacts = run_backtest(
            signal_bars=signal_bars,
            execution_bars=execution_bars,
            funding_rates=funding,
            strategy_config=cfg.strategy,
            execution_config=execution_config,
            risk_config=cfg.risk,
            instrument_config=instrument_config,
        )
        summary = build_summary(
            equity_curve=artifacts.equity_curve,
            trades=artifacts.trades,
            initial_equity=per_symbol_initial_equity,
        )
        summary["symbol"] = symbol
        summary["capital_allocation_pct"] = round(100 / len(resolved_symbols), 2)
        sleeve_summaries.append(summary)

        report_prefix = f"{symbol_slug_value}_{cfg.strategy.name}_sleeve"
        artifact_identity = backtest_sleeve_artifact_identity(
            config=cfg,
            project_root=project_root.resolve(),
            portfolio_symbols=resolved_symbols,
            symbol=symbol,
        )
        trades_path, equity_path, summary_path = write_backtest_artifacts(
            storage=storage,
            report_prefix=report_prefix,
            trades_frame=trades_frame(artifacts.trades),
            equity_curve=artifacts.equity_curve,
            summary=summary,
            signal_frame=artifacts.signal_frame,
            execution_bars=execution_bars,
            artifact_identity=artifact_identity,
        )
        sleeve_artifacts.append(
            {
                "symbol": symbol,
                "trades": str(trades_path),
                "equity_curve": str(equity_path),
                "summary": str(summary_path),
            }
        )

        equity_curves_by_symbol[symbol] = artifacts.equity_curve
        artifacts_by_symbol[symbol] = artifacts
        trades_by_symbol[symbol] = artifacts.trades
        all_trades.extend(artifacts.trades)

    portfolio_equity = combine_portfolio_equity_curves(equity_curves_by_symbol)
    portfolio_trades = build_portfolio_trade_frame(trades_by_symbol)
    portfolio_summary = build_portfolio_summary(
        equity_curve=portfolio_equity,
        trades=all_trades,
        initial_equity=config.execution.initial_equity,
        symbols=resolved_symbols,
    )
    portfolio_summary = attach_equal_weight_portfolio_construction(
        portfolio_summary,
        per_symbol_initial_equity=per_symbol_initial_equity,
    )
    portfolio_allocation_overlay = build_portfolio_risk_budget_overlay(
        symbol_artifacts=artifacts_by_symbol,
        execution_config=config.execution,
        risk_config=config.risk,
    )
    portfolio_summary = attach_portfolio_risk_budget_overlay(
        portfolio_summary,
        allocation_frame=portfolio_allocation_overlay,
    )

    portfolio_prefix = portfolio_report_prefix(resolved_symbols, config.strategy.name)
    portfolio_identity = backtest_artifact_identity(
        config=config,
        project_root=project_root.resolve(),
        symbols=resolved_symbols,
    )
    trades_path, equity_path, summary_path = write_backtest_artifacts(
        storage=storage,
        report_prefix=portfolio_prefix,
        trades_frame=portfolio_trades,
        equity_curve=portfolio_equity,
        summary=portfolio_summary,
        allocation_overlay=portfolio_allocation_overlay,
        artifact_identity=portfolio_identity,
    )
    sleeves_path = storage.report_dir / f"{portfolio_prefix}_sleeves.csv"
    pd.DataFrame(sleeve_summaries).to_csv(sleeves_path, index=False)

    return {
        "task": "backtest",
        "mode": "portfolio",
        "symbols": resolved_symbols,
        "summary": portfolio_summary,
        "artifacts": {
            "portfolio_trades": str(trades_path),
            "portfolio_equity_curve": str(equity_path),
            "portfolio_summary": str(summary_path),
            "portfolio_sleeves": str(sleeves_path),
        },
        "sleeves": sleeve_artifacts,
    }


def _run_report_task(*, config: AppConfig, project_root: Path) -> dict[str, Any]:
    storage = config.storage
    resolved_symbols = configured_symbols(config)
    resolved_root = project_root.resolve()

    if len(resolved_symbols) == 1:
        artifact_identity, resolution = _backtest_artifact_resolution(
            config=config,
            project_root=resolved_root,
            symbols=resolved_symbols,
        )
        report_prefix = str(artifact_identity["logical_prefix"])
        trades_path = _artifact_resolution_path(
            resolution,
            "trades",
            storage.report_dir / f"{report_prefix}_trades.csv",
        )
        equity_path = _artifact_resolution_path(
            resolution,
            "equity_curve",
            storage.report_dir / f"{report_prefix}_equity_curve.csv",
        )
        summary_path = _artifact_resolution_path(
            resolution,
            "summary",
            storage.report_dir / f"{report_prefix}_summary.json",
        )
        output_path = backtest_artifact_paths(
            storage=storage,
            artifact_identity=artifact_identity,
            include_dashboard=True,
        )["dashboard"]
        render_dashboard(
            summary_path=summary_path,
            equity_curve_path=equity_path,
            trades_path=trades_path,
            output_path=output_path,
            title=f"{resolved_symbols[0]} {config.strategy.name}",
        )
        register_artifact_group(
            report_dir=storage.report_dir,
            identity=artifact_identity,
            artifacts={"dashboard": output_path},
            legacy_artifact_sets=[
                backtest_legacy_artifact_paths(
                    storage=storage,
                    report_prefix=report_prefix,
                    include_dashboard=True,
                )
            ],
        )
        return {
            "task": "report",
            "mode": "single",
            "symbols": resolved_symbols,
            "artifacts": {
                "dashboard": str(output_path),
                "logical_prefix": report_prefix,
                "artifact_fingerprint": artifact_identity["artifact_fingerprint"],
            },
        }

    dashboard_paths: list[str] = []
    for symbol in resolved_symbols:
        artifact_identity, resolution = _sleeve_artifact_resolution(
            config=config,
            project_root=resolved_root,
            portfolio_symbols=resolved_symbols,
            symbol=symbol,
        )
        report_prefix = str(artifact_identity["logical_prefix"])
        trades_path = _artifact_resolution_path(
            resolution,
            "trades",
            storage.report_dir / f"{report_prefix}_trades.csv",
        )
        equity_path = _artifact_resolution_path(
            resolution,
            "equity_curve",
            storage.report_dir / f"{report_prefix}_equity_curve.csv",
        )
        summary_path = _artifact_resolution_path(
            resolution,
            "summary",
            storage.report_dir / f"{report_prefix}_summary.json",
        )
        output_path = backtest_artifact_paths(
            storage=storage,
            artifact_identity=artifact_identity,
            include_dashboard=True,
        )["dashboard"]
        render_dashboard(
            summary_path=summary_path,
            equity_curve_path=equity_path,
            trades_path=trades_path,
            output_path=output_path,
            title=f"{symbol} {config.strategy.name} 子报表",
        )
        register_artifact_group(
            report_dir=storage.report_dir,
            identity=artifact_identity,
            artifacts={"dashboard": output_path},
            legacy_artifact_sets=[
                backtest_legacy_artifact_paths(
                    storage=storage,
                    report_prefix=report_prefix,
                    include_dashboard=True,
                )
            ],
        )
        dashboard_paths.append(str(output_path))

    artifact_identity, resolution = _backtest_artifact_resolution(
        config=config,
        project_root=resolved_root,
        symbols=resolved_symbols,
    )
    portfolio_prefix = str(artifact_identity["logical_prefix"])
    trades_path = _artifact_resolution_path(
        resolution,
        "trades",
        storage.report_dir / f"{portfolio_prefix}_trades.csv",
    )
    equity_path = _artifact_resolution_path(
        resolution,
        "equity_curve",
        storage.report_dir / f"{portfolio_prefix}_equity_curve.csv",
    )
    summary_path = _artifact_resolution_path(
        resolution,
        "summary",
        storage.report_dir / f"{portfolio_prefix}_summary.json",
    )
    output_path = backtest_artifact_paths(
        storage=storage,
        artifact_identity=artifact_identity,
        include_dashboard=True,
    )["dashboard"]
    render_dashboard(
        summary_path=summary_path,
        equity_curve_path=equity_path,
        trades_path=trades_path,
        output_path=output_path,
        title=f"{' / '.join(resolved_symbols)} {config.strategy.name} 组合总览",
    )
    register_artifact_group(
        report_dir=storage.report_dir,
        identity=artifact_identity,
        artifacts={"dashboard": output_path},
        legacy_artifact_sets=[
            backtest_legacy_artifact_paths(
                storage=storage,
                report_prefix=portfolio_prefix,
                include_dashboard=True,
            )
        ],
    )
    return {
        "task": "report",
        "mode": "portfolio",
        "symbols": resolved_symbols,
        "artifacts": {
            "portfolio_dashboard": str(output_path),
            "sleeve_dashboards": dashboard_paths,
            "logical_prefix": portfolio_prefix,
            "artifact_fingerprint": artifact_identity["artifact_fingerprint"],
        },
    }


def _run_sweep_task(*, config: AppConfig, project_root: Path) -> dict[str, Any]:
    cfg, storage, signal_bars, execution_bars, funding, _symbol_slug_value = load_symbol_report_inputs(
        cfg=config,
        project_root=project_root,
        symbol=config.instrument.symbol,
    )
    defaults = project_sweep_defaults()
    fast_values = defaults["fast_values"]
    slow_values = defaults["slow_values"]
    atr_values = defaults["atr_values"]
    results = run_parameter_sweep(
        signal_bars=signal_bars,
        execution_bars=execution_bars,
        funding_rates=funding,
        strategy_config=cfg.strategy,
        execution_config=cfg.execution,
        risk_config=cfg.risk,
        instrument_config=cfg.instrument,
        fast_values=fast_values,
        slow_values=slow_values,
        atr_values=atr_values,
    )
    artifact_identity = sweep_artifact_identity(
        config=cfg,
        project_root=project_root.resolve(),
        extra={
            "fast_values": fast_values,
            "slow_values": slow_values,
            "atr_values": atr_values,
        },
    )
    report_prefix = str(artifact_identity["logical_prefix"])
    artifact_paths = canonical_artifact_paths(
        report_dir=storage.report_dir,
        identity=artifact_identity,
        suffixes={
            "sweep_csv": "sweep.csv",
            "dashboard": "sweep_dashboard.html",
        },
    )
    results_path = artifact_paths["sweep_csv"]
    dashboard_path = artifact_paths["dashboard"]
    results.to_csv(results_path, index=False)
    render_sweep_dashboard(
        results=results,
        output_path=dashboard_path,
        title=f"{cfg.instrument.symbol} {cfg.strategy.name}",
    )
    register_artifact_group(
        report_dir=storage.report_dir,
        identity=artifact_identity,
        artifacts=artifact_paths,
        legacy_artifact_sets=[
            {
                "sweep_csv": storage.report_dir / f"{report_prefix}_sweep.csv",
                "dashboard": storage.report_dir / f"{report_prefix}_sweep_dashboard.html",
            }
        ],
    )
    return {
        "task": "sweep",
        "mode": "single",
        "symbols": [cfg.instrument.symbol],
        "artifacts": {
            "sweep_csv": str(results_path),
            "sweep_dashboard": str(dashboard_path),
            "logical_prefix": report_prefix,
            "artifact_fingerprint": artifact_identity["artifact_fingerprint"],
        },
        "top_rows": results.head(5).to_dict(orient="records"),
    }


def _run_research_task(*, config: AppConfig, project_root: Path) -> dict[str, Any]:
    cfg, storage, signal_bars, execution_bars, funding, _symbol_slug_value = load_symbol_report_inputs(
        cfg=config,
        project_root=project_root,
        symbol=config.instrument.symbol,
    )
    defaults = project_research_defaults()
    variants = defaults["variant_values"]
    fast_values = defaults["fast_values"]
    slow_values = defaults["slow_values"]
    atr_values = defaults["atr_values"]
    trend_ema_values = defaults["trend_ema_values"]
    adx_values = defaults["adx_values"]
    results = run_trend_research(
        signal_bars=signal_bars,
        execution_bars=execution_bars,
        funding_rates=funding,
        strategy_config=cfg.strategy,
        execution_config=cfg.execution,
        risk_config=cfg.risk,
        instrument_config=cfg.instrument,
        variants=variants,
        fast_values=fast_values,
        slow_values=slow_values,
        atr_values=atr_values,
        trend_ema_values=trend_ema_values,
        adx_threshold_values=adx_values,
    )
    report_prefix = default_project_research_report_prefix(cfg)
    artifact_identity = trend_research_artifact_identity(
        config=cfg,
        project_root=project_root.resolve(),
        logical_prefix=report_prefix,
        extra={
            "variants": variants,
            "fast_values": fast_values,
            "slow_values": slow_values,
            "atr_values": atr_values,
            "trend_ema_values": trend_ema_values,
            "adx_values": adx_values,
        },
    )
    artifact_paths = canonical_artifact_paths(
        report_dir=storage.report_dir,
        identity=artifact_identity,
        suffixes={
            "research_csv": "research.csv",
            "dashboard": "dashboard.html",
        },
    )
    results_path = artifact_paths["research_csv"]
    dashboard_path = artifact_paths["dashboard"]
    results.to_csv(results_path, index=False)
    render_trend_research_dashboard(
        results=results,
        output_path=dashboard_path,
        title=f"{cfg.instrument.symbol} {cfg.strategy.name} {cfg.strategy.signal_bar} 趋势研究",
    )
    register_artifact_group(
        report_dir=storage.report_dir,
        identity=artifact_identity,
        artifacts=artifact_paths,
        legacy_artifact_sets=[
            {
                "research_csv": storage.report_dir / f"{report_prefix}.csv",
                "dashboard": storage.report_dir / f"{report_prefix}.html",
            }
        ],
    )
    return {
        "task": "research",
        "mode": "single",
        "symbols": [cfg.instrument.symbol],
        "artifacts": {
            "research_csv": str(results_path),
            "research_dashboard": str(dashboard_path),
            "logical_prefix": report_prefix,
            "artifact_fingerprint": artifact_identity["artifact_fingerprint"],
        },
        "top_rows": results.head(10).to_dict(orient="records"),
    }
