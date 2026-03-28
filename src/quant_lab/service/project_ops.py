from __future__ import annotations

from pathlib import Path
import threading
from typing import Any

import pandas as pd

from quant_lab.backtest.engine import run_backtest
from quant_lab.backtest.metrics import build_summary
from quant_lab.backtest.portfolio import (
    build_portfolio_summary,
    build_portfolio_trade_frame,
    combine_portfolio_equity_curves,
)
from quant_lab.backtest.sweep import run_parameter_sweep
from quant_lab.backtest.trend_research import run_trend_research
from quant_lab.cli import (
    _load_symbol_report_inputs,
    _parse_float_list,
    _parse_int_list,
    _parse_text_list,
    _portfolio_report_prefix,
    _resolve_instrument_config,
    _symbol_slug,
    _trades_frame,
    _write_backtest_artifacts,
)
from quant_lab.config import AppConfig, configured_symbols
from quant_lab.models import TradeRecord
from quant_lab.reporting.dashboard import render_dashboard
from quant_lab.reporting.sweep_dashboard import render_sweep_dashboard
from quant_lab.reporting.trend_research_dashboard import render_trend_research_dashboard
from quant_lab.service.database import ProjectTaskRun, session_scope

DEFAULT_SWEEP_FAST = "10,20,30"
DEFAULT_SWEEP_SLOW = "50,80,120"
DEFAULT_SWEEP_ATR = "1.5,2.0,2.5"
DEFAULT_RESEARCH_VARIANTS = "breakout_retest,breakout_retest_regime,breakout_retest_adx,breakout_retest_regime_adx"
DEFAULT_RESEARCH_FAST = "8,12,16"
DEFAULT_RESEARCH_SLOW = "24,36,48,72"
DEFAULT_RESEARCH_ATR = "2.5,3.0,3.5"
DEFAULT_RESEARCH_TREND_EMA = "200"
DEFAULT_RESEARCH_ADX = "20,25"
SUPPORTED_PROJECT_TASKS = {"backtest", "report", "sweep", "research"}


def run_project_task(*, config: AppConfig, project_root: Path, task: str) -> dict[str, Any]:
    normalized = _normalize_project_task(task)
    _ensure_project_task_ready(config=config, project_root=project_root, task=normalized)
    if normalized == "backtest":
        return _run_backtest_task(config=config, project_root=project_root)
    if normalized == "report":
        return _run_report_task(config=config, project_root=project_root)
    if normalized == "sweep":
        return _run_sweep_task(config=config, project_root=project_root)
    if normalized == "research":
        return _run_research_task(config=config, project_root=project_root)
    raise ValueError(f"Unsupported project task: {task}")


def execute_project_task(
    *,
    config: AppConfig,
    session_factory,
    project_root: Path,
    task: str,
) -> tuple[ProjectTaskRun, dict[str, Any]]:
    normalized = _normalize_project_task(task)
    run = _create_project_task_run(session_factory=session_factory, task=normalized, status="running")
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
            raise RuntimeError("Project task run record disappeared before completion.")
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
    run = _create_project_task_run(session_factory=session_factory, task=normalized, status="queued")
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
        "status": run.status,
        "request_payload": run.request_payload,
        "result_payload": run.result_payload,
        "artifact_payload": run.artifact_payload,
        "error_message": run.error_message,
        "started_at": run.started_at.isoformat() if run.started_at is not None else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at is not None else None,
        "created_at": run.created_at.isoformat() if run.created_at is not None else None,
    }


def build_project_task_preflight(*, config: AppConfig, project_root: Path) -> dict[str, Any]:
    storage = config.storage.resolved(project_root.resolve())
    resolved_symbols = configured_symbols(config)
    tasks = {
        task_name: _project_task_readiness(
            config=config,
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
        raise ValueError(f"Unsupported project task: {task}")
    return normalized


def _ensure_project_task_ready(*, config: AppConfig, project_root: Path, task: str) -> None:
    payload = build_project_task_preflight(config=config, project_root=project_root)
    task_payload = payload["tasks"][task]
    if task_payload["ready"]:
        return
    missing_lines = "\n".join(f"- {item}" for item in task_payload["missing"])
    raise FileNotFoundError(
        f"Project task '{task}' is not ready.\n"
        f"Hint: {task_payload['hint']}\n"
        f"Missing artifacts ({len(task_payload['missing'])}):\n{missing_lines}"
    )


def _create_project_task_run(*, session_factory, task: str, status: str) -> ProjectTaskRun:
    with session_scope(session_factory) as session:
        run = ProjectTaskRun(
            task_name=task,
            status=status,
            request_payload={"task": task},
        )
        session.add(run)
        session.flush()
        session.refresh(run)
        return run


def _project_task_readiness(*, config: AppConfig, storage, task: str, resolved_symbols: list[str]) -> dict[str, Any]:
    missing: list[str] = []
    required: list[str] = []
    hint = ""

    if task == "backtest":
        required = _backtest_required_paths(storage=storage, symbols=resolved_symbols, config=config)
        hint = "Run the download step for all configured symbols before starting the portfolio backtest."
    elif task == "report":
        required = _report_required_paths(storage=storage, symbols=resolved_symbols, config=config)
        hint = "Run backtest first so the required summary, equity curve, and trades artifacts exist."
    elif task == "sweep":
        required = _backtest_required_paths(storage=storage, symbols=[config.instrument.symbol], config=config)
        hint = "Run the download step for the primary instrument before starting the parameter sweep."
    elif task == "research":
        required = _backtest_required_paths(storage=storage, symbols=[config.instrument.symbol], config=config)
        hint = "Run the download step for the primary instrument before starting the research scan."
    else:
        raise ValueError(f"Unsupported project task: {task}")

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
        symbol_slug = _symbol_slug(symbol)
        required.extend(
            [
                storage.raw_dir / f"{symbol_slug}_{config.strategy.signal_bar}.parquet",
                storage.raw_dir / f"{symbol_slug}_{config.strategy.execution_bar}.parquet",
                storage.raw_dir / f"{symbol_slug}_funding.parquet",
            ]
        )
    return required


def _report_required_paths(*, storage, symbols: list[str], config: AppConfig) -> list[Path]:
    if len(symbols) == 1:
        symbol_slug = _symbol_slug(symbols[0])
        report_prefix = f"{symbol_slug}_{config.strategy.name}"
        return [
            storage.report_dir / f"{report_prefix}_summary.json",
            storage.report_dir / f"{report_prefix}_equity_curve.csv",
            storage.report_dir / f"{report_prefix}_trades.csv",
        ]

    required: list[Path] = []
    for symbol in symbols:
        symbol_slug = _symbol_slug(symbol)
        report_prefix = f"{symbol_slug}_{config.strategy.name}_sleeve"
        required.extend(
            [
                storage.report_dir / f"{report_prefix}_summary.json",
                storage.report_dir / f"{report_prefix}_equity_curve.csv",
                storage.report_dir / f"{report_prefix}_trades.csv",
            ]
        )

    portfolio_prefix = _portfolio_report_prefix(symbols, config.strategy.name)
    required.extend(
        [
            storage.report_dir / f"{portfolio_prefix}_summary.json",
            storage.report_dir / f"{portfolio_prefix}_equity_curve.csv",
            storage.report_dir / f"{portfolio_prefix}_trades.csv",
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
                raise RuntimeError(f"Project task run {run_id} disappeared before execution.")
            run.status = "running"

    try:
        result = run_project_task(config=config, project_root=project_root, task=task)
    except Exception as exc:
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
            raise RuntimeError("Project task run record disappeared before completion.")
        run.status = "completed"
        run.result_payload = result
        run.artifact_payload = result.get("artifacts") if isinstance(result, dict) else None
        run.error_message = None
        run.finished_at = pd.Timestamp.now(tz="UTC").to_pydatetime()
    return result


def _run_backtest_task(*, config: AppConfig, project_root: Path) -> dict[str, Any]:
    resolved_symbols = configured_symbols(config)
    storage = config.storage

    if len(resolved_symbols) == 1:
        symbol = resolved_symbols[0]
        cfg, storage, signal_bars, execution_bars, funding, symbol_slug = _load_symbol_report_inputs(
            cfg=config,
            project_root=project_root,
            symbol=symbol,
        )
        instrument_config = _resolve_instrument_config(cfg, storage, symbol)
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
        report_prefix = f"{symbol_slug}_{cfg.strategy.name}"
        trades_path, equity_path, summary_path = _write_backtest_artifacts(
            storage=storage,
            report_prefix=report_prefix,
            trades_frame=_trades_frame(artifacts.trades),
            equity_curve=artifacts.equity_curve,
            summary=summary,
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
    trades_by_symbol: dict[str, list[TradeRecord]] = {}
    all_trades: list[TradeRecord] = []
    sleeve_summaries: list[dict[str, object]] = []
    sleeve_artifacts: list[dict[str, str]] = []

    for symbol in resolved_symbols:
        cfg, storage, signal_bars, execution_bars, funding, symbol_slug = _load_symbol_report_inputs(
            cfg=config,
            project_root=project_root,
            symbol=symbol,
        )
        instrument_config = _resolve_instrument_config(cfg, storage, symbol)
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

        report_prefix = f"{symbol_slug}_{cfg.strategy.name}_sleeve"
        trades_path, equity_path, summary_path = _write_backtest_artifacts(
            storage=storage,
            report_prefix=report_prefix,
            trades_frame=_trades_frame(artifacts.trades),
            equity_curve=artifacts.equity_curve,
            summary=summary,
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
    portfolio_summary["allocation_mode"] = "equal_weight"
    portfolio_summary["per_symbol_initial_equity"] = round(per_symbol_initial_equity, 2)

    portfolio_prefix = _portfolio_report_prefix(resolved_symbols, config.strategy.name)
    trades_path, equity_path, summary_path = _write_backtest_artifacts(
        storage=storage,
        report_prefix=portfolio_prefix,
        trades_frame=portfolio_trades,
        equity_curve=portfolio_equity,
        summary=portfolio_summary,
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

    if len(resolved_symbols) == 1:
        symbol_slug = _symbol_slug(resolved_symbols[0])
        report_prefix = f"{symbol_slug}_{config.strategy.name}"
        trades_path = storage.report_dir / f"{report_prefix}_trades.csv"
        equity_path = storage.report_dir / f"{report_prefix}_equity_curve.csv"
        summary_path = storage.report_dir / f"{report_prefix}_summary.json"
        output_path = storage.report_dir / f"{report_prefix}_dashboard.html"
        render_dashboard(
            summary_path=summary_path,
            equity_curve_path=equity_path,
            trades_path=trades_path,
            output_path=output_path,
            title=f"{resolved_symbols[0]} {config.strategy.name}",
        )
        return {
            "task": "report",
            "mode": "single",
            "symbols": resolved_symbols,
            "artifacts": {"dashboard": str(output_path)},
        }

    dashboard_paths: list[str] = []
    for symbol in resolved_symbols:
        symbol_slug = _symbol_slug(symbol)
        report_prefix = f"{symbol_slug}_{config.strategy.name}_sleeve"
        trades_path = storage.report_dir / f"{report_prefix}_trades.csv"
        equity_path = storage.report_dir / f"{report_prefix}_equity_curve.csv"
        summary_path = storage.report_dir / f"{report_prefix}_summary.json"
        output_path = storage.report_dir / f"{report_prefix}_dashboard.html"
        render_dashboard(
            summary_path=summary_path,
            equity_curve_path=equity_path,
            trades_path=trades_path,
            output_path=output_path,
            title=f"{symbol} {config.strategy.name} Sleeve",
        )
        dashboard_paths.append(str(output_path))

    portfolio_prefix = _portfolio_report_prefix(resolved_symbols, config.strategy.name)
    trades_path = storage.report_dir / f"{portfolio_prefix}_trades.csv"
    equity_path = storage.report_dir / f"{portfolio_prefix}_equity_curve.csv"
    summary_path = storage.report_dir / f"{portfolio_prefix}_summary.json"
    output_path = storage.report_dir / f"{portfolio_prefix}_dashboard.html"
    render_dashboard(
        summary_path=summary_path,
        equity_curve_path=equity_path,
        trades_path=trades_path,
        output_path=output_path,
        title=f"{' / '.join(resolved_symbols)} {config.strategy.name} Portfolio",
    )
    return {
        "task": "report",
        "mode": "portfolio",
        "symbols": resolved_symbols,
        "artifacts": {
            "portfolio_dashboard": str(output_path),
            "sleeve_dashboards": dashboard_paths,
        },
    }


def _run_sweep_task(*, config: AppConfig, project_root: Path) -> dict[str, Any]:
    cfg, storage, signal_bars, execution_bars, funding, symbol_slug = _load_symbol_report_inputs(
        cfg=config,
        project_root=project_root,
        symbol=config.instrument.symbol,
    )
    fast_values = _parse_int_list(DEFAULT_SWEEP_FAST)
    slow_values = _parse_int_list(DEFAULT_SWEEP_SLOW)
    atr_values = _parse_float_list(DEFAULT_SWEEP_ATR)
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
    report_prefix = f"{symbol_slug}_{cfg.strategy.name}"
    results_path = storage.report_dir / f"{report_prefix}_sweep.csv"
    dashboard_path = storage.report_dir / f"{report_prefix}_sweep_dashboard.html"
    results.to_csv(results_path, index=False)
    render_sweep_dashboard(
        results=results,
        output_path=dashboard_path,
        title=f"{cfg.instrument.symbol} {cfg.strategy.name}",
    )
    return {
        "task": "sweep",
        "mode": "single",
        "symbols": [cfg.instrument.symbol],
        "artifacts": {
            "sweep_csv": str(results_path),
            "sweep_dashboard": str(dashboard_path),
        },
        "top_rows": results.head(5).to_dict(orient="records"),
    }


def _run_research_task(*, config: AppConfig, project_root: Path) -> dict[str, Any]:
    cfg, storage, signal_bars, execution_bars, funding, symbol_slug = _load_symbol_report_inputs(
        cfg=config,
        project_root=project_root,
        symbol=config.instrument.symbol,
    )
    results = run_trend_research(
        signal_bars=signal_bars,
        execution_bars=execution_bars,
        funding_rates=funding,
        strategy_config=cfg.strategy,
        execution_config=cfg.execution,
        risk_config=cfg.risk,
        instrument_config=cfg.instrument,
        variants=_parse_text_list(DEFAULT_RESEARCH_VARIANTS),
        fast_values=_parse_int_list(DEFAULT_RESEARCH_FAST),
        slow_values=_parse_int_list(DEFAULT_RESEARCH_SLOW),
        atr_values=_parse_float_list(DEFAULT_RESEARCH_ATR),
        trend_ema_values=_parse_int_list(DEFAULT_RESEARCH_TREND_EMA),
        adx_threshold_values=_parse_float_list(DEFAULT_RESEARCH_ADX),
    )
    report_prefix = f"{symbol_slug}_{cfg.strategy.name}_trend_research"
    results_path = storage.report_dir / f"{report_prefix}.csv"
    dashboard_path = storage.report_dir / f"{report_prefix}.html"
    results.to_csv(results_path, index=False)
    render_trend_research_dashboard(
        results=results,
        output_path=dashboard_path,
        title=f"{cfg.instrument.symbol} {cfg.strategy.name} {cfg.strategy.signal_bar} Trend Research",
    )
    return {
        "task": "research",
        "mode": "single",
        "symbols": [cfg.instrument.symbol],
        "artifacts": {
            "research_csv": str(results_path),
            "research_dashboard": str(dashboard_path),
        },
        "top_rows": results.head(10).to_dict(orient="records"),
    }
