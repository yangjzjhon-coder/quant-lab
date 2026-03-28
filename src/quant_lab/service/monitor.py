from __future__ import annotations

import asyncio
import csv
import json
from contextlib import asynccontextmanager
from io import StringIO
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import pandas as pd
from pydantic import BaseModel
from sqlalchemy import desc, select

from quant_lab.alerts.delivery import deliver_alerts
from quant_lab.config import AppConfig, configured_symbols
from quant_lab.service.client_dashboard import render_client_dashboard
from quant_lab.service.database import AlertEvent, ProjectTaskRun, RuntimeSnapshot, ServiceHeartbeat, session_scope
from quant_lab.service.dashboard import render_runtime_dashboard


@dataclass
class MonitorArtifacts:
    snapshot: RuntimeSnapshot
    heartbeat: ServiceHeartbeat
    alerts_sent: list[str]


class AlignLeverageRequest(BaseModel):
    apply: bool = False
    confirm: str = ""
    rearm_protective_stop: bool = False


class AlertTestRequest(BaseModel):
    message: str = "client console test"


class ProjectTaskRequest(BaseModel):
    task: str


class ResearchTaskCreateRequest(BaseModel):
    title: str
    hypothesis: str = ""
    owner_role: str = "research_lead"
    priority: str = "high"
    symbols: list[str] = []
    notes: str = ""


class StrategyCandidateCreateRequest(BaseModel):
    candidate_name: str
    strategy_name: str
    variant: str
    timeframe: str
    symbol_scope: list[str] = []
    config_path: str | None = None
    author_role: str = "strategy_builder"
    thesis: str = ""
    tags: list[str] = []
    task_id: int | None = None
    details: dict[str, Any] = {}


class CandidateEvaluateRequest(BaseModel):
    evaluator_role: str = "backtest_validator"
    evaluation_type: str = "backtest"
    summary_path: str | None = None
    report_path: str | None = None
    trades_path: str | None = None
    equity_curve_path: str | None = None
    notes: str = ""


class CandidateApprovalRequest(BaseModel):
    decider_role: str = "risk_officer"
    decision: str
    scope: str = "demo"
    reason: str = ""


class ResearchMaterializeRequest(BaseModel):
    results_path: str | None = None
    top_n: int = 3
    task_id: int | None = None
    task_title: str | None = None
    owner_role: str = "research_lead"
    author_role: str = "strategy_builder"
    notes: str = ""


class ResearchBacktestCandidateRequest(BaseModel):
    build_report: bool = True
    evaluate: bool = True
    evaluator_role: str = "backtest_validator"
    notes: str = ""


class ResearchPromoteRequest(BaseModel):
    results_path: str | None = None
    top_n: int = 3
    task_id: int | None = None
    task_title: str | None = None
    owner_role: str = "research_lead"
    author_role: str = "strategy_builder"
    evaluator_role: str = "backtest_validator"
    build_report: bool = True
    notes: str = ""


class ResearchAIRunRequest(BaseModel):
    role: str = "research_lead"
    task: str
    context: dict[str, Any] = {}
    system_prompt: str | None = None
    temperature: float | None = None
    max_output_tokens: int | None = None


_PROXY_EGRESS_IP_CACHE: dict[str, tuple[float, str | None]] = {}
_PROXY_EGRESS_IP_TTL_SECONDS = 600.0


def run_monitor_cycle(config: AppConfig, session_factory, project_root: Path) -> MonitorArtifacts:
    report_inputs = _load_report_state(config=config, project_root=project_root)

    alerts_sent: list[str] = []
    with session_scope(session_factory) as session:
        latest_snapshot = session.execute(
            select(RuntimeSnapshot)
            .where(RuntimeSnapshot.symbol == config.instrument.symbol)
            .order_by(desc(RuntimeSnapshot.created_at))
            .limit(1)
        ).scalar_one_or_none()

        snapshot = RuntimeSnapshot(
            symbol=config.instrument.symbol,
            strategy_name=config.strategy.name,
            report_timestamp=report_inputs.report_timestamp,
            report_stale=int(report_inputs.report_stale),
            halted=int(report_inputs.halted),
            latest_equity=report_inputs.latest_equity,
            latest_cash=report_inputs.latest_cash,
            latest_unrealized_pnl=report_inputs.latest_unrealized_pnl,
            total_return_pct=report_inputs.summary["total_return_pct"],
            max_drawdown_pct=report_inputs.summary["max_drawdown_pct"],
            trade_count=int(report_inputs.summary["trade_count"]),
            summary=report_inputs.summary,
        )
        session.add(snapshot)

        heartbeat = ServiceHeartbeat(
            service_name="quant-lab-monitor",
            status="ok" if not report_inputs.report_stale else "stale",
            details={
                "symbol": config.instrument.symbol,
                "strategy_name": config.strategy.name,
                "latest_equity": report_inputs.latest_equity,
                "halted": report_inputs.halted,
                "report_timestamp": report_inputs.report_timestamp.isoformat(),
            },
        )
        session.add(heartbeat)
        session.flush()

        alerts_sent.extend(
            _maybe_send_state_change_alerts(
                session=session,
                config=config,
                latest_snapshot=latest_snapshot,
                current_snapshot=snapshot,
            )
        )

    return MonitorArtifacts(snapshot=snapshot, heartbeat=heartbeat, alerts_sent=alerts_sent)


def build_service_app(config: AppConfig, session_factory, project_root: Path):
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import FileResponse, HTMLResponse

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        monitor_task = asyncio.create_task(
            _monitor_loop(config=config, session_factory=session_factory, project_root=project_root)
        )
        try:
            yield
        finally:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

    app = FastAPI(title="quant-lab service", version="0.1.0", lifespan=lifespan)

    @app.get("/", response_class=HTMLResponse)
    def dashboard() -> HTMLResponse:
        return render_runtime_dashboard(config)

    @app.get("/client", response_class=HTMLResponse)
    def client_dashboard() -> HTMLResponse:
        return render_client_dashboard(config)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "symbol": config.instrument.symbol,
            "strategy": config.strategy.name,
        }

    @app.get("/runtime/latest")
    def runtime_latest() -> dict[str, Any]:
        with session_scope(session_factory) as session:
            snapshot = session.execute(
                select(RuntimeSnapshot).order_by(desc(RuntimeSnapshot.created_at)).limit(1)
            ).scalar_one_or_none()
            if snapshot is None:
                return {"snapshot": None}
            return {"snapshot": _snapshot_to_dict(snapshot)}

    @app.get("/runtime/history")
    def runtime_history(limit: int = 20) -> dict[str, Any]:
        with session_scope(session_factory) as session:
            snapshots = session.execute(
                select(RuntimeSnapshot).order_by(desc(RuntimeSnapshot.created_at)).limit(limit)
            ).scalars()
            return {"snapshots": [_snapshot_to_dict(item) for item in snapshots]}

    @app.get("/runtime/preflight")
    def runtime_preflight() -> dict[str, Any]:
        return build_preflight_payload(config=config, session_factory=session_factory, project_root=project_root)

    @app.get("/alerts")
    def alerts(limit: int = 50) -> dict[str, Any]:
        with session_scope(session_factory) as session:
            items = session.execute(
                select(AlertEvent).order_by(desc(AlertEvent.created_at)).limit(limit)
            ).scalars()
            return {"alerts": [_alert_to_dict(item) for item in items]}

    @app.get("/heartbeats")
    def heartbeats(limit: int = 50) -> dict[str, Any]:
        with session_scope(session_factory) as session:
            items = session.execute(
                select(ServiceHeartbeat).order_by(desc(ServiceHeartbeat.created_at)).limit(limit)
            ).scalars()
            return {"heartbeats": [_heartbeat_to_dict(item) for item in items]}

    @app.get("/project/tasks")
    def project_tasks(limit: int = 20) -> dict[str, Any]:
        from quant_lab.service.project_ops import serialize_project_task_run

        with session_scope(session_factory) as session:
            items = session.execute(
                select(ProjectTaskRun).order_by(desc(ProjectTaskRun.created_at)).limit(limit)
            ).scalars()
            return {"tasks": [serialize_project_task_run(item) for item in items]}

    @app.get("/project/preflight")
    def project_preflight() -> dict[str, Any]:
        from quant_lab.service.project_ops import build_project_task_preflight

        return build_project_task_preflight(config=config, project_root=project_root)

    @app.get("/research/ai/status")
    def research_ai_status(probe: bool = False) -> dict[str, Any]:
        from quant_lab.service.research_ai import build_research_ai_status

        return build_research_ai_status(config=config, probe=probe)

    @app.post("/research/ai/run")
    def research_ai_run(payload: ResearchAIRunRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.research_ai import ResearchAIRequest, run_research_ai_request

        try:
            return run_research_ai_request(
                config=config,
                request=ResearchAIRequest(
                    role=payload.role,
                    task=payload.task,
                    context=payload.context,
                    system_prompt=payload.system_prompt,
                    temperature=payload.temperature,
                    max_output_tokens=payload.max_output_tokens,
                ),
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc

    @app.get("/artifacts")
    def artifacts() -> dict[str, Any]:
        return _artifact_payload_catalog(config=config, project_root=project_root)

    @app.get("/artifacts/open/{file_name:path}")
    def artifact_open(file_name: str) -> FileResponse:
        storage = config.storage.resolved(project_root)
        report_dir = storage.report_dir.resolve()
        candidate = (report_dir / Path(file_name).name).resolve()
        try:
            candidate.relative_to(report_dir)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Artifact path is outside the reports directory.") from exc
        if not candidate.exists() or not candidate.is_file():
            raise HTTPException(status_code=404, detail="Artifact not found.")
        return FileResponse(candidate)

    @app.get("/reports/backtest")
    def report_backtest() -> FileResponse:
        payload = _artifact_payload_catalog(config=config, project_root=project_root)
        report = payload["backtest_report"]
        if not report["exists"]:
            raise HTTPException(status_code=404, detail="Backtest dashboard not found.")
        return FileResponse(report["path"], media_type="text/html; charset=utf-8")

    @app.get("/reports/sweep")
    def report_sweep() -> FileResponse:
        payload = _artifact_payload_catalog(config=config, project_root=project_root)
        report = payload["sweep_report"]
        if not report["exists"]:
            raise HTTPException(status_code=404, detail="Sweep dashboard not found.")
        return FileResponse(report["path"], media_type="text/html; charset=utf-8")

    @app.post("/monitor/run")
    def monitor_run() -> dict[str, Any]:
        artifacts = run_monitor_cycle(config=config, session_factory=session_factory, project_root=project_root)
        return {
            "snapshot": _snapshot_to_dict(artifacts.snapshot),
            "alerts_sent": artifacts.alerts_sent,
        }

    @app.get("/client/snapshot")
    def client_snapshot() -> dict[str, Any]:
        from quant_lab.service.client_ops import build_client_snapshot

        return {"snapshot": build_client_snapshot(config=config, session_factory=session_factory, project_root=project_root)}

    @app.post("/client/reconcile")
    def client_reconcile() -> dict[str, Any]:
        from quant_lab.service.client_ops import build_client_snapshot

        return {"snapshot": build_client_snapshot(config=config, session_factory=session_factory, project_root=project_root)}

    @app.post("/client/align-leverage")
    def client_align_leverage(payload: AlignLeverageRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.client_ops import run_client_align_leverage

        try:
            return run_client_align_leverage(
                config=config,
                session_factory=session_factory,
                project_root=project_root,
                apply=payload.apply,
                confirm=payload.confirm,
                rearm_protective_stop=payload.rearm_protective_stop,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc

    @app.post("/client/alert-test")
    def client_alert_test(payload: AlertTestRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.client_ops import run_client_alert_test

        try:
            return run_client_alert_test(
                config=config,
                session_factory=session_factory,
                message=payload.message,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc

    @app.post("/project/run")
    def project_run(payload: ProjectTaskRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.project_ops import execute_project_task, serialize_project_task_run

        try:
            run, result = execute_project_task(
                config=config,
                session_factory=session_factory,
                project_root=project_root,
                task=payload.task,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc
        return {
            "task_run": serialize_project_task_run(run),
            "result": result,
            "artifacts": _artifact_payload_catalog(config=config, project_root=project_root),
        }

    @app.post("/project/submit")
    def project_submit(payload: ProjectTaskRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.project_ops import serialize_project_task_run, submit_project_task

        try:
            run = submit_project_task(
                config=config,
                session_factory=session_factory,
                project_root=project_root,
                task=payload.task,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc
        return {"task_run": serialize_project_task_run(run)}

    @app.get("/research/overview")
    def research_overview(limit: int = 10) -> dict[str, Any]:
        from quant_lab.service.research_ops import build_research_overview

        return build_research_overview(session_factory=session_factory, limit=limit)

    @app.get("/research/tasks")
    def research_tasks(limit: int = 20, status: str | None = None) -> dict[str, Any]:
        from quant_lab.service.research_ops import list_research_tasks, serialize_research_task

        return {
            "tasks": [
                serialize_research_task(item)
                for item in list_research_tasks(session_factory=session_factory, limit=limit, status=status)
            ]
        }

    @app.post("/research/tasks")
    def research_task_create(payload: ResearchTaskCreateRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.research_ops import create_research_task, serialize_research_task

        try:
            task = create_research_task(
                session_factory=session_factory,
                title=payload.title,
                hypothesis=payload.hypothesis,
                owner_role=payload.owner_role,
                priority=payload.priority,
                symbols=payload.symbols,
                notes=payload.notes,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc
        return {"task": serialize_research_task(task)}

    @app.get("/research/candidates")
    def research_candidates(
        limit: int = 20,
        status: str | None = None,
        approved_only: bool = False,
    ) -> dict[str, Any]:
        from quant_lab.service.research_ops import list_strategy_candidates, serialize_strategy_candidate

        return {
            "candidates": [
                serialize_strategy_candidate(item)
                for item in list_strategy_candidates(
                    session_factory=session_factory,
                    limit=limit,
                    status=status,
                    approved_only=approved_only,
                )
            ]
        }

    @app.post("/research/candidates")
    def research_candidate_create(payload: StrategyCandidateCreateRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.research_ops import register_strategy_candidate, serialize_strategy_candidate

        try:
            candidate = register_strategy_candidate(
                session_factory=session_factory,
                candidate_name=payload.candidate_name,
                strategy_name=payload.strategy_name,
                variant=payload.variant,
                timeframe=payload.timeframe,
                symbol_scope=payload.symbol_scope,
                config_path=payload.config_path,
                author_role=payload.author_role,
                thesis=payload.thesis,
                tags=payload.tags,
                task_id=payload.task_id,
                details=payload.details,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc
        return {"candidate": serialize_strategy_candidate(candidate)}

    @app.post("/research/candidates/{candidate_id}/evaluate")
    def research_candidate_evaluate(candidate_id: int, payload: CandidateEvaluateRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.research_ops import (
            evaluate_strategy_candidate,
            infer_candidate_artifacts,
            infer_strategy_candidate_artifacts_by_id,
            serialize_evaluation_report,
            serialize_strategy_candidate,
        )

        try:
            inferred = infer_strategy_candidate_artifacts_by_id(
                session_factory=session_factory,
                candidate_id=candidate_id,
                project_root=project_root,
            )
        except Exception:
            inferred = infer_candidate_artifacts(config=config, project_root=project_root)
        if payload.summary_path is None and not Path(inferred["summary_path"]).exists():
            inferred = infer_candidate_artifacts(config=config, project_root=project_root)
        summary_path = Path(payload.summary_path or inferred["summary_path"])
        report_path = Path(payload.report_path or inferred["report_path"])
        trades_path = Path(payload.trades_path or inferred["trades_path"])
        equity_curve_path = Path(payload.equity_curve_path or inferred["equity_curve_path"])
        try:
            candidate, report = evaluate_strategy_candidate(
                session_factory=session_factory,
                candidate_id=candidate_id,
                evaluator_role=payload.evaluator_role,
                evaluation_type=payload.evaluation_type,
                summary_path=summary_path,
                report_path=report_path if report_path.exists() else None,
                trades_path=trades_path if trades_path.exists() else None,
                equity_curve_path=equity_curve_path if equity_curve_path.exists() else None,
                notes=payload.notes,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc
        return {
            "candidate": serialize_strategy_candidate(candidate),
            "evaluation_report": serialize_evaluation_report(report),
        }

    @app.post("/research/candidates/{candidate_id}/approve")
    def research_candidate_approve(candidate_id: int, payload: CandidateApprovalRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.research_ops import (
            approve_strategy_candidate,
            serialize_approval_decision,
            serialize_strategy_candidate,
        )

        try:
            candidate, approval = approve_strategy_candidate(
                session_factory=session_factory,
                candidate_id=candidate_id,
                decision=payload.decision,
                decider_role=payload.decider_role,
                scope=payload.scope,
                reason=payload.reason,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc
        return {
            "candidate": serialize_strategy_candidate(candidate),
            "approval": serialize_approval_decision(approval),
        }

    @app.post("/research/candidates/{candidate_id}/backtest")
    def research_candidate_backtest(candidate_id: int, payload: ResearchBacktestCandidateRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.research_ops import backtest_strategy_candidate, evaluate_backtested_candidate

        try:
            if payload.evaluate:
                return evaluate_backtested_candidate(
                    session_factory=session_factory,
                    candidate_id=candidate_id,
                    project_root=project_root,
                    build_report=payload.build_report,
                    evaluator_role=payload.evaluator_role,
                    evaluation_type="backtest",
                    notes=payload.notes,
                )
            return backtest_strategy_candidate(
                session_factory=session_factory,
                candidate_id=candidate_id,
                project_root=project_root,
                build_report=payload.build_report,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc

    @app.post("/research/materialize-top")
    def research_materialize_top(payload: ResearchMaterializeRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.research_ops import materialize_trend_research_candidates

        storage = config.storage.resolved(project_root.resolve())
        symbol_slug = config.instrument.symbol.replace("/", "-")
        inferred_path = storage.report_dir / f"{symbol_slug}_{config.strategy.name}_trend_research.csv"
        resolved_results_path = Path(payload.results_path).expanduser() if payload.results_path else inferred_path
        if not resolved_results_path.is_absolute():
            resolved_results_path = (project_root / resolved_results_path).resolve()
        if not resolved_results_path.exists():
            raise HTTPException(status_code=404, detail=f"Trend research CSV not found: {resolved_results_path}")

        try:
            results = pd.read_csv(resolved_results_path)
            return materialize_trend_research_candidates(
                session_factory=session_factory,
                config=config,
                project_root=project_root,
                base_config_path=project_root / "config" / "settings.yaml",
                results_frame=results,
                results_path=resolved_results_path,
                top_n=payload.top_n,
                task_id=payload.task_id,
                task_title=payload.task_title,
                owner_role=payload.owner_role,
                author_role=payload.author_role,
                notes=payload.notes,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc

    @app.post("/research/promote-top")
    def research_promote_top(payload: ResearchPromoteRequest) -> dict[str, Any]:
        from fastapi import HTTPException

        from quant_lab.service.research_ops import promote_trend_research_candidates

        storage = config.storage.resolved(project_root.resolve())
        symbol_slug = config.instrument.symbol.replace("/", "-")
        inferred_path = storage.report_dir / f"{symbol_slug}_{config.strategy.name}_trend_research.csv"
        resolved_results_path = Path(payload.results_path).expanduser() if payload.results_path else inferred_path
        if not resolved_results_path.is_absolute():
            resolved_results_path = (project_root / resolved_results_path).resolve()
        if not resolved_results_path.exists():
            raise HTTPException(status_code=404, detail=f"Trend research CSV not found: {resolved_results_path}")

        try:
            results = pd.read_csv(resolved_results_path)
            return promote_trend_research_candidates(
                session_factory=session_factory,
                config=config,
                project_root=project_root,
                base_config_path=project_root / "config" / "settings.yaml",
                results_frame=results,
                results_path=resolved_results_path,
                top_n=payload.top_n,
                task_id=payload.task_id,
                task_title=payload.task_title,
                owner_role=payload.owner_role,
                author_role=payload.author_role,
                evaluator_role=payload.evaluator_role,
                notes=payload.notes,
                build_report=payload.build_report,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{type(exc).__name__}: {exc}") from exc

    return app


async def _monitor_loop(config: AppConfig, session_factory, project_root: Path) -> None:
    while True:
        try:
            run_monitor_cycle(config=config, session_factory=session_factory, project_root=project_root)
        except Exception:
            # Keep the background loop alive even if one cycle fails.
            pass
        await asyncio.sleep(config.service.heartbeat_interval_seconds)


def _maybe_send_state_change_alerts(
    session,
    config: AppConfig,
    latest_snapshot: RuntimeSnapshot | None,
    current_snapshot: RuntimeSnapshot,
) -> list[str]:
    sent: list[str] = []

    previous_halted = bool(latest_snapshot.halted) if latest_snapshot is not None else False
    current_halted = bool(current_snapshot.halted)

    if config.alerts.send_on_halt and current_halted and not previous_halted:
        if _deliver_alert(
            session=session,
            config=config,
            event_key="runtime_halted",
            level="warning",
            title="Trading halted",
            message=(
                f"*Trading halted*\n"
                f"Symbol: `{config.instrument.symbol}`\n"
                f"Strategy: `{config.strategy.name}`\n"
                f"Equity: `{current_snapshot.latest_equity:,.2f}`\n"
                f"Max DD: `{current_snapshot.max_drawdown_pct:.2f}%`"
            ),
        ):
            sent.append("runtime_halted")

    if config.alerts.send_on_recovery and previous_halted and not current_halted:
        if _deliver_alert(
            session=session,
            config=config,
            event_key="runtime_recovered",
            level="info",
            title="Trading resumed",
            message=(
                f"*Trading resumed*\n"
                f"Symbol: `{config.instrument.symbol}`\n"
                f"Strategy: `{config.strategy.name}`\n"
                f"Equity: `{current_snapshot.latest_equity:,.2f}`"
            ),
        ):
            sent.append("runtime_recovered")

    previous_stale = bool(latest_snapshot.report_stale) if latest_snapshot is not None else False
    current_stale = bool(current_snapshot.report_stale)
    if config.alerts.send_on_report_stale and current_stale and not previous_stale:
        if _deliver_alert(
            session=session,
            config=config,
            event_key="report_stale",
            level="warning",
            title="Report data is stale",
            message=(
                f"*Report stale*\n"
                f"Symbol: `{config.instrument.symbol}`\n"
                f"Strategy: `{config.strategy.name}`\n"
                f"Latest report timestamp: `{current_snapshot.report_timestamp.isoformat()}`"
            ),
        ):
            sent.append("report_stale")

    return sent


def _deliver_alert(
    session,
    config: AppConfig,
    event_key: str,
    level: str,
    title: str,
    message: str,
) -> bool:
    results = deliver_alerts(config.alerts, title=title, message=message)
    any_delivered = False
    for result in results:
        if result.delivered:
            any_delivered = True
        session.add(
            AlertEvent(
                event_key=event_key,
                channel=result.channel,
                level=level,
                title=title,
                message=message if result.error is None else f"{message}\n\nerror: {result.error}",
                status=result.status,
                delivered_at=result.delivered_at,
            )
        )
    return any_delivered


@dataclass
class _ReportState:
    summary: dict[str, Any]
    report_timestamp: datetime
    report_stale: bool
    halted: bool
    latest_equity: float
    latest_cash: float
    latest_unrealized_pnl: float


def _load_report_state(config: AppConfig, project_root: Path) -> _ReportState:
    storage = config.storage.resolved(project_root)
    report_prefix = _primary_report_prefix(config)
    summary_path = storage.report_dir / f"{report_prefix}_summary.json"
    equity_path = storage.report_dir / f"{report_prefix}_equity_curve.csv"

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    latest = _read_last_equity_row(equity_path)
    report_timestamp = pd.Timestamp(latest["timestamp"]).to_pydatetime()
    if report_timestamp.tzinfo is None:
        report_timestamp = report_timestamp.replace(tzinfo=timezone.utc)
    artifact_mtime = max(summary_path.stat().st_mtime, equity_path.stat().st_mtime)
    artifact_timestamp = datetime.fromtimestamp(artifact_mtime, tz=timezone.utc)
    age_minutes = (datetime.now(timezone.utc) - artifact_timestamp).total_seconds() / 60

    return _ReportState(
        summary=summary,
        report_timestamp=report_timestamp,
        report_stale=age_minutes > config.service.report_stale_minutes,
        halted=_parse_bool(latest["halted"]),
        latest_equity=float(latest["equity"]),
        latest_cash=float(latest["cash"]),
        latest_unrealized_pnl=float(latest["unrealized_pnl"]),
    )


def _read_last_equity_row(equity_path: Path) -> dict[str, str]:
    with equity_path.open("r", encoding="utf-8", newline="") as file:
        header = file.readline().strip()

    with equity_path.open("rb") as file:
        file.seek(0, 2)
        position = file.tell()
        chunk = b""
        while position > 0:
            read_size = min(4096, position)
            position -= read_size
            file.seek(position)
            chunk = file.read(read_size) + chunk
            if chunk.count(b"\n") >= 2:
                break

    lines = [line.decode("utf-8").strip() for line in chunk.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"No equity rows found in {equity_path}")

    last_line = lines[-1]
    reader = csv.DictReader(StringIO(f"{header}\n{last_line}\n"))
    row = next(reader)
    if row is None:
        raise RuntimeError(f"Unable to parse latest equity row from {equity_path}")
    return row


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _snapshot_to_dict(snapshot: RuntimeSnapshot) -> dict[str, Any]:
    return {
        "id": snapshot.id,
        "symbol": snapshot.symbol,
        "strategy_name": snapshot.strategy_name,
        "report_timestamp": _serialize_datetime(snapshot.report_timestamp),
        "report_stale": bool(snapshot.report_stale),
        "halted": bool(snapshot.halted),
        "latest_equity": snapshot.latest_equity,
        "latest_cash": snapshot.latest_cash,
        "latest_unrealized_pnl": snapshot.latest_unrealized_pnl,
        "total_return_pct": snapshot.total_return_pct,
        "max_drawdown_pct": snapshot.max_drawdown_pct,
        "trade_count": snapshot.trade_count,
        "summary": snapshot.summary,
        "created_at": _serialize_datetime(snapshot.created_at),
    }


def _alert_to_dict(alert: AlertEvent) -> dict[str, Any]:
    return {
        "id": alert.id,
        "event_key": alert.event_key,
        "channel": alert.channel,
        "level": alert.level,
        "title": alert.title,
        "message": alert.message,
        "status": alert.status,
        "delivered_at": _serialize_datetime(alert.delivered_at),
        "created_at": _serialize_datetime(alert.created_at),
    }


def _heartbeat_to_dict(heartbeat: ServiceHeartbeat) -> dict[str, Any]:
    return {
        "id": heartbeat.id,
        "service_name": heartbeat.service_name,
        "status": heartbeat.status,
        "details": heartbeat.details,
        "created_at": _serialize_datetime(heartbeat.created_at),
    }


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _artifact_payload_legacy(config: AppConfig, project_root: Path) -> dict[str, Any]:
    storage = config.storage.resolved(project_root)
    symbol_slug = config.instrument.symbol.replace("/", "-")
    report_prefix = f"{symbol_slug}_{config.strategy.name}"
    report_dir = storage.report_dir
    return {
        "backtest_report": _artifact_meta(
            label="回测 HTML 报表",
            path=report_dir / f"{report_prefix}_dashboard.html",
            url="/reports/backtest",
        ),
        "sweep_report": _artifact_meta(
            label="参数扫描 HTML 报表",
            path=report_dir / f"{report_prefix}_sweep_dashboard.html",
            url="/reports/sweep",
        ),
        "summary": _artifact_meta(
            label="回测汇总 JSON",
            path=report_dir / f"{report_prefix}_summary.json",
        ),
        "equity_curve": _artifact_meta(
            label="净值曲线 CSV",
            path=report_dir / f"{report_prefix}_equity_curve.csv",
        ),
        "trades": _artifact_meta(
            label="成交明细 CSV",
            path=report_dir / f"{report_prefix}_trades.csv",
        ),
        "sweep_csv": _artifact_meta(
            label="参数扫描 CSV",
            path=report_dir / f"{report_prefix}_sweep.csv",
        ),
    }


def _artifact_meta(label: str, path: Path, url: str | None = None) -> dict[str, Any]:
    return {
        "label": label,
        "path": str(path),
        "exists": path.exists(),
        "url": url,
    }


def _artifact_open_url(path: Path) -> str:
    return f"/artifacts/open/{quote(path.name)}"


def _safe_json_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _summary_metrics(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "initial_equity": summary.get("initial_equity"),
        "final_equity": summary.get("final_equity"),
        "total_return_pct": summary.get("total_return_pct"),
        "annualized_return_pct": summary.get("annualized_return_pct"),
        "max_drawdown_pct": summary.get("max_drawdown_pct"),
        "trade_count": summary.get("trade_count"),
        "win_rate_pct": summary.get("win_rate_pct"),
        "profit_factor": summary.get("profit_factor"),
        "sharpe": summary.get("sharpe"),
        "capital_allocation_pct": summary.get("capital_allocation_pct"),
        "symbol_count": summary.get("symbol_count"),
    }


def _sleeve_report_payloads(config: AppConfig, report_dir: Path, symbols: list[str]) -> list[dict[str, Any]]:
    sleeves: list[dict[str, Any]] = []
    for symbol in symbols:
        prefix = f"{symbol.replace('/', '-')}_{config.strategy.name}_sleeve"
        summary_path = report_dir / f"{prefix}_summary.json"
        dashboard_path = report_dir / f"{prefix}_dashboard.html"
        equity_path = report_dir / f"{prefix}_equity_curve.csv"
        trades_path = report_dir / f"{prefix}_trades.csv"
        summary = _safe_json_payload(summary_path)
        sleeves.append(
            {
                "symbol": symbol,
                "label": f"{symbol} Sleeve",
                "metrics": _summary_metrics(summary),
                "dashboard": _artifact_meta(
                    label="Sleeve HTML",
                    path=dashboard_path,
                    url=_artifact_open_url(dashboard_path),
                ),
                "summary_file": _artifact_meta(
                    label="Sleeve Summary JSON",
                    path=summary_path,
                    url=_artifact_open_url(summary_path),
                ),
                "equity_curve": _artifact_meta(
                    label="Sleeve Equity Curve CSV",
                    path=equity_path,
                    url=_artifact_open_url(equity_path),
                ),
                "trades": _artifact_meta(
                    label="Sleeve Trades CSV",
                    path=trades_path,
                    url=_artifact_open_url(trades_path),
                ),
            }
        )
    return sleeves


def _artifact_catalog(report_dir: Path, limit: int = 60) -> list[dict[str, Any]]:
    if not report_dir.exists():
        return []

    allowed_suffixes = {".html", ".json", ".csv", ".md", ".xlsx"}
    items: list[dict[str, Any]] = []
    for path in sorted(report_dir.iterdir(), key=lambda item: item.stat().st_mtime, reverse=True):
        if not path.is_file() or path.suffix.lower() not in allowed_suffixes:
            continue
        stat = path.stat()
        items.append(
            {
                "name": path.name,
                "label": path.name,
                "path": str(path),
                "exists": True,
                "url": f"/artifacts/open/{quote(path.name)}",
                "category": _artifact_category(path),
                "group_label": _artifact_group_label(path),
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            }
        )
        if len(items) >= limit:
            break
    return items


def _artifact_category(path: Path) -> str:
    name = path.name.lower()
    suffix = path.suffix.lower()
    if suffix == ".html":
        if "sweep" in name:
            return "sweep_dashboard"
        if "trend_research" in name:
            return "research_dashboard"
        if "portfolio" in name:
            return "portfolio_dashboard"
        return "backtest_dashboard"
    if suffix == ".json":
        return "summary_json" if name.endswith("_summary.json") else "json"
    if suffix == ".csv":
        if "trades" in name:
            return "trades_csv"
        if "sweep" in name:
            return "sweep_csv"
        if "trend_research" in name:
            return "research_csv"
        return "csv"
    if suffix == ".xlsx":
        return "spreadsheet"
    if suffix == ".md":
        return "notes"
    return "artifact"


def _artifact_group_label(path: Path) -> str:
    mapping = {
        "backtest_dashboard": "Backtest HTML",
        "portfolio_dashboard": "Portfolio HTML",
        "research_dashboard": "Research HTML",
        "sweep_dashboard": "Sweep HTML",
        "summary_json": "Summary JSON",
        "trades_csv": "Trades CSV",
        "sweep_csv": "Sweep CSV",
        "research_csv": "Research CSV",
        "spreadsheet": "Excel",
        "notes": "Notes",
        "csv": "CSV",
        "json": "JSON",
    }
    return mapping.get(_artifact_category(path), "Artifact")


def _artifact_payload_catalog(config: AppConfig, project_root: Path) -> dict[str, Any]:
    storage = config.storage.resolved(project_root)
    report_prefix = _primary_report_prefix(config)
    sweep_prefix = _primary_sweep_prefix(config)
    report_dir = storage.report_dir
    symbols = configured_symbols(config)
    portfolio_mode = len(symbols) > 1
    summary_path = report_dir / f"{report_prefix}_summary.json"
    sleeve_reports = _sleeve_report_payloads(config, report_dir, symbols) if portfolio_mode else []
    return {
        "mode": "portfolio" if portfolio_mode else "single",
        "symbols": symbols,
        "backtest_report": _artifact_meta(
            label="Portfolio HTML" if portfolio_mode else "Backtest HTML",
            path=report_dir / f"{report_prefix}_dashboard.html",
            url="/reports/backtest",
        ),
        "sweep_report": _artifact_meta(
            label="Sweep HTML",
            path=report_dir / f"{sweep_prefix}_sweep_dashboard.html",
            url="/reports/sweep",
        ),
        "summary": _artifact_meta(
            label="Portfolio Summary JSON" if portfolio_mode else "Summary JSON",
            path=summary_path,
        ),
        "equity_curve": _artifact_meta(
            label="Portfolio Equity Curve CSV" if portfolio_mode else "Equity Curve CSV",
            path=report_dir / f"{report_prefix}_equity_curve.csv",
        ),
        "trades": _artifact_meta(
            label="Portfolio Trades CSV" if portfolio_mode else "Trades CSV",
            path=report_dir / f"{report_prefix}_trades.csv",
        ),
        "sweep_csv": _artifact_meta(
            label="Sweep CSV",
            path=report_dir / f"{sweep_prefix}_sweep.csv",
        ),
        "summary_metrics": _summary_metrics(_safe_json_payload(summary_path)),
        "sleeve_reports": sleeve_reports,
        "catalog": _artifact_catalog(report_dir),
    }


def _primary_report_prefix(config: AppConfig) -> str:
    symbols = configured_symbols(config)
    if len(symbols) == 1:
        return f"{symbols[0].replace('/', '-')}_{config.strategy.name}"
    base_assets = "_".join(symbol.split("-")[0].lower() for symbol in symbols)
    return f"portfolio_{base_assets}_{config.strategy.name}"


def _primary_sweep_prefix(config: AppConfig) -> str:
    return f"{config.instrument.symbol.replace('/', '-')}_{config.strategy.name}"


def build_preflight_payload(config: AppConfig, session_factory, project_root: Path) -> dict[str, Any]:
    from quant_lab.execution.strategy_router import build_strategy_router_status
    from quant_lab.service.research_ops import resolve_execution_approval

    latest_demo_heartbeat = None
    with session_scope(session_factory) as session:
        latest_demo_heartbeat = session.execute(
            select(ServiceHeartbeat)
            .where(ServiceHeartbeat.service_name == "quant-lab-demo-loop")
            .order_by(desc(ServiceHeartbeat.created_at))
            .limit(1)
        ).scalar_one_or_none()

    executor_state = _load_executor_state(config=config, project_root=project_root)
    executor_state = _normalize_executor_state_payload(
        executor_state=executor_state,
        latest_demo_heartbeat=latest_demo_heartbeat,
    )
    execution_approval = resolve_execution_approval(
        session_factory=session_factory,
        config=config,
        required_scope="demo",
    )
    strategy_router = build_strategy_router_status(
        session_factory=session_factory,
        config=config,
        required_scope="demo",
    )

    demo_checks = {
        "use_demo": bool(config.okx.use_demo),
        "allow_order_placement": bool(config.trading.allow_order_placement),
        "api_key": bool(config.okx.api_key),
        "secret_key": bool(config.okx.secret_key),
        "passphrase": bool(config.okx.passphrase),
        "approved_candidate_gate": bool(execution_approval["ready"]),
    }
    demo_reasons: list[str] = []
    if not demo_checks["use_demo"]:
        demo_reasons.append("okx.use_demo=false")
    if not demo_checks["allow_order_placement"]:
        demo_reasons.append("trading.allow_order_placement=false")
    if not demo_checks["api_key"]:
        demo_reasons.append("missing OKX_API_KEY")
    if not demo_checks["secret_key"]:
        demo_reasons.append("missing OKX_SECRET_KEY")
    if not demo_checks["passphrase"]:
        demo_reasons.append("missing OKX_PASSPHRASE")
    if not demo_checks["approved_candidate_gate"]:
        for reason in execution_approval.get("reasons") or []:
            demo_reasons.append(f"execution approval: {reason}")

    demo_ready = all(demo_checks.values())
    demo_mode = "submit_ready" if demo_ready else "plan_only"
    if any((demo_checks["use_demo"], demo_checks["allow_order_placement"])) and not demo_ready:
        demo_mode = "submit_blocked"

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

    return {
        "demo_trading": {
            "mode": demo_mode,
            "ready": demo_ready,
            "checks": demo_checks,
            "reasons": demo_reasons,
        },
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
        "okx_connectivity": _build_okx_connectivity_payload(config=config, latest_demo_heartbeat=latest_demo_heartbeat),
        "execution_loop": {
            "latest_heartbeat": _heartbeat_to_dict(latest_demo_heartbeat) if latest_demo_heartbeat is not None else None,
            "executor_state": executor_state,
        },
        "execution_approval": execution_approval,
        "strategy_router": strategy_router,
    }


def _normalize_executor_state_payload(
    *,
    executor_state: dict[str, Any] | None,
    latest_demo_heartbeat: ServiceHeartbeat | None,
) -> dict[str, Any] | None:
    if not isinstance(executor_state, dict):
        return executor_state
    last_error = executor_state.get("last_error")
    if not isinstance(last_error, dict) or latest_demo_heartbeat is None:
        return executor_state
    if latest_demo_heartbeat.status == "error":
        return executor_state

    error_timestamp_raw = last_error.get("timestamp")
    if not error_timestamp_raw:
        return executor_state
    try:
        error_timestamp = datetime.fromisoformat(str(error_timestamp_raw).replace("Z", "+00:00"))
    except ValueError:
        return executor_state

    heartbeat_created_at = latest_demo_heartbeat.created_at
    if heartbeat_created_at is None:
        return executor_state
    if heartbeat_created_at.tzinfo is None:
        heartbeat_created_at = heartbeat_created_at.replace(tzinfo=timezone.utc)
    if error_timestamp.tzinfo is None:
        error_timestamp = error_timestamp.replace(tzinfo=timezone.utc)
    if heartbeat_created_at < error_timestamp:
        return executor_state

    payload = dict(executor_state)
    payload["recovered_error"] = last_error
    payload["last_error"] = None
    return payload


def _build_okx_connectivity_payload(
    *,
    config: AppConfig,
    latest_demo_heartbeat: ServiceHeartbeat | None,
) -> dict[str, Any]:
    heartbeat_details = latest_demo_heartbeat.details if latest_demo_heartbeat is not None else {}
    heartbeat_details = heartbeat_details if isinstance(heartbeat_details, dict) else {}
    latest_auth_error = heartbeat_details.get("error")
    egress_ip = _resolve_proxy_egress_ip(config.okx.proxy_url)
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


def _resolve_proxy_egress_ip(proxy_url: str | None) -> str | None:
    if not proxy_url:
        return None
    now = datetime.now(timezone.utc).timestamp()
    cached = _PROXY_EGRESS_IP_CACHE.get(proxy_url)
    if cached and cached[0] > now:
        return cached[1]

    egress_ip: str | None = None
    try:
        with httpx.Client(proxy=proxy_url, timeout=8.0) as client:
            response = client.get("https://api.ipify.org")
            response.raise_for_status()
            value = response.text.strip()
            egress_ip = value or None
    except Exception:
        egress_ip = None

    _PROXY_EGRESS_IP_CACHE[proxy_url] = (now + _PROXY_EGRESS_IP_TTL_SECONDS, egress_ip)
    return egress_ip


def _load_executor_state(config: AppConfig, project_root: Path) -> dict[str, Any] | None:
    storage = config.storage.resolved(project_root)
    state_path = storage.data_dir / "demo_executor_state.json"
    if not state_path.exists():
        return None
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"path": str(state_path), "status": "invalid_json"}
    return {
        "path": str(state_path),
        "last_submitted_at": payload.get("last_submitted_at"),
        "last_submitted_signature": payload.get("last_submitted_signature"),
        "last_error": payload.get("last_error"),
        "last_plan": payload.get("last_plan"),
        "last_signal": payload.get("last_signal"),
        "symbols": payload.get("symbols"),
    }
