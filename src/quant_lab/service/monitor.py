from __future__ import annotations

import asyncio
import csv
import html as html_lib
import json
from contextlib import asynccontextmanager
from io import StringIO
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pandas as pd
from pydantic import BaseModel
from sqlalchemy import desc, select

from quant_lab.alerts.delivery import deliver_alerts
from quant_lab.errors import (
    InvalidRequestError,
    NotFoundError,
    QuantLabError,
    ServiceOperationError,
    normalize_error,
)
from quant_lab.logging_utils import configure_logging, get_logger
from quant_lab.application.project_tasks import resolve_project_research_results_path
from quant_lab.artifacts import (
    artifact_resolution_path,
    primary_report_prefix as artifact_primary_report_prefix,
    backtest_artifact_resolution as resolve_backtest_artifact_group,
    resolve_artifact_open_path,
    sleeve_report_prefix as artifact_sleeve_report_prefix,
    sleeve_backtest_artifact_resolution as resolve_sleeve_backtest_artifact_group,
    symbol_slug,
    sweep_prefix as artifact_sweep_prefix,
    sweep_artifact_resolution as resolve_sweep_artifact_group,
)
from quant_lab.config import AppConfig, configured_symbols
from quant_lab.service.client_dashboard import render_client_dashboard
from quant_lab.service.database import AlertEvent, ProjectTaskRun, RuntimeSnapshot, ServiceHeartbeat, session_scope
from quant_lab.service.dashboard import render_runtime_dashboard
from quant_lab.service.demo_runtime import (
    build_preflight_payload as runtime_build_preflight_payload,
    heartbeat_service_name,
    resolve_proxy_egress_ip as runtime_resolve_proxy_egress_ip,
    serialize_alert_event,
    serialize_service_heartbeat,
)
from quant_lab.service.serialization import serialize_utc_datetime

LOGGER = get_logger(__name__)


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


class ResearchAgentRunRequest(BaseModel):
    role: str = "research_lead"
    task: str
    title: str | None = None
    hypothesis: str = ""
    symbols: list[str] = []
    context: dict[str, Any] = {}
    task_id: int | None = None
    create_task: bool = True
    register_candidate: bool = True
    owner_role: str = "research_lead"
    author_role: str = "strategy_builder"
    priority: str = "high"
    notes: str = ""
    candidate_name: str | None = None
    strategy_name: str | None = None
    variant: str | None = None
    timeframe: str | None = None
    thesis: str | None = None
    tags: list[str] = []


def _raise_api_error(exc: Exception) -> None:
    raise normalize_error(exc) from exc


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
                "report_timestamp": serialize_utc_datetime(report_inputs.report_timestamp),
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
    from fastapi import FastAPI, Request
    from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

    configure_logging(project_root=project_root.resolve())
    no_cache_headers = {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
    }

    def _apply_no_cache(response: HTMLResponse | FileResponse | JSONResponse):
        response.headers.update(no_cache_headers)
        return response

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

    @app.exception_handler(QuantLabError)
    async def _handle_quant_lab_error(request: Request, exc: QuantLabError) -> JSONResponse:
        LOGGER.warning(
            "service api handled error method=%s path=%s code=%s status=%s detail=%s",
            request.method,
            request.url.path,
            exc.error_code,
            exc.status_code,
            exc.detail,
        )
        return JSONResponse(status_code=exc.status_code, content=exc.to_payload())

    @app.exception_handler(Exception)
    async def _handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception(
            "service api unexpected error method=%s path=%s",
            request.method,
            request.url.path,
        )
        payload = ServiceOperationError(
            "Internal server error.",
            error_code="internal_server_error",
        ).to_payload()
        return JSONResponse(status_code=500, content=payload)

    @app.get("/", response_class=HTMLResponse)
    def dashboard() -> HTMLResponse:
        artifacts = _artifact_payload_catalog(config=config, project_root=project_root)
        visual_note, visual_html = _render_initial_visual_reports(artifacts)
        sleeves_html = _render_initial_portfolio_sleeves(artifacts)
        return _apply_no_cache(
            render_runtime_dashboard(
                config,
                initial_visual_reports_note=visual_note,
                initial_visual_reports_html=visual_html,
                initial_portfolio_sleeves_html=sleeves_html,
            )
        )

    @app.get("/client", response_class=HTMLResponse)
    def client_dashboard() -> HTMLResponse:
        artifacts = _artifact_payload_catalog(config=config, project_root=project_root)
        visual_note, visual_html = _render_initial_visual_reports(artifacts)
        return _apply_no_cache(
            render_client_dashboard(
                config,
                initial_visual_reports_note=visual_note,
                initial_visual_reports_html=visual_html,
            )
        )

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
            return {"alerts": [serialize_alert_event(item) for item in items]}

    @app.get("/heartbeats")
    def heartbeats(limit: int = 50) -> dict[str, Any]:
        with session_scope(session_factory) as session:
            items = session.execute(
                select(ServiceHeartbeat).order_by(desc(ServiceHeartbeat.created_at)).limit(limit)
            ).scalars()
            return {"heartbeats": [serialize_service_heartbeat(item, include_status_label=True) for item in items]}

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

    @app.get("/market-data/status")
    def market_data_status(probe: bool = False) -> dict[str, Any]:
        from quant_lab.service.market_data import build_market_data_status

        return build_market_data_status(config=config, probe=probe)

    @app.get("/integrations/overview")
    def integrations_overview(probe: bool = False) -> dict[str, Any]:
        from quant_lab.service.integrations import build_integration_overview

        return build_integration_overview(config=config, probe=probe)

    @app.get("/research/ai/status")
    def research_ai_status(probe: bool = False) -> dict[str, Any]:
        from quant_lab.service.research_ai import build_research_ai_status

        return build_research_ai_status(config=config, probe=probe)

    @app.post("/research/ai/run")
    def research_ai_run(payload: ResearchAIRunRequest) -> dict[str, Any]:
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
            _raise_api_error(exc)

    @app.get("/research/agent/status")
    def research_agent_status(probe: bool = False) -> dict[str, Any]:
        from quant_lab.service.research_agent import build_research_agent_status

        return build_research_agent_status(config=config, probe=probe)

    @app.post("/research/agent/run")
    def research_agent_run(payload: ResearchAgentRunRequest) -> dict[str, Any]:
        from quant_lab.service.research_agent import ResearchAgentRequest, run_research_agent_workflow

        try:
            return run_research_agent_workflow(
                config=config,
                session_factory=session_factory,
                request=ResearchAgentRequest(
                    role=payload.role,
                    task=payload.task,
                    title=payload.title,
                    hypothesis=payload.hypothesis,
                    symbols=payload.symbols,
                    context=payload.context,
                    task_id=payload.task_id,
                    create_task=payload.create_task,
                    register_candidate=payload.register_candidate,
                    owner_role=payload.owner_role,
                    author_role=payload.author_role,
                    priority=payload.priority,
                    notes=payload.notes,
                    candidate_name=payload.candidate_name,
                    strategy_name=payload.strategy_name,
                    variant=payload.variant,
                    timeframe=payload.timeframe,
                    thesis=payload.thesis,
                    tags=payload.tags,
                ),
            )
        except Exception as exc:
            _raise_api_error(exc)

    @app.get("/artifacts")
    def artifacts() -> dict[str, Any]:
        return _artifact_payload_catalog(config=config, project_root=project_root)

    @app.get("/artifacts/open/{file_name:path}")
    def artifact_open(file_name: str) -> FileResponse:
        storage = config.storage.resolved(project_root)
        report_dir = storage.report_dir.resolve()
        candidate = resolve_artifact_open_path(report_dir, file_name)
        if candidate is None:
            raise NotFoundError("Artifact not found.", error_code="artifact_not_found")
        try:
            candidate.relative_to(report_dir)
        except ValueError as exc:
            raise InvalidRequestError(
                "Artifact path is outside the reports directory.",
                error_code="artifact_path_outside_reports_dir",
            ) from exc
        if not candidate.exists() or not candidate.is_file():
            raise NotFoundError("Artifact not found.", error_code="artifact_not_found")
        return _apply_no_cache(FileResponse(candidate))

    @app.get("/reports/backtest")
    def report_backtest() -> FileResponse:
        payload = _artifact_payload_catalog(config=config, project_root=project_root)
        report = payload["backtest_report"]
        if not report["exists"]:
            raise NotFoundError(
                "未找到回测报表页面。",
                error_code="backtest_dashboard_not_found",
            )
        return _apply_no_cache(FileResponse(report["path"], media_type="text/html; charset=utf-8"))

    @app.get("/reports/sweep")
    def report_sweep() -> FileResponse:
        payload = _artifact_payload_catalog(config=config, project_root=project_root)
        report = payload["sweep_report"]
        if not report["exists"]:
            raise NotFoundError(
                "Sweep dashboard not found.",
                error_code="sweep_dashboard_not_found",
            )
        return _apply_no_cache(FileResponse(report["path"], media_type="text/html; charset=utf-8"))

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
            _raise_api_error(exc)

    @app.post("/client/alert-test")
    def client_alert_test(payload: AlertTestRequest) -> dict[str, Any]:
        from quant_lab.service.client_ops import run_client_alert_test

        try:
            return run_client_alert_test(
                config=config,
                session_factory=session_factory,
                message=payload.message,
            )
        except Exception as exc:
            _raise_api_error(exc)

    @app.post("/project/run")
    def project_run(payload: ProjectTaskRequest) -> dict[str, Any]:
        from quant_lab.service.project_ops import execute_project_task, serialize_project_task_run

        try:
            run, result = execute_project_task(
                config=config,
                session_factory=session_factory,
                project_root=project_root,
                task=payload.task,
            )
        except Exception as exc:
            _raise_api_error(exc)
        return {
            "task_run": serialize_project_task_run(run),
            "result": result,
            "artifacts": _artifact_payload_catalog(config=config, project_root=project_root),
        }

    @app.post("/project/submit")
    def project_submit(payload: ProjectTaskRequest) -> dict[str, Any]:
        from quant_lab.service.project_ops import serialize_project_task_run, submit_project_task

        try:
            run = submit_project_task(
                config=config,
                session_factory=session_factory,
                project_root=project_root,
                task=payload.task,
            )
        except Exception as exc:
            _raise_api_error(exc)
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
            _raise_api_error(exc)
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
            _raise_api_error(exc)
        return {"candidate": serialize_strategy_candidate(candidate)}

    @app.post("/research/candidates/{candidate_id}/evaluate")
    def research_candidate_evaluate(candidate_id: int, payload: CandidateEvaluateRequest) -> dict[str, Any]:
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
                artifact_payload_source=inferred,
            )
        except Exception as exc:
            _raise_api_error(exc)
        return {
            "candidate": serialize_strategy_candidate(candidate),
            "evaluation_report": serialize_evaluation_report(report),
        }

    @app.post("/research/candidates/{candidate_id}/approve")
    def research_candidate_approve(candidate_id: int, payload: CandidateApprovalRequest) -> dict[str, Any]:
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
            _raise_api_error(exc)
        return {
            "candidate": serialize_strategy_candidate(candidate),
            "approval": serialize_approval_decision(approval),
        }

    @app.post("/research/candidates/{candidate_id}/backtest")
    def research_candidate_backtest(candidate_id: int, payload: ResearchBacktestCandidateRequest) -> dict[str, Any]:
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
            _raise_api_error(exc)

    @app.post("/research/materialize-top")
    def research_materialize_top(payload: ResearchMaterializeRequest) -> dict[str, Any]:
        from quant_lab.service.research_ops import materialize_trend_research_candidates

        resolved_results_path = resolve_project_research_results_path(
            config=config,
            project_root=project_root,
            results_path=Path(payload.results_path) if payload.results_path else None,
        )
        if not resolved_results_path.exists():
            raise NotFoundError(
                f"Trend research CSV not found: {resolved_results_path}",
                error_code="trend_research_csv_not_found",
            )

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
            _raise_api_error(exc)

    @app.post("/research/promote-top")
    def research_promote_top(payload: ResearchPromoteRequest) -> dict[str, Any]:
        from quant_lab.service.research_ops import promote_trend_research_candidates

        resolved_results_path = resolve_project_research_results_path(
            config=config,
            project_root=project_root,
            results_path=Path(payload.results_path) if payload.results_path else None,
        )
        if not resolved_results_path.exists():
            raise NotFoundError(
                f"Trend research CSV not found: {resolved_results_path}",
                error_code="trend_research_csv_not_found",
            )

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
            _raise_api_error(exc)

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
                f"Latest report timestamp: `{serialize_utc_datetime(current_snapshot.report_timestamp)}`"
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
    resolution = _backtest_artifact_resolution(config=config, project_root=project_root)
    summary_path = _artifact_resolution_path(
        resolution,
        "summary",
        config.storage.resolved(project_root).report_dir / f"{artifact_primary_report_prefix(config)}_summary.json",
    )
    equity_path = _artifact_resolution_path(
        resolution,
        "equity_curve",
        config.storage.resolved(project_root).report_dir / f"{artifact_primary_report_prefix(config)}_equity_curve.csv",
    )

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


def _backtest_artifact_resolution(config: AppConfig, project_root: Path) -> dict[str, Any]:
    _, resolution = resolve_backtest_artifact_group(config=config, project_root=project_root)
    return resolution


def _sleeve_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    portfolio_symbols: list[str],
    symbol: str,
) -> dict[str, Any]:
    _, resolution = resolve_sleeve_backtest_artifact_group(
        config=config,
        project_root=project_root,
        portfolio_symbols=portfolio_symbols,
        symbol=symbol,
    )
    return resolution


def _sweep_artifact_resolution(config: AppConfig, project_root: Path) -> dict[str, Any]:
    _, resolution = resolve_sweep_artifact_group(config=config, project_root=project_root)
    return resolution


def _snapshot_to_dict(snapshot: RuntimeSnapshot) -> dict[str, Any]:
    return {
        "id": snapshot.id,
        "symbol": snapshot.symbol,
        "strategy_name": snapshot.strategy_name,
        "report_timestamp": serialize_utc_datetime(snapshot.report_timestamp),
        "report_stale": bool(snapshot.report_stale),
        "halted": bool(snapshot.halted),
        "latest_equity": snapshot.latest_equity,
        "latest_cash": snapshot.latest_cash,
        "latest_unrealized_pnl": snapshot.latest_unrealized_pnl,
        "total_return_pct": snapshot.total_return_pct,
        "max_drawdown_pct": snapshot.max_drawdown_pct,
        "trade_count": snapshot.trade_count,
        "summary": snapshot.summary,
        "created_at": serialize_utc_datetime(snapshot.created_at),
    }


def _artifact_payload_legacy(config: AppConfig, project_root: Path) -> dict[str, Any]:
    storage = config.storage.resolved(project_root)
    symbol_slug = config.instrument.symbol.replace("/", "-")
    report_prefix = f"{symbol_slug}_{config.strategy.name}"
    report_dir = storage.report_dir
    return {
        "backtest_report": _artifact_meta(
            label="回测网页报表",
            path=report_dir / f"{report_prefix}_dashboard.html",
            url="/reports/backtest",
        ),
        "sweep_report": _artifact_meta(
            label="参数扫描网页报表",
            path=report_dir / f"{report_prefix}_sweep_dashboard.html",
            url="/reports/sweep",
        ),
        "summary": _artifact_meta(
            label="回测摘要文件",
            path=report_dir / f"{report_prefix}_summary.json",
        ),
        "equity_curve": _artifact_meta(
            label="净值曲线",
            path=report_dir / f"{report_prefix}_equity_curve.csv",
        ),
        "trades": _artifact_meta(
            label="成交明细",
            path=report_dir / f"{report_prefix}_trades.csv",
        ),
        "sweep_csv": _artifact_meta(
            label="参数扫描结果表",
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


def _artifact_resolution_path(resolution: dict[str, Any], key: str, fallback: Path) -> Path:
    return artifact_resolution_path(resolution, key, fallback)


def _artifact_meta_from_resolution(
    *,
    label: str,
    key: str,
    resolution: dict[str, Any],
    fallback: Path,
    url: str | None = None,
) -> dict[str, Any]:
    path = _artifact_resolution_path(resolution, key, fallback)
    payload = _artifact_meta(label=label, path=path, url=url)
    payload["resolved_via"] = resolution.get("resolved_via")
    payload["logical_prefix"] = resolution.get("logical_prefix")
    payload["artifact_fingerprint"] = resolution.get("artifact_fingerprint")
    payload["canonical_path"] = str(path)
    return payload


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
        "allocation_mode": summary.get("allocation_mode"),
        "portfolio_construction": summary.get("portfolio_construction"),
        "runtime_allocation_reference": summary.get("runtime_allocation_reference"),
        "historical_allocation_overlay": summary.get("historical_allocation_overlay"),
        "historical_allocated_risk_pct_avg": summary.get("historical_allocated_risk_pct_avg"),
    }


def _sleeve_report_payloads(config: AppConfig, report_dir: Path, symbols: list[str]) -> list[dict[str, Any]]:
    sleeves: list[dict[str, Any]] = []
    for symbol in symbols:
        resolution = _sleeve_artifact_resolution(
            config=config,
            project_root=report_dir.parent.parent,
            portfolio_symbols=symbols,
            symbol=symbol,
        )
        prefix = artifact_sleeve_report_prefix(symbol, config.strategy.name)
        summary_path = _artifact_resolution_path(resolution, "summary", report_dir / f"{prefix}_summary.json")
        dashboard_path = _artifact_resolution_path(resolution, "dashboard", report_dir / f"{prefix}_dashboard.html")
        equity_path = _artifact_resolution_path(resolution, "equity_curve", report_dir / f"{prefix}_equity_curve.csv")
        trades_path = _artifact_resolution_path(resolution, "trades", report_dir / f"{prefix}_trades.csv")
        summary = _safe_json_payload(summary_path)
        sleeves.append(
            {
                "symbol": symbol,
                "label": f"{symbol} 子报表",
                "metrics": _summary_metrics(summary),
                "dashboard": _artifact_meta_from_resolution(
                    label="子报表页面",
                    key="dashboard",
                    resolution=resolution,
                    fallback=report_dir / f"{prefix}_dashboard.html",
                    url=_artifact_open_url(dashboard_path),
                ),
                "summary_file": _artifact_meta_from_resolution(
                    label="子报表摘要",
                    key="summary",
                    resolution=resolution,
                    fallback=report_dir / f"{prefix}_summary.json",
                    url=_artifact_open_url(summary_path),
                ),
                "equity_curve": _artifact_meta_from_resolution(
                    label="子报表权益曲线",
                    key="equity_curve",
                    resolution=resolution,
                    fallback=report_dir / f"{prefix}_equity_curve.csv",
                    url=_artifact_open_url(equity_path),
                ),
                "trades": _artifact_meta_from_resolution(
                    label="子报表成交明细",
                    key="trades",
                    resolution=resolution,
                    fallback=report_dir / f"{prefix}_trades.csv",
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
        if path.name.endswith("_latest.json"):
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
                "modified_at": serialize_utc_datetime(datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)),
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
        "backtest_dashboard": "回测网页报表",
        "portfolio_dashboard": "组合网页报表",
        "research_dashboard": "研究网页报表",
        "sweep_dashboard": "参数扫描网页报表",
        "summary_json": "摘要文件",
        "trades_csv": "成交明细表",
        "sweep_csv": "参数扫描结果表",
        "research_csv": "研究结果表",
        "spreadsheet": "电子表格",
        "notes": "说明文档",
        "csv": "数据表",
        "json": "数据文件",
    }
    return mapping.get(_artifact_category(path), "产物")


def _artifact_payload_catalog(config: AppConfig, project_root: Path) -> dict[str, Any]:
    storage = config.storage.resolved(project_root)
    report_dir = storage.report_dir
    symbols = configured_symbols(config)
    portfolio_mode = len(symbols) > 1
    backtest_resolution = _backtest_artifact_resolution(config=config, project_root=project_root)
    sweep_resolution = _sweep_artifact_resolution(config=config, project_root=project_root)
    report_prefix = artifact_primary_report_prefix(config)
    sweep_report_prefix = artifact_sweep_prefix(config)
    summary_path = _artifact_resolution_path(backtest_resolution, "summary", report_dir / f"{report_prefix}_summary.json")
    sleeve_reports = _sleeve_report_payloads(config, report_dir, symbols) if portfolio_mode else []
    return {
        "mode": "portfolio" if portfolio_mode else "single",
        "symbols": symbols,
        "backtest_report": _artifact_meta_from_resolution(
            label="组合网页报表" if portfolio_mode else "回测网页报表",
            key="dashboard",
            resolution=backtest_resolution,
            fallback=report_dir / f"{report_prefix}_dashboard.html",
            url="/reports/backtest",
        ),
        "sweep_report": _artifact_meta_from_resolution(
            label="参数扫描网页报表",
            key="dashboard",
            resolution=sweep_resolution,
            fallback=report_dir / f"{sweep_report_prefix}_sweep_dashboard.html",
            url="/reports/sweep",
        ),
        "summary": _artifact_meta_from_resolution(
            label="组合摘要文件" if portfolio_mode else "摘要文件",
            key="summary",
            resolution=backtest_resolution,
            fallback=report_dir / f"{report_prefix}_summary.json",
        ),
        "equity_curve": _artifact_meta_from_resolution(
            label="组合权益曲线" if portfolio_mode else "权益曲线",
            key="equity_curve",
            resolution=backtest_resolution,
            fallback=report_dir / f"{report_prefix}_equity_curve.csv",
        ),
        "trades": _artifact_meta_from_resolution(
            label="组合成交明细" if portfolio_mode else "成交明细",
            key="trades",
            resolution=backtest_resolution,
            fallback=report_dir / f"{report_prefix}_trades.csv",
        ),
        "sweep_csv": _artifact_meta_from_resolution(
            label="参数扫描结果表",
            key="sweep_csv",
            resolution=sweep_resolution,
            fallback=report_dir / f"{sweep_report_prefix}_sweep.csv",
        ),
        "summary_metrics": _summary_metrics(_safe_json_payload(summary_path)),
        "sleeve_reports": sleeve_reports,
        "catalog": _artifact_catalog(report_dir),
    }


def _collect_visual_report_cards(payload: dict[str, Any]) -> list[dict[str, str]]:
    seen: set[str] = set()
    cards: list[dict[str, str]] = []
    preferred = {"backtest_dashboard", "portfolio_dashboard", "sweep_dashboard"}

    def push(item: dict[str, Any] | None, *, default_label: str | None = None, default_kind: str | None = None) -> None:
        if not isinstance(item, dict):
            return
        if not item.get("exists") or not item.get("url"):
            return
        path = str(item.get("path") or "")
        if path and path in seen:
            return
        if path:
            seen.add(path)
        cards.append(
            {
                "label": str(item.get("label") or default_label or "网页报表"),
                "kind": str(item.get("kind") or default_kind or "回测报表"),
                "path": path,
                "url": str(item.get("url") or ""),
            }
        )

    def push_backtest() -> None:
        report = payload.get("backtest_report") if isinstance(payload.get("backtest_report"), dict) else None
        if report is None:
            return
        push(
            {
                **report,
                "label": "当前组合总览" if payload.get("mode") == "portfolio" else "当前主回测",
                "kind": "主报表",
            }
        )

    def push_sleeves() -> None:
        for item in payload.get("sleeve_reports") or []:
            if not isinstance(item, dict):
                continue
            dashboard = item.get("dashboard") if isinstance(item.get("dashboard"), dict) else None
            if dashboard is None:
                continue
            push(
                {
                    **dashboard,
                    "label": f"{item.get('symbol') or '标的'} 子报表",
                    "kind": "组合子报表",
                }
            )

    if payload.get("mode") == "portfolio":
        push_sleeves()
        push_backtest()
    else:
        push_backtest()
        push_sleeves()

    catalog = payload.get("catalog") if isinstance(payload.get("catalog"), list) else []
    dashboard_items = [
        entry
        for entry in catalog
        if isinstance(entry, dict) and "dashboard" in str(entry.get("category") or "") and entry.get("url")
    ]
    for item in sorted(
        dashboard_items,
        key=lambda entry: (
            -2 if "multi_cycle" in str(entry.get("name") or "") else 0,
            -1 if str(entry.get("category") or "") in preferred else 0,
            str(entry.get("name") or ""),
        ),
    )[:10]:
        push(
            {
                **item,
                "label": str(item.get("group_label") or item.get("label") or item.get("name") or "网页报表"),
                "kind": str(item.get("name") or "最近产物"),
            }
        )

    return cards


def _render_initial_visual_reports(payload: dict[str, Any]) -> tuple[str, str]:
    cards = _collect_visual_report_cards(payload)
    if not cards:
        return (
            "当前还没有可嵌入的网页回测报表，请先运行回测或生成报表任务。",
            '<div class="empty">还没有可视化回测预览。</div>',
        )

    html_parts: list[str] = []
    for index, item in enumerate(cards):
        card_class = "report-card primary" if index == 0 else "report-card"
        kind = "多周期回测" if "multi_cycle" in item["kind"] else item["kind"]
        url = f'{item["url"]}{"&" if "?" in item["url"] else "?"}_ts=boot'
        html_parts.append(
            f"""<article class="{card_class}">
      <div class="report-head">
        <div>
          <strong>{html_lib.escape(item["label"])}</strong>
          <div class="hint">{html_lib.escape(kind)}</div>
          <div class="hint">{html_lib.escape(item["path"] or "--")}</div>
        </div>
        <div class="report-tags">
          <span class="tag">K 线回放</span>
          <span class="tag">开平仓标记</span>
          <span class="tag warn">止损线</span>
        </div>
      </div>
      <div class="report-preview">
        <iframe src="{html_lib.escape(url, quote=True)}" loading="lazy" referrerpolicy="no-referrer"></iframe>
      </div>
      <a href="{html_lib.escape(url, quote=True)}" target="_blank" rel="noreferrer">新窗口打开</a>
    </article>"""
        )

    return (
        "主报表置顶显示，便于直接查看 K 线回放、交易标记与止损线。",
        "".join(html_parts),
    )


def _render_initial_portfolio_sleeves(payload: dict[str, Any]) -> str:
    sleeves = payload.get("sleeve_reports") if isinstance(payload.get("sleeve_reports"), list) else []
    if not sleeves:
        return '<div class="empty">当前没有可展示的组合子报表。</div>'

    html_parts: list[str] = []
    for item in sleeves:
        if not isinstance(item, dict):
            continue
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        metrics_lines: list[str] = []
        if metrics.get("final_equity") is not None:
            metrics_lines.append(f"最终权益：{float(metrics['final_equity']):,.2f}")
        if metrics.get("total_return_pct") is not None:
            metrics_lines.append(f"总收益：{float(metrics['total_return_pct']):.2f}%")
        if metrics.get("max_drawdown_pct") is not None:
            metrics_lines.append(f"最大回撤：{float(metrics['max_drawdown_pct']):.2f}%")
        if metrics.get("trade_count") is not None:
            metrics_lines.append(f"交易笔数：{int(float(metrics['trade_count']))}")
        metrics_text = " | ".join(metrics_lines) if metrics_lines else json.dumps(metrics, ensure_ascii=False)
        dashboard = item.get("dashboard") if isinstance(item.get("dashboard"), dict) else {}
        url = str(dashboard.get("url") or "").strip()
        link = (
            f'<a href="{html_lib.escape(url, quote=True)}" target="_blank" rel="noreferrer">打开子报表</a>'
            if url
            else "暂无报表"
        )
        html_parts.append(
            f"""<div class="feed-item">
      <strong>{html_lib.escape(str(item.get("symbol") or "标的"))}</strong>
      <div>{html_lib.escape(metrics_text)}</div>
      <div class="note">{link}</div>
    </div>"""
        )
    return "".join(html_parts) if html_parts else '<div class="empty">当前没有可展示的组合子报表。</div>'


def build_preflight_payload(config: AppConfig, session_factory, project_root: Path) -> dict[str, Any]:
    return runtime_build_preflight_payload(
        config=config,
        session_factory=session_factory,
        project_root=project_root,
        resolve_proxy_egress_ip_fn=_resolve_proxy_egress_ip,
    )


def _resolve_proxy_egress_ip(proxy_url: str | None) -> str | None:
    return runtime_resolve_proxy_egress_ip(proxy_url)
