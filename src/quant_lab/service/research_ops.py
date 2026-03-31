from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from sqlalchemy import desc, select

from quant_lab.application.project_tasks import default_project_research_artifact_resolution
from quant_lab.application.report_runtime import (
    backtest_artifact_paths,
    backtest_legacy_artifact_paths,
    trades_frame as build_trades_frame,
    write_backtest_artifacts,
)
from quant_lab.artifacts import (
    artifact_resolution_path,
    backtest_artifact_resolution,
    candidate_backtest_artifact_identity,
    candidate_backtest_artifact_resolution,
    candidate_backtest_sleeve_artifact_identity,
    candidate_backtest_sleeve_artifact_resolution,
    candidate_report_prefix,
    candidate_sleeve_report_prefix,
    register_artifact_group,
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
from quant_lab.config import AppConfig, InstrumentConfig, configured_symbols, ensure_storage_dirs, load_config
from quant_lab.errors import ConfigurationError, InvalidRequestError, NotFoundError
from quant_lab.models import BacktestArtifacts, TradeRecord
from quant_lab.providers.market_data import build_market_data_provider
from quant_lab.reporting.dashboard import render_dashboard
from quant_lab.service.database import (
    ApprovalDecision,
    EvaluationReport,
    ResearchTask,
    StrategyCandidate,
    session_scope,
)
from quant_lab.service.serialization import serialize_utc_datetime

AI_RESEARCH_ROLES = {
    "research_lead",
    "factor_analyst",
    "strategy_builder",
    "backtest_validator",
    "risk_officer",
}

TASK_STATUSES = {"proposed", "in_progress", "review", "closed"}
CANDIDATE_STATUSES = {
    "draft",
    "evaluation_passed",
    "evaluation_review",
    "evaluation_failed",
    "approved",
    "rejected",
    "watchlist",
}
APPROVAL_DECISIONS = {"approve", "reject", "watchlist"}
APPROVAL_SCOPES = {"research", "demo", "live"}
EVALUATION_TYPES = {"backtest", "walk_forward", "paper_forward", "risk_review"}


def create_research_task(
    *,
    session_factory,
    title: str,
    hypothesis: str,
    owner_role: str,
    priority: str,
    symbols: list[str],
    notes: str,
) -> ResearchTask:
    _validate_role(owner_role)
    status = "proposed"
    cleaned_symbols = _clean_text_list(symbols)
    with session_scope(session_factory) as session:
        task = ResearchTask(
            title=title.strip(),
            hypothesis=hypothesis.strip(),
            owner_role=owner_role,
            priority=(priority or "medium").strip().lower(),
            status=status,
            symbols=cleaned_symbols,
            notes=notes.strip(),
        )
        session.add(task)
        session.flush()
        session.refresh(task)
        return task


def list_research_tasks(*, session_factory, limit: int = 20, status: str | None = None) -> list[ResearchTask]:
    with session_scope(session_factory) as session:
        query = select(ResearchTask).order_by(desc(ResearchTask.created_at)).limit(limit)
        if status:
            query = (
                select(ResearchTask)
                .where(ResearchTask.status == status.strip().lower())
                .order_by(desc(ResearchTask.created_at))
                .limit(limit)
            )
        return list(session.execute(query).scalars())


def register_strategy_candidate(
    *,
    session_factory,
    candidate_name: str,
    strategy_name: str,
    variant: str,
    timeframe: str,
    symbol_scope: list[str],
    config_path: str | None,
    author_role: str,
    thesis: str,
    tags: list[str],
    task_id: int | None = None,
    details: dict[str, Any] | None = None,
) -> StrategyCandidate:
    _validate_role(author_role)
    with session_scope(session_factory) as session:
        if task_id is not None and session.get(ResearchTask, task_id) is None:
            raise NotFoundError(
                f"Research task {task_id} does not exist.",
                error_code="research_task_not_found",
            )

        candidate = StrategyCandidate(
            task_id=task_id,
            candidate_name=candidate_name.strip(),
            strategy_name=strategy_name.strip(),
            variant=variant.strip(),
            timeframe=timeframe.strip(),
            symbol_scope=_clean_text_list(symbol_scope),
            config_path=str(config_path).strip() if config_path else None,
            author_role=author_role,
            status="draft",
            thesis=thesis.strip(),
            tags=_clean_text_list(tags),
            details=details or {},
        )
        session.add(candidate)
        session.flush()
        session.refresh(candidate)
        return candidate


def list_strategy_candidates(
    *,
    session_factory,
    limit: int = 20,
    status: str | None = None,
    approved_only: bool = False,
) -> list[StrategyCandidate]:
    with session_scope(session_factory) as session:
        query = select(StrategyCandidate)
        if approved_only:
            query = query.where(StrategyCandidate.status == "approved")
        elif status:
            query = query.where(StrategyCandidate.status == status.strip().lower())
        query = query.order_by(desc(StrategyCandidate.created_at)).limit(limit)
        return list(session.execute(query).scalars())


def infer_candidate_artifacts(*, config: AppConfig, project_root: Path) -> dict[str, Any]:
    storage = config.storage.resolved(project_root.resolve())
    identity, resolution = backtest_artifact_resolution(
        config=config,
        project_root=project_root.resolve(),
        symbols=configured_symbols(config),
    )
    prefix = str(identity["logical_prefix"])
    return _artifact_bundle_payload(
        summary_path=artifact_resolution_path(
            resolution,
            "summary",
            storage.report_dir / f"{prefix}_summary.json",
        ),
        report_path=artifact_resolution_path(
            resolution,
            "dashboard",
            storage.report_dir / f"{prefix}_dashboard.html",
        ),
        trades_path=artifact_resolution_path(
            resolution,
            "trades",
            storage.report_dir / f"{prefix}_trades.csv",
        ),
        equity_curve_path=artifact_resolution_path(
            resolution,
            "equity_curve",
            storage.report_dir / f"{prefix}_equity_curve.csv",
        ),
        logical_prefix=prefix,
        artifact_fingerprint=str(resolution.get("artifact_fingerprint") or identity["artifact_fingerprint"]),
        resolved_via=str(resolution.get("resolved_via") or ""),
    )


def infer_strategy_candidate_artifacts(
    *,
    candidate: StrategyCandidate,
    config: AppConfig,
    project_root: Path,
) -> dict[str, Any]:
    storage = config.storage.resolved(project_root.resolve())
    symbols = configured_symbols(config)
    if len(symbols) == 1:
        identity, resolution = candidate_backtest_artifact_resolution(
            config=config,
            project_root=project_root,
            candidate_id=candidate.id,
            candidate_name=candidate.candidate_name,
            symbols=symbols,
        )
        prefix = str(identity["logical_prefix"])
        legacy_prefix = candidate_report_prefix(candidate.candidate_name)
        payload = _artifact_bundle_payload(
            summary_path=artifact_resolution_path(
                resolution,
                "summary",
                storage.report_dir / f"{legacy_prefix}_summary.json",
            ),
            report_path=artifact_resolution_path(
                resolution,
                "dashboard",
                storage.report_dir / f"{legacy_prefix}_dashboard.html",
            ),
            trades_path=artifact_resolution_path(
                resolution,
                "trades",
                storage.report_dir / f"{legacy_prefix}_trades.csv",
            ),
            equity_curve_path=artifact_resolution_path(
                resolution,
                "equity_curve",
                storage.report_dir / f"{legacy_prefix}_equity_curve.csv",
            ),
            logical_prefix=prefix,
            artifact_fingerprint=str(resolution.get("artifact_fingerprint") or identity["artifact_fingerprint"]),
            resolved_via=str(resolution.get("resolved_via") or ""),
            sleeves=[],
        )
        payload["mode"] = "single"
        payload["prefix"] = prefix
        payload["symbols"] = symbols
        return payload

    portfolio_identity, portfolio_resolution = candidate_backtest_artifact_resolution(
        config=config,
        project_root=project_root,
        candidate_id=candidate.id,
        candidate_name=candidate.candidate_name,
        symbols=symbols,
    )
    portfolio_prefix = str(portfolio_identity["logical_prefix"])
    legacy_portfolio_prefix = candidate_report_prefix(candidate.candidate_name)
    sleeves = []
    for symbol in symbols:
        sleeve_identity, sleeve_resolution = candidate_backtest_sleeve_artifact_resolution(
            config=config,
            project_root=project_root,
            candidate_id=candidate.id,
            candidate_name=candidate.candidate_name,
            portfolio_symbols=symbols,
            symbol=symbol,
        )
        sleeve_prefix = str(sleeve_identity["logical_prefix"])
        legacy_sleeve_prefix = candidate_sleeve_report_prefix(candidate.candidate_name, symbol)
        sleeve_payload = _artifact_bundle_payload(
            summary_path=artifact_resolution_path(
                sleeve_resolution,
                "summary",
                storage.report_dir / f"{legacy_sleeve_prefix}_summary.json",
            ),
            report_path=artifact_resolution_path(
                sleeve_resolution,
                "dashboard",
                storage.report_dir / f"{legacy_sleeve_prefix}_dashboard.html",
            ),
            trades_path=artifact_resolution_path(
                sleeve_resolution,
                "trades",
                storage.report_dir / f"{legacy_sleeve_prefix}_trades.csv",
            ),
            equity_curve_path=artifact_resolution_path(
                sleeve_resolution,
                "equity_curve",
                storage.report_dir / f"{legacy_sleeve_prefix}_equity_curve.csv",
            ),
            logical_prefix=sleeve_prefix,
            artifact_fingerprint=str(
                sleeve_resolution.get("artifact_fingerprint") or sleeve_identity["artifact_fingerprint"]
            ),
            resolved_via=str(sleeve_resolution.get("resolved_via") or ""),
        )
        sleeve_payload["symbol"] = symbol
        sleeve_payload["prefix"] = sleeve_prefix
        sleeves.append(sleeve_payload)
    payload = _artifact_bundle_payload(
        summary_path=artifact_resolution_path(
            portfolio_resolution,
            "summary",
            storage.report_dir / f"{legacy_portfolio_prefix}_summary.json",
        ),
        report_path=artifact_resolution_path(
            portfolio_resolution,
            "dashboard",
            storage.report_dir / f"{legacy_portfolio_prefix}_dashboard.html",
        ),
        trades_path=artifact_resolution_path(
            portfolio_resolution,
            "trades",
            storage.report_dir / f"{legacy_portfolio_prefix}_trades.csv",
        ),
        equity_curve_path=artifact_resolution_path(
            portfolio_resolution,
            "equity_curve",
            storage.report_dir / f"{legacy_portfolio_prefix}_equity_curve.csv",
        ),
        logical_prefix=portfolio_prefix,
        artifact_fingerprint=str(
            portfolio_resolution.get("artifact_fingerprint") or portfolio_identity["artifact_fingerprint"]
        ),
        resolved_via=str(portfolio_resolution.get("resolved_via") or ""),
        sleeves=sleeves,
    )
    payload["mode"] = "portfolio"
    payload["prefix"] = portfolio_prefix
    payload["symbols"] = symbols
    return payload


def infer_strategy_candidate_artifacts_by_id(
    *,
    session_factory,
    candidate_id: int,
    project_root: Path,
) -> dict[str, Any]:
    with session_scope(session_factory) as session:
        candidate = session.get(StrategyCandidate, candidate_id)
        if candidate is None:
            raise NotFoundError(
                f"Strategy candidate {candidate_id} does not exist.",
                error_code="strategy_candidate_not_found",
            )
    candidate_config_path = _resolve_candidate_config_path(candidate=candidate, project_root=project_root)
    candidate_config = load_config(candidate_config_path)
    candidate_config.storage = candidate_config.storage.resolved(project_root.resolve())
    return infer_strategy_candidate_artifacts(
        candidate=candidate,
        config=candidate_config,
        project_root=project_root,
    )


def _candidate_backtest_legacy_artifact_sets(
    *,
    storage,
    current_prefix: str,
    legacy_prefix: str,
    include_dashboard: bool = False,
    include_allocation_overlay: bool = False,
) -> list[dict[str, Path]]:
    prefixes = [current_prefix]
    if legacy_prefix != current_prefix:
        prefixes.append(legacy_prefix)
    return [
        backtest_legacy_artifact_paths(
            storage=storage,
            report_prefix=prefix,
            include_dashboard=include_dashboard,
            include_allocation_overlay=include_allocation_overlay,
        )
        for prefix in prefixes
    ]


def _artifact_file_payload(
    *,
    path: Path | None,
    logical_prefix: str | None = None,
    artifact_fingerprint: str | None = None,
    resolved_via: str | None = None,
    canonical_path: Path | None = None,
    label: str | None = None,
    url: str | None = None,
) -> dict[str, Any] | None:
    if path is None:
        return None
    payload = {
        "path": str(path),
        "exists": path.exists(),
        "resolved_via": resolved_via or None,
        "logical_prefix": logical_prefix,
        "artifact_fingerprint": artifact_fingerprint,
        "canonical_path": str(canonical_path or path),
    }
    if label is not None:
        payload["label"] = label
    if url is not None:
        payload["url"] = url
    return payload


def _artifact_bundle_payload(
    *,
    summary_path: Path,
    report_path: Path | None,
    trades_path: Path | None,
    equity_curve_path: Path | None,
    logical_prefix: str | None = None,
    artifact_fingerprint: str | None = None,
    resolved_via: str | None = None,
    summary_canonical_path: Path | None = None,
    report_canonical_path: Path | None = None,
    trades_canonical_path: Path | None = None,
    equity_curve_canonical_path: Path | None = None,
    sleeves: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "summary_path": str(summary_path),
        "report_path": str(report_path) if report_path is not None else None,
        "trades_path": str(trades_path) if trades_path is not None else None,
        "equity_curve_path": str(equity_curve_path) if equity_curve_path is not None else None,
        "summary_file": _artifact_file_payload(
            path=summary_path,
            logical_prefix=logical_prefix,
            artifact_fingerprint=artifact_fingerprint,
            resolved_via=resolved_via,
            canonical_path=summary_canonical_path,
        ),
        "report_file": _artifact_file_payload(
            path=report_path,
            logical_prefix=logical_prefix,
            artifact_fingerprint=artifact_fingerprint,
            resolved_via=resolved_via,
            canonical_path=report_canonical_path,
        ),
        "trades_file": _artifact_file_payload(
            path=trades_path,
            logical_prefix=logical_prefix,
            artifact_fingerprint=artifact_fingerprint,
            resolved_via=resolved_via,
            canonical_path=trades_canonical_path,
        ),
        "equity_curve_file": _artifact_file_payload(
            path=equity_curve_path,
            logical_prefix=logical_prefix,
            artifact_fingerprint=artifact_fingerprint,
            resolved_via=resolved_via,
            canonical_path=equity_curve_canonical_path,
        ),
    }
    if logical_prefix:
        payload["logical_prefix"] = logical_prefix
    if artifact_fingerprint:
        payload["artifact_fingerprint"] = artifact_fingerprint
    if resolved_via:
        payload["resolved_via"] = resolved_via
    if sleeves is not None:
        payload["sleeves"] = sleeves
    return payload


def _artifact_payload_with_overrides(
    artifact_payload_source: dict[str, Any] | None,
    *,
    summary_path: Path,
    report_path: Path | None,
    trades_path: Path | None,
    equity_curve_path: Path | None,
) -> dict[str, Any]:
    source = dict(artifact_payload_source) if isinstance(artifact_payload_source, dict) else {}
    logical_prefix = str(source.get("logical_prefix") or "") or None
    artifact_fingerprint = str(source.get("artifact_fingerprint") or "") or None
    default_resolved_via = str(source.get("resolved_via") or "") or None

    def _source_flat_path(key: str) -> Path | None:
        raw_value = source.get(key)
        if raw_value in {"", None}:
            return None
        return Path(str(raw_value))

    def _source_file_meta(key: str) -> dict[str, Any]:
        raw_value = source.get(key)
        return raw_value if isinstance(raw_value, dict) else {}

    def _resolved_meta(
        *,
        flat_key: str,
        file_key: str,
        resolved_path: Path | None,
    ) -> tuple[str | None, Path | None]:
        if resolved_path is None:
            return None, None
        source_path = _source_flat_path(flat_key)
        source_meta = _source_file_meta(file_key)
        if source_path is not None and source_path == resolved_path:
            resolved_via = str(source_meta.get("resolved_via") or default_resolved_via or "") or None
            canonical_raw = source_meta.get("canonical_path")
            canonical_path = Path(str(canonical_raw)) if canonical_raw not in {"", None} else resolved_path
            return resolved_via, canonical_path
        return "explicit_path", resolved_path

    summary_resolved_via, summary_canonical_path = _resolved_meta(
        flat_key="summary_path",
        file_key="summary_file",
        resolved_path=summary_path,
    )
    report_resolved_via, report_canonical_path = _resolved_meta(
        flat_key="report_path",
        file_key="report_file",
        resolved_path=report_path,
    )
    trades_resolved_via, trades_canonical_path = _resolved_meta(
        flat_key="trades_path",
        file_key="trades_file",
        resolved_path=trades_path,
    )
    equity_curve_resolved_via, equity_curve_canonical_path = _resolved_meta(
        flat_key="equity_curve_path",
        file_key="equity_curve_file",
        resolved_path=equity_curve_path,
    )

    payload = dict(source)
    payload.update(
        _artifact_bundle_payload(
            summary_path=summary_path,
            report_path=report_path,
            trades_path=trades_path,
            equity_curve_path=equity_curve_path,
            logical_prefix=logical_prefix,
            artifact_fingerprint=artifact_fingerprint,
            resolved_via=summary_resolved_via or default_resolved_via,
            summary_canonical_path=summary_canonical_path,
            report_canonical_path=report_canonical_path,
            trades_canonical_path=trades_canonical_path,
            equity_curve_canonical_path=equity_curve_canonical_path,
            sleeves=source.get("sleeves") if isinstance(source.get("sleeves"), list) else None,
        )
    )
    payload["summary_file"] = _artifact_file_payload(
        path=summary_path,
        logical_prefix=logical_prefix,
        artifact_fingerprint=artifact_fingerprint,
        resolved_via=summary_resolved_via or default_resolved_via,
        canonical_path=summary_canonical_path,
    )
    payload["report_file"] = _artifact_file_payload(
        path=report_path,
        logical_prefix=logical_prefix,
        artifact_fingerprint=artifact_fingerprint,
        resolved_via=report_resolved_via or default_resolved_via,
        canonical_path=report_canonical_path,
    )
    payload["trades_file"] = _artifact_file_payload(
        path=trades_path,
        logical_prefix=logical_prefix,
        artifact_fingerprint=artifact_fingerprint,
        resolved_via=trades_resolved_via or default_resolved_via,
        canonical_path=trades_canonical_path,
    )
    payload["equity_curve_file"] = _artifact_file_payload(
        path=equity_curve_path,
        logical_prefix=logical_prefix,
        artifact_fingerprint=artifact_fingerprint,
        resolved_via=equity_curve_resolved_via or default_resolved_via,
        canonical_path=equity_curve_canonical_path,
    )
    return payload


def _research_results_artifact_payload(
    *,
    config: AppConfig,
    project_root: Path,
    results_path: Path | None,
) -> dict[str, Any] | None:
    if results_path is None:
        return None
    resolved_results_path = results_path.resolve()
    identity, resolution = default_project_research_artifact_resolution(
        config=config,
        project_root=project_root.resolve(),
    )
    report_dir = config.storage.resolved(project_root.resolve()).report_dir
    canonical_path = artifact_resolution_path(
        resolution,
        "research_csv",
        report_dir / f"{identity['logical_prefix']}.csv",
    )
    resolved_via = str(resolution.get("resolved_via") or "") or None
    if resolved_results_path != canonical_path:
        resolved_via = "explicit_path"
        canonical_path = resolved_results_path
    return _artifact_file_payload(
        path=resolved_results_path,
        logical_prefix=str(identity["logical_prefix"]),
        artifact_fingerprint=str(resolution.get("artifact_fingerprint") or identity["artifact_fingerprint"]),
        resolved_via=resolved_via,
        canonical_path=canonical_path,
    )


def evaluate_strategy_candidate(
    *,
    session_factory,
    candidate_id: int,
    evaluator_role: str,
    evaluation_type: str,
    summary_path: Path,
    report_path: Path | None = None,
    trades_path: Path | None = None,
    equity_curve_path: Path | None = None,
    notes: str = "",
    artifact_payload_source: dict[str, Any] | None = None,
) -> tuple[StrategyCandidate, EvaluationReport]:
    _validate_role(evaluator_role)
    normalized_type = (evaluation_type or "backtest").strip().lower()
    if normalized_type not in EVALUATION_TYPES:
        raise InvalidRequestError(
            f"Unsupported evaluation type: {evaluation_type}",
            error_code="unsupported_evaluation_type",
        )
    if not summary_path.exists():
        raise NotFoundError(
            f"未找到摘要产物：{summary_path}",
            error_code="summary_artifact_not_found",
        )

    raw_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary_metrics = _extract_summary_metrics(raw_summary)
    score_total = _evaluation_score(summary_metrics)
    status = _evaluation_status(summary_metrics=summary_metrics, score_total=score_total)
    artifacts = _artifact_payload_with_overrides(
        artifact_payload_source,
        summary_path=summary_path,
        report_path=report_path,
        trades_path=trades_path,
        equity_curve_path=equity_curve_path,
    )
    now = datetime.now(timezone.utc)

    with session_scope(session_factory) as session:
        candidate = session.get(StrategyCandidate, candidate_id)
        if candidate is None:
            raise NotFoundError(
                f"Strategy candidate {candidate_id} does not exist.",
                error_code="strategy_candidate_not_found",
            )

        report = EvaluationReport(
            candidate_id=candidate_id,
            evaluator_role=evaluator_role,
            evaluation_type=normalized_type,
            status=status,
            score_total=score_total,
            summary_metrics=summary_metrics,
            artifact_payload=artifacts,
            notes=notes.strip(),
        )
        session.add(report)

        candidate.latest_score = score_total
        candidate.latest_evaluation_status = status
        candidate.last_evaluated_at = now
        candidate.status = status
        session.flush()
        session.refresh(candidate)
        session.refresh(report)
        return candidate, report


def approve_strategy_candidate(
    *,
    session_factory,
    candidate_id: int,
    decision: str,
    decider_role: str,
    scope: str,
    reason: str,
) -> tuple[StrategyCandidate, ApprovalDecision]:
    _validate_role(decider_role)
    normalized_decision = (decision or "").strip().lower()
    normalized_scope = (scope or "demo").strip().lower()
    if normalized_decision not in APPROVAL_DECISIONS:
        raise InvalidRequestError(
            f"Unsupported approval decision: {decision}",
            error_code="unsupported_approval_decision",
        )
    if normalized_scope not in APPROVAL_SCOPES:
        raise InvalidRequestError(
            f"Unsupported approval scope: {scope}",
            error_code="unsupported_approval_scope",
        )

    with session_scope(session_factory) as session:
        candidate = session.get(StrategyCandidate, candidate_id)
        if candidate is None:
            raise NotFoundError(
                f"Strategy candidate {candidate_id} does not exist.",
                error_code="strategy_candidate_not_found",
            )

        approval = ApprovalDecision(
            candidate_id=candidate_id,
            decider_role=decider_role,
            decision=normalized_decision,
            scope=normalized_scope,
            reason=reason.strip(),
        )
        session.add(approval)

        candidate.latest_decision = normalized_decision
        candidate.approval_scope = normalized_scope if normalized_decision == "approve" else None
        candidate.status = {
            "approve": "approved",
            "reject": "rejected",
            "watchlist": "watchlist",
        }[normalized_decision]
        session.flush()
        session.refresh(candidate)
        session.refresh(approval)
        return candidate, approval


def build_research_overview(*, session_factory, limit: int = 10) -> dict[str, Any]:
    tasks = list_research_tasks(session_factory=session_factory, limit=limit)
    candidates = list_strategy_candidates(session_factory=session_factory, limit=limit)
    approved = list_strategy_candidates(session_factory=session_factory, limit=limit, approved_only=True)

    with session_scope(session_factory) as session:
        task_statuses = list(session.execute(select(ResearchTask.status)).scalars())
        candidate_statuses = list(session.execute(select(StrategyCandidate.status)).scalars())
        evaluations = list(
            session.execute(
                select(EvaluationReport).order_by(desc(EvaluationReport.created_at)).limit(limit)
            ).scalars()
        )
        approvals = list(
            session.execute(
                select(ApprovalDecision).order_by(desc(ApprovalDecision.created_at)).limit(limit)
            ).scalars()
        )

    return {
        "roles": sorted(AI_RESEARCH_ROLES),
        "task_counts": _count_by_key(task_statuses),
        "candidate_counts": _count_by_key(candidate_statuses),
        "tasks": [serialize_research_task(task) for task in tasks],
        "candidates": [serialize_strategy_candidate(candidate) for candidate in candidates],
        "approved_candidates": [serialize_strategy_candidate(candidate) for candidate in approved],
        "latest_evaluations": [serialize_evaluation_report(report) for report in evaluations],
        "latest_approvals": [serialize_approval_decision(decision) for decision in approvals],
    }


def backtest_strategy_candidate(
    *,
    session_factory,
    candidate_id: int,
    project_root: Path,
    build_report: bool = True,
) -> dict[str, Any]:
    with session_scope(session_factory) as session:
        candidate = session.get(StrategyCandidate, candidate_id)
        if candidate is None:
            raise NotFoundError(
                f"Strategy candidate {candidate_id} does not exist.",
                error_code="strategy_candidate_not_found",
            )

    candidate_config_path = _resolve_candidate_config_path(candidate=candidate, project_root=project_root)
    candidate_config = load_config(candidate_config_path)
    candidate_config.storage = candidate_config.storage.resolved(project_root.resolve())
    ensure_storage_dirs(candidate_config.storage)
    symbols = configured_symbols(candidate_config)

    if len(symbols) == 1:
        symbol = symbols[0]
        artifact_identity = candidate_backtest_artifact_identity(
            config=candidate_config,
            project_root=project_root.resolve(),
            candidate_id=candidate.id,
            candidate_name=candidate.candidate_name,
            symbols=symbols,
        )
        legacy_prefix = candidate_report_prefix(candidate.candidate_name)
        current_prefix = str(artifact_identity["logical_prefix"])
        artifact_paths = backtest_artifact_paths(
            storage=candidate_config.storage,
            artifact_identity=artifact_identity,
            include_dashboard=build_report,
        )
        signal_bars, execution_bars, funding_rates, _symbol_slug_value = _load_candidate_symbol_datasets(
            config=candidate_config,
            storage=candidate_config.storage,
            symbol=symbol,
        )
        instrument_config = _resolve_candidate_instrument_config(
            config=candidate_config,
            storage=candidate_config.storage,
            symbol=symbol,
        )
        artifacts = run_backtest(
            signal_bars=signal_bars,
            execution_bars=execution_bars,
            funding_rates=funding_rates,
            strategy_config=candidate_config.strategy,
            execution_config=candidate_config.execution,
            risk_config=candidate_config.risk,
            instrument_config=instrument_config,
        )
        summary = build_summary(
            equity_curve=artifacts.equity_curve,
            trades=artifacts.trades,
            initial_equity=candidate_config.execution.initial_equity,
        )
        write_backtest_artifacts(
            storage=candidate_config.storage,
            report_prefix=current_prefix,
            trades_frame=build_trades_frame(artifacts.trades),
            equity_curve=artifacts.equity_curve,
            summary=summary,
            signal_frame=artifacts.signal_frame,
            execution_bars=execution_bars,
            artifact_identity=artifact_identity,
            additional_legacy_report_prefixes=[legacy_prefix],
        )
        if build_report:
            render_dashboard(
                summary_path=artifact_paths["summary"],
                equity_curve_path=artifact_paths["equity_curve"],
                trades_path=artifact_paths["trades"],
                output_path=artifact_paths["dashboard"],
                title=f"{candidate.candidate_name} 回测",
            )
            register_artifact_group(
                report_dir=candidate_config.storage.report_dir,
                identity=artifact_identity,
                artifacts={"dashboard": artifact_paths["dashboard"]},
                legacy_artifact_sets=_candidate_backtest_legacy_artifact_sets(
                    storage=candidate_config.storage,
                    current_prefix=current_prefix,
                    legacy_prefix=legacy_prefix,
                    include_dashboard=True,
                ),
            )
        resolved_artifact_paths = infer_strategy_candidate_artifacts(
            candidate=candidate,
            config=candidate_config,
            project_root=project_root,
        )
        return {
            "mode": "single",
            "symbols": symbols,
            "summary": summary,
            "candidate": serialize_strategy_candidate(candidate),
            "config_path": str(candidate_config_path),
            "artifacts": resolved_artifact_paths,
        }

    per_symbol_initial_equity = candidate_config.execution.initial_equity / len(symbols)
    equity_curves_by_symbol: dict[str, pd.DataFrame] = {}
    artifacts_by_symbol: dict[str, BacktestArtifacts] = {}
    trades_by_symbol: dict[str, list[TradeRecord]] = {}
    all_trades: list[TradeRecord] = []
    sleeve_summaries: list[dict[str, Any]] = []
    for symbol in symbols:
        sleeve_identity = candidate_backtest_sleeve_artifact_identity(
            config=candidate_config,
            project_root=project_root.resolve(),
            candidate_id=candidate.id,
            candidate_name=candidate.candidate_name,
            portfolio_symbols=symbols,
            symbol=symbol,
        )
        legacy_sleeve_prefix = candidate_sleeve_report_prefix(candidate.candidate_name, symbol)
        sleeve_prefix = str(sleeve_identity["logical_prefix"])
        sleeve_paths = backtest_artifact_paths(
            storage=candidate_config.storage,
            artifact_identity=sleeve_identity,
            include_dashboard=build_report,
        )
        signal_bars, execution_bars, funding_rates, _symbol_slug_value = _load_candidate_symbol_datasets(
            config=candidate_config,
            storage=candidate_config.storage,
            symbol=symbol,
        )
        instrument_config = _resolve_candidate_instrument_config(
            config=candidate_config,
            storage=candidate_config.storage,
            symbol=symbol,
        )
        execution_config = candidate_config.execution.model_copy(update={"initial_equity": per_symbol_initial_equity})
        artifacts = run_backtest(
            signal_bars=signal_bars,
            execution_bars=execution_bars,
            funding_rates=funding_rates,
            strategy_config=candidate_config.strategy,
            execution_config=execution_config,
            risk_config=candidate_config.risk,
            instrument_config=instrument_config,
        )
        summary = build_summary(
            equity_curve=artifacts.equity_curve,
            trades=artifacts.trades,
            initial_equity=per_symbol_initial_equity,
        )
        summary["symbol"] = symbol
        summary["capital_allocation_pct"] = round(100 / len(symbols), 2)
        sleeve_summaries.append(summary)

        write_backtest_artifacts(
            storage=candidate_config.storage,
            report_prefix=sleeve_prefix,
            trades_frame=build_trades_frame(artifacts.trades),
            equity_curve=artifacts.equity_curve,
            summary=summary,
            signal_frame=artifacts.signal_frame,
            execution_bars=execution_bars,
            artifact_identity=sleeve_identity,
            additional_legacy_report_prefixes=[legacy_sleeve_prefix],
        )
        if build_report:
            render_dashboard(
                summary_path=sleeve_paths["summary"],
                equity_curve_path=sleeve_paths["equity_curve"],
                trades_path=sleeve_paths["trades"],
                output_path=sleeve_paths["dashboard"],
                title=f"{candidate.candidate_name} {symbol} 子报表",
            )
            register_artifact_group(
                report_dir=candidate_config.storage.report_dir,
                identity=sleeve_identity,
                artifacts={"dashboard": sleeve_paths["dashboard"]},
                legacy_artifact_sets=_candidate_backtest_legacy_artifact_sets(
                    storage=candidate_config.storage,
                    current_prefix=sleeve_prefix,
                    legacy_prefix=legacy_sleeve_prefix,
                    include_dashboard=True,
                ),
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
        initial_equity=candidate_config.execution.initial_equity,
        symbols=symbols,
    )
    portfolio_summary = attach_equal_weight_portfolio_construction(
        portfolio_summary,
        per_symbol_initial_equity=per_symbol_initial_equity,
    )
    portfolio_allocation_overlay = build_portfolio_risk_budget_overlay(
        symbol_artifacts=artifacts_by_symbol,
        execution_config=candidate_config.execution,
        risk_config=candidate_config.risk,
    )
    portfolio_summary = attach_portfolio_risk_budget_overlay(
        portfolio_summary,
        allocation_frame=portfolio_allocation_overlay,
    )
    portfolio_identity = candidate_backtest_artifact_identity(
        config=candidate_config,
        project_root=project_root.resolve(),
        candidate_id=candidate.id,
        candidate_name=candidate.candidate_name,
        symbols=symbols,
    )
    legacy_portfolio_prefix = candidate_report_prefix(candidate.candidate_name)
    portfolio_prefix = str(portfolio_identity["logical_prefix"])
    artifact_paths = backtest_artifact_paths(
        storage=candidate_config.storage,
        artifact_identity=portfolio_identity,
        include_dashboard=build_report,
        include_allocation_overlay=True,
    )
    write_backtest_artifacts(
        storage=candidate_config.storage,
        report_prefix=portfolio_prefix,
        trades_frame=portfolio_trades,
        equity_curve=portfolio_equity,
        summary=portfolio_summary,
        allocation_overlay=portfolio_allocation_overlay,
        artifact_identity=portfolio_identity,
        additional_legacy_report_prefixes=[legacy_portfolio_prefix],
    )
    if build_report:
        render_dashboard(
            summary_path=artifact_paths["summary"],
            equity_curve_path=artifact_paths["equity_curve"],
            trades_path=artifact_paths["trades"],
            output_path=artifact_paths["dashboard"],
            title=f"{candidate.candidate_name} 组合回测",
        )
        register_artifact_group(
            report_dir=candidate_config.storage.report_dir,
            identity=portfolio_identity,
            artifacts={"dashboard": artifact_paths["dashboard"]},
            legacy_artifact_sets=_candidate_backtest_legacy_artifact_sets(
                storage=candidate_config.storage,
                current_prefix=portfolio_prefix,
                legacy_prefix=legacy_portfolio_prefix,
                include_dashboard=True,
                include_allocation_overlay=True,
            ),
        )
    resolved_artifact_paths = infer_strategy_candidate_artifacts(
        candidate=candidate,
        config=candidate_config,
        project_root=project_root,
    )
    return {
        "mode": "portfolio",
        "symbols": symbols,
        "summary": portfolio_summary,
        "candidate": serialize_strategy_candidate(candidate),
        "config_path": str(candidate_config_path),
        "artifacts": resolved_artifact_paths,
        "sleeves": sleeve_summaries,
    }


def evaluate_backtested_candidate(
    *,
    session_factory,
    candidate_id: int,
    project_root: Path,
    build_report: bool = True,
    evaluator_role: str = "backtest_validator",
    evaluation_type: str = "backtest",
    notes: str = "",
) -> dict[str, Any]:
    backtest_payload = backtest_strategy_candidate(
        session_factory=session_factory,
        candidate_id=candidate_id,
        project_root=project_root,
        build_report=build_report,
    )
    artifacts = backtest_payload["artifacts"]
    candidate, report = evaluate_strategy_candidate(
        session_factory=session_factory,
        candidate_id=candidate_id,
        evaluator_role=evaluator_role,
        evaluation_type=evaluation_type,
        summary_path=Path(artifacts["summary_path"]),
        report_path=Path(artifacts["report_path"]) if build_report else None,
        trades_path=Path(artifacts["trades_path"]),
        equity_curve_path=Path(artifacts["equity_curve_path"]),
        notes=notes,
        artifact_payload_source=artifacts,
    )
    return {
        "candidate": serialize_strategy_candidate(candidate),
        "evaluation_report": serialize_evaluation_report(report),
        "backtest": backtest_payload,
    }


def materialize_trend_research_candidates(
    *,
    session_factory,
    config: AppConfig,
    project_root: Path,
    base_config_path: Path,
    results_frame: pd.DataFrame,
    results_path: Path | None = None,
    top_n: int = 3,
    task_id: int | None = None,
    task_title: str | None = None,
    owner_role: str = "research_lead",
    author_role: str = "strategy_builder",
    notes: str = "",
) -> dict[str, Any]:
    _validate_role(owner_role)
    _validate_role(author_role)
    if top_n < 1:
        raise InvalidRequestError("top_n must be >= 1", error_code="invalid_top_n")

    ranked = _prepare_research_results_frame(results_frame)
    if ranked.empty:
        raise InvalidRequestError("Research results are empty.", error_code="empty_research_results")

    symbol = config.instrument.symbol
    selected_rows = ranked.head(top_n).reset_index(drop=True)
    research_task = _resolve_research_task_for_materialization(
        session_factory=session_factory,
        config=config,
        task_id=task_id,
        task_title=task_title,
        owner_role=owner_role,
        notes=notes,
        symbol=symbol,
    )
    project_root = project_root.resolve()
    base_config_path = base_config_path.resolve()
    results_artifact = _research_results_artifact_payload(
        config=config,
        project_root=project_root,
        results_path=results_path,
    )

    created_candidates: list[dict[str, Any]] = []
    for rank, (_, row) in enumerate(selected_rows.iterrows(), start=1):
        row_payload = _sanitize_research_row(row)
        candidate_name = _build_materialized_candidate_name(
            symbol=symbol,
            signal_bar=config.strategy.signal_bar,
            task_id=research_task.id,
            rank=rank,
            row_payload=row_payload,
        )
        relative_config_path = Path("config") / "candidates" / f"{candidate_name}.yaml"
        absolute_config_path = (project_root / relative_config_path).resolve()
        _write_candidate_config(
            base_config=config,
            symbol=symbol,
            row_payload=row_payload,
            destination=absolute_config_path,
        )

        candidate = register_strategy_candidate(
            session_factory=session_factory,
            candidate_name=candidate_name,
            strategy_name=str(row_payload.get("strategy_name") or config.strategy.name),
            variant=str(row_payload["variant"]),
            timeframe=str(config.strategy.signal_bar),
            symbol_scope=[symbol],
            config_path=relative_config_path.as_posix(),
            author_role=author_role,
            thesis=_build_materialized_candidate_thesis(
                symbol=symbol,
                rank=rank,
                row_payload=row_payload,
            ),
            tags=_build_materialized_candidate_tags(symbol=symbol, row_payload=row_payload),
            task_id=research_task.id,
            details={
                "source": "trend_research_materialize_top",
                "rank": rank,
                "base_config_path": str(base_config_path),
                "results_path": str(results_path.resolve()) if results_path is not None else None,
                "config_relative_path": relative_config_path.as_posix(),
                "config_path": str(absolute_config_path),
                "research_row": row_payload,
            },
        )
        created_candidates.append(
            {
                "rank": rank,
                "config_relative_path": relative_config_path.as_posix(),
                "config_path": str(absolute_config_path),
                "research_metrics": _candidate_metric_snapshot(row_payload),
                "candidate": serialize_strategy_candidate(candidate),
            }
        )

    return {
        "task": serialize_research_task(research_task),
        "results_path": str(results_path.resolve()) if results_path is not None else None,
        "results_artifact": results_artifact,
        "top_n": top_n,
        "created_count": len(created_candidates),
        "candidates": created_candidates,
    }


def promote_trend_research_candidates(
    *,
    session_factory,
    config: AppConfig,
    project_root: Path,
    base_config_path: Path,
    results_frame: pd.DataFrame,
    results_path: Path | None = None,
    top_n: int = 3,
    task_id: int | None = None,
    task_title: str | None = None,
    owner_role: str = "research_lead",
    author_role: str = "strategy_builder",
    evaluator_role: str = "backtest_validator",
    notes: str = "",
    build_report: bool = True,
) -> dict[str, Any]:
    materialized = materialize_trend_research_candidates(
        session_factory=session_factory,
        config=config,
        project_root=project_root,
        base_config_path=base_config_path,
        results_frame=results_frame,
        results_path=results_path,
        top_n=top_n,
        task_id=task_id,
        task_title=task_title,
        owner_role=owner_role,
        author_role=author_role,
        notes=notes,
    )
    evaluated: list[dict[str, Any]] = []
    for item in materialized["candidates"]:
        candidate_id = int(item["candidate"]["id"])
        evaluated.append(
            evaluate_backtested_candidate(
                session_factory=session_factory,
                candidate_id=candidate_id,
                project_root=project_root,
                build_report=build_report,
                evaluator_role=evaluator_role,
                evaluation_type="backtest",
                notes="Auto-evaluated from trend research promotion pipeline.",
            )
        )
    materialized["evaluated_count"] = len(evaluated)
    materialized["evaluations"] = evaluated
    return materialized


def resolve_execution_approval(
    *,
    session_factory,
    config: AppConfig,
    required_scope: str = "demo",
) -> dict[str, Any]:
    required = bool(config.trading.require_approved_candidate)
    router_enabled = bool(config.trading.strategy_router_enabled)
    configured_candidate_id = config.trading.execution_candidate_id
    configured_candidate_name = config.trading.execution_candidate_name
    normalized_symbols = _normalize_symbols(configured_symbols(config))
    reasons: list[str] = []
    selected_by: str | None = None
    selected: StrategyCandidate | None = None
    router_routes: list[dict[str, Any]] = []

    with session_scope(session_factory) as session:
        if router_enabled:
            candidate_map = config.trading.execution_candidate_map or {}
            if not candidate_map:
                reasons.append("strategy router is enabled but trading.execution_candidate_map is empty")
            else:
                for raw_key, raw_value in sorted(candidate_map.items()):
                    try:
                        candidate_id = int(raw_value)
                    except (TypeError, ValueError):
                        reasons.append(f"route {raw_key} has invalid candidate id {raw_value}")
                        continue
                    candidate = session.get(StrategyCandidate, candidate_id)
                    route_reasons: list[str] = []
                    if candidate is None:
                        route_reasons.append(f"candidate {candidate_id} not found")
                    else:
                        if candidate.status != "approved":
                            route_reasons.append(f"candidate {candidate_id} is not approved")
                        if not _scope_allows(candidate.approval_scope, required_scope):
                            route_reasons.append(
                                f"candidate {candidate_id} scope {candidate.approval_scope or 'none'} is not compatible with {required_scope}"
                            )
                    router_routes.append(
                        {
                            "route_key": str(raw_key),
                            "candidate_id": candidate_id,
                            "candidate": serialize_strategy_candidate(candidate) if candidate is not None else None,
                            "ready": not route_reasons,
                            "reasons": route_reasons,
                        }
                    )
                    reasons.extend(f"{raw_key}: {item}" for item in route_reasons)
            gate_ready = not reasons if required else True
            return {
                "required": required,
                "required_scope": required_scope,
                "ready": gate_ready,
                "router_enabled": True,
                "selected_by": "strategy_router_pool",
                "configured_candidate_id": configured_candidate_id,
                "configured_candidate_name": configured_candidate_name,
                "candidate": None,
                "candidate_map": dict(candidate_map),
                "routes": router_routes,
                "available_matches": [],
                "reasons": reasons,
            }

        if configured_candidate_id is not None:
            selected = session.get(StrategyCandidate, configured_candidate_id)
            selected_by = "candidate_id"
            if selected is None:
                reasons.append(f"execution candidate id {configured_candidate_id} not found")
        elif configured_candidate_name:
            selected = session.execute(
                select(StrategyCandidate)
                .where(StrategyCandidate.candidate_name == str(configured_candidate_name).strip())
                .order_by(desc(StrategyCandidate.created_at))
                .limit(1)
            ).scalar_one_or_none()
            selected_by = "candidate_name"
            if selected is None:
                reasons.append(f"execution candidate name '{configured_candidate_name}' not found")
        else:
            matches = _find_matching_approved_candidates(
                session=session,
                config=config,
                required_scope=required_scope,
            )
            if len(matches) == 1:
                selected = matches[0]
                selected_by = "strategy_match"
            elif len(matches) > 1:
                reasons.append("multiple approved candidates match the current strategy; bind one explicitly")

        if selected is not None:
            reasons.extend(
                _candidate_validation_reasons(
                    candidate=selected,
                    config=config,
                    required_scope=required_scope,
                    normalized_symbols=normalized_symbols,
                )
            )

        available_matches = [
            serialize_strategy_candidate(candidate)
            for candidate in _find_matching_approved_candidates(
                session=session,
                config=config,
                required_scope=required_scope,
            )
        ]

    gate_ready = not reasons if required else True
    if selected is None and required:
        reasons.append("no approved execution candidate is bound to the current strategy")
        gate_ready = False

    return {
        "required": required,
        "required_scope": required_scope,
        "ready": gate_ready,
        "router_enabled": False,
        "configured_candidate_id": configured_candidate_id,
        "configured_candidate_name": configured_candidate_name,
        "selected_by": selected_by,
        "candidate": serialize_strategy_candidate(selected) if selected is not None else None,
        "candidate_map": {},
        "routes": router_routes,
        "available_matches": available_matches,
        "reasons": reasons,
    }


def serialize_research_task(task: ResearchTask) -> dict[str, Any]:
    return {
        "id": task.id,
        "title": task.title,
        "hypothesis": task.hypothesis,
        "owner_role": task.owner_role,
        "priority": task.priority,
        "status": task.status,
        "symbols": task.symbols,
        "notes": task.notes,
        "created_at": serialize_utc_datetime(task.created_at),
        "updated_at": serialize_utc_datetime(task.updated_at),
        "closed_at": serialize_utc_datetime(task.closed_at),
    }


def serialize_strategy_candidate(candidate: StrategyCandidate) -> dict[str, Any]:
    return {
        "id": candidate.id,
        "task_id": candidate.task_id,
        "candidate_name": candidate.candidate_name,
        "strategy_name": candidate.strategy_name,
        "variant": candidate.variant,
        "timeframe": candidate.timeframe,
        "symbol_scope": candidate.symbol_scope,
        "config_path": candidate.config_path,
        "author_role": candidate.author_role,
        "status": candidate.status,
        "thesis": candidate.thesis,
        "tags": candidate.tags,
        "details": candidate.details,
        "latest_score": candidate.latest_score,
        "latest_evaluation_status": candidate.latest_evaluation_status,
        "latest_decision": candidate.latest_decision,
        "approval_scope": candidate.approval_scope,
        "last_evaluated_at": serialize_utc_datetime(candidate.last_evaluated_at),
        "created_at": serialize_utc_datetime(candidate.created_at),
        "updated_at": serialize_utc_datetime(candidate.updated_at),
    }


def serialize_evaluation_report(report: EvaluationReport) -> dict[str, Any]:
    return {
        "id": report.id,
        "candidate_id": report.candidate_id,
        "evaluator_role": report.evaluator_role,
        "evaluation_type": report.evaluation_type,
        "status": report.status,
        "score_total": report.score_total,
        "summary_metrics": report.summary_metrics,
        "artifact_payload": report.artifact_payload,
        "notes": report.notes,
        "created_at": serialize_utc_datetime(report.created_at),
    }


def serialize_approval_decision(decision: ApprovalDecision) -> dict[str, Any]:
    return {
        "id": decision.id,
        "candidate_id": decision.candidate_id,
        "decider_role": decision.decider_role,
        "decision": decision.decision,
        "scope": decision.scope,
        "reason": decision.reason,
        "created_at": serialize_utc_datetime(decision.created_at),
    }


def _validate_role(role: str) -> None:
    normalized = (role or "").strip().lower()
    if normalized not in AI_RESEARCH_ROLES:
        supported = ", ".join(sorted(AI_RESEARCH_ROLES))
        raise InvalidRequestError(
            f"Unsupported role: {role}. Supported roles: {supported}",
            error_code="unsupported_research_role",
        )


def _clean_text_list(values: list[str] | tuple[str, ...] | None) -> list[str]:
    if not values:
        return []
    cleaned: list[str] = []
    for item in values:
        normalized = str(item).strip()
        if normalized and normalized not in cleaned:
            cleaned.append(normalized)
    return cleaned


def _extract_summary_metrics(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "initial_equity": _safe_float(summary.get("initial_equity")),
        "final_equity": _safe_float(summary.get("final_equity")),
        "total_return_pct": _safe_float(summary.get("total_return_pct")),
        "annualized_return_pct": _safe_float(summary.get("annualized_return_pct")),
        "max_drawdown_pct": _safe_float(summary.get("max_drawdown_pct")),
        "trade_count": _safe_int(summary.get("trade_count")),
        "win_rate_pct": _safe_float(summary.get("win_rate_pct")),
        "profit_factor": _safe_float(summary.get("profit_factor")),
        "sharpe": _safe_float(summary.get("sharpe")),
    }


def _evaluation_score(summary_metrics: dict[str, Any]) -> float:
    total_return_pct = _safe_float(summary_metrics.get("total_return_pct")) or 0.0
    max_drawdown_pct = _safe_float(summary_metrics.get("max_drawdown_pct")) or 0.0
    profit_factor = _safe_float(summary_metrics.get("profit_factor")) or 0.0
    sharpe = _safe_float(summary_metrics.get("sharpe")) or 0.0
    trade_count = _safe_int(summary_metrics.get("trade_count")) or 0

    return round(
        (
            _scaled_score(total_return_pct, low=0.0, high=40.0) * 20.0
            + _scaled_score(sharpe, low=0.3, high=1.5) * 30.0
            + _scaled_score(profit_factor, low=1.0, high=2.0) * 25.0
            + _scaled_score(20.0 - max_drawdown_pct, low=0.0, high=12.0) * 15.0
            + _scaled_score(float(trade_count), low=10.0, high=80.0) * 10.0
        ),
        2,
    )


def _evaluation_status(*, summary_metrics: dict[str, Any], score_total: float) -> str:
    sharpe = _safe_float(summary_metrics.get("sharpe")) or 0.0
    profit_factor = _safe_float(summary_metrics.get("profit_factor")) or 0.0
    max_drawdown_pct = _safe_float(summary_metrics.get("max_drawdown_pct")) or 100.0
    trade_count = _safe_int(summary_metrics.get("trade_count")) or 0
    if score_total >= 70.0 and sharpe >= 0.7 and profit_factor >= 1.3 and max_drawdown_pct <= 15.0 and trade_count >= 20:
        return "evaluation_passed"
    if score_total >= 50.0 and profit_factor >= 1.05 and max_drawdown_pct <= 20.0:
        return "evaluation_review"
    return "evaluation_failed"


def _scaled_score(value: float, *, low: float, high: float) -> float:
    if high <= low:
        return 0.0
    if value <= low:
        return 0.0
    if value >= high:
        return 1.0
    return (value - low) / (high - low)


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _count_by_key(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def _find_matching_approved_candidates(*, session, config: AppConfig, required_scope: str) -> list[StrategyCandidate]:
    candidates = list(
        session.execute(
            select(StrategyCandidate)
            .where(StrategyCandidate.status == "approved")
            .order_by(desc(StrategyCandidate.updated_at), desc(StrategyCandidate.created_at))
        ).scalars()
    )
    normalized_symbols = _normalize_symbols(configured_symbols(config))
    matches: list[StrategyCandidate] = []
    for candidate in candidates:
        if candidate.strategy_name != config.strategy.name:
            continue
        if candidate.variant != config.strategy.variant:
            continue
        if str(candidate.timeframe).strip().upper() != str(config.strategy.signal_bar).strip().upper():
            continue
        if not _scope_allows(candidate.approval_scope, required_scope):
            continue
        if _normalize_symbols(candidate.symbol_scope) != normalized_symbols:
            continue
        matches.append(candidate)
    return matches


def _candidate_validation_reasons(
    *,
    candidate: StrategyCandidate,
    config: AppConfig,
    required_scope: str,
    normalized_symbols: list[str],
) -> list[str]:
    reasons: list[str] = []
    if candidate.status != "approved":
        reasons.append(f"candidate '{candidate.candidate_name}' is not approved")
    if not _scope_allows(candidate.approval_scope, required_scope):
        reasons.append(
            f"candidate '{candidate.candidate_name}' is approved for {candidate.approval_scope or 'none'}, not {required_scope}"
        )
    if candidate.strategy_name != config.strategy.name:
        reasons.append(
            f"candidate strategy_name mismatch: candidate={candidate.strategy_name}, config={config.strategy.name}"
        )
    if candidate.variant != config.strategy.variant:
        reasons.append(f"candidate variant mismatch: candidate={candidate.variant}, config={config.strategy.variant}")
    if str(candidate.timeframe).strip().upper() != str(config.strategy.signal_bar).strip().upper():
        reasons.append(
            f"candidate timeframe mismatch: candidate={candidate.timeframe}, config={config.strategy.signal_bar}"
        )
    candidate_symbols = _normalize_symbols(candidate.symbol_scope)
    if candidate_symbols != normalized_symbols:
        reasons.append(
            f"candidate symbol scope mismatch: candidate={candidate_symbols}, config={normalized_symbols}"
        )
    return reasons


def _scope_allows(candidate_scope: str | None, required_scope: str) -> bool:
    candidate = str(candidate_scope or "").strip().lower()
    required = str(required_scope or "").strip().lower()
    if candidate == required:
        return True
    if required == "demo" and candidate == "live":
        return True
    return False


def _normalize_symbols(symbols: list[str] | None) -> list[str]:
    if not symbols:
        return []
    return sorted(_clean_text_list(symbols))


def _prepare_research_results_frame(results_frame: pd.DataFrame) -> pd.DataFrame:
    required = {"variant", "fast_ema", "slow_ema", "atr_stop_multiple"}
    missing = sorted(required - set(results_frame.columns))
    if missing:
        raise InvalidRequestError(
            f"Research results are missing required columns: {', '.join(missing)}",
            error_code="research_results_missing_columns",
        )

    frame = results_frame.copy()
    if "research_score" in frame.columns:
        sort_columns = ["research_score", "bear_return_pct", "sharpe", "total_return_pct", "max_drawdown_pct"]
        sort_columns = [column for column in sort_columns if column in frame.columns]
        if sort_columns:
            ascending = [False, False, False, False, True][: len(sort_columns)]
            frame = frame.sort_values(sort_columns, ascending=ascending, kind="stable")
    return frame.reset_index(drop=True)


def _resolve_research_task_for_materialization(
    *,
    session_factory,
    config: AppConfig,
    task_id: int | None,
    task_title: str | None,
    owner_role: str,
    notes: str,
    symbol: str,
) -> ResearchTask:
    if task_id is not None:
        with session_scope(session_factory) as session:
            task = session.get(ResearchTask, task_id)
            if task is None:
                raise NotFoundError(
                    f"Research task {task_id} does not exist.",
                    error_code="research_task_not_found",
                )
            return task

    return create_research_task(
        session_factory=session_factory,
        title=(task_title or f"Materialize top trend candidates for {symbol}").strip(),
        hypothesis=(
            "Convert the top-ranked trend research parameter sets into candidate strategy configs "
            "for later evaluation and routing."
        ),
        owner_role=owner_role,
        priority="high",
        symbols=[symbol],
        notes=notes.strip(),
    )


def _sanitize_research_row(row: pd.Series) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in row.to_dict().items():
        if pd.isna(value):
            payload[key] = None
        elif hasattr(value, "item"):
            payload[key] = value.item()
        else:
            payload[key] = value
    return payload


def _build_materialized_candidate_name(
    *,
    symbol: str,
    signal_bar: str,
    task_id: int,
    rank: int,
    row_payload: dict[str, Any],
) -> str:
    symbol_token = _slugify(symbol.replace("-SWAP", "")).replace("usdt", "").strip("_")
    variant_token = _slugify(str(row_payload.get("variant") or "candidate"))
    signal_token = _slugify(str(signal_bar).lower())
    fast_token = f"f{_number_token(row_payload.get('fast_ema'))}"
    slow_token = f"s{_number_token(row_payload.get('slow_ema'))}"
    atr_token = f"atr{_number_token(row_payload.get('atr_stop_multiple'))}"
    trend_token = f"te{_number_token(row_payload.get('trend_ema', 0))}"
    adx_token = f"adx{_number_token(row_payload.get('adx_threshold', 0))}"
    return "_".join(
        token
        for token in (
            symbol_token or "symbol",
            signal_token or "bar",
            variant_token or "variant",
            fast_token,
            slow_token,
            atr_token,
            trend_token,
            adx_token,
            f"t{task_id}",
            f"r{rank:02d}",
        )
        if token
    )[:240]


def _write_candidate_config(
    *,
    base_config: AppConfig,
    symbol: str,
    row_payload: dict[str, Any],
    destination: Path,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    candidate_config = base_config.model_copy(deep=True)
    candidate_config.instrument.symbol = symbol
    candidate_config.portfolio.symbols = [symbol]
    candidate_config.strategy.name = str(row_payload.get("strategy_name") or candidate_config.strategy.name)
    candidate_config.strategy.variant = str(row_payload["variant"])
    candidate_config.strategy.fast_ema = int(float(row_payload["fast_ema"]))
    candidate_config.strategy.slow_ema = int(float(row_payload["slow_ema"]))
    candidate_config.strategy.atr_stop_multiple = float(row_payload["atr_stop_multiple"])
    if row_payload.get("trend_ema") is not None:
        candidate_config.strategy.trend_ema = int(float(row_payload["trend_ema"]))
    if row_payload.get("adx_threshold") is not None:
        candidate_config.strategy.adx_threshold = float(row_payload["adx_threshold"])

    payload = candidate_config.model_dump(mode="json", exclude_none=True)
    destination.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _build_materialized_candidate_thesis(*, symbol: str, rank: int, row_payload: dict[str, Any]) -> str:
    return (
        f"Auto-generated candidate from {symbol} trend research rank #{rank}. "
        f"research_score={_fmt_metric(row_payload.get('research_score'))}, "
        f"bear_return_pct={_fmt_metric(row_payload.get('bear_return_pct'))}, "
        f"max_drawdown_pct={_fmt_metric(row_payload.get('max_drawdown_pct'))}, "
        f"sharpe={_fmt_metric(row_payload.get('sharpe'))}, "
        f"total_return_pct={_fmt_metric(row_payload.get('total_return_pct'))}."
    )


def _build_materialized_candidate_tags(*, symbol: str, row_payload: dict[str, Any]) -> list[str]:
    tags = [
        "trend_research",
        _slugify(symbol.lower()),
        _slugify(str(row_payload.get("variant") or "")),
    ]
    if row_payload.get("research_score") is not None:
        tags.append(f"score_{_number_token(row_payload['research_score'])}")
    return [tag for tag in tags if tag]


def _candidate_metric_snapshot(row_payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "research_score",
        "total_return_pct",
        "max_drawdown_pct",
        "sharpe",
        "profit_factor",
        "bear_return_pct",
        "bear_trade_count",
        "bull_return_pct",
    )
    return {key: row_payload.get(key) for key in keys if key in row_payload}


def _slugify(value: str) -> str:
    chars: list[str] = []
    for char in str(value).lower():
        if char.isalnum():
            chars.append(char)
        else:
            chars.append("_")
    slug = "".join(chars).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug


def _number_token(value: Any) -> str:
    raw = _safe_float(value)
    if raw is None:
        return "0"
    if abs(raw - int(raw)) < 1e-9:
        return str(int(raw))
    return str(raw).replace("-", "n").replace(".", "p")


def _fmt_metric(value: Any) -> str:
    raw = _safe_float(value)
    if raw is None:
        return "n/a"
    return f"{raw:.4f}".rstrip("0").rstrip(".")


def _resolve_candidate_config_path(*, candidate: StrategyCandidate, project_root: Path) -> Path:
    raw_path = str(candidate.config_path or "").strip()
    if not raw_path:
        raise ConfigurationError(
            f"Strategy candidate {candidate.id} does not have config_path.",
            error_code="strategy_candidate_config_missing",
        )
    config_path = Path(raw_path)
    if not config_path.is_absolute():
        config_path = (project_root / config_path).resolve()
    if not config_path.exists():
        raise NotFoundError(
            f"Candidate config_path not found: {config_path}",
            error_code="strategy_candidate_config_not_found",
        )
    return config_path


def _load_candidate_symbol_datasets(
    *,
    config: AppConfig,
    storage,
    symbol: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    symbol_slug = _symbol_slug(symbol)
    signal_path = storage.raw_dir / f"{symbol_slug}_{config.strategy.signal_bar}.parquet"
    execution_path = storage.raw_dir / f"{symbol_slug}_{config.strategy.execution_bar}.parquet"
    funding_path = storage.raw_dir / f"{symbol_slug}_funding.parquet"
    for path in (signal_path, execution_path, funding_path):
        if not path.exists():
            raise NotFoundError(
                f"Missing required dataset: {path}",
                error_code="market_dataset_not_found",
            )

    signal_bars = pd.read_parquet(signal_path)
    execution_bars = pd.read_parquet(execution_path)
    funding_rates = pd.read_parquet(funding_path)
    if str(config.strategy.variant).strip().lower() in {
        "high_weight_long",
        "trend_regime_long",
        "trend_pullback_long",
        "trend_breakout_long",
    }:
        signal_bars = _enrich_candidate_signal_bars(
            signal_bars=signal_bars,
            mark_price_bars=_read_parquet_if_exists(storage.raw_dir / f"{symbol_slug}_mark_price_{config.strategy.signal_bar}.parquet"),
            index_bars=_read_parquet_if_exists(storage.raw_dir / f"{symbol_slug}_index_{config.strategy.signal_bar}.parquet"),
        )
    return signal_bars, execution_bars, funding_rates, symbol_slug


def _resolve_candidate_instrument_config(
    *,
    config: AppConfig,
    storage,
    symbol: str,
) -> InstrumentConfig:
    if symbol == config.instrument.symbol:
        return config.instrument

    metadata_path = storage.raw_dir / f"{_symbol_slug(symbol)}_instrument.json"
    if metadata_path.exists():
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        return InstrumentConfig.model_validate(payload)

    client = build_market_data_provider(config)
    try:
        instrument = client.fetch_instrument_details(
            inst_type=config.instrument.instrument_type,
            inst_id=symbol,
        )
    finally:
        client.close()
    metadata_path.write_text(json.dumps(instrument, ensure_ascii=False, indent=2), encoding="utf-8")
    return InstrumentConfig.model_validate(instrument)


def _symbol_slug(symbol: str) -> str:
    return symbol.replace("/", "-")


def _read_parquet_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _enrich_candidate_signal_bars(
    *,
    signal_bars: pd.DataFrame,
    mark_price_bars: pd.DataFrame,
    index_bars: pd.DataFrame,
) -> pd.DataFrame:
    enriched = signal_bars.copy()
    enriched["timestamp"] = pd.to_datetime(enriched["timestamp"], utc=True)
    enriched = _merge_reference_close(enriched, mark_price_bars, target_column="mark_close")
    enriched = _merge_reference_close(enriched, index_bars, target_column="index_close")
    return enriched


def _merge_reference_close(frame: pd.DataFrame, reference: pd.DataFrame, *, target_column: str) -> pd.DataFrame:
    if reference.empty or "timestamp" not in reference.columns or "close" not in reference.columns:
        return frame
    prepared = frame.sort_values("timestamp").copy()
    lookup = reference[["timestamp", "close"]].copy()
    lookup["timestamp"] = pd.to_datetime(lookup["timestamp"], utc=True)
    lookup["close"] = pd.to_numeric(lookup["close"], errors="coerce")
    lookup = lookup.dropna(subset=["timestamp"]).sort_values("timestamp").rename(columns={"close": target_column})
    return pd.merge_asof(prepared, lookup, on="timestamp", direction="backward")
