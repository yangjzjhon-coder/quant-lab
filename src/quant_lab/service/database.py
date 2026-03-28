from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import JSON, DateTime, Float, Integer, String, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class ServiceHeartbeat(Base):
    __tablename__ = "service_heartbeats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    service_name: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    details: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class RuntimeSnapshot(Base):
    __tablename__ = "runtime_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(64), index=True)
    strategy_name: Mapped[str] = mapped_column(String(128), index=True)
    report_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    report_stale: Mapped[int] = mapped_column(Integer, default=0)
    halted: Mapped[int] = mapped_column(Integer, default=0, index=True)
    latest_equity: Mapped[float] = mapped_column(Float)
    latest_cash: Mapped[float] = mapped_column(Float)
    latest_unrealized_pnl: Mapped[float] = mapped_column(Float)
    total_return_pct: Mapped[float] = mapped_column(Float)
    max_drawdown_pct: Mapped[float] = mapped_column(Float)
    trade_count: Mapped[int] = mapped_column(Integer)
    summary: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class AlertEvent(Base):
    __tablename__ = "alert_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_key: Mapped[str] = mapped_column(String(128), index=True)
    channel: Mapped[str] = mapped_column(String(32), index=True)
    level: Mapped[str] = mapped_column(String(16), index=True)
    title: Mapped[str] = mapped_column(String(255))
    message: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), index=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class ProjectTaskRun(Base):
    __tablename__ = "project_task_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_name: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    request_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    result_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    artifact_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class ResearchTask(Base):
    __tablename__ = "research_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(255), index=True)
    hypothesis: Mapped[str] = mapped_column(Text, default="")
    owner_role: Mapped[str] = mapped_column(String(64), index=True)
    priority: Mapped[str] = mapped_column(String(32), index=True, default="medium")
    status: Mapped[str] = mapped_column(String(32), index=True, default="proposed")
    symbols: Mapped[list[str]] = mapped_column(JSON, default=list)
    notes: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        onupdate=_utcnow,
        index=True,
    )
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


class StrategyCandidate(Base):
    __tablename__ = "strategy_candidates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    candidate_name: Mapped[str] = mapped_column(String(255), index=True)
    strategy_name: Mapped[str] = mapped_column(String(128), index=True)
    variant: Mapped[str] = mapped_column(String(128), index=True)
    timeframe: Mapped[str] = mapped_column(String(32), index=True)
    symbol_scope: Mapped[list[str]] = mapped_column(JSON, default=list)
    config_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    author_role: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True, default="draft")
    thesis: Mapped[str] = mapped_column(Text, default="")
    tags: Mapped[list[str]] = mapped_column(JSON, default=list)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    latest_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    latest_evaluation_status: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    latest_decision: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    approval_scope: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        onupdate=_utcnow,
        index=True,
    )


class EvaluationReport(Base):
    __tablename__ = "evaluation_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_id: Mapped[int] = mapped_column(Integer, index=True)
    evaluator_role: Mapped[str] = mapped_column(String(64), index=True)
    evaluation_type: Mapped[str] = mapped_column(String(64), index=True, default="backtest")
    status: Mapped[str] = mapped_column(String(32), index=True)
    score_total: Mapped[float] = mapped_column(Float)
    summary_metrics: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    artifact_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    notes: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


class ApprovalDecision(Base):
    __tablename__ = "approval_decisions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_id: Mapped[int] = mapped_column(Integer, index=True)
    decider_role: Mapped[str] = mapped_column(String(64), index=True)
    decision: Mapped[str] = mapped_column(String(32), index=True)
    scope: Mapped[str] = mapped_column(String(32), index=True, default="demo")
    reason: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)


def make_engine(database_url: str):
    engine_kwargs: dict[str, Any] = {"future": True}
    if database_url.startswith("sqlite:///"):
        engine_kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(database_url, **engine_kwargs)


def make_session_factory(database_url: str):
    engine = make_engine(database_url)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)


def init_db(database_url: str) -> None:
    _ensure_sqlite_parent(database_url)
    engine = make_engine(database_url)
    Base.metadata.create_all(engine)


@contextmanager
def session_scope(session_factory):
    session: Session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _ensure_sqlite_parent(database_url: str) -> None:
    prefix = "sqlite:///"
    if not database_url.startswith(prefix):
        return
    db_path = Path(database_url[len(prefix) :])
    db_path.parent.mkdir(parents=True, exist_ok=True)
