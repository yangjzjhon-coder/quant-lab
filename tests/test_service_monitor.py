from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient
from sqlalchemy import select

from quant_lab.config import (
    AlertsConfig,
    AppConfig,
    DatabaseConfig,
    InstrumentConfig,
    MarketDataConfig,
    OkxConfig,
    PortfolioConfig,
    ResearchAIConfig,
    ServiceConfig,
    StorageConfig,
    StrategyConfig,
)
from quant_lab.providers.market_data import register_market_data_provider
from quant_lab.service.database import AlertEvent, RuntimeSnapshot, ServiceHeartbeat, init_db, make_session_factory, session_scope
from quant_lab.service.monitor import build_service_app, run_monitor_cycle


def test_run_monitor_cycle_persists_snapshot_and_heartbeat(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        okx=OkxConfig(use_demo=True),
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    artifacts = run_monitor_cycle(config=config, session_factory=session_factory, project_root=tmp_path)
    assert artifacts.snapshot.latest_equity == 10_250.0
    assert bool(artifacts.snapshot.halted) is False
    assert artifacts.alerts_sent == []

    with session_scope(session_factory) as session:
        heartbeats = list(session.execute(select(ServiceHeartbeat)).scalars())
        snapshots = list(session.execute(select(RuntimeSnapshot)).scalars())
        assert len(heartbeats) == 1
        assert len(snapshots) == 1
        assert heartbeats[0].details["report_timestamp"].endswith("+00:00")


def test_service_api_exposes_latest_runtime_snapshot(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        okx=OkxConfig(use_demo=True),
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    run_monitor_cycle(config=config, session_factory=session_factory, project_root=tmp_path)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert "运行总览" in dashboard.text
        assert "apiErrorSummary" in dashboard.text
        assert "buildApiError" in dashboard.text
        assert "requestJson(path, options = {})" in dashboard.text
        assert "function formatMetricLines(metrics, options = {})" in dashboard.text
        assert "heartbeatSummary" in dashboard.text
        assert "heartbeatMetric" in dashboard.text
        assert "summary.demo_mode" in dashboard.text
        assert "task.status_label" in dashboard.text
        assert "item.status_label" in dashboard.text

        response = client.get("/runtime/latest")
        assert response.status_code == 200
        payload = response.json()
        assert payload["snapshot"]["symbol"] == "BTC-USDT-SWAP"
        assert payload["snapshot"]["latest_equity"] == 10250.0
        assert payload["snapshot"]["report_timestamp"].endswith("+00:00")
        assert payload["snapshot"]["created_at"].endswith("+00:00")

        with session_scope(session_factory) as session:
            session.add(
                AlertEvent(
                    event_key="manual_test",
                    channel="email",
                    level="info",
                    title="Manual test",
                    message="hello",
                    status="sent",
                )
            )

        alerts = client.get("/alerts")
        assert alerts.status_code == 200
        assert alerts.json()["alerts"][0]["status_label"] == "已发送"

        heartbeats = client.get("/heartbeats")
        assert heartbeats.status_code == 200
        assert len(heartbeats.json()["heartbeats"]) >= 1
        assert heartbeats.json()["heartbeats"][0]["created_at"].endswith("+00:00")
        assert heartbeats.json()["heartbeats"][0]["status_label"] == "正常"

        preflight = client.get("/runtime/preflight")
        assert preflight.status_code == 200
        payload = preflight.json()
        assert payload["demo_trading"]["mode"] == "plan_only"
        assert payload["demo_trading"]["ready"] is False
        assert payload["alerts"]["any_ready"] is False
        assert payload["dashboard_summary"]["demo_mode"]["label"] == "仅演练"
        assert payload["dashboard_summary"]["loop"]["label"] == "缺失"

        artifacts = client.get("/artifacts")
        assert artifacts.status_code == 200
        assert artifacts.json()["backtest_report"]["exists"] is True
        assert any(item["modified_at"].endswith("+00:00") for item in artifacts.json()["catalog"])
        assert any(item["name"].endswith("_dashboard.html") for item in artifacts.json()["catalog"])

        report = client.get("/reports/backtest")
        assert report.status_code == 200
        assert "Backtest dashboard placeholder" in report.text

        artifact_file = client.get("/artifacts/open/BTC-USDT-SWAP_ema_trend_4h_dashboard.html")
        assert artifact_file.status_code == 200
        assert "Backtest dashboard placeholder" in artifact_file.text

        missing_artifact = client.get("/artifacts/open/not_found.html")
        assert missing_artifact.status_code == 404


def test_service_api_exposes_market_data_and_integration_status(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"

    class _StubProvider:
        provider_name = "stub_feed_service"

        def __init__(self, config: AppConfig) -> None:
            self.config = config

        def close(self) -> None:
            return None

        def missing_configuration(self, *, cfg) -> list[str]:
            return []

        def warnings(self, *, cfg) -> list[str]:
            return []

        def probe(self, *, cfg) -> dict[str, object]:
            return {"ok": True, "endpoint": "feed://service"}

        def capabilities(self, *, cfg, configured: bool) -> list[str]:
            return ["history_candles"] if configured else []

    register_market_data_provider("stub_feed_service", lambda config: _StubProvider(config))

    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        market_data=MarketDataConfig(
            provider="stub_feed_service",
            provider_options={"symbol_map": {"BTC-USDT-SWAP": "BTC-PERP"}},
        ),
        research_ai=ResearchAIConfig(
            enabled=True,
            provider="openai_compatible",
            base_url="https://api.example.com/v1",
            api_key="secret",
            model="gpt-test",
        ),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        market_data = client.get("/market-data/status?probe=true")
        assert market_data.status_code == 200
        market_data_payload = market_data.json()
        assert market_data_payload["provider"] == "stub_feed_service"
        assert market_data_payload["ready"] is True
        assert market_data_payload["probe"]["endpoint"] == "feed://service"
        assert market_data_payload["provider_options_keys"] == ["symbol_map"]

        integrations = client.get("/integrations/overview")
        assert integrations.status_code == 200
        integrations_payload = integrations.json()
        assert integrations_payload["summary"]["total"] == 3
        assert integrations_payload["statuses"]["market_data"]["provider"] == "stub_feed_service"
        assert integrations_payload["statuses"]["research_ai"]["provider"] == "openai_compatible"
        assert integrations_payload["statuses"]["research_agent"]["provider"] == "disabled"


def test_runtime_dashboard_shows_portfolio_header_and_mode(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        portfolio=PortfolioConfig(symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"]),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert "BTC-USDT-SWAP / ETH-USDT-SWAP" in dashboard.text
        assert "组合模式" in dashboard.text
        assert "运行总览" in dashboard.text
        assert "策略可视化回测" in dashboard.text
        assert "visual-reports-feed" in dashboard.text
        assert "组合子报表" in dashboard.text
        assert "portfolio-sleeves-feed" in dashboard.text


def test_run_monitor_cycle_prefers_portfolio_report_artifacts(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    _write_portfolio_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        okx=OkxConfig(use_demo=True),
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        portfolio=PortfolioConfig(symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"]),
        strategy=StrategyConfig(name="breakout_retest_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    artifacts = run_monitor_cycle(config=config, session_factory=session_factory, project_root=tmp_path)

    assert artifacts.snapshot.latest_equity == 20_500.0
    assert artifacts.snapshot.total_return_pct == 2.5
    assert artifacts.snapshot.trade_count == 4


def test_service_api_prefers_portfolio_backtest_artifacts_when_portfolio_mode_enabled(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    _write_portfolio_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        portfolio=PortfolioConfig(symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"]),
        strategy=StrategyConfig(name="breakout_retest_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    run_monitor_cycle(config=config, session_factory=session_factory, project_root=tmp_path)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        artifacts = client.get("/artifacts")
        assert artifacts.status_code == 200
        payload = artifacts.json()
        assert payload["mode"] == "portfolio"
        assert payload["symbols"] == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
        assert payload["backtest_report"]["exists"] is True
        assert payload["backtest_report"]["path"].endswith("portfolio_btc_eth_breakout_retest_4h_dashboard.html")
        assert payload["summary"]["path"].endswith("portfolio_btc_eth_breakout_retest_4h_summary.json")
        assert payload["summary_metrics"]["final_equity"] == 20500.0
        assert len(payload["sleeve_reports"]) == 2
        assert payload["sleeve_reports"][0]["symbol"] == "BTC-USDT-SWAP"
        assert payload["sleeve_reports"][0]["metrics"]["final_equity"] == 10420.0
        assert payload["sleeve_reports"][0]["dashboard"]["exists"] is True
        assert payload["sleeve_reports"][1]["symbol"] == "ETH-USDT-SWAP"
        assert payload["sleeve_reports"][1]["metrics"]["trade_count"] == 2

        report = client.get("/reports/backtest")
        assert report.status_code == 200
        assert "Portfolio dashboard placeholder" in report.text

        artifact_file = client.get("/artifacts/open/portfolio_btc_eth_breakout_retest_4h_dashboard.html")
        assert artifact_file.status_code == 200
        assert "Portfolio dashboard placeholder" in artifact_file.text


def test_runtime_dashboard_exposes_project_tasks_and_project_run_route(tmp_path: Path, monkeypatch) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        okx=OkxConfig(use_demo=True),
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    monkeypatch.setattr(
        "quant_lab.service.project_ops.run_project_task",
        lambda **kwargs: {
            "task": kwargs["task"],
            "mode": "single",
            "symbols": ["BTC-USDT-SWAP"],
            "artifacts": {"dashboard": "data/reports/BTC-USDT-SWAP_ema_trend_4h_dashboard.html"},
        },
    )

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert "项目任务" in dashboard.text
        assert "运行回测" in dashboard.text
        assert "运行研究" in dashboard.text

        response = client.post("/project/run", json={"task": "report"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["result"]["task"] == "report"
        assert payload["task_run"]["created_at"].endswith("+00:00")
        assert payload["task_run"]["started_at"].endswith("+00:00")
        assert payload["task_run"]["finished_at"].endswith("+00:00")
        assert "catalog" in payload["artifacts"]


def test_project_tasks_endpoint_returns_recent_task_runs(tmp_path: Path, monkeypatch) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        okx=OkxConfig(use_demo=True),
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    monkeypatch.setattr(
        "quant_lab.service.project_ops.run_project_task",
        lambda **kwargs: {
            "task": kwargs["task"],
            "mode": "single",
            "symbols": ["BTC-USDT-SWAP"],
        },
    )

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        run_response = client.post("/project/run", json={"task": "backtest"})
        assert run_response.status_code == 200

        response = client.get("/project/tasks?limit=10")
        assert response.status_code == 200
        payload = response.json()
        assert len(payload["tasks"]) == 1
        assert payload["tasks"][0]["task_name"] == "backtest"
        assert payload["tasks"][0]["task_label"] == "回测"
        assert payload["tasks"][0]["status"] == "completed"
        assert payload["tasks"][0]["status_label"] == "已完成"
        assert payload["tasks"][0]["created_at"].endswith("+00:00")
        assert payload["tasks"][0]["started_at"].endswith("+00:00")
        assert payload["tasks"][0]["finished_at"].endswith("+00:00")


def test_project_preflight_endpoint_reports_missing_dependencies(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    for suffix in ("4H.parquet", "1m.parquet", "funding.parquet"):
        (raw_dir / f"BTC-USDT-SWAP_{suffix}").write_text("ok", encoding="utf-8")

    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        portfolio=PortfolioConfig(symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"]),
        strategy=StrategyConfig(name="breakout_retest_4h", signal_bar="4H", execution_bar="1m"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=raw_dir,
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.get("/project/preflight")
        assert response.status_code == 200
        payload = response.json()
        assert payload["tasks"]["sweep"]["ready"] is True
        assert payload["tasks"]["research"]["ready"] is True
        assert payload["tasks"]["backtest"]["ready"] is False
        assert any(item.endswith("ETH-USDT-SWAP_4H.parquet") for item in payload["tasks"]["backtest"]["missing"])
        assert payload["tasks"]["report"]["ready"] is False


def test_project_submit_runs_task_in_background(tmp_path: Path, monkeypatch) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    def _fake_run_project_task(**kwargs):
        time.sleep(0.1)
        return {
            "task": kwargs["task"],
            "mode": "single",
            "symbols": ["BTC-USDT-SWAP"],
        }

    monkeypatch.setattr("quant_lab.service.project_ops.run_project_task", _fake_run_project_task)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.post("/project/submit", json={"task": "sweep"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["task_run"]["task_name"] == "sweep"
        assert payload["task_run"]["status"] in {"queued", "running"}

        deadline = time.time() + 3
        latest: dict[str, object] | None = None
        while time.time() < deadline:
            latest = client.get("/project/tasks?limit=1").json()["tasks"][0]
            if latest["status"] == "completed":
                break
            time.sleep(0.05)

        assert latest is not None
        assert latest["status"] == "completed"
        assert latest["result_payload"]["task"] == "sweep"


def test_service_api_returns_structured_error_for_invalid_project_task(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.post("/project/run", json={"task": "invalid"})
        assert response.status_code == 400
        payload = response.json()
        assert payload["error_code"] == "unsupported_project_task"
        assert payload["error_type"] == "invalid_request"
        assert payload["retryable"] is False
        assert "Unsupported project task" in payload["detail"]


def test_service_api_returns_conflict_payload_for_duplicate_project_submit(tmp_path: Path, monkeypatch) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    def _fake_run_project_task(**kwargs):
        time.sleep(0.2)
        return {
            "task": kwargs["task"],
            "mode": "single",
            "symbols": ["BTC-USDT-SWAP"],
        }

    monkeypatch.setattr("quant_lab.service.project_ops.run_project_task", _fake_run_project_task)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        first = client.post("/project/submit", json={"task": "report"})
        assert first.status_code == 200

        duplicate = client.post("/project/submit", json={"task": "report"})
        assert duplicate.status_code == 409
        payload = duplicate.json()
        assert payload["error_code"] == "project_task_already_active"
        assert payload["error_type"] == "conflict"
        assert payload["retryable"] is False

        deadline = time.time() + 3
        while time.time() < deadline:
            tasks = client.get("/project/tasks?limit=1").json()["tasks"]
            if tasks and tasks[0]["status"] == "completed":
                break
            time.sleep(0.05)


def test_service_api_returns_configuration_payload_for_disabled_research_agent(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.post(
            "/research/agent/run",
            json={"task": "test disabled research agent"},
        )
        assert response.status_code == 409
        payload = response.json()
        assert payload["error_code"] == "research_agent_disabled"
        assert payload["error_type"] == "configuration_error"
        assert payload["retryable"] is False


def test_service_api_returns_not_found_payload_for_missing_strategy_candidate(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.post(
            "/research/candidates/999/approve",
            json={"decision": "approve", "scope": "demo", "reason": "missing candidate"},
        )
        assert response.status_code == 404
        payload = response.json()
        assert payload["error_code"] == "strategy_candidate_not_found"
        assert payload["error_type"] == "not_found"
        assert payload["retryable"] is False


def test_runtime_preflight_surfaces_demo_loop_and_alert_readiness(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        okx=OkxConfig(use_demo=True),
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(
            telegram_enabled=True,
            telegram_bot_token="bot",
            telegram_chat_id="chat",
            email_enabled=True,
            email_from="bot@example.com",
            email_to=["desk@example.com"],
            smtp_host="smtp.example.com",
            smtp_username="mailer",
            smtp_password="secret",
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="plan_only",
                details={"cycle": 7, "action": "open"},
            )
        )
        session.add(
            AlertEvent(
                event_key="manual_test",
                channel="telegram",
                level="info",
                title="Manual test alert",
                message="ok",
                status="sent",
            )
        )
    state_path = tmp_path / "data" / "demo_executor_state.json"
    state_path.write_text(json.dumps({"last_submitted_at": "2026-03-25T00:00:00+00:00"}), encoding="utf-8")

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.get("/runtime/preflight")
        assert response.status_code == 200
        payload = response.json()
        assert payload["alerts"]["any_ready"] is True
        assert payload["alerts"]["channels"]["telegram"]["ready"] is True
        assert payload["alerts"]["channels"]["email"]["ready"] is True
        assert "okx_connectivity" in payload
        assert payload["okx_connectivity"]["proxy_url"] is None
        assert payload["execution_loop"]["latest_heartbeat"]["service_name"] == "quant-lab-demo-loop"
        assert payload["execution_loop"]["latest_heartbeat"]["status_label"] == "仅演练"
        assert payload["execution_loop"]["latest_heartbeat"]["details"]["cycle"] == 7
        assert payload["execution_loop"]["executor_state"]["last_submitted_at"] == "2026-03-25T00:00:00+00:00"
        assert payload["runtime_policy"]["decision_source"] == "quant_lab.application.runtime_policy"
        assert payload["rollout_policy"]["status"] == "inactive"
        assert payload["dashboard_summary"]["demo_mode"]["value"] == "plan_only"
        assert payload["dashboard_summary"]["demo_mode"]["label"] == "仅演练"
        assert payload["dashboard_summary"]["alerts"]["value"] == "telegram, email"
        assert payload["dashboard_summary"]["alerts"]["label"] == "telegram, email"
        assert payload["dashboard_summary"]["loop"]["value"] == "plan_only"
        assert payload["dashboard_summary"]["loop"]["label"] == "仅演练"
        assert payload["dashboard_summary"]["loop"]["note"] == "循环 7 | 开仓"
        assert payload["dashboard_summary"]["status"]["label"] == "plan_only"
        assert payload["dashboard_summary"]["status"]["display_label"] == "仅演练"
        assert payload["dashboard_summary"]["status"]["ok"] is True


def test_runtime_preflight_defaults_live_mode_to_submit_blocked(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.get("/runtime/preflight")
        assert response.status_code == 200
        payload = response.json()
        assert payload["demo_trading"]["mode"] == "submit_blocked"
        assert payload["demo_trading"]["ready"] is False
        assert payload["runtime_policy"]["execution_mode"] == "live"
        assert payload["rollout_policy"]["status"] == "inactive"
        assert "okx.use_demo=false" in payload["demo_trading"]["reasons"]


def test_runtime_preflight_clears_stale_executor_error_after_recovery(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="plan_only",
                details={"cycle": 8, "mode": "portfolio"},
            )
        )

    state_path = tmp_path / "data" / "demo_executor_state.json"
    state_path.write_text(
        json.dumps(
            {
                "last_error": {
                    "cycle": 7,
                    "message": "old unauthorized",
                    "timestamp": "2026-03-27T09:05:01.711340+00:00",
                }
            }
        ),
        encoding="utf-8",
    )

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.get("/runtime/preflight")
        assert response.status_code == 200
        payload = response.json()
        assert payload["execution_loop"]["executor_state"]["last_error"] is None
        assert payload["execution_loop"]["executor_state"]["recovered_error"]["message"] == "old unauthorized"


def test_runtime_preflight_surfaces_okx_connectivity_hint(tmp_path: Path, monkeypatch) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    config.okx.profile = "okx-demo"
    config.okx.config_file = Path("/mnt/c/Users/Administrator/.okx/config.toml")
    config.okx.use_demo = True
    config.okx.proxy_url = "http://172.19.0.1:7897"
    config.okx.api_key = "key"
    config.okx.secret_key = "secret"
    config.okx.passphrase = "passphrase"
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="error",
                details={"error": "HTTPStatusError: Client error '401 Unauthorized' for url 'https://www.okx.com/api/v5/account/config'"},
            )
        )

    monkeypatch.setattr("quant_lab.service.monitor._resolve_proxy_egress_ip", lambda proxy_url: "206.237.119.228")

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.get("/runtime/preflight")
        assert response.status_code == 200
        payload = response.json()
        connectivity = payload["okx_connectivity"]
        assert connectivity["profile"] == "okx-demo"
        assert connectivity["proxy_url"] == "http://172.19.0.1:7897"
        assert connectivity["egress_ip"] == "206.237.119.228"
        assert "401 Unauthorized" in connectivity["latest_auth_error"]
        assert any("206.237.119.228" in item for item in connectivity["notes"])


def test_runtime_preflight_marks_email_not_ready_without_password(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(
            email_enabled=True,
            email_from="bot@example.com",
            email_to=["desk@example.com"],
            smtp_host="smtp.example.com",
            smtp_username="mailer",
            smtp_password=None,
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.get("/runtime/preflight")
        assert response.status_code == 200
        payload = response.json()
        assert payload["alerts"]["channels"]["email"]["enabled"] is True
        assert payload["alerts"]["channels"]["email"]["ready"] is False


def test_runtime_preflight_normalizes_flat_single_legacy_heartbeat_details(tmp_path: Path) -> None:
    _write_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="warning",
                details={
                    "cycle": 5,
                    "action": "flip",
                    "desired_side": 1,
                    "current_side": -1,
                    "current_contracts": 3.0,
                    "target_contracts": 5.0,
                    "latest_price": 71344.0,
                    "signal_time": "2026-03-25T08:00:00+00:00",
                    "effective_time": "2026-03-25T08:01:00+00:00",
                    "position_mode": "net_mode",
                    "response_count": 0,
                    "warning_count": 1,
                    "submitted": False,
                    "total_equity": 12_000.0,
                    "available_equity": 10_500.0,
                    "currency": "USDT",
                },
            )
        )

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.get("/runtime/preflight")
        assert response.status_code == 200
        payload = response.json()
        heartbeat = payload["execution_loop"]["latest_heartbeat"]
        assert heartbeat["service_name"] == "quant-lab-demo-loop"
        assert heartbeat["details"]["summary"]["mode"] == "single"
        assert heartbeat["details"]["plan"]["action"] == "flip"
        assert heartbeat["details"]["plan"]["target_contracts"] == 5.0
        assert heartbeat["details"]["position"]["contracts"] == 3.0
        assert heartbeat["details"]["account"]["total_equity"] == 12_000.0
        assert heartbeat["details"]["signal"]["alpha_signal"]["side"] == 1


def test_runtime_preflight_normalizes_flat_portfolio_legacy_heartbeat_details(tmp_path: Path) -> None:
    _write_portfolio_report_artifacts(tmp_path)
    db_path = tmp_path / "quant_lab.db"
    config = AppConfig(
        okx=OkxConfig(use_demo=True),
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        portfolio=PortfolioConfig(symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"]),
        strategy=StrategyConfig(name="breakout_retest_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data/raw",
            report_dir=tmp_path / "data/reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{db_path.as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        alerts=AlertsConfig(telegram_enabled=False),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="warning",
                details={
                    "mode": "portfolio",
                    "cycle": 7,
                    "action": "1/2 submitted",
                    "symbol_count": 2,
                    "submitted_symbol_count": 1,
                    "actionable_symbol_count": 2,
                    "active_position_symbol_count": 1,
                    "response_count": 1,
                    "warning_count": 1,
                    "total_equity": 20_000.0,
                    "available_equity": 19_000.0,
                    "currency": "USDT",
                    "symbol_states": {
                        "BTC-USDT-SWAP": {
                            "action": "open",
                            "desired_side": 1,
                            "current_side": 0,
                            "current_contracts": 0.0,
                            "target_contracts": 8.0,
                            "latest_price": 71344.0,
                            "signal_time": "2026-03-25T08:00:00+00:00",
                            "effective_time": "2026-03-25T08:01:00+00:00",
                            "position_mode": "net_mode",
                        },
                        "ETH-USDT-SWAP": {
                            "action": "hold",
                            "desired_side": 1,
                            "current_side": 1,
                            "current_contracts": 5.0,
                            "target_contracts": 4.0,
                            "latest_price": 3820.0,
                            "signal_time": "2026-03-25T08:00:00+00:00",
                            "effective_time": "2026-03-25T08:01:00+00:00",
                            "position_mode": "net_mode",
                        },
                    },
                },
            )
        )

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.get("/runtime/preflight")
        assert response.status_code == 200
        payload = response.json()
        heartbeat = payload["execution_loop"]["latest_heartbeat"]
        assert heartbeat["service_name"] == "quant-lab-demo-loop"
        assert heartbeat["status_label"] == "警告"
        assert heartbeat["details"]["summary"]["mode"] == "portfolio"
        assert heartbeat["details"]["summary"]["actionable_symbol_count"] == 2
        assert heartbeat["details"]["account"]["total_equity"] == 20_000.0
        assert heartbeat["details"]["symbol_states"]["BTC-USDT-SWAP"]["plan"]["target_contracts"] == 8.0
        assert heartbeat["details"]["symbol_states"]["ETH-USDT-SWAP"]["position"]["contracts"] == 5.0
        assert heartbeat["details"]["symbol_states"]["BTC-USDT-SWAP"]["signal"]["alpha_signal"]["side"] == 1
        assert payload["dashboard_summary"]["loop"]["mode"] == "portfolio"
        assert payload["dashboard_summary"]["loop"]["value"] == "warning"
        assert payload["dashboard_summary"]["loop"]["label"] == "警告"
        assert payload["dashboard_summary"]["loop"]["note"] == "循环 7 | 组合 2 标的 | 可执行 2 | 持仓中 1"
        assert payload["dashboard_summary"]["status"]["label"] == "portfolio plan_only"
        assert payload["dashboard_summary"]["status"]["display_label"] == "组合 | 仅演练"


def _write_report_artifacts(tmp_path: Path) -> None:
    report_dir = tmp_path / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    prefix = "BTC-USDT-SWAP_ema_trend_4h"

    (report_dir / f"{prefix}_summary.json").write_text(
        json.dumps(
            {
                "initial_equity": 10000.0,
                "final_equity": 10250.0,
                "total_return_pct": 2.5,
                "annualized_return_pct": 15.1,
                "max_drawdown_pct": 1.9,
                "trade_count": 2,
                "win_rate_pct": 50.0,
                "profit_factor": 1.4,
                "sharpe": 1.1,
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=3, freq="h", tz="UTC"),
            "cash": [10000.0, 10050.0, 10250.0],
            "equity": [10000.0, 10040.0, 10250.0],
            "unrealized_pnl": [0.0, -10.0, 0.0],
            "halted": [False, False, False],
            "position_side": [0, 1, 0],
            "contracts": [0.0, 2.0, 0.0],
        }
    ).to_csv(report_dir / f"{prefix}_equity_curve.csv", index=False)
    pd.DataFrame(
        {
            "trade_id": [1],
            "side": ["buy"],
            "entry_time": ["2025-01-01T01:00:00+00:00"],
            "exit_time": ["2025-01-01T02:00:00+00:00"],
            "pnl": [250.0],
        }
    ).to_csv(report_dir / f"{prefix}_trades.csv", index=False)
    (report_dir / f"{prefix}_dashboard.html").write_text(
        "<html><body>Backtest dashboard placeholder</body></html>",
        encoding="utf-8",
    )
    (report_dir / f"{prefix}_sweep_dashboard.html").write_text(
        "<html><body>Sweep dashboard placeholder</body></html>",
        encoding="utf-8",
    )


def _write_portfolio_report_artifacts(tmp_path: Path) -> None:
    report_dir = tmp_path / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    prefix = "portfolio_btc_eth_breakout_retest_4h"

    (report_dir / f"{prefix}_summary.json").write_text(
        json.dumps(
            {
                "initial_equity": 20000.0,
                "final_equity": 20500.0,
                "total_return_pct": 2.5,
                "annualized_return_pct": 11.8,
                "max_drawdown_pct": 3.2,
                "trade_count": 4,
                "win_rate_pct": 50.0,
                "profit_factor": 1.5,
                "sharpe": 1.2,
                "symbols": ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-02-01", periods=3, freq="h", tz="UTC"),
            "cash": [20000.0, 20200.0, 20500.0],
            "equity": [20000.0, 20180.0, 20500.0],
            "unrealized_pnl": [0.0, -20.0, 0.0],
            "halted": [False, False, False],
            "position_side": [0, 1, 0],
            "contracts": [0.0, 4.0, 0.0],
        }
    ).to_csv(report_dir / f"{prefix}_equity_curve.csv", index=False)
    pd.DataFrame(
        {
            "trade_id": [1, 2],
            "symbol": ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
            "side": ["buy", "buy"],
            "entry_time": ["2025-02-01T01:00:00+00:00", "2025-02-01T01:00:00+00:00"],
            "exit_time": ["2025-02-01T02:00:00+00:00", "2025-02-01T02:00:00+00:00"],
            "pnl": [300.0, 200.0],
        }
    ).to_csv(report_dir / f"{prefix}_trades.csv", index=False)
    (report_dir / f"{prefix}_dashboard.html").write_text(
        "<html><body>Portfolio dashboard placeholder</body></html>",
        encoding="utf-8",
    )
    _write_portfolio_sleeve_report_artifact(
        report_dir=report_dir,
        symbol="BTC-USDT-SWAP",
        strategy_name="breakout_retest_4h",
        final_equity=10420.0,
        total_return_pct=4.2,
        max_drawdown_pct=5.1,
        trade_count=2,
        win_rate_pct=50.0,
        sharpe=0.9,
        allocation_pct=50.0,
    )
    _write_portfolio_sleeve_report_artifact(
        report_dir=report_dir,
        symbol="ETH-USDT-SWAP",
        strategy_name="breakout_retest_4h",
        final_equity=10080.0,
        total_return_pct=0.8,
        max_drawdown_pct=3.4,
        trade_count=2,
        win_rate_pct=50.0,
        sharpe=0.5,
        allocation_pct=50.0,
    )


def _write_portfolio_sleeve_report_artifact(
    *,
    report_dir: Path,
    symbol: str,
    strategy_name: str,
    final_equity: float,
    total_return_pct: float,
    max_drawdown_pct: float,
    trade_count: int,
    win_rate_pct: float,
    sharpe: float,
    allocation_pct: float,
) -> None:
    prefix = f"{symbol}_{strategy_name}_sleeve"
    (report_dir / f"{prefix}_summary.json").write_text(
        json.dumps(
            {
                "initial_equity": 10000.0,
                "final_equity": final_equity,
                "total_return_pct": total_return_pct,
                "annualized_return_pct": 3.1,
                "max_drawdown_pct": max_drawdown_pct,
                "trade_count": trade_count,
                "win_rate_pct": win_rate_pct,
                "profit_factor": 1.4,
                "sharpe": sharpe,
                "symbol": symbol,
                "capital_allocation_pct": allocation_pct,
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-02-01", periods=2, freq="h", tz="UTC"),
            "cash": [10000.0, final_equity],
            "equity": [10000.0, final_equity],
            "unrealized_pnl": [0.0, 0.0],
            "halted": [False, False],
            "position_side": [0, 0],
            "contracts": [0.0, 0.0],
        }
    ).to_csv(report_dir / f"{prefix}_equity_curve.csv", index=False)
    pd.DataFrame(
        {
            "trade_id": [1],
            "side": ["buy"],
            "entry_time": ["2025-02-01T01:00:00+00:00"],
            "exit_time": ["2025-02-01T02:00:00+00:00"],
            "pnl": [final_equity - 10000.0],
        }
    ).to_csv(report_dir / f"{prefix}_trades.csv", index=False)
    (report_dir / f"{prefix}_dashboard.html").write_text(
        f"<html><body>{symbol} sleeve dashboard placeholder</body></html>",
        encoding="utf-8",
    )
