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
    PortfolioConfig,
    ServiceConfig,
    StorageConfig,
    StrategyConfig,
)
from quant_lab.service.database import AlertEvent, RuntimeSnapshot, ServiceHeartbeat, init_db, make_session_factory, session_scope
from quant_lab.service.monitor import build_service_app, run_monitor_cycle


def test_run_monitor_cycle_persists_snapshot_and_heartbeat(tmp_path: Path) -> None:
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


def test_service_api_exposes_latest_runtime_snapshot(tmp_path: Path) -> None:
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
    run_monitor_cycle(config=config, session_factory=session_factory, project_root=tmp_path)

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert "quant-lab runtime" in dashboard.text

        response = client.get("/runtime/latest")
        assert response.status_code == 200
        payload = response.json()
        assert payload["snapshot"]["symbol"] == "BTC-USDT-SWAP"
        assert payload["snapshot"]["latest_equity"] == 10250.0
        assert payload["snapshot"]["report_timestamp"].endswith("+00:00")
        assert payload["snapshot"]["created_at"].endswith("+00:00")

        heartbeats = client.get("/heartbeats")
        assert heartbeats.status_code == 200
        assert len(heartbeats.json()["heartbeats"]) >= 1

        preflight = client.get("/runtime/preflight")
        assert preflight.status_code == 200
        payload = preflight.json()
        assert payload["demo_trading"]["mode"] == "plan_only"
        assert payload["demo_trading"]["ready"] is False
        assert payload["alerts"]["any_ready"] is False

        artifacts = client.get("/artifacts")
        assert artifacts.status_code == 200
        assert artifacts.json()["backtest_report"]["exists"] is True
        assert any(item["name"].endswith("_dashboard.html") for item in artifacts.json()["catalog"])

        report = client.get("/reports/backtest")
        assert report.status_code == 200
        assert "Backtest dashboard placeholder" in report.text

        artifact_file = client.get("/artifacts/open/BTC-USDT-SWAP_ema_trend_4h_dashboard.html")
        assert artifact_file.status_code == 200
        assert "Backtest dashboard placeholder" in artifact_file.text

        missing_artifact = client.get("/artifacts/open/not_found.html")
        assert missing_artifact.status_code == 404


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
        assert "Runtime Mode" in dashboard.text
        assert "Portfolio Sleeves" in dashboard.text
        assert "portfolio-sleeves-feed" in dashboard.text


def test_run_monitor_cycle_prefers_portfolio_report_artifacts(tmp_path: Path) -> None:
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
        assert "Project Tasks" in dashboard.text
        assert "Run Backtest" in dashboard.text
        assert "Run Research" in dashboard.text

        response = client.post("/project/run", json={"task": "report"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["result"]["task"] == "report"
        assert "catalog" in payload["artifacts"]


def test_project_tasks_endpoint_returns_recent_task_runs(tmp_path: Path, monkeypatch) -> None:
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
        assert payload["tasks"][0]["status"] == "completed"


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


def test_runtime_preflight_surfaces_demo_loop_and_alert_readiness(tmp_path: Path) -> None:
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
        assert payload["execution_loop"]["latest_heartbeat"]["details"]["cycle"] == 7
        assert payload["execution_loop"]["executor_state"]["last_submitted_at"] == "2026-03-25T00:00:00+00:00"


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
