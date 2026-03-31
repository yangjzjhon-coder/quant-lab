from __future__ import annotations

from pathlib import Path

import typer
from fastapi.testclient import TestClient

from quant_lab.config import AppConfig, DatabaseConfig, InstrumentConfig, OkxConfig, ServiceConfig, StorageConfig, StrategyConfig
from quant_lab.service.database import init_db, make_session_factory
from quant_lab.service.monitor import build_service_app


def test_client_dashboard_and_api_routes_work(tmp_path: Path, monkeypatch) -> None:
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    monkeypatch.setattr(
        "quant_lab.service.client_ops.build_client_snapshot",
        lambda config, session_factory, project_root: {
            "preflight": {"demo_trading": {"mode": "submit_ready", "ready": True}},
            "reconcile": {"checks": {"leverage_match": True, "size_match": False}},
        },
    )
    monkeypatch.setattr(
        "quant_lab.service.client_ops.run_client_align_leverage",
        lambda **kwargs: {
            "target_leverage": "3",
            "apply_requested": kwargs["apply"],
            "used_stop_rearm": kwargs["rearm_protective_stop"],
        },
    )
    monkeypatch.setattr(
        "quant_lab.service.client_ops.run_client_alert_test",
        lambda **kwargs: {"sent": True, "channels": ["email"], "message": kwargs["message"]},
    )

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        runtime_page = client.get("/")
        assert runtime_page.status_code == 200
        assert "/client" in runtime_page.text
        assert "运行总览" in runtime_page.text

        client_page = client.get("/client")
        assert client_page.status_code == 200
        assert "apiErrorSummary" in client_page.text
        assert "buildApiError" in client_page.text
        assert "requestJson(path, options = {})" in client_page.text
        assert "function formatMetricLines(metrics, options = {})" in client_page.text
        assert "heartbeatSummary" in client_page.text
        assert "renderClientLoadError(error)" in client_page.text
        assert "runClientRequest(" in client_page.text
        assert "requested_total_risk_pct" in client_page.text
        assert "allocated_total_risk_pct" in client_page.text
        assert "budgeted_equity_total" in client_page.text
        assert "portfolio_risk" in client_page.text
        assert "planning_account" in client_page.text
        assert "策略可视化回测" in client_page.text
        assert "visual-reports-feed" in client_page.text
        assert "quant-lab 本地客户端" in client_page.text
        assert "应用杠杆校准" in client_page.text
        assert "模拟执行历史" in client_page.text
        assert "最近事件" in client_page.text
        assert "发送测试告警" in client_page.text
        assert "标的状态" in client_page.text
        assert "snapshot.headline_summary" in client_page.text
        assert "item.action_label" in client_page.text

        snapshot = client.get("/client/snapshot")
        assert snapshot.status_code == 200
        assert snapshot.json()["snapshot"]["preflight"]["demo_trading"]["ready"] is True

        reconcile = client.post("/client/reconcile")
        assert reconcile.status_code == 200
        assert reconcile.json()["snapshot"]["reconcile"]["checks"]["size_match"] is False

        align = client.post(
            "/client/align-leverage",
            json={"apply": True, "confirm": "OKX_DEMO", "rearm_protective_stop": True},
        )
        assert align.status_code == 200
        payload = align.json()
        assert payload["apply_requested"] is True
        assert payload["used_stop_rearm"] is True

        alert = client.post("/client/alert-test", json={"message": "hello from client"})
        assert alert.status_code == 200
        assert alert.json()["sent"] is True
        assert alert.json()["message"] == "hello from client"


def test_client_align_leverage_api_returns_structured_error_payload(tmp_path: Path, monkeypatch) -> None:
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    monkeypatch.setattr(
        "quant_lab.service.client_ops.build_client_snapshot",
        lambda config, session_factory, project_root: {
            "preflight": {"demo_trading": {"mode": "plan_only", "ready": False}},
            "reconcile": {"checks": {}},
        },
    )
    monkeypatch.setattr(
        "quant_lab.service.client_ops.run_client_align_leverage",
        lambda **kwargs: (_ for _ in ()).throw(
            typer.BadParameter("Refusing to mutate OKX demo account. Pass --confirm OKX_DEMO to continue.")
        ),
    )

    app = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app) as client:
        response = client.post("/client/align-leverage", json={"apply": True, "confirm": ""})
        assert response.status_code == 400
        payload = response.json()
        assert payload["error_code"] == "cli_validation_error"
        assert payload["error_type"] == "invalid_request"
        assert payload["retryable"] is False


def _runtime_config(tmp_path: Path) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        okx=OkxConfig(use_demo=True),
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
    )
