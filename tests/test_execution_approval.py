from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient
from typer.testing import CliRunner

from quant_lab.cli import app
from quant_lab.config import (
    AppConfig,
    DatabaseConfig,
    InstrumentConfig,
    OkxConfig,
    ServiceConfig,
    StorageConfig,
    StrategyConfig,
    TradingConfig,
)
from quant_lab.service.database import StrategyCandidate, init_db, make_session_factory, session_scope
from quant_lab.service.monitor import build_service_app


def test_runtime_preflight_blocks_submit_when_approved_candidate_is_required_but_missing(tmp_path: Path) -> None:
    config = _runtime_config(
        tmp_path,
        trading=TradingConfig(
            allow_order_placement=True,
            require_approved_candidate=True,
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app_instance = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app_instance) as client:
        response = client.get("/runtime/preflight")
        assert response.status_code == 200
        payload = response.json()
        assert payload["demo_trading"]["checks"]["approved_candidate_gate"] is False
        assert payload["demo_trading"]["ready"] is False
        assert payload["execution_approval"]["required"] is True
        assert payload["execution_approval"]["ready"] is False
        assert any("execution approval:" in item for item in payload["demo_trading"]["reasons"])


def test_runtime_preflight_allows_submit_when_bound_candidate_is_approved(tmp_path: Path) -> None:
    config = _runtime_config(
        tmp_path,
        trading=TradingConfig(
            allow_order_placement=True,
            require_approved_candidate=True,
            execution_candidate_id=1,
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    with session_scope(session_factory) as session:
        session.add(
            StrategyCandidate(
                id=1,
                candidate_name="btc_ema_cross_demo",
                strategy_name="ema_trend_4h",
                variant="ema_cross",
                timeframe="4H",
                symbol_scope=["BTC-USDT-SWAP"],
                config_path="config/settings.yaml",
                author_role="strategy_builder",
                status="approved",
                thesis="Approved for demo execution.",
                tags=["demo"],
                details={},
                latest_score=78.5,
                latest_evaluation_status="evaluation_passed",
                latest_decision="approve",
                approval_scope="demo",
            )
        )

    app_instance = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app_instance) as client:
        response = client.get("/runtime/preflight")
        assert response.status_code == 200
        payload = response.json()
        assert payload["demo_trading"]["checks"]["approved_candidate_gate"] is True
        assert payload["demo_trading"]["ready"] is True
        assert payload["execution_approval"]["ready"] is True
        assert payload["execution_approval"]["candidate"]["id"] == 1
        assert payload["execution_approval"]["candidate"]["candidate_name"] == "btc_ema_cross_demo"


def test_research_bind_candidate_updates_trading_section_in_yaml(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("trading:\n  allow_order_placement: false\n", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research-bind-candidate",
            "--config",
            str(config_path),
            "--candidate-id",
            "12",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["updated_trading"]["execution_candidate_id"] == 12
    updated = config_path.read_text(encoding="utf-8")
    assert "require_approved_candidate: true" in updated
    assert "execution_candidate_id: 12" in updated


def test_runtime_preflight_accepts_valid_strategy_router_pool(tmp_path: Path) -> None:
    config = _runtime_config(
        tmp_path,
        trading=TradingConfig(
            allow_order_placement=True,
            require_approved_candidate=True,
            strategy_router_enabled=True,
            execution_candidate_map={"bull_trend": 7},
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    with session_scope(session_factory) as session:
        session.add(
            StrategyCandidate(
                id=7,
                candidate_name="btc_router_candidate",
                strategy_name="ema_trend_4h",
                variant="ema_cross",
                timeframe="4H",
                symbol_scope=["BTC-USDT-SWAP"],
                config_path="config/router.yaml",
                author_role="strategy_builder",
                status="approved",
                thesis="Router pool candidate.",
                tags=["router"],
                details={},
                latest_score=79.0,
                latest_evaluation_status="evaluation_passed",
                latest_decision="approve",
                approval_scope="demo",
            )
        )

    app_instance = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app_instance) as client:
        response = client.get("/runtime/preflight")
        assert response.status_code == 200
        payload = response.json()
        assert payload["demo_trading"]["checks"]["approved_candidate_gate"] is True
        assert payload["execution_approval"]["router_enabled"] is True
        assert payload["execution_approval"]["ready"] is True
        assert payload["strategy_router"]["enabled"] is True
        assert payload["strategy_router"]["routes"][0]["route_key"] == "bull_trend"


def _runtime_config(tmp_path: Path, *, trading: TradingConfig) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        okx=OkxConfig(
            use_demo=True,
            api_key="key",
            secret_key="secret",
            passphrase="pass",
        ),
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h", variant="ema_cross", signal_bar="4H"),
        trading=trading,
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
    )
