from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from quant_lab.cli import app
from quant_lab.config import (
    AppConfig,
    DatabaseConfig,
    InstrumentConfig,
    PortfolioConfig,
    StorageConfig,
    StrategyConfig,
)
from quant_lab.execution.planner import AccountSnapshot


def test_demo_portfolio_plan_outputs_portfolio_summary(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    cfg = _runtime_config(tmp_path)

    monkeypatch.setattr("quant_lab.cli._load_runtime_context", lambda config, project_root: (cfg, object()))
    monkeypatch.setattr("quant_lab.cli.init_db", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "quant_lab.cli._load_demo_portfolio_state",
        lambda cfg, symbols, **kwargs: (
            _account_payload(),
            {},
        ),
    )
    monkeypatch.setattr(
        "quant_lab.cli._build_demo_portfolio_payload",
        lambda **kwargs: {
            "mode": "portfolio",
            "summary": {"symbol_count": 2},
            "symbol_states": {
                "BTC-USDT-SWAP": {"plan": {"target_contracts": 12.0}},
                "ETH-USDT-SWAP": {"plan": {"target_contracts": 24.0}},
            },
        },
    )

    runner = CliRunner()
    result = runner.invoke(app, ["demo-portfolio-plan", "--config", str(config_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["mode"] == "portfolio"
    assert payload["summary"]["symbol_count"] == 2
    assert payload["symbol_states"]["BTC-USDT-SWAP"]["plan"]["target_contracts"] == 12.0


def test_demo_portfolio_drill_outputs_runtime_preflight_and_symbols(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    cfg = _runtime_config(tmp_path)

    monkeypatch.setattr("quant_lab.cli._load_runtime_context", lambda config, project_root: (cfg, object()))
    monkeypatch.setattr("quant_lab.cli.init_db", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "quant_lab.cli._run_demo_portfolio_loop_cycle",
        lambda **kwargs: (
            {
                "mode": "portfolio",
                "payload": {
                    "cycle": 1,
                    "submitted_symbols": [],
                    "symbol_payloads": {
                        "BTC-USDT-SWAP": {"action": "hold"},
                        "ETH-USDT-SWAP": {"action": "open"},
                    },
                    "alerts_sent": [],
                    "response_count": 0,
                    "warning_count": 0,
                    "executor_state_path": str(tmp_path / "data" / "demo_state.json"),
                    "status": "plan_only",
                },
            },
            False,
        ),
    )
    monkeypatch.setattr(
        "quant_lab.cli.build_preflight_payload",
        lambda **kwargs: {
            "demo_trading": {"mode": "plan_only", "ready": False, "reasons": ["missing OKX_API_KEY"]},
            "alerts": {"any_ready": False, "channels": {}},
            "execution_loop": {"latest_heartbeat": None, "executor_state": None},
        },
    )

    runner = CliRunner()
    result = runner.invoke(app, ["demo-portfolio-drill", "--config", str(config_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["mode"] == "portfolio"
    assert payload["symbols"] == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    assert payload["runtime_preflight"]["demo_trading"]["mode"] == "plan_only"
    assert payload["drill"]["symbol_payloads"]["ETH-USDT-SWAP"]["action"] == "open"


def _runtime_config(tmp_path: Path) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP", settle_currency="USDT"),
        portfolio=PortfolioConfig(symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"]),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
    )


def _account_payload() -> dict[str, object]:
    return AccountSnapshot(
        total_equity=20_000.0,
        available_equity=20_000.0,
        currency="USDT",
        source="test",
        account_mode="net_mode",
        can_trade=True,
        raw=None,
    )
