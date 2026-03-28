from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from typer.testing import CliRunner

from quant_lab.cli import app
from quant_lab.config import AppConfig, DatabaseConfig, InstrumentConfig, StorageConfig, StrategyConfig
from quant_lab.execution.planner import (
    AccountSnapshot,
    OrderInstruction,
    OrderPlan,
    PositionSnapshot,
    SignalSnapshot,
)


def test_demo_preflight_returns_nonzero_when_submit_not_ready(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    cfg = _runtime_config(tmp_path)

    monkeypatch.setattr("quant_lab.cli._load_runtime_context", lambda config, project_root: (cfg, object()))
    monkeypatch.setattr("quant_lab.cli.init_db", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "quant_lab.cli.build_preflight_payload",
        lambda **kwargs: {
            "demo_trading": {"mode": "plan_only", "ready": False, "reasons": ["missing OKX_API_KEY"]},
            "alerts": {"any_ready": False, "channels": {}},
            "execution_loop": {"latest_heartbeat": None, "executor_state": None},
        },
    )

    runner = CliRunner()
    result = runner.invoke(app, ["demo-preflight", "--config", str(config_path), "--assert-submit-ready"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["demo_trading"]["mode"] == "plan_only"
    assert payload["demo_trading"]["ready"] is False


def test_demo_drill_outputs_runtime_preflight_and_plan(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    cfg = _runtime_config(tmp_path)
    signal = _signal_snapshot()
    plan = _order_plan(signal)

    monkeypatch.setattr("quant_lab.cli._load_runtime_context", lambda config, project_root: (cfg, object()))
    monkeypatch.setattr("quant_lab.cli.init_db", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "quant_lab.cli._run_demo_loop_cycle",
        lambda **kwargs: (
            {
                "account": AccountSnapshot(
                    total_equity=10_000.0,
                    available_equity=10_000.0,
                    currency="USDT",
                    source="test",
                    account_mode="net_mode",
                ),
                "position": PositionSnapshot(side=0, contracts=0.0),
                "signal": signal,
                "plan": plan,
                "payload": {
                    "cycle": 1,
                    "submitted": False,
                    "responses": [],
                    "loop_warnings": [],
                    "executor_state_path": str(tmp_path / "data" / "demo_state.json"),
                    "alerts_sent": [],
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
    result = runner.invoke(app, ["demo-drill", "--config", str(config_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["submit_requested"] is False
    assert payload["runtime_preflight"]["demo_trading"]["mode"] == "plan_only"
    assert payload["drill"]["submitted"] is False
    assert payload["plan"]["action"] == "open"


def _runtime_config(tmp_path: Path) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP", settle_currency="USDT"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
    )


def _signal_snapshot() -> SignalSnapshot:
    signal_time = _utc("2025-01-21T00:00:00+00:00")
    return SignalSnapshot(
        signal_time=signal_time,
        effective_time=signal_time,
        latest_execution_time=signal_time,
        latest_price=100_000.0,
        latest_high=100_500.0,
        latest_low=99_500.0,
        latest_liquidity_quote=1_500_000.0,
        desired_side=1,
        previous_side=0,
        stop_distance=1_500.0,
        ready=True,
    )


def _order_plan(signal: SignalSnapshot) -> OrderPlan:
    return OrderPlan(
        action="open",
        reason="signal changed",
        desired_side=1,
        current_side=0,
        current_contracts=0.0,
        target_contracts=12.0,
        equity_reference=10_000.0,
        latest_price=100_000.0,
        entry_price_estimate=100_100.0,
        stop_price=98_600.0,
        stop_distance=1_500.0,
        signal_time=signal.signal_time,
        effective_time=signal.effective_time,
        position_mode="net_mode",
        instructions=[
            OrderInstruction(
                purpose="open_target",
                inst_id="BTC-USDT-SWAP",
                td_mode="cross",
                side="buy",
                ord_type="market",
                size=12.0,
                reduce_only=False,
                pos_side="net",
                estimated_fill_price=100_100.0,
                stop_price=98_600.0,
            )
        ],
    )


def _utc(raw: str):
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
