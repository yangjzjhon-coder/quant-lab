from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from typer.testing import CliRunner

from quant_lab.cli import _build_demo_reconcile_payload, app
from quant_lab.config import AppConfig, DatabaseConfig, InstrumentConfig, StorageConfig, StrategyConfig, TradingConfig
from quant_lab.execution.planner import AccountSnapshot, OrderPlan, PositionSnapshot, SignalSnapshot


def test_build_demo_reconcile_payload_flags_leverage_mismatch_and_missing_stop(tmp_path: Path) -> None:
    cfg = _runtime_config(tmp_path)
    account = AccountSnapshot(
        total_equity=10_000.0,
        available_equity=10_000.0,
        currency="USDT",
        source="test",
        account_mode="long_short_mode",
        can_trade=True,
    )
    position = PositionSnapshot(side=-1, contracts=12.0, position_mode="long_short_mode")
    signal = _signal_snapshot()
    plan = _order_plan(signal)

    payload = _build_demo_reconcile_payload(
        cfg=cfg,
        account=account,
        position=position,
        signal=signal,
        plan=plan,
        state={
            "leverage_payload": {"data": [{"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "lever": "100"}]},
            "pending_orders_payload": {"data": []},
            "pending_algo_orders_payload": {"data": []},
        },
        executor_state={},
    )

    assert payload["checks"]["trade_permission"] is True
    assert payload["checks"]["position_mode_match"] is True
    assert payload["checks"]["leverage_match"] is False
    assert payload["checks"]["size_match"] is True
    assert payload["checks"]["protective_stop_ready"] is False
    assert any("protective stop order" in warning for warning in payload["warnings"])


def test_demo_reconcile_command_reports_live_stop_tracking(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    cfg = _runtime_config(tmp_path)
    signal = _signal_snapshot()
    plan = _order_plan(signal)
    account = AccountSnapshot(
        total_equity=10_000.0,
        available_equity=10_000.0,
        currency="USDT",
        source="test",
        account_mode="long_short_mode",
        can_trade=True,
    )
    position = PositionSnapshot(side=-1, contracts=12.0, position_mode="long_short_mode")

    monkeypatch.setattr("quant_lab.cli._load_runtime_context", lambda config, project_root: (cfg, object()))
    monkeypatch.setattr("quant_lab.cli.init_db", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "quant_lab.cli._load_demo_state",
        lambda current_cfg, **kwargs: (
            account,
            position,
            {
                "signal": signal,
                "plan": plan,
                "leverage_payload": {
                    "data": [{"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "lever": "3"}]
                },
                "pending_orders_payload": {"data": []},
                "pending_algo_orders_payload": {
                    "data": [
                        {
                            "algoId": "algo-1",
                            "algoClOrdId": "stop-1",
                            "instId": "BTC-USDT-SWAP",
                            "side": "buy",
                            "posSide": "short",
                            "sz": "12",
                            "state": "live",
                            "slTriggerPx": "102000",
                        }
                    ]
                },
            },
        ),
    )
    monkeypatch.setattr(
        "quant_lab.cli._load_executor_state",
        lambda path: {
            "last_submitted_at": "2026-03-25T00:00:00+00:00",
            "last_submission_refs": [{"attach_algo_cl_ord_ids": ["stop-1"]}],
        },
    )

    runner = CliRunner()
    result = runner.invoke(app, ["demo-reconcile", "--config", str(config_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["checks"]["leverage_match"] is True
    assert payload["checks"]["protective_stop_ready"] is True
    assert payload["checks"]["tracked_stop_order_seen"] is True
    assert payload["checks"]["size_match"] is True
    assert payload["exchange"]["executor_tracking"]["matched_live_algo_client_ids"] == ["stop-1"]


def _runtime_config(tmp_path: Path) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP", settle_currency="USDT", lot_size=0.01),
        strategy=StrategyConfig(name="ema_trend_4h"),
        trading=TradingConfig(position_mode="long_short_mode"),
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
        desired_side=-1,
        previous_side=0,
        stop_distance=1_500.0,
        ready=True,
    )


def _order_plan(signal: SignalSnapshot) -> OrderPlan:
    return OrderPlan(
        action="open",
        reason="signal changed",
        desired_side=-1,
        current_side=0,
        current_contracts=0.0,
        target_contracts=12.0,
        equity_reference=10_000.0,
        latest_price=100_000.0,
        entry_price_estimate=99_900.0,
        stop_price=101_400.0,
        stop_distance=1_500.0,
        signal_time=signal.signal_time,
        effective_time=signal.effective_time,
        position_mode="long_short_mode",
    )


def _utc(raw: str):
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
