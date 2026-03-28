from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select

from quant_lab.alerts.delivery import AlertDeliveryResult
from quant_lab.cli import _run_demo_loop_cycle
from quant_lab.config import AlertsConfig, AppConfig, DatabaseConfig, InstrumentConfig, StorageConfig, StrategyConfig
from quant_lab.execution.planner import (
    AccountSnapshot,
    OrderInstruction,
    OrderPlan,
    PositionSnapshot,
    SignalSnapshot,
)
from quant_lab.service.database import AlertEvent, ServiceHeartbeat, init_db, make_session_factory, session_scope


def test_run_demo_loop_cycle_records_submission_heartbeat_and_alert(tmp_path: Path, monkeypatch) -> None:
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    signal = _signal_snapshot()
    plan = _order_plan(signal)
    account = AccountSnapshot(
        total_equity=10_000.0,
        available_equity=10_000.0,
        currency="USDT",
        source="test",
        account_mode="net_mode",
    )
    position = PositionSnapshot(side=0, contracts=0.0)

    monkeypatch.setattr(
        "quant_lab.cli._load_demo_state",
        lambda cfg: (account, position, {"signal": signal, "plan": plan}),
    )
    monkeypatch.setattr(
        "quant_lab.cli._submit_order_plan",
        lambda cfg, current_plan: [{"purpose": "open_target", "response": {"code": "0"}}],
    )
    monkeypatch.setattr(
        "quant_lab.cli.deliver_alerts",
        lambda alerts_cfg, **kwargs: [
            AlertDeliveryResult(
                channel="telegram",
                status="sent",
                delivered=True,
                delivered_at=datetime.now(timezone.utc),
            )
        ],
    )

    cycle_state, had_error = _run_demo_loop_cycle(
        cfg=config,
        session_factory=session_factory,
        cycle=1,
        submit=True,
        state_path=tmp_path / "demo_state.json",
    )

    assert had_error is False
    assert cycle_state["payload"]["submitted"] is True
    assert cycle_state["payload"]["alerts_sent"] == ["telegram"]
    persisted_state = json.loads((tmp_path / "demo_state.json").read_text(encoding="utf-8"))
    assert persisted_state["last_submission_refs"][0]["purpose"] == "open_target"
    assert persisted_state["last_submission_refs"][0]["order_id"] is None

    with session_scope(session_factory) as session:
        heartbeats = list(session.execute(select(ServiceHeartbeat)).scalars())
        alerts = list(session.execute(select(AlertEvent)).scalars())
        assert len(heartbeats) == 1
        assert heartbeats[0].service_name == "quant-lab-demo-loop"
        assert heartbeats[0].status == "submitted"
        assert heartbeats[0].details["current_contracts"] == 0.0
        assert heartbeats[0].details["total_equity"] == 10_000.0
        assert heartbeats[0].details["latest_price"] == 100_000.0
        assert len(alerts) == 1
        assert alerts[0].event_key == "demo_order_submitted"
        assert alerts[0].channel == "telegram"
        assert alerts[0].status == "sent"


def test_run_demo_loop_cycle_records_error_heartbeat_and_alert(tmp_path: Path, monkeypatch) -> None:
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    monkeypatch.setattr("quant_lab.cli._load_demo_state", lambda cfg: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(
        "quant_lab.cli.deliver_alerts",
        lambda alerts_cfg, **kwargs: [
            AlertDeliveryResult(
                channel="email",
                status="sent",
                delivered=True,
                delivered_at=datetime.now(timezone.utc),
            )
        ],
    )

    cycle_state, had_error = _run_demo_loop_cycle(
        cfg=config,
        session_factory=session_factory,
        cycle=3,
        submit=True,
        state_path=tmp_path / "demo_state.json",
    )

    assert had_error is True
    assert "RuntimeError: boom" in cycle_state["error"]

    with session_scope(session_factory) as session:
        heartbeats = list(session.execute(select(ServiceHeartbeat)).scalars())
        alerts = list(session.execute(select(AlertEvent)).scalars())
        assert len(heartbeats) == 1
        assert heartbeats[0].status == "error"
        assert len(alerts) == 1
        assert alerts[0].event_key == "demo_loop_error"
        assert alerts[0].channel == "email"
        assert alerts[0].status == "sent"


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
        alerts=AlertsConfig(
            send_on_demo_submit=True,
            send_on_demo_error=True,
            demo_error_cooldown_seconds=0,
        ),
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
