from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select

from quant_lab.alerts.delivery import AlertDeliveryResult
from quant_lab.cli import _run_demo_loop_cycle
from quant_lab.config import (
    AlertsConfig,
    AppConfig,
    DatabaseConfig,
    InstrumentConfig,
    OkxConfig,
    StorageConfig,
    StrategyConfig,
    TradingConfig,
)
from quant_lab.execution.planner import (
    AccountSnapshot,
    OrderInstruction,
    OrderPlan,
    PositionSnapshot,
    SignalSnapshot,
)
from quant_lab.service.database import AlertEvent, ServiceHeartbeat, init_db, make_session_factory, session_scope
from quant_lab.service.demo_runtime import normalize_demo_heartbeat_contract, normalize_demo_heartbeat_details


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
        assert heartbeats[0].service_name == "quant-lab-demo-loop-single"
        assert heartbeats[0].status == "submitted"
        assert "current_contracts" not in heartbeats[0].details
        assert "total_equity" not in heartbeats[0].details
        assert "latest_price" not in heartbeats[0].details
        assert heartbeats[0].details["summary"]["mode"] == "single"
        assert heartbeats[0].details["plan"]["action"] == "open"
        assert heartbeats[0].details["plan"]["latest_price"] == 100_000.0
        assert heartbeats[0].details["signal"]["desired_side"] == 1
        assert heartbeats[0].details["signal"]["latest_price"] == 100_000.0
        assert heartbeats[0].details["account"]["available_equity"] == 10_000.0
        assert heartbeats[0].details["account"]["total_equity"] == 10_000.0
        assert heartbeats[0].details["position"]["contracts"] == 0.0
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
        assert heartbeats[0].details["summary"]["status"] == "error"
        assert heartbeats[0].details["executor_state"]["path"] == str(tmp_path / "demo_state.json")
        assert "RuntimeError: boom" in heartbeats[0].details["error"]
        assert "cycle" not in heartbeats[0].details
        assert len(alerts) == 1
        assert alerts[0].event_key == "demo_loop_error"
        assert alerts[0].channel == "email"
        assert alerts[0].status == "sent"


def test_normalize_demo_heartbeat_contract_strips_single_compatibility_aliases() -> None:
    payload = normalize_demo_heartbeat_contract(
        {
            "cycle": 5,
            "action": "flip",
            "desired_side": 1,
            "current_side": -1,
            "current_contracts": 3.0,
            "target_contracts": 5.0,
            "latest_price": 71_344.0,
            "signal_time": "2026-03-25T08:00:00+00:00",
            "effective_time": "2026-03-25T08:01:00+00:00",
            "response_count": 0,
            "warning_count": 1,
            "submitted": False,
            "total_equity": 12_000.0,
            "available_equity": 10_500.0,
            "currency": "USDT",
        },
        status="warning",
    )

    assert payload["mode"] == "single"
    assert payload["summary"]["cycle"] == 5
    assert payload["plan"]["action"] == "flip"
    assert payload["plan"]["target_contracts"] == 5.0
    assert payload["position"]["contracts"] == 3.0
    assert payload["account"]["total_equity"] == 12_000.0
    assert payload["signal"]["alpha_signal"]["side"] == 1
    assert "cycle" not in payload
    assert "action" not in payload
    assert "current_contracts" not in payload
    assert "target_contracts" not in payload
    assert "total_equity" not in payload


def test_normalize_demo_heartbeat_details_whitelists_single_compatibility_aliases() -> None:
    payload = normalize_demo_heartbeat_details(
        {
            "cycle": 5,
            "action": "flip",
            "desired_side": 1,
            "current_side": -1,
            "current_contracts": 3.0,
            "target_contracts": 5.0,
            "latest_price": 71_344.0,
            "signal_time": "2026-03-25T08:00:00+00:00",
            "effective_time": "2026-03-25T08:01:00+00:00",
            "position_mode": "net_mode",
            "response_count": 0,
            "warning_count": 1,
            "submitted": False,
            "total_equity": 12_000.0,
            "available_equity": 10_500.0,
            "currency": "USDT",
            "unexpected_field": "should_not_leak",
        },
        status="warning",
    )

    assert payload["cycle"] == 5
    assert payload["action"] == "flip"
    assert payload["current_contracts"] == 3.0
    assert payload["target_contracts"] == 5.0
    assert payload["total_equity"] == 12_000.0
    assert payload["summary"]["mode"] == "single"
    assert payload["account"]["currency"] == "USDT"
    assert "submitted" in payload
    assert "response_count" in payload
    assert "warning_count" in payload
    assert "position_mode" not in payload
    assert "currency" not in payload
    assert "unexpected_field" not in payload
    assert "reason" not in payload
    assert "desired_side" not in payload
    assert "current_side" not in payload
    assert "latest_price" not in payload
    assert "signal_time" not in payload
    assert "effective_time" not in payload
    assert "executor_state_path" not in payload
    assert "executor_state_status" not in payload
    assert set(payload) == {
        "mode",
        "summary",
        "account",
        "position",
        "planning_account",
        "signal",
        "plan",
        "cycle",
        "status",
        "submitted",
        "response_count",
        "warning_count",
        "action",
        "current_contracts",
        "target_contracts",
        "total_equity",
        "available_equity",
    }


def _runtime_config(tmp_path: Path) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        okx=OkxConfig(
            use_demo=True,
            api_key="demo-key",
            secret_key="demo-secret",
            passphrase="demo-passphrase",
        ),
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP", settle_currency="USDT"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        trading=TradingConfig(allow_order_placement=True),
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
