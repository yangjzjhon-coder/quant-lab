from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select

from quant_lab.alerts.delivery import AlertDeliveryResult
from quant_lab.cli import _run_demo_portfolio_loop_cycle
from quant_lab.config import (
    AlertsConfig,
    AppConfig,
    DatabaseConfig,
    InstrumentConfig,
    PortfolioConfig,
    StorageConfig,
    StrategyConfig,
)
from quant_lab.execution.planner import (
    AccountSnapshot,
    OrderInstruction,
    OrderPlan,
    PositionSnapshot,
    SignalSnapshot,
)
from quant_lab.service.database import AlertEvent, ServiceHeartbeat, init_db, make_session_factory, session_scope


def test_run_demo_portfolio_loop_cycle_records_portfolio_heartbeat_and_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    account = AccountSnapshot(
        total_equity=20_000.0,
        available_equity=20_000.0,
        currency="USDT",
        source="test",
        account_mode="net_mode",
    )
    symbol_states = {
        "BTC-USDT-SWAP": _symbol_state("BTC-USDT-SWAP", desired_side=1, target_contracts=12.0),
        "ETH-USDT-SWAP": _symbol_state("ETH-USDT-SWAP", desired_side=1, target_contracts=24.0),
    }

    monkeypatch.setattr(
        "quant_lab.cli._load_demo_portfolio_state",
        lambda cfg, symbols: (account, {symbol: symbol_states[symbol] for symbol in symbols}),
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

    state_path = tmp_path / "demo_portfolio_state.json"
    cycle_state, had_error = _run_demo_portfolio_loop_cycle(
        cfg=config,
        session_factory=session_factory,
        cycle=1,
        submit=True,
        state_path=state_path,
        symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
    )

    assert had_error is False
    assert cycle_state["mode"] == "portfolio"
    assert cycle_state["payload"]["submitted_symbols"] == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    persisted_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted_state["symbols"]["BTC-USDT-SWAP"]["last_plan"]["action"] == "open"
    assert persisted_state["symbols"]["ETH-USDT-SWAP"]["last_submission_refs"][0]["purpose"] == "open_target"

    with session_scope(session_factory) as session:
        heartbeat = session.execute(select(ServiceHeartbeat)).scalar_one()
        alerts = list(session.execute(select(AlertEvent)).scalars())
        assert heartbeat.service_name == "quant-lab-demo-loop"
        assert heartbeat.status == "submitted"
        assert heartbeat.details["mode"] == "portfolio"
        assert heartbeat.details["symbol_count"] == 2
        assert heartbeat.details["submitted_symbol_count"] == 2
        assert heartbeat.details["symbol_states"]["BTC-USDT-SWAP"]["target_contracts"] == 12.0
        assert len(alerts) == 1
        assert alerts[0].event_key == "demo_order_submitted"
        assert alerts[0].channel == "telegram"


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
        alerts=AlertsConfig(
            send_on_demo_submit=True,
            send_on_demo_error=True,
            demo_error_cooldown_seconds=0,
        ),
    )


def _symbol_state(symbol: str, *, desired_side: int, target_contracts: float) -> dict[str, object]:
    signal = _signal_snapshot(desired_side=desired_side)
    position = PositionSnapshot(side=0, contracts=0.0)
    planning_account = AccountSnapshot(
        total_equity=10_000.0,
        available_equity=10_000.0,
        currency="USDT",
        source="test_allocated",
        account_mode="net_mode",
    )
    plan = OrderPlan(
        action="open",
        reason="signal changed",
        desired_side=desired_side,
        current_side=0,
        current_contracts=0.0,
        target_contracts=target_contracts,
        equity_reference=10_000.0,
        latest_price=signal.latest_price,
        entry_price_estimate=signal.latest_price * 1.001,
        stop_price=signal.latest_price - 1500.0,
        stop_distance=1500.0,
        signal_time=signal.signal_time,
        effective_time=signal.effective_time,
        position_mode="net_mode",
        instructions=[
            OrderInstruction(
                purpose="open_target",
                inst_id=symbol,
                td_mode="cross",
                side="buy" if desired_side > 0 else "sell",
                ord_type="market",
                size=target_contracts,
                reduce_only=False,
                pos_side="net",
                estimated_fill_price=signal.latest_price * 1.001,
                stop_price=signal.latest_price - 1500.0,
            )
        ],
    )
    return {
        "account": planning_account,
        "position": position,
        "planning_account": planning_account,
        "instrument_config": InstrumentConfig(symbol=symbol, settle_currency="USDT"),
        "signal": signal,
        "plan": plan,
    }


def _signal_snapshot(*, desired_side: int) -> SignalSnapshot:
    signal_time = _utc("2025-01-21T00:00:00+00:00")
    return SignalSnapshot(
        signal_time=signal_time,
        effective_time=signal_time,
        latest_execution_time=signal_time,
        latest_price=100_000.0 if desired_side > 0 else 99_000.0,
        latest_high=100_500.0,
        latest_low=99_500.0,
        latest_liquidity_quote=1_500_000.0,
        desired_side=desired_side,
        previous_side=0,
        stop_distance=1_500.0,
        ready=True,
    )


def _utc(raw: str):
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
