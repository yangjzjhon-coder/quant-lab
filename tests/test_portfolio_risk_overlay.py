from __future__ import annotations

from datetime import datetime, timezone

from quant_lab.data.public_factors import PublicFactorSnapshot
from quant_lab.execution.planner import OrderInstruction, OrderPlan, SignalSnapshot
from quant_lab.risk.portfolio import apply_factor_overlay_to_plan, apply_portfolio_risk_caps
from quant_lab.config import InstrumentConfig


def test_apply_factor_overlay_blocks_weak_signal() -> None:
    plan = _plan("BTC-USDT-SWAP", target_contracts=10.0)
    factor = PublicFactorSnapshot(symbol="BTC-USDT-SWAP", score=0.2, confidence=1.0, risk_multiplier=0.55)

    decision = apply_factor_overlay_to_plan(
        symbol="BTC-USDT-SWAP",
        plan=plan,
        lot_size=0.01,
        factor_snapshot=factor,
        min_factor_score=0.35,
    )

    assert decision.blocked is True
    assert plan.target_contracts == 0.0
    assert plan.instructions == []
    assert plan.action == "hold"


def test_apply_portfolio_risk_caps_prefers_higher_priority_symbol() -> None:
    btc_plan = _plan("BTC-USDT-SWAP", target_contracts=100.0)
    eth_plan = _plan("ETH-USDT-SWAP", target_contracts=100.0)
    btc_signal = _signal(strategy_score=42.0)
    eth_signal = _signal(strategy_score=20.0)

    symbol_states = {
        "BTC-USDT-SWAP": {
            "plan": btc_plan,
            "signal": btc_signal,
            "instrument_config": InstrumentConfig(symbol="BTC-USDT-SWAP", contract_value=1.0, lot_size=0.01),
            "public_factor_snapshot": PublicFactorSnapshot(symbol="BTC-USDT-SWAP", score=0.85, confidence=1.0, risk_multiplier=1.15),
        },
        "ETH-USDT-SWAP": {
            "plan": eth_plan,
            "signal": eth_signal,
            "instrument_config": InstrumentConfig(symbol="ETH-USDT-SWAP", contract_value=1.0, lot_size=0.01),
            "public_factor_snapshot": PublicFactorSnapshot(symbol="ETH-USDT-SWAP", score=0.40, confidence=1.0, risk_multiplier=0.75),
        },
    }

    decisions = apply_portfolio_risk_caps(
        symbol_states=symbol_states,
        total_equity=10_000.0,
        portfolio_max_total_risk=0.025,
        portfolio_max_same_direction_risk=0.025,
    )

    assert decisions["BTC-USDT-SWAP"].priority_score > decisions["ETH-USDT-SWAP"].priority_score
    assert btc_plan.target_contracts >= eth_plan.target_contracts
    assert eth_plan.target_contracts < 100.0


def _signal(*, strategy_score: float) -> SignalSnapshot:
    ts = datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc)
    return SignalSnapshot(
        signal_time=ts,
        effective_time=ts,
        latest_execution_time=ts,
        latest_price=100.0,
        latest_high=101.0,
        latest_low=99.0,
        latest_liquidity_quote=1_000_000.0,
        desired_side=1,
        previous_side=0,
        stop_distance=2.0,
        strategy_score=strategy_score,
        ready=True,
    )


def _plan(symbol: str, *, target_contracts: float) -> OrderPlan:
    ts = datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc)
    return OrderPlan(
        action="open",
        reason="test",
        desired_side=1,
        current_side=0,
        current_contracts=0.0,
        target_contracts=target_contracts,
        equity_reference=10_000.0,
        latest_price=100.0,
        entry_price_estimate=100.0,
        stop_price=98.0,
        stop_distance=2.0,
        signal_time=ts,
        effective_time=ts,
        position_mode="net_mode",
        instructions=[
            OrderInstruction(
                purpose="open_target",
                inst_id=symbol,
                td_mode="cross",
                side="buy",
                ord_type="market",
                size=target_contracts,
                reduce_only=False,
                pos_side="net",
                estimated_fill_price=100.0,
                stop_price=98.0,
            )
        ],
    )
