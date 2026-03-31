from __future__ import annotations

from datetime import datetime, timezone

from quant_lab.application.demo_support import build_demo_portfolio_payload, load_demo_portfolio_state
from quant_lab.config import AppConfig, ExecutionConfig, InstrumentConfig, PortfolioConfig, RiskConfig, StrategyConfig
from quant_lab.execution.planner import (
    AccountSnapshot,
    OrderInstruction,
    OrderPlan,
    PositionSnapshot,
    SignalSnapshot,
)
from quant_lab.risk.portfolio import PortfolioRiskDecision


def test_load_demo_portfolio_state_uses_allocated_portfolio_budget(monkeypatch) -> None:
    cfg = _runtime_config()
    recorded_allocations: list[float | None] = []

    def _load_symbol_state(
        _cfg,
        symbol,
        *,
        allocated_equity=None,
        **_kwargs,
    ):
        recorded_allocations.append(allocated_equity)
        account = _account(20_000.0, source="live_okx")
        position = PositionSnapshot(side=0, contracts=0.0)
        return account, position, {
            "symbol": symbol,
            "instrument_config": InstrumentConfig(symbol=symbol, settle_currency="USDT", contract_value=1.0, lot_size=0.01),
            "planning_account": account,
            "signal": _signal(strategy_score=40.0 if symbol.startswith("BTC") else 20.0),
            "plan": _plan(symbol, target_contracts=12.0 if symbol.startswith("BTC") else 8.0),
            "router_decision": None,
            "public_factor_snapshot": None,
            "factor_overlay": None,
        }

    def _portfolio_caps(**_kwargs):
        return {
            "BTC-USDT-SWAP": PortfolioRiskDecision(
                symbol="BTC-USDT-SWAP",
                desired_side=1,
                priority_score=40.0,
                base_risk_fraction=0.03,
                scaled_risk_fraction=0.02,
                requested_target_contracts=12.0,
                final_target_contracts=10.0,
                factor_score=None,
                factor_multiplier=None,
                applied_scale=0.833333,
                blocked=False,
                reasons=[],
            ),
            "ETH-USDT-SWAP": PortfolioRiskDecision(
                symbol="ETH-USDT-SWAP",
                desired_side=1,
                priority_score=20.0,
                base_risk_fraction=0.02,
                scaled_risk_fraction=0.01,
                requested_target_contracts=8.0,
                final_target_contracts=4.0,
                factor_score=None,
                factor_multiplier=None,
                applied_scale=0.5,
                blocked=False,
                reasons=[],
            ),
        }

    monkeypatch.setattr("quant_lab.application.demo_support.load_demo_state_for_symbol", _load_symbol_state)
    monkeypatch.setattr("quant_lab.application.demo_support.apply_portfolio_risk_caps", _portfolio_caps)

    account, states = load_demo_portfolio_state(cfg, ["BTC-USDT-SWAP", "ETH-USDT-SWAP"])

    assert account.available_equity == 20_000.0
    assert recorded_allocations == [None, None]
    assert states["BTC-USDT-SWAP"]["planning_account"].available_equity == 20_000.0
    assert states["ETH-USDT-SWAP"]["planning_account"].available_equity == 10_000.0
    assert states["BTC-USDT-SWAP"]["plan"].equity_reference == 20_000.0
    assert states["ETH-USDT-SWAP"]["plan"].equity_reference == 10_000.0


def test_build_demo_portfolio_payload_reports_priority_risk_budget_summary() -> None:
    cfg = _runtime_config()
    account = _account(20_000.0, source="portfolio_base")
    btc_account = _account(20_000.0, source="portfolio_base_portfolio_risk_budget")
    eth_account = _account(10_000.0, source="portfolio_base_portfolio_risk_budget")

    payload = build_demo_portfolio_payload(
        cfg=cfg,
        account=account,
        symbol_states={
            "BTC-USDT-SWAP": _symbol_state(
                symbol="BTC-USDT-SWAP",
                account=account,
                planning_account=btc_account,
                strategy_score=40.0,
                target_contracts=10.0,
                regime="bull_trend",
                decision=PortfolioRiskDecision(
                    symbol="BTC-USDT-SWAP",
                    desired_side=1,
                    priority_score=40.0,
                    base_risk_fraction=0.03,
                    scaled_risk_fraction=0.02,
                    requested_target_contracts=12.0,
                    final_target_contracts=10.0,
                    factor_score=None,
                    factor_multiplier=None,
                    applied_scale=0.833333,
                    blocked=False,
                    reasons=[],
                ),
            ),
            "ETH-USDT-SWAP": _symbol_state(
                symbol="ETH-USDT-SWAP",
                account=account,
                planning_account=eth_account,
                strategy_score=20.0,
                target_contracts=4.0,
                regime="range",
                decision=PortfolioRiskDecision(
                    symbol="ETH-USDT-SWAP",
                    desired_side=1,
                    priority_score=20.0,
                    base_risk_fraction=0.02,
                    scaled_risk_fraction=0.01,
                    requested_target_contracts=8.0,
                    final_target_contracts=4.0,
                    factor_score=None,
                    factor_multiplier=None,
                    applied_scale=0.5,
                    blocked=False,
                    reasons=["portfolio-total risk cap scaled target by 0.50"],
                ),
            ),
        },
        include_exchange_checks=False,
    )

    summary = payload["summary"]
    assert summary["allocation_mode"] == "priority_risk_budget"
    assert summary["requested_total_risk_fraction"] == 0.05
    assert summary["allocated_total_risk_fraction"] == 0.03
    assert summary["requested_total_risk_pct"] == 5.0
    assert summary["allocated_total_risk_pct"] == 3.0
    assert summary["portfolio_total_risk_cap_pct"] == 3.0
    assert summary["same_direction_risk_cap_pct"] == 2.5
    assert summary["planning_equity_reference"] == 20_000.0
    assert summary["budgeted_equity_total"] == 30_000.0
    assert summary["budgeted_symbol_count"] == 2
    assert summary["per_symbol_planning_equity"] == 15_000.0
    assert summary["routed_ready_symbol_count"] == 2
    assert summary["bull_trend_symbol_count"] == 1
    assert summary["range_symbol_count"] == 1
    assert payload["symbol_states"]["BTC-USDT-SWAP"]["planning_account"]["available_equity"] == 20_000.0
    assert payload["symbol_states"]["ETH-USDT-SWAP"]["portfolio_risk"]["scaled_risk_fraction"] == 0.01
    assert payload["symbol_states"]["BTC-USDT-SWAP"]["router_decision"]["route"]["label"] == "BTC-USDT-SWAP:bull_trend"
    assert payload["symbol_states"]["ETH-USDT-SWAP"]["router_decision"]["display"]["status_label"] == "route_ready"


def _runtime_config() -> AppConfig:
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP", settle_currency="USDT"),
        portfolio=PortfolioConfig(symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"]),
        strategy=StrategyConfig(name="breakout_retest_4h"),
        execution=ExecutionConfig(initial_equity=20_000.0),
        risk=RiskConfig(
            risk_per_trade=0.02,
            portfolio_max_total_risk=0.03,
            portfolio_max_same_direction_risk=0.025,
        ),
    )


def _symbol_state(
    *,
    symbol: str,
    account: AccountSnapshot,
    planning_account: AccountSnapshot,
    strategy_score: float,
    target_contracts: float,
    regime: str,
    decision: PortfolioRiskDecision,
) -> dict[str, object]:
    return {
        "account": account,
        "position": PositionSnapshot(side=0, contracts=0.0),
        "planning_account": planning_account,
        "instrument_config": InstrumentConfig(symbol=symbol, settle_currency="USDT", contract_value=1.0, lot_size=0.01),
        "signal": _signal(strategy_score=strategy_score),
        "plan": _plan(symbol, target_contracts=target_contracts),
        "router_decision": {"symbol": symbol, "regime": regime, "ready": True},
        "public_factor_snapshot": None,
        "factor_overlay": None,
        "portfolio_risk": decision,
    }


def _account(equity: float, *, source: str) -> AccountSnapshot:
    return AccountSnapshot(
        total_equity=equity,
        available_equity=equity,
        currency="USDT",
        source=source,
        account_mode="net_mode",
        can_trade=True,
    )


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
        equity_reference=20_000.0,
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
