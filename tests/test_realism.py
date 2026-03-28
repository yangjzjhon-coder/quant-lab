from __future__ import annotations

import pandas as pd

from quant_lab.backtest.engine import _raw_liquidation_exit_price
from quant_lab.backtest.realism import (
    bar_liquidity_quote,
    cap_contracts_by_liquidity,
    conservative_funding_change,
    estimate_fill_bps,
)
from quant_lab.models import Position


def test_estimate_fill_bps_increases_with_participation() -> None:
    low = estimate_fill_bps(
        price=100.0,
        high=101.0,
        low=99.0,
        order_contracts=1.0,
        contract_value=1.0,
        liquidity_quote=10_000.0,
        base_slippage_bps=5.0,
        market_impact_bps=12.0,
        excess_impact_bps=18.0,
        volatility_impact_share=0.25,
        max_bar_participation=0.1,
    )
    high = estimate_fill_bps(
        price=100.0,
        high=101.0,
        low=99.0,
        order_contracts=20.0,
        contract_value=1.0,
        liquidity_quote=10_000.0,
        base_slippage_bps=5.0,
        market_impact_bps=12.0,
        excess_impact_bps=18.0,
        volatility_impact_share=0.25,
        max_bar_participation=0.1,
    )

    assert high > low


def test_cap_contracts_by_liquidity_limits_position_size() -> None:
    capped = cap_contracts_by_liquidity(
        desired_contracts=10.0,
        price=100.0,
        contract_value=1.0,
        liquidity_quote=1_000.0,
        max_bar_participation=0.1,
        lot_size=1.0,
    )

    assert capped == 1.0


def test_conservative_funding_penalizes_missing_history() -> None:
    penalty = conservative_funding_change(
        side=1,
        contracts=2.0,
        contract_value=1.0,
        price=100.0,
        actual_rate=None,
        fallback_rate_bps=1.0,
    )

    assert penalty == -0.02


def test_bar_liquidity_quote_prefers_quote_volume() -> None:
    liquidity = bar_liquidity_quote(
        price=100.0,
        contract_value=1.0,
        volume=50.0,
        volume_ccy=5.0,
        volume_quote=900.0,
    )

    assert liquidity == 900.0


def test_raw_liquidation_exit_price_stays_inside_bar_range() -> None:
    position = Position(
        side=1,
        contracts=1.0,
        contract_value=1.0,
        entry_time=pd.Timestamp("2025-01-01T00:00:00Z"),
        signal_time=pd.Timestamp("2025-01-01T00:00:00Z"),
        entry_price=100.0,
        stop_price=90.0,
        entry_fee=0.0,
    )

    price = _raw_liquidation_exit_price(
        position=position,
        cash=20.0,
        fee_rate=0.001,
        bar_high=95.0,
        bar_low=70.0,
        fallback_price=80.0,
    )

    assert 70.0 <= price <= 95.0
