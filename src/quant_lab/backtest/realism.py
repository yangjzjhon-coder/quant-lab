from __future__ import annotations

import math


def bar_liquidity_quote(
    *,
    price: float,
    contract_value: float,
    volume: float | None = None,
    volume_ccy: float | None = None,
    volume_quote: float | None = None,
) -> float:
    quote = _positive_or_zero(volume_quote)
    if quote > 0:
        return quote

    base_notional = _positive_or_zero(volume_ccy) * max(price, 0.0)
    if base_notional > 0:
        return base_notional

    return _positive_or_zero(volume) * max(contract_value, 0.0) * max(price, 0.0)


def cap_contracts_by_liquidity(
    *,
    desired_contracts: float,
    price: float,
    contract_value: float,
    liquidity_quote: float,
    max_bar_participation: float,
    lot_size: float,
) -> float:
    if desired_contracts <= 0 or price <= 0 or contract_value <= 0 or max_bar_participation <= 0:
        return 0.0

    available_quote = liquidity_quote * max_bar_participation
    if available_quote <= 0:
        return 0.0

    max_contracts = available_quote / (price * contract_value)
    if lot_size <= 0:
        return float(min(desired_contracts, max_contracts))

    steps = math.floor(min(desired_contracts, max_contracts) / lot_size)
    return steps * lot_size


def estimate_fill_bps(
    *,
    price: float,
    high: float,
    low: float,
    order_contracts: float,
    contract_value: float,
    liquidity_quote: float,
    base_slippage_bps: float,
    market_impact_bps: float,
    excess_impact_bps: float,
    volatility_impact_share: float,
    max_bar_participation: float,
) -> float:
    order_notional = max(order_contracts, 0.0) * max(contract_value, 0.0) * max(price, 0.0)
    available_quote = max(liquidity_quote * max(max_bar_participation, 0.0), 1.0)
    participation = order_notional / available_quote if available_quote > 0 else 0.0
    volatility_bps = 0.0 if price <= 0 else max(high - low, 0.0) / price * 10_000

    impact = max(base_slippage_bps, 0.0)
    impact += max(market_impact_bps, 0.0) * math.sqrt(max(participation, 0.0))
    impact += max(volatility_impact_share, 0.0) * max(volatility_bps, 0.0)
    if participation > 1:
        impact += max(excess_impact_bps, 0.0) * (participation - 1)
    return impact


def conservative_funding_change(
    *,
    side: int,
    contracts: float,
    contract_value: float,
    price: float,
    actual_rate: float | None,
    fallback_rate_bps: float,
) -> float:
    notional = max(contracts, 0.0) * max(contract_value, 0.0) * max(price, 0.0)
    if notional <= 0:
        return 0.0
    if actual_rate is None:
        return -(notional * max(fallback_rate_bps, 0.0) / 10_000)
    return -side * notional * float(actual_rate)


def _positive_or_zero(value: float | None) -> float:
    return float(value) if value and value > 0 else 0.0
