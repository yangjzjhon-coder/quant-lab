from __future__ import annotations

from dataclasses import dataclass
from math import floor

import pandas as pd


def position_size_from_risk(
    equity: float,
    entry_price: float,
    stop_price: float,
    risk_fraction: float,
    max_leverage: float,
    contract_value: float,
    lot_size: float,
    min_size: float,
    minimum_notional: float,
) -> float:
    if equity <= 0 or entry_price <= 0 or contract_value <= 0:
        return 0.0

    unit_risk = abs(entry_price - stop_price) * contract_value
    if unit_risk <= 0:
        return 0.0

    risk_budget = equity * risk_fraction
    raw_contracts = risk_budget / unit_risk

    max_notional = equity * max_leverage
    max_contracts = max_notional / (entry_price * contract_value)
    capped_contracts = min(raw_contracts, max_contracts)

    rounded_contracts = _round_down_to_lot(capped_contracts, lot_size)
    notional = rounded_contracts * contract_value * entry_price

    if rounded_contracts < min_size:
        return 0.0
    if notional < minimum_notional:
        return 0.0
    return rounded_contracts


@dataclass
class WeeklyDrawdownGuard:
    threshold: float
    current_week: tuple[int, int] | None = None
    weekly_peak: float = 0.0
    halted: bool = False
    resume_equity: float | None = None

    def update(self, timestamp: pd.Timestamp, equity: float) -> bool:
        iso = timestamp.isocalendar()
        week_id = (int(iso.year), int(iso.week))

        if self.halted and self.resume_equity is not None and equity >= self.resume_equity:
            self.halted = False
            self.resume_equity = None
            self.current_week = week_id
            self.weekly_peak = equity
            return False

        if self.current_week != week_id:
            self.current_week = week_id
            if not self.halted:
                self.weekly_peak = equity

        self.weekly_peak = max(self.weekly_peak, equity)
        if not self.halted and self.weekly_peak > 0:
            drawdown = (self.weekly_peak - equity) / self.weekly_peak
            if drawdown >= self.threshold:
                self.halted = True
                self.resume_equity = self.weekly_peak * (1.0 - self.threshold)
        return self.halted


def _round_down_to_lot(value: float, lot_size: float) -> float:
    if lot_size <= 0:
        return value
    steps = floor(value / lot_size)
    return steps * lot_size
