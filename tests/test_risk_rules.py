import pandas as pd

from quant_lab.risk.rules import WeeklyDrawdownGuard, position_size_from_risk


def test_position_size_from_risk_caps_to_leverage_and_rounds_to_lot() -> None:
    contracts = position_size_from_risk(
        equity=10_000,
        entry_price=100_000,
        stop_price=99_000,
        risk_fraction=0.02,
        max_leverage=3.0,
        contract_value=0.01,
        lot_size=1.0,
        min_size=1.0,
        minimum_notional=25.0,
    )

    assert contracts == 20.0


def test_weekly_drawdown_guard_halts_and_resets_next_week() -> None:
    guard = WeeklyDrawdownGuard(threshold=0.06)
    monday = pd.Timestamp("2025-01-06T00:00:00Z")
    tuesday = pd.Timestamp("2025-01-07T00:00:00Z")
    next_monday = pd.Timestamp("2025-01-13T00:00:00Z")

    assert guard.update(monday, 10_000) is False
    assert guard.update(tuesday, 9_390) is True
    assert guard.update(next_monday, 9_500) is False
