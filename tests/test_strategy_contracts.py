from __future__ import annotations

import pandas as pd

from quant_lab.config import StrategyConfig
from quant_lab.strategies.ema_trend import prepare_signal_frame
from quant_lab.strategy_contracts import execution_signal_from_row


def test_prepare_signal_frame_attaches_contract_columns() -> None:
    config = StrategyConfig(name="breakout_retest_4h", variant="breakout_retest_regime")
    frame = prepare_signal_frame(_signal_bars(range(100, 220)), config)

    latest = frame.iloc[-1]
    assert "alpha_side" in frame.columns
    assert "alpha_score" in frame.columns
    assert "alpha_regime" in frame.columns
    assert "risk_stop_distance" in frame.columns
    assert "risk_stop_price" in frame.columns
    assert "risk_multiplier" in frame.columns
    assert "execution_desired_side" in frame.columns
    assert latest["execution_desired_side"] == latest["desired_side"]
    assert latest["risk_stop_distance"] == latest["stop_distance"]
    assert latest["contract_strategy_name"] == "breakout_retest_4h"
    assert latest["contract_strategy_variant"] == "breakout_retest_regime"


def test_execution_signal_from_row_builds_alpha_and_risk_contracts() -> None:
    row = pd.Series(
        {
            "timestamp": pd.Timestamp("2025-01-01T00:00:00Z"),
            "execution_desired_side": 1,
            "alpha_side": 1,
            "alpha_score": 42.5,
            "alpha_regime": 1,
            "risk_stop_distance": 15.0,
            "risk_stop_price": 98.5,
            "risk_multiplier": 0.85,
            "route_key": "bull_trend",
        }
    )

    contract = execution_signal_from_row(
        row,
        previous_side=0,
        strategy_name="breakout_retest_4h",
        strategy_variant="breakout_retest_regime",
    )

    assert contract.desired_side == 1
    assert contract.side_changed is True
    assert contract.alpha_signal.side == 1
    assert contract.alpha_signal.score == 42.5
    assert contract.alpha_signal.regime == "bull_trend"
    assert contract.risk_signal.stop_distance == 15.0
    assert contract.risk_signal.stop_price == 98.5
    assert contract.risk_signal.risk_multiplier == 0.85
    assert contract.route_key == "bull_trend"


def _signal_bars(closes) -> pd.DataFrame:
    closes = list(closes)
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="4h", tz="UTC"),
            "open": [value - 0.8 for value in closes],
            "high": [value + 0.1 for value in closes],
            "low": [value - 1 for value in closes],
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": [500.0] * (len(closes) - 1) + [2_000.0],
        }
    )
