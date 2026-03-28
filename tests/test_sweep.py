from __future__ import annotations

import pandas as pd

from quant_lab.backtest.sweep import run_parameter_sweep
from quant_lab.config import ExecutionConfig, InstrumentConfig, RiskConfig, StrategyConfig


def test_run_parameter_sweep_skips_invalid_ema_pairs() -> None:
    closes = list(range(100, 260))
    signal_bars = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="4h", tz="UTC"),
            "open": closes,
            "high": [value + 1 for value in closes],
            "low": [value - 1 for value in closes],
            "close": closes,
            "volume": [10.0] * len(closes),
        }
    )
    execution_bars = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=50_000, freq="min", tz="UTC"),
            "open": [100.0] * 50_000,
            "high": [101.0] * 50_000,
            "low": [99.0] * 50_000,
            "close": [100.0] * 50_000,
            "volume": [10.0] * 50_000,
        }
    )
    funding = pd.DataFrame(columns=["timestamp", "funding_rate", "realized_rate"])

    results = run_parameter_sweep(
        signal_bars=signal_bars,
        execution_bars=execution_bars,
        funding_rates=funding,
        strategy_config=StrategyConfig(),
        execution_config=ExecutionConfig(),
        risk_config=RiskConfig(),
        instrument_config=InstrumentConfig(lot_size=0.01, min_size=0.01, tick_size=0.1, settle_currency="USDT"),
        fast_values=[10, 20],
        slow_values=[10, 30],
        atr_values=[1.5, 2.0],
    )

    assert len(results) == 4
    assert (results["fast_ema"] < results["slow_ema"]).all()
    assert "score_return_over_dd" in results.columns
