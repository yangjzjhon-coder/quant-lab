from __future__ import annotations

import pandas as pd

from quant_lab.backtest.engine import run_backtest_from_signal_frame
from quant_lab.backtest.sweep import _run_backtest_summary_only
from quant_lab.config import ExecutionConfig, InstrumentConfig, RiskConfig, StrategyConfig


def test_run_backtest_aligns_funding_to_next_execution_bar() -> None:
    signal_frame = _signal_frame()
    execution_bars = _execution_bars()
    funding_rates = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2025-01-01T08:00:00.250Z")],
            "realized_rate": [0.001],
        }
    )
    execution_config = _execution_config()
    instrument_config = _instrument_config()

    artifacts = run_backtest_from_signal_frame(
        signal_frame=signal_frame,
        execution_bars=execution_bars,
        funding_rates=funding_rates,
        execution_config=execution_config,
        risk_config=RiskConfig(),
        instrument_config=instrument_config,
    )

    assert len(artifacts.trades) == 1
    trade = artifacts.trades[0]
    expected_funding = round(-(trade.contracts * instrument_config.contract_value * 100.0 * 0.001), 8)
    fallback_funding = round(
        -(trade.contracts * instrument_config.contract_value * 100.0 * execution_config.missing_funding_rate_bps / 10_000),
        8,
    )

    assert trade.exit_reason == "end_of_test"
    assert trade.funding_pnl == expected_funding
    assert trade.funding_pnl != fallback_funding


def test_sweep_summary_uses_same_funding_alignment(monkeypatch) -> None:
    signal_frame = _signal_frame()
    execution_config = _execution_config()

    monkeypatch.setattr("quant_lab.backtest.sweep.prepare_signal_frame", lambda *_args, **_kwargs: signal_frame)

    summary = _run_backtest_summary_only(
        signal_bars=pd.DataFrame(),
        execution_bars=_execution_bars(),
        funding_rates=pd.DataFrame(
            {
                "timestamp": [pd.Timestamp("2025-01-01T08:00:00.250Z")],
                "realized_rate": [0.001],
            }
        ),
        strategy_config=StrategyConfig(signal_bar="1m", execution_bar="1m"),
        execution_config=execution_config,
        risk_config=RiskConfig(),
        instrument_config=_instrument_config(),
    )

    assert summary["final_equity"] == 9980.0


def _signal_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2025-01-01T07:58:30Z")],
            "desired_side": [1],
            "stop_distance": [1.0],
        }
    )
    frame.attrs["signal_bar"] = "1m"
    return frame


def _execution_bars() -> pd.DataFrame:
    timestamps = pd.to_datetime(
        [
            "2025-01-01T07:59:30Z",
            "2025-01-01T08:00:30Z",
            "2025-01-01T08:01:30Z",
        ],
        utc=True,
    )
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0, 100.0, 100.0],
            "high": [100.0, 100.0, 100.0],
            "low": [100.0, 100.0, 100.0],
            "close": [100.0, 100.0, 100.0],
            "volume": [1_000.0, 1_000.0, 1_000.0],
        }
    )


def _execution_config() -> ExecutionConfig:
    return ExecutionConfig(
        fee_bps=0.0,
        slippage_bps=0.0,
        latency_minutes=0,
        market_impact_bps=0.0,
        excess_impact_bps=0.0,
        volatility_impact_share=0.0,
        max_bar_participation=1.0,
        minimum_notional=1.0,
        missing_funding_rate_bps=1.0,
    )


def _instrument_config() -> InstrumentConfig:
    return InstrumentConfig(
        symbol="BTC-USDT-SWAP",
        contract_value=1.0,
        lot_size=1.0,
        min_size=1.0,
        tick_size=0.1,
        settle_currency="USDT",
    )
