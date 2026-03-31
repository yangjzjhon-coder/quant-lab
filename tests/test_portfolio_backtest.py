from __future__ import annotations

import pandas as pd

from quant_lab.backtest.portfolio import (
    attach_equal_weight_portfolio_construction,
    attach_portfolio_risk_budget_overlay,
    build_portfolio_summary,
    build_portfolio_risk_budget_overlay,
    build_portfolio_trade_frame,
    combine_portfolio_equity_curves,
)
from quant_lab.config import ExecutionConfig, RiskConfig
from quant_lab.models import BacktestArtifacts
from quant_lab.models import TradeRecord


def test_combine_portfolio_equity_curves_sums_two_sleeves() -> None:
    timestamps = pd.date_range("2025-01-01", periods=3, freq="h", tz="UTC")
    btc_curve = pd.DataFrame(
        {
            "timestamp": timestamps,
            "cash": [5_000.0, 5_020.0, 5_100.0],
            "equity": [5_000.0, 5_040.0, 5_120.0],
            "unrealized_pnl": [0.0, 20.0, 20.0],
            "halted": [False, False, False],
            "position_side": [0, 1, 1],
            "contracts": [0.0, 10.0, 10.0],
        }
    )
    eth_curve = pd.DataFrame(
        {
            "timestamp": timestamps,
            "cash": [5_000.0, 4_980.0, 5_030.0],
            "equity": [5_000.0, 5_010.0, 5_060.0],
            "unrealized_pnl": [0.0, 30.0, 30.0],
            "halted": [False, True, False],
            "position_side": [0, 1, 0],
            "contracts": [0.0, 6.0, 0.0],
        }
    )

    combined = combine_portfolio_equity_curves(
        {
            "BTC-USDT-SWAP": btc_curve,
            "ETH-USDT-SWAP": eth_curve,
        }
    )

    assert list(combined["equity"]) == [10_000.0, 10_050.0, 10_180.0]
    assert list(combined["cash"]) == [10_000.0, 10_000.0, 10_130.0]
    assert list(combined["unrealized_pnl"]) == [0.0, 50.0, 50.0]
    assert list(combined["halted"]) == [False, True, False]
    assert list(combined["active_positions"]) == [0, 2, 1]


def test_build_portfolio_trade_frame_keeps_symbol_labels() -> None:
    btc_trade = TradeRecord(
        signal_time=pd.Timestamp("2025-01-01T00:00:00Z"),
        entry_time=pd.Timestamp("2025-01-01T00:01:00Z"),
        exit_time=pd.Timestamp("2025-01-01T01:00:00Z"),
        side="long",
        contracts=5.0,
        entry_price=100.0,
        exit_price=103.0,
        stop_price=98.0,
        gross_pnl=15.0,
        funding_pnl=0.0,
        fee_paid=1.0,
        net_pnl=14.0,
        exit_reason="signal_flip",
        symbol="BTC-USDT-SWAP",
    )
    eth_trade = TradeRecord(
        signal_time=pd.Timestamp("2025-01-01T02:00:00Z"),
        entry_time=pd.Timestamp("2025-01-01T02:01:00Z"),
        exit_time=pd.Timestamp("2025-01-01T03:00:00Z"),
        side="long",
        contracts=8.0,
        entry_price=200.0,
        exit_price=205.0,
        stop_price=194.0,
        gross_pnl=40.0,
        funding_pnl=-1.0,
        fee_paid=2.0,
        net_pnl=37.0,
        exit_reason="end_of_test",
        symbol="ETH-USDT-SWAP",
    )

    frame = build_portfolio_trade_frame(
        {
            "BTC-USDT-SWAP": [btc_trade],
            "ETH-USDT-SWAP": [eth_trade],
        }
    )

    assert list(frame["symbol"]) == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    assert list(frame["net_pnl"]) == [14.0, 37.0]


def test_build_portfolio_summary_adds_symbol_metadata() -> None:
    equity_curve = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=3, freq="h", tz="UTC"),
            "cash": [10_000.0, 10_100.0, 10_180.0],
            "equity": [10_000.0, 10_050.0, 10_180.0],
            "unrealized_pnl": [0.0, -50.0, 0.0],
            "halted": [False, False, False],
            "position_side": [0, 0, 0],
            "contracts": [0.0, 0.0, 0.0],
        }
    )
    trades = [
        TradeRecord(
            signal_time=pd.Timestamp("2025-01-01T00:00:00Z"),
            entry_time=pd.Timestamp("2025-01-01T00:01:00Z"),
            exit_time=pd.Timestamp("2025-01-01T01:00:00Z"),
            side="long",
            contracts=5.0,
            entry_price=100.0,
            exit_price=103.0,
            stop_price=98.0,
            gross_pnl=15.0,
            funding_pnl=0.0,
            fee_paid=1.0,
            net_pnl=14.0,
            exit_reason="signal_flip",
            symbol="BTC-USDT-SWAP",
        )
    ]

    summary = build_portfolio_summary(
        equity_curve=equity_curve,
        trades=trades,
        initial_equity=10_000.0,
        symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
    )

    assert summary["symbol_count"] == 2
    assert summary["symbols"] == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]


def test_attach_equal_weight_portfolio_construction_marks_backtest_semantics() -> None:
    enriched = attach_equal_weight_portfolio_construction(
        {
            "symbol_count": 2,
            "symbols": ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
        },
        per_symbol_initial_equity=5_000.0,
    )

    assert enriched["allocation_mode"] == "equal_weight"
    assert enriched["portfolio_construction"] == "equal_weight_sleeves"
    assert enriched["capital_allocator"] == "per_symbol_initial_equity"
    assert enriched["per_symbol_initial_equity"] == 5_000.0
    assert enriched["runtime_allocation_reference"] == "priority_risk_budget"
    assert "single-symbol sleeves" in str(enriched["allocation_note"])


def test_build_portfolio_risk_budget_overlay_tracks_requested_and_allocated_risk() -> None:
    timestamps = pd.date_range("2025-01-01", periods=4, freq="h", tz="UTC")
    btc_signal = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=4, freq="h", tz="UTC"),
            "desired_side": [0, 1, 1, 1],
            "stop_distance": [1.0, 1.0, 1.0, 1.0],
            "strategy_score": [0.0, 40.0, 40.0, 40.0],
            "strategy_risk_multiplier": [0.0, 1.0, 1.0, 1.0],
            "trend_regime": [0, 1, 1, 1],
        }
    )
    btc_signal.attrs["signal_bar"] = "1H"
    eth_signal = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=4, freq="h", tz="UTC"),
            "desired_side": [0, 0, 1, 1],
            "stop_distance": [1.0, 1.0, 1.0, 1.0],
            "strategy_score": [0.0, 0.0, 20.0, 20.0],
            "strategy_risk_multiplier": [0.0, 0.0, 1.0, 1.0],
            "trend_regime": [0, 0, 0, 0],
        }
    )
    eth_signal.attrs["signal_bar"] = "1H"

    overlay = build_portfolio_risk_budget_overlay(
        symbol_artifacts={
            "BTC-USDT-SWAP": BacktestArtifacts(trades=[], equity_curve=_equity_curve(timestamps), signal_frame=btc_signal),
            "ETH-USDT-SWAP": BacktestArtifacts(trades=[], equity_curve=_equity_curve(timestamps), signal_frame=eth_signal),
        },
        execution_config=ExecutionConfig(initial_equity=10_000.0, latency_minutes=0),
        risk_config=RiskConfig(
            risk_per_trade=0.02,
            portfolio_max_total_risk=0.03,
            portfolio_max_same_direction_risk=0.025,
        ),
    )

    latest = overlay.iloc[-1]
    assert latest["requested_total_risk_fraction"] == 0.04
    assert latest["allocated_total_risk_fraction"] == 0.025
    assert latest["active_symbol_count"] == 2
    assert latest["allocated_symbol_count"] == 2
    assert latest["bull_trend_symbol_count"] == 1
    assert latest["range_symbol_count"] == 1
    assert latest["dominant_regime"] == "mixed"
    assert latest["BTC-USDT-SWAP__allocated_risk_fraction"] > latest["ETH-USDT-SWAP__allocated_risk_fraction"]


def test_attach_portfolio_risk_budget_overlay_adds_historical_metrics() -> None:
    overlay = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=2, freq="h", tz="UTC"),
            "requested_total_risk_fraction": [0.02, 0.04],
            "allocated_total_risk_fraction": [0.02, 0.03],
            "active_symbol_count": [1, 2],
            "allocated_symbol_count": [1, 2],
            "bull_trend_symbol_count": [1, 1],
            "bear_trend_symbol_count": [0, 0],
            "range_symbol_count": [0, 1],
        }
    )

    summary = attach_portfolio_risk_budget_overlay({"allocation_mode": "equal_weight"}, allocation_frame=overlay)

    assert summary["historical_allocation_overlay"] == "priority_risk_budget"
    assert summary["historical_requested_risk_pct_avg"] == 3.0
    assert summary["historical_requested_risk_pct_max"] == 4.0
    assert summary["historical_allocated_risk_pct_avg"] == 2.5
    assert summary["historical_allocated_risk_pct_max"] == 3.0
    assert summary["historical_active_symbol_count_avg"] == 1.5
    assert summary["historical_allocated_symbol_count_avg"] == 1.5
    assert summary["historical_allocation_observation_count"] == 2
    assert summary["historical_bull_trend_symbol_count_avg"] == 1.0
    assert summary["historical_bear_trend_symbol_count_avg"] == 0.0
    assert summary["historical_range_symbol_count_avg"] == 0.5


def _equity_curve(timestamps: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "cash": [10_000.0] * len(timestamps),
            "equity": [10_000.0] * len(timestamps),
            "unrealized_pnl": [0.0] * len(timestamps),
            "halted": [False] * len(timestamps),
            "position_side": [0.0] * len(timestamps),
            "contracts": [0.0] * len(timestamps),
        }
    )
