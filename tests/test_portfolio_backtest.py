from __future__ import annotations

import pandas as pd

from quant_lab.backtest.portfolio import (
    build_portfolio_summary,
    build_portfolio_trade_frame,
    combine_portfolio_equity_curves,
)
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
