from __future__ import annotations

import json

import pandas as pd

from quant_lab.reporting.dashboard import render_dashboard


def test_render_dashboard_writes_html_file(tmp_path) -> None:
    summary_path = tmp_path / "summary.json"
    equity_path = tmp_path / "equity.csv"
    trades_path = tmp_path / "trades.csv"
    output_path = tmp_path / "dashboard.html"

    summary_path.write_text(
        json.dumps(
            {
                "initial_equity": 10_000.0,
                "final_equity": 10_250.0,
                "total_return_pct": 2.5,
                "annualized_return_pct": 16.8,
                "max_drawdown_pct": 1.9,
                "trade_count": 2,
                "win_rate_pct": 50.0,
                "profit_factor": 1.4,
                "sharpe": 1.1,
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=5, freq="h", tz="UTC"),
            "cash": [10_000, 10_000, 10_100, 10_100, 10_250],
            "equity": [10_000, 10_030, 10_100, 10_040, 10_250],
            "unrealized_pnl": [0, 30, 0, -60, 0],
            "halted": [False, False, False, False, False],
            "position_side": [0, 1, 0, -1, 0],
            "contracts": [0, 5, 0, 3, 0],
        }
    ).to_csv(equity_path, index=False)
    pd.DataFrame(
        {
            "signal_time": pd.date_range("2025-01-01", periods=2, freq="2h", tz="UTC"),
            "entry_time": pd.date_range("2025-01-01 00:01", periods=2, freq="2h", tz="UTC"),
            "exit_time": pd.date_range("2025-01-01 01:30", periods=2, freq="2h", tz="UTC"),
            "side": ["long", "short"],
            "contracts": [5, 3],
            "entry_price": [100.0, 110.0],
            "exit_price": [103.0, 108.0],
            "stop_price": [98.0, 112.0],
            "gross_pnl": [15.0, 6.0],
            "funding_pnl": [0.0, 0.0],
            "fee_paid": [1.0, 1.0],
            "net_pnl": [14.0, 5.0],
            "exit_reason": ["signal_flip", "end_of_test"],
            "symbol": ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
        }
    ).to_csv(trades_path, index=False)

    render_dashboard(
        summary_path=summary_path,
        equity_curve_path=equity_path,
        trades_path=trades_path,
        output_path=output_path,
        title="Test Dashboard",
    )

    content = output_path.read_text(encoding="utf-8")
    assert "Test Dashboard" in content
    assert "Equity Curve" in content
    assert "Trade PnL" in content
    assert "BTC-USDT-SWAP" in content
