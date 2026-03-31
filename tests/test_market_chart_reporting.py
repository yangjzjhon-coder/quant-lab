from __future__ import annotations

import json

import pandas as pd

from quant_lab.reporting.dashboard import render_dashboard


def test_render_dashboard_embeds_tradingview_style_market_chart_when_companion_files_exist(tmp_path) -> None:
    summary_path = tmp_path / "btc_summary.json"
    equity_path = tmp_path / "equity.csv"
    trades_path = tmp_path / "trades.csv"
    signals_path = tmp_path / "btc_signals.csv"
    execution_bars_path = tmp_path / "btc_execution_bars.csv"
    output_path = tmp_path / "dashboard.html"

    summary_path.write_text(
        json.dumps(
            {
                "initial_equity": 10_000.0,
                "final_equity": 10_250.0,
                "total_return_pct": 2.5,
                "annualized_return_pct": 16.8,
                "max_drawdown_pct": 1.9,
                "trade_count": 1,
                "win_rate_pct": 100.0,
                "profit_factor": 1.4,
                "sharpe": 1.1,
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=5, freq="4h", tz="UTC"),
            "cash": [10_000, 10_000, 10_000, 10_050, 10_250],
            "equity": [10_000, 10_020, 10_060, 10_120, 10_250],
            "unrealized_pnl": [0, 20, 60, 70, 0],
            "halted": [False, False, False, False, False],
            "position_side": [0, 1, 1, 1, 0],
            "contracts": [0, 3, 3, 3, 0],
        }
    ).to_csv(equity_path, index=False)
    pd.DataFrame(
        {
            "signal_time": [pd.Timestamp("2025-01-01T00:00:00Z")],
            "entry_time": [pd.Timestamp("2025-01-02T00:00:00Z")],
            "exit_time": [pd.Timestamp("2025-01-02T12:00:00Z")],
            "side": ["long"],
            "contracts": [3],
            "entry_price": [100.0],
            "exit_price": [106.0],
            "stop_price": [97.5],
            "gross_pnl": [18.0],
            "funding_pnl": [0.0],
            "fee_paid": [1.0],
            "net_pnl": [17.0],
            "exit_reason": ["take_profit_cross"],
            "symbol": ["BTC-USDT-SWAP"],
            "leverage": [3.0],
        }
    ).to_csv(trades_path, index=False)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=3, freq="D", tz="UTC"),
            "action_time": pd.date_range("2025-01-02", periods=3, freq="D", tz="UTC"),
            "open": [99.0, 100.0, 102.0],
            "high": [101.0, 103.0, 107.0],
            "low": [97.0, 99.0, 101.0],
            "close": [100.0, 102.0, 106.0],
            "ema_12": [99.5, 100.2, 101.5],
            "ema_144": [95.0, 95.2, 95.5],
            "ema_169": [94.5, 94.7, 95.0],
            "open_long_signal": [False, True, False],
            "take_profit_signal": [False, False, True],
            "synthetic_setup": [False, True, False],
            "synthetic_high_touches_ema12": [False, True, False],
            "synthetic_low_touches_ema144": [False, True, False],
            "window_close_above_ema12": [False, True, True],
            "allow_long": [False, True, True],
            "fresh_setup_signal": [False, True, False],
            "current_close_above_ema12": [False, True, True],
            "trend_state": ["neutral", "bull_trend", "bull_trend"],
            "stop_loss_price": [None, 97.5, None],
            "signal_leverage": [None, 3.0, None],
        }
    ).to_csv(signals_path, index=False)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=12, freq="4h", tz="UTC"),
            "open": [99.0, 99.5, 100.0, 100.5, 101.0, 101.8, 102.0, 102.8, 103.5, 104.5, 105.0, 105.5],
            "high": [100.0, 100.5, 101.0, 101.5, 102.5, 102.8, 103.2, 104.0, 105.0, 106.0, 106.5, 106.8],
            "low": [98.5, 99.0, 99.8, 100.0, 100.8, 101.2, 101.8, 102.3, 103.0, 104.0, 104.5, 105.0],
            "close": [99.5, 100.0, 100.5, 101.0, 101.8, 102.0, 102.8, 103.5, 104.5, 105.0, 105.5, 106.0],
        }
    ).to_csv(execution_bars_path, index=False)

    render_dashboard(
        summary_path=summary_path,
        equity_curve_path=equity_path,
        trades_path=trades_path,
        output_path=output_path,
        title="BTC Review",
    )

    content = output_path.read_text(encoding="utf-8")
    assert "TradingView 风格多周期 K 线回放" in content
    assert "做多触发" in content
    assert "开仓" in content
    assert "止损线" in content
    assert "执行回放" in content
    assert "交易逻辑复盘" in content
    assert "交易定位器" in content
    assert 'id="trade-focus-select"' in content
    assert "聚焦交易" in content
    assert "定位表格" in content
    assert "完整逻辑层" in content
    assert "纯回放视图" in content
    assert 'id="trade-review-T1"' in content
    assert 'src="https://cdn.plot.ly' not in content
    assert "Plotly.newPlot" in content
