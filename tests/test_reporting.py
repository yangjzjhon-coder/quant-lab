from __future__ import annotations

import json

import pandas as pd

from quant_lab.reporting.dashboard import render_dashboard
from quant_lab.reporting.sweep_dashboard import render_sweep_dashboard
from quant_lab.reporting.trend_research_dashboard import render_trend_research_dashboard


def test_render_dashboard_writes_html_file(tmp_path) -> None:
    summary_path = tmp_path / "summary.json"
    equity_path = tmp_path / "equity.csv"
    trades_path = tmp_path / "trades.csv"
    allocation_path = tmp_path / "allocation_overlay.csv"
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
                "allocation_mode": "equal_weight",
                "portfolio_construction": "equal_weight_sleeves",
                "capital_allocator": "per_symbol_initial_equity",
                "per_symbol_initial_equity": 5_000.0,
                "runtime_allocation_reference": "priority_risk_budget",
                "allocation_note": "Portfolio backtests aggregate single-symbol sleeves with equal initial capital.",
                "historical_allocation_overlay": "priority_risk_budget",
                "historical_requested_risk_pct_avg": 3.2,
                "historical_allocated_risk_pct_avg": 2.7,
                "historical_allocated_risk_pct_max": 3.0,
                "historical_bull_trend_symbol_count_avg": 1.2,
                "historical_range_symbol_count_avg": 0.8,
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
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=5, freq="h", tz="UTC"),
            "requested_total_risk_fraction": [0.00, 0.02, 0.04, 0.03, 0.03],
            "allocated_total_risk_fraction": [0.00, 0.02, 0.03, 0.025, 0.03],
            "active_symbol_count": [0, 1, 2, 2, 2],
            "allocated_symbol_count": [0, 1, 2, 2, 2],
            "bull_trend_symbol_count": [0, 1, 1, 1, 2],
            "bear_trend_symbol_count": [0, 0, 0, 0, 0],
            "range_symbol_count": [0, 0, 1, 1, 0],
            "dominant_regime": ["flat", "bull_trend", "mixed", "mixed", "bull_trend"],
        }
    ).to_csv(allocation_path, index=False)

    render_dashboard(
        summary_path=summary_path,
        equity_curve_path=equity_path,
        trades_path=trades_path,
        output_path=output_path,
        title="Test Dashboard",
    )

    content = output_path.read_text(encoding="utf-8")
    assert "Test Dashboard" in content
    assert "权益曲线" in content
    assert "单笔交易净盈亏" in content
    assert "BTC-USDT-SWAP" in content
    assert "组合构建方式" in content
    assert "equal_weight_sleeves" in content
    assert "运行时分配参考" in content
    assert "priority_risk_budget" in content
    assert "历史仓位覆盖" in content
    assert "历史分配风险均值" in content
    assert "组合风险预算" in content
    assert "最新分配状态" in content
    assert "状态分布" in content
    assert "多头标的数" in content
    assert 'src="https://cdn.plot.ly' not in content
    assert "Plotly.newPlot" in content


def test_render_sweep_dashboard_is_self_contained(tmp_path) -> None:
    output_path = tmp_path / "sweep_dashboard.html"
    results = pd.DataFrame(
        [
            {
                "fast_ema": 12,
                "slow_ema": 30,
                "atr_stop_multiple": 3.5,
                "total_return_pct": 12.3,
                "max_drawdown_pct": 4.2,
                "trade_count": 18,
                "sharpe": 1.4,
                "win_rate_pct": 53.0,
                "score_return_over_dd": 2.9,
            },
            {
                "fast_ema": 15,
                "slow_ema": 36,
                "atr_stop_multiple": 4.0,
                "total_return_pct": 10.5,
                "max_drawdown_pct": 5.1,
                "trade_count": 14,
                "sharpe": 1.1,
                "win_rate_pct": 50.0,
                "score_return_over_dd": 2.1,
            },
        ]
    )

    render_sweep_dashboard(results=results, output_path=output_path, title="Sweep Test")

    content = output_path.read_text(encoding="utf-8")
    assert "Sweep Test" in content
    assert "参数扫描看板" in content
    assert "收益与回撤散点图" in content
    assert 'src="https://cdn.plot.ly' not in content
    assert "Plotly.newPlot" in content


def test_render_trend_research_dashboard_is_self_contained(tmp_path) -> None:
    output_path = tmp_path / "trend_research.html"
    results = pd.DataFrame(
        [
            {
                "variant": "ema_cross",
                "fast_ema": 12,
                "slow_ema": 30,
                "atr_stop_multiple": 3.5,
                "trend_ema": 80,
                "adx_threshold": 20.0,
                "total_return_pct": 16.8,
                "bear_return_pct": 5.4,
                "max_drawdown_pct": 4.9,
                "sharpe": 1.5,
                "research_score": 0.82,
            },
            {
                "variant": "breakout_retest_regime",
                "fast_ema": 14,
                "slow_ema": 36,
                "atr_stop_multiple": 4.0,
                "trend_ema": 90,
                "adx_threshold": 22.0,
                "total_return_pct": 14.2,
                "bear_return_pct": 4.2,
                "max_drawdown_pct": 5.6,
                "sharpe": 1.2,
                "research_score": 0.71,
            },
        ]
    )

    render_trend_research_dashboard(results=results, output_path=output_path, title="Research Test")

    content = output_path.read_text(encoding="utf-8")
    assert "Research Test" in content
    assert "趋势研究看板" in content
    assert "变体对比" in content
    assert 'src="https://cdn.plot.ly' not in content
    assert "Plotly.newPlot" in content
