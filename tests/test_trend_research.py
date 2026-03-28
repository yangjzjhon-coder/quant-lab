from __future__ import annotations

import pandas as pd

from quant_lab.backtest.trend_research import build_regime_metrics, rank_trend_research_results


def test_build_regime_metrics_splits_bear_and_bull_trades() -> None:
    signal_frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2025-01-01T00:00:00Z",
                    "2025-01-01T01:00:00Z",
                    "2025-01-01T02:00:00Z",
                ]
            ),
            "close": [100.0, 95.0, 105.0],
            "trend_regime": [-1, -1, 1],
        }
    )
    trades = pd.DataFrame(
        {
            "signal_time": pd.to_datetime(
                [
                    "2025-01-01T00:00:00Z",
                    "2025-01-01T02:00:00Z",
                ]
            ),
            "side": ["short", "long"],
            "net_pnl": [300.0, 200.0],
        }
    )

    metrics = build_regime_metrics(signal_frame=signal_frame, trades_frame=trades, initial_equity=10_000.0)

    assert metrics["bear_trade_count"] == 1
    assert metrics["bear_return_pct"] == 3.0
    assert metrics["bull_trade_count"] == 1
    assert metrics["bull_return_pct"] == 1.94
    assert metrics["short_trade_count"] == 1
    assert metrics["long_trade_count"] == 1


def test_build_regime_metrics_normalizes_bear_return_after_prior_gains() -> None:
    signal_frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2025-01-01T00:00:00Z",
                    "2025-01-01T01:00:00Z",
                ]
            ),
            "close": [100.0, 90.0],
            "trend_regime": [1, -1],
        }
    )
    trades = pd.DataFrame(
        {
            "signal_time": pd.to_datetime(
                [
                    "2025-01-01T00:00:00Z",
                    "2025-01-01T01:00:00Z",
                ]
            ),
            "side": ["long", "short"],
            "net_pnl": [10_000.0, 10_000.0],
        }
    )

    metrics = build_regime_metrics(signal_frame=signal_frame, trades_frame=trades, initial_equity=10_000.0)

    assert metrics["bear_return_pct"] == 50.0


def test_rank_trend_research_results_prioritizes_bear_return_and_drawdown() -> None:
    frame = pd.DataFrame(
        [
            {
                "variant": "ema_cross",
                "total_return_pct": 90.0,
                "bear_return_pct": 5.0,
                "max_drawdown_pct": 28.0,
                "sharpe": 1.6,
            },
            {
                "variant": "ema_cross_regime_adx",
                "total_return_pct": 75.0,
                "bear_return_pct": 25.0,
                "max_drawdown_pct": 14.0,
                "sharpe": 2.0,
            },
        ]
    )

    ranked = rank_trend_research_results(frame)

    assert ranked.iloc[0]["variant"] == "ema_cross_regime_adx"
    assert "research_score" in ranked.columns
    assert ranked.iloc[0]["research_score"] > ranked.iloc[1]["research_score"]
