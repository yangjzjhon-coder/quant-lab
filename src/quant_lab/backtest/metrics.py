from __future__ import annotations

import math
from collections.abc import Sequence

import pandas as pd

from quant_lab.models import TradeRecord


def build_summary(
    equity_curve: pd.DataFrame,
    trades: Sequence[TradeRecord],
    initial_equity: float,
) -> dict[str, float | int]:
    if equity_curve.empty:
        return {
            "initial_equity": initial_equity,
            "final_equity": initial_equity,
            "total_return_pct": 0.0,
            "annualized_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "trade_count": 0,
            "win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "sharpe": 0.0,
        }

    curve = equity_curve.copy()
    curve["returns"] = curve["equity"].pct_change().fillna(0.0)
    drawdown = curve["equity"] / curve["equity"].cummax() - 1.0

    final_equity = float(curve["equity"].iloc[-1])
    total_return = (final_equity / initial_equity) - 1.0

    period_days = max((curve["timestamp"].iloc[-1] - curve["timestamp"].iloc[0]).total_seconds() / 86400, 1)
    annualized_return = (1 + total_return) ** (365 / period_days) - 1 if total_return > -1 else -1.0

    sharpe = _annualized_sharpe(curve["returns"])
    net_pnls = [trade.net_pnl for trade in trades]
    wins = [value for value in net_pnls if value > 0]
    losses = [value for value in net_pnls if value < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))

    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf") if gross_profit > 0 else 0.0
    win_rate = len(wins) / len(trades) if trades else 0.0

    return {
        "initial_equity": round(initial_equity, 2),
        "final_equity": round(final_equity, 2),
        "total_return_pct": round(total_return * 100, 2),
        "annualized_return_pct": round(annualized_return * 100, 2),
        "max_drawdown_pct": round(abs(drawdown.min()) * 100, 2),
        "trade_count": len(trades),
        "win_rate_pct": round(win_rate * 100, 2),
        "profit_factor": _safe_metric(profit_factor),
        "sharpe": round(sharpe, 2),
    }


def _annualized_sharpe(returns: pd.Series) -> float:
    nonzero = returns.dropna()
    if len(nonzero) < 2:
        return 0.0

    std = nonzero.std()
    if std == 0:
        return 0.0

    minutes_per_year = 365 * 24 * 60
    return float((nonzero.mean() / std) * math.sqrt(minutes_per_year))


def _safe_metric(value: float) -> float | None:
    if not math.isfinite(value):
        return None
    return round(value, 2)
