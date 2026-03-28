from __future__ import annotations

from itertools import product

import pandas as pd

from quant_lab.backtest.engine import _normalize_funding_frame, _normalize_market_frame, run_backtest
from quant_lab.backtest.metrics import build_summary
from quant_lab.config import ExecutionConfig, InstrumentConfig, RiskConfig, StrategyConfig


def run_trend_research(
    signal_bars: pd.DataFrame,
    execution_bars: pd.DataFrame,
    funding_rates: pd.DataFrame,
    strategy_config: StrategyConfig,
    execution_config: ExecutionConfig,
    risk_config: RiskConfig,
    instrument_config: InstrumentConfig,
    variants: list[str],
    fast_values: list[int],
    slow_values: list[int],
    atr_values: list[float],
    trend_ema_values: list[int],
    adx_threshold_values: list[float],
) -> pd.DataFrame:
    normalized_signal = _normalize_market_frame(signal_bars)
    normalized_execution = _normalize_market_frame(execution_bars)
    normalized_funding = _normalize_funding_frame(funding_rates)
    rows: list[dict[str, float | int | str | None]] = []

    for variant in sorted({item.strip().lower() for item in variants if item.strip()}):
        use_regime = "regime" in variant
        use_adx = "adx" in variant
        candidate_trend_emas = sorted(set(trend_ema_values if use_regime else [strategy_config.trend_ema]))
        candidate_adx_thresholds = sorted(
            set(adx_threshold_values if use_adx else [strategy_config.adx_threshold])
        )

        for fast_ema, slow_ema, atr_multiple, trend_ema, adx_threshold in product(
            sorted(set(fast_values)),
            sorted(set(slow_values)),
            sorted(set(atr_values)),
            candidate_trend_emas,
            candidate_adx_thresholds,
        ):
            if fast_ema >= slow_ema:
                continue

            candidate = strategy_config.model_copy(
                update={
                    "variant": variant,
                    "fast_ema": fast_ema,
                    "slow_ema": slow_ema,
                    "atr_stop_multiple": atr_multiple,
                    "trend_ema": trend_ema,
                    "adx_threshold": adx_threshold,
                }
            )
            artifacts = run_backtest(
                signal_bars=normalized_signal,
                execution_bars=normalized_execution,
                funding_rates=normalized_funding,
                strategy_config=candidate,
                execution_config=execution_config,
                risk_config=risk_config,
                instrument_config=instrument_config,
            )
            summary = build_summary(
                equity_curve=artifacts.equity_curve,
                trades=artifacts.trades,
                initial_equity=execution_config.initial_equity,
            )
            regime_metrics = build_regime_metrics(
                signal_frame=artifacts.signal_frame,
                trades_frame=_trades_frame(artifacts.trades),
                initial_equity=execution_config.initial_equity,
            )

            rows.append(
                {
                    "strategy_name": candidate.name,
                    "variant": variant,
                    "fast_ema": fast_ema,
                    "slow_ema": slow_ema,
                    "atr_stop_multiple": atr_multiple,
                    "trend_ema": trend_ema,
                    "adx_threshold": adx_threshold,
                    **summary,
                    **regime_metrics,
                }
            )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return rank_trend_research_results(frame)


def build_regime_metrics(
    *,
    signal_frame: pd.DataFrame,
    trades_frame: pd.DataFrame,
    initial_equity: float,
) -> dict[str, float | int]:
    metrics: dict[str, float | int] = {}
    signal_lookup = (
        signal_frame[["timestamp", "trend_regime"]]
        .drop_duplicates(subset=["timestamp"], keep="last")
        .rename(columns={"timestamp": "signal_time"})
    )

    market = signal_frame.copy()
    market["market_returns"] = market["close"].pct_change().fillna(0.0)
    bear_market_returns = market.loc[market["trend_regime"] < 0, "market_returns"]
    bull_market_returns = market.loc[market["trend_regime"] > 0, "market_returns"]

    metrics["bear_market_price_return_pct"] = _compound_returns_pct(bear_market_returns)
    metrics["bull_market_price_return_pct"] = _compound_returns_pct(bull_market_returns)
    metrics["bear_bar_ratio_pct"] = round((market["trend_regime"] < 0).mean() * 100, 2)

    if trades_frame.empty:
        metrics.update(
            {
                "bear_trade_count": 0,
                "bear_win_rate_pct": 0.0,
                "bear_profit_factor": 0.0,
                "bear_return_pct": 0.0,
                "bull_trade_count": 0,
                "bull_win_rate_pct": 0.0,
                "bull_profit_factor": 0.0,
                "bull_return_pct": 0.0,
                "long_trade_count": 0,
                "short_trade_count": 0,
            }
        )
        return metrics

    classified = trades_frame.merge(signal_lookup, on="signal_time", how="left")
    classified = _attach_trade_returns(classified, initial_equity=initial_equity)
    bear_trades = classified.loc[classified["trend_regime"] < 0]
    bull_trades = classified.loc[classified["trend_regime"] > 0]

    metrics.update(_trade_metrics(bear_trades, prefix="bear", initial_equity=initial_equity))
    metrics.update(_trade_metrics(bull_trades, prefix="bull", initial_equity=initial_equity))
    metrics["long_trade_count"] = int((classified["side"] == "long").sum())
    metrics["short_trade_count"] = int((classified["side"] == "short").sum())
    return metrics


def rank_trend_research_results(results: pd.DataFrame) -> pd.DataFrame:
    frame = results.copy()
    frame["return_over_dd"] = (frame["total_return_pct"] / frame["max_drawdown_pct"].clip(lower=0.01)).round(4)
    frame["bear_return_over_dd"] = (frame["bear_return_pct"] / frame["max_drawdown_pct"].clip(lower=0.01)).round(4)

    frame["rank_bear_return"] = frame["bear_return_pct"].rank(method="average", pct=True)
    frame["rank_low_dd"] = (-frame["max_drawdown_pct"]).rank(method="average", pct=True)
    frame["rank_sharpe"] = frame["sharpe"].rank(method="average", pct=True)
    frame["rank_total_return"] = frame["total_return_pct"].rank(method="average", pct=True)
    frame["research_score"] = (
        frame["rank_bear_return"] * 0.35
        + frame["rank_low_dd"] * 0.30
        + frame["rank_sharpe"] * 0.20
        + frame["rank_total_return"] * 0.15
    ).round(4)

    return frame.sort_values(
        by=["research_score", "bear_return_pct", "sharpe", "total_return_pct", "max_drawdown_pct"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)


def _trade_metrics(frame: pd.DataFrame, *, prefix: str, initial_equity: float) -> dict[str, float | int]:
    if frame.empty:
        return {
            f"{prefix}_trade_count": 0,
            f"{prefix}_win_rate_pct": 0.0,
            f"{prefix}_profit_factor": 0.0,
            f"{prefix}_return_pct": 0.0,
        }

    net = frame["net_pnl"].astype(float)
    wins = net.loc[net > 0]
    losses = net.loc[net < 0]
    gross_profit = float(wins.sum())
    gross_loss = abs(float(losses.sum()))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf") if gross_profit > 0 else 0.0
    win_rate = float((net > 0).mean()) * 100
    return {
        f"{prefix}_trade_count": int(len(frame)),
        f"{prefix}_win_rate_pct": round(win_rate, 2),
        f"{prefix}_profit_factor": _safe_metric(profit_factor),
        f"{prefix}_return_pct": _compound_trade_returns_pct(frame["trade_return"]),
    }


def _compound_returns_pct(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return round(((1 + series.astype(float)).prod() - 1) * 100, 2)


def _compound_trade_returns_pct(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return round(((1 + series.astype(float)).prod() - 1) * 100, 2)


def _safe_metric(value: float) -> float | None:
    if pd.isna(value) or value == float("inf"):
        return None
    return round(float(value), 2)


def _attach_trade_returns(frame: pd.DataFrame, *, initial_equity: float) -> pd.DataFrame:
    if frame.empty:
        return frame.assign(trade_return=pd.Series(dtype=float))

    ordered = frame.copy()
    sort_columns = [column for column in ("signal_time", "entry_time", "exit_time") if column in ordered.columns]
    ordered = ordered.sort_values(sort_columns, kind="stable").reset_index(drop=True)

    equity = float(initial_equity)
    trade_returns: list[float] = []
    for value in ordered["net_pnl"].astype(float):
        denominator = equity if equity != 0 else initial_equity
        trade_returns.append(0.0 if denominator == 0 else float(value) / denominator)
        equity += float(value)

    ordered["trade_return"] = trade_returns
    return ordered


def _trades_frame(trades) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame(
            columns=[
                "signal_time",
                "entry_time",
                "exit_time",
                "side",
                "contracts",
                "entry_price",
                "exit_price",
                "stop_price",
                "gross_pnl",
                "funding_pnl",
                "fee_paid",
                "net_pnl",
                "exit_reason",
            ]
        )
    return pd.DataFrame([trade.to_dict() for trade in trades])
