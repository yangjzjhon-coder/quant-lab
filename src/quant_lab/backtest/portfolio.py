from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from quant_lab.backtest.metrics import build_summary
from quant_lab.config import ExecutionConfig, RiskConfig
from quant_lab.models import BacktestArtifacts, TradeRecord
from quant_lab.risk.portfolio import allocate_portfolio_risk_budgets, portfolio_priority_score
from quant_lab.strategy_contracts import apply_signal_contract_columns
from quant_lab.utils.timeframes import bar_to_timedelta


def combine_portfolio_equity_curves(equity_curves: dict[str, pd.DataFrame]) -> pd.DataFrame:
    if not equity_curves:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "cash",
                "equity",
                "unrealized_pnl",
                "halted",
                "position_side",
                "contracts",
                "active_positions",
            ]
        )

    merged: pd.DataFrame | None = None
    initial_values: dict[str, dict[str, float | bool]] = {}

    for symbol, equity_curve in equity_curves.items():
        normalized = equity_curve.copy()
        normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True)
        normalized = normalized.sort_values("timestamp").reset_index(drop=True)
        slug = _symbol_slug(symbol)
        renamed = normalized.rename(
            columns={
                "cash": f"cash__{slug}",
                "equity": f"equity__{slug}",
                "unrealized_pnl": f"unrealized__{slug}",
                "halted": f"halted__{slug}",
                "position_side": f"position_side__{slug}",
                "contracts": f"contracts__{slug}",
            }
        )[
            [
                "timestamp",
                f"cash__{slug}",
                f"equity__{slug}",
                f"unrealized__{slug}",
                f"halted__{slug}",
                f"position_side__{slug}",
                f"contracts__{slug}",
            ]
        ]
        initial_values[slug] = {
            "cash": float(renamed.iloc[0][f"cash__{slug}"]),
            "equity": float(renamed.iloc[0][f"equity__{slug}"]),
            "unrealized": float(renamed.iloc[0][f"unrealized__{slug}"]),
            "halted": bool(renamed.iloc[0][f"halted__{slug}"]),
            "position_side": float(renamed.iloc[0][f"position_side__{slug}"]),
            "contracts": float(renamed.iloc[0][f"contracts__{slug}"]),
        }
        merged = renamed if merged is None else merged.merge(renamed, on="timestamp", how="outer")

    assert merged is not None
    merged = merged.sort_values("timestamp").reset_index(drop=True)

    cash_columns: list[str] = []
    equity_columns: list[str] = []
    unrealized_columns: list[str] = []
    halted_columns: list[str] = []
    contracts_columns: list[str] = []
    position_columns: list[str] = []

    for slug, initial in initial_values.items():
        cash_column = f"cash__{slug}"
        equity_column = f"equity__{slug}"
        unrealized_column = f"unrealized__{slug}"
        halted_column = f"halted__{slug}"
        position_column = f"position_side__{slug}"
        contracts_column = f"contracts__{slug}"

        merged[cash_column] = merged[cash_column].ffill().fillna(initial["cash"])
        merged[equity_column] = merged[equity_column].ffill().fillna(initial["equity"])
        merged[unrealized_column] = merged[unrealized_column].ffill().fillna(initial["unrealized"])
        merged[halted_column] = merged[halted_column].ffill().fillna(initial["halted"]).astype(bool)
        merged[position_column] = merged[position_column].ffill().fillna(initial["position_side"])
        merged[contracts_column] = merged[contracts_column].ffill().fillna(initial["contracts"])

        cash_columns.append(cash_column)
        equity_columns.append(equity_column)
        unrealized_columns.append(unrealized_column)
        halted_columns.append(halted_column)
        contracts_columns.append(contracts_column)
        position_columns.append(position_column)

    active_positions = (
        merged[position_columns]
        .fillna(0.0)
        .astype(float)
        .ne(0.0)
        .sum(axis=1)
    )
    combined = pd.DataFrame(
        {
            "timestamp": merged["timestamp"],
            "cash": merged[cash_columns].sum(axis=1),
            "equity": merged[equity_columns].sum(axis=1),
            "unrealized_pnl": merged[unrealized_columns].sum(axis=1),
            "halted": merged[halted_columns].any(axis=1),
            "position_side": 0,
            "contracts": merged[contracts_columns].sum(axis=1),
            "active_positions": active_positions,
        }
    )
    return combined.reset_index(drop=True)


def build_portfolio_trade_frame(trades_by_symbol: dict[str, Sequence[TradeRecord]]) -> pd.DataFrame:
    columns = [
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
        "symbol",
    ]
    rows: list[dict[str, object]] = []
    for symbol, trades in trades_by_symbol.items():
        for trade in trades:
            row = trade.to_dict()
            row["symbol"] = row.get("symbol") or symbol
            rows.append(row)

    if not rows:
        return pd.DataFrame(columns=columns)

    frame = pd.DataFrame(rows)
    missing_columns = [column for column in columns if column not in frame.columns]
    for column in missing_columns:
        frame[column] = None
    return frame[columns].sort_values(["entry_time", "exit_time", "symbol"]).reset_index(drop=True)


def build_portfolio_summary(
    *,
    equity_curve: pd.DataFrame,
    trades: Sequence[TradeRecord],
    initial_equity: float,
    symbols: Sequence[str],
) -> dict[str, object]:
    summary = build_summary(
        equity_curve=equity_curve,
        trades=trades,
        initial_equity=initial_equity,
    )
    summary["symbol_count"] = len(symbols)
    summary["symbols"] = list(symbols)
    return summary


def attach_equal_weight_portfolio_construction(
    summary: dict[str, object],
    *,
    per_symbol_initial_equity: float,
    runtime_allocation_reference: str = "priority_risk_budget",
) -> dict[str, object]:
    enriched = dict(summary)
    enriched["allocation_mode"] = "equal_weight"
    enriched["portfolio_construction"] = "equal_weight_sleeves"
    enriched["capital_allocator"] = "per_symbol_initial_equity"
    enriched["per_symbol_initial_equity"] = round(per_symbol_initial_equity, 2)
    enriched["runtime_allocation_reference"] = runtime_allocation_reference
    enriched["allocation_note"] = (
        "Portfolio backtests aggregate single-symbol sleeves with equal initial capital. "
        "This differs from demo runtime portfolio sizing, which can use priority_risk_budget."
    )
    return enriched


def build_portfolio_risk_budget_overlay(
    *,
    symbol_artifacts: dict[str, BacktestArtifacts],
    execution_config: ExecutionConfig,
    risk_config: RiskConfig,
) -> pd.DataFrame:
    if not symbol_artifacts:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "requested_total_risk_fraction",
                "allocated_total_risk_fraction",
                "active_symbol_count",
                "allocated_symbol_count",
                "bull_trend_symbol_count",
                "bear_trend_symbol_count",
                "range_symbol_count",
                "dominant_regime",
            ]
        )

    execution_index = pd.DatetimeIndex(
        sorted(
            {
                pd.Timestamp(timestamp)
                for artifacts in symbol_artifacts.values()
                for timestamp in artifacts.equity_curve["timestamp"]
            }
        )
    )
    if execution_index.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "requested_total_risk_fraction",
                "allocated_total_risk_fraction",
                "active_symbol_count",
                "allocated_symbol_count",
                "bull_trend_symbol_count",
                "bear_trend_symbol_count",
                "range_symbol_count",
                "dominant_regime",
            ]
        )

    state_frames = {
        symbol: _portfolio_execution_state_frame(
            signal_frame=artifacts.signal_frame,
            execution_index=execution_index,
            execution_config=execution_config,
        )
        for symbol, artifacts in symbol_artifacts.items()
    }
    rows: list[dict[str, object]] = []

    for position, timestamp in enumerate(execution_index):
        requests: dict[str, dict[str, float | int | None]] = {}
        row: dict[str, object] = {"timestamp": timestamp}
        requested_total = 0.0
        allocated_total = 0.0
        active_symbol_count = 0
        allocated_symbol_count = 0
        regime_counts = {"bull_trend": 0, "bear_trend": 0, "range": 0}

        for symbol, state_frame in state_frames.items():
            state = state_frame.iloc[position]
            desired_side = int(state["desired_side"])
            strategy_score = _optional_float(state.get("strategy_score"))
            strategy_risk_multiplier = max(0.0, _optional_float(state.get("strategy_risk_multiplier")) or 0.0)
            regime = _normalize_regime_value(state.get("regime"))
            route_key = _optional_text(state.get("route_key"))
            requested_risk_fraction = (
                risk_config.risk_per_trade * strategy_risk_multiplier
                if desired_side != 0 and strategy_risk_multiplier > 0
                else 0.0
            )
            priority_score = portfolio_priority_score(strategy_score=strategy_score, factor_score=None)
            requests[symbol] = {
                "desired_side": desired_side,
                "priority_score": priority_score,
                "requested_risk_fraction": requested_risk_fraction,
            }
            row[f"{_symbol_slug(symbol)}__desired_side"] = desired_side
            row[f"{_symbol_slug(symbol)}__priority_score"] = priority_score
            row[f"{_symbol_slug(symbol)}__requested_risk_fraction"] = round(requested_risk_fraction, 6)
            row[f"{_symbol_slug(symbol)}__regime"] = regime
            row[f"{_symbol_slug(symbol)}__route_key"] = route_key
            requested_total += requested_risk_fraction
            if requested_risk_fraction > 0:
                active_symbol_count += 1
                if regime in regime_counts:
                    regime_counts[regime] += 1

        allocations = allocate_portfolio_risk_budgets(
            requests=requests,
            portfolio_max_total_risk=risk_config.portfolio_max_total_risk,
            portfolio_max_same_direction_risk=risk_config.portfolio_max_same_direction_risk,
        )
        for symbol, allocation in allocations.items():
            row[f"{_symbol_slug(symbol)}__allocated_risk_fraction"] = allocation.allocated_risk_fraction
            row[f"{_symbol_slug(symbol)}__allocation_scale"] = allocation.allocation_scale
            allocated_total += allocation.allocated_risk_fraction
            if allocation.allocated_risk_fraction > 0:
                allocated_symbol_count += 1

        row["requested_total_risk_fraction"] = round(requested_total, 6)
        row["allocated_total_risk_fraction"] = round(allocated_total, 6)
        row["active_symbol_count"] = active_symbol_count
        row["allocated_symbol_count"] = allocated_symbol_count
        row["bull_trend_symbol_count"] = regime_counts["bull_trend"]
        row["bear_trend_symbol_count"] = regime_counts["bear_trend"]
        row["range_symbol_count"] = regime_counts["range"]
        row["dominant_regime"] = _dominant_regime_label(regime_counts)
        rows.append(row)

    return pd.DataFrame(rows)


def attach_portfolio_risk_budget_overlay(
    summary: dict[str, object],
    *,
    allocation_frame: pd.DataFrame,
) -> dict[str, object]:
    enriched = dict(summary)
    if allocation_frame.empty:
        enriched["historical_allocation_overlay"] = "unavailable"
        return enriched

    requested = allocation_frame["requested_total_risk_fraction"].astype(float)
    allocated = allocation_frame["allocated_total_risk_fraction"].astype(float)
    active = allocation_frame["active_symbol_count"].astype(float)
    allocated_symbols = allocation_frame["allocated_symbol_count"].astype(float)

    enriched["historical_allocation_overlay"] = "priority_risk_budget"
    enriched["historical_requested_risk_pct_avg"] = round(requested.mean() * 100.0, 2)
    enriched["historical_requested_risk_pct_max"] = round(requested.max() * 100.0, 2)
    enriched["historical_allocated_risk_pct_avg"] = round(allocated.mean() * 100.0, 2)
    enriched["historical_allocated_risk_pct_max"] = round(allocated.max() * 100.0, 2)
    enriched["historical_active_symbol_count_avg"] = round(active.mean(), 2)
    enriched["historical_allocated_symbol_count_avg"] = round(allocated_symbols.mean(), 2)
    enriched["historical_allocation_observation_count"] = int(len(allocation_frame))
    for column, field in (
        ("bull_trend_symbol_count", "historical_bull_trend_symbol_count_avg"),
        ("bear_trend_symbol_count", "historical_bear_trend_symbol_count_avg"),
        ("range_symbol_count", "historical_range_symbol_count_avg"),
    ):
        if column in allocation_frame.columns:
            enriched[field] = round(allocation_frame[column].astype(float).mean(), 2)
    return enriched


def _portfolio_execution_state_frame(
    *,
    signal_frame: pd.DataFrame,
    execution_index: pd.DatetimeIndex,
    execution_config: ExecutionConfig,
) -> pd.DataFrame:
    if execution_index.empty:
        return pd.DataFrame(
            columns=["timestamp", "desired_side", "strategy_score", "strategy_risk_multiplier", "regime", "route_key"]
        )

    frame = apply_signal_contract_columns(signal_frame.copy())
    if frame.empty:
        return pd.DataFrame(
            {
                "timestamp": execution_index,
                "desired_side": 0,
                "strategy_score": pd.NA,
                "strategy_risk_multiplier": 0.0,
                "regime": pd.NA,
                "route_key": pd.NA,
            }
        )

    if "timestamp" not in frame.columns:
        raise ValueError("signal_frame must contain timestamp for portfolio allocation overlay")

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    signal_bar = _infer_signal_bar(frame)
    frame["effective_time"] = (
        frame["timestamp"]
        + bar_to_timedelta(signal_bar)
        + pd.to_timedelta(execution_config.latency_minutes, unit="m")
    )
    if "regime" not in frame.columns and "alpha_regime" in frame.columns:
        frame["regime"] = frame["alpha_regime"]
    if "regime" not in frame.columns:
        if "trend_regime" in frame.columns:
            frame["regime"] = frame["trend_regime"].apply(_normalize_regime_value)
        else:
            frame["regime"] = pd.NA
    else:
        frame["regime"] = frame["regime"].apply(_normalize_regime_value)
    if "route_key" not in frame.columns:
        frame["route_key"] = pd.NA

    if "desired_side" not in frame.columns and "execution_desired_side" in frame.columns:
        frame["desired_side"] = frame["execution_desired_side"]
    if "strategy_score" not in frame.columns and "alpha_score" in frame.columns:
        frame["strategy_score"] = frame["alpha_score"]
    if "strategy_risk_multiplier" not in frame.columns and "risk_multiplier" in frame.columns:
        frame["strategy_risk_multiplier"] = frame["risk_multiplier"]

    for column in ("desired_side", "strategy_score", "strategy_risk_multiplier"):
        if column not in frame.columns:
            frame[column] = 0.0 if column == "strategy_risk_multiplier" else pd.NA
    frame["desired_side"] = frame["desired_side"].fillna(0).astype(int)
    frame["strategy_risk_multiplier"] = pd.to_numeric(frame["strategy_risk_multiplier"], errors="coerce").fillna(1.0)
    frame["strategy_score"] = pd.to_numeric(frame["strategy_score"], errors="coerce")
    frame["route_key"] = frame["route_key"].apply(_optional_text)

    effective = frame[
        ["effective_time", "desired_side", "strategy_score", "strategy_risk_multiplier", "regime", "route_key"]
    ].rename(
        columns={"effective_time": "timestamp"}
    )
    target = pd.DataFrame({"timestamp": execution_index})
    merged = pd.merge_asof(
        target.sort_values("timestamp"),
        effective.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )
    merged["desired_side"] = merged["desired_side"].fillna(0).astype(int)
    merged["strategy_risk_multiplier"] = merged["strategy_risk_multiplier"].fillna(0.0)
    return merged


def _infer_signal_bar(signal_frame: pd.DataFrame) -> str:
    signal_bar = signal_frame.attrs.get("signal_bar")
    if isinstance(signal_bar, str) and signal_bar.strip():
        return signal_bar

    timestamps = pd.DatetimeIndex(signal_frame["timestamp"]).sort_values().unique()
    if len(timestamps) < 2:
        return "1H"
    delta = pd.Timedelta(timestamps[1] - timestamps[0])
    if delta % pd.Timedelta(hours=1) == pd.Timedelta(0):
        hours = int(delta / pd.Timedelta(hours=1))
        if hours > 0:
            return f"{hours}H"
    if delta % pd.Timedelta(minutes=1) == pd.Timedelta(0):
        minutes = int(delta / pd.Timedelta(minutes=1))
        if minutes > 0:
            return f"{minutes}m"
    return "1H"


def _optional_float(value: object) -> float | None:
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_text(value: object) -> str | None:
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    text = str(value).strip()
    return text or None


def _normalize_regime_value(value: object) -> str | None:
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"bull", "bull_trend", "bulltrend", "uptrend", "long"}:
            return "bull_trend"
        if normalized in {"bear", "bear_trend", "beartrend", "downtrend", "short"}:
            return "bear_trend"
        if normalized in {"range", "neutral", "flat"}:
            return "range"
        return normalized or None
    numeric = _optional_float(value)
    if numeric is None:
        return None
    if numeric > 0:
        return "bull_trend"
    if numeric < 0:
        return "bear_trend"
    return "range"


def _dominant_regime_label(regime_counts: dict[str, int]) -> str:
    total = sum(regime_counts.values())
    if total <= 0:
        return "flat"
    top_value = max(regime_counts.values())
    leaders = [key for key, value in regime_counts.items() if value == top_value and value > 0]
    if len(leaders) != 1:
        return "mixed"
    return leaders[0]


def _symbol_slug(symbol: str) -> str:
    return symbol.replace("/", "-").replace(":", "-").replace(".", "-")
