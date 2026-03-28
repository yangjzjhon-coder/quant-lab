from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from quant_lab.backtest.engine import run_backtest_from_signal_frame
from quant_lab.config import AppConfig, ExecutionConfig, InstrumentConfig, RiskConfig, StrategyConfig, load_config
from quant_lab.execution.strategy_router import (
    _normalized_candidate_map,
    _resolve_route_key,
    _scope_allows,
    build_market_regime_frame,
)
from quant_lab.models import BacktestArtifacts
from quant_lab.service.database import StrategyCandidate, session_scope
from quant_lab.strategies.ema_trend import prepare_signal_frame


@dataclass
class RoutedBacktestArtifacts:
    artifacts: BacktestArtifacts
    route_frame: pd.DataFrame
    route_summary: dict[str, Any]


@dataclass
class _RouteCandidateRuntime:
    route_key: str
    candidate_id: int
    candidate_name: str
    strategy_name: str
    variant: str
    strategy_config: StrategyConfig
    signal_frame: pd.DataFrame
    signal_lookup: dict[pd.Timestamp, dict[str, Any]]


def run_routed_backtest(
    *,
    session_factory,
    config: AppConfig,
    project_root: Path,
    symbol: str,
    signal_bars: pd.DataFrame,
    execution_bars: pd.DataFrame,
    funding_rates: pd.DataFrame,
    execution_config: ExecutionConfig,
    risk_config: RiskConfig,
    instrument_config: InstrumentConfig,
    required_scope: str = "demo",
) -> RoutedBacktestArtifacts:
    if not config.trading.strategy_router_enabled:
        raise ValueError("trading.strategy_router_enabled must be true for routed backtest")

    candidate_map = _normalized_candidate_map(config.trading.execution_candidate_map)
    if not candidate_map:
        raise ValueError("trading.execution_candidate_map is empty")

    base_strategy = config.strategy.model_copy(deep=True)
    regime_frame = build_market_regime_frame(signal_bars=signal_bars, strategy_config=base_strategy)
    base_signal_frame = prepare_signal_frame(signal_bars, base_strategy)
    base_signal_frame.attrs["signal_bar"] = base_strategy.signal_bar

    route_candidates = _load_route_candidates(
        session_factory=session_factory,
        config=config,
        project_root=project_root,
        symbol=symbol,
        signal_bars=signal_bars,
        required_scope=required_scope,
    )
    routed_signal_frame, route_frame, route_summary = build_routed_signal_frame(
        symbol=symbol,
        base_strategy=base_strategy,
        base_signal_frame=base_signal_frame,
        regime_frame=regime_frame,
        route_candidates=route_candidates,
        candidate_map=candidate_map,
        fallback_to_config=bool(config.trading.strategy_router_fallback_to_config),
    )
    artifacts = run_backtest_from_signal_frame(
        signal_frame=routed_signal_frame,
        execution_bars=execution_bars,
        funding_rates=funding_rates,
        execution_config=execution_config,
        risk_config=risk_config,
        instrument_config=instrument_config,
    )
    return RoutedBacktestArtifacts(
        artifacts=artifacts,
        route_frame=route_frame,
        route_summary=route_summary,
    )


def build_routed_signal_frame(
    *,
    symbol: str,
    base_strategy: StrategyConfig,
    base_signal_frame: pd.DataFrame,
    regime_frame: pd.DataFrame,
    route_candidates: dict[str, _RouteCandidateRuntime],
    candidate_map: dict[str, int],
    fallback_to_config: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    base_lookup = _signal_lookup(base_signal_frame)
    route_rows: list[dict[str, Any]] = []
    signal_rows: list[dict[str, Any]] = []

    for regime_row in regime_frame.itertuples(index=False):
        timestamp = pd.Timestamp(regime_row.timestamp)
        regime = str(regime_row.regime)
        route_key = _resolve_route_key(candidate_map=candidate_map, symbol=symbol, regime=regime)
        base_row = base_lookup[timestamp]
        selected_row = base_row
        route_status = "base_config_fallback" if fallback_to_config else "flat_missing_route"
        selected_strategy_source = "base_config_fallback" if fallback_to_config else "flat_missing_route"
        selected_candidate_id: int | None = None
        selected_candidate_name: str | None = None
        selected_strategy_name = base_strategy.name
        selected_variant = base_strategy.variant
        fallback_used = False

        if route_key is not None and route_key in route_candidates:
            candidate = route_candidates[route_key]
            selected_row = candidate.signal_lookup[timestamp]
            route_status = "candidate_config"
            selected_strategy_source = "candidate_config"
            selected_candidate_id = candidate.candidate_id
            selected_candidate_name = candidate.candidate_name
            selected_strategy_name = candidate.strategy_name
            selected_variant = candidate.variant
        elif fallback_to_config:
            fallback_used = True
        else:
            selected_row = {
                "desired_side": 0,
                "stop_distance": 0.0,
                "stop_price": pd.NA,
                "strategy_score": pd.NA,
                "strategy_risk_multiplier": 0.0,
            }

        signal_rows.append(
            {
                "timestamp": timestamp,
                "desired_side": int(selected_row["desired_side"]),
                "stop_distance": float(selected_row["stop_distance"]),
                "stop_price": selected_row.get("stop_price"),
                "strategy_score": selected_row.get("strategy_score"),
                "strategy_risk_multiplier": selected_row.get("strategy_risk_multiplier", 1.0),
                "regime": regime,
                "route_key": route_key,
                "route_status": route_status,
                "selected_candidate_id": selected_candidate_id,
                "selected_candidate_name": selected_candidate_name,
                "selected_strategy_source": selected_strategy_source,
                "selected_strategy_name": selected_strategy_name,
                "selected_variant": selected_variant,
                "fallback_used": fallback_used,
            }
        )
        route_rows.append(
            {
                "timestamp": timestamp,
                "symbol": symbol,
                "regime": regime,
                "route_key": route_key,
                "route_status": route_status,
                "selected_candidate_id": selected_candidate_id,
                "selected_candidate_name": selected_candidate_name,
                "selected_strategy_source": selected_strategy_source,
                "selected_strategy_name": selected_strategy_name,
                "selected_variant": selected_variant,
                "desired_side": int(selected_row["desired_side"]),
                "stop_distance": float(selected_row["stop_distance"]),
                "strategy_score": selected_row.get("strategy_score"),
                "strategy_risk_multiplier": selected_row.get("strategy_risk_multiplier", 1.0),
                "fallback_used": fallback_used,
                "close": float(regime_row.close),
                "ema_fast": float(regime_row.ema_fast),
                "ema_slow": float(regime_row.ema_slow),
                "ema_trend": float(regime_row.ema_trend),
                "trend_slope_pct": float(regime_row.trend_slope_pct),
                "distance_to_trend_pct": float(regime_row.distance_to_trend_pct),
                "ema_spread_pct": float(regime_row.ema_spread_pct),
            }
        )

    routed_signal_frame = pd.DataFrame(signal_rows).sort_values("timestamp").reset_index(drop=True)
    routed_signal_frame.attrs["signal_bar"] = base_strategy.signal_bar
    route_frame = pd.DataFrame(route_rows).sort_values("timestamp").reset_index(drop=True)
    route_summary = summarize_route_frame(route_frame)
    return routed_signal_frame, route_frame, route_summary


def _load_route_candidates(
    *,
    session_factory,
    config: AppConfig,
    project_root: Path,
    symbol: str,
    signal_bars: pd.DataFrame,
    required_scope: str,
) -> dict[str, _RouteCandidateRuntime]:
    runtime_strategy = config.strategy.model_copy(deep=True)
    candidate_map = _normalized_candidate_map(config.trading.execution_candidate_map)
    candidates_by_id: dict[int, _RouteCandidateRuntime] = {}
    route_candidates: dict[str, _RouteCandidateRuntime] = {}

    with session_scope(session_factory) as session:
        for route_key, candidate_id in sorted(candidate_map.items()):
            candidate = session.get(StrategyCandidate, candidate_id)
            if candidate is None:
                raise ValueError(f"route {route_key} points to missing candidate {candidate_id}")
            strategy_config = _load_candidate_strategy_config(candidate=candidate, project_root=project_root)
            _validate_route_candidate(
                candidate=candidate,
                required_scope=required_scope,
                symbol=symbol,
                runtime_strategy=runtime_strategy,
                strategy_config=strategy_config,
            )
            runtime = candidates_by_id.get(candidate_id)
            if runtime is None:
                signal_frame = prepare_signal_frame(signal_bars, strategy_config)
                signal_frame.attrs["signal_bar"] = strategy_config.signal_bar
                runtime = _RouteCandidateRuntime(
                    route_key=route_key,
                    candidate_id=candidate.id,
                    candidate_name=candidate.candidate_name,
                    strategy_name=strategy_config.name,
                    variant=strategy_config.variant,
                    strategy_config=strategy_config,
                    signal_frame=signal_frame,
                    signal_lookup=_signal_lookup(signal_frame),
                )
                candidates_by_id[candidate_id] = runtime
            route_candidates[route_key] = runtime
    return route_candidates


def _validate_route_candidate(
    *,
    candidate: StrategyCandidate,
    required_scope: str,
    symbol: str,
    runtime_strategy: StrategyConfig,
    strategy_config: StrategyConfig,
) -> None:
    reasons: list[str] = []
    if candidate.status != "approved":
        reasons.append(f"candidate {candidate.id} is not approved")
    if not _scope_allows(candidate.approval_scope, required_scope):
        reasons.append(
            f"candidate {candidate.id} scope {candidate.approval_scope or 'none'} is not compatible with {required_scope}"
        )
    normalized_scope = sorted(str(item).strip() for item in (candidate.symbol_scope or []) if str(item).strip())
    if normalized_scope and symbol not in normalized_scope:
        reasons.append(f"candidate {candidate.id} symbol scope {normalized_scope} does not include {symbol}")
    if strategy_config.signal_bar != runtime_strategy.signal_bar:
        reasons.append(
            f"candidate {candidate.id} signal_bar {strategy_config.signal_bar} does not match runtime {runtime_strategy.signal_bar}"
        )
    if strategy_config.execution_bar != runtime_strategy.execution_bar:
        reasons.append(
            f"candidate {candidate.id} execution_bar {strategy_config.execution_bar} does not match runtime {runtime_strategy.execution_bar}"
        )

    if reasons:
        joined = "; ".join(reasons)
        raise ValueError(f"route candidate validation failed: {joined}")


def _load_candidate_strategy_config(*, candidate: StrategyCandidate, project_root: Path) -> StrategyConfig:
    raw_path = str(candidate.config_path or "").strip()
    if not raw_path:
        raise ValueError(f"candidate {candidate.id} does not have a config_path")
    config_path = Path(raw_path)
    if not config_path.is_absolute():
        config_path = (project_root / config_path).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"candidate config_path not found: {config_path}")
    loaded = load_config(config_path)
    return loaded.strategy.model_copy(deep=True)


def _signal_lookup(signal_frame: pd.DataFrame) -> dict[pd.Timestamp, dict[str, Any]]:
    return {
        pd.Timestamp(row.timestamp): {
            "desired_side": int(row.desired_side),
            "stop_distance": float(row.stop_distance),
            "stop_price": row.stop_price,
            "strategy_score": getattr(row, "strategy_score", pd.NA),
            "strategy_risk_multiplier": getattr(row, "strategy_risk_multiplier", 1.0),
        }
        for row in signal_frame.itertuples(index=False)
    }


def summarize_route_frame(route_frame: pd.DataFrame) -> dict[str, Any]:
    total_bars = len(route_frame)
    if total_bars == 0:
        return {
            "total_signal_bars": 0,
            "route_status_counts": {},
            "regime_counts": {},
            "route_key_counts": {},
            "candidate_usage": [],
            "candidate_bar_pct": 0.0,
            "fallback_bar_pct": 0.0,
            "flat_bar_pct": 0.0,
        }

    route_status_counts = {
        str(key): int(value)
        for key, value in route_frame["route_status"].value_counts(dropna=False).to_dict().items()
    }
    regime_counts = {
        str(key): int(value)
        for key, value in route_frame["regime"].value_counts(dropna=False).to_dict().items()
    }
    route_key_counts = {
        str(key): int(value)
        for key, value in route_frame["route_key"].fillna("unmapped").value_counts(dropna=False).to_dict().items()
    }
    candidate_usage_frame = (
        route_frame.loc[route_frame["selected_candidate_id"].notna(), ["selected_candidate_id", "selected_candidate_name"]]
        .value_counts(dropna=False)
        .reset_index(name="bar_count")
    )
    candidate_usage = [
        {
            "candidate_id": int(row.selected_candidate_id),
            "candidate_name": None if pd.isna(row.selected_candidate_name) else str(row.selected_candidate_name),
            "bar_count": int(row.bar_count),
            "bar_pct": round((float(row.bar_count) / total_bars) * 100.0, 2),
        }
        for row in candidate_usage_frame.itertuples(index=False)
    ]

    candidate_bar_pct = round((route_status_counts.get("candidate_config", 0) / total_bars) * 100.0, 2)
    fallback_bar_pct = round((route_status_counts.get("base_config_fallback", 0) / total_bars) * 100.0, 2)
    flat_bar_pct = round(
        (
            (
                route_status_counts.get("flat_missing_route", 0)
                + route_status_counts.get("flat_invalid_route", 0)
            )
            / total_bars
        )
        * 100.0,
        2,
    )
    return {
        "total_signal_bars": total_bars,
        "route_status_counts": route_status_counts,
        "regime_counts": regime_counts,
        "route_key_counts": route_key_counts,
        "candidate_usage": candidate_usage,
        "candidate_bar_pct": candidate_bar_pct,
        "fallback_bar_pct": fallback_bar_pct,
        "flat_bar_pct": flat_bar_pct,
    }
