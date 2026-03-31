from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from quant_lab.config import AppConfig, StrategyConfig, configured_symbols, load_config
from quant_lab.service.database import StrategyCandidate, session_scope
from quant_lab.service.research_ops import serialize_strategy_candidate
from quant_lab.strategy_contracts import normalize_regime_label


@dataclass
class StrategyRouteDecision:
    enabled: bool
    ready: bool
    symbol: str
    regime: str
    route_key: str | None
    required_scope: str
    fallback_used: bool
    selected_strategy_source: str
    selected_strategy_name: str
    selected_variant: str
    selected_signal_bar: str
    selected_execution_bar: str
    candidate: dict[str, Any] | None
    reasons: list[str]
    regime_metrics: dict[str, Any]
    strategy_config: StrategyConfig

    def to_dict(self) -> dict[str, Any]:
        return serialize_strategy_route_decision(self)


def serialize_strategy_route_decision(
    decision: StrategyRouteDecision | dict[str, Any] | None,
    *,
    default_strategy: StrategyConfig | None = None,
    symbol: str | None = None,
    required_scope: str | None = None,
) -> dict[str, Any]:
    raw = _raw_route_decision_mapping(decision)
    route_payload = raw.get("route") if isinstance(raw.get("route"), dict) else {}
    selection_payload = raw.get("selection") if isinstance(raw.get("selection"), dict) else {}
    display_payload = raw.get("display") if isinstance(raw.get("display"), dict) else {}
    candidate = raw.get("candidate") if isinstance(raw.get("candidate"), dict) else None
    enabled_value = raw.get("enabled")
    if enabled_value is None:
        enabled_value = bool(
            route_payload
            or selection_payload
            or candidate
            or _optional_text(raw.get("regime"))
            or _optional_text(raw.get("route_key"))
            or _optional_text(raw.get("selected_strategy_name"))
            or _optional_text(raw.get("selected_variant"))
            or raw.get("ready") is not None
        )
    ready_value = bool(raw.get("ready"))

    resolved_symbol = _optional_text(raw.get("symbol")) or _optional_text(route_payload.get("symbol")) or symbol
    resolved_regime = normalize_regime_label(route_payload.get("regime"))
    if resolved_regime is None:
        resolved_regime = normalize_regime_label(raw.get("regime"))
    resolved_route_key = _optional_text(route_payload.get("route_key")) or _optional_text(raw.get("route_key"))
    resolved_required_scope = (
        _optional_text(route_payload.get("required_scope"))
        or _optional_text(raw.get("required_scope"))
        or required_scope
        or "demo"
    )
    fallback_used = bool(
        route_payload.get("fallback_used")
        if "fallback_used" in route_payload
        else raw.get("fallback_used")
    )

    selected_strategy_source = (
        _optional_text(selection_payload.get("source"))
        or _optional_text(raw.get("selected_strategy_source"))
        or ("base_config" if not enabled_value else "unresolved")
    )
    selected_strategy_name = (
        _optional_text(selection_payload.get("strategy_name"))
        or _optional_text(raw.get("selected_strategy_name"))
        or (default_strategy.name if default_strategy is not None else None)
    )
    selected_variant = (
        _optional_text(selection_payload.get("strategy_variant"))
        or _optional_text(raw.get("selected_variant"))
        or (default_strategy.variant if default_strategy is not None else None)
    )
    selected_signal_bar = (
        _optional_text(selection_payload.get("signal_bar"))
        or _optional_text(raw.get("selected_signal_bar"))
        or (default_strategy.signal_bar if default_strategy is not None else None)
    )
    selected_execution_bar = (
        _optional_text(selection_payload.get("execution_bar"))
        or _optional_text(raw.get("selected_execution_bar"))
        or (default_strategy.execution_bar if default_strategy is not None else None)
    )

    reasons = [str(item) for item in (raw.get("reasons") or []) if str(item).strip()]
    regime_metrics = raw.get("regime_metrics") if isinstance(raw.get("regime_metrics"), dict) else {}
    candidate_name = _optional_text(display_payload.get("candidate_name"))
    if candidate_name is None and isinstance(candidate, dict):
        candidate_name = _optional_text(candidate.get("candidate_name"))

    route_label = _optional_text(display_payload.get("route_label")) or _route_display_label(
        symbol=resolved_symbol,
        regime=resolved_regime,
        route_key=resolved_route_key,
    )
    selection_label = _optional_text(display_payload.get("selection_label")) or _selection_display_label(
        strategy_name=selected_strategy_name,
        strategy_variant=selected_variant,
        signal_bar=selected_signal_bar,
        execution_bar=selected_execution_bar,
    )
    status_label = _optional_text(display_payload.get("status_label")) or _route_status_label(
        enabled=enabled_value,
        ready=ready_value,
        fallback_used=fallback_used,
        selected_strategy_source=selected_strategy_source,
    )

    return {
        "enabled": enabled_value,
        "ready": ready_value,
        "symbol": resolved_symbol,
        "regime": resolved_regime,
        "route_key": resolved_route_key,
        "required_scope": resolved_required_scope,
        "fallback_used": fallback_used,
        "selected_strategy_source": selected_strategy_source,
        "selected_strategy_name": selected_strategy_name,
        "selected_variant": selected_variant,
        "selected_signal_bar": selected_signal_bar,
        "selected_execution_bar": selected_execution_bar,
        "candidate": candidate,
        "reasons": reasons,
        "regime_metrics": regime_metrics,
        "route": {
            "symbol": resolved_symbol,
            "regime": resolved_regime,
            "route_key": resolved_route_key,
            "required_scope": resolved_required_scope,
            "fallback_used": fallback_used,
            "label": route_label,
        },
        "selection": {
            "source": selected_strategy_source,
            "strategy_name": selected_strategy_name,
            "strategy_variant": selected_variant,
            "signal_bar": selected_signal_bar,
            "execution_bar": selected_execution_bar,
            "label": selection_label,
        },
        "display": {
            "route_label": route_label,
            "selection_label": selection_label,
            "status_label": status_label,
            "candidate_name": candidate_name,
        },
    }


def build_strategy_router_status(*, session_factory, config: AppConfig, required_scope: str = "demo") -> dict[str, Any]:
    enabled = bool(config.trading.strategy_router_enabled)
    candidate_map = _normalized_candidate_map(config.trading.execution_candidate_map)
    route_entries: list[dict[str, Any]] = []
    reasons: list[str] = []

    if not enabled:
        return {
            "enabled": False,
            "ready": True,
            "required_scope": required_scope,
            "candidate_map": candidate_map,
            "routes": [],
            "reasons": [],
        }

    if not candidate_map:
        reasons.append("strategy router is enabled but trading.execution_candidate_map is empty")
        return {
            "enabled": True,
            "ready": False,
            "required_scope": required_scope,
            "candidate_map": candidate_map,
            "routes": [],
            "reasons": reasons,
        }

    with session_scope(session_factory) as session:
        for route_key, candidate_id in sorted(candidate_map.items()):
            candidate = session.get(StrategyCandidate, candidate_id)
            entry_reasons: list[str] = []
            payload = serialize_strategy_candidate(candidate) if candidate is not None else None
            if candidate is None:
                entry_reasons.append(f"candidate {candidate_id} not found")
            else:
                if candidate.status != "approved":
                    entry_reasons.append(f"candidate {candidate_id} is not approved")
                if not _scope_allows(candidate.approval_scope, required_scope):
                    entry_reasons.append(
                        f"candidate {candidate_id} scope {candidate.approval_scope or 'none'} is not compatible with {required_scope}"
                    )
            route_entries.append(
                {
                    "route_key": route_key,
                    "candidate_id": candidate_id,
                    "candidate": payload,
                    "ready": not entry_reasons,
                    "reasons": entry_reasons,
                    "display": {
                        "route_label": route_key,
                        "candidate_name": payload.get("candidate_name") if isinstance(payload, dict) else None,
                        "reason_text": "; ".join(entry_reasons),
                    },
                }
            )
            reasons.extend(f"{route_key}: {item}" for item in entry_reasons)

    return {
        "enabled": True,
        "ready": not reasons,
        "required_scope": required_scope,
        "candidate_map": candidate_map,
        "routes": route_entries,
        "reasons": reasons,
    }


def resolve_strategy_route(
    *,
    session_factory,
    config: AppConfig,
    project_root: Path,
    symbol: str,
    signal_bars: pd.DataFrame,
    required_scope: str = "demo",
) -> StrategyRouteDecision:
    base_strategy = config.strategy.model_copy(deep=True)
    regime_payload = detect_market_regime(signal_bars=signal_bars, strategy_config=base_strategy)
    router_status = build_strategy_router_status(session_factory=session_factory, config=config, required_scope=required_scope)

    if not config.trading.strategy_router_enabled:
        return StrategyRouteDecision(
            enabled=False,
            ready=True,
            symbol=symbol,
            regime=regime_payload["regime"],
            route_key=None,
            required_scope=required_scope,
            fallback_used=False,
            selected_strategy_source="base_config",
            selected_strategy_name=base_strategy.name,
            selected_variant=base_strategy.variant,
            selected_signal_bar=base_strategy.signal_bar,
            selected_execution_bar=base_strategy.execution_bar,
            candidate=None,
            reasons=[],
            regime_metrics=regime_payload["metrics"],
            strategy_config=base_strategy,
        )

    candidate_map = _normalized_candidate_map(config.trading.execution_candidate_map)
    route_key = _resolve_route_key(candidate_map=candidate_map, symbol=symbol, regime=regime_payload["regime"])
    reasons: list[str] = []
    fallback_used = False
    selected_strategy = base_strategy
    selected_source = "base_config_fallback"
    selected_candidate: dict[str, Any] | None = None
    ready = False

    if route_key is None:
        if not router_status.get("routes"):
            reasons.extend(router_status.get("reasons") or [])
        reasons.append(f"no routed candidate configured for {symbol} in regime {regime_payload['regime']}")
    else:
        candidate_id = candidate_map.get(route_key)
        route_entry = next(
            (item for item in router_status.get("routes") or [] if item.get("route_key") == route_key),
            None,
        )
        if isinstance(route_entry, dict):
            reasons.extend([str(item) for item in route_entry.get("reasons") or []])
        with session_scope(session_factory) as session:
            candidate = session.get(StrategyCandidate, candidate_id) if candidate_id is not None else None
            if candidate is None:
                reasons.append(f"candidate {candidate_id} for route {route_key} was not found")
            else:
                selected_candidate = serialize_strategy_candidate(candidate)
                reasons.extend(
                    _route_candidate_reasons(
                        candidate=candidate,
                        required_scope=required_scope,
                        symbol=symbol,
                        runtime_config=config,
                    )
                )
                candidate_strategy = _load_candidate_strategy_config(
                    candidate=candidate,
                    project_root=project_root,
                )
                if candidate_strategy is None:
                    reasons.append(f"candidate {candidate.candidate_name} does not have a valid config_path")
                else:
                    strategy_reasons = _candidate_strategy_reasons(
                        candidate_strategy=candidate_strategy,
                        runtime_strategy=base_strategy,
                    )
                    reasons.extend(strategy_reasons)
                    if not strategy_reasons:
                        selected_strategy = candidate_strategy
                        selected_source = "candidate_config"
                        ready = not reasons

    if not ready and config.trading.strategy_router_fallback_to_config:
        fallback_used = True
        selected_strategy = base_strategy
        selected_source = "base_config_fallback"

    return StrategyRouteDecision(
        enabled=True,
        ready=ready,
        symbol=symbol,
        regime=regime_payload["regime"],
        route_key=route_key,
        required_scope=required_scope,
        fallback_used=fallback_used,
        selected_strategy_source=selected_source,
        selected_strategy_name=selected_strategy.name,
        selected_variant=selected_strategy.variant,
        selected_signal_bar=selected_strategy.signal_bar,
        selected_execution_bar=selected_strategy.execution_bar,
        candidate=selected_candidate,
        reasons=reasons,
        regime_metrics=regime_payload["metrics"],
        strategy_config=selected_strategy,
    )


def detect_market_regime(*, signal_bars: pd.DataFrame, strategy_config: StrategyConfig) -> dict[str, Any]:
    regime_frame = build_market_regime_frame(signal_bars=signal_bars, strategy_config=strategy_config)
    latest = regime_frame.iloc[-1]
    return {
        "regime": str(latest["regime"]),
        "metrics": {
            "close": round(float(latest["close"]), 6),
            "ema_fast": round(float(latest["ema_fast"]), 6),
            "ema_slow": round(float(latest["ema_slow"]), 6),
            "ema_trend": round(float(latest["ema_trend"]), 6),
            "trend_slope_pct": round(float(latest["trend_slope_pct"]), 4),
            "distance_to_trend_pct": round(float(latest["distance_to_trend_pct"]), 4),
            "ema_spread_pct": round(float(latest["ema_spread_pct"]), 4),
            "timestamp": str(latest["timestamp"]),
        },
    }


def build_market_regime_frame(*, signal_bars: pd.DataFrame, strategy_config: StrategyConfig) -> pd.DataFrame:
    frame = signal_bars.copy()
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    if frame.empty:
        raise ValueError("signal_bars is empty")

    fast_span = max(12, int(strategy_config.fast_ema))
    slow_span = max(fast_span + 1, int(strategy_config.slow_ema))
    trend_span = max(slow_span + 1, int(strategy_config.trend_ema))
    slope_window = max(3, int(strategy_config.trend_slope_window))

    frame["ema_fast"] = frame["close"].ewm(span=fast_span, adjust=False).mean()
    frame["ema_slow"] = frame["close"].ewm(span=slow_span, adjust=False).mean()
    frame["ema_trend"] = frame["close"].ewm(span=trend_span, adjust=False).mean()
    frame["trend_slope_pct"] = frame["ema_trend"].pct_change(slope_window)
    frame["trend_slope_pct"] = frame["trend_slope_pct"].fillna(0.0) * 100.0
    frame["distance_to_trend_pct"] = ((frame["close"] / frame["ema_trend"].replace(0, pd.NA)) - 1.0) * 100.0
    frame["ema_spread_pct"] = ((frame["ema_fast"] / frame["ema_slow"].replace(0, pd.NA)) - 1.0) * 100.0
    frame["distance_to_trend_pct"] = frame["distance_to_trend_pct"].fillna(0.0)
    frame["ema_spread_pct"] = frame["ema_spread_pct"].fillna(0.0)

    frame["regime"] = "range"
    bull_mask = (
        (frame["close"] > frame["ema_trend"])
        & (frame["ema_fast"] > frame["ema_slow"])
        & (frame["trend_slope_pct"] > 0)
    )
    bear_mask = (
        (frame["close"] < frame["ema_trend"])
        & (frame["ema_fast"] < frame["ema_slow"])
        & (frame["trend_slope_pct"] < 0)
    )
    frame.loc[bull_mask, "regime"] = "bull_trend"
    frame.loc[bear_mask, "regime"] = "bear_trend"

    return frame[
        [
            "timestamp",
            "close",
            "ema_fast",
            "ema_slow",
            "ema_trend",
            "trend_slope_pct",
            "distance_to_trend_pct",
            "ema_spread_pct",
            "regime",
        ]
    ].reset_index(drop=True)


def _normalized_candidate_map(raw_map: dict[str, Any] | None) -> dict[str, int]:
    if not isinstance(raw_map, dict):
        return {}
    normalized: dict[str, int] = {}
    for raw_key, raw_value in raw_map.items():
        key = str(raw_key).strip()
        if not key:
            continue
        try:
            normalized[key] = int(raw_value)
        except (TypeError, ValueError):
            continue
    return normalized


def _resolve_route_key(*, candidate_map: dict[str, int], symbol: str, regime: str) -> str | None:
    for key in (f"{symbol}:{regime}", regime, f"{symbol}:default", "default"):
        if key in candidate_map:
            return key
    return None


def _route_candidate_reasons(
    *,
    candidate: StrategyCandidate,
    required_scope: str,
    symbol: str,
    runtime_config: AppConfig,
) -> list[str]:
    reasons: list[str] = []
    if candidate.status != "approved":
        reasons.append(f"candidate {candidate.id} is not approved")
    if not _scope_allows(candidate.approval_scope, required_scope):
        reasons.append(
            f"candidate {candidate.id} scope {candidate.approval_scope or 'none'} is not compatible with {required_scope}"
        )
    normalized_scope = sorted(candidate.symbol_scope or [])
    if normalized_scope and symbol not in normalized_scope:
        reasons.append(f"candidate {candidate.id} symbol scope {normalized_scope} does not include {symbol}")
    runtime_symbols = configured_symbols(runtime_config)
    if len(runtime_symbols) == 1 and normalized_scope and normalized_scope != [symbol]:
        reasons.append(f"candidate {candidate.id} symbol scope {normalized_scope} does not match single-symbol runtime")
    return reasons


def _load_candidate_strategy_config(*, candidate: StrategyCandidate, project_root: Path) -> StrategyConfig | None:
    raw_path = str(candidate.config_path or "").strip()
    if not raw_path:
        return None
    config_path = Path(raw_path)
    if not config_path.is_absolute():
        config_path = (project_root / config_path).resolve()
    if not config_path.exists():
        return None
    loaded = load_config(config_path)
    return loaded.strategy.model_copy(deep=True)


def _candidate_strategy_reasons(
    *,
    candidate_strategy: StrategyConfig,
    runtime_strategy: StrategyConfig,
) -> list[str]:
    reasons: list[str] = []
    if candidate_strategy.signal_bar != runtime_strategy.signal_bar:
        reasons.append(
            f"candidate signal_bar {candidate_strategy.signal_bar} does not match runtime {runtime_strategy.signal_bar}"
        )
    if candidate_strategy.execution_bar != runtime_strategy.execution_bar:
        reasons.append(
            f"candidate execution_bar {candidate_strategy.execution_bar} does not match runtime {runtime_strategy.execution_bar}"
        )
    return reasons


def _scope_allows(candidate_scope: str | None, required_scope: str) -> bool:
    candidate = str(candidate_scope or "").strip().lower()
    required = str(required_scope or "").strip().lower()
    if candidate == required:
        return True
    if required == "demo" and candidate == "live":
        return True
    return False


def _raw_route_decision_mapping(decision: StrategyRouteDecision | dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(decision, StrategyRouteDecision):
        return {
            "enabled": decision.enabled,
            "ready": decision.ready,
            "symbol": decision.symbol,
            "regime": decision.regime,
            "route_key": decision.route_key,
            "required_scope": decision.required_scope,
            "fallback_used": decision.fallback_used,
            "selected_strategy_source": decision.selected_strategy_source,
            "selected_strategy_name": decision.selected_strategy_name,
            "selected_variant": decision.selected_variant,
            "selected_signal_bar": decision.selected_signal_bar,
            "selected_execution_bar": decision.selected_execution_bar,
            "candidate": decision.candidate,
            "reasons": decision.reasons,
            "regime_metrics": decision.regime_metrics,
        }
    if isinstance(decision, dict):
        return dict(decision)
    return {}


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _route_display_label(*, symbol: str | None, regime: str | None, route_key: str | None) -> str:
    if route_key:
        return route_key
    if symbol and regime:
        return f"{symbol}:{regime}"
    if regime:
        return regime
    if symbol:
        return symbol
    return "unresolved"


def _selection_display_label(
    *,
    strategy_name: str | None,
    strategy_variant: str | None,
    signal_bar: str | None,
    execution_bar: str | None,
) -> str:
    strategy_bits = " / ".join(
        [item for item in (strategy_name, strategy_variant) if item]
    ) or "unresolved"
    if signal_bar and execution_bar:
        return f"{strategy_bits} @ {signal_bar}->{execution_bar}"
    if signal_bar:
        return f"{strategy_bits} @ {signal_bar}"
    return strategy_bits


def _route_status_label(
    *,
    enabled: bool,
    ready: bool,
    fallback_used: bool,
    selected_strategy_source: str | None,
) -> str:
    if not enabled:
        return "router_disabled"
    if ready:
        return "route_ready"
    if fallback_used:
        return "fallback_active"
    if selected_strategy_source == "candidate_config":
        return "candidate_blocked"
    return "route_blocked"
