from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


@dataclass
class AlphaSignalContract:
    side: int
    score: float | None
    regime: str | None = None
    strategy_name: str | None = None
    strategy_variant: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RiskSignalContract:
    stop_distance: float
    stop_price: float | None
    risk_multiplier: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ExecutionSignalContract:
    signal_time: pd.Timestamp
    desired_side: int
    previous_side: int
    side_changed: bool
    alpha_signal: AlphaSignalContract
    risk_signal: RiskSignalContract
    route_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["signal_time"] = self.signal_time.isoformat()
        return payload


def apply_signal_contract_columns(
    frame: pd.DataFrame,
    *,
    strategy_name: str | None = None,
    strategy_variant: str | None = None,
) -> pd.DataFrame:
    normalized = frame.copy()

    if "alpha_side" not in normalized.columns:
        if "entry_side" in normalized.columns:
            normalized["alpha_side"] = pd.to_numeric(normalized["entry_side"], errors="coerce").fillna(0).astype(int)
        elif "desired_side" in normalized.columns:
            normalized["alpha_side"] = pd.to_numeric(normalized["desired_side"], errors="coerce").fillna(0).astype(int)
        else:
            normalized["alpha_side"] = 0

    if "alpha_score" not in normalized.columns:
        alpha_score = None
        for column in ("strategy_score", "long_factor_score", "high_weight_factor_score"):
            if column in normalized.columns:
                alpha_score = pd.to_numeric(normalized[column], errors="coerce")
                break
        normalized["alpha_score"] = alpha_score if alpha_score is not None else pd.Series(pd.NA, index=normalized.index)

    if "alpha_regime" not in normalized.columns:
        regime_series = None
        for column in ("regime", "trend_regime"):
            if column in normalized.columns:
                regime_series = normalized[column].map(normalize_regime_label)
                break
        normalized["alpha_regime"] = regime_series if regime_series is not None else pd.Series(pd.NA, index=normalized.index)

    if "risk_stop_distance" not in normalized.columns:
        if "stop_distance" in normalized.columns:
            normalized["risk_stop_distance"] = pd.to_numeric(normalized["stop_distance"], errors="coerce").fillna(0.0)
        else:
            normalized["risk_stop_distance"] = 0.0

    if "risk_stop_price" not in normalized.columns:
        if "stop_price" in normalized.columns:
            normalized["risk_stop_price"] = pd.to_numeric(normalized["stop_price"], errors="coerce")
        else:
            normalized["risk_stop_price"] = pd.NA

    if "risk_multiplier" not in normalized.columns:
        if "strategy_risk_multiplier" in normalized.columns:
            normalized["risk_multiplier"] = pd.to_numeric(
                normalized["strategy_risk_multiplier"],
                errors="coerce",
            ).fillna(1.0)
        else:
            normalized["risk_multiplier"] = 1.0

    if "execution_desired_side" not in normalized.columns:
        if "desired_side" in normalized.columns:
            normalized["execution_desired_side"] = pd.to_numeric(
                normalized["desired_side"],
                errors="coerce",
            ).fillna(0).astype(int)
        else:
            normalized["execution_desired_side"] = normalized["alpha_side"]

    if strategy_name is not None and "contract_strategy_name" not in normalized.columns:
        normalized["contract_strategy_name"] = strategy_name
    if strategy_variant is not None and "contract_strategy_variant" not in normalized.columns:
        normalized["contract_strategy_variant"] = strategy_variant
    return normalized


def execution_signal_from_row(
    row: pd.Series | Any,
    *,
    previous_side: int = 0,
    strategy_name: str | None = None,
    strategy_variant: str | None = None,
) -> ExecutionSignalContract:
    signal_time = pd.Timestamp(_read_value(row, "timestamp"))
    desired_side = _to_int(
        _read_value(row, "execution_desired_side"),
        fallback=_to_int(_read_value(row, "desired_side"), fallback=0),
    )
    alpha_side = _to_int(
        _read_value(row, "alpha_side"),
        fallback=_to_int(_read_value(row, "entry_side"), fallback=desired_side),
    )
    alpha_score = _to_float(
        _read_value(row, "alpha_score"),
        _read_value(row, "strategy_score"),
        _read_value(row, "long_factor_score"),
        fallback=None,
    )
    alpha_regime = normalize_regime_label(
        _read_value(row, "alpha_regime", fallback=_read_value(row, "regime", fallback=_read_value(row, "trend_regime")))
    )
    stop_distance = _to_float(
        _read_value(row, "risk_stop_distance"),
        _read_value(row, "stop_distance"),
        fallback=0.0,
    ) or 0.0
    stop_price = _to_float(
        _read_value(row, "risk_stop_price"),
        _read_value(row, "stop_price"),
        fallback=None,
    )
    risk_multiplier = max(
        0.0,
        _to_float(
            _read_value(row, "risk_multiplier"),
            _read_value(row, "strategy_risk_multiplier"),
            fallback=1.0,
        )
        or 0.0,
    )
    route_key = _optional_text(_read_value(row, "route_key", fallback=None))
    contract_strategy_name = _optional_text(
        _read_value(row, "contract_strategy_name", fallback=strategy_name)
    ) or strategy_name
    contract_strategy_variant = _optional_text(
        _read_value(row, "contract_strategy_variant", fallback=strategy_variant)
    ) or strategy_variant

    return ExecutionSignalContract(
        signal_time=signal_time,
        desired_side=desired_side,
        previous_side=int(previous_side),
        side_changed=int(desired_side) != int(previous_side),
        alpha_signal=AlphaSignalContract(
            side=alpha_side,
            score=alpha_score,
            regime=alpha_regime,
            strategy_name=contract_strategy_name,
            strategy_variant=contract_strategy_variant,
        ),
        risk_signal=RiskSignalContract(
            stop_distance=stop_distance,
            stop_price=stop_price,
            risk_multiplier=risk_multiplier,
        ),
        route_key=route_key,
    )


def normalize_regime_label(value: object) -> str | None:
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
    numeric = _to_float(value, fallback=None)
    if numeric is None:
        return None
    if numeric > 0:
        return "bull_trend"
    if numeric < 0:
        return "bear_trend"
    return "range"


def _read_value(row: pd.Series | Any, key: str, *, fallback: Any = None) -> Any:
    if isinstance(row, pd.Series):
        return row.get(key, fallback)
    if isinstance(row, dict):
        return row.get(key, fallback)
    if hasattr(row, key):
        return getattr(row, key)
    if hasattr(row, "_asdict"):
        return row._asdict().get(key, fallback)
    return fallback


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


def _to_float(*values: Any, fallback: float | None = 0.0) -> float | None:
    for value in values:
        if value in {None, "", " "}:
            continue
        try:
            if pd.isna(value):
                continue
        except TypeError:
            pass
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return fallback


def _to_int(value: Any, *, fallback: int = 0) -> int:
    try:
        if value is None or pd.isna(value):
            return fallback
    except TypeError:
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback
