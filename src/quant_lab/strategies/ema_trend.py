from __future__ import annotations

import pandas as pd

from quant_lab.config import StrategyConfig
from quant_lab.strategy_contracts import apply_signal_contract_columns
from quant_lab.utils.timeframes import bar_to_timedelta


SUPPORTED_VARIANTS = {
    "ema_cross",
    "ema_cross_regime",
    "ema_cross_adx",
    "ema_cross_regime_adx",
    "factor_trend_long",
    "high_weight_long",
    "trend_regime_long",
    "trend_pullback_long",
    "trend_breakout_long",
    "breakout_retest",
    "breakout_retest_regime",
    "breakout_retest_adx",
    "breakout_retest_regime_adx",
}


def prepare_signal_frame(candles: pd.DataFrame, config: StrategyConfig) -> pd.DataFrame:
    frame = candles.copy()
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    normalized_variant = (config.variant or "ema_cross").strip().lower()
    frame["quote_volume"] = _quote_volume(frame)

    frame["ema_fast"] = frame["close"].ewm(span=config.fast_ema, adjust=False).mean()
    frame["ema_slow"] = frame["close"].ewm(span=config.slow_ema, adjust=False).mean()
    frame["ema_trend"] = frame["close"].ewm(span=config.trend_ema, adjust=False).mean()
    frame["pullback_ema"] = frame["close"].ewm(span=config.fast_ema, adjust=False).mean()
    frame["atr"] = _average_true_range(frame, config.atr_period)
    frame["adx"] = _average_directional_index(frame, config.adx_period)
    frame["breakout_high"] = frame["high"].rolling(config.slow_ema, min_periods=config.slow_ema).max().shift(1)
    frame["breakout_low"] = frame["low"].rolling(config.slow_ema, min_periods=config.slow_ema).min().shift(1)
    frame["channel_width_atr"] = (frame["breakout_high"] - frame["breakout_low"]) / frame["atr"].replace(0, pd.NA)
    frame["atr_pct"] = frame["atr"] / frame["close"].replace(0, pd.NA)
    frame["atr_pct_baseline"] = frame["atr_pct"].rolling(config.volatility_window, min_periods=5).median()
    frame["volatility_ratio"] = frame["atr_pct"] / frame["atr_pct_baseline"].replace(0, pd.NA)
    frame["volume_avg"] = frame["quote_volume"].rolling(config.volume_window, min_periods=1).mean()
    frame["volume_ratio"] = frame["quote_volume"] / frame["volume_avg"].replace(0, pd.NA)
    frame["cmf"] = _chaikin_money_flow(frame, config.cmf_period)
    frame["body_atr_ratio"] = (frame["close"] - frame["open"]).abs() / frame["atr"].replace(0, pd.NA)
    frame["ema_spread_atr"] = (frame["ema_fast"] - frame["ema_slow"]) / frame["atr"].replace(0, pd.NA)
    frame["slow_ema_slope_atr"] = (
        frame["ema_slow"] - frame["ema_slow"].shift(config.trend_slope_window)
    ) / frame["atr"].replace(0, pd.NA)
    frame["trend_slope_atr"] = (
        frame["ema_trend"] - frame["ema_trend"].shift(config.trend_slope_window)
    ) / frame["atr"].replace(0, pd.NA)
    frame["breakout_distance_atr"] = (frame["close"] - frame["breakout_high"]) / frame["atr"].replace(0, pd.NA)
    frame = _attach_completed_daily_context(frame)
    frame["rolling_vwap"] = _rolling_vwap(frame, config.slow_ema)
    frame["basis_bps"] = _basis_bps(frame)
    frame["daily_regime_score"] = frame.apply(
        lambda row: _daily_ema_position_score(
            close=row["close"],
            daily_close=row["daily_close"],
            daily_ema_200=row["daily_ema_200"],
            daily_ema_200_slope=row["daily_ema_200_slope"],
        ),
        axis=1,
    )
    frame["trend_alignment_score"] = frame.apply(
        lambda row: _trend_alignment_score(
            close=row["close"],
            trend_ema=row["ema_trend"],
            ema_fast=row["ema_fast"],
            ema_slow=row["ema_slow"],
            ema_spread_atr=row["ema_spread_atr"],
            min_ema_spread_atr=config.min_ema_spread_atr,
            daily_close=row["daily_close"],
            daily_ema_200=row["daily_ema_200"],
        ),
        axis=1,
    )
    frame["trend_strength_score"] = frame.apply(
        lambda row: _trend_strength_score(
            slow_ema_slope_atr=row["slow_ema_slope_atr"],
            adx=row["adx"],
            adx_threshold=config.adx_threshold,
        ),
        axis=1,
    )
    frame["flow_quality_score"] = frame.apply(
        lambda row: _flow_quality_score(
            volume_ratio=row["volume_ratio"],
            min_volume_ratio=config.min_volume_ratio,
            cmf=row["cmf"],
            min_cmf_abs=config.min_cmf_abs,
            body_atr_ratio=row["body_atr_ratio"],
            min_body_atr_ratio=config.min_body_atr_ratio,
        ),
        axis=1,
    )
    frame["pullback_quality_score"] = frame.apply(
        lambda row: _pullback_quality_score(
            open_price=row["open"],
            close=row["close"],
            low=row["low"],
            trend_ema=row["ema_trend"],
            pullback_ema=row["pullback_ema"],
            rolling_vwap=row["rolling_vwap"],
            atr=row["atr"],
        ),
        axis=1,
    )
    frame["breakout_quality_score"] = frame.apply(
        lambda row: _breakout_quality_score(
            breakout_distance_atr=row["breakout_distance_atr"],
            breakout_buffer_atr=config.breakout_buffer_atr,
            max_breakout_extension_atr=config.max_breakout_extension_atr,
            channel_width_atr=row["channel_width_atr"],
            min_channel_width_atr=config.min_channel_width_atr,
            volatility_ratio=row["volatility_ratio"],
            min_volatility_ratio=config.min_volatility_ratio,
            max_volatility_ratio=config.max_volatility_ratio,
        ),
        axis=1,
    )
    frame["basis_regime_score"] = frame["basis_bps"].apply(_basis_regime_score)
    frame["stop_distance"] = frame["atr"] * config.atr_stop_multiple
    frame["trend_regime"] = frame.apply(
        lambda row: _trend_regime(close=row["close"], trend_ema=row["ema_trend"]),
        axis=1,
    )
    high_weight_scores = frame.apply(
        lambda row: _high_weight_long_components(
            close=row["close"],
            trend_ema=row["ema_trend"],
            ema_fast=row["ema_fast"],
            ema_slow=row["ema_slow"],
            daily_close=row["daily_close"],
            daily_ema_200=row["daily_ema_200"],
            daily_ema_200_slope=row["daily_ema_200_slope"],
            slow_ema_slope_atr=row["slow_ema_slope_atr"],
            breakout_distance_atr=row["breakout_distance_atr"],
            breakout_buffer_atr=config.breakout_buffer_atr,
            max_breakout_extension_atr=config.max_breakout_extension_atr,
            volume_ratio=row["volume_ratio"],
            min_volume_ratio=config.min_volume_ratio,
            basis_bps=row["basis_bps"],
        ),
        axis=1,
        result_type="expand",
    )
    frame = pd.concat([frame, high_weight_scores], axis=1)
    frame["trend_regime_score"] = frame.apply(
        lambda row: _weighted_strategy_score(
            {
                "daily_regime_score": (row["daily_regime_score"], 25.0),
                "trend_alignment_score": (row["trend_alignment_score"], 25.0),
                "trend_strength_score": (row["trend_strength_score"], 25.0),
                "flow_quality_score": (row["flow_quality_score"], 15.0),
                "basis_regime_score": (row["basis_regime_score"], 10.0),
            }
        ),
        axis=1,
    )
    frame["trend_regime_core_score"] = frame.apply(
        lambda row: _weighted_strategy_score(
            {
                "daily_regime_score": (row["daily_regime_score"], 25.0),
                "trend_alignment_score": (row["trend_alignment_score"], 25.0),
            }
        ),
        axis=1,
    )
    frame["trend_pullback_score"] = frame.apply(
        lambda row: _weighted_strategy_score(
            {
                "daily_regime_score": (row["daily_regime_score"], 20.0),
                "trend_alignment_score": (row["trend_alignment_score"], 20.0),
                "pullback_quality_score": (row["pullback_quality_score"], 30.0),
                "flow_quality_score": (row["flow_quality_score"], 20.0),
                "basis_regime_score": (row["basis_regime_score"], 10.0),
            }
        ),
        axis=1,
    )
    frame["trend_pullback_core_score"] = frame.apply(
        lambda row: _weighted_strategy_score(
            {
                "daily_regime_score": (row["daily_regime_score"], 20.0),
                "trend_alignment_score": (row["trend_alignment_score"], 20.0),
                "pullback_quality_score": (row["pullback_quality_score"], 30.0),
            }
        ),
        axis=1,
    )
    frame["trend_breakout_score"] = frame.apply(
        lambda row: _weighted_strategy_score(
            {
                "daily_regime_score": (row["daily_regime_score"], 20.0),
                "trend_alignment_score": (row["trend_alignment_score"], 20.0),
                "breakout_quality_score": (row["breakout_quality_score"], 30.0),
                "flow_quality_score": (row["flow_quality_score"], 20.0),
                "basis_regime_score": (row["basis_regime_score"], 10.0),
            }
        ),
        axis=1,
    )
    frame["trend_breakout_core_score"] = frame.apply(
        lambda row: _weighted_strategy_score(
            {
                "daily_regime_score": (row["daily_regime_score"], 20.0),
                "trend_alignment_score": (row["trend_alignment_score"], 20.0),
                "breakout_quality_score": (row["breakout_quality_score"], 30.0),
            }
        ),
        axis=1,
    )
    frame["long_factor_score"] = frame.apply(
        lambda row: _factor_trend_long_score(
            close=row["close"],
            ema_fast=row["ema_fast"],
            ema_slow=row["ema_slow"],
            trend_ema=row["ema_trend"],
            adx=row["adx"],
            adx_threshold=config.adx_threshold,
            volume_ratio=row["volume_ratio"],
            min_volume_ratio=config.min_volume_ratio,
            cmf=row["cmf"],
            min_cmf_abs=config.min_cmf_abs,
            body_atr_ratio=row["body_atr_ratio"],
            min_body_atr_ratio=config.min_body_atr_ratio,
            breakout_distance_atr=row["breakout_distance_atr"],
            breakout_buffer_atr=config.breakout_buffer_atr,
            max_breakout_extension_atr=config.max_breakout_extension_atr,
            channel_width_atr=row["channel_width_atr"],
            min_channel_width_atr=config.min_channel_width_atr,
            volatility_ratio=row["volatility_ratio"],
            min_volatility_ratio=config.min_volatility_ratio,
            max_volatility_ratio=config.max_volatility_ratio,
            ema_spread_atr=row["ema_spread_atr"],
            min_ema_spread_atr=config.min_ema_spread_atr,
            trend_slope_atr=row["trend_slope_atr"],
            min_trend_slope_atr=config.min_trend_slope_atr,
            pullback_ema=row["pullback_ema"],
            breakout_high=row["breakout_high"],
            atr=row["atr"],
            retest_tolerance_atr=config.retest_tolerance_atr,
        ),
        axis=1,
    )
    frame["entry_side"] = frame.apply(
        lambda row: _desired_side_from_indicators(
            variant=normalized_variant,
            ema_fast=row["ema_fast"],
            ema_slow=row["ema_slow"],
            close=row["close"],
            trend_ema=row["ema_trend"],
            adx=row["adx"],
            adx_threshold=config.adx_threshold,
            allow_short=config.allow_short,
            volume_ratio=row["volume_ratio"],
            min_volume_ratio=config.min_volume_ratio,
            cmf=row["cmf"],
            min_cmf_abs=config.min_cmf_abs,
            body_atr_ratio=row["body_atr_ratio"],
            min_body_atr_ratio=config.min_body_atr_ratio,
            breakout_high=row["breakout_high"],
            breakout_low=row["breakout_low"],
            pullback_ema=row["pullback_ema"],
            atr=row["atr"],
            channel_width_atr=row["channel_width_atr"],
            min_channel_width_atr=config.min_channel_width_atr,
            volatility_ratio=row["volatility_ratio"],
            min_volatility_ratio=config.min_volatility_ratio,
            max_volatility_ratio=config.max_volatility_ratio,
            breakout_buffer_atr=config.breakout_buffer_atr,
            retest_tolerance_atr=config.retest_tolerance_atr,
            max_breakout_extension_atr=config.max_breakout_extension_atr,
            ema_spread_atr=row["ema_spread_atr"],
            min_ema_spread_atr=config.min_ema_spread_atr,
            trend_slope_atr=row["trend_slope_atr"],
            min_trend_slope_atr=config.min_trend_slope_atr,
            breakout_distance_atr=row["breakout_distance_atr"],
            long_factor_score=row["long_factor_score"],
            high_weight_factor_score=row["high_weight_factor_score"],
            high_weight_core_score=row["high_weight_core_score"],
            trend_regime_score=row["trend_regime_score"],
            trend_regime_core_score=row["trend_regime_core_score"],
            trend_pullback_score=row["trend_pullback_score"],
            trend_pullback_core_score=row["trend_pullback_core_score"],
            trend_breakout_score=row["trend_breakout_score"],
            trend_breakout_core_score=row["trend_breakout_core_score"],
            min_trend_score=config.min_trend_score,
        ),
        axis=1,
    )
    frame["strategy_score"] = pd.NA
    frame["strategy_risk_multiplier"] = 1.0
    if normalized_variant == "high_weight_long":
        frame["strategy_score"] = frame["high_weight_factor_score"]
        frame["strategy_risk_multiplier"] = frame["strategy_score"].apply(_factor_score_risk_multiplier)
    elif normalized_variant == "trend_regime_long":
        frame["strategy_score"] = frame["trend_regime_score"]
        frame["strategy_risk_multiplier"] = frame["strategy_score"].apply(_factor_score_risk_multiplier)
    elif normalized_variant == "trend_pullback_long":
        frame["strategy_score"] = frame["trend_pullback_score"]
        frame["strategy_risk_multiplier"] = frame["strategy_score"].apply(_factor_score_risk_multiplier)
    elif normalized_variant == "trend_breakout_long":
        frame["strategy_score"] = frame["trend_breakout_score"]
        frame["strategy_risk_multiplier"] = frame["strategy_score"].apply(_factor_score_risk_multiplier)
    elif normalized_variant == "factor_trend_long":
        frame["strategy_score"] = frame["long_factor_score"]
        frame["strategy_risk_multiplier"] = frame["strategy_score"].apply(_factor_score_risk_multiplier)

    minimum_requirements = [
        config.slow_ema,
        config.atr_period,
        config.volume_window,
        config.cmf_period,
        config.volatility_window,
    ]
    if "regime" in normalized_variant:
        minimum_requirements.append(config.trend_ema)
    if "adx" in normalized_variant:
        minimum_requirements.append(config.adx_period)
    if normalized_variant in {"high_weight_long", "trend_regime_long", "trend_pullback_long", "trend_breakout_long"}:
        minimum_requirements.append(_daily_history_requirement_bars(config.signal_bar, ema_days=205))

    minimum_history = max(minimum_requirements) + 1
    if minimum_history > 0:
        frame.loc[: minimum_history - 1, "entry_side"] = 0

    if normalized_variant in {
        "factor_trend_long",
        "high_weight_long",
        "trend_regime_long",
        "trend_pullback_long",
        "trend_breakout_long",
    }:
        desired_side, stop_prices = _apply_factor_trend_position_state(frame)
        frame["desired_side"] = desired_side
        frame["stop_price"] = stop_prices
    elif "breakout_retest" in normalized_variant:
        desired_side, stop_prices = _apply_breakout_position_state(frame)
        frame["desired_side"] = desired_side
        frame["stop_price"] = stop_prices
    else:
        frame["desired_side"] = frame["entry_side"]
        frame["stop_price"] = pd.NA

    if minimum_history > 0:
        frame.loc[: minimum_history - 1, "desired_side"] = 0
        frame.loc[: minimum_history - 1, "stop_price"] = pd.NA

    return apply_signal_contract_columns(
        frame,
        strategy_name=config.name,
        strategy_variant=normalized_variant,
    )


def _apply_breakout_position_state(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    desired_side: list[int] = []
    stop_prices: list[float | pd.NA] = []
    current_side = 0
    trailing_stop: float | None = None
    highest_price: float | None = None

    for row in frame.itertuples(index=False):
        entry_side = int(row.entry_side)
        close = float(row.close)
        high = float(row.high)
        stop_distance = _optional_float(row.stop_distance)
        trend_ema = _optional_float(row.ema_trend)

        if current_side == 0:
            trailing_stop = None
            highest_price = None
            if entry_side == 1 and stop_distance is not None and stop_distance > 0:
                current_side = 1
                highest_price = high
                trailing_stop = close - stop_distance
        elif current_side == 1:
            highest_price = max(highest_price if highest_price is not None else high, high)
            if stop_distance is not None and stop_distance > 0:
                candidate_stop = highest_price - stop_distance
                trailing_stop = candidate_stop if trailing_stop is None else max(trailing_stop, candidate_stop)
            if trend_ema is None or close <= trend_ema:
                current_side = 0
                trailing_stop = None
                highest_price = None

        desired_side.append(current_side)
        stop_prices.append(trailing_stop if current_side != 0 and trailing_stop is not None else pd.NA)

    return pd.Series(desired_side, index=frame.index, dtype="int64"), pd.Series(stop_prices, index=frame.index)


def _apply_factor_trend_position_state(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    desired_side: list[int] = []
    stop_prices: list[float | pd.NA] = []
    current_side = 0
    trailing_stop: float | None = None
    highest_price: float | None = None

    for row in frame.itertuples(index=False):
        entry_side = int(row.entry_side)
        close = float(row.close)
        high = float(row.high)
        ema_fast = _optional_float(row.ema_fast)
        ema_slow = _optional_float(row.ema_slow)
        trend_ema = _optional_float(row.ema_trend)
        stop_distance = _optional_float(row.stop_distance)

        if current_side == 0:
            trailing_stop = None
            highest_price = None
            if entry_side == 1 and stop_distance is not None and stop_distance > 0:
                current_side = 1
                highest_price = high
                trailing_stop = close - stop_distance
        elif current_side == 1:
            highest_price = max(highest_price if highest_price is not None else high, high)
            if stop_distance is not None and stop_distance > 0:
                candidate_stop = highest_price - stop_distance
                trailing_stop = candidate_stop if trailing_stop is None else max(trailing_stop, candidate_stop)
            if (
                trend_ema is None
                or close <= trend_ema
                or ema_fast is None
                or ema_slow is None
                or ema_fast <= ema_slow
            ):
                current_side = 0
                trailing_stop = None
                highest_price = None

        desired_side.append(current_side)
        stop_prices.append(trailing_stop if current_side != 0 and trailing_stop is not None else pd.NA)

    return pd.Series(desired_side, index=frame.index, dtype="int64"), pd.Series(stop_prices, index=frame.index)


def _desired_side_from_indicators(
    *,
    variant: str,
    ema_fast: float,
    ema_slow: float,
    close: float,
    trend_ema: float,
    adx: float,
    adx_threshold: float,
    allow_short: bool,
    volume_ratio: float = 0.0,
    min_volume_ratio: float = 0.0,
    cmf: float = 0.0,
    min_cmf_abs: float = 0.0,
    body_atr_ratio: float = 0.0,
    min_body_atr_ratio: float = 0.0,
    breakout_high: float = 0.0,
    breakout_low: float = 0.0,
    pullback_ema: float = 0.0,
    atr: float = 0.0,
    channel_width_atr: float = 0.0,
    min_channel_width_atr: float = 0.0,
    volatility_ratio: float = 0.0,
    min_volatility_ratio: float = 0.0,
    max_volatility_ratio: float = 0.0,
    breakout_buffer_atr: float = 0.0,
    retest_tolerance_atr: float = 0.0,
    max_breakout_extension_atr: float = 0.0,
    ema_spread_atr: float = 0.0,
    min_ema_spread_atr: float = 0.0,
    trend_slope_atr: float = 0.0,
    min_trend_slope_atr: float = 0.0,
    breakout_distance_atr: float = 0.0,
    long_factor_score: float = 0.0,
    high_weight_factor_score: float = 0.0,
    high_weight_core_score: float = 0.0,
    trend_regime_score: float = 0.0,
    trend_regime_core_score: float = 0.0,
    trend_pullback_score: float = 0.0,
    trend_pullback_core_score: float = 0.0,
    trend_breakout_score: float = 0.0,
    trend_breakout_core_score: float = 0.0,
    min_trend_score: float = 0.0,
) -> int:
    normalized_variant = (variant or "ema_cross").strip().lower()
    if normalized_variant not in SUPPORTED_VARIANTS:
        raise ValueError(f"Unsupported strategy variant: {variant}")

    if normalized_variant == "high_weight_long":
        return _high_weight_long_side(
            close=close,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
            trend_ema=trend_ema,
            high_weight_factor_score=high_weight_factor_score,
            high_weight_core_score=high_weight_core_score,
            min_trend_score=min_trend_score,
        )

    if normalized_variant == "trend_regime_long":
        return _scored_long_side(
            close=close,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
            trend_ema=trend_ema,
            strategy_score=trend_regime_score,
            core_score=trend_regime_core_score,
            core_threshold=30.0,
            min_trend_score=min_trend_score,
        )

    if normalized_variant == "trend_pullback_long":
        return _scored_long_side(
            close=close,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
            trend_ema=trend_ema,
            strategy_score=trend_pullback_score,
            core_score=trend_pullback_core_score,
            core_threshold=38.0,
            min_trend_score=min_trend_score,
        )

    if normalized_variant == "trend_breakout_long":
        return _scored_long_side(
            close=close,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
            trend_ema=trend_ema,
            strategy_score=trend_breakout_score,
            core_score=trend_breakout_core_score,
            core_threshold=38.0,
            min_trend_score=min_trend_score,
        )

    if normalized_variant == "factor_trend_long":
        return _factor_trend_long_side(
            close=close,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
            trend_ema=trend_ema,
            adx=adx,
            adx_threshold=adx_threshold,
            volume_ratio=volume_ratio,
            min_volume_ratio=min_volume_ratio,
            cmf=cmf,
            min_cmf_abs=min_cmf_abs,
            body_atr_ratio=body_atr_ratio,
            min_body_atr_ratio=min_body_atr_ratio,
            channel_width_atr=channel_width_atr,
            min_channel_width_atr=min_channel_width_atr,
            volatility_ratio=volatility_ratio,
            min_volatility_ratio=min_volatility_ratio,
            max_volatility_ratio=max_volatility_ratio,
            breakout_distance_atr=breakout_distance_atr,
            breakout_buffer_atr=breakout_buffer_atr,
            max_breakout_extension_atr=max_breakout_extension_atr,
            ema_spread_atr=ema_spread_atr,
            min_ema_spread_atr=min_ema_spread_atr,
            trend_slope_atr=trend_slope_atr,
            min_trend_slope_atr=min_trend_slope_atr,
            long_factor_score=long_factor_score,
            min_trend_score=min_trend_score,
        )

    if "breakout_retest" in normalized_variant:
        base_side = _breakout_retest_side(
            close=close,
            high_breakout=breakout_high,
            low_breakout=breakout_low,
            pullback_ema=pullback_ema,
            atr=atr,
            channel_width_atr=channel_width_atr,
            min_channel_width_atr=min_channel_width_atr,
            volatility_ratio=volatility_ratio,
            min_volatility_ratio=min_volatility_ratio,
            max_volatility_ratio=max_volatility_ratio,
            breakout_buffer_atr=breakout_buffer_atr,
            retest_tolerance_atr=retest_tolerance_atr,
            max_breakout_extension_atr=max_breakout_extension_atr,
            allow_short=allow_short,
        )
    else:
        base_side = _ema_cross_side(ema_fast=ema_fast, ema_slow=ema_slow, allow_short=allow_short)
    if base_side == 0:
        return 0

    if "regime" in normalized_variant:
        regime_side = _trend_regime(close=close, trend_ema=trend_ema)
        if regime_side != base_side:
            return 0

    if "adx" in normalized_variant and adx < adx_threshold:
        return 0

    if not _volume_confirms_side(
        side=base_side,
        volume_ratio=volume_ratio,
        min_volume_ratio=min_volume_ratio,
        cmf=cmf,
        min_cmf_abs=min_cmf_abs,
        body_atr_ratio=body_atr_ratio,
        min_body_atr_ratio=min_body_atr_ratio,
    ):
        return 0

    return base_side


def _ema_cross_side(ema_fast: float, ema_slow: float, allow_short: bool) -> int:
    if ema_fast > ema_slow:
        return 1
    if allow_short and ema_fast < ema_slow:
        return -1
    return 0


def _trend_regime(close: float, trend_ema: float) -> int:
    if close > trend_ema:
        return 1
    if close < trend_ema:
        return -1
    return 0


def _average_true_range(frame: pd.DataFrame, period: int) -> pd.Series:
    previous_close = frame["close"].shift(1)
    ranges = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - previous_close).abs(),
            (frame["low"] - previous_close).abs(),
        ],
        axis=1,
    )
    true_range = ranges.max(axis=1)
    return true_range.ewm(alpha=1 / period, adjust=False).mean()


def _average_directional_index(frame: pd.DataFrame, period: int) -> pd.Series:
    up_move = frame["high"].diff()
    down_move = -frame["low"].diff()

    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    atr = _average_true_range(frame, period).replace(0, pd.NA)
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr

    denominator = (plus_di + minus_di).replace(0, pd.NA)
    dx = (100 * (plus_di - minus_di).abs() / denominator).fillna(0.0)
    return dx.ewm(alpha=1 / period, adjust=False).mean()


def _quote_volume(frame: pd.DataFrame) -> pd.Series:
    if "volume_quote" in frame.columns:
        return frame["volume_quote"].astype(float)
    if "volume_ccy" in frame.columns:
        return frame["volume_ccy"].astype(float) * frame["close"].astype(float)
    return frame["volume"].astype(float) * frame["close"].astype(float)


def _chaikin_money_flow(frame: pd.DataFrame, period: int) -> pd.Series:
    denominator = (frame["high"] - frame["low"]).replace(0, pd.NA)
    multiplier = (((frame["close"] - frame["low"]) - (frame["high"] - frame["close"])) / denominator).fillna(0.0)
    money_flow_volume = multiplier * frame["quote_volume"]
    rolling_volume = frame["quote_volume"].rolling(period, min_periods=1).sum().replace(0, pd.NA)
    return (money_flow_volume.rolling(period, min_periods=1).sum() / rolling_volume).fillna(0.0)


def _volume_confirms_side(
    *,
    side: int,
    volume_ratio: float,
    min_volume_ratio: float,
    cmf: float,
    min_cmf_abs: float,
    body_atr_ratio: float,
    min_body_atr_ratio: float,
) -> bool:
    if pd.isna(volume_ratio) or volume_ratio < min_volume_ratio:
        return False
    if pd.isna(body_atr_ratio) or body_atr_ratio < min_body_atr_ratio:
        return False
    if side > 0:
        return not pd.isna(cmf) and cmf >= min_cmf_abs
    if side < 0:
        return not pd.isna(cmf) and cmf <= -min_cmf_abs
    return False


def _breakout_retest_side(
    *,
    close: float,
    high_breakout: float,
    low_breakout: float,
    pullback_ema: float,
    atr: float,
    channel_width_atr: float,
    min_channel_width_atr: float,
    volatility_ratio: float,
    min_volatility_ratio: float,
    max_volatility_ratio: float,
    breakout_buffer_atr: float,
    retest_tolerance_atr: float,
    max_breakout_extension_atr: float,
    allow_short: bool,
) -> int:
    _ = allow_short
    if (
        pd.isna(high_breakout)
        or pd.isna(low_breakout)
        or pd.isna(pullback_ema)
        or pd.isna(atr)
        or atr <= 0
        or pd.isna(channel_width_atr)
        or channel_width_atr < min_channel_width_atr
        or pd.isna(volatility_ratio)
        or volatility_ratio < min_volatility_ratio
        or volatility_ratio > max_volatility_ratio
    ):
        return 0

    long_breakout = _breakout_retest_long(
        close=close,
        breakout_level=high_breakout,
        pullback_ema=pullback_ema,
        atr=atr,
        breakout_buffer_atr=breakout_buffer_atr,
        retest_tolerance_atr=retest_tolerance_atr,
        max_breakout_extension_atr=max_breakout_extension_atr,
    )
    if long_breakout:
        return 1

    return 0


def _optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _breakout_retest_long(
    *,
    close: float,
    breakout_level: float,
    pullback_ema: float,
    atr: float,
    breakout_buffer_atr: float,
    retest_tolerance_atr: float,
    max_breakout_extension_atr: float,
) -> bool:
    breakout_distance = close - breakout_level
    if breakout_distance < breakout_buffer_atr * atr:
        return False
    if breakout_distance > max_breakout_extension_atr * atr:
        return False
    if close < pullback_ema:
        return False
    if pullback_ema < breakout_level - (retest_tolerance_atr * atr):
        return False
    return True


def _breakout_retest_short(
    *,
    close: float,
    breakout_level: float,
    pullback_ema: float,
    atr: float,
    breakout_buffer_atr: float,
    retest_tolerance_atr: float,
    max_breakout_extension_atr: float,
) -> bool:
    breakout_distance = breakout_level - close
    if breakout_distance < breakout_buffer_atr * atr:
        return False
    if breakout_distance > max_breakout_extension_atr * atr:
        return False
    if close > pullback_ema:
        return False
    if pullback_ema > breakout_level + (retest_tolerance_atr * atr):
        return False
    return True


def _scored_long_side(
    *,
    close: float,
    ema_fast: float,
    ema_slow: float,
    trend_ema: float,
    strategy_score: float,
    core_score: float,
    core_threshold: float,
    min_trend_score: float,
) -> int:
    if (
        pd.isna(close)
        or pd.isna(ema_fast)
        or pd.isna(ema_slow)
        or pd.isna(trend_ema)
        or close <= trend_ema
        or ema_fast <= ema_slow
        or pd.isna(strategy_score)
        or strategy_score < min_trend_score
        or pd.isna(core_score)
        or core_score < core_threshold
    ):
        return 0
    return 1


def _high_weight_long_side(
    *,
    close: float,
    ema_fast: float,
    ema_slow: float,
    trend_ema: float,
    high_weight_factor_score: float,
    high_weight_core_score: float,
    min_trend_score: float,
) -> int:
    if (
        pd.isna(close)
        or pd.isna(ema_fast)
        or pd.isna(ema_slow)
        or pd.isna(trend_ema)
        or close <= trend_ema
        or ema_fast <= ema_slow
        or pd.isna(high_weight_core_score)
        or high_weight_core_score < 12.0
        or pd.isna(high_weight_factor_score)
        or high_weight_factor_score < min_trend_score
    ):
        return 0
    return 1


def _factor_trend_long_side(
    *,
    close: float,
    ema_fast: float,
    ema_slow: float,
    trend_ema: float,
    adx: float,
    adx_threshold: float,
    volume_ratio: float,
    min_volume_ratio: float,
    cmf: float,
    min_cmf_abs: float,
    body_atr_ratio: float,
    min_body_atr_ratio: float,
    channel_width_atr: float,
    min_channel_width_atr: float,
    volatility_ratio: float,
    min_volatility_ratio: float,
    max_volatility_ratio: float,
    breakout_distance_atr: float,
    breakout_buffer_atr: float,
    max_breakout_extension_atr: float,
    ema_spread_atr: float,
    min_ema_spread_atr: float,
    trend_slope_atr: float,
    min_trend_slope_atr: float,
    long_factor_score: float,
    min_trend_score: float,
) -> int:
    if (
        pd.isna(close)
        or pd.isna(ema_fast)
        or pd.isna(ema_slow)
        or pd.isna(trend_ema)
        or close <= trend_ema
        or ema_fast <= ema_slow
        or pd.isna(adx)
        or adx < adx_threshold
        or pd.isna(ema_spread_atr)
        or ema_spread_atr < min_ema_spread_atr
        or pd.isna(trend_slope_atr)
        or trend_slope_atr < min_trend_slope_atr
        or pd.isna(breakout_distance_atr)
        or breakout_distance_atr < breakout_buffer_atr
        or breakout_distance_atr > max_breakout_extension_atr
        or pd.isna(channel_width_atr)
        or channel_width_atr < min_channel_width_atr
        or pd.isna(volatility_ratio)
        or volatility_ratio < min_volatility_ratio
        or volatility_ratio > max_volatility_ratio
    ):
        return 0

    if not _volume_confirms_side(
        side=1,
        volume_ratio=volume_ratio,
        min_volume_ratio=min_volume_ratio,
        cmf=cmf,
        min_cmf_abs=min_cmf_abs,
        body_atr_ratio=body_atr_ratio,
        min_body_atr_ratio=min_body_atr_ratio,
    ):
        return 0

    return 1 if not pd.isna(long_factor_score) and long_factor_score >= min_trend_score else 0


def _factor_trend_long_score(
    *,
    close: float,
    ema_fast: float,
    ema_slow: float,
    trend_ema: float,
    adx: float,
    adx_threshold: float,
    volume_ratio: float,
    min_volume_ratio: float,
    cmf: float,
    min_cmf_abs: float,
    body_atr_ratio: float,
    min_body_atr_ratio: float,
    breakout_distance_atr: float,
    breakout_buffer_atr: float,
    max_breakout_extension_atr: float,
    channel_width_atr: float,
    min_channel_width_atr: float,
    volatility_ratio: float,
    min_volatility_ratio: float,
    max_volatility_ratio: float,
    ema_spread_atr: float,
    min_ema_spread_atr: float,
    trend_slope_atr: float,
    min_trend_slope_atr: float,
    pullback_ema: float,
    breakout_high: float,
    atr: float,
    retest_tolerance_atr: float,
) -> float:
    score = 0.0

    if not pd.isna(close) and not pd.isna(trend_ema) and close > trend_ema:
        score += 6.0
    if not pd.isna(ema_fast) and not pd.isna(ema_slow) and ema_fast > ema_slow:
        score += 6.0
    if not pd.isna(ema_spread_atr) and ema_spread_atr >= min_ema_spread_atr:
        score += 5.0
    if not pd.isna(trend_slope_atr) and trend_slope_atr >= min_trend_slope_atr:
        score += 5.0
    if not pd.isna(adx) and adx >= adx_threshold:
        score += 5.0
    if not pd.isna(breakout_distance_atr) and breakout_buffer_atr <= breakout_distance_atr <= max_breakout_extension_atr:
        score += 5.0
    if not pd.isna(volume_ratio) and volume_ratio >= min_volume_ratio:
        score += 4.0
    if not pd.isna(cmf) and cmf >= min_cmf_abs:
        score += 4.0
    if not pd.isna(body_atr_ratio) and body_atr_ratio >= min_body_atr_ratio:
        score += 3.0
    if not pd.isna(channel_width_atr) and channel_width_atr >= min_channel_width_atr:
        score += 3.0
    if not pd.isna(volatility_ratio) and min_volatility_ratio <= volatility_ratio <= max_volatility_ratio:
        score += 2.0
    if (
        not pd.isna(pullback_ema)
        and not pd.isna(breakout_high)
        and not pd.isna(atr)
        and atr > 0
        and pullback_ema >= breakout_high - (retest_tolerance_atr * atr)
    ):
        score += 2.0

    return score


def _weighted_strategy_score(components: dict[str, tuple[float | None, float]]) -> float:
    score = 0.0
    for value, weight in components.values():
        if value is None or pd.isna(value):
            continue
        score += float(value) * float(weight)
    return round(score, 4)


def _attach_completed_daily_context(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.assign(
            daily_close=pd.Series(dtype=float),
            daily_ema_200=pd.Series(dtype=float),
            daily_ema_200_slope=pd.Series(dtype=float),
        )

    base = frame[["timestamp", "close"]].copy()
    base["timestamp"] = pd.to_datetime(base["timestamp"], utc=True).astype("datetime64[ns, UTC]")
    enriched = frame.copy()
    enriched["timestamp"] = pd.to_datetime(enriched["timestamp"], utc=True).astype("datetime64[ns, UTC]")
    daily = (
        base.set_index("timestamp")["close"]
        .astype(float)
        .resample("1D")
        .last()
        .dropna()
        .to_frame("daily_close")
    )
    if daily.empty:
        enriched = frame.copy()
        enriched["daily_close"] = pd.NA
        enriched["daily_ema_200"] = pd.NA
        enriched["daily_ema_200_slope"] = pd.NA
        return enriched

    daily["daily_ema_200"] = daily["daily_close"].ewm(span=200, adjust=False).mean()
    daily["daily_ema_200_slope"] = daily["daily_ema_200"] - daily["daily_ema_200"].shift(5)
    daily = daily.reset_index()
    daily["timestamp"] = daily["timestamp"] + pd.Timedelta(days=1)
    daily["timestamp"] = pd.to_datetime(daily["timestamp"], utc=True).astype("datetime64[ns, UTC]")

    return pd.merge_asof(
        enriched.sort_values("timestamp"),
        daily.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )


def _rolling_vwap(frame: pd.DataFrame, window: int) -> pd.Series:
    weighted_price = frame["close"].astype(float) * frame["quote_volume"].astype(float)
    rolling_notional = weighted_price.rolling(window, min_periods=max(2, window // 3)).sum()
    rolling_volume = frame["quote_volume"].astype(float).rolling(window, min_periods=max(2, window // 3)).sum()
    return rolling_notional / rolling_volume.replace(0, pd.NA)


def _basis_bps(frame: pd.DataFrame) -> pd.Series:
    if "mark_close" not in frame.columns or "index_close" not in frame.columns:
        return pd.Series(float("nan"), index=frame.index, dtype="float64")
    mark_close = pd.to_numeric(frame["mark_close"], errors="coerce")
    index_close = pd.to_numeric(frame["index_close"], errors="coerce")
    return (((mark_close / index_close.replace(0, pd.NA)) - 1.0) * 10_000).astype("float64")


def _high_weight_long_components(
    *,
    close: float,
    trend_ema: float,
    ema_fast: float,
    ema_slow: float,
    daily_close: float,
    daily_ema_200: float,
    daily_ema_200_slope: float,
    slow_ema_slope_atr: float,
    breakout_distance_atr: float,
    breakout_buffer_atr: float,
    max_breakout_extension_atr: float,
    volume_ratio: float,
    min_volume_ratio: float,
    basis_bps: float,
) -> dict[str, float]:
    weights = {
        "high_weight_daily_ema_score": 6.0,
        "high_weight_slow_ema_slope_score": 5.0,
        "high_weight_multi_tf_score": 6.0,
        "high_weight_breakout_score": 5.0,
        "high_weight_volume_score": 6.0,
        "high_weight_basis_score": 5.0,
    }
    scores = {
        "high_weight_daily_ema_score": _daily_ema_position_score(
            close=close,
            daily_close=daily_close,
            daily_ema_200=daily_ema_200,
            daily_ema_200_slope=daily_ema_200_slope,
        ),
        "high_weight_slow_ema_slope_score": _bounded_linear_score(
            value=slow_ema_slope_atr,
            lower=0.02,
            upper=0.18,
        ),
        "high_weight_multi_tf_score": _multi_timeframe_alignment_score(
            close=close,
            trend_ema=trend_ema,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
            daily_close=daily_close,
            daily_ema_200=daily_ema_200,
        ),
        "high_weight_breakout_score": _breakout_strength_score(
            breakout_distance_atr=breakout_distance_atr,
            breakout_buffer_atr=breakout_buffer_atr,
            max_breakout_extension_atr=max_breakout_extension_atr,
        ),
        "high_weight_volume_score": _bounded_linear_score(
            value=volume_ratio,
            lower=min_volume_ratio,
            upper=min_volume_ratio + 0.8,
        ),
        "high_weight_basis_score": _basis_regime_score(basis_bps),
    }

    available_weight = 0.0
    raw_score = 0.0
    for key, weight in weights.items():
        score = scores[key]
        if score is None:
            continue
        available_weight += weight
        raw_score += score * weight

    core_score = sum(
        (
            (scores["high_weight_daily_ema_score"] or 0.0) * weights["high_weight_daily_ema_score"],
            (scores["high_weight_multi_tf_score"] or 0.0) * weights["high_weight_multi_tf_score"],
            (scores["high_weight_breakout_score"] or 0.0) * weights["high_weight_breakout_score"],
        )
    )
    normalized_score = (raw_score / available_weight) * 100 if available_weight > 0 else 0.0

    return {
        **{
            key: round(0.0 if value is None else float(value), 4)
            for key, value in scores.items()
        },
        "high_weight_available_weight": round(available_weight, 4),
        "high_weight_raw_score": round(raw_score, 4),
        "high_weight_core_score": round(core_score, 4),
        "high_weight_factor_score": round(normalized_score, 4),
    }


def _daily_ema_position_score(
    *,
    close: float,
    daily_close: float,
    daily_ema_200: float,
    daily_ema_200_slope: float,
) -> float | None:
    if pd.isna(close) or pd.isna(daily_close) or pd.isna(daily_ema_200):
        return None
    if daily_close > daily_ema_200 and not pd.isna(daily_ema_200_slope) and daily_ema_200_slope > 0:
        return 1.0
    if daily_close > daily_ema_200:
        return 0.75
    if daily_ema_200 > 0 and abs((close / daily_ema_200) - 1.0) <= 0.01:
        return 0.5
    return 0.0


def _multi_timeframe_alignment_score(
    *,
    close: float,
    trend_ema: float,
    ema_fast: float,
    ema_slow: float,
    daily_close: float,
    daily_ema_200: float,
) -> float | None:
    values: list[float] = []
    if not pd.isna(close) and not pd.isna(trend_ema):
        values.append(1.0 if close > trend_ema else 0.0)
    if not pd.isna(ema_fast) and not pd.isna(ema_slow):
        values.append(1.0 if ema_fast > ema_slow else 0.0)
    if not pd.isna(daily_close) and not pd.isna(daily_ema_200):
        values.append(1.0 if daily_close > daily_ema_200 else 0.0)
    if not values:
        return None
    return sum(values) / len(values)


def _trend_alignment_score(
    *,
    close: float,
    trend_ema: float,
    ema_fast: float,
    ema_slow: float,
    ema_spread_atr: float,
    min_ema_spread_atr: float,
    daily_close: float,
    daily_ema_200: float,
) -> float | None:
    values: list[float] = []
    if not pd.isna(close) and not pd.isna(trend_ema):
        values.append(1.0 if close > trend_ema else 0.0)
    if not pd.isna(ema_fast) and not pd.isna(ema_slow):
        values.append(1.0 if ema_fast > ema_slow else 0.0)
    if not pd.isna(ema_spread_atr):
        values.append(_bounded_linear_score(value=ema_spread_atr, lower=min_ema_spread_atr * 0.5, upper=min_ema_spread_atr * 2.0))
    if not pd.isna(daily_close) and not pd.isna(daily_ema_200):
        values.append(1.0 if daily_close > daily_ema_200 else 0.0)
    valid = [item for item in values if item is not None and not pd.isna(item)]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _trend_strength_score(
    *,
    slow_ema_slope_atr: float,
    adx: float,
    adx_threshold: float,
) -> float | None:
    slope_score = _bounded_linear_score(value=slow_ema_slope_atr, lower=0.01, upper=0.15)
    adx_score = _bounded_linear_score(value=adx, lower=max(10.0, adx_threshold - 5.0), upper=adx_threshold + 15.0)
    valid = [item for item in (slope_score, adx_score) if item is not None and not pd.isna(item)]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _flow_quality_score(
    *,
    volume_ratio: float,
    min_volume_ratio: float,
    cmf: float,
    min_cmf_abs: float,
    body_atr_ratio: float,
    min_body_atr_ratio: float,
) -> float | None:
    volume_score = _bounded_linear_score(
        value=volume_ratio,
        lower=max(0.8, min_volume_ratio - 0.25),
        upper=min_volume_ratio + 0.6,
    )
    cmf_score = _bounded_linear_score(
        value=cmf,
        lower=max(0.0, min_cmf_abs * 0.5),
        upper=max(0.08, min_cmf_abs * 3.0),
    )
    body_score = _bounded_linear_score(
        value=body_atr_ratio,
        lower=max(0.05, min_body_atr_ratio * 0.5),
        upper=max(0.45, min_body_atr_ratio * 2.0),
    )
    valid = [item for item in (volume_score, cmf_score, body_score) if item is not None and not pd.isna(item)]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _pullback_quality_score(
    *,
    open_price: float,
    close: float,
    low: float,
    trend_ema: float,
    pullback_ema: float,
    rolling_vwap: float,
    atr: float,
) -> float | None:
    if any(pd.isna(item) for item in (open_price, close, low, trend_ema, pullback_ema, atr)) or atr <= 0:
        return None

    touch_price = min(value for value in (pullback_ema, rolling_vwap) if not pd.isna(value)) if not pd.isna(rolling_vwap) else pullback_ema
    touch = low <= touch_price + (0.25 * atr)
    rebound = close >= pullback_ema and close > open_price
    structure = low >= trend_ema - (0.6 * atr)
    extension_score = _bounded_linear_score(value=(close - pullback_ema) / atr, lower=0.0, upper=0.8)

    score = 0.0
    if touch:
        score += 0.4
    if rebound:
        score += 0.3
    if structure:
        score += 0.2
    if extension_score is not None:
        score += 0.1 * float(extension_score)
    return max(0.0, min(1.0, round(score, 4)))


def _breakout_quality_score(
    *,
    breakout_distance_atr: float,
    breakout_buffer_atr: float,
    max_breakout_extension_atr: float,
    channel_width_atr: float,
    min_channel_width_atr: float,
    volatility_ratio: float,
    min_volatility_ratio: float,
    max_volatility_ratio: float,
) -> float | None:
    breakout_score = _breakout_strength_score(
        breakout_distance_atr=breakout_distance_atr,
        breakout_buffer_atr=breakout_buffer_atr,
        max_breakout_extension_atr=max_breakout_extension_atr,
    )
    channel_score = _bounded_linear_score(
        value=channel_width_atr,
        lower=min_channel_width_atr,
        upper=min_channel_width_atr + 2.0,
    )
    if pd.isna(volatility_ratio):
        volatility_score = None
    elif volatility_ratio < min_volatility_ratio or volatility_ratio > max_volatility_ratio:
        volatility_score = 0.0
    else:
        midpoint = (min_volatility_ratio + max_volatility_ratio) / 2.0
        width = max(0.2, (max_volatility_ratio - min_volatility_ratio) / 2.0)
        volatility_score = max(0.0, min(1.0, 1.0 - abs(volatility_ratio - midpoint) / width))

    valid = [item for item in (breakout_score, channel_score, volatility_score) if item is not None and not pd.isna(item)]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _breakout_strength_score(
    *,
    breakout_distance_atr: float,
    breakout_buffer_atr: float,
    max_breakout_extension_atr: float,
) -> float | None:
    if pd.isna(breakout_distance_atr):
        return None
    if breakout_distance_atr < breakout_buffer_atr or breakout_distance_atr > max_breakout_extension_atr:
        return 0.0
    optimal = min(max_breakout_extension_atr, max(0.35, breakout_buffer_atr * 2.0))
    tolerance = max(0.25, (max_breakout_extension_atr - breakout_buffer_atr))
    score = 1.0 - (abs(breakout_distance_atr - optimal) / tolerance)
    return max(0.0, min(1.0, score))


def _basis_regime_score(basis_bps: float) -> float | None:
    if pd.isna(basis_bps):
        return None
    if -2.0 <= basis_bps <= 12.0:
        return 1.0
    if -6.0 <= basis_bps <= 20.0:
        return 0.75
    if -10.0 <= basis_bps <= 30.0:
        return 0.45
    return 0.0


def _bounded_linear_score(*, value: float, lower: float, upper: float) -> float | None:
    if pd.isna(value):
        return None
    if upper <= lower:
        return 1.0 if value >= lower else 0.0
    if value <= lower:
        return 0.0
    if value >= upper:
        return 1.0
    return (value - lower) / (upper - lower)


def _factor_score_risk_multiplier(score: float) -> float:
    if pd.isna(score):
        return 1.0
    if score >= 70.0:
        return 1.0
    if score >= 55.0:
        return 0.5
    return 0.0


def _daily_history_requirement_bars(signal_bar: str, *, ema_days: int) -> int:
    signal_delta = bar_to_timedelta(signal_bar)
    if signal_delta <= pd.Timedelta(0):
        return ema_days
    return int(pd.Timedelta(days=ema_days) / signal_delta)
