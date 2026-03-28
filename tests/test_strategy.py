import pandas as pd

from quant_lab.config import StrategyConfig
from quant_lab.strategies.ema_trend import _desired_side_from_indicators, prepare_signal_frame


def test_prepare_signal_frame_generates_long_bias_in_uptrend() -> None:
    closes = list(range(100, 220))
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="4h", tz="UTC"),
            "open": [value - 0.8 for value in closes],
            "high": [value + 0.1 for value in closes],
            "low": [value - 1 for value in closes],
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": [500.0] * (len(closes) - 1) + [2_000.0],
        }
    )

    result = prepare_signal_frame(frame, StrategyConfig())
    assert result["desired_side"].iloc[-1] == 1
    assert result["stop_distance"].iloc[-1] > 0


def test_prepare_signal_frame_can_signal_short_when_enabled() -> None:
    closes = list(range(220, 100, -1))
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="4h", tz="UTC"),
            "open": [value + 0.8 for value in closes],
            "high": [value + 1 for value in closes],
            "low": [value - 0.1 for value in closes],
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": [500.0] * (len(closes) - 1) + [2_000.0],
        }
    )

    result = prepare_signal_frame(frame, StrategyConfig(allow_short=True))
    assert result["desired_side"].iloc[-1] == -1


def test_prepare_signal_frame_blocks_signal_when_volume_confirmation_fails() -> None:
    closes = list(range(100, 220))
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="4h", tz="UTC"),
            "open": [value - 0.1 for value in closes],
            "high": [value + 1 for value in closes],
            "low": [value - 1 for value in closes],
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": [1_000.0] * len(closes),
        }
    )

    result = prepare_signal_frame(frame, StrategyConfig())

    assert result["desired_side"].iloc[-1] == 0


def test_prepare_signal_frame_can_signal_breakout_retest_long() -> None:
    closes = [100 + (0.5 * idx) for idx in range(90)]
    closes[-1] = 146.0
    opens = [value - 0.4 for value in closes]
    highs = [value + 0.6 for value in closes]
    lows = [value - 0.8 for value in closes]
    lows[-1] = 143.1
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="1h", tz="UTC"),
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": [900.0] * (len(closes) - 1) + [3_000.0],
        }
    )

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="breakout_retest_regime",
            fast_ema=12,
            slow_ema=24,
            trend_ema=48,
            atr_period=14,
            volume_window=12,
            cmf_period=10,
            volatility_window=24,
            retest_tolerance_atr=2.0,
        ),
    )

    assert result["desired_side"].iloc[-1] == 1


def test_prepare_signal_frame_blocks_breakout_when_extension_is_too_large() -> None:
    closes = [100 + (0.5 * idx) for idx in range(90)]
    closes[-1] = 150.0
    opens = [value - 0.4 for value in closes]
    highs = [value + 0.6 for value in closes]
    lows = [value - 0.8 for value in closes]
    lows[-1] = 143.1
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="1h", tz="UTC"),
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": [900.0] * (len(closes) - 1) + [3_000.0],
        }
    )

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="breakout_retest_regime",
            fast_ema=12,
            slow_ema=24,
            trend_ema=48,
            atr_period=14,
            volume_window=12,
            cmf_period=10,
            volatility_window=24,
            max_breakout_extension_atr=0.8,
        ),
    )

    assert result["desired_side"].iloc[-1] == 0


def test_breakout_retest_holds_long_after_entry_signal_fades() -> None:
    closes = [100 + (0.5 * idx) for idx in range(96)]
    closes[-6] = 145.5
    closes[-5] = 146.2
    closes[-4] = 146.8
    closes[-3] = 147.1
    closes[-2] = 147.4
    closes[-1] = 147.6
    opens = [value - 0.4 for value in closes]
    highs = [value + 0.6 for value in closes]
    lows = [value - 0.8 for value in closes]
    volume_quote = [900.0] * len(closes)
    volume_quote[-6] = 3_000.0
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="1h", tz="UTC"),
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": volume_quote,
        }
    )

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="breakout_retest_regime",
            fast_ema=12,
            slow_ema=24,
            trend_ema=48,
            atr_period=14,
            volume_window=12,
            cmf_period=10,
            volatility_window=24,
            retest_tolerance_atr=2.0,
        ),
    )

    assert result["entry_side"].iloc[-1] == 0
    assert result["desired_side"].iloc[-1] == 1
    active_stops = result.loc[result["desired_side"] == 1, "stop_price"].dropna().astype(float)
    assert active_stops.is_monotonic_increasing


def test_breakout_retest_flattens_when_trend_breaks() -> None:
    closes = [100 + (0.5 * idx) for idx in range(90)]
    closes[-1] = 146.0
    closes.extend([146.6, 147.0, 130.0])
    opens = [value - 0.4 for value in closes]
    highs = [value + 0.6 for value in closes]
    lows = [value - 0.8 for value in closes]
    lows[89] = 143.1
    volume_quote = [900.0] * len(closes)
    volume_quote[89] = 3_000.0
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="1h", tz="UTC"),
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": volume_quote,
        }
    )

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="breakout_retest_regime",
            fast_ema=12,
            slow_ema=24,
            trend_ema=48,
            atr_period=14,
            volume_window=12,
            cmf_period=10,
            volatility_window=24,
            retest_tolerance_atr=2.0,
        ),
    )

    assert result["desired_side"].iloc[-2] == 1
    assert result["desired_side"].iloc[-1] == 0
    assert pd.isna(result["stop_price"].iloc[-1])


def test_prepare_signal_frame_can_signal_factor_trend_long() -> None:
    closes = [100 + (0.55 * idx) for idx in range(110)]
    closes[-1] = 164.0
    opens = [value - 0.5 for value in closes]
    highs = [value + 0.8 for value in closes]
    lows = [value - 0.9 for value in closes]
    lows[-1] = 160.9
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="4h", tz="UTC"),
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": [900.0] * (len(closes) - 1) + [3_500.0],
        }
    )

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="factor_trend_long",
            allow_short=False,
            fast_ema=12,
            slow_ema=36,
            trend_ema=80,
            atr_period=14,
            atr_stop_multiple=3.0,
            volume_window=12,
            cmf_period=10,
            volatility_window=36,
            min_volume_ratio=1.1,
            min_cmf_abs=0.03,
            min_body_atr_ratio=0.2,
            min_channel_width_atr=1.0,
            adx_threshold=18.0,
            min_trend_score=32.0,
            trend_slope_window=3,
            min_trend_slope_atr=0.04,
            min_ema_spread_atr=0.15,
            retest_tolerance_atr=2.0,
            max_breakout_extension_atr=2.2,
        ),
    )

    assert result["entry_side"].iloc[-1] == 1
    assert result["desired_side"].iloc[-1] == 1
    assert float(result["long_factor_score"].iloc[-1]) >= 32.0


def test_factor_trend_long_holds_after_entry_when_volume_cools() -> None:
    closes = [100 + (0.55 * idx) for idx in range(116)]
    closes[-7] = 162.8
    closes[-6] = 163.7
    closes[-5] = 164.5
    closes[-4] = 165.2
    closes[-3] = 165.8
    closes[-2] = 166.1
    closes[-1] = 166.4
    opens = [value - 0.5 for value in closes]
    highs = [value + 0.8 for value in closes]
    lows = [value - 0.9 for value in closes]
    lows[-7] = 160.6
    volume_quote = [900.0] * len(closes)
    volume_quote[-7] = 3_600.0
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="4h", tz="UTC"),
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": volume_quote,
        }
    )

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="factor_trend_long",
            allow_short=False,
            fast_ema=12,
            slow_ema=36,
            trend_ema=80,
            atr_period=14,
            atr_stop_multiple=3.0,
            volume_window=12,
            cmf_period=10,
            volatility_window=36,
            min_volume_ratio=1.1,
            min_cmf_abs=0.03,
            min_body_atr_ratio=0.2,
            min_channel_width_atr=1.0,
            adx_threshold=18.0,
            min_trend_score=32.0,
            trend_slope_window=3,
            min_trend_slope_atr=0.04,
            min_ema_spread_atr=0.15,
            retest_tolerance_atr=2.0,
            max_breakout_extension_atr=2.2,
        ),
    )

    assert result["entry_side"].iloc[-1] == 0
    assert result["desired_side"].iloc[-1] == 1
    active_stops = result.loc[result["desired_side"] == 1, "stop_price"].dropna().astype(float)
    assert active_stops.is_monotonic_increasing


def test_factor_trend_long_flattens_when_trend_breaks() -> None:
    closes = [100 + (0.55 * idx) for idx in range(112)]
    closes[-4] = 163.0
    closes[-3] = 164.2
    closes[-2] = 165.0
    closes[-1] = 138.0
    opens = [value - 0.5 for value in closes]
    highs = [value + 0.8 for value in closes]
    lows = [value - 0.9 for value in closes]
    lows[-4] = 160.6
    volume_quote = [900.0] * len(closes)
    volume_quote[-4] = 3_600.0
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="4h", tz="UTC"),
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": volume_quote,
        }
    )

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="factor_trend_long",
            allow_short=False,
            fast_ema=12,
            slow_ema=36,
            trend_ema=80,
            atr_period=14,
            atr_stop_multiple=3.0,
            volume_window=12,
            cmf_period=10,
            volatility_window=36,
            min_volume_ratio=1.1,
            min_cmf_abs=0.03,
            min_body_atr_ratio=0.2,
            min_channel_width_atr=1.0,
            adx_threshold=18.0,
            min_trend_score=32.0,
            trend_slope_window=3,
            min_trend_slope_atr=0.04,
            min_ema_spread_atr=0.15,
            retest_tolerance_atr=2.0,
            max_breakout_extension_atr=2.2,
        ),
    )

    assert result["desired_side"].iloc[-2] == 1
    assert result["desired_side"].iloc[-1] == 0
    assert pd.isna(result["stop_price"].iloc[-1])


def test_prepare_signal_frame_can_signal_high_weight_long() -> None:
    periods = 1400
    closes = [100 + (0.28 * idx) for idx in range(periods)]
    closes[-1] = closes[-2] + 1.2
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=periods, freq="4h", tz="UTC"),
            "open": [value - 0.5 for value in closes],
            "high": [value + 0.9 for value in closes],
            "low": [value - 1.0 for value in closes],
            "close": closes,
            "volume": [10.0] * periods,
            "volume_quote": [900.0] * (periods - 1) + [4_200.0],
            "mark_close": [value * 1.0008 for value in closes],
            "index_close": closes,
        }
    )

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="high_weight_long",
            allow_short=False,
            fast_ema=20,
            slow_ema=50,
            trend_ema=80,
            atr_period=14,
            atr_stop_multiple=3.0,
            volume_window=24,
            min_volume_ratio=1.15,
            breakout_buffer_atr=0.05,
            max_breakout_extension_atr=1.8,
            min_trend_score=55.0,
        ),
    )

    assert result["entry_side"].iloc[-1] == 1
    assert result["desired_side"].iloc[-1] == 1
    assert float(result["high_weight_core_score"].iloc[-1]) >= 12.0
    assert float(result["strategy_score"].iloc[-1]) >= 55.0
    assert float(result["strategy_risk_multiplier"].iloc[-1]) > 0.0


def test_high_weight_long_blocks_when_trend_structure_breaks() -> None:
    periods = 1400
    closes = [100 + (0.28 * idx) for idx in range(periods)]
    closes[-1] = closes[-2] - 45.0
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=periods, freq="4h", tz="UTC"),
            "open": [value - 0.5 for value in closes],
            "high": [value + 0.9 for value in closes],
            "low": [value - 1.0 for value in closes],
            "close": closes,
            "volume": [10.0] * periods,
            "volume_quote": [900.0] * (periods - 1) + [4_200.0],
            "mark_close": [value * 1.0008 for value in closes],
            "index_close": closes,
        }
    )

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="high_weight_long",
            allow_short=False,
            fast_ema=20,
            slow_ema=50,
            trend_ema=80,
            atr_period=14,
            atr_stop_multiple=3.0,
            volume_window=24,
            min_volume_ratio=1.15,
            breakout_buffer_atr=0.05,
            max_breakout_extension_atr=1.8,
            min_trend_score=55.0,
        ),
    )

    assert result["desired_side"].iloc[-1] == 0


def test_prepare_signal_frame_can_signal_trend_regime_long() -> None:
    periods = 1400
    closes = [100 + (0.32 * idx) for idx in range(periods)]
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=periods, freq="4h", tz="UTC"),
            "open": [value - 0.45 for value in closes],
            "high": [value + 0.85 for value in closes],
            "low": [value - 0.95 for value in closes],
            "close": closes,
            "volume": [10.0] * periods,
            "volume_quote": [1_200.0] * periods,
            "mark_close": [value * 1.0009 for value in closes],
            "index_close": closes,
        }
    )

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="trend_regime_long",
            allow_short=False,
            fast_ema=20,
            slow_ema=50,
            trend_ema=80,
            atr_period=14,
            volume_window=24,
            min_trend_score=60.0,
        ),
    )

    assert result["entry_side"].iloc[-1] == 1
    assert float(result["trend_regime_score"].iloc[-1]) >= 60.0


def test_prepare_signal_frame_can_signal_trend_pullback_long() -> None:
    periods = 1400
    closes = [100 + (0.30 * idx) for idx in range(periods)]
    closes[-3] = closes[-4] + 1.4
    closes[-2] = closes[-3] + 1.1
    closes[-1] = closes[-2] - 1.6
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=periods, freq="4h", tz="UTC"),
            "open": [value - 0.4 for value in closes],
            "high": [value + 0.9 for value in closes],
            "low": [value - 1.0 for value in closes],
            "close": closes,
            "volume": [10.0] * periods,
            "volume_quote": [1_000.0] * periods,
            "mark_close": [value * 1.0008 for value in closes],
            "index_close": closes,
        }
    )
    frame.loc[frame.index[-1], "open"] = closes[-1] - 0.6
    frame.loc[frame.index[-1], "low"] = closes[-1] - 1.2
    frame.loc[frame.index[-1], "high"] = closes[-1] + 1.0
    frame.loc[frame.index[-1], "volume_quote"] = 1_600.0

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="trend_pullback_long",
            allow_short=False,
            fast_ema=20,
            slow_ema=50,
            trend_ema=80,
            atr_period=14,
            volume_window=24,
            min_trend_score=58.0,
            min_volume_ratio=0.9,
            min_cmf_abs=0.01,
            min_body_atr_ratio=0.05,
        ),
    )

    assert result["entry_side"].iloc[-1] == 1
    assert float(result["trend_pullback_score"].iloc[-1]) >= 58.0


def test_prepare_signal_frame_can_signal_trend_breakout_long() -> None:
    periods = 1400
    closes = [100 + (0.22 * idx) for idx in range(periods)]
    closes[-1] = closes[-2] + 3.0
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=periods, freq="4h", tz="UTC"),
            "open": [value - 0.4 for value in closes],
            "high": [value + 0.8 for value in closes],
            "low": [value - 0.9 for value in closes],
            "close": closes,
            "volume": [10.0] * periods,
            "volume_quote": [1_000.0] * (periods - 1) + [2_500.0],
            "mark_close": [value * 1.0009 for value in closes],
            "index_close": closes,
        }
    )
    frame.loc[frame.index[-1], "low"] = closes[-2] + 0.8

    result = prepare_signal_frame(
        frame,
        StrategyConfig(
            variant="trend_breakout_long",
            allow_short=False,
            fast_ema=20,
            slow_ema=50,
            trend_ema=80,
            atr_period=14,
            volume_window=24,
            min_trend_score=60.0,
            min_volume_ratio=0.95,
            min_cmf_abs=0.01,
            min_body_atr_ratio=0.05,
            max_breakout_extension_atr=2.5,
        ),
    )

    assert result["entry_side"].iloc[-1] == 1
    assert float(result["trend_breakout_score"].iloc[-1]) >= 60.0


def test_regime_variant_flattens_counter_trend_signal() -> None:
    side = _desired_side_from_indicators(
        variant="ema_cross_regime",
        ema_fast=110,
        ema_slow=100,
        close=90,
        trend_ema=120,
        adx=35,
        adx_threshold=20,
        allow_short=True,
        volume_ratio=2.0,
        min_volume_ratio=1.15,
        cmf=0.2,
        min_cmf_abs=0.05,
        body_atr_ratio=0.5,
        min_body_atr_ratio=0.2,
    )
    assert side == 0


def test_adx_variant_requires_trend_strength() -> None:
    side = _desired_side_from_indicators(
        variant="ema_cross_adx",
        ema_fast=110,
        ema_slow=100,
        close=130,
        trend_ema=120,
        adx=15,
        adx_threshold=20,
        allow_short=True,
        volume_ratio=2.0,
        min_volume_ratio=1.15,
        cmf=0.2,
        min_cmf_abs=0.05,
        body_atr_ratio=0.5,
        min_body_atr_ratio=0.2,
    )
    assert side == 0
