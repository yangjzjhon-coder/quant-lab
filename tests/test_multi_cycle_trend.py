from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_lab.strategies.multi_cycle_trend import (
    MultiCycleTrendParameters,
    generate_trading_signals,
    resample_intraday_to_daily,
    run_multi_cycle_backtest_on_4h,
)


def test_generate_trading_signals_emits_long_setup_and_leverage() -> None:
    frame = _daily_frame(
        closes=[100, 101, 102, 103, 104, 105, 106, 108],
        lows=[99, 100, 101, 102, 101, 102, 103, 104],
        highs=[101, 102, 103, 104, 105, 106, 107, 109],
    )
    params = MultiCycleTrendParameters(
        synthetic_window=4,
        touch_ema_span=2,
        support_ema_span=3,
        trend_fast_ema_spans=(3, 4),
        trend_slow_ema_spans=(5, 6),
        monthly_ema_span=12,
        require_fresh_setup=False,
    )

    signals = generate_trading_signals(frame, params=params)

    last = signals.iloc[-1]
    assert last["trend_state"] == "bull_trend"
    assert bool(last["synthetic_setup"]) is True
    assert bool(last["open_long_signal"]) is True
    assert round(float(last["stop_loss_price"]), 2) == 100.99
    assert float(last["signal_leverage"]) > 0


def test_generate_trading_signals_default_mode_only_emits_fresh_setup_entries() -> None:
    frame = _daily_frame(
        closes=[100, 101, 102, 103, 104, 105, 106, 108],
        lows=[99, 100, 101, 102, 101, 102, 103, 104],
        highs=[101, 102, 103, 104, 105, 106, 107, 109],
    )
    params = MultiCycleTrendParameters(
        synthetic_window=4,
        touch_ema_span=2,
        support_ema_span=3,
        trend_fast_ema_spans=(3, 4),
        trend_slow_ema_spans=(5, 6),
        monthly_ema_span=12,
    )

    signals = generate_trading_signals(frame, params=params)

    assert int(signals["open_long_signal"].fillna(False).sum()) == 0
    assert int(signals["fresh_setup_signal"].fillna(False).sum()) >= 1


def test_generate_trading_signals_handles_nan_window_without_entry() -> None:
    frame = _daily_frame(
        closes=[100, 101, 102, 103, 104, 105, 106, 108],
        lows=[99, 100, 101, 102, None, 102, 103, 104],
        highs=[101, 102, 103, 104, 105, 106, 107, 109],
    )
    params = MultiCycleTrendParameters(
        synthetic_window=4,
        touch_ema_span=2,
        support_ema_span=3,
        trend_fast_ema_spans=(3, 4),
        trend_slow_ema_spans=(5, 6),
        monthly_ema_span=12,
    )

    signals = generate_trading_signals(frame, params=params)

    assert bool(signals.iloc[4]["open_long_signal"]) is False
    assert bool(signals.iloc[5]["open_long_signal"]) is False


def test_resample_intraday_to_daily_rolls_up_expected_fields() -> None:
    intraday = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T12:00:00Z",
                    "2026-01-02T00:00:00Z",
                    "2026-01-02T12:00:00Z",
                ],
                utc=True,
            ),
            "open": [100.0, 101.0, 105.0, 106.0],
            "high": [102.0, 103.0, 107.0, 108.0],
            "low": [99.0, 100.0, 104.0, 105.0],
            "close": [101.0, 102.0, 106.0, 107.0],
            "volume": [1.0, 2.0, 3.0, 4.0],
            "volume_ccy": [10.0, 20.0, 30.0, 40.0],
            "volume_quote": [100.0, 200.0, 300.0, 400.0],
        }
    )

    daily = resample_intraday_to_daily(intraday)

    assert len(daily) == 2
    assert daily.iloc[0]["open"] == 100.0
    assert daily.iloc[0]["high"] == 103.0
    assert daily.iloc[0]["low"] == 99.0
    assert daily.iloc[0]["close"] == 102.0
    assert daily.iloc[0]["volume"] == 3.0


def test_run_multi_cycle_backtest_on_4h_enters_on_next_4h_open_after_daily_signal() -> None:
    params = _test_params()
    execution = _intraday_4h_frame(
        [
            {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0},
            {"open": 100.0, "high": 102.0, "low": 100.0, "close": 101.0},
            {"open": 101.0, "high": 103.0, "low": 101.0, "close": 102.0},
            {"open": 102.0, "high": 104.0, "low": 102.0, "close": 103.0},
            {"open": 103.0, "high": 105.0, "low": 101.0, "close": 104.0},
            {"open": 104.0, "high": 106.0, "low": 102.0, "close": 105.0},
            {"open": 105.0, "high": 107.0, "low": 103.0, "close": 106.0},
            {"open": 106.0, "high": 109.0, "low": 104.0, "close": 108.0},
            {
                "open": 110.0,
                "high": 110.0,
                "low": 101.1,
                "close": 101.2,
                "intraday_closes": [110.0, 106.0, 104.0, 103.0, 102.0, 101.2],
            },
            {
                "open": 101.0,
                "high": 102.0,
                "low": 101.0,
                "close": 101.1,
                "intraday_closes": [101.0, 101.0, 101.0, 101.0, 101.0, 101.1],
            },
        ]
    )

    results = run_multi_cycle_backtest_on_4h(execution, params=params)

    first_signal = results["signals"].loc[results["signals"]["open_long_signal"]].iloc[0]
    first_trade = results["trades"].iloc[0]
    expected_entry_time = pd.Timestamp(first_signal["timestamp"]) + pd.Timedelta(days=1)

    assert pd.Timestamp(first_trade["entry_time"]) == expected_entry_time
    assert float(first_trade["entry_price"]) == 105.0


def test_run_multi_cycle_backtest_on_4h_stops_out_on_bar_close() -> None:
    params = _test_params()
    execution = _intraday_4h_frame(
        [
            {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0},
            {"open": 100.0, "high": 102.0, "low": 100.0, "close": 101.0},
            {"open": 101.0, "high": 103.0, "low": 101.0, "close": 102.0},
            {"open": 102.0, "high": 104.0, "low": 102.0, "close": 103.0},
            {"open": 103.0, "high": 105.0, "low": 101.0, "close": 104.0},
            {"open": 104.0, "high": 106.0, "low": 102.0, "close": 105.0},
            {
                "open": 106.0,
                "high": 106.0,
                "low": 99.5,
                "close": 100.0,
                "intraday_closes": [106.0, 100.0, 100.0, 100.0, 100.0, 100.0],
            },
        ]
    )

    results = run_multi_cycle_backtest_on_4h(execution, params=params)

    first_trade = results["trades"].iloc[0]

    assert first_trade["exit_reason"] == "stop_loss_close_4h"
    assert pd.Timestamp(first_trade["entry_time"]) == pd.Timestamp("2026-01-07T00:00:00Z")
    assert pd.Timestamp(first_trade["exit_time"]) == pd.Timestamp("2026-01-07T04:00:00Z")
    assert float(first_trade["exit_price"]) == 100.0


def test_run_multi_cycle_backtest_on_4h_takes_profit_on_next_4h_open_after_daily_cross() -> None:
    params = _test_params()
    execution = _intraday_4h_frame(
        [
            {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0},
            {"open": 100.0, "high": 102.0, "low": 100.0, "close": 101.0},
            {"open": 101.0, "high": 103.0, "low": 101.0, "close": 102.0},
            {"open": 102.0, "high": 104.0, "low": 102.0, "close": 103.0},
            {"open": 103.0, "high": 105.0, "low": 101.0, "close": 104.0},
            {"open": 104.0, "high": 106.0, "low": 102.0, "close": 105.0},
            {"open": 105.0, "high": 107.0, "low": 103.0, "close": 106.0},
            {"open": 106.0, "high": 109.0, "low": 104.0, "close": 108.0},
            {
                "open": 110.0,
                "high": 110.0,
                "low": 101.1,
                "close": 101.2,
                "intraday_closes": [110.0, 106.0, 104.0, 103.0, 102.0, 101.2],
            },
            {
                "open": 101.0,
                "high": 102.0,
                "low": 101.0,
                "close": 101.1,
                "intraday_closes": [101.0, 101.0, 101.0, 101.0, 101.0, 101.1],
            },
        ]
    )

    results = run_multi_cycle_backtest_on_4h(execution, params=params)

    first_trade = results["trades"].iloc[0]
    take_profit_signal = results["signals"].loc[
        (results["signals"]["take_profit_signal"])
        & (pd.to_datetime(results["signals"]["timestamp"], utc=True) > pd.Timestamp(first_trade["signal_time"]))
    ].iloc[0]
    expected_exit_time = pd.Timestamp(take_profit_signal["timestamp"]) + pd.Timedelta(days=1)

    assert first_trade["exit_reason"] == "take_profit_cross"
    assert pd.Timestamp(first_trade["exit_time"]) == expected_exit_time
    assert float(first_trade["exit_price"]) == 101.0


def _daily_frame(*, closes: list[float], lows: list[float | None], highs: list[float]) -> pd.DataFrame:
    timestamps = pd.date_range("2026-01-01", periods=len(closes), freq="D", tz="UTC")
    opens = [closes[0], *closes[:-1]]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
        }
    )


def _intraday_4h_frame(daily_specs: list[dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, float | pd.Timestamp]] = []
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    for day_offset, spec in enumerate(daily_specs):
        day_start = start + pd.Timedelta(days=day_offset)
        intraday_closes = spec.get("intraday_closes")
        if intraday_closes is None:
            intraday_closes = [float(spec["open"])] * 5 + [float(spec["close"])]
        if len(intraday_closes) != 6:
            raise ValueError("each daily spec must expand to exactly six 4H closes")
        for index, close_value in enumerate(intraday_closes):
            open_value = float(spec["open"]) if index == 0 else float(intraday_closes[index - 1])
            rows.append(
                {
                    "timestamp": day_start + pd.Timedelta(hours=4 * index),
                    "open": open_value,
                    "high": max(open_value, float(close_value), float(spec["high"])),
                    "low": min(open_value, float(close_value), float(spec["low"])),
                    "close": float(close_value),
                }
            )
    return pd.DataFrame(rows)


def _test_params() -> MultiCycleTrendParameters:
    return MultiCycleTrendParameters(
        synthetic_window=4,
        touch_ema_span=2,
        support_ema_span=3,
        trend_fast_ema_spans=(3, 4),
        trend_slow_ema_spans=(5, 6),
        monthly_ema_span=12,
        require_fresh_setup=False,
    )
