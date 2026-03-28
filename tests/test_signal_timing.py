from __future__ import annotations

import pandas as pd

from quant_lab.backtest.engine import _build_signal_events


def test_build_signal_events_waits_for_signal_bar_close() -> None:
    signal_frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-01-01T00:00:00Z"]),
            "desired_side": [1],
            "stop_distance": [10.0],
        }
    )
    execution_bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2025-01-01T04:00:00Z",
                    "2025-01-01T04:01:00Z",
                    "2025-01-01T04:02:00Z",
                ]
            )
        }
    )

    events = _build_signal_events(
        signal_frame=signal_frame,
        execution_bars=execution_bars,
        signal_bar="4H",
        latency_minutes=1,
    )

    assert list(events) == [pd.Timestamp("2025-01-01T04:01:00Z")]


def test_build_signal_events_emits_managed_stop_updates() -> None:
    signal_frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2025-01-01T00:00:00Z",
                    "2025-01-01T04:00:00Z",
                    "2025-01-01T08:00:00Z",
                ]
            ),
            "desired_side": [0, 1, 1],
            "stop_distance": [10.0, 10.0, 10.0],
            "stop_price": [pd.NA, 100.0, 105.0],
        }
    )
    execution_bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2025-01-01T04:01:00Z",
                    "2025-01-01T08:01:00Z",
                    "2025-01-01T12:01:00Z",
                ]
            )
        }
    )

    events = _build_signal_events(
        signal_frame=signal_frame,
        execution_bars=execution_bars,
        signal_bar="4H",
        latency_minutes=1,
    )

    assert list(events) == [
        pd.Timestamp("2025-01-01T08:01:00Z"),
        pd.Timestamp("2025-01-01T12:01:00Z"),
    ]
    assert events[pd.Timestamp("2025-01-01T12:01:00Z")]["stop_price"] == 105.0
