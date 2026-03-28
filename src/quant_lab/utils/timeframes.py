from __future__ import annotations

import pandas as pd


def bar_to_timedelta(bar: str) -> pd.Timedelta:
    normalized = bar.strip()
    if normalized.endswith("m"):
        return pd.to_timedelta(int(normalized[:-1]), unit="m")
    if normalized.endswith("H"):
        return pd.to_timedelta(int(normalized[:-1]), unit="h")
    if normalized.endswith("D"):
        return pd.to_timedelta(int(normalized[:-1]), unit="d")
    if normalized.endswith("W"):
        return pd.to_timedelta(int(normalized[:-1]) * 7, unit="d")
    raise ValueError(f"Unsupported bar interval: {bar}")
