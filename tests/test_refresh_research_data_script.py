from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from quant_lab.data.okx_public_client import OkxApiError


def test_fetch_segment_does_not_retry_non_network_errors(monkeypatch) -> None:
    module = _load_script_module()
    calls = {"count": 0}

    class FakeClient:
        def fetch_history_candles(self, **kwargs):
            calls["count"] += 1
            raise ValueError("bad data")

    monkeypatch.setattr(module.time, "sleep", lambda *_args, **_kwargs: None)

    with pytest.raises(ValueError, match="bad data"):
        module.fetch_segment(
            FakeClient(),
            inst_id="BTC-USDT-SWAP",
            bar="1m",
            start_ts=pd.Timestamp("2025-01-01T00:00:00Z"),
            end_ts=pd.Timestamp("2025-01-01T00:01:00Z"),
        )

    assert calls["count"] == 1


def test_fetch_segment_retries_retryable_okx_errors(monkeypatch) -> None:
    module = _load_script_module()
    calls = {"count": 0}

    class FakeClient:
        def fetch_history_candles(self, **kwargs):
            calls["count"] += 1
            if calls["count"] < 3:
                raise OkxApiError("temporary")
            return pd.DataFrame({"timestamp": [pd.Timestamp("2025-01-01T00:00:00Z")]})

    monkeypatch.setattr(module.time, "sleep", lambda *_args, **_kwargs: None)

    frame = module.fetch_segment(
        FakeClient(),
        inst_id="BTC-USDT-SWAP",
        bar="1m",
        start_ts=pd.Timestamp("2025-01-01T00:00:00Z"),
        end_ts=pd.Timestamp("2025-01-01T00:01:00Z"),
    )

    assert calls["count"] == 3
    assert len(frame) == 1


def _load_script_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "refresh_research_data.py"
    spec = importlib.util.spec_from_file_location("refresh_research_data_script", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load script module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
