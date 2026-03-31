from __future__ import annotations

import json
from pathlib import Path

from quant_lab.config import AppConfig, DatabaseConfig, InstrumentConfig, StorageConfig, StrategyConfig
from quant_lab.service.demo_runtime import executor_state_path, load_executor_state_info, reset_executor_state


def test_reset_executor_state_masks_legacy_fallback(tmp_path: Path) -> None:
    config = _runtime_config(tmp_path)
    legacy_path = config.storage.data_dir / "demo_executor_state.json"
    legacy_path.write_text(
        json.dumps({"last_submitted_at": "2025-01-01T00:00:00+00:00"}, ensure_ascii=False),
        encoding="utf-8",
    )

    before = load_executor_state_info(config=config, project_root=None, mode="single")

    assert before["status"] == "ok"
    assert before["legacy_fallback_used"] is True
    assert before["payload"]["last_submitted_at"] == "2025-01-01T00:00:00+00:00"

    primary_path = executor_state_path(config=config, project_root=None, mode="single")
    reset_executor_state(path=primary_path)

    after = load_executor_state_info(config=config, project_root=None, mode="single")

    assert primary_path.exists()
    assert json.loads(primary_path.read_text(encoding="utf-8")) == {}
    assert after["status"] == "ok"
    assert after["legacy_fallback_used"] is False
    assert after["payload"] == {}


def _runtime_config(tmp_path: Path) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP", settle_currency="USDT"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
    )
