from __future__ import annotations

from pathlib import Path

from quant_lab.application.project_tasks import (
    DEFAULT_PROJECT_RESEARCH_ADX,
    DEFAULT_PROJECT_RESEARCH_ATR,
    DEFAULT_PROJECT_RESEARCH_FAST,
    DEFAULT_PROJECT_RESEARCH_SLOW,
    DEFAULT_PROJECT_RESEARCH_TREND_EMA,
    DEFAULT_PROJECT_RESEARCH_VARIANTS,
    DEFAULT_PROJECT_SWEEP_ATR,
    DEFAULT_PROJECT_SWEEP_FAST,
    DEFAULT_PROJECT_SWEEP_SLOW,
    default_project_research_artifact_resolution,
    default_project_research_report_prefix,
    project_research_defaults,
    project_sweep_defaults,
    project_task_identity,
    resolve_project_research_results_path,
)
from quant_lab.artifacts import canonical_artifact_path, sweep_artifact_identity, trend_research_artifact_identity, update_artifact_manifest
from quant_lab.cli import research_trend, sweep
from quant_lab.config import AppConfig, InstrumentConfig, StorageConfig, StrategyConfig
from quant_lab.service.project_ops import _project_task_identity_payload


def test_project_sweep_defaults_match_cli_option_defaults() -> None:
    defaults = project_sweep_defaults()

    assert defaults["fast"] == DEFAULT_PROJECT_SWEEP_FAST
    assert defaults["slow"] == DEFAULT_PROJECT_SWEEP_SLOW
    assert defaults["atr"] == DEFAULT_PROJECT_SWEEP_ATR
    assert defaults["fast_values"] == [10, 20, 30]
    assert defaults["slow_values"] == [50, 80, 120]
    assert defaults["atr_values"] == [1.5, 2.0, 2.5]
    assert sweep.__defaults__[2].default == DEFAULT_PROJECT_SWEEP_FAST
    assert sweep.__defaults__[3].default == DEFAULT_PROJECT_SWEEP_SLOW
    assert sweep.__defaults__[4].default == DEFAULT_PROJECT_SWEEP_ATR


def test_project_research_defaults_match_cli_option_defaults() -> None:
    defaults = project_research_defaults()

    assert defaults["variants"] == DEFAULT_PROJECT_RESEARCH_VARIANTS
    assert defaults["fast"] == DEFAULT_PROJECT_RESEARCH_FAST
    assert defaults["slow"] == DEFAULT_PROJECT_RESEARCH_SLOW
    assert defaults["atr"] == DEFAULT_PROJECT_RESEARCH_ATR
    assert defaults["trend_ema"] == DEFAULT_PROJECT_RESEARCH_TREND_EMA
    assert defaults["adx"] == DEFAULT_PROJECT_RESEARCH_ADX
    assert defaults["variant_values"] == [
        "breakout_retest",
        "breakout_retest_regime",
        "breakout_retest_adx",
        "breakout_retest_regime_adx",
    ]
    assert defaults["fast_values"] == [8, 12, 16]
    assert defaults["slow_values"] == [24, 36, 48, 72]
    assert defaults["atr_values"] == [2.5, 3.0, 3.5]
    assert defaults["trend_ema_values"] == [200]
    assert defaults["adx_values"] == [20.0, 25.0]
    assert research_trend.__defaults__[2].default == DEFAULT_PROJECT_RESEARCH_VARIANTS
    assert research_trend.__defaults__[3].default == DEFAULT_PROJECT_RESEARCH_FAST
    assert research_trend.__defaults__[4].default == DEFAULT_PROJECT_RESEARCH_SLOW
    assert research_trend.__defaults__[5].default == DEFAULT_PROJECT_RESEARCH_ATR
    assert research_trend.__defaults__[6].default == DEFAULT_PROJECT_RESEARCH_TREND_EMA
    assert research_trend.__defaults__[7].default == DEFAULT_PROJECT_RESEARCH_ADX


def test_project_task_identity_for_sweep_uses_shared_defaults(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    defaults = project_sweep_defaults()

    identity = project_task_identity(config=config, project_root=tmp_path, task="sweep")
    expected = sweep_artifact_identity(
        config=config,
        project_root=tmp_path,
        extra={
            "fast_values": defaults["fast_values"],
            "slow_values": defaults["slow_values"],
            "atr_values": defaults["atr_values"],
        },
    )

    assert identity["logical_prefix"] == expected["logical_prefix"]
    assert identity["artifact_fingerprint"] == expected["artifact_fingerprint"]


def test_project_task_identity_payload_for_research_uses_shared_defaults(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    defaults = project_research_defaults()

    payload = _project_task_identity_payload(config=config, project_root=tmp_path, task="research")
    expected = trend_research_artifact_identity(
        config=config,
        project_root=tmp_path,
        logical_prefix=default_project_research_report_prefix(config),
        extra={
            "variants": defaults["variant_values"],
            "fast_values": defaults["fast_values"],
            "slow_values": defaults["slow_values"],
            "atr_values": defaults["atr_values"],
            "trend_ema_values": defaults["trend_ema_values"],
            "adx_values": defaults["adx_values"],
        },
    )

    assert payload["task"] == "research"
    assert payload["logical_prefix"] == expected["logical_prefix"]
    assert payload["artifact_fingerprint"] == expected["artifact_fingerprint"]
    assert payload["symbols"] == ["BTC-USDT-SWAP"]
    assert payload["mode"] == "single"


def test_resolve_project_research_results_path_prefers_manifest_artifact(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    report_dir = config.storage.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    identity, _ = default_project_research_artifact_resolution(config=config, project_root=tmp_path)
    fingerprint = str(identity["artifact_fingerprint"])
    results_path = canonical_artifact_path(
        report_dir,
        str(identity["logical_prefix"]),
        fingerprint,
        "research.csv",
    )
    results_path.write_text("strategy_name\nema_trend_4h\n", encoding="utf-8")
    update_artifact_manifest(
        report_dir=report_dir,
        identity=identity,
        artifacts={"research_csv": results_path},
    )

    resolved = resolve_project_research_results_path(config=config, project_root=tmp_path)

    assert resolved == results_path.resolve()


def test_resolve_project_research_results_path_resolves_relative_explicit_path(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    relative_path = Path("data") / "reports" / "custom_research.csv"

    resolved = resolve_project_research_results_path(
        config=config,
        project_root=tmp_path,
        results_path=relative_path,
    )

    assert resolved == (tmp_path / relative_path).resolve()


def _build_config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="breakout_retest_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data" / "raw",
            report_dir=tmp_path / "data" / "reports",
        ),
    )
