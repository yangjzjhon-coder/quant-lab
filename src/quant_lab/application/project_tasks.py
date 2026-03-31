from __future__ import annotations

from pathlib import Path
from typing import Any

from quant_lab.application.report_runtime import parse_float_list, parse_int_list, parse_text_list
from quant_lab.artifacts import (
    artifact_resolution_path,
    backtest_artifact_identity,
    sweep_artifact_identity,
    trend_research_artifact_identity,
    trend_research_artifact_resolution,
    trend_research_prefix,
)
from quant_lab.config import AppConfig, configured_symbols

SUPPORTED_PROJECT_TASKS = frozenset({"backtest", "report", "sweep", "research"})

DEFAULT_PROJECT_SWEEP_FAST = "10,20,30"
DEFAULT_PROJECT_SWEEP_SLOW = "50,80,120"
DEFAULT_PROJECT_SWEEP_ATR = "1.5,2.0,2.5"

DEFAULT_PROJECT_RESEARCH_VARIANTS = "breakout_retest,breakout_retest_regime,breakout_retest_adx,breakout_retest_regime_adx"
DEFAULT_PROJECT_RESEARCH_FAST = "8,12,16"
DEFAULT_PROJECT_RESEARCH_SLOW = "24,36,48,72"
DEFAULT_PROJECT_RESEARCH_ATR = "2.5,3.0,3.5"
DEFAULT_PROJECT_RESEARCH_TREND_EMA = "200"
DEFAULT_PROJECT_RESEARCH_ADX = "20,25"


def project_sweep_defaults() -> dict[str, Any]:
    return {
        "fast": DEFAULT_PROJECT_SWEEP_FAST,
        "slow": DEFAULT_PROJECT_SWEEP_SLOW,
        "atr": DEFAULT_PROJECT_SWEEP_ATR,
        "fast_values": parse_int_list(DEFAULT_PROJECT_SWEEP_FAST),
        "slow_values": parse_int_list(DEFAULT_PROJECT_SWEEP_SLOW),
        "atr_values": parse_float_list(DEFAULT_PROJECT_SWEEP_ATR),
    }


def project_research_defaults() -> dict[str, Any]:
    return {
        "variants": DEFAULT_PROJECT_RESEARCH_VARIANTS,
        "fast": DEFAULT_PROJECT_RESEARCH_FAST,
        "slow": DEFAULT_PROJECT_RESEARCH_SLOW,
        "atr": DEFAULT_PROJECT_RESEARCH_ATR,
        "trend_ema": DEFAULT_PROJECT_RESEARCH_TREND_EMA,
        "adx": DEFAULT_PROJECT_RESEARCH_ADX,
        "variant_values": parse_text_list(DEFAULT_PROJECT_RESEARCH_VARIANTS),
        "fast_values": parse_int_list(DEFAULT_PROJECT_RESEARCH_FAST),
        "slow_values": parse_int_list(DEFAULT_PROJECT_RESEARCH_SLOW),
        "atr_values": parse_float_list(DEFAULT_PROJECT_RESEARCH_ATR),
        "trend_ema_values": parse_int_list(DEFAULT_PROJECT_RESEARCH_TREND_EMA),
        "adx_values": parse_float_list(DEFAULT_PROJECT_RESEARCH_ADX),
    }


def default_project_research_report_prefix(config: AppConfig) -> str:
    return trend_research_prefix(config)


def default_project_research_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    defaults = project_research_defaults()
    return trend_research_artifact_resolution(
        config=config,
        project_root=project_root,
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


def resolve_project_research_results_path(
    *,
    config: AppConfig,
    project_root: Path,
    results_path: Path | None = None,
) -> Path:
    resolved_root = project_root.resolve()
    if results_path is not None:
        explicit_path = Path(results_path).expanduser()
        if not explicit_path.is_absolute():
            explicit_path = (resolved_root / explicit_path).resolve()
        return explicit_path

    identity, resolution = default_project_research_artifact_resolution(
        config=config,
        project_root=resolved_root,
    )
    report_dir = config.storage.resolved(resolved_root).report_dir
    return artifact_resolution_path(
        resolution,
        "research_csv",
        report_dir / f"{identity['logical_prefix']}.csv",
    )


def project_task_identity(*, config: AppConfig, project_root: Path, task: str) -> dict[str, Any]:
    normalized = str(task).strip().lower()
    resolved_symbols = configured_symbols(config)

    if normalized in {"backtest", "report"}:
        return backtest_artifact_identity(config=config, project_root=project_root, symbols=resolved_symbols)

    if normalized == "sweep":
        defaults = project_sweep_defaults()
        return sweep_artifact_identity(
            config=config,
            project_root=project_root,
            extra={
                "fast_values": defaults["fast_values"],
                "slow_values": defaults["slow_values"],
                "atr_values": defaults["atr_values"],
            },
        )

    if normalized == "research":
        defaults = project_research_defaults()
        return trend_research_artifact_identity(
            config=config,
            project_root=project_root,
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

    raise ValueError(f"Unsupported project task: {task}")
