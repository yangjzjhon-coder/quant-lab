from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_lab.application.report_runtime import write_backtest_artifacts
from quant_lab.artifacts import (
    artifact_resolution_path,
    backtest_artifact_resolution,
    canonical_artifact_path,
    candidate_backtest_artifact_identity,
    candidate_backtest_artifact_resolution,
    candidate_backtest_sleeve_artifact_resolution,
    read_artifact_manifest,
    register_artifact_group,
    sleeve_backtest_artifact_resolution,
    sweep_artifact_identity,
    sweep_artifact_resolution,
    update_artifact_manifest,
)
from quant_lab.config import AppConfig, InstrumentConfig, PortfolioConfig, StorageConfig, StrategyConfig


def test_backtest_artifact_resolution_prefers_manifest_paths(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    report_dir = config.storage.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    identity, _ = backtest_artifact_resolution(config=config, project_root=tmp_path)
    summary_path = canonical_artifact_path(
        report_dir,
        str(identity["logical_prefix"]),
        str(identity["artifact_fingerprint"]),
        "summary.json",
    )
    summary_path.write_text("{}", encoding="utf-8")
    update_artifact_manifest(
        report_dir=report_dir,
        identity=identity,
        artifacts={"summary": summary_path},
    )

    _, resolution = backtest_artifact_resolution(config=config, project_root=tmp_path)
    resolved = artifact_resolution_path(
        resolution,
        "summary",
        report_dir / f"{identity['logical_prefix']}_summary.json",
    )

    assert resolution["resolved_via"] == "manifest"
    assert resolved == summary_path.resolve()


def test_sleeve_backtest_artifact_resolution_falls_back_to_legacy_paths(tmp_path: Path) -> None:
    config = _build_config(tmp_path, portfolio=True)
    report_dir = config.storage.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    identity, resolution = sleeve_backtest_artifact_resolution(
        config=config,
        project_root=tmp_path,
        portfolio_symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
        symbol="ETH-USDT-SWAP",
    )
    legacy_summary_path = report_dir / f"{identity['logical_prefix']}_summary.json"

    resolved = artifact_resolution_path(resolution, "summary", legacy_summary_path)

    assert resolution["resolved_via"] == "legacy_fixed_name"
    assert resolved == legacy_summary_path


def test_sweep_artifact_resolution_ignores_manifest_with_other_fingerprint(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    report_dir = config.storage.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    stale_identity = sweep_artifact_identity(
        config=config,
        project_root=tmp_path,
        extra={"fast_values": [7], "slow_values": [21], "atr_values": [1.2]},
    )
    stale_dashboard_path = canonical_artifact_path(
        report_dir,
        str(stale_identity["logical_prefix"]),
        str(stale_identity["artifact_fingerprint"]),
        "sweep_dashboard.html",
    )
    stale_dashboard_path.write_text("<html>stale</html>", encoding="utf-8")
    update_artifact_manifest(
        report_dir=report_dir,
        identity=stale_identity,
        artifacts={"dashboard": stale_dashboard_path},
    )

    identity, resolution = sweep_artifact_resolution(config=config, project_root=tmp_path)
    legacy_dashboard_path = report_dir / f"{identity['logical_prefix']}_sweep_dashboard.html"
    resolved = artifact_resolution_path(resolution, "dashboard", legacy_dashboard_path)

    assert resolution["resolved_via"] == "legacy_fixed_name"
    assert resolved == legacy_dashboard_path


def test_candidate_backtest_artifact_resolution_uses_candidate_id_and_legacy_fallback(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    report_dir = config.storage.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    identity, resolution = candidate_backtest_artifact_resolution(
        config=config,
        project_root=tmp_path,
        candidate_id=7,
        candidate_name="BTC Regime V1",
        symbols=["BTC-USDT-SWAP"],
    )
    legacy_summary_path = report_dir / "candidate_btc_regime_v1_summary.json"

    resolved = artifact_resolution_path(resolution, "summary", legacy_summary_path)

    assert identity["logical_prefix"] == "candidate_7_btc_regime_v1"
    assert resolution["resolved_via"] == "legacy_fixed_name"
    assert resolved == legacy_summary_path


def test_candidate_sleeve_artifact_resolution_uses_candidate_id_and_legacy_fallback(tmp_path: Path) -> None:
    config = _build_config(tmp_path, portfolio=True)
    report_dir = config.storage.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    identity, resolution = candidate_backtest_sleeve_artifact_resolution(
        config=config,
        project_root=tmp_path,
        candidate_id=11,
        candidate_name="BTC ETH Regime V2",
        portfolio_symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
        symbol="ETH-USDT-SWAP",
    )
    legacy_summary_path = report_dir / "candidate_btc_eth_regime_v2_ETH-USDT-SWAP_sleeve_summary.json"

    resolved = artifact_resolution_path(resolution, "summary", legacy_summary_path)

    assert identity["logical_prefix"] == "candidate_11_btc_eth_regime_v2_ETH-USDT-SWAP_sleeve"
    assert resolution["resolved_via"] == "legacy_fixed_name"
    assert resolved == legacy_summary_path


def test_register_artifact_group_merges_multiple_legacy_alias_sets(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    report_dir = config.storage.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    identity, _ = backtest_artifact_resolution(config=config, project_root=tmp_path)
    summary_path = canonical_artifact_path(
        report_dir,
        str(identity["logical_prefix"]),
        str(identity["artifact_fingerprint"]),
        "summary.json",
    )
    summary_path.write_text("{}", encoding="utf-8")

    register_artifact_group(
        report_dir=report_dir,
        identity=identity,
        artifacts={"summary": summary_path},
        legacy_artifact_sets=[
            {"summary": report_dir / "btc_summary.json"},
            {"summary": report_dir / "btc_summary_alt.json"},
        ],
    )

    manifest = read_artifact_manifest(report_dir, str(identity["logical_prefix"]))

    assert manifest is not None
    assert manifest["aliases"]["btc_summary.json"] == str(summary_path)
    assert manifest["aliases"]["btc_summary_alt.json"] == str(summary_path)


def test_write_backtest_artifacts_registers_additional_candidate_legacy_prefix(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    report_dir = config.storage.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    identity = candidate_backtest_artifact_identity(
        config=config,
        project_root=tmp_path,
        candidate_id=7,
        candidate_name="BTC Regime V1",
        symbols=["BTC-USDT-SWAP"],
    )
    write_backtest_artifacts(
        storage=config.storage,
        report_prefix=str(identity["logical_prefix"]),
        trades_frame=pd.DataFrame(),
        equity_curve=pd.DataFrame(columns=["timestamp", "equity"]),
        summary={"net_return_pct": 1.23},
        artifact_identity=identity,
        additional_legacy_report_prefixes=["candidate_btc_regime_v1"],
    )

    _, resolution = candidate_backtest_artifact_resolution(
        config=config,
        project_root=tmp_path,
        candidate_id=7,
        candidate_name="BTC Regime V1",
        symbols=["BTC-USDT-SWAP"],
    )
    resolved = artifact_resolution_path(
        resolution,
        "summary",
        report_dir / "candidate_btc_regime_v1_summary.json",
    )
    expected = canonical_artifact_path(
        report_dir,
        str(identity["logical_prefix"]),
        str(identity["artifact_fingerprint"]),
        "summary.json",
    ).resolve()

    assert resolution["resolved_via"] == "manifest"
    assert resolved == expected


def test_write_backtest_artifacts_writes_visual_companion_files(tmp_path: Path) -> None:
    config = _build_config(tmp_path)
    report_dir = config.storage.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    identity, _ = backtest_artifact_resolution(config=config, project_root=tmp_path)
    write_backtest_artifacts(
        storage=config.storage,
        report_prefix=str(identity["logical_prefix"]),
        trades_frame=pd.DataFrame(),
        equity_curve=pd.DataFrame(columns=["timestamp", "equity"]),
        summary={"net_return_pct": 1.23},
        signal_frame=pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=2, freq="4h", tz="UTC"),
                "desired_side": [0, 1],
                "stop_distance": [0.0, 10.0],
            }
        ),
        execution_bars=pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=2, freq="1h", tz="UTC"),
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.0, 100.0],
                "close": [100.5, 101.5],
            }
        ),
        artifact_identity=identity,
    )

    signals_path = canonical_artifact_path(
        report_dir,
        str(identity["logical_prefix"]),
        str(identity["artifact_fingerprint"]),
        "signals.csv",
    )
    execution_bars_path = canonical_artifact_path(
        report_dir,
        str(identity["logical_prefix"]),
        str(identity["artifact_fingerprint"]),
        "execution_bars.csv",
    )

    assert signals_path.exists()
    assert execution_bars_path.exists()


def _build_config(tmp_path: Path, *, portfolio: bool = False) -> AppConfig:
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(
            data_dir=tmp_path / "data",
            raw_dir=tmp_path / "data" / "raw",
            report_dir=tmp_path / "data" / "reports",
        ),
        portfolio=PortfolioConfig(
            enabled=portfolio,
            symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"] if portfolio else [],
        ),
    )
