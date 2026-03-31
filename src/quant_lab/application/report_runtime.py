from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path
from typing import Any

import pandas as pd
import typer

from quant_lab.artifacts import canonical_artifact_paths, register_artifact_group
from quant_lab.config import AppConfig, InstrumentConfig, StorageConfig, ensure_storage_dirs, load_config
from quant_lab.models import TradeRecord
from quant_lab.providers.market_data import build_market_data_provider


def trades_frame(trades: list[TradeRecord]) -> pd.DataFrame:
    columns = [field.name for field in fields(TradeRecord)]
    if not trades:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame([trade.to_dict() for trade in trades], columns=columns)


def backtest_legacy_artifact_paths(
    *,
    storage: StorageConfig,
    report_prefix: str,
    include_dashboard: bool = False,
    include_allocation_overlay: bool = False,
) -> dict[str, Path]:
    paths = {
        "trades": storage.report_dir / f"{report_prefix}_trades.csv",
        "equity_curve": storage.report_dir / f"{report_prefix}_equity_curve.csv",
        "summary": storage.report_dir / f"{report_prefix}_summary.json",
    }
    if include_dashboard:
        paths["dashboard"] = storage.report_dir / f"{report_prefix}_dashboard.html"
    if include_allocation_overlay:
        paths["allocation_overlay"] = storage.report_dir / f"{report_prefix}_allocation_overlay.csv"
    return paths


def backtest_artifact_paths(
    *,
    storage: StorageConfig,
    artifact_identity: dict[str, Any],
    include_dashboard: bool = False,
    include_allocation_overlay: bool = False,
) -> dict[str, Path]:
    suffixes = {
        "trades": "trades.csv",
        "equity_curve": "equity_curve.csv",
        "summary": "summary.json",
    }
    if include_dashboard:
        suffixes["dashboard"] = "dashboard.html"
    if include_allocation_overlay:
        suffixes["allocation_overlay"] = "allocation_overlay.csv"
    return canonical_artifact_paths(
        report_dir=storage.report_dir,
        identity=artifact_identity,
        suffixes=suffixes,
    )


def backtest_visual_companion_paths(
    *,
    storage: StorageConfig,
    report_prefix: str,
    artifact_identity: dict[str, Any] | None = None,
) -> dict[str, Path]:
    if artifact_identity is None:
        return {
            "signals": storage.report_dir / f"{report_prefix}_signals.csv",
            "execution_bars": storage.report_dir / f"{report_prefix}_execution_bars.csv",
        }
    return canonical_artifact_paths(
        report_dir=storage.report_dir,
        identity=artifact_identity,
        suffixes={
            "signals": "signals.csv",
            "execution_bars": "execution_bars.csv",
        },
    )


def routed_artifact_paths(
    *,
    storage: StorageConfig,
    artifact_identity: dict[str, Any],
    include_dashboard: bool = False,
    include_allocation_overlay: bool = False,
    include_routes: bool = False,
) -> dict[str, Path]:
    suffixes = {
        "trades": "trades.csv",
        "equity_curve": "equity_curve.csv",
        "summary": "summary.json",
    }
    if include_dashboard:
        suffixes["dashboard"] = "dashboard.html"
    if include_allocation_overlay:
        suffixes["allocation_overlay"] = "allocation_overlay.csv"
    if include_routes:
        suffixes["routes"] = "routes.csv"
        suffixes["routing_summary"] = "routing_summary.json"
    return canonical_artifact_paths(
        report_dir=storage.report_dir,
        identity=artifact_identity,
        suffixes=suffixes,
    )


def routed_legacy_artifact_paths(
    *,
    storage: StorageConfig,
    report_prefix: str,
    include_dashboard: bool = False,
    include_allocation_overlay: bool = False,
    include_routes: bool = False,
) -> dict[str, Path]:
    paths = backtest_legacy_artifact_paths(
        storage=storage,
        report_prefix=report_prefix,
        include_dashboard=include_dashboard,
        include_allocation_overlay=include_allocation_overlay,
    )
    if include_routes:
        paths["routes"] = storage.report_dir / f"{report_prefix}_routes.csv"
        paths["routing_summary"] = storage.report_dir / f"{report_prefix}_routing_summary.json"
    return paths


def write_backtest_artifacts(
    *,
    storage: StorageConfig,
    report_prefix: str,
    trades_frame: pd.DataFrame,
    equity_curve: pd.DataFrame,
    summary: dict[str, object],
    allocation_overlay: pd.DataFrame | None = None,
    signal_frame: pd.DataFrame | None = None,
    execution_bars: pd.DataFrame | None = None,
    artifact_identity: dict[str, Any] | None = None,
    additional_legacy_report_prefixes: list[str] | None = None,
) -> tuple[Path, Path, Path]:
    include_allocation_overlay = allocation_overlay is not None
    legacy_paths = backtest_legacy_artifact_paths(
        storage=storage,
        report_prefix=report_prefix,
        include_allocation_overlay=include_allocation_overlay,
    )
    visual_paths = backtest_visual_companion_paths(
        storage=storage,
        report_prefix=report_prefix,
        artifact_identity=artifact_identity,
    )
    if artifact_identity is None:
        trades_path = legacy_paths["trades"]
        equity_path = legacy_paths["equity_curve"]
        summary_path = legacy_paths["summary"]
        trades_frame.to_csv(trades_path, index=False)
        equity_curve.to_csv(equity_path, index=False)
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        if allocation_overlay is not None:
            allocation_overlay.to_csv(legacy_paths["allocation_overlay"], index=False)
        if signal_frame is not None:
            signal_frame.to_csv(visual_paths["signals"], index=False)
        if execution_bars is not None:
            execution_bars.to_csv(visual_paths["execution_bars"], index=False)
        return trades_path, equity_path, summary_path

    canonical_paths = backtest_artifact_paths(
        storage=storage,
        artifact_identity=artifact_identity,
        include_allocation_overlay=include_allocation_overlay,
    )
    trades_path = canonical_paths["trades"]
    equity_path = canonical_paths["equity_curve"]
    summary_path = canonical_paths["summary"]
    trades_frame.to_csv(trades_path, index=False)
    equity_curve.to_csv(equity_path, index=False)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if allocation_overlay is not None:
        allocation_overlay.to_csv(canonical_paths["allocation_overlay"], index=False)
    if signal_frame is not None:
        signal_frame.to_csv(visual_paths["signals"], index=False)
    if execution_bars is not None:
        execution_bars.to_csv(visual_paths["execution_bars"], index=False)
    legacy_artifact_sets = [legacy_paths]
    for extra_prefix in additional_legacy_report_prefixes or []:
        normalized_prefix = str(extra_prefix).strip()
        if not normalized_prefix or normalized_prefix == report_prefix:
            continue
        legacy_artifact_sets.append(
            backtest_legacy_artifact_paths(
                storage=storage,
                report_prefix=normalized_prefix,
                include_allocation_overlay=include_allocation_overlay,
            )
        )
    register_artifact_group(
        report_dir=storage.report_dir,
        identity=artifact_identity,
        artifacts=canonical_paths,
        legacy_artifact_sets=legacy_artifact_sets,
    )
    return trades_path, equity_path, summary_path


def write_routing_artifacts(
    *,
    storage: StorageConfig,
    report_prefix: str,
    route_frame: pd.DataFrame,
    route_summary: dict[str, object],
    artifact_identity: dict[str, Any] | None = None,
    additional_legacy_report_prefixes: list[str] | None = None,
) -> tuple[Path, Path]:
    legacy_paths = routed_legacy_artifact_paths(
        storage=storage,
        report_prefix=report_prefix,
        include_routes=True,
    )
    if artifact_identity is None:
        route_path = legacy_paths["routes"]
        route_summary_path = legacy_paths["routing_summary"]
        route_frame.to_csv(route_path, index=False)
        route_summary_path.write_text(json.dumps(route_summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return route_path, route_summary_path

    canonical_paths = routed_artifact_paths(
        storage=storage,
        artifact_identity=artifact_identity,
        include_routes=True,
    )
    route_path = canonical_paths["routes"]
    route_summary_path = canonical_paths["routing_summary"]
    route_frame.to_csv(route_path, index=False)
    route_summary_path.write_text(json.dumps(route_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    legacy_artifact_sets = [legacy_paths]
    for extra_prefix in additional_legacy_report_prefixes or []:
        normalized_prefix = str(extra_prefix).strip()
        if not normalized_prefix or normalized_prefix == report_prefix:
            continue
        legacy_artifact_sets.append(
            routed_legacy_artifact_paths(
                storage=storage,
                report_prefix=normalized_prefix,
                include_routes=True,
            )
        )
    register_artifact_group(
        report_dir=storage.report_dir,
        identity=artifact_identity,
        artifacts={
            "routes": route_path,
            "routing_summary": route_summary_path,
        },
        legacy_artifact_sets=legacy_artifact_sets,
    )
    return route_path, route_summary_path


def load_report_inputs(
    config: Path,
    project_root: Path,
) -> tuple[AppConfig, StorageConfig, pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    cfg = load_config(config)
    storage = cfg.storage.resolved(project_root.resolve())
    ensure_storage_dirs(storage)
    symbol = cfg.instrument.symbol
    signal_bars, execution_bars, funding, symbol_slug_value = load_symbol_datasets(
        storage=storage,
        symbol=symbol,
        signal_bar=cfg.strategy.signal_bar,
        execution_bar=cfg.strategy.execution_bar,
        variant=cfg.strategy.variant,
    )
    return cfg, storage, signal_bars, execution_bars, funding, symbol_slug_value


def load_symbol_report_inputs(
    *,
    cfg: AppConfig,
    project_root: Path,
    symbol: str,
) -> tuple[AppConfig, StorageConfig, pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    storage = cfg.storage.resolved(project_root.resolve())
    ensure_storage_dirs(storage)
    signal_bars, execution_bars, funding, symbol_slug_value = load_symbol_datasets(
        storage=storage,
        symbol=symbol,
        signal_bar=cfg.strategy.signal_bar,
        execution_bar=cfg.strategy.execution_bar,
        variant=cfg.strategy.variant,
    )
    return cfg, storage, signal_bars, execution_bars, funding, symbol_slug_value


def load_symbol_routed_report_inputs(
    *,
    cfg: AppConfig,
    project_root: Path,
    symbol: str,
) -> tuple[AppConfig, StorageConfig, pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    cfg, storage, signal_bars, execution_bars, funding, symbol_slug_value = load_symbol_report_inputs(
        cfg=cfg,
        project_root=project_root,
        symbol=symbol,
    )
    signal_bars = enrich_signal_bars_for_high_weight_strategy(
        signal_bars=signal_bars,
        mark_price_bars=read_parquet_if_exists(
            storage.raw_dir / f"{symbol_slug_value}_mark_price_{cfg.strategy.signal_bar}.parquet"
        ),
        index_bars=read_parquet_if_exists(storage.raw_dir / f"{symbol_slug_value}_index_{cfg.strategy.signal_bar}.parquet"),
    )
    return cfg, storage, signal_bars, execution_bars, funding, symbol_slug_value


def load_symbol_datasets(
    *,
    storage: StorageConfig,
    symbol: str,
    signal_bar: str,
    execution_bar: str,
    variant: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    symbol_slug_value = symbol_slug(symbol)
    signal_path = storage.raw_dir / f"{symbol_slug_value}_{signal_bar}.parquet"
    execution_path = storage.raw_dir / f"{symbol_slug_value}_{execution_bar}.parquet"
    funding_path = storage.raw_dir / f"{symbol_slug_value}_funding.parquet"

    for path in (signal_path, execution_path, funding_path):
        if not path.exists():
            raise typer.BadParameter(f"Missing required dataset: {path}")

    signal_bars = pd.read_parquet(signal_path)
    execution_bars = pd.read_parquet(execution_path)
    funding = pd.read_parquet(funding_path)
    if (variant or "").strip().lower() in {
        "high_weight_long",
        "trend_regime_long",
        "trend_pullback_long",
        "trend_breakout_long",
    }:
        signal_bars = enrich_signal_bars_for_high_weight_strategy(
            signal_bars=signal_bars,
            mark_price_bars=read_parquet_if_exists(storage.raw_dir / f"{symbol_slug_value}_mark_price_{signal_bar}.parquet"),
            index_bars=read_parquet_if_exists(storage.raw_dir / f"{symbol_slug_value}_index_{signal_bar}.parquet"),
        )
    return signal_bars, execution_bars, funding, symbol_slug_value


def symbol_slug(symbol: str) -> str:
    return symbol.replace("/", "-")


def portfolio_report_prefix(symbols: list[str], strategy_name: str) -> str:
    base_assets = "_".join(symbol.split("-")[0].lower() for symbol in symbols)
    return f"portfolio_{base_assets}_{strategy_name}"


def parse_int_list(raw: str) -> list[int]:
    values = [int(chunk.strip()) for chunk in raw.split(",") if chunk.strip()]
    if not values:
        raise typer.BadParameter("Expected at least one integer value.")
    return values


def parse_float_list(raw: str) -> list[float]:
    values = [float(chunk.strip()) for chunk in raw.split(",") if chunk.strip()]
    if not values:
        raise typer.BadParameter("Expected at least one float value.")
    return values


def parse_text_list(raw: str) -> list[str]:
    values = [chunk.strip() for chunk in raw.split(",") if chunk.strip()]
    if not values:
        raise typer.BadParameter("Expected at least one text value.")
    return values


def instrument_metadata_path(storage: StorageConfig, symbol: str) -> Path:
    return storage.raw_dir / f"{symbol_slug(symbol)}_instrument.json"


def read_parquet_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def enrich_signal_bars_for_high_weight_strategy(
    *,
    signal_bars: pd.DataFrame,
    mark_price_bars: pd.DataFrame,
    index_bars: pd.DataFrame,
) -> pd.DataFrame:
    enriched = signal_bars.copy()
    enriched["timestamp"] = pd.to_datetime(enriched["timestamp"], utc=True)
    enriched = merge_reference_close(enriched, mark_price_bars, target_column="mark_close")
    enriched = merge_reference_close(enriched, index_bars, target_column="index_close")
    return enriched


def merge_reference_close(
    frame: pd.DataFrame,
    reference: pd.DataFrame,
    *,
    target_column: str,
) -> pd.DataFrame:
    if reference.empty or "timestamp" not in reference.columns or "close" not in reference.columns:
        return frame

    prepared = frame.sort_values("timestamp").copy()
    lookup = reference[["timestamp", "close"]].copy()
    lookup["timestamp"] = pd.to_datetime(lookup["timestamp"], utc=True)
    lookup["close"] = pd.to_numeric(lookup["close"], errors="coerce")
    lookup = lookup.dropna(subset=["timestamp"]).sort_values("timestamp").rename(columns={"close": target_column})
    return pd.merge_asof(prepared, lookup, on="timestamp", direction="backward")


def resolve_instrument_config(cfg: AppConfig, storage: StorageConfig, symbol: str) -> InstrumentConfig:
    if symbol == cfg.instrument.symbol:
        return cfg.instrument

    metadata_path = instrument_metadata_path(storage, symbol)
    if metadata_path.exists():
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        return InstrumentConfig.model_validate(payload)

    client = build_market_data_provider(cfg)
    try:
        instrument = client.fetch_instrument_details(
            inst_type=cfg.instrument.instrument_type,
            inst_id=symbol,
        )
    finally:
        client.close()

    metadata_path.write_text(json.dumps(instrument, ensure_ascii=False, indent=2), encoding="utf-8")
    return InstrumentConfig.model_validate(instrument)
