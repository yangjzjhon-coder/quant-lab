from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from quant_lab.config import AppConfig, configured_symbols
from quant_lab.utils.files import atomic_write_json

MANIFEST_SUFFIX = "_latest.json"
MANIFEST_RESOLVED_VIA = "manifest"
LEGACY_RESOLVED_VIA = "legacy_fixed_name"


def symbol_slug(symbol: str) -> str:
    return str(symbol).replace("/", "-")


def portfolio_report_prefix(symbols: list[str], strategy_name: str) -> str:
    base_assets = "_".join(str(symbol).split("-")[0].lower() for symbol in symbols)
    return f"portfolio_{base_assets}_{strategy_name}"


def primary_report_prefix(config: AppConfig, symbols: list[str] | None = None) -> str:
    resolved_symbols = list(symbols or configured_symbols(config))
    if len(resolved_symbols) == 1:
        return f"{symbol_slug(resolved_symbols[0])}_{config.strategy.name}"
    return portfolio_report_prefix(resolved_symbols, config.strategy.name)


def sweep_prefix(config: AppConfig) -> str:
    return f"{symbol_slug(config.instrument.symbol)}_{config.strategy.name}"


def trend_research_prefix(config: AppConfig) -> str:
    return f"{symbol_slug(config.instrument.symbol)}_{config.strategy.name}_trend_research"


def sleeve_report_prefix(symbol: str, strategy_name: str) -> str:
    return f"{symbol_slug(symbol)}_{strategy_name}_sleeve"


def routed_report_prefix(config: AppConfig, symbols: list[str] | None = None) -> str:
    return f"{primary_report_prefix(config, symbols)}_routed"


def routed_sleeve_report_prefix(symbol: str, strategy_name: str) -> str:
    return f"{symbol_slug(symbol)}_{strategy_name}_routed_sleeve"


def candidate_report_prefix(candidate_name: str, candidate_id: int | None = None) -> str:
    slug = _artifact_token(candidate_name) or "candidate"
    if candidate_id is None:
        return f"candidate_{slug}"
    return f"candidate_{candidate_id}_{slug}"


def candidate_sleeve_report_prefix(candidate_name: str, symbol: str, candidate_id: int | None = None) -> str:
    return f"{candidate_report_prefix(candidate_name, candidate_id)}_{symbol_slug(symbol)}_sleeve"


def manifest_path(report_dir: Path, logical_prefix: str) -> Path:
    return report_dir / f"{logical_prefix}{MANIFEST_SUFFIX}"


def canonical_artifact_path(report_dir: Path, logical_prefix: str, artifact_fingerprint: str, suffix: str) -> Path:
    return report_dir / f"{logical_prefix}__{artifact_fingerprint}_{suffix}"


def canonical_artifact_paths(
    *,
    report_dir: Path,
    identity: dict[str, Any],
    suffixes: dict[str, str],
) -> dict[str, Path]:
    logical_prefix = str(identity["logical_prefix"])
    artifact_fingerprint = str(identity["artifact_fingerprint"])
    return {
        key: canonical_artifact_path(report_dir, logical_prefix, artifact_fingerprint, suffix)
        for key, suffix in suffixes.items()
    }


def build_artifact_identity(
    *,
    config: AppConfig,
    project_root: Path,
    artifact_kind: str,
    logical_prefix: str,
    symbols: list[str] | None = None,
    mode: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_symbols = list(symbols or configured_symbols(config))
    resolved_mode = mode or ("portfolio" if len(resolved_symbols) > 1 else "single")
    storage = config.storage.resolved(project_root.resolve())
    fingerprint_payload = {
        "artifact_kind": artifact_kind,
        "mode": resolved_mode,
        "symbols": resolved_symbols,
        "strategy": config.strategy.model_dump(mode="json"),
        "execution": config.execution.model_dump(mode="json"),
        "risk": config.risk.model_dump(mode="json"),
        "instrument_payloads": _local_instrument_payloads(config=config, storage=storage, symbols=resolved_symbols),
        "extra": extra or {},
    }
    serialized = json.dumps(fingerprint_payload, ensure_ascii=False, sort_keys=True)
    return {
        "artifact_kind": artifact_kind,
        "logical_prefix": logical_prefix,
        "artifact_fingerprint": hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:12],
        "mode": resolved_mode,
        "symbols": resolved_symbols,
        "fingerprint_payload": fingerprint_payload,
    }


def backtest_artifact_identity(
    *,
    config: AppConfig,
    project_root: Path,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    resolved_symbols = list(symbols or configured_symbols(config))
    return build_artifact_identity(
        config=config,
        project_root=project_root,
        artifact_kind="backtest",
        logical_prefix=primary_report_prefix(config, resolved_symbols),
        symbols=resolved_symbols,
        mode="portfolio" if len(resolved_symbols) > 1 else "single",
    )


def backtest_sleeve_artifact_identity(
    *,
    config: AppConfig,
    project_root: Path,
    portfolio_symbols: list[str],
    symbol: str,
) -> dict[str, Any]:
    return build_artifact_identity(
        config=config,
        project_root=project_root,
        artifact_kind="backtest_sleeve",
        logical_prefix=sleeve_report_prefix(symbol, config.strategy.name),
        symbols=portfolio_symbols,
        mode="portfolio",
        extra={"scope": "sleeve", "symbol": symbol},
    )


def sweep_artifact_identity(*, config: AppConfig, project_root: Path, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    return build_artifact_identity(
        config=config,
        project_root=project_root,
        artifact_kind="sweep",
        logical_prefix=sweep_prefix(config),
        symbols=[config.instrument.symbol],
        mode="single",
        extra=extra,
    )


def routed_backtest_artifact_identity(
    *,
    config: AppConfig,
    project_root: Path,
    symbols: list[str] | None = None,
    required_scope: str = "demo",
) -> dict[str, Any]:
    resolved_symbols = list(symbols or configured_symbols(config))
    return build_artifact_identity(
        config=config,
        project_root=project_root,
        artifact_kind="routed_backtest",
        logical_prefix=routed_report_prefix(config, resolved_symbols),
        symbols=resolved_symbols,
        mode="portfolio" if len(resolved_symbols) > 1 else "single",
        extra={
            "required_scope": required_scope,
            "strategy_router_enabled": bool(config.trading.strategy_router_enabled),
            "strategy_router_fallback_to_config": bool(config.trading.strategy_router_fallback_to_config),
            "execution_candidate_map": dict(config.trading.execution_candidate_map or {}),
        },
    )


def routed_backtest_sleeve_artifact_identity(
    *,
    config: AppConfig,
    project_root: Path,
    portfolio_symbols: list[str],
    symbol: str,
    required_scope: str = "demo",
) -> dict[str, Any]:
    return build_artifact_identity(
        config=config,
        project_root=project_root,
        artifact_kind="routed_backtest_sleeve",
        logical_prefix=routed_sleeve_report_prefix(symbol, config.strategy.name),
        symbols=portfolio_symbols,
        mode="portfolio",
        extra={
            "scope": "routed_sleeve",
            "symbol": symbol,
            "required_scope": required_scope,
            "strategy_router_enabled": bool(config.trading.strategy_router_enabled),
            "strategy_router_fallback_to_config": bool(config.trading.strategy_router_fallback_to_config),
            "execution_candidate_map": dict(config.trading.execution_candidate_map or {}),
        },
    )


def candidate_backtest_artifact_identity(
    *,
    config: AppConfig,
    project_root: Path,
    candidate_id: int,
    candidate_name: str,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    resolved_symbols = list(symbols or configured_symbols(config))
    return build_artifact_identity(
        config=config,
        project_root=project_root,
        artifact_kind="candidate_backtest",
        logical_prefix=candidate_report_prefix(candidate_name, candidate_id),
        symbols=resolved_symbols,
        mode="portfolio" if len(resolved_symbols) > 1 else "single",
        extra={"scope": "candidate", "candidate_id": candidate_id, "candidate_name": candidate_name},
    )


def candidate_backtest_sleeve_artifact_identity(
    *,
    config: AppConfig,
    project_root: Path,
    candidate_id: int,
    candidate_name: str,
    portfolio_symbols: list[str],
    symbol: str,
) -> dict[str, Any]:
    return build_artifact_identity(
        config=config,
        project_root=project_root,
        artifact_kind="candidate_backtest_sleeve",
        logical_prefix=candidate_sleeve_report_prefix(candidate_name, symbol, candidate_id),
        symbols=portfolio_symbols,
        mode="portfolio",
        extra={
            "scope": "candidate_sleeve",
            "candidate_id": candidate_id,
            "candidate_name": candidate_name,
            "symbol": symbol,
        },
    )


def artifact_resolution_path(resolution: dict[str, Any], key: str, fallback: Path) -> Path:
    artifacts = resolution.get("artifacts")
    if isinstance(artifacts, dict):
        candidate = artifacts.get(key)
        if isinstance(candidate, Path):
            return candidate
        if candidate not in {"", None}:
            return Path(str(candidate))
    return fallback


def backtest_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    symbols: list[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_root = project_root.resolve()
    storage = config.storage.resolved(resolved_root)
    identity = backtest_artifact_identity(config=config, project_root=resolved_root, symbols=symbols)
    logical_prefix = str(identity["logical_prefix"])
    resolution = resolve_artifact_group(
        report_dir=storage.report_dir,
        logical_prefix=logical_prefix,
        expected_fingerprint=str(identity["artifact_fingerprint"]),
        legacy_artifacts=_backtest_legacy_artifacts(storage.report_dir, logical_prefix),
    )
    return identity, resolution


def sleeve_backtest_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    portfolio_symbols: list[str],
    symbol: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_root = project_root.resolve()
    storage = config.storage.resolved(resolved_root)
    identity = backtest_sleeve_artifact_identity(
        config=config,
        project_root=resolved_root,
        portfolio_symbols=portfolio_symbols,
        symbol=symbol,
    )
    logical_prefix = str(identity["logical_prefix"])
    resolution = resolve_artifact_group(
        report_dir=storage.report_dir,
        logical_prefix=logical_prefix,
        expected_fingerprint=str(identity["artifact_fingerprint"]),
        legacy_artifacts=_backtest_legacy_artifacts(storage.report_dir, logical_prefix),
    )
    return identity, resolution


def sweep_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    extra: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_root = project_root.resolve()
    storage = config.storage.resolved(resolved_root)
    identity = sweep_artifact_identity(config=config, project_root=resolved_root, extra=extra)
    logical_prefix = str(identity["logical_prefix"])
    resolution = resolve_artifact_group(
        report_dir=storage.report_dir,
        logical_prefix=logical_prefix,
        expected_fingerprint=str(identity["artifact_fingerprint"]),
        legacy_artifacts=_sweep_legacy_artifacts(storage.report_dir, logical_prefix),
    )
    return identity, resolution


def trend_research_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    logical_prefix: str | None = None,
    extra: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_root = project_root.resolve()
    storage = config.storage.resolved(resolved_root)
    identity = trend_research_artifact_identity(
        config=config,
        project_root=resolved_root,
        logical_prefix=logical_prefix,
        extra=extra,
    )
    resolved_prefix = str(identity["logical_prefix"])
    resolution = resolve_artifact_group(
        report_dir=storage.report_dir,
        logical_prefix=resolved_prefix,
        expected_fingerprint=str(identity["artifact_fingerprint"]),
        legacy_artifacts=_trend_research_legacy_artifacts(storage.report_dir, resolved_prefix),
    )
    return identity, resolution


def routed_backtest_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    symbols: list[str] | None = None,
    required_scope: str = "demo",
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_root = project_root.resolve()
    storage = config.storage.resolved(resolved_root)
    identity = routed_backtest_artifact_identity(
        config=config,
        project_root=resolved_root,
        symbols=symbols,
        required_scope=required_scope,
    )
    logical_prefix = str(identity["logical_prefix"])
    resolution = resolve_artifact_group(
        report_dir=storage.report_dir,
        logical_prefix=logical_prefix,
        expected_fingerprint=str(identity["artifact_fingerprint"]),
        legacy_artifacts=_routed_backtest_legacy_artifacts(storage.report_dir, logical_prefix),
    )
    return identity, resolution


def routed_backtest_sleeve_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    portfolio_symbols: list[str],
    symbol: str,
    required_scope: str = "demo",
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_root = project_root.resolve()
    storage = config.storage.resolved(resolved_root)
    identity = routed_backtest_sleeve_artifact_identity(
        config=config,
        project_root=resolved_root,
        portfolio_symbols=portfolio_symbols,
        symbol=symbol,
        required_scope=required_scope,
    )
    logical_prefix = str(identity["logical_prefix"])
    resolution = resolve_artifact_group(
        report_dir=storage.report_dir,
        logical_prefix=logical_prefix,
        expected_fingerprint=str(identity["artifact_fingerprint"]),
        legacy_artifacts=_routed_backtest_legacy_artifacts(storage.report_dir, logical_prefix),
    )
    return identity, resolution


def candidate_backtest_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    candidate_id: int,
    candidate_name: str,
    symbols: list[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_root = project_root.resolve()
    storage = config.storage.resolved(resolved_root)
    resolved_symbols = list(symbols or configured_symbols(config))
    identity = candidate_backtest_artifact_identity(
        config=config,
        project_root=resolved_root,
        candidate_id=candidate_id,
        candidate_name=candidate_name,
        symbols=resolved_symbols,
    )
    resolution = resolve_artifact_group(
        report_dir=storage.report_dir,
        logical_prefix=str(identity["logical_prefix"]),
        expected_fingerprint=str(identity["artifact_fingerprint"]),
        legacy_artifacts=_backtest_legacy_artifacts(
            storage.report_dir,
            candidate_report_prefix(candidate_name),
        ),
    )
    return identity, resolution


def candidate_backtest_sleeve_artifact_resolution(
    *,
    config: AppConfig,
    project_root: Path,
    candidate_id: int,
    candidate_name: str,
    portfolio_symbols: list[str],
    symbol: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_root = project_root.resolve()
    storage = config.storage.resolved(resolved_root)
    identity = candidate_backtest_sleeve_artifact_identity(
        config=config,
        project_root=resolved_root,
        candidate_id=candidate_id,
        candidate_name=candidate_name,
        portfolio_symbols=portfolio_symbols,
        symbol=symbol,
    )
    resolution = resolve_artifact_group(
        report_dir=storage.report_dir,
        logical_prefix=str(identity["logical_prefix"]),
        expected_fingerprint=str(identity["artifact_fingerprint"]),
        legacy_artifacts=_backtest_legacy_artifacts(
            storage.report_dir,
            candidate_sleeve_report_prefix(candidate_name, symbol),
        ),
    )
    return identity, resolution


def trend_research_artifact_identity(
    *,
    config: AppConfig,
    project_root: Path,
    logical_prefix: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_artifact_identity(
        config=config,
        project_root=project_root,
        artifact_kind="research",
        logical_prefix=logical_prefix or trend_research_prefix(config),
        symbols=[config.instrument.symbol],
        mode="single",
        extra=extra,
    )


def read_artifact_manifest(report_dir: Path, logical_prefix: str) -> dict[str, Any] | None:
    path = manifest_path(report_dir, logical_prefix)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def update_artifact_manifest(
    *,
    report_dir: Path,
    identity: dict[str, Any],
    artifacts: dict[str, Path],
    aliases: dict[str, Path] | None = None,
) -> Path:
    logical_prefix = str(identity["logical_prefix"])
    current = read_artifact_manifest(report_dir, logical_prefix)
    same_fingerprint = current and current.get("artifact_fingerprint") == identity["artifact_fingerprint"]

    merged_artifacts: dict[str, str] = {}
    merged_aliases: dict[str, str] = {}
    if same_fingerprint:
        current_artifacts = current.get("artifacts")
        if isinstance(current_artifacts, dict):
            merged_artifacts.update({str(key): str(value) for key, value in current_artifacts.items()})
        current_aliases = current.get("aliases")
        if isinstance(current_aliases, dict):
            merged_aliases.update({str(key): str(value) for key, value in current_aliases.items()})

    merged_artifacts.update({key: str(path) for key, path in artifacts.items()})
    if aliases:
        merged_aliases.update({key: str(path) for key, path in aliases.items()})

    payload = {
        "artifact_kind": identity["artifact_kind"],
        "logical_prefix": logical_prefix,
        "artifact_fingerprint": identity["artifact_fingerprint"],
        "mode": identity["mode"],
        "symbols": list(identity["symbols"]),
        "artifacts": merged_artifacts,
        "aliases": merged_aliases,
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    path = manifest_path(report_dir, logical_prefix)
    atomic_write_json(path, payload)
    return path


def register_artifact_group(
    *,
    report_dir: Path,
    identity: dict[str, Any],
    artifacts: dict[str, Path],
    legacy_artifact_sets: list[dict[str, Path]] | tuple[dict[str, Path], ...] | None = None,
) -> Path:
    aliases: dict[str, Path] = {}
    for legacy_artifacts in legacy_artifact_sets or ():
        aliases.update(build_alias_map(legacy_artifacts=legacy_artifacts, canonical_artifacts=artifacts))
    return update_artifact_manifest(
        report_dir=report_dir,
        identity=identity,
        artifacts=artifacts,
        aliases=aliases or None,
    )


def build_alias_map(*, legacy_artifacts: dict[str, Path], canonical_artifacts: dict[str, Path]) -> dict[str, Path]:
    aliases: dict[str, Path] = {}
    for key, legacy_path in legacy_artifacts.items():
        canonical_path = canonical_artifacts.get(key)
        if canonical_path is None:
            continue
        aliases[legacy_path.name] = canonical_path
    return aliases


def resolve_artifact_group(
    *,
    report_dir: Path,
    logical_prefix: str,
    legacy_artifacts: dict[str, Path],
    expected_fingerprint: str | None = None,
) -> dict[str, Any]:
    manifest = read_artifact_manifest(report_dir, logical_prefix)
    manifest_matches = bool(manifest) and (
        expected_fingerprint is None or manifest.get("artifact_fingerprint") == expected_fingerprint
    )
    if manifest_matches:
        manifest_artifacts = manifest.get("artifacts")
        if isinstance(manifest_artifacts, dict):
            resolved_artifacts = {
                str(key): Path(str(value)).resolve()
                for key, value in manifest_artifacts.items()
                if value not in {"", None}
            }
            aliases = manifest.get("aliases")
            return {
                "resolved_via": MANIFEST_RESOLVED_VIA,
                "logical_prefix": logical_prefix,
                "artifact_fingerprint": manifest.get("artifact_fingerprint"),
                "artifacts": resolved_artifacts,
                "aliases": dict(aliases) if isinstance(aliases, dict) else {},
            }

    return {
        "resolved_via": LEGACY_RESOLVED_VIA,
        "logical_prefix": logical_prefix,
        "artifact_fingerprint": None,
        "artifacts": legacy_artifacts,
        "aliases": {},
    }


def resolve_artifact_open_path(report_dir: Path, file_name: str) -> Path | None:
    requested = Path(file_name).name
    direct = (report_dir / requested).resolve()
    try:
        direct.relative_to(report_dir.resolve())
    except ValueError:
        return None
    if direct.exists() and direct.is_file():
        return direct

    for manifest_file in sorted(report_dir.glob(f"*{MANIFEST_SUFFIX}")):
        manifest = read_artifact_manifest(report_dir, manifest_file.name[: -len(MANIFEST_SUFFIX)])
        if not manifest:
            continue
        aliases = manifest.get("aliases")
        if not isinstance(aliases, dict):
            continue
        target = aliases.get(requested)
        if target in {"", None}:
            continue
        candidate = Path(str(target)).resolve()
        try:
            candidate.relative_to(report_dir.resolve())
        except ValueError:
            continue
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _backtest_legacy_artifacts(report_dir: Path, logical_prefix: str) -> dict[str, Path]:
    return {
        "dashboard": report_dir / f"{logical_prefix}_dashboard.html",
        "summary": report_dir / f"{logical_prefix}_summary.json",
        "equity_curve": report_dir / f"{logical_prefix}_equity_curve.csv",
        "trades": report_dir / f"{logical_prefix}_trades.csv",
        "allocation_overlay": report_dir / f"{logical_prefix}_allocation_overlay.csv",
    }


def _sweep_legacy_artifacts(report_dir: Path, logical_prefix: str) -> dict[str, Path]:
    return {
        "dashboard": report_dir / f"{logical_prefix}_sweep_dashboard.html",
        "sweep_csv": report_dir / f"{logical_prefix}_sweep.csv",
    }


def _trend_research_legacy_artifacts(report_dir: Path, logical_prefix: str) -> dict[str, Path]:
    return {
        "dashboard": report_dir / f"{logical_prefix}.html",
        "research_csv": report_dir / f"{logical_prefix}.csv",
    }


def _routed_backtest_legacy_artifacts(report_dir: Path, logical_prefix: str) -> dict[str, Path]:
    legacy = _backtest_legacy_artifacts(report_dir, logical_prefix)
    legacy["routes"] = report_dir / f"{logical_prefix}_routes.csv"
    legacy["routing_summary"] = report_dir / f"{logical_prefix}_routing_summary.json"
    return legacy


def _artifact_token(value: str) -> str:
    chars: list[str] = []
    for char in str(value).strip().lower():
        if char.isalnum():
            chars.append(char)
        else:
            chars.append("_")
    token = "".join(chars).strip("_")
    while "__" in token:
        token = token.replace("__", "_")
    return token


def _local_instrument_payloads(*, config: AppConfig, storage, symbols: list[str]) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for symbol in symbols:
        if symbol == config.instrument.symbol:
            payloads[symbol] = config.instrument.model_dump(mode="json")
            continue
        metadata_path = storage.raw_dir / f"{symbol_slug(symbol)}_instrument.json"
        if metadata_path.exists():
            try:
                raw_payload = json.loads(metadata_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                raw_payload = None
            if isinstance(raw_payload, dict):
                payloads[symbol] = raw_payload
                continue
        payloads[symbol] = {"symbol": symbol}
    return payloads
