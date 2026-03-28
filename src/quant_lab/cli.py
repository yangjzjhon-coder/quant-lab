from __future__ import annotations

import hashlib
import json
import time
from dataclasses import fields
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import typer
import uvicorn

from quant_lab.alerts.delivery import deliver_alerts
from quant_lab.backtest.engine import run_backtest
from quant_lab.backtest.metrics import build_summary
from quant_lab.backtest.portfolio import (
    build_portfolio_summary,
    build_portfolio_trade_frame,
    combine_portfolio_equity_curves,
)
from quant_lab.backtest.routed import run_routed_backtest, summarize_route_frame
from quant_lab.backtest.sweep import run_parameter_sweep
from quant_lab.backtest.trend_research import run_trend_research
from quant_lab.config import (
    InstrumentConfig,
    configured_symbols,
    ensure_storage_dirs,
    load_config,
    update_trading_section,
    update_instrument_section,
)
from quant_lab.data.public_factors import PublicFactorSnapshot, load_public_factor_snapshot
from quant_lab.data.okx_private_client import OkxPrivateClient
from quant_lab.data.okx_public_client import OkxApiError, OkxPublicClient
from quant_lab.execution.planner import (
    AccountSnapshot,
    OrderPlan,
    PositionSnapshot,
    build_account_snapshot,
    build_order_plan,
    build_position_snapshot,
    build_signal_snapshot,
    extract_okx_max_size,
)
from quant_lab.execution.strategy_router import resolve_strategy_route
from quant_lab.models import TradeRecord
from quant_lab.reporting.dashboard import render_dashboard
from quant_lab.reporting.sweep_dashboard import render_sweep_dashboard
from quant_lab.reporting.trend_research_dashboard import render_trend_research_dashboard
from quant_lab.risk.portfolio import apply_factor_overlay_to_plan, apply_portfolio_risk_caps
from quant_lab.service.database import AlertEvent, ServiceHeartbeat, init_db, make_session_factory, session_scope
from quant_lab.service.monitor import build_preflight_payload, build_service_app, run_monitor_cycle
from quant_lab.service.research_ai import ResearchAIRequest, build_research_ai_status, run_research_ai_request
from quant_lab.service.research_ops import (
    approve_strategy_candidate,
    backtest_strategy_candidate,
    build_research_overview,
    evaluate_backtested_candidate,
    create_research_task,
    evaluate_strategy_candidate,
    infer_candidate_artifacts,
    infer_strategy_candidate_artifacts_by_id,
    list_research_tasks,
    list_strategy_candidates,
    materialize_trend_research_candidates,
    promote_trend_research_candidates,
    register_strategy_candidate,
    resolve_execution_approval,
    serialize_approval_decision,
    serialize_evaluation_report,
    serialize_research_task,
    serialize_strategy_candidate,
)
from quant_lab.utils.timeframes import bar_to_timedelta as parse_bar_timedelta

app = typer.Typer(help="Local crypto backtesting toolkit for realistic OKX trend and breakout systems.")


def _trades_frame(trades: list[TradeRecord]) -> pd.DataFrame:
    columns = [field.name for field in fields(TradeRecord)]
    if not trades:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame([trade.to_dict() for trade in trades], columns=columns)


def _write_backtest_artifacts(
    *,
    storage,
    report_prefix: str,
    trades_frame: pd.DataFrame,
    equity_curve: pd.DataFrame,
    summary: dict[str, object],
) -> tuple[Path, Path, Path]:
    trades_path = storage.report_dir / f"{report_prefix}_trades.csv"
    equity_path = storage.report_dir / f"{report_prefix}_equity_curve.csv"
    summary_path = storage.report_dir / f"{report_prefix}_summary.json"

    trades_frame.to_csv(trades_path, index=False)
    equity_curve.to_csv(equity_path, index=False)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return trades_path, equity_path, summary_path


def _write_routing_artifacts(
    *,
    storage,
    report_prefix: str,
    route_frame: pd.DataFrame,
    route_summary: dict[str, object],
) -> tuple[Path, Path]:
    route_path = storage.report_dir / f"{report_prefix}_routes.csv"
    route_summary_path = storage.report_dir / f"{report_prefix}_routing_summary.json"
    route_frame.to_csv(route_path, index=False)
    route_summary_path.write_text(json.dumps(route_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return route_path, route_summary_path


def _load_report_inputs(
    config: Path,
    project_root: Path,
) -> tuple[object, object, pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    cfg = load_config(config)
    storage = cfg.storage.resolved(project_root.resolve())
    ensure_storage_dirs(storage)
    symbol = cfg.instrument.symbol
    signal_bars, execution_bars, funding, symbol_slug = _load_symbol_datasets(
        storage=storage,
        symbol=symbol,
        signal_bar=cfg.strategy.signal_bar,
        execution_bar=cfg.strategy.execution_bar,
        variant=cfg.strategy.variant,
    )
    return cfg, storage, signal_bars, execution_bars, funding, symbol_slug


def _load_symbol_report_inputs(
    *,
    cfg,
    project_root: Path,
    symbol: str,
) -> tuple[object, object, pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    storage = cfg.storage.resolved(project_root.resolve())
    ensure_storage_dirs(storage)
    signal_bars, execution_bars, funding, symbol_slug = _load_symbol_datasets(
        storage=storage,
        symbol=symbol,
        signal_bar=cfg.strategy.signal_bar,
        execution_bar=cfg.strategy.execution_bar,
        variant=cfg.strategy.variant,
    )
    return cfg, storage, signal_bars, execution_bars, funding, symbol_slug


def _load_symbol_routed_report_inputs(
    *,
    cfg,
    project_root: Path,
    symbol: str,
) -> tuple[object, object, pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    cfg, storage, signal_bars, execution_bars, funding, symbol_slug = _load_symbol_report_inputs(
        cfg=cfg,
        project_root=project_root,
        symbol=symbol,
    )
    signal_bars = _enrich_signal_bars_for_high_weight_strategy(
        signal_bars=signal_bars,
        mark_price_bars=_read_parquet_if_exists(storage.raw_dir / f"{symbol_slug}_mark_price_{cfg.strategy.signal_bar}.parquet"),
        index_bars=_read_parquet_if_exists(storage.raw_dir / f"{symbol_slug}_index_{cfg.strategy.signal_bar}.parquet"),
    )
    return cfg, storage, signal_bars, execution_bars, funding, symbol_slug


def _load_symbol_datasets(
    *,
    storage,
    symbol: str,
    signal_bar: str,
    execution_bar: str,
    variant: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    symbol_slug = _symbol_slug(symbol)
    signal_path = storage.raw_dir / f"{symbol_slug}_{signal_bar}.parquet"
    execution_path = storage.raw_dir / f"{symbol_slug}_{execution_bar}.parquet"
    funding_path = storage.raw_dir / f"{symbol_slug}_funding.parquet"

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
        signal_bars = _enrich_signal_bars_for_high_weight_strategy(
            signal_bars=signal_bars,
            mark_price_bars=_read_parquet_if_exists(storage.raw_dir / f"{symbol_slug}_mark_price_{signal_bar}.parquet"),
            index_bars=_read_parquet_if_exists(storage.raw_dir / f"{symbol_slug}_index_{signal_bar}.parquet"),
        )
    return signal_bars, execution_bars, funding, symbol_slug


def _load_runtime_context(config: Path, project_root: Path):
    cfg = _load_app_context(config=config, project_root=project_root)
    session_factory = make_session_factory(cfg.database.url)
    return cfg, session_factory


def _load_app_context(config: Path, project_root: Path):
    cfg = load_config(config)
    storage = cfg.storage.resolved(project_root.resolve())
    cfg.storage = storage
    cfg.database = cfg.database.resolved(project_root.resolve())
    ensure_storage_dirs(storage)
    return cfg


def _symbol_slug(symbol: str) -> str:
    return symbol.replace("/", "-")


def _symbol_list_label(symbols: list[str]) -> str:
    return ", ".join(symbols)


def _portfolio_report_prefix(symbols: list[str], strategy_name: str) -> str:
    base_assets = "_".join(symbol.split("-")[0].lower() for symbol in symbols)
    return f"portfolio_{base_assets}_{strategy_name}"


def _parse_int_list(raw: str) -> list[int]:
    values = [int(chunk.strip()) for chunk in raw.split(",") if chunk.strip()]
    if not values:
        raise typer.BadParameter("Expected at least one integer value.")
    return values


def _parse_float_list(raw: str) -> list[float]:
    values = [float(chunk.strip()) for chunk in raw.split(",") if chunk.strip()]
    if not values:
        raise typer.BadParameter("Expected at least one float value.")
    return values


def _parse_text_list(raw: str) -> list[str]:
    values = [chunk.strip() for chunk in raw.split(",") if chunk.strip()]
    if not values:
        raise typer.BadParameter("Expected at least one text value.")
    return values


def _attach_routing_summary(summary: dict[str, object], route_summary: dict[str, object]) -> dict[str, object]:
    enriched = dict(summary)
    enriched["routing_mode"] = "candidate_router"
    enriched["routing_candidate_bar_pct"] = route_summary.get("candidate_bar_pct")
    enriched["routing_fallback_bar_pct"] = route_summary.get("fallback_bar_pct")
    enriched["routing_flat_bar_pct"] = route_summary.get("flat_bar_pct")
    enriched["routing_total_signal_bars"] = route_summary.get("total_signal_bars")
    enriched["routing_regime_counts"] = route_summary.get("regime_counts")
    enriched["routing_route_status_counts"] = route_summary.get("route_status_counts")
    return enriched


def _resolve_symbols(cfg, raw_symbols: str | None) -> list[str]:
    if raw_symbols:
        return _parse_text_list(raw_symbols)
    return configured_symbols(cfg)


def _instrument_metadata_path(storage, symbol: str) -> Path:
    return storage.raw_dir / f"{_symbol_slug(symbol)}_instrument.json"


def _index_inst_id(symbol: str) -> str:
    parts = [part for part in symbol.split("-") if part]
    if len(parts) >= 2:
        return "-".join(parts[:2])
    return symbol


def _read_parquet_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _enrich_signal_bars_for_high_weight_strategy(
    *,
    signal_bars: pd.DataFrame,
    mark_price_bars: pd.DataFrame,
    index_bars: pd.DataFrame,
) -> pd.DataFrame:
    enriched = signal_bars.copy()
    enriched["timestamp"] = pd.to_datetime(enriched["timestamp"], utc=True)
    enriched = _merge_reference_close(enriched, mark_price_bars, target_column="mark_close")
    enriched = _merge_reference_close(enriched, index_bars, target_column="index_close")
    return enriched


def _merge_reference_close(
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


def _merge_deduped_frame(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    *,
    subset: list[str],
) -> pd.DataFrame:
    if existing.empty:
        combined = incoming.copy()
    elif incoming.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, incoming], ignore_index=True)

    if combined.empty:
        return combined

    for column in ("timestamp",):
        if column in combined.columns:
            combined[column] = pd.to_datetime(combined[column], utc=True)

    combined = combined.drop_duplicates(subset=subset, keep="last")
    sort_columns = [column for column in ("timestamp", *subset) if column in combined.columns]
    if sort_columns:
        combined = combined.sort_values(sort_columns, kind="stable")
    return combined.reset_index(drop=True)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _books_full_summary(snapshot: dict[str, Any]) -> pd.DataFrame:
    bids = snapshot.get("bids") or []
    asks = snapshot.get("asks") or []

    best_bid = bids[0] if bids else {}
    best_ask = asks[0] if asks else {}
    best_bid_price = _safe_float(best_bid.get("price"), fallback=0.0)
    best_ask_price = _safe_float(best_ask.get("price"), fallback=0.0)

    return pd.DataFrame(
        [
            {
                "timestamp": snapshot.get("timestamp"),
                "symbol": snapshot.get("symbol"),
                "depth": snapshot.get("depth"),
                "best_bid_price": best_bid_price,
                "best_bid_size": _safe_float(best_bid.get("size"), fallback=None),
                "best_ask_price": best_ask_price,
                "best_ask_size": _safe_float(best_ask.get("size"), fallback=None),
                "spread": (best_ask_price - best_bid_price) if best_bid_price and best_ask_price else None,
                "bid_top5_notional": round(
                    sum(
                        (_safe_float(level.get("price"), fallback=0.0) or 0.0)
                        * (_safe_float(level.get("size"), fallback=0.0) or 0.0)
                        for level in bids[:5]
                    ),
                    4,
                ),
                "ask_top5_notional": round(
                    sum(
                        (_safe_float(level.get("price"), fallback=0.0) or 0.0)
                        * (_safe_float(level.get("size"), fallback=0.0) or 0.0)
                        for level in asks[:5]
                    ),
                    4,
                ),
            }
        ]
    )


def _load_symbol_public_factor_snapshot(
    *,
    cfg,
    symbol: str,
    asof: pd.Timestamp | None,
) -> PublicFactorSnapshot | None:
    if not cfg.strategy.use_public_factor_overlay:
        return None
    try:
        return load_public_factor_snapshot(
            raw_dir=cfg.storage.raw_dir,
            symbol=symbol,
            signal_bar=cfg.strategy.signal_bar,
            asof=asof,
        )
    except Exception as exc:
        return PublicFactorSnapshot(
            symbol=symbol,
            asof=asof,
            score=0.5,
            confidence=0.0,
            risk_multiplier=1.0,
            notes=[f"public factor load failed: {type(exc).__name__}: {exc}"],
        )


def _resolve_instrument_config(cfg, storage, symbol: str) -> InstrumentConfig:
    if symbol == cfg.instrument.symbol:
        return cfg.instrument

    metadata_path = _instrument_metadata_path(storage, symbol)
    if metadata_path.exists():
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        return InstrumentConfig.model_validate(payload)

    client = OkxPublicClient(base_url=cfg.okx.rest_base_url, proxy_url=cfg.okx.proxy_url)
    try:
        instrument = client.fetch_instrument_details(
            inst_type=cfg.instrument.instrument_type,
            inst_id=symbol,
        )
    finally:
        client.close()

    metadata_path.write_text(json.dumps(instrument, ensure_ascii=False, indent=2), encoding="utf-8")
    return InstrumentConfig.model_validate(instrument)


def _require_private_credentials(cfg) -> None:
    missing = [
        name
        for name, value in (
            ("OKX_API_KEY", cfg.okx.api_key),
            ("OKX_SECRET_KEY", cfg.okx.secret_key),
            ("OKX_PASSPHRASE", cfg.okx.passphrase),
        )
        if not value
    ]
    if missing:
        raise typer.BadParameter(
            "Missing OKX private credentials. "
            f"Set them in .env or environment variables: {', '.join(missing)}"
        )


def _build_private_client(cfg) -> OkxPrivateClient:
    _require_private_credentials(cfg)
    return OkxPrivateClient(
        api_key=str(cfg.okx.api_key),
        secret_key=str(cfg.okx.secret_key),
        passphrase=str(cfg.okx.passphrase),
        base_url=cfg.okx.rest_base_url,
        use_demo=cfg.okx.use_demo,
        proxy_url=cfg.okx.proxy_url,
    )


def _bar_to_timedelta(bar: str) -> pd.Timedelta:
    try:
        return parse_bar_timedelta(bar)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _fetch_live_market_data(cfg) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _fetch_live_market_data_for_symbol(cfg, cfg.instrument.symbol)


def _fetch_live_market_data_for_symbol(cfg, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    now = pd.Timestamp.now(tz="UTC")
    signal_bars_needed = max(
        cfg.trading.signal_lookback_bars,
        cfg.strategy.slow_ema + cfg.strategy.atr_period + 10,
    )
    execution_bars_needed = max(cfg.trading.execution_lookback_bars, cfg.execution.latency_minutes + 20)

    signal_start = now - (_bar_to_timedelta(cfg.strategy.signal_bar) * signal_bars_needed)
    execution_start = now - (_bar_to_timedelta(cfg.strategy.execution_bar) * execution_bars_needed)

    client = OkxPublicClient(base_url=cfg.okx.rest_base_url, proxy_url=cfg.okx.proxy_url)
    try:
        signal_bars = client.fetch_history_candles(
            inst_id=symbol,
            bar=cfg.strategy.signal_bar,
            start=signal_start,
            end=now,
        )
        execution_bars = client.fetch_history_candles(
            inst_id=symbol,
            bar=cfg.strategy.execution_bar,
            start=execution_start,
            end=now,
        )
    finally:
        client.close()

    if signal_bars.empty or execution_bars.empty:
        raise typer.BadParameter("Unable to fetch enough recent market data for demo planning.")
    return signal_bars, execution_bars


def _load_demo_state(
    cfg,
    *,
    session_factory=None,
    project_root: Path | None = None,
) -> tuple[AccountSnapshot, PositionSnapshot, dict[str, object]]:
    return _load_demo_state_for_symbol(
        cfg,
        cfg.instrument.symbol,
        session_factory=session_factory,
        project_root=project_root,
    )


def _load_demo_state_for_symbol(
    cfg,
    symbol: str,
    *,
    session_factory=None,
    project_root: Path | None = None,
    private_client: OkxPrivateClient | None = None,
    shared_balance_payload: dict[str, object] | None = None,
    shared_account_config_payload: dict[str, object] | None = None,
    allocated_equity: float | None = None,
) -> tuple[AccountSnapshot, PositionSnapshot, dict[str, object]]:
    instrument_config = _resolve_instrument_config(cfg, cfg.storage, symbol)
    signal_bars, execution_bars = _fetch_live_market_data_for_symbol(cfg, symbol)
    router_decision = None
    strategy_config_for_signal = cfg.strategy
    if session_factory is not None and project_root is not None:
        router_decision = resolve_strategy_route(
            session_factory=session_factory,
            config=cfg,
            project_root=project_root,
            symbol=symbol,
            signal_bars=signal_bars,
            required_scope="demo",
        )
        strategy_config_for_signal = router_decision.strategy_config
    signal = build_signal_snapshot(
        signal_bars=signal_bars,
        execution_bars=execution_bars,
        strategy_config=strategy_config_for_signal,
        execution_config=cfg.execution,
    )

    balance_payload = None
    positions_payload = None
    account_config_payload = None
    max_size_payload = None
    leverage_payload = None
    pending_orders_payload = None
    pending_algo_orders_payload = None
    warnings: list[str] = []

    owns_client = False
    if cfg.okx.api_key and cfg.okx.secret_key and cfg.okx.passphrase:
        if private_client is None:
            private_client = _build_private_client(cfg)
            owns_client = True
        try:
            account_config_payload = shared_account_config_payload or private_client.get_account_config()
            balance_payload = shared_balance_payload or private_client.get_balance(ccy=instrument_config.settle_currency)
            positions_payload = private_client.get_positions(
                inst_type=instrument_config.instrument_type,
                inst_id=symbol,
            )
            max_size_payload = private_client.get_max_order_size(
                inst_id=symbol,
                td_mode=cfg.trading.td_mode,
                ccy=instrument_config.settle_currency,
                leverage=cfg.execution.max_leverage,
            )
            leverage_payload = private_client.get_leverage_info(
                inst_id=symbol,
                mgn_mode=cfg.trading.td_mode,
            )
            pending_orders_payload = private_client.get_pending_orders(
                inst_type=instrument_config.instrument_type,
                inst_id=symbol,
            )
            pending_algo_orders_payload = private_client.get_pending_algo_orders(
                inst_id=symbol,
                ord_type="conditional",
            )
        finally:
            if owns_client:
                private_client.close()
    else:
        warnings.append("Private OKX credentials are missing. Plan falls back to config equity and flat position.")

    account = build_account_snapshot(
        balance_payload=balance_payload,
        account_config_payload=account_config_payload,
        settle_currency=instrument_config.settle_currency,
        fallback_equity=cfg.execution.initial_equity,
    )
    planning_account = account
    if allocated_equity is not None and allocated_equity > 0:
        allocated_value = min(allocated_equity, account.available_equity or allocated_equity)
        planning_account = AccountSnapshot(
            total_equity=allocated_value,
            available_equity=allocated_value,
            currency=account.currency,
            source=f"{account.source}_allocated",
            account_mode=account.account_mode,
            can_trade=account.can_trade,
            raw=account.raw,
        )
        warnings.append(f"Planning equity for {symbol} is capped to equal-weight sleeve capital {allocated_value:.2f}.")
    account_position_mode = account.account_mode or cfg.trading.position_mode
    position = build_position_snapshot(
        positions_payload=positions_payload,
        inst_id=symbol,
        position_mode=account_position_mode,
    )
    max_buy, max_sell = extract_okx_max_size(max_size_payload)
    plan = build_order_plan(
        signal=signal,
        account=planning_account,
        position=position,
        instrument_config=instrument_config,
        execution_config=cfg.execution,
        risk_config=cfg.risk,
        trading_config=cfg.trading,
        max_buy_contracts=max_buy,
        max_sell_contracts=max_sell,
    )
    if router_decision is not None and router_decision.enabled and not router_decision.ready:
        plan.warnings.append(
            "Strategy router is not ready for execution: " + "; ".join(router_decision.reasons or ["route not ready"])
        )
    plan.warnings.extend(warnings)
    public_factor_snapshot = _load_symbol_public_factor_snapshot(
        cfg=cfg,
        symbol=symbol,
        asof=signal.latest_execution_time,
    )
    factor_overlay = apply_factor_overlay_to_plan(
        symbol=symbol,
        plan=plan,
        lot_size=instrument_config.lot_size,
        factor_snapshot=public_factor_snapshot,
        min_factor_score=cfg.strategy.min_public_factor_score,
    )
    return account, position, {
        "symbol": symbol,
        "instrument_config": instrument_config,
        "planning_account": planning_account,
        "signal": signal,
        "plan": plan,
        "router_decision": router_decision.to_dict() if router_decision is not None else None,
        "public_factor_snapshot": public_factor_snapshot,
        "factor_overlay": factor_overlay,
        "account_config_payload": account_config_payload,
        "balance_payload": balance_payload,
        "positions_payload": positions_payload,
        "max_size_payload": max_size_payload,
        "leverage_payload": leverage_payload,
        "pending_orders_payload": pending_orders_payload,
        "pending_algo_orders_payload": pending_algo_orders_payload,
    }


def _load_demo_portfolio_state(
    cfg,
    symbols: list[str],
    *,
    session_factory=None,
    project_root: Path | None = None,
) -> tuple[AccountSnapshot, dict[str, dict[str, object]]]:
    if not symbols:
        raise typer.BadParameter("Portfolio demo requires at least one symbol.")

    shared_account_payload = None
    shared_balance_payload = None
    private_client = None
    if cfg.okx.api_key and cfg.okx.secret_key and cfg.okx.passphrase:
        private_client = _build_private_client(cfg)
        shared_account_payload = private_client.get_account_config()
        shared_balance_payload = private_client.get_balance(ccy=cfg.instrument.settle_currency)

    base_account = build_account_snapshot(
        balance_payload=shared_balance_payload,
        account_config_payload=shared_account_payload,
        settle_currency=cfg.instrument.settle_currency,
        fallback_equity=cfg.execution.initial_equity,
    )
    total_equity_reference = base_account.available_equity or base_account.total_equity or cfg.execution.initial_equity
    sleeve_equity = total_equity_reference / len(symbols)
    states: dict[str, dict[str, object]] = {}
    try:
        for symbol in symbols:
            account, position, state = _load_demo_state_for_symbol(
                cfg,
                symbol,
                session_factory=session_factory,
                project_root=project_root,
                private_client=private_client,
                shared_balance_payload=shared_balance_payload,
                shared_account_config_payload=shared_account_payload,
                allocated_equity=sleeve_equity,
            )
            states[symbol] = {
                "account": account,
                "position": position,
                **state,
            }
        portfolio_risk_controls = apply_portfolio_risk_caps(
            symbol_states=states,
            total_equity=total_equity_reference,
            portfolio_max_total_risk=cfg.risk.portfolio_max_total_risk,
            portfolio_max_same_direction_risk=cfg.risk.portfolio_max_same_direction_risk,
        )
        for symbol, decision in portfolio_risk_controls.items():
            if symbol in states:
                states[symbol]["portfolio_risk"] = decision
    finally:
        if private_client is not None:
            private_client.close()
    return base_account, states


def _demo_state_payload(
    cfg,
    account: AccountSnapshot,
    position: PositionSnapshot,
    signal,
    plan: OrderPlan,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    warnings = list(plan.warnings)
    if account.account_mode and account.account_mode != cfg.trading.position_mode:
        warnings.append(
            f"OKX account posMode={account.account_mode}, while config expects {cfg.trading.position_mode}."
        )
    if not cfg.okx.use_demo:
        warnings.append("okx.use_demo=false. For safety, demo-execute will refuse order submission.")

    payload: dict[str, object] = {
        "instrument": cfg.instrument.symbol,
        "okx_use_demo": cfg.okx.use_demo,
        "account": account.to_dict(),
        "position": position.to_dict(),
        "signal": signal.to_dict(),
        "plan": plan.to_dict(),
        "warnings": warnings,
    }
    if extra:
        payload.update(extra)
    return payload


def _extract_submission_refs(responses: list[dict[str, object]]) -> list[dict[str, object]]:
    refs: list[dict[str, object]] = []
    for item in responses:
        if not isinstance(item, dict):
            continue
        request = item.get("request")
        response = item.get("response")
        request_payload = request if isinstance(request, dict) else {}
        response_payload = response if isinstance(response, dict) else {}
        response_rows = response_payload.get("data")
        response_row = response_rows[0] if isinstance(response_rows, list) and response_rows else {}
        response_row = response_row if isinstance(response_row, dict) else {}
        attach_algo_ords = request_payload.get("attachAlgoOrds")
        attach_algo_cl_ord_ids: list[str] = []
        if isinstance(attach_algo_ords, list):
            for algo in attach_algo_ords:
                if isinstance(algo, dict) and algo.get("attachAlgoClOrdId"):
                    attach_algo_cl_ord_ids.append(str(algo["attachAlgoClOrdId"]))
        refs.append(
            {
                "purpose": item.get("purpose"),
                "inst_id": request_payload.get("instId"),
                "side": request_payload.get("side"),
                "pos_side": request_payload.get("posSide"),
                "size": _safe_float(request_payload.get("sz"), fallback=None),
                "client_order_id": request_payload.get("clOrdId"),
                "order_id": response_row.get("ordId"),
                "algo_id": response_row.get("algoId"),
                "attach_algo_cl_ord_ids": attach_algo_cl_ord_ids,
            }
        )
    return refs


def _dump_demo_state(
    cfg,
    account: AccountSnapshot,
    position: PositionSnapshot,
    signal,
    plan: OrderPlan,
    extra: dict[str, object] | None = None,
) -> str:
    return json.dumps(
        _demo_state_payload(
            cfg=cfg,
            account=account,
            position=position,
            signal=signal,
            plan=plan,
            extra=extra,
        ),
        ensure_ascii=False,
        indent=2,
    )


def _build_client_order_id(tag: str, sequence: int) -> str:
    prefix = "".join(ch for ch in tag if ch.isalnum()).lower()[:10] or "qlab"
    timestamp = int(pd.Timestamp.now(tz="UTC").timestamp() * 1000)
    return f"{prefix}{timestamp}{sequence:02d}"[:32]


def _executor_state_path(cfg) -> Path:
    return cfg.storage.data_dir / "demo_executor_state.json"


def _load_executor_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_executor_state(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _executor_log_path(cfg) -> Path:
    return cfg.storage.data_dir / "demo-loop.log"


def _plan_signature(signal, plan: OrderPlan) -> str:
    raw = json.dumps(
        {
            "signal_time": signal.signal_time.isoformat(),
            "effective_time": signal.effective_time.isoformat(),
            "desired_side": signal.desired_side,
            "action": plan.action,
            "instructions": [item.to_dict() for item in plan.instructions],
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _alert_is_due(executor_state: dict[str, object], event_key: str, cooldown_seconds: int) -> bool:
    if cooldown_seconds <= 0:
        return True
    alert_timestamps = executor_state.get("alert_timestamps")
    if not isinstance(alert_timestamps, dict):
        return True
    raw_value = alert_timestamps.get(event_key)
    if not raw_value:
        return True
    try:
        previous = pd.Timestamp(str(raw_value))
    except ValueError:
        return True
    if previous.tzinfo is None:
        previous = previous.tz_localize("UTC")
    now = pd.Timestamp.now(tz="UTC")
    return (now - previous) >= pd.Timedelta(seconds=cooldown_seconds)


def _mark_alert_sent(executor_state: dict[str, object], event_key: str) -> None:
    alert_timestamps = executor_state.get("alert_timestamps")
    if not isinstance(alert_timestamps, dict):
        alert_timestamps = {}
    alert_timestamps[event_key] = pd.Timestamp.now(tz="UTC").isoformat()
    executor_state["alert_timestamps"] = alert_timestamps


def _persist_heartbeat(session_factory, *, service_name: str, status: str, details: dict[str, object]) -> None:
    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name=service_name,
                status=status,
                details=details,
            )
        )


def _persist_alert_results(
    session_factory,
    *,
    cfg,
    event_key: str,
    level: str,
    title: str,
    message: str,
) -> list[str]:
    results = deliver_alerts(cfg.alerts, title=title, message=message)
    sent_channels: list[str] = []
    with session_scope(session_factory) as session:
        for result in results:
            if result.delivered:
                sent_channels.append(result.channel)
            session.add(
                AlertEvent(
                    event_key=event_key,
                    channel=result.channel,
                    level=level,
                    title=title,
                    message=message if result.error is None else f"{message}\n\nerror: {result.error}",
                    status=result.status,
                    delivered_at=result.delivered_at,
                )
            )
    return sent_channels


def _okx_rows(payload: dict[str, object] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows = payload.get("data")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _safe_float(value: object, *, fallback: float | None = 0.0) -> float | None:
    if value is None or value == "" or value == " ":
        return fallback
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _expected_stop_side(position_side: int) -> str | None:
    if position_side > 0:
        return "sell"
    if position_side < 0:
        return "buy"
    return None


def _expected_position_leg(position_side: int, position_mode: str) -> str | None:
    if position_mode == "long_short_mode":
        if position_side > 0:
            return "long"
        if position_side < 0:
            return "short"
    if position_mode == "net_mode":
        return "net"
    return None


def _matching_stop_orders(cfg, position: PositionSnapshot, pending_algo_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if position.side == 0 or position.contracts <= 0:
        return []

    expected_side = _expected_stop_side(position.side)
    expected_leg = _expected_position_leg(position.side, cfg.trading.position_mode)
    size_tolerance = max(cfg.instrument.lot_size, 1e-9)
    matches: list[dict[str, Any]] = []

    for row in pending_algo_orders:
        if row.get("instId") != cfg.instrument.symbol:
            continue
        state = str(row.get("state") or "").lower()
        if state and state not in {"live", "effective", "partially_effective"}:
            continue
        if expected_side and row.get("side") != expected_side:
            continue
        row_leg = row.get("posSide")
        if expected_leg and row_leg not in {None, "", expected_leg}:
            continue
        size = _safe_float(row.get("sz"), fallback=None)
        if size is not None and abs(size - position.contracts) > size_tolerance:
            continue
        matches.append(row)

    return matches


def _summarize_executor_state(executor_state: dict[str, object]) -> dict[str, object] | None:
    if not executor_state:
        return None
    return {
        "last_submitted_at": executor_state.get("last_submitted_at"),
        "last_submitted_signature": executor_state.get("last_submitted_signature"),
        "last_submission_refs": executor_state.get("last_submission_refs"),
        "last_error": executor_state.get("last_error"),
        "last_plan": executor_state.get("last_plan"),
        "last_signal": executor_state.get("last_signal"),
    }


def _build_demo_reconcile_payload(
    *,
    cfg,
    account: AccountSnapshot,
    position: PositionSnapshot,
    signal,
    plan: OrderPlan,
    state: dict[str, object],
    executor_state: dict[str, object],
) -> dict[str, object]:
    payload = _demo_state_payload(
        cfg=cfg,
        account=account,
        position=position,
        signal=signal,
        plan=plan,
    )
    warnings = list(payload["warnings"])
    public_factor_snapshot = state.get("public_factor_snapshot")
    if isinstance(public_factor_snapshot, PublicFactorSnapshot):
        payload["public_factors"] = public_factor_snapshot.to_dict()
    factor_overlay = state.get("factor_overlay")
    if factor_overlay is not None and hasattr(factor_overlay, "to_dict"):
        payload["factor_overlay"] = factor_overlay.to_dict()
    router_decision = state.get("router_decision")
    if isinstance(router_decision, dict):
        payload["router_decision"] = router_decision
    portfolio_risk = state.get("portfolio_risk")
    if portfolio_risk is not None and hasattr(portfolio_risk, "to_dict"):
        payload["portfolio_risk"] = portfolio_risk.to_dict()

    leverage_rows = _okx_rows(state.get("leverage_payload"))
    pending_orders = _okx_rows(state.get("pending_orders_payload"))
    pending_algo_orders = _okx_rows(state.get("pending_algo_orders_payload"))
    matching_stop_orders = _matching_stop_orders(cfg, position, pending_algo_orders)

    leverage_values = [
        value
        for value in (_safe_float(row.get("lever"), fallback=None) for row in leverage_rows)
        if value is not None
    ]
    leverage_match = None
    if leverage_values:
        leverage_match = all(abs(value - cfg.execution.max_leverage) <= 0.01 for value in leverage_values)

    position_mode_match = None
    if account.account_mode:
        position_mode_match = account.account_mode == cfg.trading.position_mode

    protective_stop_needed = position.side != 0 and position.contracts > 0
    protective_stop_ready = bool(matching_stop_orders) if protective_stop_needed else True
    size_match = None
    if position.side != 0 and plan.target_contracts > 0:
        size_tolerance = max(cfg.instrument.lot_size, 1e-9)
        size_match = abs(position.contracts - plan.target_contracts) <= size_tolerance

    last_submission_refs = executor_state.get("last_submission_refs")
    tracked_algo_ids: set[str] = set()
    if isinstance(last_submission_refs, list):
        for ref in last_submission_refs:
            if not isinstance(ref, dict):
                continue
            attach_ids = ref.get("attach_algo_cl_ord_ids")
            if isinstance(attach_ids, list):
                for value in attach_ids:
                    if value:
                        tracked_algo_ids.add(str(value))
    tracked_live_algo_ids = sorted(
        {
            str(row.get("algoClOrdId"))
            for row in pending_algo_orders
            if row.get("algoClOrdId") and str(row.get("algoClOrdId")) in tracked_algo_ids
        }
    )

    if account.can_trade is False:
        warnings.append("OKX account permission does not include trade.")
    if position_mode_match is False:
        warnings.append(
            f"OKX account posMode={account.account_mode}, but config expects {cfg.trading.position_mode}."
        )
    if leverage_match is False:
        warnings.append(
            f"OKX leverage setting does not match config max_leverage={cfg.execution.max_leverage:.2f}."
        )
    if pending_orders:
        warnings.append(f"There are {len(pending_orders)} pending standard orders still working on OKX.")
    if protective_stop_needed and not matching_stop_orders:
        warnings.append("Current live position does not have a visible protective stop order on OKX.")
    if size_match is False:
        warnings.append(
            f"Current live contracts {position.contracts:.4f} differ from latest target {plan.target_contracts:.4f}."
        )
    if executor_state.get("last_error"):
        warnings.append("Executor state still contains the last loop error. Inspect executor_state.last_error.")

    payload["checks"] = {
        "trade_permission": account.can_trade,
        "position_mode_match": position_mode_match,
        "leverage_match": leverage_match,
        "size_match": size_match,
        "protective_stop_ready": protective_stop_ready,
        "open_orders_idle": len(pending_orders) == 0,
        "executor_state_present": bool(executor_state),
        "tracked_stop_order_seen": bool(tracked_live_algo_ids) if tracked_algo_ids else None,
    }
    payload["exchange"] = {
        "pending_orders": {
            "count": len(pending_orders),
            "items": [
                {
                    "ord_id": row.get("ordId"),
                    "client_order_id": row.get("clOrdId"),
                    "side": row.get("side"),
                    "pos_side": row.get("posSide"),
                    "size": row.get("sz"),
                    "state": row.get("state"),
                }
                for row in pending_orders[:10]
            ],
        },
        "pending_algo_orders": {
            "count": len(pending_algo_orders),
            "items": [
                {
                    "algo_id": row.get("algoId"),
                    "algo_client_id": row.get("algoClOrdId"),
                    "side": row.get("side"),
                    "pos_side": row.get("posSide"),
                    "size": row.get("sz"),
                    "state": row.get("state"),
                    "sl_trigger_px": row.get("slTriggerPx"),
                }
                for row in pending_algo_orders[:10]
            ],
        },
        "leverage": {
            "expected": cfg.execution.max_leverage,
            "values": leverage_values,
            "items": [
                {
                    "inst_id": row.get("instId"),
                    "mgn_mode": row.get("mgnMode"),
                    "pos_side": row.get("posSide"),
                    "leverage": row.get("lever"),
                }
                for row in leverage_rows
            ],
        },
        "protection_stop": {
            "needed": protective_stop_needed,
            "ready": protective_stop_ready,
            "expected_side": _expected_stop_side(position.side),
            "expected_pos_side": _expected_position_leg(position.side, cfg.trading.position_mode),
            "matched_count": len(matching_stop_orders),
            "matched_algo_ids": [row.get("algoId") for row in matching_stop_orders],
        },
    }
    payload["executor_state"] = _summarize_executor_state(executor_state)
    if tracked_algo_ids:
        payload["exchange"]["executor_tracking"] = {
            "tracked_attach_algo_client_ids": sorted(tracked_algo_ids),
            "matched_live_algo_client_ids": tracked_live_algo_ids,
        }
    payload["warnings"] = warnings
    return payload


def _build_demo_portfolio_payload(
    *,
    cfg,
    account: AccountSnapshot,
    symbol_states: dict[str, dict[str, object]],
    include_exchange_checks: bool,
    executor_state: dict[str, object] | None = None,
) -> dict[str, object]:
    symbols_payload: dict[str, object] = {}
    warnings: list[str] = []
    active_positions = 0
    actionable_symbols = 0
    ready_symbols = 0
    leverage_ready_count = 0
    stop_ready_count = 0
    size_match_count = 0
    public_factor_ready_count = 0
    regime_counts = {"bull_trend": 0, "bear_trend": 0, "range": 0}
    routed_ready_count = 0
    for symbol, state in symbol_states.items():
        if include_exchange_checks:
            symbol_executor_state = {}
            if isinstance(executor_state, dict):
                per_symbol_state = executor_state.get("symbols")
                if isinstance(per_symbol_state, dict):
                    candidate = per_symbol_state.get(symbol)
                    if isinstance(candidate, dict):
                        symbol_executor_state = candidate
            symbol_payload = _build_demo_reconcile_payload(
                cfg=cfg,
                account=state["account"],
                position=state["position"],
                signal=state["signal"],
                plan=state["plan"],
                state=state,
                executor_state=symbol_executor_state,
            )
        else:
            extra_payload = {
                "instrument": symbol,
                "planning_account": state["planning_account"].to_dict(),
            }
            public_factor_snapshot = state.get("public_factor_snapshot")
            if isinstance(public_factor_snapshot, PublicFactorSnapshot):
                extra_payload["public_factors"] = public_factor_snapshot.to_dict()
            factor_overlay = state.get("factor_overlay")
            if factor_overlay is not None and hasattr(factor_overlay, "to_dict"):
                extra_payload["factor_overlay"] = factor_overlay.to_dict()
            router_decision = state.get("router_decision")
            if isinstance(router_decision, dict):
                extra_payload["router_decision"] = router_decision
            portfolio_risk = state.get("portfolio_risk")
            if portfolio_risk is not None and hasattr(portfolio_risk, "to_dict"):
                extra_payload["portfolio_risk"] = portfolio_risk.to_dict()
            symbol_payload = _demo_state_payload(
                cfg=cfg,
                account=state["account"],
                position=state["position"],
                signal=state["signal"],
                plan=state["plan"],
                extra=extra_payload,
            )
        symbol_payload["instrument"] = symbol
        symbol_payload["planning_account"] = state["planning_account"].to_dict()
        symbols_payload[symbol] = symbol_payload
        router_decision = state.get("router_decision")
        if isinstance(router_decision, dict):
            regime = str(router_decision.get("regime") or "").strip().lower()
            if regime in regime_counts:
                regime_counts[regime] += 1
            if router_decision.get("ready") is True:
                routed_ready_count += 1
        warnings.extend(f"[{symbol}] {item}" for item in symbol_payload.get("warnings", []))
        if state["position"].side != 0 and state["position"].contracts > 0:
            active_positions += 1
        if state["plan"].instructions:
            actionable_symbols += 1
        if state["signal"].ready:
            ready_symbols += 1
        if include_exchange_checks:
            checks = symbol_payload.get("checks") or {}
            if checks.get("leverage_match") is True:
                leverage_ready_count += 1
            if checks.get("protective_stop_ready") is True:
                stop_ready_count += 1
            if checks.get("size_match") is True:
                size_match_count += 1
        public_factors = symbol_payload.get("public_factors") or {}
        if _safe_float(public_factors.get("confidence"), fallback=0.0) > 0:
            public_factor_ready_count += 1

    summary: dict[str, object] = {
        "symbol_count": len(symbol_states),
        "ready_symbol_count": ready_symbols,
        "actionable_symbol_count": actionable_symbols,
        "active_position_symbol_count": active_positions,
        "allocation_mode": "equal_weight",
        "public_factor_ready_symbol_count": public_factor_ready_count,
        "routed_ready_symbol_count": routed_ready_count,
        "bull_trend_symbol_count": regime_counts["bull_trend"],
        "bear_trend_symbol_count": regime_counts["bear_trend"],
        "range_symbol_count": regime_counts["range"],
    }
    if symbol_states:
        any_state = next(iter(symbol_states.values()))
        summary["per_symbol_planning_equity"] = round(any_state["planning_account"].available_equity, 2)
    if include_exchange_checks:
        summary["leverage_ready_symbol_count"] = leverage_ready_count
        summary["protective_stop_ready_symbol_count"] = stop_ready_count
        summary["size_match_symbol_count"] = size_match_count

    return {
        "mode": "portfolio",
        "symbols": list(symbol_states.keys()),
        "account": account.to_dict(),
        "summary": summary,
        "warnings": warnings,
        "symbol_states": symbols_payload,
    }


def _format_decimal_text(value: float) -> str:
    return format(value, "f").rstrip("0").rstrip(".") or "0"


def _build_leverage_alignment_requests(
    cfg,
    leverage_rows: list[dict[str, Any]],
    *,
    inst_id: str | None = None,
) -> list[dict[str, object]]:
    target = cfg.execution.max_leverage
    requests: list[dict[str, object]] = []
    seen: set[tuple[tuple[str, object], ...]] = set()
    instrument_id = inst_id or next(
        (
            str(row.get("instId"))
            for row in leverage_rows
            if row.get("instId")
        ),
        cfg.instrument.symbol,
    )

    pos_sides: list[str | None] = [None]
    if cfg.trading.td_mode == "isolated" and cfg.trading.position_mode == "long_short_mode":
        discovered = [
            str(row.get("posSide"))
            for row in leverage_rows
            if row.get("posSide") not in {None, "", "net"}
        ]
        pos_sides = discovered or ["long", "short"]

    for pos_side in pos_sides:
        relevant_rows = leverage_rows
        if pos_side is not None:
            relevant_rows = [row for row in leverage_rows if str(row.get("posSide") or "") == pos_side]
        current_values = [
            value
            for value in (_safe_float(row.get("lever"), fallback=None) for row in relevant_rows)
            if value is not None
        ]
        if current_values and all(abs(value - target) <= 0.01 for value in current_values):
            continue

        request: dict[str, object] = {"inst_id": instrument_id, "lever": target, "mgn_mode": cfg.trading.td_mode}
        if pos_side is not None:
            request["pos_side"] = pos_side

        signature = tuple(sorted(request.items()))
        if signature in seen:
            continue
        seen.add(signature)
        requests.append(request)

    return requests


def _build_demo_align_leverage_context(cfg) -> dict[str, object]:
    symbols = configured_symbols(cfg)
    executor_state = _load_executor_state(_executor_state_path(cfg))
    portfolio_mode = len(symbols) > 1
    symbol_contexts: dict[str, dict[str, object]] = {}

    if portfolio_mode:
        account, symbol_states = _load_demo_portfolio_state(cfg, symbols)
        before = _build_demo_portfolio_payload(
            cfg=cfg,
            account=account,
            symbol_states=symbol_states,
            include_exchange_checks=True,
            executor_state=executor_state,
        )
        executor_symbols = executor_state.get("symbols") if isinstance(executor_state, dict) else {}
        executor_symbols = executor_symbols if isinstance(executor_symbols, dict) else {}
        for symbol, state in symbol_states.items():
            symbol_executor_state = executor_symbols.get(symbol)
            symbol_executor_state = symbol_executor_state if isinstance(symbol_executor_state, dict) else {}
            leverage_rows = _okx_rows(state.get("leverage_payload"))
            planned_requests = _build_leverage_alignment_requests(cfg, leverage_rows, inst_id=symbol)
            blockers = _leverage_alignment_blockers(cfg, state) if planned_requests else []
            symbol_contexts[symbol] = {
                "account": state["account"],
                "position": state["position"],
                "state": state,
                "executor_state": symbol_executor_state,
                "leverage_rows": leverage_rows,
                "planned_requests": planned_requests,
                "blockers": blockers,
            }
    else:
        account, position, state = _load_demo_state(cfg)
        before = _build_demo_reconcile_payload(
            cfg=cfg,
            account=account,
            position=position,
            signal=state["signal"],
            plan=state["plan"],
            state=state,
            executor_state=executor_state,
        )
        leverage_rows = _okx_rows(state.get("leverage_payload"))
        planned_requests = _build_leverage_alignment_requests(
            cfg,
            leverage_rows,
            inst_id=cfg.instrument.symbol,
        )
        symbol_contexts[cfg.instrument.symbol] = {
            "account": account,
            "position": position,
            "state": state,
            "executor_state": executor_state,
            "leverage_rows": leverage_rows,
            "planned_requests": planned_requests,
            "blockers": _leverage_alignment_blockers(cfg, state) if planned_requests else [],
        }

    planned_requests: list[dict[str, object]] = []
    blockers: list[str] = []
    symbol_plans: dict[str, dict[str, object]] = {}
    for symbol, context in symbol_contexts.items():
        symbol_requests = [dict(item) for item in context["planned_requests"]]
        symbol_blockers = [str(item) for item in context["blockers"]]
        planned_requests.extend(symbol_requests)
        blockers.extend([f"[{symbol}] {item}" for item in symbol_blockers])
        symbol_plans[symbol] = {
            "planned_requests": symbol_requests,
            "blockers": symbol_blockers,
        }

    return {
        "mode": "portfolio" if portfolio_mode else "single",
        "symbols": symbols,
        "before": before,
        "planned_requests": planned_requests,
        "blockers": blockers,
        "symbol_plans": symbol_plans,
        "_symbol_contexts": symbol_contexts,
    }


def _apply_demo_align_leverage(
    cfg,
    *,
    symbol_contexts: dict[str, dict[str, object]],
    rearm_protective_stop: bool,
) -> tuple[dict[str, dict[str, object]], bool]:
    symbol_results: dict[str, dict[str, object]] = {}
    success = True

    for symbol, context in symbol_contexts.items():
        planned_requests = [dict(item) for item in context["planned_requests"]]
        symbol_blockers = [str(item) for item in context["blockers"]]
        leverage_rows = context["leverage_rows"]
        state = context["state"]
        position = context["position"]
        result: dict[str, object] = {
            "planned_requests": planned_requests,
            "blockers": symbol_blockers,
        }

        if not planned_requests:
            result.update({"status": "already_aligned", "applied": False, "already_aligned": True})
            symbol_results[symbol] = result
            continue

        if symbol_blockers and not rearm_protective_stop:
            result.update(
                {
                    "status": "blocked",
                    "applied": False,
                    "already_aligned": False,
                    "hint": (
                        "Re-run with --rearm-protective-stop if you want quant-lab to "
                        "cancel and restore the stop for this symbol."
                    ),
                }
            )
            symbol_results[symbol] = result
            success = False
            continue

        try:
            if symbol_blockers:
                stop_orders = _extract_rearmable_stop_orders(cfg, position, state)
                if not stop_orders:
                    result.update(
                        {
                            "status": "error",
                            "applied": False,
                            "already_aligned": False,
                            "used_stop_rearm": True,
                            "error": "Unable to locate a matching live protective stop order to re-arm.",
                        }
                    )
                    symbol_results[symbol] = result
                    success = False
                    continue
                alignment = _align_demo_leverage_with_stop_rearm(
                    cfg,
                    leverage_rows=leverage_rows,
                    stop_orders=stop_orders,
                )
                result["used_stop_rearm"] = True
            else:
                alignment = _align_demo_leverage(cfg, leverage_rows=leverage_rows)
                result["used_stop_rearm"] = False

            result.update(
                {
                    "status": "aligned",
                    "applied": True,
                    "already_aligned": False,
                    "alignment": alignment,
                }
            )
        except OkxApiError as exc:
            result.update(
                {
                    "status": "error",
                    "applied": False,
                    "already_aligned": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            success = False

        symbol_results[symbol] = result

    return symbol_results, success


def _run_demo_align_leverage_action(
    cfg,
    *,
    apply: bool,
    confirm: str,
    rearm_protective_stop: bool,
    refresh_snapshot: Callable[[], dict[str, Any]] | None = None,
) -> tuple[dict[str, object], bool]:
    context = _build_demo_align_leverage_context(cfg)
    payload: dict[str, object] = {
        "mode": context["mode"],
        "symbols": context["symbols"],
        "target_leverage": _format_decimal_text(cfg.execution.max_leverage),
        "apply_requested": apply,
        "planned_requests": context["planned_requests"],
        "blockers": context["blockers"],
        "symbol_plans": context["symbol_plans"],
        "before": context["before"],
    }

    if not apply:
        return payload, True

    _require_private_credentials(cfg)
    _validate_demo_account_mutation(cfg, confirm)
    symbol_results, success = _apply_demo_align_leverage(
        cfg,
        symbol_contexts=context["_symbol_contexts"],
        rearm_protective_stop=rearm_protective_stop,
    )
    payload["symbol_results"] = symbol_results
    payload["used_stop_rearm"] = any(
        bool(result.get("used_stop_rearm"))
        for result in symbol_results.values()
        if isinstance(result, dict)
    )
    payload["applied"] = success
    if len(symbol_results) == 1:
        only_result = next(iter(symbol_results.values()))
        only_result = only_result if isinstance(only_result, dict) else {}
        for key in ("alignment", "hint", "error"):
            if key in only_result:
                payload[key] = only_result[key]

    refreshed = refresh_snapshot() if refresh_snapshot is not None else None
    if refreshed is None:
        refreshed_context = _build_demo_align_leverage_context(cfg)
        payload["after"] = refreshed_context["before"]
    else:
        payload["after"] = refreshed["reconcile"]
        if "preflight" in refreshed:
            payload["preflight"] = refreshed["preflight"]
    return payload, success


def _validate_demo_account_mutation(cfg, confirm: str) -> None:
    if not cfg.okx.use_demo:
        raise typer.BadParameter("Account mutation is blocked because okx.use_demo=false.")
    if confirm != "OKX_DEMO":
        raise typer.BadParameter("Refusing to mutate OKX demo account. Pass --confirm OKX_DEMO to continue.")


def _align_demo_leverage(cfg, *, leverage_rows: list[dict[str, Any]]) -> dict[str, object]:
    requests = _build_leverage_alignment_requests(cfg, leverage_rows)
    if not requests:
        return {
            "target": cfg.execution.max_leverage,
            "already_aligned": True,
            "request_count": 0,
            "requests": [],
            "responses": [],
        }

    private_client = _build_private_client(cfg)
    try:
        responses: list[dict[str, object]] = []
        for request in requests:
            response = private_client.set_leverage(
                lever=float(request["lever"]),
                mgn_mode=str(request["mgn_mode"]),
                inst_id=str(request["inst_id"]),
                pos_side=str(request["pos_side"]) if request.get("pos_side") else None,
            )
            responses.append(
                {
                    "request": request,
                    "response": response,
                }
            )
    finally:
        private_client.close()

    return {
        "target": cfg.execution.max_leverage,
        "already_aligned": False,
        "request_count": len(requests),
        "requests": requests,
        "responses": responses,
    }


def _leverage_alignment_blockers(cfg, state: dict[str, object]) -> list[str]:
    blockers: list[str] = []
    pending_algo_orders = _okx_rows(state.get("pending_algo_orders_payload"))
    if cfg.trading.td_mode == "cross" and pending_algo_orders:
        blockers.append(
            "OKX blocks cross leverage changes while TP/SL or other algo orders are live. "
            "Cancel and later re-arm the protective stop before retrying."
        )
    return blockers


def _extract_rearmable_stop_orders(
    cfg,
    position: PositionSnapshot,
    state: dict[str, object],
) -> list[dict[str, object]]:
    pending_algo_orders = _okx_rows(state.get("pending_algo_orders_payload"))
    matching_orders = _matching_stop_orders(cfg, position, pending_algo_orders)
    stop_orders: list[dict[str, object]] = []
    for index, row in enumerate(matching_orders, start=1):
        trigger_px = _safe_float(row.get("slTriggerPx"), fallback=None)
        order_px = _safe_float(row.get("slOrdPx"), fallback=-1.0)
        size = _safe_float(row.get("sz"), fallback=None)
        if trigger_px is None or size is None:
            continue
        stop_orders.append(
            {
                "index": index,
                "algo_id": row.get("algoId"),
                "algo_client_id": row.get("algoClOrdId"),
                "inst_id": row.get("instId") or cfg.instrument.symbol,
                "td_mode": row.get("tdMode") or cfg.trading.td_mode,
                "side": row.get("side"),
                "pos_side": row.get("posSide") or _expected_position_leg(position.side, cfg.trading.position_mode),
                "size": size,
                "sl_trigger_px": trigger_px,
                "sl_ord_px": order_px if order_px is not None else -1.0,
                "sl_trigger_px_type": row.get("slTriggerPxType") or cfg.trading.stop_trigger_price_type,
            }
        )
    return stop_orders


def _wait_until_algo_orders_absent(private_client: OkxPrivateClient, *, inst_id: str, algo_ids: set[str]) -> bool:
    if not algo_ids:
        return True
    for _ in range(10):
        payload = private_client.get_pending_algo_orders(inst_id=inst_id, ord_type="conditional")
        rows = _okx_rows(payload)
        live_ids = {str(row.get("algoId")) for row in rows if row.get("algoId")}
        if not (algo_ids & live_ids):
            return True
        time.sleep(0.5)
    return False


def _rearm_stop_orders(private_client: OkxPrivateClient, cfg, stop_orders: list[dict[str, object]]) -> list[dict[str, object]]:
    responses: list[dict[str, object]] = []
    for stop in stop_orders:
        algo_cl_ord_id = _build_client_order_id(f"{cfg.trading.order_tag}rs", int(stop["index"]))
        response = private_client.place_algo_order(
            inst_id=str(stop["inst_id"]),
            td_mode=str(stop["td_mode"]),
            side=str(stop["side"]),
            ord_type="conditional",
            size=float(stop["size"]),
            pos_side=str(stop["pos_side"]) if stop.get("pos_side") else None,
            algo_cl_ord_id=algo_cl_ord_id,
            tag=cfg.trading.order_tag[:16] if cfg.trading.order_tag else None,
            sl_trigger_px=float(stop["sl_trigger_px"]),
            sl_ord_px=float(stop["sl_ord_px"]),
            sl_trigger_px_type=str(stop["sl_trigger_px_type"]),
        )
        responses.append(
            {
                "request": {
                    "inst_id": stop["inst_id"],
                    "td_mode": stop["td_mode"],
                    "side": stop["side"],
                    "pos_side": stop["pos_side"],
                    "size": stop["size"],
                    "sl_trigger_px": stop["sl_trigger_px"],
                    "sl_ord_px": stop["sl_ord_px"],
                    "sl_trigger_px_type": stop["sl_trigger_px_type"],
                    "algo_cl_ord_id": algo_cl_ord_id,
                },
                "response": response,
            }
        )
    return responses


def _align_demo_leverage_with_stop_rearm(
    cfg,
    *,
    leverage_rows: list[dict[str, Any]],
    stop_orders: list[dict[str, object]],
) -> dict[str, object]:
    requests = _build_leverage_alignment_requests(cfg, leverage_rows)
    if not requests:
        return {
            "target": cfg.execution.max_leverage,
            "already_aligned": True,
            "request_count": 0,
            "requests": [],
            "cancel": None,
            "responses": [],
            "rearm": None,
            "verified_stop_absence": True,
        }

    private_client = _build_private_client(cfg)
    try:
        cancel_payload = [
            {
                "algoId": str(stop["algo_id"]),
                "instId": str(stop["inst_id"]),
            }
            for stop in stop_orders
            if stop.get("algo_id")
        ]
        cancel_response = private_client.cancel_algo_orders(cancel_payload) if cancel_payload else None
        verified_stop_absence = _wait_until_algo_orders_absent(
            private_client,
            inst_id=cfg.instrument.symbol,
            algo_ids={str(stop["algo_id"]) for stop in stop_orders if stop.get("algo_id")},
        )
        responses: list[dict[str, object]] = []
        rearm_response = None
        try:
            for request in requests:
                response = private_client.set_leverage(
                    lever=float(request["lever"]),
                    mgn_mode=str(request["mgn_mode"]),
                    inst_id=str(request["inst_id"]),
                    pos_side=str(request["pos_side"]) if request.get("pos_side") else None,
                )
                responses.append(
                    {
                        "request": request,
                        "response": response,
                    }
                )
        finally:
            rearm_response = _rearm_stop_orders(private_client, cfg, stop_orders)
    finally:
        private_client.close()

    return {
        "target": cfg.execution.max_leverage,
        "already_aligned": False,
        "request_count": len(requests),
        "requests": requests,
        "cancel": cancel_response,
        "responses": responses,
        "rearm": rearm_response,
        "verified_stop_absence": verified_stop_absence,
    }


def _demo_submit_message(cfg, signal, plan: OrderPlan, cycle: int, responses: list[dict[str, object]]) -> str:
    return (
        "Demo order submitted\n"
        f"Cycle: {cycle}\n"
        f"Symbol: {cfg.instrument.symbol}\n"
        f"Strategy: {cfg.strategy.name}\n"
        f"Action: {plan.action}\n"
        f"Desired side: {signal.desired_side}\n"
        f"Target contracts: {plan.target_contracts:.4f}\n"
        f"Instructions: {len(plan.instructions)}\n"
        f"Responses: {len(responses)}\n"
        f"Signal time: {signal.signal_time.isoformat()}"
    )


def _demo_error_message(cfg, cycle: int, error: Exception) -> str:
    return (
        "Demo loop cycle failed\n"
        f"Cycle: {cycle}\n"
        f"Symbol: {cfg.instrument.symbol}\n"
        f"Strategy: {cfg.strategy.name}\n"
        f"Error: {type(error).__name__}: {error}"
    )


def _demo_portfolio_submit_message(cfg, cycle: int, symbol_payloads: dict[str, dict[str, object]]) -> str:
    submitted_symbols = [symbol for symbol, payload in symbol_payloads.items() if payload.get("submitted")]
    lines = [
        "Portfolio demo orders submitted",
        f"Cycle: {cycle}",
        f"Symbols: {', '.join(symbol_payloads.keys())}",
        f"Submitted symbols: {', '.join(submitted_symbols) if submitted_symbols else '--'}",
        f"Strategy: {cfg.strategy.name}",
    ]
    for symbol, payload in symbol_payloads.items():
        lines.append(
            (
                f"- {symbol}: action={payload.get('action')} "
                f"desired_side={payload.get('desired_side')} "
                f"target_contracts={_format_decimal_text(float(payload.get('target_contracts') or 0.0))} "
                f"responses={payload.get('response_count')}"
            )
        )
    return "\n".join(lines)


def _demo_portfolio_error_message(cfg, cycle: int, error: Exception, symbols: list[str]) -> str:
    return (
        "Portfolio demo loop cycle failed\n"
        f"Cycle: {cycle}\n"
        f"Symbols: {', '.join(symbols)}\n"
        f"Strategy: {cfg.strategy.name}\n"
        f"Error: {type(error).__name__}: {error}"
    )


def _portfolio_demo_status(statuses: list[str]) -> str:
    if any(status == "submitted" for status in statuses):
        return "submitted"
    if any(status == "warning" for status in statuses):
        return "warning"
    if any(status == "duplicate" for status in statuses):
        return "duplicate"
    if any(status == "plan_only" for status in statuses):
        return "plan_only"
    if statuses and all(status == "idle" for status in statuses):
        return "idle"
    return statuses[0] if statuses else "ok"


def _portfolio_symbol_executor_state(executor_state: dict[str, object], symbol: str) -> dict[str, object]:
    symbols_state = executor_state.get("symbols")
    if not isinstance(symbols_state, dict):
        symbols_state = {}
        executor_state["symbols"] = symbols_state
    symbol_state = symbols_state.get(symbol)
    if not isinstance(symbol_state, dict):
        symbol_state = {}
        symbols_state[symbol] = symbol_state
    return symbol_state


def _run_demo_loop_cycle(
    *,
    cfg,
    session_factory,
    project_root: Path | None = None,
    cycle: int,
    submit: bool,
    state_path: Path,
) -> tuple[dict[str, object], bool]:
    executor_state = _load_executor_state(state_path)
    try:
        execution_approval = resolve_execution_approval(session_factory=session_factory, config=cfg, required_scope="demo")
        if project_root is None:
            account, position, state = _load_demo_state(cfg)
        else:
            account, position, state = _load_demo_state(
                cfg,
                session_factory=session_factory,
                project_root=project_root,
            )
        plan: OrderPlan = state["plan"]
        signal = state["signal"]
        signature = _plan_signature(signal, plan)
        already_submitted = executor_state.get("last_submitted_signature") == signature

        submitted = False
        responses: list[dict[str, object]] = []
        loop_warnings: list[str] = []
        heartbeat_status = "ok"
        alerts_sent: list[str] = []
        router_decision = state.get("router_decision") if isinstance(state, dict) else None

        if submit and not execution_approval["ready"]:
            heartbeat_status = "warning"
            loop_warnings.append(
                "Skip submit because approved candidate gate is not satisfied: "
                + "; ".join(execution_approval.get("reasons") or ["execution approval not ready"])
            )
        elif submit and isinstance(router_decision, dict) and router_decision.get("enabled") and not router_decision.get("ready"):
            heartbeat_status = "warning"
            loop_warnings.append(
                "Skip submit because strategy router did not resolve an executable approved candidate: "
                + "; ".join(router_decision.get("reasons") or ["router decision not ready"])
            )
        elif submit and plan.instructions and not already_submitted:
            if account.account_mode and account.account_mode != cfg.trading.position_mode:
                heartbeat_status = "warning"
                loop_warnings.append(
                    f"Skip submit because account posMode={account.account_mode}, config={cfg.trading.position_mode}."
                )
            else:
                responses = _submit_order_plan(cfg, plan)
                submission_refs = _extract_submission_refs(responses)
                submitted = True
                heartbeat_status = "submitted"
                executor_state.update(
                    {
                        "last_submitted_signature": signature,
                        "last_submitted_at": pd.Timestamp.now(tz="UTC").isoformat(),
                        "last_plan": plan.to_dict(),
                        "last_signal": signal.to_dict(),
                        "last_submit_response_count": len(responses),
                        "last_submission_refs": submission_refs,
                    }
                )
                if cfg.alerts.send_on_demo_submit:
                    alerts_sent = _persist_alert_results(
                        session_factory,
                        cfg=cfg,
                        event_key="demo_order_submitted",
                        level="info",
                        title="Demo order submitted",
                        message=_demo_submit_message(cfg, signal, plan, cycle, responses),
                    )
        elif submit and plan.instructions and already_submitted:
            heartbeat_status = "duplicate"
            loop_warnings.append("Skip submit because this exact plan signature was already submitted.")
        elif submit and not plan.instructions:
            heartbeat_status = "idle"
        elif not submit:
            heartbeat_status = "plan_only"

        if loop_warnings and heartbeat_status == "ok":
            heartbeat_status = "warning"

        payload = {
            "cycle": cycle,
            "submitted": submitted,
            "responses": responses,
            "loop_warnings": loop_warnings,
            "executor_state_path": str(state_path),
            "alerts_sent": alerts_sent,
            "router_decision": state.get("router_decision"),
        }
        _save_executor_state(state_path, executor_state)
        _persist_heartbeat(
            session_factory,
            service_name="quant-lab-demo-loop",
            status=heartbeat_status,
            details={
                "cycle": cycle,
                "symbol": cfg.instrument.symbol,
                "strategy_name": state.get("router_decision", {}).get("selected_strategy_name", cfg.strategy.name),
                "strategy_variant": state.get("router_decision", {}).get("selected_variant", cfg.strategy.variant),
                "action": plan.action,
                "reason": plan.reason,
                "desired_side": signal.desired_side,
                "current_side": position.side,
                "current_contracts": position.contracts,
                "target_contracts": plan.target_contracts,
                "submitted": submitted,
                "response_count": len(responses),
                "warning_count": len(loop_warnings),
                "warnings": loop_warnings,
                "already_submitted": already_submitted,
                "latest_price": signal.latest_price,
                "total_equity": account.total_equity,
                "available_equity": account.available_equity,
                "execution_approval": execution_approval,
                "router_decision": state.get("router_decision"),
                "signal_time": signal.signal_time.isoformat(),
                "effective_time": signal.effective_time.isoformat(),
                "executor_state_path": str(state_path),
            },
        )
        return (
            {
                "account": account,
                "position": position,
                "signal": signal,
                "plan": plan,
                "router_decision": state.get("router_decision"),
                "payload": payload,
            },
            False,
        )
    except Exception as exc:
        if cfg.alerts.send_on_demo_error and _alert_is_due(
            executor_state,
            event_key="demo_loop_error",
            cooldown_seconds=cfg.alerts.demo_error_cooldown_seconds,
        ):
            sent_channels = _persist_alert_results(
                session_factory,
                cfg=cfg,
                event_key="demo_loop_error",
                level="warning",
                title="Demo loop error",
                message=_demo_error_message(cfg, cycle, exc),
            )
            if sent_channels:
                _mark_alert_sent(executor_state, "demo_loop_error")
        executor_state["last_error"] = {
            "cycle": cycle,
            "message": f"{type(exc).__name__}: {exc}",
            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
        }
        _save_executor_state(state_path, executor_state)
        _persist_heartbeat(
            session_factory,
            service_name="quant-lab-demo-loop",
            status="error",
            details={
                "cycle": cycle,
                "symbol": cfg.instrument.symbol,
                "strategy_name": cfg.strategy.name,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
        return (
            {
                "error": f"{type(exc).__name__}: {exc}",
                "cycle": cycle,
                "submitted": False,
                "responses": [],
                "loop_warnings": [],
                "executor_state_path": str(state_path),
            },
            True,
        )


def _run_demo_portfolio_loop_cycle(
    *,
    cfg,
    session_factory,
    project_root: Path | None = None,
    cycle: int,
    submit: bool,
    state_path: Path,
    symbols: list[str],
) -> tuple[dict[str, object], bool]:
    executor_state = _load_executor_state(state_path)
    try:
        execution_approval = resolve_execution_approval(session_factory=session_factory, config=cfg, required_scope="demo")
        if project_root is None:
            account, symbol_states = _load_demo_portfolio_state(cfg, symbols)
        else:
            account, symbol_states = _load_demo_portfolio_state(
                cfg,
                symbols,
                session_factory=session_factory,
                project_root=project_root,
            )
        symbol_payloads: dict[str, dict[str, object]] = {}
        statuses: list[str] = []
        submitted_symbols: list[str] = []
        total_responses = 0
        total_warnings = 0

        for symbol, state in symbol_states.items():
            plan: OrderPlan = state["plan"]
            signal = state["signal"]
            position: PositionSnapshot = state["position"]
            symbol_executor_state = _portfolio_symbol_executor_state(executor_state, symbol)
            signature = _plan_signature(signal, plan)
            already_submitted = symbol_executor_state.get("last_submitted_signature") == signature

            submitted = False
            responses: list[dict[str, object]] = []
            loop_warnings: list[str] = []
            status = "ok"
            router_decision = state.get("router_decision") if isinstance(state, dict) else None

            if submit and not execution_approval["ready"]:
                status = "warning"
                loop_warnings.append(
                    "Skip submit because approved candidate gate is not satisfied: "
                    + "; ".join(execution_approval.get("reasons") or ["execution approval not ready"])
                )
            elif submit and isinstance(router_decision, dict) and router_decision.get("enabled") and not router_decision.get("ready"):
                status = "warning"
                loop_warnings.append(
                    "Skip submit because strategy router did not resolve an executable approved candidate: "
                    + "; ".join(router_decision.get("reasons") or ["router decision not ready"])
                )
            elif submit and plan.instructions and not already_submitted:
                if account.account_mode and account.account_mode != cfg.trading.position_mode:
                    status = "warning"
                    loop_warnings.append(
                        f"Skip submit because account posMode={account.account_mode}, config={cfg.trading.position_mode}."
                    )
                else:
                    responses = _submit_order_plan(cfg, plan)
                    submitted = True
                    status = "submitted"
                    symbol_executor_state.update(
                        {
                            "last_submitted_signature": signature,
                            "last_submitted_at": pd.Timestamp.now(tz="UTC").isoformat(),
                            "last_plan": plan.to_dict(),
                            "last_signal": signal.to_dict(),
                            "last_submit_response_count": len(responses),
                            "last_submission_refs": _extract_submission_refs(responses),
                        }
                    )
            elif submit and plan.instructions and already_submitted:
                status = "duplicate"
                loop_warnings.append("Skip submit because this exact plan signature was already submitted.")
            elif submit and not plan.instructions:
                status = "idle"
            elif not submit:
                status = "plan_only"

            if loop_warnings and status == "ok":
                status = "warning"

            symbol_payload = {
                "symbol": symbol,
                "action": plan.action,
                "reason": plan.reason,
                "desired_side": signal.desired_side,
                "current_side": position.side,
                "current_contracts": position.contracts,
                "target_contracts": plan.target_contracts,
                "submitted": submitted,
                "response_count": len(responses),
                "responses": responses,
                "warning_count": len(loop_warnings),
                "warnings": loop_warnings,
                "already_submitted": already_submitted,
                "latest_price": signal.latest_price,
                "signal_time": signal.signal_time.isoformat(),
                "effective_time": signal.effective_time.isoformat(),
                "planning_equity": state["planning_account"].available_equity,
                "router_decision": state.get("router_decision"),
                "strategy_score": signal.strategy_score,
                "public_factor_score": (
                    state["public_factor_snapshot"].score
                    if isinstance(state.get("public_factor_snapshot"), PublicFactorSnapshot)
                    else None
                ),
                "public_factor_confidence": (
                    state["public_factor_snapshot"].confidence
                    if isinstance(state.get("public_factor_snapshot"), PublicFactorSnapshot)
                    else None
                ),
                "portfolio_risk": (
                    state["portfolio_risk"].to_dict()
                    if state.get("portfolio_risk") is not None and hasattr(state["portfolio_risk"], "to_dict")
                    else None
                ),
                "status": status,
            }
            symbol_payloads[symbol] = symbol_payload
            statuses.append(status)
            total_responses += len(responses)
            total_warnings += len(loop_warnings)
            if submitted:
                submitted_symbols.append(symbol)

        alerts_sent: list[str] = []
        if submitted_symbols and cfg.alerts.send_on_demo_submit:
            alerts_sent = _persist_alert_results(
                session_factory,
                cfg=cfg,
                event_key="demo_order_submitted",
                level="info",
                title="Portfolio demo orders submitted",
                message=_demo_portfolio_submit_message(cfg, cycle, symbol_payloads),
            )

        portfolio_status = _portfolio_demo_status(statuses)
        actionable_symbol_count = sum(
            1 for state in symbol_states.values() if state["plan"].instructions
        )
        active_position_symbol_count = sum(
            1 for state in symbol_states.values() if state["position"].side != 0 and state["position"].contracts > 0
        )
        heartbeat_details = {
            "cycle": cycle,
            "mode": "portfolio",
            "symbols": symbols,
            "symbol_count": len(symbols),
            "submitted_symbol_count": len(submitted_symbols),
            "submitted_symbols": submitted_symbols,
            "actionable_symbol_count": actionable_symbol_count,
            "active_position_symbol_count": active_position_symbol_count,
            "response_count": total_responses,
            "warning_count": total_warnings,
            "total_equity": account.total_equity,
            "available_equity": account.available_equity,
            "execution_approval": execution_approval,
            "strategy_router_enabled": bool(cfg.trading.strategy_router_enabled),
            "symbol_states": symbol_payloads,
            "executor_state_path": str(state_path),
        }
        executor_state["portfolio"] = {
            "last_cycle": cycle,
            "symbols": symbols,
            "status": portfolio_status,
            "submitted_symbols": submitted_symbols,
            "last_error": executor_state.get("last_error"),
        }
        _save_executor_state(state_path, executor_state)
        _persist_heartbeat(
            session_factory,
            service_name="quant-lab-demo-loop",
            status=portfolio_status,
            details=heartbeat_details,
        )
        return (
            {
                "mode": "portfolio",
                "account": account,
                "symbols": symbols,
                "symbol_states": symbol_states,
                "payload": {
                    "cycle": cycle,
                    "submitted_symbols": submitted_symbols,
                    "symbol_payloads": symbol_payloads,
                    "alerts_sent": alerts_sent,
                    "response_count": total_responses,
                    "warning_count": total_warnings,
                    "executor_state_path": str(state_path),
                    "status": portfolio_status,
                },
            },
            False,
        )
    except Exception as exc:
        if cfg.alerts.send_on_demo_error and _alert_is_due(
            executor_state,
            event_key="demo_loop_error",
            cooldown_seconds=cfg.alerts.demo_error_cooldown_seconds,
        ):
            sent_channels = _persist_alert_results(
                session_factory,
                cfg=cfg,
                event_key="demo_loop_error",
                level="warning",
                title="Portfolio demo loop error",
                message=_demo_portfolio_error_message(cfg, cycle, exc, symbols),
            )
            if sent_channels:
                _mark_alert_sent(executor_state, "demo_loop_error")
        executor_state["last_error"] = {
            "cycle": cycle,
            "message": f"{type(exc).__name__}: {exc}",
            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
            "symbols": symbols,
        }
        _save_executor_state(state_path, executor_state)
        _persist_heartbeat(
            session_factory,
            service_name="quant-lab-demo-loop",
            status="error",
            details={
                "cycle": cycle,
                "mode": "portfolio",
                "symbols": symbols,
                "strategy_name": cfg.strategy.name,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
        return (
            {
                "mode": "portfolio",
                "error": f"{type(exc).__name__}: {exc}",
                "cycle": cycle,
                "symbols": symbols,
                "submitted_symbols": [],
                "response_count": 0,
                "warning_count": 0,
                "executor_state_path": str(state_path),
            },
            True,
        )


def _validate_submit_permissions(cfg, session_factory, confirm: str) -> None:
    if not cfg.okx.use_demo:
        raise typer.BadParameter("Order submission is blocked because okx.use_demo=false.")
    if not cfg.trading.allow_order_placement:
        raise typer.BadParameter(
            "Order submission is disabled. Set trading.allow_order_placement=true or QUANT_LAB_ALLOW_ORDER_PLACEMENT=true."
        )
    execution_approval = resolve_execution_approval(session_factory=session_factory, config=cfg, required_scope="demo")
    if not execution_approval["ready"]:
        reasons = "; ".join(execution_approval.get("reasons") or ["execution approval not ready"])
        raise typer.BadParameter(
            "Order submission is blocked by the approved-candidate gate. "
            f"Reasons: {reasons}"
        )
    if confirm != "OKX_DEMO":
        raise typer.BadParameter("Refusing to submit orders. Pass --confirm OKX_DEMO to continue.")


def _submit_order_plan(cfg, plan: OrderPlan) -> list[dict[str, object]]:
    private_client = _build_private_client(cfg)
    try:
        responses: list[dict[str, object]] = []
        tag = cfg.trading.order_tag[:16] if cfg.trading.order_tag else None
        for index, instruction in enumerate(plan.instructions, start=1):
            attach_algo_ords = [item.to_request_payload() for item in instruction.attach_algo_orders]
            for algo_index, algo_payload in enumerate(attach_algo_ords, start=1):
                algo_payload.setdefault(
                    "attachAlgoClOrdId",
                    _build_client_order_id(f"{cfg.trading.order_tag}sl", (index * 10) + algo_index),
                )

            cl_ord_id = _build_client_order_id(cfg.trading.order_tag, index)
            response = private_client.place_order(
                inst_id=instruction.inst_id,
                td_mode=instruction.td_mode,
                side=instruction.side,
                ord_type=instruction.ord_type,
                size=instruction.size,
                pos_side=instruction.pos_side,
                reduce_only=instruction.reduce_only,
                cl_ord_id=cl_ord_id,
                tag=tag,
                attach_algo_ords=attach_algo_ords or None,
            )
            request_payload = instruction.to_request_payload(client_order_id=cl_ord_id, tag=tag)
            if attach_algo_ords:
                request_payload["attachAlgoOrds"] = attach_algo_ords
            responses.append(
                {
                    "purpose": instruction.purpose,
                    "request": request_payload,
                    "response": response,
                }
            )
        return responses
    finally:
        private_client.close()


@app.command()
def download(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    start: str = typer.Option(..., help="UTC start date, e.g. 2023-01-01"),
    end: str = typer.Option(..., help="UTC end date, e.g. 2026-03-01"),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    symbols: str | None = typer.Option(
        None,
        help="Optional comma-separated symbols. Defaults to portfolio.symbols or instrument.symbol from config.",
    ),
) -> None:
    cfg = load_config(config)
    storage = cfg.storage.resolved(project_root.resolve())
    ensure_storage_dirs(storage)

    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC")
    resolved_symbols = _resolve_symbols(cfg, symbols)

    typer.echo(
        f"Downloading OKX data for {_symbol_list_label(resolved_symbols)} "
        f"from {start_ts.date()} to {end_ts.date()}..."
    )

    client = OkxPublicClient(base_url=cfg.okx.rest_base_url, proxy_url=cfg.okx.proxy_url)
    try:
        for symbol in resolved_symbols:
            symbol_slug = _symbol_slug(symbol)
            instrument = client.fetch_instrument_details(
                inst_type=cfg.instrument.instrument_type,
                inst_id=symbol,
            )
            signal_bars = client.fetch_history_candles(
                inst_id=symbol,
                bar=cfg.strategy.signal_bar,
                start=start_ts,
                end=end_ts,
            )
            execution_bars = client.fetch_history_candles(
                inst_id=symbol,
                bar=cfg.strategy.execution_bar,
                start=start_ts,
                end=end_ts,
            )
            funding = client.fetch_funding_rate_history(
                inst_id=symbol,
                start=start_ts,
                end=end_ts,
            )

            signal_path = storage.raw_dir / f"{symbol_slug}_{cfg.strategy.signal_bar}.parquet"
            execution_path = storage.raw_dir / f"{symbol_slug}_{cfg.strategy.execution_bar}.parquet"
            funding_path = storage.raw_dir / f"{symbol_slug}_funding.parquet"
            metadata_path = _instrument_metadata_path(storage, symbol)

            signal_bars.to_parquet(signal_path, index=False)
            execution_bars.to_parquet(execution_path, index=False)
            funding.to_parquet(funding_path, index=False)
            metadata_path.write_text(json.dumps(instrument, ensure_ascii=False, indent=2), encoding="utf-8")

            typer.echo(f"[{symbol}] signal bars: {len(signal_bars)} -> {signal_path}")
            typer.echo(f"[{symbol}] execution bars: {len(execution_bars)} -> {execution_path}")
            typer.echo(f"[{symbol}] funding rows: {len(funding)} -> {funding_path}")
            typer.echo(f"[{symbol}] instrument metadata -> {metadata_path}")
            if funding.empty:
                typer.echo(
                    f"[{symbol}] Warning: no funding history was returned for this date range. "
                    "Backtests over this range will apply a conservative adverse funding fallback."
                )
    finally:
        client.close()


@app.command("download-public-factors")
def download_public_factors(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    start: str = typer.Option(..., help="UTC start date, e.g. 2023-01-01"),
    end: str = typer.Option(..., help="UTC end date, e.g. 2026-03-01"),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    symbols: str | None = typer.Option(
        None,
        help="Optional comma-separated symbols. Defaults to portfolio.symbols or instrument.symbol from config.",
    ),
    trade_pages: int = typer.Option(5, min=1, max=100, help="Number of history-trades pages to fetch per symbol."),
    trade_limit: int = typer.Option(100, min=1, max=100, help="Rows per history-trades page."),
    book_depth: int = typer.Option(50, min=1, max=5000, help="books-full depth to request."),
) -> None:
    cfg = load_config(config)
    storage = cfg.storage.resolved(project_root.resolve())
    ensure_storage_dirs(storage)

    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC")
    resolved_symbols = _resolve_symbols(cfg, symbols)

    typer.echo(
        f"Downloading OKX public factor data for {_symbol_list_label(resolved_symbols)} "
        f"from {start_ts.date()} to {end_ts.date()}..."
    )

    client = OkxPublicClient(base_url=cfg.okx.rest_base_url, proxy_url=cfg.okx.proxy_url)
    try:
        for symbol in resolved_symbols:
            symbol_slug = _symbol_slug(symbol)
            index_inst_id = _index_inst_id(symbol)

            open_interest = pd.DataFrame(
                [client.fetch_open_interest(inst_type=cfg.instrument.instrument_type, inst_id=symbol)]
            )
            mark_price = pd.DataFrame(
                [client.fetch_mark_price(inst_type=cfg.instrument.instrument_type, inst_id=symbol)]
            )
            index_ticker = pd.DataFrame([client.fetch_index_ticker(index_inst_id=index_inst_id)])
            trades = client.fetch_history_trades(
                inst_id=symbol,
                limit=trade_limit,
                max_pages=trade_pages,
            )
            books_full = client.fetch_books_full_snapshot(inst_id=symbol, depth=book_depth)
            books_full_summary = _books_full_summary(books_full)
            mark_candles = client.fetch_history_mark_price_candles(
                inst_id=symbol,
                bar=cfg.strategy.signal_bar,
                start=start_ts,
                end=end_ts,
            )
            index_candles = client.fetch_history_index_candles(
                index_inst_id=index_inst_id,
                bar=cfg.strategy.signal_bar,
                start=start_ts,
                end=end_ts,
            )

            open_interest_path = storage.raw_dir / f"{symbol_slug}_open_interest.parquet"
            mark_price_path = storage.raw_dir / f"{symbol_slug}_mark_price.parquet"
            index_ticker_path = storage.raw_dir / f"{symbol_slug}_index_ticker.parquet"
            trades_path = storage.raw_dir / f"{symbol_slug}_history_trades.parquet"
            books_full_summary_path = storage.raw_dir / f"{symbol_slug}_books_full_summary.parquet"
            books_full_snapshot_path = storage.raw_dir / f"{symbol_slug}_books_full_latest.json"
            mark_candles_path = storage.raw_dir / f"{symbol_slug}_mark_price_{cfg.strategy.signal_bar}.parquet"
            index_candles_path = storage.raw_dir / f"{symbol_slug}_index_{cfg.strategy.signal_bar}.parquet"

            merged_open_interest = _merge_deduped_frame(
                _read_parquet_if_exists(open_interest_path),
                open_interest,
                subset=["timestamp", "symbol"],
            )
            merged_mark_price = _merge_deduped_frame(
                _read_parquet_if_exists(mark_price_path),
                mark_price,
                subset=["timestamp", "symbol"],
            )
            merged_index_ticker = _merge_deduped_frame(
                _read_parquet_if_exists(index_ticker_path),
                index_ticker,
                subset=["timestamp", "index_inst_id"],
            )
            merged_trades = _merge_deduped_frame(
                _read_parquet_if_exists(trades_path),
                trades,
                subset=["symbol", "trade_id"],
            )
            merged_books_full_summary = _merge_deduped_frame(
                _read_parquet_if_exists(books_full_summary_path),
                books_full_summary,
                subset=["timestamp", "symbol"],
            )
            merged_mark_candles = _merge_deduped_frame(
                _read_parquet_if_exists(mark_candles_path),
                mark_candles,
                subset=["timestamp"],
            )
            merged_index_candles = _merge_deduped_frame(
                _read_parquet_if_exists(index_candles_path),
                index_candles,
                subset=["timestamp"],
            )

            merged_open_interest.to_parquet(open_interest_path, index=False)
            merged_mark_price.to_parquet(mark_price_path, index=False)
            merged_index_ticker.to_parquet(index_ticker_path, index=False)
            merged_trades.to_parquet(trades_path, index=False)
            merged_books_full_summary.to_parquet(books_full_summary_path, index=False)
            merged_mark_candles.to_parquet(mark_candles_path, index=False)
            merged_index_candles.to_parquet(index_candles_path, index=False)
            books_full_snapshot_path.write_text(
                json.dumps(_json_ready(books_full), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            typer.echo(f"[{symbol}] open interest rows: {len(merged_open_interest)} -> {open_interest_path}")
            typer.echo(f"[{symbol}] mark price rows: {len(merged_mark_price)} -> {mark_price_path}")
            typer.echo(f"[{symbol}] index ticker rows: {len(merged_index_ticker)} -> {index_ticker_path}")
            typer.echo(f"[{symbol}] history trades rows: {len(merged_trades)} -> {trades_path}")
            typer.echo(
                f"[{symbol}] books-full summaries: {len(merged_books_full_summary)} -> {books_full_summary_path}"
            )
            typer.echo(f"[{symbol}] books-full latest snapshot -> {books_full_snapshot_path}")
            typer.echo(f"[{symbol}] mark price candles: {len(merged_mark_candles)} -> {mark_candles_path}")
            typer.echo(f"[{symbol}] index candles: {len(merged_index_candles)} -> {index_candles_path}")
    finally:
        client.close()


@app.command()
def backtest(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    symbols: str | None = typer.Option(
        None,
        help="Optional comma-separated symbols. Defaults to portfolio.symbols or instrument.symbol from config.",
    ),
) -> None:
    cfg = _load_app_context(config=config, project_root=project_root)
    resolved_symbols = _resolve_symbols(cfg, symbols)

    if len(resolved_symbols) == 1:
        cfg, storage, signal_bars, execution_bars, funding, symbol_slug = _load_symbol_report_inputs(
            cfg=cfg,
            project_root=project_root,
            symbol=resolved_symbols[0],
        )
        instrument_config = _resolve_instrument_config(cfg, storage, resolved_symbols[0])
        if funding.empty:
            typer.echo(
                "Warning: funding dataset is empty for this run. "
                "Missing funding timestamps will be charged with a conservative adverse fallback."
            )

        artifacts = run_backtest(
            signal_bars=signal_bars,
            execution_bars=execution_bars,
            funding_rates=funding,
            strategy_config=cfg.strategy,
            execution_config=cfg.execution,
            risk_config=cfg.risk,
            instrument_config=instrument_config,
        )
        summary = build_summary(
            equity_curve=artifacts.equity_curve,
            trades=artifacts.trades,
            initial_equity=cfg.execution.initial_equity,
        )

        report_prefix = f"{symbol_slug}_{cfg.strategy.name}"
        trades_path, equity_path, summary_path = _write_backtest_artifacts(
            storage=storage,
            report_prefix=report_prefix,
            trades_frame=_trades_frame(artifacts.trades),
            equity_curve=artifacts.equity_curve,
            summary=summary,
        )

        typer.echo("Backtest complete.")
        typer.echo(json.dumps(summary, ensure_ascii=False, indent=2))
        typer.echo(f"Trades: {trades_path}")
        typer.echo(f"Equity curve: {equity_path}")
        typer.echo(f"Summary: {summary_path}")
        return

    typer.echo(
        f"Running portfolio backtest for {len(resolved_symbols)} symbols: {_symbol_list_label(resolved_symbols)}"
    )
    storage = cfg.storage
    per_symbol_initial_equity = cfg.execution.initial_equity / len(resolved_symbols)
    equity_curves_by_symbol: dict[str, pd.DataFrame] = {}
    trades_by_symbol: dict[str, list[TradeRecord]] = {}
    all_trades: list[TradeRecord] = []
    sleeve_summaries: list[dict[str, object]] = []

    for symbol in resolved_symbols:
        _cfg, _storage, signal_bars, execution_bars, funding, symbol_slug = _load_symbol_report_inputs(
            cfg=cfg,
            project_root=project_root,
            symbol=symbol,
        )
        instrument_config = _resolve_instrument_config(cfg, storage, symbol)
        execution_config = cfg.execution.model_copy(update={"initial_equity": per_symbol_initial_equity})

        if funding.empty:
            typer.echo(
                f"[{symbol}] Warning: funding dataset is empty for this run. "
                "Missing funding timestamps will be charged with a conservative adverse fallback."
            )

        artifacts = run_backtest(
            signal_bars=signal_bars,
            execution_bars=execution_bars,
            funding_rates=funding,
            strategy_config=cfg.strategy,
            execution_config=execution_config,
            risk_config=cfg.risk,
            instrument_config=instrument_config,
        )
        summary = build_summary(
            equity_curve=artifacts.equity_curve,
            trades=artifacts.trades,
            initial_equity=per_symbol_initial_equity,
        )
        summary["symbol"] = symbol
        summary["capital_allocation_pct"] = round(100 / len(resolved_symbols), 2)
        sleeve_summaries.append(summary)

        report_prefix = f"{symbol_slug}_{cfg.strategy.name}_sleeve"
        trades_path, equity_path, summary_path = _write_backtest_artifacts(
            storage=storage,
            report_prefix=report_prefix,
            trades_frame=_trades_frame(artifacts.trades),
            equity_curve=artifacts.equity_curve,
            summary=summary,
        )
        typer.echo(f"[{symbol}] trades: {trades_path}")
        typer.echo(f"[{symbol}] equity: {equity_path}")
        typer.echo(f"[{symbol}] summary: {summary_path}")

        equity_curves_by_symbol[symbol] = artifacts.equity_curve
        trades_by_symbol[symbol] = artifacts.trades
        all_trades.extend(artifacts.trades)

    portfolio_equity = combine_portfolio_equity_curves(equity_curves_by_symbol)
    portfolio_trades = build_portfolio_trade_frame(trades_by_symbol)
    portfolio_summary = build_portfolio_summary(
        equity_curve=portfolio_equity,
        trades=all_trades,
        initial_equity=cfg.execution.initial_equity,
        symbols=resolved_symbols,
    )
    portfolio_summary["allocation_mode"] = "equal_weight"
    portfolio_summary["per_symbol_initial_equity"] = round(per_symbol_initial_equity, 2)

    portfolio_prefix = _portfolio_report_prefix(resolved_symbols, cfg.strategy.name)
    trades_path, equity_path, summary_path = _write_backtest_artifacts(
        storage=storage,
        report_prefix=portfolio_prefix,
        trades_frame=portfolio_trades,
        equity_curve=portfolio_equity,
        summary=portfolio_summary,
    )
    sleeves_path = storage.report_dir / f"{portfolio_prefix}_sleeves.csv"
    pd.DataFrame(sleeve_summaries).to_csv(sleeves_path, index=False)

    typer.echo("Portfolio backtest complete.")
    typer.echo(json.dumps(portfolio_summary, ensure_ascii=False, indent=2))
    typer.echo(f"Portfolio trades: {trades_path}")
    typer.echo(f"Portfolio equity curve: {equity_path}")
    typer.echo(f"Portfolio summary: {summary_path}")
    typer.echo(f"Portfolio sleeves: {sleeves_path}")


@app.command("sync-instrument")
def sync_instrument(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    dry_run: bool = typer.Option(False, help="Show the exchange metadata without writing the config."),
) -> None:
    cfg = load_config(config)
    client = OkxPublicClient(base_url=cfg.okx.rest_base_url)
    try:
        instrument = client.fetch_instrument_details(
            inst_type=cfg.instrument.instrument_type,
            inst_id=cfg.instrument.symbol,
        )
    finally:
        client.close()

    typer.echo(json.dumps(instrument, ensure_ascii=False, indent=2))
    if dry_run:
        return

    update_instrument_section(
        config_path=config,
        instrument_data={
            "symbol": instrument["symbol"],
            "instrument_type": instrument["instrument_type"],
            "contract_value": instrument["contract_value"],
            "contract_value_currency": instrument["contract_value_currency"],
            "lot_size": instrument["lot_size"],
            "min_size": instrument["min_size"],
            "tick_size": instrument["tick_size"],
            "settle_currency": instrument["settle_currency"],
        },
    )
    typer.echo(f"Updated instrument settings in {config}")


@app.command("sweep")
def sweep(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    fast: str = typer.Option("10,20,30", help="Comma-separated fast EMA values."),
    slow: str = typer.Option("50,80,120", help="Comma-separated slow EMA values."),
    atr: str = typer.Option("1.5,2.0,2.5", help="Comma-separated ATR stop multiples."),
) -> None:
    cfg, storage, signal_bars, execution_bars, funding, symbol_slug = _load_report_inputs(
        config=config,
        project_root=project_root,
    )
    if funding.empty:
        typer.echo(
            "Warning: funding dataset is empty for this sweep. "
            "Missing funding timestamps will be charged with a conservative adverse fallback."
        )

    fast_values = _parse_int_list(fast)
    slow_values = _parse_int_list(slow)
    atr_values = _parse_float_list(atr)

    typer.echo(
        f"Running sweep for {cfg.instrument.symbol}: "
        f"{len(fast_values)} fast x {len(slow_values)} slow x {len(atr_values)} atr values..."
    )
    results = run_parameter_sweep(
        signal_bars=signal_bars,
        execution_bars=execution_bars,
        funding_rates=funding,
        strategy_config=cfg.strategy,
        execution_config=cfg.execution,
        risk_config=cfg.risk,
        instrument_config=cfg.instrument,
        fast_values=fast_values,
        slow_values=slow_values,
        atr_values=atr_values,
    )
    if results.empty:
        raise typer.BadParameter("No valid parameter combinations were produced. Check the EMA ranges.")

    report_prefix = f"{symbol_slug}_{cfg.strategy.name}"
    results_path = storage.report_dir / f"{report_prefix}_sweep.csv"
    dashboard_path = storage.report_dir / f"{report_prefix}_sweep_dashboard.html"
    results.to_csv(results_path, index=False)
    render_sweep_dashboard(
        results=results,
        output_path=dashboard_path,
        title=f"{cfg.instrument.symbol} {cfg.strategy.name}",
    )

    typer.echo(f"Sweep results: {results_path}")
    typer.echo(f"Sweep dashboard: {dashboard_path}")
    typer.echo("Top 5 combinations:")
    typer.echo(
        results.head(5)[
            [
                "fast_ema",
                "slow_ema",
                "atr_stop_multiple",
                "total_return_pct",
                "max_drawdown_pct",
                "sharpe",
                "trade_count",
            ]
        ].to_string(index=False)
    )


@app.command("research-trend")
def research_trend(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    variants: str = typer.Option(
        "breakout_retest,breakout_retest_regime,breakout_retest_adx,breakout_retest_regime_adx",
        help="Comma-separated strategy variants.",
    ),
    fast: str = typer.Option("8,12,16", help="Comma-separated fast EMA or pullback EMA values."),
    slow: str = typer.Option("24,36,48,72", help="Comma-separated slow EMA or breakout window values."),
    atr: str = typer.Option("2.5,3.0,3.5", help="Comma-separated ATR stop multiples."),
    trend_ema: str = typer.Option("200", help="Comma-separated long-term EMA values for regime filters."),
    adx: str = typer.Option("20,25", help="Comma-separated ADX thresholds for trend filters."),
    output_prefix: str | None = typer.Option(None, help="Optional custom report prefix."),
) -> None:
    cfg, storage, signal_bars, execution_bars, funding, symbol_slug = _load_report_inputs(
        config=config,
        project_root=project_root,
    )
    if funding.empty:
        typer.echo(
            "Warning: funding dataset is empty for this research run. "
            "Missing funding timestamps will be charged with a conservative adverse fallback."
        )

    variant_values = _parse_text_list(variants)
    fast_values = _parse_int_list(fast)
    slow_values = _parse_int_list(slow)
    atr_values = _parse_float_list(atr)
    trend_ema_values = _parse_int_list(trend_ema)
    adx_values = _parse_float_list(adx)

    typer.echo(
        f"Running trend research for {cfg.instrument.symbol}: "
        f"{len(variant_values)} variants, {len(fast_values)} fast, {len(slow_values)} slow, "
        f"{len(atr_values)} atr, {len(trend_ema_values)} trend EMA, {len(adx_values)} adx thresholds..."
    )

    results = run_trend_research(
        signal_bars=signal_bars,
        execution_bars=execution_bars,
        funding_rates=funding,
        strategy_config=cfg.strategy,
        execution_config=cfg.execution,
        risk_config=cfg.risk,
        instrument_config=cfg.instrument,
        variants=variant_values,
        fast_values=fast_values,
        slow_values=slow_values,
        atr_values=atr_values,
        trend_ema_values=trend_ema_values,
        adx_threshold_values=adx_values,
    )
    if results.empty:
        raise typer.BadParameter("No valid research combinations were produced.")

    report_prefix = output_prefix or f"{symbol_slug}_{cfg.strategy.name}_trend_research"
    results_path = storage.report_dir / f"{report_prefix}.csv"
    dashboard_path = storage.report_dir / f"{report_prefix}.html"

    results.to_csv(results_path, index=False)
    render_trend_research_dashboard(
        results=results,
        output_path=dashboard_path,
        title=f"{cfg.instrument.symbol} {cfg.strategy.name} {cfg.strategy.signal_bar} Trend Research",
    )

    typer.echo(f"Research results: {results_path}")
    typer.echo(f"Research dashboard: {dashboard_path}")
    typer.echo("Top 10 candidates:")
    typer.echo(
        results.head(10)[
            [
                "variant",
                "fast_ema",
                "slow_ema",
                "atr_stop_multiple",
                "trend_ema",
                "adx_threshold",
                "total_return_pct",
                "bear_return_pct",
                "max_drawdown_pct",
                "sharpe",
                "research_score",
            ]
        ].to_string(index=False)
    )


@app.command("report")
def report(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    output: Path | None = typer.Option(None, dir_okay=False, help="Optional custom HTML output path."),
    symbols: str | None = typer.Option(
        None,
        help="Optional comma-separated symbols. Defaults to portfolio.symbols or instrument.symbol from config.",
    ),
) -> None:
    cfg = _load_app_context(config=config, project_root=project_root)
    storage = cfg.storage
    resolved_symbols = _resolve_symbols(cfg, symbols)

    if len(resolved_symbols) == 1:
        symbol_slug = _symbol_slug(resolved_symbols[0])
        report_prefix = f"{symbol_slug}_{cfg.strategy.name}"
        trades_path = storage.report_dir / f"{report_prefix}_trades.csv"
        equity_path = storage.report_dir / f"{report_prefix}_equity_curve.csv"
        summary_path = storage.report_dir / f"{report_prefix}_summary.json"
        output_path = output or (storage.report_dir / f"{report_prefix}_dashboard.html")

        for path in (summary_path, equity_path, trades_path):
            if not path.exists():
                raise typer.BadParameter(f"Missing required report artifact: {path}")

        render_dashboard(
            summary_path=summary_path,
            equity_curve_path=equity_path,
            trades_path=trades_path,
            output_path=output_path,
            title=f"{resolved_symbols[0]} {cfg.strategy.name}",
        )
        typer.echo(f"Dashboard: {output_path}")
        return

    dashboard_paths: list[Path] = []
    for symbol in resolved_symbols:
        symbol_slug = _symbol_slug(symbol)
        report_prefix = f"{symbol_slug}_{cfg.strategy.name}_sleeve"
        trades_path = storage.report_dir / f"{report_prefix}_trades.csv"
        equity_path = storage.report_dir / f"{report_prefix}_equity_curve.csv"
        summary_path = storage.report_dir / f"{report_prefix}_summary.json"
        output_path = storage.report_dir / f"{report_prefix}_dashboard.html"

        for path in (summary_path, equity_path, trades_path):
            if not path.exists():
                raise typer.BadParameter(f"Missing required report artifact: {path}")

        render_dashboard(
            summary_path=summary_path,
            equity_curve_path=equity_path,
            trades_path=trades_path,
            output_path=output_path,
            title=f"{symbol} {cfg.strategy.name} Sleeve",
        )
        dashboard_paths.append(output_path)
        typer.echo(f"[{symbol}] dashboard: {output_path}")

    portfolio_prefix = _portfolio_report_prefix(resolved_symbols, cfg.strategy.name)
    trades_path = storage.report_dir / f"{portfolio_prefix}_trades.csv"
    equity_path = storage.report_dir / f"{portfolio_prefix}_equity_curve.csv"
    summary_path = storage.report_dir / f"{portfolio_prefix}_summary.json"
    output_path = output or (storage.report_dir / f"{portfolio_prefix}_dashboard.html")

    for path in (summary_path, equity_path, trades_path):
        if not path.exists():
            raise typer.BadParameter(f"Missing required portfolio artifact: {path}")

    render_dashboard(
        summary_path=summary_path,
        equity_curve_path=equity_path,
        trades_path=trades_path,
        output_path=output_path,
        title=f"Portfolio {_symbol_list_label(resolved_symbols)} {cfg.strategy.name}",
    )
    dashboard_paths.append(output_path)

    typer.echo("Rendered dashboards:")
    for path in dashboard_paths:
        typer.echo(str(path))


@app.command("research-create-task")
def research_create_task(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    title: str = typer.Option(..., help="Research task title."),
    hypothesis: str = typer.Option("", help="Core research hypothesis."),
    owner_role: str = typer.Option("research_lead", help="Owner role for the research task."),
    priority: str = typer.Option("high", help="Task priority label."),
    symbols: str | None = typer.Option(
        None,
        help="Optional comma-separated symbols. Defaults to configured symbols from the config.",
    ),
    notes: str = typer.Option("", help="Free-form research notes."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    task = create_research_task(
        session_factory=session_factory,
        title=title,
        hypothesis=hypothesis,
        owner_role=owner_role,
        priority=priority,
        symbols=_resolve_symbols(cfg, symbols),
        notes=notes,
    )
    typer.echo(json.dumps(serialize_research_task(task), ensure_ascii=False, indent=2))


@app.command("research-list-tasks")
def research_list_tasks(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    status: str | None = typer.Option(None, help="Optional task status filter."),
    limit: int = typer.Option(20, min=1, max=200, help="Max number of tasks to return."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    payload = [serialize_research_task(item) for item in list_research_tasks(session_factory=session_factory, limit=limit, status=status)]
    typer.echo(json.dumps({"tasks": payload}, ensure_ascii=False, indent=2))


@app.command("research-register-candidate")
def research_register_candidate(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    name: str = typer.Option(..., help="Candidate strategy name used in the research registry."),
    task_id: int | None = typer.Option(None, help="Optional linked research task id."),
    strategy_name: str | None = typer.Option(None, help="Implementation strategy name. Defaults to config.strategy.name."),
    variant: str | None = typer.Option(None, help="Implementation variant. Defaults to config.strategy.variant."),
    timeframe: str | None = typer.Option(None, help="Signal timeframe. Defaults to config.strategy.signal_bar."),
    symbols: str | None = typer.Option(
        None,
        help="Optional comma-separated symbols. Defaults to configured symbols from the config.",
    ),
    config_path: Path | None = typer.Option(None, exists=False, dir_okay=False, help="Optional config file path recorded on the candidate."),
    author_role: str = typer.Option("strategy_builder", help="Research role registering the candidate."),
    thesis: str = typer.Option("", help="One-paragraph strategy thesis."),
    tags: str | None = typer.Option(None, help="Optional comma-separated tags."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    candidate = register_strategy_candidate(
        session_factory=session_factory,
        candidate_name=name,
        task_id=task_id,
        strategy_name=strategy_name or cfg.strategy.name,
        variant=variant or cfg.strategy.variant,
        timeframe=timeframe or cfg.strategy.signal_bar,
        symbol_scope=_resolve_symbols(cfg, symbols),
        config_path=str(config_path) if config_path else str(config),
        author_role=author_role,
        thesis=thesis,
        tags=_parse_text_list(tags) if tags else [],
        details={
            "project_root": str(project_root.resolve()),
            "config_path": str((config_path or config).resolve()),
        },
    )
    typer.echo(json.dumps(serialize_strategy_candidate(candidate), ensure_ascii=False, indent=2))


@app.command("research-list-candidates")
def research_list_candidates(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    status: str | None = typer.Option(None, help="Optional candidate status filter."),
    approved_only: bool = typer.Option(False, help="Show only approved strategy candidates."),
    limit: int = typer.Option(20, min=1, max=200, help="Max number of candidates to return."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    payload = [
        serialize_strategy_candidate(item)
        for item in list_strategy_candidates(
            session_factory=session_factory,
            limit=limit,
            status=status,
            approved_only=approved_only,
        )
    ]
    typer.echo(json.dumps({"candidates": payload}, ensure_ascii=False, indent=2))


@app.command("research-evaluate-candidate")
def research_evaluate_candidate(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    candidate_id: int = typer.Option(..., help="Candidate id in the research registry."),
    summary_path: Path | None = typer.Option(
        None,
        exists=False,
        dir_okay=False,
        help="Optional summary artifact path. Defaults to the latest artifacts inferred from the config.",
    ),
    report_path: Path | None = typer.Option(None, exists=False, dir_okay=False, help="Optional report HTML artifact path."),
    trades_path: Path | None = typer.Option(None, exists=False, dir_okay=False, help="Optional trades CSV artifact path."),
    equity_curve_path: Path | None = typer.Option(
        None,
        exists=False,
        dir_okay=False,
        help="Optional equity curve CSV artifact path.",
    ),
    evaluator_role: str = typer.Option("backtest_validator", help="Research role running the evaluation."),
    evaluation_type: str = typer.Option("backtest", help="Evaluation type label."),
    notes: str = typer.Option("", help="Free-form evaluator notes."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    try:
        inferred = infer_strategy_candidate_artifacts_by_id(
            session_factory=session_factory,
            candidate_id=candidate_id,
            project_root=project_root.resolve(),
        )
    except Exception:
        inferred = infer_candidate_artifacts(config=cfg, project_root=project_root.resolve())
    if summary_path is None and not Path(inferred["summary_path"]).exists():
        inferred = infer_candidate_artifacts(config=cfg, project_root=project_root.resolve())
    resolved_summary = summary_path or Path(inferred["summary_path"])
    resolved_report = report_path or Path(inferred["report_path"])
    resolved_trades = trades_path or Path(inferred["trades_path"])
    resolved_equity = equity_curve_path or Path(inferred["equity_curve_path"])
    candidate, report = evaluate_strategy_candidate(
        session_factory=session_factory,
        candidate_id=candidate_id,
        evaluator_role=evaluator_role,
        evaluation_type=evaluation_type,
        summary_path=resolved_summary,
        report_path=resolved_report if resolved_report.exists() else None,
        trades_path=resolved_trades if resolved_trades.exists() else None,
        equity_curve_path=resolved_equity if resolved_equity.exists() else None,
        notes=notes,
    )
    typer.echo(
        json.dumps(
            {
                "candidate": serialize_strategy_candidate(candidate),
                "evaluation_report": serialize_evaluation_report(report),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


@app.command("research-approve-candidate")
def research_approve_candidate(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    candidate_id: int = typer.Option(..., help="Candidate id in the research registry."),
    decision: str = typer.Option(..., help="Approval decision: approve, reject, or watchlist."),
    scope: str = typer.Option("demo", help="Approval scope: research, demo, or live."),
    decider_role: str = typer.Option("risk_officer", help="Research role recording the decision."),
    reason: str = typer.Option("", help="Approval or rejection reason."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    candidate, approval = approve_strategy_candidate(
        session_factory=session_factory,
        candidate_id=candidate_id,
        decision=decision,
        scope=scope,
        decider_role=decider_role,
        reason=reason,
    )
    typer.echo(
        json.dumps(
            {
                "candidate": serialize_strategy_candidate(candidate),
                "approval": serialize_approval_decision(approval),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


@app.command("research-backtest-candidate")
def research_backtest_candidate(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    candidate_id: int = typer.Option(..., help="Candidate id in the research registry."),
    evaluate: bool = typer.Option(
        True,
        help="Run evaluation immediately after backtest artifacts are generated.",
    ),
    build_report: bool = typer.Option(True, help="Render HTML dashboard for the candidate run."),
    evaluator_role: str = typer.Option("backtest_validator", help="Research role recorded on the evaluation."),
    notes: str = typer.Option("", help="Optional evaluation notes."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    if evaluate:
        payload = evaluate_backtested_candidate(
            session_factory=session_factory,
            candidate_id=candidate_id,
            project_root=project_root.resolve(),
            build_report=build_report,
            evaluator_role=evaluator_role,
            evaluation_type="backtest",
            notes=notes,
        )
    else:
        payload = backtest_strategy_candidate(
            session_factory=session_factory,
            candidate_id=candidate_id,
            project_root=project_root.resolve(),
            build_report=build_report,
        )
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@app.command("research-bind-candidate")
def research_bind_candidate(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    candidate_id: int | None = typer.Option(None, help="Candidate id to bind into trading.execution_candidate_id."),
    candidate_name: str | None = typer.Option(
        None,
        help="Candidate name to bind into trading.execution_candidate_name.",
    ),
    require_approved: bool = typer.Option(
        True,
        help="Whether to set trading.require_approved_candidate=true in the config.",
    ),
    clear_other_selector: bool = typer.Option(
        True,
        help="Clear the other selector field to avoid ambiguous binding.",
    ),
) -> None:
    if candidate_id is None and not candidate_name:
        raise typer.BadParameter("Provide either --candidate-id or --candidate-name.")
    if candidate_id is not None and candidate_name:
        raise typer.BadParameter("Use only one selector: --candidate-id or --candidate-name.")

    payload: dict[str, object] = {
        "require_approved_candidate": bool(require_approved),
    }
    if candidate_id is not None:
        payload["execution_candidate_id"] = candidate_id
        if clear_other_selector:
            payload["execution_candidate_name"] = None
    else:
        payload["execution_candidate_name"] = str(candidate_name).strip()
        if clear_other_selector:
            payload["execution_candidate_id"] = None

    update_trading_section(config, payload)
    typer.echo(
        json.dumps(
            {
                "config": str(config),
                "updated_trading": payload,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


@app.command("research-set-route")
def research_set_route(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    route_key: str = typer.Option(
        ...,
        help="Route key such as bull_trend, bear_trend, range, BTC-USDT-SWAP:bull_trend, or default.",
    ),
    candidate_id: int = typer.Option(..., help="Approved candidate id for this route."),
    enable_router: bool = typer.Option(True, help="Whether to set trading.strategy_router_enabled=true."),
) -> None:
    cfg = load_config(config)
    route_map = dict(cfg.trading.execution_candidate_map or {})
    route_map[str(route_key).strip()] = int(candidate_id)
    payload = {
        "strategy_router_enabled": bool(enable_router),
        "execution_candidate_map": route_map,
        "execution_candidate_id": None,
        "execution_candidate_name": None,
    }
    update_trading_section(config, payload)
    typer.echo(json.dumps({"config": str(config), "updated_trading": payload}, ensure_ascii=False, indent=2))


@app.command("research-overview")
def research_overview(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    limit: int = typer.Option(10, min=1, max=100, help="Max number of rows per section."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    typer.echo(
        json.dumps(
            build_research_overview(session_factory=session_factory, limit=limit),
            ensure_ascii=False,
            indent=2,
        )
    )


@app.command("research-ai-status")
def research_ai_status(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    probe: bool = typer.Option(False, help="Send a lightweight probe request to the configured provider."),
) -> None:
    cfg = _load_app_context(config=config, project_root=project_root)
    payload = build_research_ai_status(config=cfg, probe=probe)
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@app.command("research-ai-run")
def research_ai_run(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    task: str = typer.Option(..., help="Research task or prompt sent to the configured AI provider."),
    role: str = typer.Option("research_lead", help="Research role used to select prompt and model defaults."),
    context_json: str | None = typer.Option(
        None,
        help="Optional JSON object containing structured context for the request.",
    ),
    system_prompt: str | None = typer.Option(None, help="Optional system prompt override."),
    temperature: float | None = typer.Option(None, help="Optional sampling temperature override."),
    max_output_tokens: int | None = typer.Option(None, min=1, help="Optional output token cap override."),
) -> None:
    cfg = _load_app_context(config=config, project_root=project_root)
    context: dict[str, Any] = {}
    if context_json:
        try:
            parsed = json.loads(context_json)
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(f"Invalid --context-json payload: {exc}") from exc
        if not isinstance(parsed, dict):
            raise typer.BadParameter("--context-json must decode into a JSON object.")
        context = parsed

    payload = run_research_ai_request(
        config=cfg,
        request=ResearchAIRequest(
            role=role,
            task=task,
            context=context,
            system_prompt=system_prompt,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
        ),
    )
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@app.command("research-materialize-top")
def research_materialize_top(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    results_path: Path | None = typer.Option(
        None,
        exists=False,
        dir_okay=False,
        help="Optional trend research CSV path. Defaults to the latest inferred trend research CSV for the primary symbol.",
    ),
    top_n: int = typer.Option(3, min=1, max=20, help="How many top-ranked rows to convert into strategy candidates."),
    task_id: int | None = typer.Option(None, help="Optional existing research task id to attach the candidates to."),
    task_title: str | None = typer.Option(None, help="Optional title for a new research task when task_id is not provided."),
    owner_role: str = typer.Option("research_lead", help="Owner role for the auto-created research task."),
    author_role: str = typer.Option("strategy_builder", help="Role recorded on the generated candidates."),
    notes: str = typer.Option("", help="Optional notes stored on the auto-created research task."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    storage = cfg.storage.resolved(project_root.resolve())
    symbol_slug = _symbol_slug(cfg.instrument.symbol)
    resolved_results_path = results_path or (storage.report_dir / f"{symbol_slug}_{cfg.strategy.name}_trend_research.csv")
    if not resolved_results_path.exists():
        raise typer.BadParameter(f"Trend research CSV not found: {resolved_results_path}")

    results = pd.read_csv(resolved_results_path)
    payload = materialize_trend_research_candidates(
        session_factory=session_factory,
        config=cfg,
        project_root=project_root,
        base_config_path=config,
        results_frame=results,
        results_path=resolved_results_path,
        top_n=top_n,
        task_id=task_id,
        task_title=task_title,
        owner_role=owner_role,
        author_role=author_role,
        notes=notes,
    )
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@app.command("research-promote-top")
def research_promote_top(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    results_path: Path | None = typer.Option(
        None,
        exists=False,
        dir_okay=False,
        help="Optional trend research CSV path. Defaults to the latest inferred trend research CSV for the primary symbol.",
    ),
    top_n: int = typer.Option(3, min=1, max=20, help="How many top-ranked rows to materialize and evaluate."),
    task_id: int | None = typer.Option(None, help="Optional existing research task id to attach the candidates to."),
    task_title: str | None = typer.Option(None, help="Optional title for a new research task when task_id is not provided."),
    owner_role: str = typer.Option("research_lead", help="Owner role for the auto-created research task."),
    author_role: str = typer.Option("strategy_builder", help="Role recorded on the generated candidates."),
    evaluator_role: str = typer.Option("backtest_validator", help="Role recorded on the automatic evaluations."),
    build_report: bool = typer.Option(True, help="Render HTML dashboard for each promoted candidate."),
    notes: str = typer.Option("", help="Optional notes stored on the auto-created research task."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    storage = cfg.storage.resolved(project_root.resolve())
    symbol_slug = _symbol_slug(cfg.instrument.symbol)
    resolved_results_path = results_path or (storage.report_dir / f"{symbol_slug}_{cfg.strategy.name}_trend_research.csv")
    if not resolved_results_path.exists():
        raise typer.BadParameter(f"Trend research CSV not found: {resolved_results_path}")

    results = pd.read_csv(resolved_results_path)
    payload = promote_trend_research_candidates(
        session_factory=session_factory,
        config=cfg,
        project_root=project_root.resolve(),
        base_config_path=config,
        results_frame=results,
        results_path=resolved_results_path,
        top_n=top_n,
        task_id=task_id,
        task_title=task_title,
        owner_role=owner_role,
        author_role=author_role,
        evaluator_role=evaluator_role,
        notes=notes,
        build_report=build_report,
    )
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@app.command("research-routed-backtest")
def research_routed_backtest(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    symbols: str | None = typer.Option(
        None,
        help="Optional comma-separated symbols. Defaults to portfolio.symbols or instrument.symbol from config.",
    ),
    required_scope: str = typer.Option("demo", help="Required approval scope for routed candidates."),
    output_prefix: str | None = typer.Option(None, help="Optional custom prefix for single-symbol artifacts."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    resolved_symbols = _resolve_symbols(cfg, symbols)

    if len(resolved_symbols) == 1:
        symbol = resolved_symbols[0]
        cfg, storage, signal_bars, execution_bars, funding, symbol_slug = _load_symbol_routed_report_inputs(
            cfg=cfg,
            project_root=project_root,
            symbol=symbol,
        )
        instrument_config = _resolve_instrument_config(cfg, storage, symbol)
        routed = run_routed_backtest(
            session_factory=session_factory,
            config=cfg,
            project_root=project_root.resolve(),
            symbol=symbol,
            signal_bars=signal_bars,
            execution_bars=execution_bars,
            funding_rates=funding,
            execution_config=cfg.execution,
            risk_config=cfg.risk,
            instrument_config=instrument_config,
            required_scope=required_scope,
        )
        summary = _attach_routing_summary(
            build_summary(
                equity_curve=routed.artifacts.equity_curve,
                trades=routed.artifacts.trades,
                initial_equity=cfg.execution.initial_equity,
            ),
            routed.route_summary,
        )
        report_prefix = output_prefix or f"{symbol_slug}_{cfg.strategy.name}_routed"
        trades_path, equity_path, summary_path = _write_backtest_artifacts(
            storage=storage,
            report_prefix=report_prefix,
            trades_frame=_trades_frame(routed.artifacts.trades),
            equity_curve=routed.artifacts.equity_curve,
            summary=summary,
        )
        route_path, route_summary_path = _write_routing_artifacts(
            storage=storage,
            report_prefix=report_prefix,
            route_frame=routed.route_frame,
            route_summary=routed.route_summary,
        )
        dashboard_path = storage.report_dir / f"{report_prefix}_dashboard.html"
        render_dashboard(
            summary_path=summary_path,
            equity_curve_path=equity_path,
            trades_path=trades_path,
            output_path=dashboard_path,
            title=f"{symbol} {cfg.strategy.name} Routed",
        )
        typer.echo("Routed backtest complete.")
        typer.echo(json.dumps(summary, ensure_ascii=False, indent=2))
        typer.echo(f"Trades: {trades_path}")
        typer.echo(f"Equity curve: {equity_path}")
        typer.echo(f"Summary: {summary_path}")
        typer.echo(f"Dashboard: {dashboard_path}")
        typer.echo(f"Routes: {route_path}")
        typer.echo(f"Routing summary: {route_summary_path}")
        return

    typer.echo(
        f"Running portfolio routed backtest for {len(resolved_symbols)} symbols: {_symbol_list_label(resolved_symbols)}"
    )
    storage = cfg.storage
    per_symbol_initial_equity = cfg.execution.initial_equity / len(resolved_symbols)
    equity_curves_by_symbol: dict[str, pd.DataFrame] = {}
    trades_by_symbol: dict[str, list[TradeRecord]] = {}
    all_trades: list[TradeRecord] = []
    sleeve_summaries: list[dict[str, object]] = []
    sleeve_artifacts: list[dict[str, str]] = []
    route_frames: list[pd.DataFrame] = []

    for symbol in resolved_symbols:
        _cfg, _storage, signal_bars, execution_bars, funding, symbol_slug = _load_symbol_routed_report_inputs(
            cfg=cfg,
            project_root=project_root,
            symbol=symbol,
        )
        instrument_config = _resolve_instrument_config(cfg, storage, symbol)
        execution_config = cfg.execution.model_copy(update={"initial_equity": per_symbol_initial_equity})
        routed = run_routed_backtest(
            session_factory=session_factory,
            config=cfg,
            project_root=project_root.resolve(),
            symbol=symbol,
            signal_bars=signal_bars,
            execution_bars=execution_bars,
            funding_rates=funding,
            execution_config=execution_config,
            risk_config=cfg.risk,
            instrument_config=instrument_config,
            required_scope=required_scope,
        )
        summary = _attach_routing_summary(
            build_summary(
                equity_curve=routed.artifacts.equity_curve,
                trades=routed.artifacts.trades,
                initial_equity=per_symbol_initial_equity,
            ),
            routed.route_summary,
        )
        summary["symbol"] = symbol
        summary["capital_allocation_pct"] = round(100 / len(resolved_symbols), 2)
        sleeve_summaries.append(summary)

        report_prefix = f"{symbol_slug}_{cfg.strategy.name}_routed_sleeve"
        trades_path, equity_path, summary_path = _write_backtest_artifacts(
            storage=storage,
            report_prefix=report_prefix,
            trades_frame=_trades_frame(routed.artifacts.trades),
            equity_curve=routed.artifacts.equity_curve,
            summary=summary,
        )
        route_path, route_summary_path = _write_routing_artifacts(
            storage=storage,
            report_prefix=report_prefix,
            route_frame=routed.route_frame,
            route_summary=routed.route_summary,
        )
        dashboard_path = storage.report_dir / f"{report_prefix}_dashboard.html"
        render_dashboard(
            summary_path=summary_path,
            equity_curve_path=equity_path,
            trades_path=trades_path,
            output_path=dashboard_path,
            title=f"{symbol} {cfg.strategy.name} Routed Sleeve",
        )
        sleeve_artifacts.append(
            {
                "symbol": symbol,
                "trades": str(trades_path),
                "equity_curve": str(equity_path),
                "summary": str(summary_path),
                "dashboard": str(dashboard_path),
                "routes": str(route_path),
                "routing_summary": str(route_summary_path),
            }
        )

        equity_curves_by_symbol[symbol] = routed.artifacts.equity_curve
        trades_by_symbol[symbol] = routed.artifacts.trades
        all_trades.extend(routed.artifacts.trades)
        route_frames.append(routed.route_frame)

    portfolio_equity = combine_portfolio_equity_curves(equity_curves_by_symbol)
    portfolio_trades = build_portfolio_trade_frame(trades_by_symbol)
    combined_route_frame = pd.concat(route_frames, ignore_index=True) if route_frames else pd.DataFrame()
    portfolio_route_summary = summarize_route_frame(combined_route_frame)
    portfolio_summary = _attach_routing_summary(
        build_portfolio_summary(
            equity_curve=portfolio_equity,
            trades=all_trades,
            initial_equity=cfg.execution.initial_equity,
            symbols=resolved_symbols,
        ),
        portfolio_route_summary,
    )
    portfolio_summary["allocation_mode"] = "equal_weight"
    portfolio_summary["per_symbol_initial_equity"] = round(per_symbol_initial_equity, 2)

    portfolio_prefix = f"{_portfolio_report_prefix(resolved_symbols, cfg.strategy.name)}_routed"
    trades_path, equity_path, summary_path = _write_backtest_artifacts(
        storage=storage,
        report_prefix=portfolio_prefix,
        trades_frame=portfolio_trades,
        equity_curve=portfolio_equity,
        summary=portfolio_summary,
    )
    route_path, route_summary_path = _write_routing_artifacts(
        storage=storage,
        report_prefix=portfolio_prefix,
        route_frame=combined_route_frame,
        route_summary=portfolio_route_summary,
    )
    sleeves_path = storage.report_dir / f"{portfolio_prefix}_sleeves.csv"
    pd.DataFrame(sleeve_summaries).to_csv(sleeves_path, index=False)
    dashboard_path = storage.report_dir / f"{portfolio_prefix}_dashboard.html"
    render_dashboard(
        summary_path=summary_path,
        equity_curve_path=equity_path,
        trades_path=trades_path,
        output_path=dashboard_path,
        title=f"Portfolio {_symbol_list_label(resolved_symbols)} {cfg.strategy.name} Routed",
    )

    typer.echo("Portfolio routed backtest complete.")
    typer.echo(json.dumps(portfolio_summary, ensure_ascii=False, indent=2))
    typer.echo(f"Portfolio trades: {trades_path}")
    typer.echo(f"Portfolio equity curve: {equity_path}")
    typer.echo(f"Portfolio summary: {summary_path}")
    typer.echo(f"Portfolio dashboard: {dashboard_path}")
    typer.echo(f"Portfolio routes: {route_path}")
    typer.echo(f"Portfolio routing summary: {route_summary_path}")
    typer.echo(f"Portfolio sleeves: {sleeves_path}")
    typer.echo(json.dumps({"sleeves": sleeve_artifacts}, ensure_ascii=False, indent=2))


@app.command("demo-account")
def demo_account(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
) -> None:
    cfg = _load_app_context(config=config, project_root=project_root)
    _require_private_credentials(cfg)

    private_client = _build_private_client(cfg)
    try:
        account_config_payload = private_client.get_account_config()
        balance_payload = private_client.get_balance(ccy=cfg.instrument.settle_currency)
        positions_payload = private_client.get_positions(
            inst_type=cfg.instrument.instrument_type,
            inst_id=cfg.instrument.symbol,
        )
        max_size_payload = private_client.get_max_order_size(
            inst_id=cfg.instrument.symbol,
            td_mode=cfg.trading.td_mode,
            ccy=cfg.instrument.settle_currency,
            leverage=cfg.execution.max_leverage,
        )
    finally:
        private_client.close()

    account = build_account_snapshot(
        balance_payload=balance_payload,
        account_config_payload=account_config_payload,
        settle_currency=cfg.instrument.settle_currency,
        fallback_equity=cfg.execution.initial_equity,
    )
    position = build_position_snapshot(
        positions_payload=positions_payload,
        inst_id=cfg.instrument.symbol,
        position_mode=cfg.trading.position_mode,
    )
    max_buy, max_sell = extract_okx_max_size(max_size_payload)
    typer.echo(
        json.dumps(
            {
                "instrument": cfg.instrument.symbol,
                "okx_use_demo": cfg.okx.use_demo,
                "account": account.to_dict(),
                "position": position.to_dict(),
                "max_buy_contracts": max_buy,
                "max_sell_contracts": max_sell,
                "account_config_payload": account_config_payload,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


@app.command("demo-plan")
def demo_plan(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    account, position, state = _load_demo_state(
        cfg,
        session_factory=session_factory,
        project_root=project_root.resolve(),
    )
    typer.echo(
        _dump_demo_state(
            cfg=cfg,
            account=account,
            position=position,
            signal=state["signal"],
            plan=state["plan"],
            extra={"router_decision": state.get("router_decision")},
        )
    )


@app.command("demo-reconcile")
def demo_reconcile(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    account, position, state = _load_demo_state(
        cfg,
        session_factory=session_factory,
        project_root=project_root.resolve(),
    )
    executor_state = _load_executor_state(_executor_state_path(cfg))
    payload = _build_demo_reconcile_payload(
        cfg=cfg,
        account=account,
        position=position,
        signal=state["signal"],
        plan=state["plan"],
        state=state,
        executor_state=executor_state,
    )
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@app.command("demo-portfolio-plan")
def demo_portfolio_plan(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    symbols: str | None = typer.Option(
        None,
        help="Optional comma-separated symbols. Defaults to portfolio.symbols from config.",
    ),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    resolved_symbols = _resolve_symbols(cfg, symbols)
    account, symbol_states = _load_demo_portfolio_state(
        cfg,
        resolved_symbols,
        session_factory=session_factory,
        project_root=project_root.resolve(),
    )
    payload = _build_demo_portfolio_payload(
        cfg=cfg,
        account=account,
        symbol_states=symbol_states,
        include_exchange_checks=False,
    )
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@app.command("demo-portfolio-reconcile")
def demo_portfolio_reconcile(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    symbols: str | None = typer.Option(
        None,
        help="Optional comma-separated symbols. Defaults to portfolio.symbols from config.",
    ),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    resolved_symbols = _resolve_symbols(cfg, symbols)
    account, symbol_states = _load_demo_portfolio_state(
        cfg,
        resolved_symbols,
        session_factory=session_factory,
        project_root=project_root.resolve(),
    )
    executor_state = _load_executor_state(_executor_state_path(cfg))
    payload = _build_demo_portfolio_payload(
        cfg=cfg,
        account=account,
        symbol_states=symbol_states,
        include_exchange_checks=True,
        executor_state=executor_state,
    )
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@app.command("demo-align-leverage")
def demo_align_leverage(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    apply: bool = typer.Option(False, help="Actually set OKX demo leverage to config.execution.max_leverage."),
    confirm: str = typer.Option("", help="Safety confirmation string. Required value: OKX_DEMO"),
    rearm_protective_stop: bool = typer.Option(
        False,
        help=(
            "If leverage change is blocked by live protective stop algo orders, "
            "temporarily cancel matching stops, adjust leverage, and place them again."
        ),
    ),
) -> None:
    cfg = _load_app_context(config=config, project_root=project_root)
    payload, success = _run_demo_align_leverage_action(
        cfg,
        apply=apply,
        confirm=confirm,
        rearm_protective_stop=rearm_protective_stop,
    )
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
    if apply and not success:
        raise typer.Exit(code=1)


@app.command("demo-preflight")
def demo_preflight(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    live_plan: bool = typer.Option(
        False,
        "--live-plan",
        help="Also fetch recent market data and build the current demo order plan.",
    ),
    assert_submit_ready: bool = typer.Option(
        False,
        help="Exit with code 1 when demo submission is not fully ready.",
    ),
) -> None:
    resolved_root = project_root.resolve()
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)

    payload = build_preflight_payload(
        config=cfg,
        session_factory=session_factory,
        project_root=resolved_root,
    )

    exit_code = 0
    if live_plan:
        try:
            account, position, state = _load_demo_state(
                cfg,
                session_factory=session_factory,
                project_root=resolved_root,
            )
            payload["live_plan"] = _demo_state_payload(
                cfg=cfg,
                account=account,
                position=position,
                signal=state["signal"],
                plan=state["plan"],
                extra={"ok": True, "router_decision": state.get("router_decision")},
            )
        except Exception as exc:
            payload["live_plan"] = {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
            exit_code = 1

    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))

    if assert_submit_ready and not payload["demo_trading"]["ready"]:
        exit_code = 1
    if exit_code:
        raise typer.Exit(code=exit_code)


@app.command("demo-execute")
def demo_execute(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    submit: bool = typer.Option(False, help="Actually submit orders to the OKX demo account."),
    confirm: str = typer.Option(
        "",
        help="Safety confirmation string. Required value: OKX_DEMO",
    ),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    account, position, state = _load_demo_state(
        cfg,
        session_factory=session_factory,
        project_root=project_root.resolve(),
    )
    plan: OrderPlan = state["plan"]

    if not submit:
        typer.echo(
            _dump_demo_state(
                cfg=cfg,
                account=account,
                position=position,
                signal=state["signal"],
                plan=plan,
                extra={"submitted": False, "router_decision": state.get("router_decision")},
            )
        )
        return

    _validate_submit_permissions(cfg, session_factory, confirm)
    router_decision = state.get("router_decision")
    if isinstance(router_decision, dict) and router_decision.get("enabled") and not router_decision.get("ready"):
        typer.echo(
            _dump_demo_state(
                cfg=cfg,
                account=account,
                position=position,
                signal=state["signal"],
                plan=plan,
                extra={
                    "submitted": False,
                    "reason": "Strategy router did not resolve an executable approved candidate.",
                    "router_decision": router_decision,
                },
            )
        )
        raise typer.Exit(code=1)
    if account.account_mode and account.account_mode != cfg.trading.position_mode:
        raise typer.BadParameter(
            f"Account posMode={account.account_mode}, but config expects {cfg.trading.position_mode}."
        )
    if not plan.instructions:
        typer.echo(
            _dump_demo_state(
                cfg=cfg,
                account=account,
                position=position,
                signal=state["signal"],
                plan=plan,
                extra={
                    "submitted": False,
                    "reason": "No executable instructions in current plan.",
                    "router_decision": state.get("router_decision"),
                },
            )
        )
        return

    responses = _submit_order_plan(cfg, plan)

    typer.echo(
        _dump_demo_state(
            cfg=cfg,
            account=account,
            position=position,
            signal=state["signal"],
            plan=plan,
            extra={"submitted": True, "responses": responses, "router_decision": state.get("router_decision")},
        )
    )


@app.command("demo-loop")
def demo_loop(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    submit: bool = typer.Option(False, help="Actually submit orders to the OKX demo account."),
    confirm: str = typer.Option("", help="Safety confirmation string. Required value: OKX_DEMO"),
    interval_seconds: int | None = typer.Option(None, help="Polling interval. Defaults to trading.poll_interval_seconds."),
    cycles: int = typer.Option(0, min=0, help="Number of cycles. 0 means run forever."),
    reset_state: bool = typer.Option(False, help="Reset duplicate-submit state before starting the loop."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    loop_interval = interval_seconds or cfg.trading.poll_interval_seconds
    state_path = _executor_state_path(cfg)
    if reset_state and state_path.exists():
        state_path.unlink()

    if submit:
        _validate_submit_permissions(cfg, session_factory, confirm)

    cycle = 0
    while cycles == 0 or cycle < cycles:
        cycle += 1
        cycle_state, had_error = _run_demo_loop_cycle(
            cfg=cfg,
            session_factory=session_factory,
            project_root=project_root.resolve(),
            cycle=cycle,
            submit=submit,
            state_path=state_path,
        )
        if had_error:
            typer.echo(json.dumps(cycle_state, ensure_ascii=False, indent=2))
            if cycles != 0:
                raise typer.Exit(code=1)
        else:
            typer.echo(
                _dump_demo_state(
                    cfg=cfg,
                    account=cycle_state["account"],
                    position=cycle_state["position"],
                    signal=cycle_state["signal"],
                    plan=cycle_state["plan"],
                    extra=cycle_state["payload"],
                )
            )

        if cycles != 0 and cycle >= cycles:
            break
        time.sleep(loop_interval)


@app.command("demo-portfolio-loop")
def demo_portfolio_loop(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    submit: bool = typer.Option(False, help="Actually submit orders to the OKX demo account."),
    confirm: str = typer.Option("", help="Safety confirmation string. Required value: OKX_DEMO"),
    interval_seconds: int | None = typer.Option(None, help="Polling interval. Defaults to trading.poll_interval_seconds."),
    cycles: int = typer.Option(0, min=0, help="Number of cycles. 0 means run forever."),
    reset_state: bool = typer.Option(False, help="Reset duplicate-submit state before starting the loop."),
    symbols: str | None = typer.Option(
        None,
        help="Optional comma-separated symbols. Defaults to portfolio.symbols from config.",
    ),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    resolved_symbols = _resolve_symbols(cfg, symbols)
    loop_interval = interval_seconds or cfg.trading.poll_interval_seconds
    state_path = _executor_state_path(cfg)
    if reset_state and state_path.exists():
        state_path.unlink()

    if submit:
        _validate_submit_permissions(cfg, session_factory, confirm)

    cycle = 0
    while cycles == 0 or cycle < cycles:
        cycle += 1
        cycle_state, had_error = _run_demo_portfolio_loop_cycle(
            cfg=cfg,
            session_factory=session_factory,
            project_root=project_root.resolve(),
            cycle=cycle,
            submit=submit,
            state_path=state_path,
            symbols=resolved_symbols,
        )
        typer.echo(json.dumps(cycle_state["payload"] if not had_error else cycle_state, ensure_ascii=False, indent=2))
        if had_error and cycles != 0:
            raise typer.Exit(code=1)
        if cycles != 0 and cycle >= cycles:
            break
        time.sleep(loop_interval)


@app.command("demo-drill")
def demo_drill(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    submit: bool = typer.Option(False, help="Actually submit orders to the OKX demo account."),
    confirm: str = typer.Option("", help="Safety confirmation string. Required value: OKX_DEMO"),
    reset_state: bool = typer.Option(False, help="Reset duplicate-submit state before running the drill."),
) -> None:
    resolved_root = project_root.resolve()
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    state_path = _executor_state_path(cfg)
    if reset_state and state_path.exists():
        state_path.unlink()

    if submit:
        _validate_submit_permissions(cfg, session_factory, confirm)

    cycle_state, had_error = _run_demo_loop_cycle(
        cfg=cfg,
        session_factory=session_factory,
        project_root=resolved_root,
        cycle=1,
        submit=submit,
        state_path=state_path,
    )
    runtime_preflight = build_preflight_payload(
        config=cfg,
        session_factory=session_factory,
        project_root=resolved_root,
    )

    if had_error:
        typer.echo(
            json.dumps(
                {
                    "submit_requested": submit,
                    "runtime_preflight": runtime_preflight,
                    "drill": cycle_state,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        raise typer.Exit(code=1)

    payload = _demo_state_payload(
        cfg=cfg,
        account=cycle_state["account"],
        position=cycle_state["position"],
        signal=cycle_state["signal"],
        plan=cycle_state["plan"],
        extra={
            "submit_requested": submit,
            "runtime_preflight": runtime_preflight,
            "drill": cycle_state["payload"],
        },
    )
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@app.command("demo-portfolio-drill")
def demo_portfolio_drill(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    submit: bool = typer.Option(False, help="Actually submit orders to the OKX demo account."),
    confirm: str = typer.Option("", help="Safety confirmation string. Required value: OKX_DEMO"),
    reset_state: bool = typer.Option(False, help="Reset duplicate-submit state before running the drill."),
    symbols: str | None = typer.Option(
        None,
        help="Optional comma-separated symbols. Defaults to portfolio.symbols from config.",
    ),
) -> None:
    resolved_root = project_root.resolve()
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    resolved_symbols = _resolve_symbols(cfg, symbols)
    state_path = _executor_state_path(cfg)
    if reset_state and state_path.exists():
        state_path.unlink()

    if submit:
        _validate_submit_permissions(cfg, session_factory, confirm)

    cycle_state, had_error = _run_demo_portfolio_loop_cycle(
        cfg=cfg,
        session_factory=session_factory,
        project_root=resolved_root,
        cycle=1,
        submit=submit,
        state_path=state_path,
        symbols=resolved_symbols,
    )
    runtime_preflight = build_preflight_payload(
        config=cfg,
        session_factory=session_factory,
        project_root=resolved_root,
    )

    payload = {
        "mode": "portfolio",
        "submit_requested": submit,
        "symbols": resolved_symbols,
        "runtime_preflight": runtime_preflight,
        "drill": cycle_state["payload"] if not had_error else cycle_state,
    }
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
    if had_error:
        raise typer.Exit(code=1)


@app.command("service-init-db")
def service_init_db(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
) -> None:
    cfg, _session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    typer.echo(f"Initialized database: {cfg.database.url}")


@app.command("service-step")
def service_step(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    artifacts = run_monitor_cycle(config=cfg, session_factory=session_factory, project_root=project_root.resolve())
    typer.echo(
        json.dumps(
            {
                "latest_equity": artifacts.snapshot.latest_equity,
                "halted": bool(artifacts.snapshot.halted),
                "report_stale": bool(artifacts.snapshot.report_stale),
                "alerts_sent": artifacts.alerts_sent,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


@app.command("service-api")
def service_api(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    app_instance = build_service_app(
        config=cfg,
        session_factory=session_factory,
        project_root=project_root.resolve(),
    )
    uvicorn.run(app_instance, host=cfg.service.host, port=cfg.service.port)


@app.command("alert-test")
def alert_test(
    config: Path = typer.Option(..., exists=True, dir_okay=False, help="Path to the YAML config file."),
    project_root: Path = typer.Option(Path("."), file_okay=False, help="Project root for storage paths."),
    message: str = typer.Option("quant-lab test alert", help="Test alert message."),
) -> None:
    cfg, session_factory = _load_runtime_context(config=config, project_root=project_root)
    init_db(cfg.database.url)
    sent_channels = _persist_alert_results(
        session_factory,
        cfg=cfg,
        event_key="manual_test",
        level="info",
        title="Manual test alert",
        message=f"quant-lab test alert\n{message}",
    )
    typer.echo(
        json.dumps(
            {
                "sent": bool(sent_channels),
                "channels": sent_channels,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
