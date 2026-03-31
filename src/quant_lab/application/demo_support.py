from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import typer

from quant_lab.alerts.delivery import deliver_alerts
from quant_lab.application import report_runtime
from quant_lab.config import configured_symbols
from quant_lab.data.okx_private_client import OkxPrivateClient
from quant_lab.data.public_factors import PublicFactorSnapshot, load_public_factor_snapshot
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
from quant_lab.execution.strategy_router import resolve_strategy_route, serialize_strategy_route_decision
from quant_lab.providers.market_data import build_market_data_provider
from quant_lab.risk.portfolio import apply_factor_overlay_to_plan, apply_portfolio_risk_caps
from quant_lab.service.database import AlertEvent, session_scope
from quant_lab.utils.timeframes import bar_to_timedelta as parse_bar_timedelta


def fetch_live_market_data_for_symbol(cfg, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    now = pd.Timestamp.now(tz="UTC")
    signal_bars_needed = max(
        cfg.trading.signal_lookback_bars,
        cfg.strategy.slow_ema + cfg.strategy.atr_period + 10,
    )
    execution_bars_needed = max(cfg.trading.execution_lookback_bars, cfg.execution.latency_minutes + 20)

    signal_start = now - (_bar_to_timedelta(cfg.strategy.signal_bar) * signal_bars_needed)
    execution_start = now - (_bar_to_timedelta(cfg.strategy.execution_bar) * execution_bars_needed)

    client = build_market_data_provider(cfg)
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


def load_demo_state(
    cfg,
    *,
    session_factory=None,
    project_root: Path | None = None,
) -> tuple[AccountSnapshot, PositionSnapshot, dict[str, object]]:
    return load_demo_state_for_symbol(
        cfg,
        cfg.instrument.symbol,
        session_factory=session_factory,
        project_root=project_root,
    )


def load_demo_state_for_symbol(
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
    instrument_config = report_runtime.resolve_instrument_config(cfg, cfg.storage, symbol)
    signal_bars, execution_bars = fetch_live_market_data_for_symbol(cfg, symbol)
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
        warnings.append(f"Planning equity for {symbol} is capped to allocated portfolio budget {allocated_value:.2f}.")
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
            "策略路由当前不可执行：" + "；".join(router_decision.reasons or ["路由未就绪"])
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
        "router_decision": (
            serialize_strategy_route_decision(
                router_decision,
                default_strategy=cfg.strategy,
                symbol=symbol,
                required_scope="demo",
            )
            if router_decision is not None
            else None
        ),
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


def _portfolio_budgeted_account(
    *,
    account: AccountSnapshot,
    total_equity_reference: float,
    risk_per_trade: float,
    allocated_risk_fraction: float,
) -> AccountSnapshot:
    if total_equity_reference <= 0 or risk_per_trade <= 0 or allocated_risk_fraction <= 0:
        budgeted_equity = 0.0
    else:
        budget_ratio = allocated_risk_fraction / risk_per_trade
        budgeted_equity = min(total_equity_reference, total_equity_reference * budget_ratio)

    return AccountSnapshot(
        total_equity=budgeted_equity,
        available_equity=budgeted_equity,
        currency=account.currency,
        source=f"{account.source}_portfolio_risk_budget",
        account_mode=account.account_mode,
        can_trade=account.can_trade,
        raw=account.raw,
    )


def load_demo_portfolio_state(
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
    states: dict[str, dict[str, object]] = {}
    try:
        for symbol in symbols:
            account, position, state = load_demo_state_for_symbol(
                cfg,
                symbol,
                session_factory=session_factory,
                project_root=project_root,
                private_client=private_client,
                shared_balance_payload=shared_balance_payload,
                shared_account_config_payload=shared_account_payload,
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
                planning_account = _portfolio_budgeted_account(
                    account=states[symbol]["account"],
                    total_equity_reference=total_equity_reference,
                    risk_per_trade=cfg.risk.risk_per_trade,
                    allocated_risk_fraction=decision.scaled_risk_fraction,
                )
                states[symbol]["planning_account"] = planning_account
                states[symbol]["plan"].equity_reference = (
                    planning_account.available_equity
                    or planning_account.total_equity
                    or 0.0
                )
    finally:
        if private_client is not None:
            private_client.close()
    return base_account, states


def demo_state_payload(
    *,
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


def persist_alert_results(
    session_factory,
    *,
    alerts_config,
    event_key: str,
    level: str,
    title: str,
    message: str,
    deliver_fn: Callable[..., Any] = deliver_alerts,
) -> list[str]:
    results = deliver_fn(alerts_config, title=title, message=message)
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


def load_executor_state_file(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def okx_rows(payload: dict[str, object] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows = payload.get("data")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def safe_float(value: object, *, fallback: float | None = 0.0) -> float | None:
    if value is None or value == "" or value == " ":
        return fallback
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def expected_stop_side(position_side: int) -> str | None:
    if position_side > 0:
        return "sell"
    if position_side < 0:
        return "buy"
    return None


def expected_position_leg(position_side: int, position_mode: str) -> str | None:
    if position_mode == "long_short_mode":
        if position_side > 0:
            return "long"
        if position_side < 0:
            return "short"
    if position_mode == "net_mode":
        return "net"
    return None


def matching_stop_orders(cfg, position: PositionSnapshot, pending_algo_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if position.side == 0 or position.contracts <= 0:
        return []

    expected_side = expected_stop_side(position.side)
    expected_leg = expected_position_leg(position.side, cfg.trading.position_mode)
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
        size = safe_float(row.get("sz"), fallback=None)
        if size is not None and abs(size - position.contracts) > size_tolerance:
            continue
        matches.append(row)

    return matches


def summarize_executor_state(executor_state: dict[str, object]) -> dict[str, object] | None:
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


def format_decimal_text(value: float) -> str:
    return format(value, "f").rstrip("0").rstrip(".") or "0"


def build_leverage_alignment_requests(
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
            for value in (safe_float(row.get("lever"), fallback=None) for row in relevant_rows)
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


def require_private_credentials(cfg) -> None:
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


def validate_demo_account_mutation(cfg, confirm: str) -> None:
    if not cfg.okx.use_demo:
        raise typer.BadParameter("Account mutation is blocked because okx.use_demo=false.")
    if confirm != "OKX_DEMO":
        raise typer.BadParameter("Refusing to mutate OKX demo account. Pass --confirm OKX_DEMO to continue.")


def build_demo_reconcile_payload(
    *,
    cfg,
    account: AccountSnapshot,
    position: PositionSnapshot,
    signal,
    plan: OrderPlan,
    state: dict[str, object],
    executor_state: dict[str, object],
) -> dict[str, object]:
    payload = demo_state_payload(
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
        payload["router_decision"] = serialize_strategy_route_decision(
            router_decision,
            default_strategy=cfg.strategy,
            symbol=str(state.get("symbol") or cfg.instrument.symbol),
            required_scope="demo",
        )
    portfolio_risk = state.get("portfolio_risk")
    if portfolio_risk is not None and hasattr(portfolio_risk, "to_dict"):
        payload["portfolio_risk"] = portfolio_risk.to_dict()

    leverage_rows = okx_rows(state.get("leverage_payload"))
    pending_orders = okx_rows(state.get("pending_orders_payload"))
    pending_algo_orders = okx_rows(state.get("pending_algo_orders_payload"))
    matching_orders = matching_stop_orders(cfg, position, pending_algo_orders)

    leverage_values = [
        value
        for value in (safe_float(row.get("lever"), fallback=None) for row in leverage_rows)
        if value is not None
    ]
    leverage_match = None
    if leverage_values:
        leverage_match = all(abs(value - cfg.execution.max_leverage) <= 0.01 for value in leverage_values)

    position_mode_match = None
    if account.account_mode:
        position_mode_match = account.account_mode == cfg.trading.position_mode

    protective_stop_needed = position.side != 0 and position.contracts > 0
    protective_stop_ready = bool(matching_orders) if protective_stop_needed else True
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
    if protective_stop_needed and not matching_orders:
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
            "expected_side": expected_stop_side(position.side),
            "expected_pos_side": expected_position_leg(position.side, cfg.trading.position_mode),
            "matched_count": len(matching_orders),
            "matched_algo_ids": [row.get("algoId") for row in matching_orders],
        },
    }
    payload["executor_state"] = summarize_executor_state(executor_state)
    if tracked_algo_ids:
        payload["exchange"]["executor_tracking"] = {
            "tracked_attach_algo_client_ids": sorted(tracked_algo_ids),
            "matched_live_algo_client_ids": tracked_live_algo_ids,
        }
    payload["warnings"] = warnings
    return payload


def build_demo_portfolio_payload(
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
    requested_total_risk_fraction = 0.0
    allocated_total_risk_fraction = 0.0
    budgeted_equity_total = 0.0
    budgeted_symbol_count = 0
    for symbol, state in symbol_states.items():
        if include_exchange_checks:
            symbol_executor_state = {}
            if isinstance(executor_state, dict):
                per_symbol_state = executor_state.get("symbols")
                if isinstance(per_symbol_state, dict):
                    candidate = per_symbol_state.get(symbol)
                    if isinstance(candidate, dict):
                        symbol_executor_state = candidate
            symbol_payload = build_demo_reconcile_payload(
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
                extra_payload["router_decision"] = serialize_strategy_route_decision(
                    router_decision,
                    default_strategy=cfg.strategy,
                    symbol=symbol,
                    required_scope="demo",
                )
            portfolio_risk = state.get("portfolio_risk")
            if portfolio_risk is not None and hasattr(portfolio_risk, "to_dict"):
                extra_payload["portfolio_risk"] = portfolio_risk.to_dict()
            symbol_payload = demo_state_payload(
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
            router_decision = serialize_strategy_route_decision(
                router_decision,
                default_strategy=cfg.strategy,
                symbol=symbol,
                required_scope="demo",
            )
            regime = str(router_decision.get("regime") or "").strip().lower()
            if regime in regime_counts:
                regime_counts[regime] += 1
            if router_decision.get("ready") is True:
                routed_ready_count += 1
        portfolio_risk = state.get("portfolio_risk")
        if portfolio_risk is not None:
            requested_total_risk_fraction += safe_float(
                getattr(portfolio_risk, "base_risk_fraction", None),
                fallback=0.0,
            ) or 0.0
            allocated_total_risk_fraction += safe_float(
                getattr(portfolio_risk, "scaled_risk_fraction", None),
                fallback=0.0,
            ) or 0.0
        planning_account = state["planning_account"]
        planning_equity = planning_account.available_equity or planning_account.total_equity or 0.0
        budgeted_equity_total += planning_equity
        if planning_equity > 0:
            budgeted_symbol_count += 1
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
        if safe_float(public_factors.get("confidence"), fallback=0.0) > 0:
            public_factor_ready_count += 1

    summary: dict[str, object] = {
        "symbol_count": len(symbol_states),
        "ready_symbol_count": ready_symbols,
        "actionable_symbol_count": actionable_symbols,
        "active_position_symbol_count": active_positions,
        "allocation_mode": "priority_risk_budget",
        "public_factor_ready_symbol_count": public_factor_ready_count,
        "routed_ready_symbol_count": routed_ready_count,
        "bull_trend_symbol_count": regime_counts["bull_trend"],
        "bear_trend_symbol_count": regime_counts["bear_trend"],
        "range_symbol_count": regime_counts["range"],
        "requested_total_risk_fraction": round(requested_total_risk_fraction, 6),
        "allocated_total_risk_fraction": round(allocated_total_risk_fraction, 6),
        "requested_total_risk_pct": round(requested_total_risk_fraction * 100.0, 2),
        "allocated_total_risk_pct": round(allocated_total_risk_fraction * 100.0, 2),
        "portfolio_total_risk_cap_pct": round(cfg.risk.portfolio_max_total_risk * 100.0, 2),
        "same_direction_risk_cap_pct": round(cfg.risk.portfolio_max_same_direction_risk * 100.0, 2),
        "planning_equity_reference": round(account.available_equity or account.total_equity or 0.0, 2),
        "budgeted_equity_total": round(budgeted_equity_total, 2),
        "budgeted_symbol_count": budgeted_symbol_count,
    }
    if symbol_states:
        summary["per_symbol_planning_equity"] = round(budgeted_equity_total / len(symbol_states), 2)
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


def leverage_alignment_blockers(cfg, state: dict[str, object]) -> list[str]:
    blockers: list[str] = []
    pending_algo_orders = okx_rows(state.get("pending_algo_orders_payload"))
    if cfg.trading.td_mode == "cross" and pending_algo_orders:
        blockers.append(
            "OKX blocks cross leverage changes while TP/SL or other algo orders are live. "
            "Cancel and later re-arm the protective stop before retrying."
        )
    return blockers


def extract_rearmable_stop_orders(
    cfg,
    position: PositionSnapshot,
    state: dict[str, object],
) -> list[dict[str, object]]:
    pending_algo_orders = okx_rows(state.get("pending_algo_orders_payload"))
    matching_orders = matching_stop_orders(cfg, position, pending_algo_orders)
    stop_orders: list[dict[str, object]] = []
    for index, row in enumerate(matching_orders, start=1):
        trigger_px = safe_float(row.get("slTriggerPx"), fallback=None)
        order_px = safe_float(row.get("slOrdPx"), fallback=-1.0)
        size = safe_float(row.get("sz"), fallback=None)
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
                "pos_side": row.get("posSide") or expected_position_leg(position.side, cfg.trading.position_mode),
                "size": size,
                "sl_trigger_px": trigger_px,
                "sl_ord_px": order_px if order_px is not None else -1.0,
                "sl_trigger_px_type": row.get("slTriggerPxType") or cfg.trading.stop_trigger_price_type,
            }
        )
    return stop_orders


def align_demo_leverage(cfg, *, leverage_rows: list[dict[str, Any]]) -> dict[str, object]:
    requests = build_leverage_alignment_requests(cfg, leverage_rows)
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


def align_demo_leverage_with_stop_rearm(
    cfg,
    *,
    leverage_rows: list[dict[str, Any]],
    stop_orders: list[dict[str, object]],
) -> dict[str, object]:
    requests = build_leverage_alignment_requests(cfg, leverage_rows)
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


def build_demo_align_leverage_context(
    cfg,
    *,
    executor_state_path_fn: Callable[..., Path],
    load_executor_state_fn: Callable[[Path], dict[str, object]],
    load_demo_state_fn: Callable[..., tuple[AccountSnapshot, PositionSnapshot, dict[str, object]]] = load_demo_state,
    load_demo_portfolio_state_fn: Callable[..., tuple[AccountSnapshot, dict[str, dict[str, object]]]] = load_demo_portfolio_state,
    build_demo_reconcile_payload_fn: Callable[..., dict[str, object]] = build_demo_reconcile_payload,
    build_demo_portfolio_payload_fn: Callable[..., dict[str, object]] = build_demo_portfolio_payload,
) -> dict[str, object]:
    symbols = configured_symbols(cfg)
    mode = "portfolio" if len(symbols) > 1 else "single"
    executor_state = load_executor_state_fn(executor_state_path_fn(cfg, mode=mode))
    portfolio_mode = len(symbols) > 1
    symbol_contexts: dict[str, dict[str, object]] = {}

    if portfolio_mode:
        account, symbol_states = load_demo_portfolio_state_fn(cfg, symbols)
        before = build_demo_portfolio_payload_fn(
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
            leverage_rows = okx_rows(state.get("leverage_payload"))
            planned_requests = build_leverage_alignment_requests(cfg, leverage_rows, inst_id=symbol)
            blockers = leverage_alignment_blockers(cfg, state) if planned_requests else []
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
        account, position, state = load_demo_state_fn(cfg)
        before = build_demo_reconcile_payload_fn(
            cfg=cfg,
            account=account,
            position=position,
            signal=state["signal"],
            plan=state["plan"],
            state=state,
            executor_state=executor_state,
        )
        leverage_rows = okx_rows(state.get("leverage_payload"))
        planned_requests = build_leverage_alignment_requests(
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
            "blockers": leverage_alignment_blockers(cfg, state) if planned_requests else [],
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


def apply_demo_align_leverage(
    cfg,
    *,
    symbol_contexts: dict[str, dict[str, object]],
    rearm_protective_stop: bool,
    align_demo_leverage_fn: Callable[..., dict[str, object]] = align_demo_leverage,
    align_demo_leverage_with_stop_rearm_fn: Callable[..., dict[str, object]] = align_demo_leverage_with_stop_rearm,
    extract_rearmable_stop_orders_fn: Callable[..., list[dict[str, object]]] = extract_rearmable_stop_orders,
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
                stop_orders = extract_rearmable_stop_orders_fn(cfg, position, state)
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
                alignment = align_demo_leverage_with_stop_rearm_fn(
                    cfg,
                    leverage_rows=leverage_rows,
                    stop_orders=stop_orders,
                )
                result["used_stop_rearm"] = True
            else:
                alignment = align_demo_leverage_fn(cfg, leverage_rows=leverage_rows)
                result["used_stop_rearm"] = False

            result.update(
                {
                    "status": "aligned",
                    "applied": True,
                    "already_aligned": False,
                    "alignment": alignment,
                }
            )
        except Exception as exc:
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


def run_demo_align_leverage_action(
    cfg,
    *,
    apply: bool,
    confirm: str,
    rearm_protective_stop: bool,
    refresh_snapshot: Callable[[], dict[str, Any]] | None = None,
    executor_state_path_fn: Callable[..., Path],
    load_executor_state_fn: Callable[[Path], dict[str, object]],
    load_demo_state_fn: Callable[..., tuple[AccountSnapshot, PositionSnapshot, dict[str, object]]] = load_demo_state,
    load_demo_portfolio_state_fn: Callable[..., tuple[AccountSnapshot, dict[str, dict[str, object]]]] = load_demo_portfolio_state,
    build_demo_reconcile_payload_fn: Callable[..., dict[str, object]] = build_demo_reconcile_payload,
    build_demo_portfolio_payload_fn: Callable[..., dict[str, object]] = build_demo_portfolio_payload,
    require_private_credentials_fn: Callable[[Any], None] = require_private_credentials,
    validate_mutation_fn: Callable[[Any, str], None] = validate_demo_account_mutation,
    align_demo_leverage_fn: Callable[..., dict[str, object]] = align_demo_leverage,
    align_demo_leverage_with_stop_rearm_fn: Callable[..., dict[str, object]] = align_demo_leverage_with_stop_rearm,
    extract_rearmable_stop_orders_fn: Callable[..., list[dict[str, object]]] = extract_rearmable_stop_orders,
) -> tuple[dict[str, object], bool]:
    context = build_demo_align_leverage_context(
        cfg,
        executor_state_path_fn=executor_state_path_fn,
        load_executor_state_fn=load_executor_state_fn,
        load_demo_state_fn=load_demo_state_fn,
        load_demo_portfolio_state_fn=load_demo_portfolio_state_fn,
        build_demo_reconcile_payload_fn=build_demo_reconcile_payload_fn,
        build_demo_portfolio_payload_fn=build_demo_portfolio_payload_fn,
    )
    payload: dict[str, object] = {
        "mode": context["mode"],
        "symbols": context["symbols"],
        "target_leverage": format_decimal_text(cfg.execution.max_leverage),
        "apply_requested": apply,
        "planned_requests": context["planned_requests"],
        "blockers": context["blockers"],
        "symbol_plans": context["symbol_plans"],
        "before": context["before"],
    }

    if not apply:
        return payload, True

    require_private_credentials_fn(cfg)
    validate_mutation_fn(cfg, confirm)
    symbol_results, success = apply_demo_align_leverage(
        cfg,
        symbol_contexts=context["_symbol_contexts"],
        rearm_protective_stop=rearm_protective_stop,
        align_demo_leverage_fn=align_demo_leverage_fn,
        align_demo_leverage_with_stop_rearm_fn=align_demo_leverage_with_stop_rearm_fn,
        extract_rearmable_stop_orders_fn=extract_rearmable_stop_orders_fn,
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
        refreshed_context = build_demo_align_leverage_context(
            cfg,
            executor_state_path_fn=executor_state_path_fn,
            load_executor_state_fn=load_executor_state_fn,
            load_demo_state_fn=load_demo_state_fn,
            load_demo_portfolio_state_fn=load_demo_portfolio_state_fn,
            build_demo_reconcile_payload_fn=build_demo_reconcile_payload_fn,
            build_demo_portfolio_payload_fn=build_demo_portfolio_payload_fn,
        )
        payload["after"] = refreshed_context["before"]
    else:
        payload["after"] = refreshed["reconcile"]
        if "preflight" in refreshed:
            payload["preflight"] = refreshed["preflight"]
    return payload, success


def _bar_to_timedelta(bar: str) -> pd.Timedelta:
    try:
        return parse_bar_timedelta(bar)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _build_private_client(cfg) -> OkxPrivateClient:
    return OkxPrivateClient(
        api_key=str(cfg.okx.api_key),
        secret_key=str(cfg.okx.secret_key),
        passphrase=str(cfg.okx.passphrase),
        base_url=cfg.okx.rest_base_url,
        use_demo=cfg.okx.use_demo,
        proxy_url=cfg.okx.proxy_url,
    )


def _build_client_order_id(tag: str, sequence: int) -> str:
    prefix = "".join(ch for ch in tag if ch.isalnum()).lower()[:10] or "qlab"
    timestamp = int(pd.Timestamp.now(tz="UTC").timestamp() * 1000)
    return f"{prefix}{timestamp}{sequence:02d}"[:32]


def _wait_until_algo_orders_absent(private_client: OkxPrivateClient, *, inst_id: str, algo_ids: set[str]) -> bool:
    if not algo_ids:
        return True
    for _ in range(10):
        payload = private_client.get_pending_algo_orders(inst_id=inst_id, ord_type="conditional")
        rows = okx_rows(payload)
        live_ids = {str(row.get("algoId")) for row in rows if row.get("algoId")}
        if not (algo_ids & live_ids):
            return True
        time.sleep(0.5)
    return False


def _rearm_stop_orders(
    private_client: OkxPrivateClient,
    cfg,
    stop_orders: list[dict[str, object]],
) -> list[dict[str, object]]:
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
