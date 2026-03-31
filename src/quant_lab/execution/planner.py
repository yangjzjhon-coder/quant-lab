from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd

from quant_lab.backtest.realism import bar_liquidity_quote, cap_contracts_by_liquidity, estimate_fill_bps
from quant_lab.backtest.engine import _entry_fill_price
from quant_lab.config import ExecutionConfig, InstrumentConfig, RiskConfig, StrategyConfig, TradingConfig
from quant_lab.risk.rules import position_size_from_risk
from quant_lab.strategy_contracts import AlphaSignalContract, RiskSignalContract, execution_signal_from_row
from quant_lab.strategies.ema_trend import prepare_signal_frame
from quant_lab.utils.timeframes import bar_to_timedelta


@dataclass
class SignalSnapshot:
    signal_time: pd.Timestamp
    effective_time: pd.Timestamp
    latest_execution_time: pd.Timestamp
    latest_price: float
    latest_high: float
    latest_low: float
    latest_liquidity_quote: float
    desired_side: int
    previous_side: int
    stop_distance: float
    ready: bool
    strategy_score: float | None = None
    strategy_risk_multiplier: float = 1.0
    signal_stop_price: float | None = None
    regime: str | None = None
    route_key: str | None = None
    alpha_signal: AlphaSignalContract | None = None
    risk_signal: RiskSignalContract | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        for key in ("signal_time", "effective_time", "latest_execution_time"):
            data[key] = data[key].isoformat()
        return data


@dataclass
class AccountSnapshot:
    total_equity: float
    available_equity: float
    currency: str | None
    source: str
    account_mode: str | None = None
    can_trade: bool | None = None
    raw: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PositionSnapshot:
    side: int
    contracts: float
    position_mode: str = "net_mode"
    avg_price: float | None = None
    mark_price: float | None = None
    raw: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AttachedAlgoOrder:
    sl_trigger_px: float
    sl_ord_px: float = -1.0
    sl_trigger_px_type: str = "mark"
    attach_algo_cl_ord_id: str | None = None

    def to_request_payload(self) -> dict[str, Any]:
        payload = {
            "slTriggerPx": _format_contracts(self.sl_trigger_px),
            "slOrdPx": _format_contracts(self.sl_ord_px),
            "slTriggerPxType": self.sl_trigger_px_type,
        }
        if self.attach_algo_cl_ord_id:
            payload["attachAlgoClOrdId"] = self.attach_algo_cl_ord_id
        return payload


@dataclass
class OrderInstruction:
    purpose: str
    inst_id: str
    td_mode: str
    side: str
    ord_type: str
    size: float
    reduce_only: bool
    pos_side: str | None = None
    estimated_fill_price: float | None = None
    stop_price: float | None = None
    attach_algo_orders: list[AttachedAlgoOrder] = field(default_factory=list)

    def to_request_payload(self, *, client_order_id: str | None = None, tag: str | None = None) -> dict[str, Any]:
        payload = {
            "instId": self.inst_id,
            "tdMode": self.td_mode,
            "side": self.side,
            "ordType": self.ord_type,
            "sz": _format_contracts(self.size),
            "reduceOnly": str(self.reduce_only).lower(),
        }
        if self.pos_side:
            payload["posSide"] = self.pos_side
        if client_order_id:
            payload["clOrdId"] = client_order_id
        if tag:
            payload["tag"] = tag
        if self.attach_algo_orders:
            payload["attachAlgoOrds"] = [item.to_request_payload() for item in self.attach_algo_orders]
        return payload

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["size"] = float(payload["size"])
        return payload


@dataclass
class OrderPlan:
    action: str
    reason: str
    desired_side: int
    current_side: int
    current_contracts: float
    target_contracts: float
    equity_reference: float
    latest_price: float
    entry_price_estimate: float | None
    stop_price: float | None
    stop_distance: float | None
    signal_time: pd.Timestamp
    effective_time: pd.Timestamp
    position_mode: str
    instructions: list[OrderInstruction] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["signal_time"] = self.signal_time.isoformat()
        data["effective_time"] = self.effective_time.isoformat()
        data["instructions"] = [instruction.to_dict() for instruction in self.instructions]
        return data


def build_signal_snapshot(
    signal_bars: pd.DataFrame,
    execution_bars: pd.DataFrame,
    strategy_config: StrategyConfig,
    execution_config: ExecutionConfig,
) -> SignalSnapshot:
    signal_frame = prepare_signal_frame(_normalize_market_frame(signal_bars), strategy_config)
    normalized_execution = _normalize_market_frame(execution_bars)

    if signal_frame.empty or normalized_execution.empty:
        raise ValueError("Signal planning requires non-empty signal and execution bars.")

    latest_signal = signal_frame.iloc[-1]
    previous_side = int(signal_frame["desired_side"].iloc[-2]) if len(signal_frame) > 1 else 0
    signal_contract = execution_signal_from_row(
        latest_signal,
        previous_side=previous_side,
        strategy_name=strategy_config.name,
        strategy_variant=strategy_config.variant,
    )
    latest_execution = normalized_execution.iloc[-1]
    signal_time = signal_contract.signal_time
    effective_time = (
        signal_time
        + bar_to_timedelta(strategy_config.signal_bar)
        + pd.to_timedelta(execution_config.latency_minutes, unit="m")
    )
    latest_execution_time = pd.Timestamp(latest_execution["timestamp"])
    latest_price = float(latest_execution["close"])
    latest_high = float(latest_execution["high"])
    latest_low = float(latest_execution["low"])
    latest_liquidity_quote = bar_liquidity_quote(
        price=latest_price,
        contract_value=1.0,
        volume=float(latest_execution["volume"]) if "volume" in latest_execution.index else None,
        volume_ccy=float(latest_execution["volume_ccy"]) if "volume_ccy" in latest_execution.index else None,
        volume_quote=float(latest_execution["volume_quote"]) if "volume_quote" in latest_execution.index else None,
    )

    return SignalSnapshot(
        signal_time=signal_time,
        effective_time=effective_time,
        latest_execution_time=latest_execution_time,
        latest_price=latest_price,
        latest_high=latest_high,
        latest_low=latest_low,
        latest_liquidity_quote=latest_liquidity_quote,
        desired_side=signal_contract.desired_side,
        previous_side=previous_side,
        stop_distance=signal_contract.risk_signal.stop_distance,
        strategy_score=signal_contract.alpha_signal.score,
        strategy_risk_multiplier=signal_contract.risk_signal.risk_multiplier,
        signal_stop_price=signal_contract.risk_signal.stop_price,
        regime=signal_contract.alpha_signal.regime,
        route_key=signal_contract.route_key,
        alpha_signal=signal_contract.alpha_signal,
        risk_signal=signal_contract.risk_signal,
        ready=latest_execution_time >= effective_time,
    )


def build_order_plan(
    *,
    signal: SignalSnapshot,
    account: AccountSnapshot,
    position: PositionSnapshot,
    instrument_config: InstrumentConfig,
    execution_config: ExecutionConfig,
    risk_config: RiskConfig,
    trading_config: TradingConfig,
    max_buy_contracts: float | None = None,
    max_sell_contracts: float | None = None,
) -> OrderPlan:
    warnings: list[str] = []
    equity_reference = account.available_equity or account.total_equity or execution_config.initial_equity

    if not signal.ready:
        return OrderPlan(
            action="wait",
            reason="Signal confirmed, but execution latency window has not completed yet.",
            desired_side=signal.desired_side,
            current_side=position.side,
            current_contracts=position.contracts,
            target_contracts=position.contracts,
            equity_reference=equity_reference,
            latest_price=signal.latest_price,
            entry_price_estimate=None,
            stop_price=signal.signal_stop_price,
            stop_distance=signal.stop_distance,
            signal_time=signal.signal_time,
            effective_time=signal.effective_time,
            position_mode=trading_config.position_mode,
            warnings=warnings,
        )

    if signal.desired_side == 0:
        instructions = (
            _build_close_instructions(position=position, instrument_config=instrument_config, trading_config=trading_config)
            if position.side != 0 and position.contracts > 0
            else []
        )
        return OrderPlan(
            action="close" if instructions else "hold",
            reason="Strategy is flat on the latest confirmed signal bar.",
            desired_side=signal.desired_side,
            current_side=position.side,
            current_contracts=position.contracts,
            target_contracts=0.0,
            equity_reference=equity_reference,
            latest_price=signal.latest_price,
            entry_price_estimate=None,
            stop_price=None,
            stop_distance=signal.stop_distance,
            signal_time=signal.signal_time,
            effective_time=signal.effective_time,
            position_mode=trading_config.position_mode,
            instructions=instructions,
            warnings=warnings,
        )

    provisional_entry_price = _entry_fill_price(
        signal.desired_side,
        signal.latest_price,
        execution_config.slippage_bps / 10_000,
    )
    provisional_stop_price = (
        signal.signal_stop_price
        if signal.signal_stop_price is not None
        else provisional_entry_price - (signal.desired_side * signal.stop_distance)
    )
    target_contracts = position_size_from_risk(
        equity=equity_reference,
        entry_price=provisional_entry_price,
        stop_price=provisional_stop_price,
        risk_fraction=risk_config.risk_per_trade * max(0.0, signal.strategy_risk_multiplier),
        max_leverage=execution_config.max_leverage,
        contract_value=instrument_config.contract_value,
        lot_size=instrument_config.lot_size,
        min_size=instrument_config.min_size,
        minimum_notional=execution_config.minimum_notional,
    )

    if trading_config.max_order_contracts is not None:
        target_contracts = min(target_contracts, trading_config.max_order_contracts)
    exchange_limit = max_buy_contracts if signal.desired_side > 0 else max_sell_contracts
    if exchange_limit is not None and exchange_limit > 0:
        if target_contracts > exchange_limit:
            warnings.append(
                f"Target size was clamped by OKX max-size endpoint: {target_contracts} -> {exchange_limit}"
            )
            target_contracts = min(target_contracts, exchange_limit)

    target_contracts = cap_contracts_by_liquidity(
        desired_contracts=target_contracts,
        price=signal.latest_price,
        contract_value=instrument_config.contract_value,
        liquidity_quote=signal.latest_liquidity_quote,
        max_bar_participation=execution_config.max_bar_participation,
        lot_size=instrument_config.lot_size,
    )
    target_contracts = _round_down_to_lot(target_contracts, instrument_config.lot_size)
    if target_contracts <= 0:
        warnings.append("Position sizing returned 0 contracts. Order placement is skipped.")
        return OrderPlan(
            action="hold",
            reason="No executable size after risk checks and exchange limits.",
            desired_side=signal.desired_side,
            current_side=position.side,
            current_contracts=position.contracts,
            target_contracts=0.0,
            equity_reference=equity_reference,
            latest_price=signal.latest_price,
            entry_price_estimate=provisional_entry_price,
            stop_price=provisional_stop_price,
            stop_distance=signal.stop_distance,
            signal_time=signal.signal_time,
            effective_time=signal.effective_time,
            position_mode=trading_config.position_mode,
            warnings=warnings,
        )

    entry_price = _entry_fill_price(
        signal.desired_side,
        signal.latest_price,
        estimate_fill_bps(
            price=signal.latest_price,
            high=signal.latest_high,
            low=signal.latest_low,
            order_contracts=target_contracts,
            contract_value=instrument_config.contract_value,
            liquidity_quote=signal.latest_liquidity_quote,
            base_slippage_bps=execution_config.slippage_bps,
            market_impact_bps=execution_config.market_impact_bps,
            excess_impact_bps=execution_config.excess_impact_bps,
            volatility_impact_share=execution_config.volatility_impact_share,
            max_bar_participation=execution_config.max_bar_participation,
        )
        / 10_000,
    )
    stop_price = entry_price - (signal.desired_side * signal.stop_distance)
    if signal.signal_stop_price is not None:
        stop_price = max(stop_price, signal.signal_stop_price) if signal.desired_side > 0 else min(
            stop_price,
            signal.signal_stop_price,
        )

    if position.side == signal.desired_side and position.contracts > 0:
        warnings.append("Existing position already matches the latest strategy side. No rebalance is performed.")
        if signal.signal_stop_price is not None:
            warnings.append("Signal model has a tighter managed stop, but live stop amendment is not automated yet.")
        return OrderPlan(
            action="hold",
            reason="Current position already matches the latest confirmed signal.",
            desired_side=signal.desired_side,
            current_side=position.side,
            current_contracts=position.contracts,
            target_contracts=target_contracts,
            equity_reference=equity_reference,
            latest_price=signal.latest_price,
            entry_price_estimate=entry_price,
            stop_price=stop_price,
            stop_distance=signal.stop_distance,
            signal_time=signal.signal_time,
            effective_time=signal.effective_time,
            position_mode=trading_config.position_mode,
            warnings=warnings,
        )

    instructions: list[OrderInstruction] = []
    action = "open"
    if position.side != 0 and position.contracts > 0:
        action = "flip"
        instructions.extend(
            _build_close_instructions(
                position=position,
                instrument_config=instrument_config,
                trading_config=trading_config,
            )
        )
    instructions.append(
        _build_open_instruction(
            desired_side=signal.desired_side,
            target_contracts=target_contracts,
            entry_price=entry_price,
            stop_price=stop_price,
            instrument_config=instrument_config,
            trading_config=trading_config,
        )
    )
    if not trading_config.attach_stop_loss_on_entry:
        warnings.append("attach_stop_loss_on_entry=false. Entry orders will be sent without an attached stop.")

    return OrderPlan(
        action=action,
        reason="The latest confirmed signal differs from the current account position.",
        desired_side=signal.desired_side,
        current_side=position.side,
        current_contracts=position.contracts,
        target_contracts=target_contracts,
        equity_reference=equity_reference,
        latest_price=signal.latest_price,
        entry_price_estimate=entry_price,
        stop_price=stop_price,
        stop_distance=signal.stop_distance,
        signal_time=signal.signal_time,
        effective_time=signal.effective_time,
        position_mode=trading_config.position_mode,
        instructions=instructions,
        warnings=warnings,
    )


def build_account_snapshot(
    *,
    balance_payload: dict[str, Any] | None,
    account_config_payload: dict[str, Any] | None,
    settle_currency: str | None,
    fallback_equity: float,
) -> AccountSnapshot:
    if not balance_payload:
        return AccountSnapshot(
            total_equity=fallback_equity,
            available_equity=fallback_equity,
            currency=settle_currency,
            source="config",
        )

    data = balance_payload.get("data", [])
    row = data[0] if data else {}
    details = row.get("details") or []
    detail = next((item for item in details if item.get("ccy") == settle_currency), details[0] if details else {})

    account_config = (account_config_payload or {}).get("data", [])
    account_mode = account_config[0].get("posMode") if account_config else None

    total_equity = _to_float(row.get("totalEq"), fallback=fallback_equity)
    available_equity = _to_float(
        detail.get("availEq"),
        detail.get("eq"),
        row.get("availEq"),
        fallback=total_equity,
    )
    can_trade = None
    if account_config:
        perm = str(account_config[0].get("perm", ""))
        can_trade = "trade" in perm.lower() if perm else None

    return AccountSnapshot(
        total_equity=total_equity,
        available_equity=available_equity,
        currency=detail.get("ccy") or settle_currency,
        source="okx_balance",
        account_mode=account_mode,
        can_trade=can_trade,
        raw={
            "balance": balance_payload,
            "account_config": account_config_payload,
        },
    )


def build_position_snapshot(
    *,
    positions_payload: dict[str, Any] | None,
    inst_id: str,
    position_mode: str,
) -> PositionSnapshot:
    if not positions_payload:
        return PositionSnapshot(side=0, contracts=0.0, position_mode=position_mode)

    items = [
        row
        for row in positions_payload.get("data", [])
        if row.get("instId") == inst_id and abs(_to_float(row.get("pos"))) > 0
    ]
    if not items:
        return PositionSnapshot(side=0, contracts=0.0, position_mode=position_mode, raw=positions_payload)

    if position_mode == "long_short_mode" and len(items) > 1:
        raise ValueError("The planner does not support holding both long and short legs at the same time.")

    row = items[0]
    pos = _to_float(row.get("pos"))
    pos_side = row.get("posSide") or "net"
    direction = (row.get("direction") or "").lower()

    if pos_side == "long" or direction == "long":
        side = 1
    elif pos_side == "short" or direction == "short":
        side = -1
    else:
        side = -1 if pos < 0 else (1 if pos > 0 else 0)

    return PositionSnapshot(
        side=side,
        contracts=abs(pos),
        position_mode=position_mode,
        avg_price=_to_float(row.get("avgPx"), fallback=None),
        mark_price=_to_float(row.get("markPx"), fallback=None),
        raw=row,
    )


def extract_okx_max_size(max_size_payload: dict[str, Any] | None) -> tuple[float | None, float | None]:
    if not max_size_payload:
        return None, None
    rows = max_size_payload.get("data", [])
    row = rows[0] if rows else {}
    return _to_float(row.get("maxBuy"), fallback=None), _to_float(row.get("maxSell"), fallback=None)


def _build_close_instructions(
    *,
    position: PositionSnapshot,
    instrument_config: InstrumentConfig,
    trading_config: TradingConfig,
) -> list[OrderInstruction]:
    if position.side == 0 or position.contracts <= 0:
        return []
    if trading_config.position_mode == "long_short_mode":
        return [
            OrderInstruction(
                purpose="close_existing",
                inst_id=instrument_config.symbol,
                td_mode=trading_config.td_mode,
                side="sell" if position.side > 0 else "buy",
                ord_type=trading_config.order_type,
                size=position.contracts,
                reduce_only=False,
                pos_side="long" if position.side > 0 else "short",
            )
        ]
    return [
        OrderInstruction(
            purpose="close_existing",
            inst_id=instrument_config.symbol,
            td_mode=trading_config.td_mode,
            side="sell" if position.side > 0 else "buy",
            ord_type=trading_config.order_type,
            size=position.contracts,
            reduce_only=True,
            pos_side="net",
        )
    ]


def _build_open_instruction(
    *,
    desired_side: int,
    target_contracts: float,
    entry_price: float,
    stop_price: float,
    instrument_config: InstrumentConfig,
    trading_config: TradingConfig,
) -> OrderInstruction:
    side = "buy" if desired_side > 0 else "sell"
    pos_side = None
    if trading_config.position_mode == "long_short_mode":
        pos_side = "long" if desired_side > 0 else "short"
    elif trading_config.position_mode == "net_mode":
        pos_side = "net"

    attached_algo_orders = []
    if trading_config.attach_stop_loss_on_entry:
        attached_algo_orders.append(
            AttachedAlgoOrder(
                sl_trigger_px=stop_price,
                sl_trigger_px_type=trading_config.stop_trigger_price_type,
            )
        )

    return OrderInstruction(
        purpose="open_target",
        inst_id=instrument_config.symbol,
        td_mode=trading_config.td_mode,
        side=side,
        ord_type=trading_config.order_type,
        size=target_contracts,
        reduce_only=False,
        pos_side=pos_side,
        estimated_fill_price=entry_price,
        stop_price=stop_price,
        attach_algo_orders=attached_algo_orders,
    )


def _normalize_market_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True)
    for column in ("open", "high", "low", "close", "volume", "volume_ccy", "volume_quote"):
        if column in normalized.columns:
            normalized[column] = normalized[column].astype(float)
    return normalized.sort_values("timestamp").reset_index(drop=True)


def _round_down_to_lot(value: float, lot_size: float) -> float:
    if lot_size <= 0:
        return value
    return (value // lot_size) * lot_size


def _signal_stop_price(signal_row: pd.Series) -> float | None:
    value = signal_row.get("stop_price")
    if value is None or pd.isna(value):
        return None
    return float(value)


def _to_float(*values: Any, fallback: float | None = 0.0) -> float | None:
    for value in values:
        if value in {None, "", " "}:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return fallback


def _format_contracts(value: float) -> str:
    return format(value, "f").rstrip("0").rstrip(".") or "0"
