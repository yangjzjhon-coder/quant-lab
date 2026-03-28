from __future__ import annotations

import pandas as pd

from quant_lab.backtest.realism import (
    bar_liquidity_quote,
    cap_contracts_by_liquidity,
    conservative_funding_change,
    estimate_fill_bps,
)
from quant_lab.config import ExecutionConfig, InstrumentConfig, RiskConfig, StrategyConfig
from quant_lab.models import BacktestArtifacts, Position, TradeRecord
from quant_lab.risk.rules import WeeklyDrawdownGuard, position_size_from_risk
from quant_lab.strategies.ema_trend import prepare_signal_frame
from quant_lab.utils.timeframes import bar_to_timedelta


def run_backtest(
    signal_bars: pd.DataFrame,
    execution_bars: pd.DataFrame,
    funding_rates: pd.DataFrame,
    strategy_config: StrategyConfig,
    execution_config: ExecutionConfig,
    risk_config: RiskConfig,
    instrument_config: InstrumentConfig,
) -> BacktestArtifacts:
    signal_bars = _normalize_market_frame(signal_bars)
    execution_bars = _normalize_market_frame(execution_bars)
    funding_rates = _normalize_funding_frame(funding_rates)

    signal_frame = prepare_signal_frame(signal_bars, strategy_config)
    return run_backtest_from_signal_frame(
        signal_frame=signal_frame,
        execution_bars=execution_bars,
        funding_rates=funding_rates,
        execution_config=execution_config,
        risk_config=risk_config,
        instrument_config=instrument_config,
    )


def run_backtest_from_signal_frame(
    *,
    signal_frame: pd.DataFrame,
    execution_bars: pd.DataFrame,
    funding_rates: pd.DataFrame,
    execution_config: ExecutionConfig,
    risk_config: RiskConfig,
    instrument_config: InstrumentConfig,
) -> BacktestArtifacts:
    signal_frame = _normalize_signal_frame(signal_frame)
    execution_bars = _normalize_market_frame(execution_bars)
    funding_rates = _normalize_funding_frame(funding_rates)

    events = _build_signal_events(
        signal_frame,
        execution_bars,
        _infer_signal_bar(signal_frame),
        execution_config.latency_minutes,
    )
    funding_lookup = {row.timestamp: row.realized_rate for row in funding_rates.itertuples(index=False)}
    funding_schedule = _expected_funding_schedule(execution_bars, execution_config.funding_interval_hours)

    fee_rate = execution_config.fee_bps / 10_000

    guard = WeeklyDrawdownGuard(threshold=risk_config.weekly_drawdown_pause)
    cash = execution_config.initial_equity
    position: Position | None = None
    trades: list[TradeRecord] = []
    equity_rows: list[dict[str, object]] = []
    liquidated = False

    for bar in execution_bars.itertuples(index=False):
        timestamp = bar.timestamp
        bar_open = float(bar.open)
        bar_high = float(bar.high)
        bar_low = float(bar.low)
        bar_close = float(bar.close)
        liquidity_quote = bar_liquidity_quote(
            price=bar_open,
            contract_value=instrument_config.contract_value,
            volume=_optional_float(getattr(bar, "volume", None)),
            volume_ccy=_optional_float(getattr(bar, "volume_ccy", None)),
            volume_quote=_optional_float(getattr(bar, "volume_quote", None)),
        )

        if position is not None and timestamp in funding_schedule:
            funding_change = conservative_funding_change(
                side=position.side,
                contracts=position.contracts,
                contract_value=position.contract_value,
                price=bar_open,
                actual_rate=funding_lookup.get(timestamp),
                fallback_rate_bps=execution_config.missing_funding_rate_bps,
            )
            cash += funding_change
            position.funding_paid += funding_change

        event = events.get(timestamp)
        if event is not None:
            desired_side = int(event["desired_side"])
            stop_distance = float(event["stop_distance"])
            signal_time = pd.Timestamp(event["signal_time"])
            managed_stop_price = _optional_float(event.get("stop_price"))
            side_changed = bool(event.get("side_changed"))
            strategy_risk_multiplier = float(event.get("strategy_risk_multiplier") or 1.0)

            if position is not None and position.side != desired_side:
                cash, trade = _close_position(
                    cash=cash,
                    position=position,
                    symbol=instrument_config.symbol,
                    exit_time=timestamp,
                    raw_exit_price=bar_open,
                    fee_rate=fee_rate,
                    exit_reason="signal_flip" if desired_side != 0 else "signal_flat",
                    liquidity_quote=liquidity_quote,
                    bar_high=bar_high,
                    bar_low=bar_low,
                    execution_config=execution_config,
                )
                trades.append(trade)
                position = None

            if position is not None and position.side == desired_side and desired_side != 0 and managed_stop_price is not None:
                position.stop_price = _tighten_stop_price(
                    side=position.side,
                    current_stop=position.stop_price,
                    candidate_stop=managed_stop_price,
                )

            if position is None and desired_side != 0 and side_changed and not guard.halted:
                provisional_entry_price = _entry_fill_price(
                    desired_side,
                    bar_open,
                    execution_config.slippage_bps / 10_000,
                )
                provisional_stop_price = _initial_stop_price(
                    side=desired_side,
                    entry_price=provisional_entry_price,
                    stop_distance=stop_distance,
                    managed_stop_price=managed_stop_price,
                )
                desired_contracts = position_size_from_risk(
                    equity=cash,
                    entry_price=provisional_entry_price,
                    stop_price=provisional_stop_price,
                    risk_fraction=risk_config.risk_per_trade * max(0.0, strategy_risk_multiplier),
                    max_leverage=execution_config.max_leverage,
                    contract_value=instrument_config.contract_value,
                    lot_size=instrument_config.lot_size,
                    min_size=instrument_config.min_size,
                    minimum_notional=execution_config.minimum_notional,
                )
                contracts = cap_contracts_by_liquidity(
                    desired_contracts=desired_contracts,
                    price=bar_open,
                    contract_value=instrument_config.contract_value,
                    liquidity_quote=liquidity_quote,
                    max_bar_participation=execution_config.max_bar_participation,
                    lot_size=instrument_config.lot_size,
                )

                if contracts > 0:
                    entry_price = _entry_fill_price(
                        desired_side,
                        bar_open,
                        _impact_rate(
                            price=bar_open,
                            high=bar_high,
                            low=bar_low,
                            contracts=contracts,
                            contract_value=instrument_config.contract_value,
                            liquidity_quote=liquidity_quote,
                            execution_config=execution_config,
                        ),
                    )
                    stop_price = _initial_stop_price(
                        side=desired_side,
                        entry_price=entry_price,
                        stop_distance=stop_distance,
                        managed_stop_price=managed_stop_price,
                    )
                    entry_fee = contracts * instrument_config.contract_value * entry_price * fee_rate
                    cash -= entry_fee
                    position = Position(
                        side=desired_side,
                        contracts=contracts,
                        contract_value=instrument_config.contract_value,
                        entry_time=timestamp,
                        signal_time=signal_time,
                        entry_price=entry_price,
                        stop_price=stop_price,
                        entry_fee=entry_fee,
                    )

        if position is not None and _stop_was_hit(position, bar_high, bar_low):
            raw_stop_price = _raw_stop_exit_price(position.side, position.stop_price, bar_open)
            cash, trade = _close_position(
                cash=cash,
                position=position,
                symbol=instrument_config.symbol,
                exit_time=timestamp,
                raw_exit_price=raw_stop_price,
                fee_rate=fee_rate,
                exit_reason="stop_loss",
                liquidity_quote=liquidity_quote,
                bar_high=bar_high,
                bar_low=bar_low,
                execution_config=execution_config,
            )
            trades.append(trade)
            position = None

        unrealized = position.unrealized_pnl(bar_close) if position is not None else 0.0
        if position is not None and cash + unrealized <= 0:
            raw_liquidation_price = _raw_liquidation_exit_price(
                position=position,
                cash=cash,
                fee_rate=fee_rate,
                bar_high=bar_high,
                bar_low=bar_low,
                fallback_price=bar_close,
            )
            cash, trade = _close_position(
                cash=cash,
                position=position,
                symbol=instrument_config.symbol,
                exit_time=timestamp,
                raw_exit_price=raw_liquidation_price,
                fee_rate=fee_rate,
                exit_reason="liquidation",
                liquidity_quote=liquidity_quote,
                bar_high=bar_high,
                bar_low=bar_low,
                execution_config=execution_config,
            )
            cash = max(cash, 0.0)
            trades.append(trade)
            position = None
            unrealized = 0.0
            liquidated = True

        equity = cash + unrealized
        halted = guard.update(timestamp, equity)
        equity_rows.append(
            {
                "timestamp": timestamp,
                "cash": round(cash, 8),
                "equity": round(equity, 8),
                "unrealized_pnl": round(unrealized, 8),
                "halted": halted,
                "position_side": position.side if position is not None else 0,
                "contracts": position.contracts if position is not None else 0.0,
            }
        )
        if liquidated:
            break

    if position is not None:
        last_bar = execution_bars.iloc[-1]
        cash, trade = _close_position(
            cash=cash,
            position=position,
            symbol=instrument_config.symbol,
            exit_time=pd.Timestamp(last_bar["timestamp"]),
            raw_exit_price=float(last_bar["close"]),
            fee_rate=fee_rate,
            exit_reason="end_of_test",
            liquidity_quote=bar_liquidity_quote(
                price=float(last_bar["close"]),
                contract_value=instrument_config.contract_value,
                volume=_optional_float(last_bar.get("volume")),
                volume_ccy=_optional_float(last_bar.get("volume_ccy")),
                volume_quote=_optional_float(last_bar.get("volume_quote")),
            ),
            bar_high=float(last_bar["high"]),
            bar_low=float(last_bar["low"]),
            execution_config=execution_config,
        )
        trades.append(trade)
        equity_rows.append(
            {
                "timestamp": pd.Timestamp(last_bar["timestamp"]),
                "cash": round(cash, 8),
                "equity": round(cash, 8),
                "unrealized_pnl": 0.0,
                "halted": guard.halted,
                "position_side": 0,
                "contracts": 0.0,
            }
        )

    equity_curve = (
        pd.DataFrame(equity_rows)
        .drop_duplicates(subset=["timestamp"], keep="last")
        .sort_values("timestamp")
    )
    return BacktestArtifacts(
        trades=trades,
        equity_curve=equity_curve.reset_index(drop=True),
        signal_frame=signal_frame,
    )


def _build_signal_events(
    signal_frame: pd.DataFrame,
    execution_bars: pd.DataFrame,
    signal_bar: str,
    latency_minutes: int,
) -> dict[pd.Timestamp, dict[str, object]]:
    frame = signal_frame.copy()
    frame["previous_side"] = frame["desired_side"].shift(1).fillna(0).astype(int)
    if "stop_price" in frame.columns:
        frame["previous_stop_price"] = frame["stop_price"].shift(1)
        stop_updates = frame.apply(
            lambda row: _managed_stop_updated(
                side=int(row["desired_side"]),
                current_stop=_optional_float(row["stop_price"]),
                previous_stop=_optional_float(row["previous_stop_price"]),
            ),
            axis=1,
        )
    else:
        frame["stop_price"] = pd.NA
        stop_updates = pd.Series(False, index=frame.index)

    changes = frame.loc[(frame["desired_side"] != frame["previous_side"]) | stop_updates].copy()
    if changes.empty:
        return {}

    desired_times = (
        changes["timestamp"]
        + bar_to_timedelta(signal_bar)
        + pd.to_timedelta(latency_minutes, unit="m")
    )
    execution_index = pd.DatetimeIndex(execution_bars["timestamp"])

    events: dict[pd.Timestamp, dict[str, object]] = {}
    for row, desired_time in zip(changes.itertuples(index=False), desired_times, strict=False):
        position = execution_index.searchsorted(desired_time)
        if position >= len(execution_index):
            continue

        effective_time = pd.Timestamp(execution_index[position])
        events[effective_time] = {
            "signal_time": pd.Timestamp(row.timestamp),
            "desired_side": int(row.desired_side),
            "stop_distance": float(row.stop_distance),
            "stop_price": _optional_float(getattr(row, "stop_price", None)),
            "side_changed": int(row.desired_side) != int(row.previous_side),
            "strategy_score": _optional_float(getattr(row, "strategy_score", None)),
            "strategy_risk_multiplier": _optional_float(getattr(row, "strategy_risk_multiplier", None)) or 1.0,
        }

    return events


def _close_position(
    cash: float,
    position: Position,
    symbol: str,
    exit_time: pd.Timestamp,
    raw_exit_price: float,
    fee_rate: float,
    exit_reason: str,
    liquidity_quote: float,
    bar_high: float,
    bar_low: float,
    execution_config: ExecutionConfig,
) -> tuple[float, TradeRecord]:
    exit_price = _exit_fill_price(
        position.side,
        raw_exit_price,
        _impact_rate(
            price=raw_exit_price,
            high=bar_high,
            low=bar_low,
            contracts=position.contracts,
            contract_value=position.contract_value,
            liquidity_quote=liquidity_quote,
            execution_config=execution_config,
        ),
    )
    gross_pnl = position.side * position.contracts * position.contract_value * (
        exit_price - position.entry_price
    )
    exit_fee = position.contracts * position.contract_value * exit_price * fee_rate
    cash += gross_pnl - exit_fee
    cash = max(cash, 0.0)
    total_fees = position.entry_fee + exit_fee
    net_pnl = gross_pnl + position.funding_paid - total_fees

    trade = TradeRecord(
        signal_time=position.signal_time,
        entry_time=position.entry_time,
        exit_time=exit_time,
        side="long" if position.side == 1 else "short",
        contracts=position.contracts,
        entry_price=round(position.entry_price, 8),
        exit_price=round(exit_price, 8),
        stop_price=round(position.stop_price, 8),
        gross_pnl=round(gross_pnl, 8),
        funding_pnl=round(position.funding_paid, 8),
        fee_paid=round(total_fees, 8),
        net_pnl=round(net_pnl, 8),
        exit_reason=exit_reason,
        symbol=symbol,
    )
    return cash, trade


def _entry_fill_price(side: int, raw_price: float, slippage_rate: float) -> float:
    return raw_price * (1 + slippage_rate) if side == 1 else raw_price * (1 - slippage_rate)


def _exit_fill_price(side: int, raw_price: float, slippage_rate: float) -> float:
    return raw_price * (1 - slippage_rate) if side == 1 else raw_price * (1 + slippage_rate)


def _stop_was_hit(position: Position, bar_high: float, bar_low: float) -> bool:
    if position.side == 1:
        return bar_low <= position.stop_price
    return bar_high >= position.stop_price


def _raw_stop_exit_price(side: int, stop_price: float, bar_open: float) -> float:
    if side == 1:
        return min(stop_price, bar_open)
    return max(stop_price, bar_open)


def _initial_stop_price(
    *,
    side: int,
    entry_price: float,
    stop_distance: float,
    managed_stop_price: float | None,
) -> float:
    base_stop = entry_price - (side * stop_distance)
    if managed_stop_price is None:
        return base_stop
    return _tighten_stop_price(side=side, current_stop=base_stop, candidate_stop=managed_stop_price)


def _tighten_stop_price(*, side: int, current_stop: float, candidate_stop: float) -> float:
    if side == 1:
        return max(current_stop, candidate_stop)
    return min(current_stop, candidate_stop)


def _managed_stop_updated(*, side: int, current_stop: float | None, previous_stop: float | None) -> bool:
    if side == 0 or current_stop is None:
        return False
    if previous_stop is None:
        return True
    if side == 1:
        return current_stop > previous_stop
    return current_stop < previous_stop


def _raw_liquidation_exit_price(
    *,
    position: Position,
    cash: float,
    fee_rate: float,
    bar_high: float,
    bar_low: float,
    fallback_price: float,
) -> float:
    exposure = position.contracts * position.contract_value
    if exposure <= 0:
        return fallback_price

    if position.side == 1:
        denominator = exposure * max(1 - fee_rate, 1e-9)
        price = ((exposure * position.entry_price) - cash) / denominator
        return min(max(price, bar_low), bar_high)

    denominator = exposure * (1 + fee_rate)
    price = (cash + (exposure * position.entry_price)) / max(denominator, 1e-9)
    return min(max(price, bar_low), bar_high)


def _normalize_market_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True)
    for column in ("open", "high", "low", "close", "volume", "volume_ccy", "volume_quote"):
        if column in normalized.columns:
            normalized[column] = normalized[column].astype(float)
    return normalized.sort_values("timestamp").reset_index(drop=True)


def _normalize_funding_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "realized_rate"])
    normalized = frame.copy()
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True)
    normalized["realized_rate"] = normalized["realized_rate"].astype(float)
    return normalized[["timestamp", "realized_rate"]].sort_values("timestamp").reset_index(drop=True)


def _normalize_signal_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if "timestamp" not in normalized.columns:
        raise ValueError("signal_frame must contain a timestamp column")
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True)

    required_columns = {"desired_side", "stop_distance"}
    missing = [column for column in sorted(required_columns) if column not in normalized.columns]
    if missing:
        raise ValueError(f"signal_frame is missing required columns: {', '.join(missing)}")

    normalized["desired_side"] = normalized["desired_side"].fillna(0).astype(int)
    normalized["stop_distance"] = pd.to_numeric(normalized["stop_distance"], errors="coerce").fillna(0.0)

    for column in ("stop_price", "strategy_score", "strategy_risk_multiplier"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    if "strategy_risk_multiplier" not in normalized.columns:
        normalized["strategy_risk_multiplier"] = 1.0
    else:
        normalized["strategy_risk_multiplier"] = normalized["strategy_risk_multiplier"].fillna(1.0)

    return normalized.sort_values("timestamp").reset_index(drop=True)


def _impact_rate(
    *,
    price: float,
    high: float,
    low: float,
    contracts: float,
    contract_value: float,
    liquidity_quote: float,
    execution_config: ExecutionConfig,
) -> float:
    bps = estimate_fill_bps(
        price=price,
        high=high,
        low=low,
        order_contracts=contracts,
        contract_value=contract_value,
        liquidity_quote=liquidity_quote,
        base_slippage_bps=execution_config.slippage_bps,
        market_impact_bps=execution_config.market_impact_bps,
        excess_impact_bps=execution_config.excess_impact_bps,
        volatility_impact_share=execution_config.volatility_impact_share,
        max_bar_participation=execution_config.max_bar_participation,
    )
    return bps / 10_000


def _expected_funding_schedule(execution_bars: pd.DataFrame, interval_hours: int) -> set[pd.Timestamp]:
    if execution_bars.empty or interval_hours <= 0:
        return set()
    start = pd.Timestamp(execution_bars["timestamp"].min()).floor(f"{interval_hours}h")
    end = pd.Timestamp(execution_bars["timestamp"].max()).ceil(f"{interval_hours}h")
    schedule = pd.date_range(start=start, end=end, freq=f"{interval_hours}h", tz="UTC")
    execution_index = set(pd.DatetimeIndex(execution_bars["timestamp"]))
    return {pd.Timestamp(ts) for ts in schedule if pd.Timestamp(ts) in execution_index}


def _optional_float(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _infer_signal_bar(signal_frame: pd.DataFrame) -> str:
    signal_bar = signal_frame.attrs.get("signal_bar")
    if isinstance(signal_bar, str) and signal_bar.strip():
        return signal_bar

    timestamps = pd.DatetimeIndex(signal_frame["timestamp"]).sort_values()
    if len(timestamps) >= 2:
        deltas = pd.Series(timestamps[1:] - timestamps[:-1])
        if not deltas.empty:
            modal = deltas.mode()
            if not modal.empty:
                delta = pd.Timedelta(modal.iloc[0])
                minutes = int(delta.total_seconds() // 60)
                if minutes > 0:
                    return _timedelta_to_bar(minutes)
    return "4H"


def _timedelta_to_bar(total_minutes: int) -> str:
    units = (
        ("D", 24 * 60),
        ("H", 60),
        ("m", 1),
    )
    for suffix, divisor in units:
        if total_minutes % divisor == 0:
            value = total_minutes // divisor
            if value > 0:
                return f"{value}{suffix}"
    return f"{total_minutes}m"
