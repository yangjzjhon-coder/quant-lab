from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import product

import pandas as pd

from quant_lab.backtest.engine import (
    _build_funding_events,
    _build_signal_events,
    _exit_fill_price,
    _funding_change_for_event,
    _impact_rate,
    _initial_stop_price,
    _entry_fill_price,
    _normalize_funding_frame,
    _normalize_market_frame,
    _optional_float,
    _raw_stop_exit_price,
    _stop_was_hit,
    _tighten_stop_price,
)
from quant_lab.backtest.realism import (
    bar_liquidity_quote,
    cap_contracts_by_liquidity,
)
from quant_lab.config import ExecutionConfig, InstrumentConfig, RiskConfig, StrategyConfig
from quant_lab.models import Position
from quant_lab.risk.rules import WeeklyDrawdownGuard, position_size_from_risk
from quant_lab.strategies.ema_trend import prepare_signal_frame


def run_parameter_sweep(
    signal_bars: pd.DataFrame,
    execution_bars: pd.DataFrame,
    funding_rates: pd.DataFrame,
    strategy_config: StrategyConfig,
    execution_config: ExecutionConfig,
    risk_config: RiskConfig,
    instrument_config: InstrumentConfig,
    fast_values: list[int],
    slow_values: list[int],
    atr_values: list[float],
) -> pd.DataFrame:
    normalized_signal = _normalize_market_frame(signal_bars)
    normalized_execution = _normalize_market_frame(execution_bars)
    normalized_funding = _normalize_funding_frame(funding_rates)

    rows: list[dict[str, float | int | str | None]] = []

    for fast_ema, slow_ema, atr_multiple in product(
        sorted(set(fast_values)),
        sorted(set(slow_values)),
        sorted(set(atr_values)),
    ):
        if fast_ema >= slow_ema:
            continue

        candidate = strategy_config.model_copy(
            update={
                "fast_ema": fast_ema,
                "slow_ema": slow_ema,
                "atr_stop_multiple": atr_multiple,
            }
        )
        summary = _run_backtest_summary_only(
            signal_bars=normalized_signal,
            execution_bars=normalized_execution,
            funding_rates=normalized_funding,
            strategy_config=candidate,
            execution_config=execution_config,
            risk_config=risk_config,
            instrument_config=instrument_config,
        )

        max_drawdown = float(summary["max_drawdown_pct"])
        total_return = float(summary["total_return_pct"])
        score = round(total_return / max(max_drawdown, 0.01), 4)

        rows.append(
            {
                "strategy_name": candidate.name,
                "fast_ema": fast_ema,
                "slow_ema": slow_ema,
                "atr_stop_multiple": atr_multiple,
                "score_return_over_dd": score,
                **summary,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    return frame.sort_values(
        by=["sharpe", "score_return_over_dd", "total_return_pct"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def _run_backtest_summary_only(
    signal_bars: pd.DataFrame,
    execution_bars: pd.DataFrame,
    funding_rates: pd.DataFrame,
    strategy_config: StrategyConfig,
    execution_config: ExecutionConfig,
    risk_config: RiskConfig,
    instrument_config: InstrumentConfig,
) -> dict[str, float | int | None]:
    signal_frame = prepare_signal_frame(signal_bars, strategy_config)
    events = _build_signal_events(
        signal_frame,
        execution_bars,
        strategy_config.signal_bar,
        execution_config.latency_minutes,
    )
    funding_events = _build_funding_events(
        execution_bars=execution_bars,
        funding_rates=funding_rates,
        interval_hours=execution_config.funding_interval_hours,
    )

    fee_rate = execution_config.fee_bps / 10_000

    guard = WeeklyDrawdownGuard(threshold=risk_config.weekly_drawdown_pause)
    cash = execution_config.initial_equity
    position: Position | None = None

    trade_count = 0
    wins = 0
    gross_profit = 0.0
    gross_loss = 0.0
    all_time_peak = cash
    max_drawdown = 0.0
    returns_stats = _ReturnStats()
    previous_equity: float | None = None
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

        funding_event = funding_events.get(timestamp)
        if position is not None and funding_event is not None:
            funding_change = _funding_change_for_event(
                side=position.side,
                contracts=position.contracts,
                contract_value=position.contract_value,
                price=bar_open,
                actual_rates=funding_event,
                fallback_rate_bps=execution_config.missing_funding_rate_bps,
            )
            cash += funding_change
            position.funding_paid += funding_change

        event = events.get(timestamp)
        if event is not None:
            desired_side = int(event["desired_side"])
            stop_distance = float(event["stop_distance"])
            managed_stop_price = _optional_float(event.get("stop_price"))
            side_changed = bool(event.get("side_changed"))

            if position is not None and position.side != desired_side:
                cash, trade_net = _close_position_summary(
                    cash=cash,
                    position=position,
                    raw_exit_price=bar_open,
                    fee_rate=fee_rate,
                    liquidity_quote=liquidity_quote,
                    bar_high=bar_high,
                    bar_low=bar_low,
                    execution_config=execution_config,
                )
                trade_count, wins, gross_profit, gross_loss = _update_trade_stats(
                    trade_net, trade_count, wins, gross_profit, gross_loss
                )
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
                    risk_fraction=risk_config.risk_per_trade,
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
                        signal_time=pd.Timestamp(event["signal_time"]),
                        entry_price=entry_price,
                        stop_price=stop_price,
                        entry_fee=entry_fee,
                    )

        if position is not None and _stop_was_hit(position, bar_high, bar_low):
            raw_stop_price = _raw_stop_exit_price(position.side, position.stop_price, bar_open)
            cash, trade_net = _close_position_summary(
                cash=cash,
                position=position,
                raw_exit_price=raw_stop_price,
                fee_rate=fee_rate,
                liquidity_quote=liquidity_quote,
                bar_high=bar_high,
                bar_low=bar_low,
                execution_config=execution_config,
            )
            trade_count, wins, gross_profit, gross_loss = _update_trade_stats(
                trade_net, trade_count, wins, gross_profit, gross_loss
            )
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
            cash, trade_net = _close_position_summary(
                cash=cash,
                position=position,
                raw_exit_price=raw_liquidation_price,
                fee_rate=fee_rate,
                liquidity_quote=liquidity_quote,
                bar_high=bar_high,
                bar_low=bar_low,
                execution_config=execution_config,
            )
            cash = max(cash, 0.0)
            trade_count, wins, gross_profit, gross_loss = _update_trade_stats(
                trade_net, trade_count, wins, gross_profit, gross_loss
            )
            position = None
            unrealized = 0.0
            liquidated = True

        equity = cash + unrealized
        guard.update(timestamp, equity)
        all_time_peak = max(all_time_peak, equity)
        if all_time_peak > 0:
            max_drawdown = max(max_drawdown, (all_time_peak - equity) / all_time_peak)
        current_return = 0.0 if previous_equity in {None, 0} else (equity / previous_equity) - 1.0
        returns_stats.update(current_return)
        previous_equity = equity
        if liquidated:
            break

    final_equity = previous_equity if previous_equity is not None else execution_config.initial_equity
    if position is not None:
        last_close = float(execution_bars.iloc[-1]["close"])
        cash, trade_net = _close_position_summary(
            cash=cash,
            position=position,
            raw_exit_price=last_close,
            fee_rate=fee_rate,
            liquidity_quote=bar_liquidity_quote(
                price=last_close,
                contract_value=instrument_config.contract_value,
                volume=_optional_float(execution_bars.iloc[-1].get("volume")),
                volume_ccy=_optional_float(execution_bars.iloc[-1].get("volume_ccy")),
                volume_quote=_optional_float(execution_bars.iloc[-1].get("volume_quote")),
            ),
            bar_high=float(execution_bars.iloc[-1]["high"]),
            bar_low=float(execution_bars.iloc[-1]["low"]),
            execution_config=execution_config,
        )
        trade_count, wins, gross_profit, gross_loss = _update_trade_stats(
            trade_net, trade_count, wins, gross_profit, gross_loss
        )
        final_equity = cash
        all_time_peak = max(all_time_peak, final_equity)
        if all_time_peak > 0:
            max_drawdown = max(max_drawdown, (all_time_peak - final_equity) / all_time_peak)
        current_return = 0.0 if previous_equity in {None, 0} else (final_equity / previous_equity) - 1.0
        returns_stats.update(current_return)

    start_ts = pd.Timestamp(execution_bars["timestamp"].iloc[0])
    end_ts = pd.Timestamp(execution_bars["timestamp"].iloc[-1])
    period_days = max((end_ts - start_ts).total_seconds() / 86400, 1)
    total_return = (final_equity / execution_config.initial_equity) - 1.0
    annualized_return = (
        (1 + total_return) ** (365 / period_days) - 1 if total_return > -1 else -1.0
    )
    gross_loss_abs = abs(gross_loss)
    profit_factor = gross_profit / gross_loss_abs if gross_loss_abs > 0 else math.inf if gross_profit > 0 else 0.0
    win_rate = wins / trade_count if trade_count else 0.0

    return {
        "initial_equity": round(execution_config.initial_equity, 2),
        "final_equity": round(final_equity, 2),
        "total_return_pct": round(total_return * 100, 2),
        "annualized_return_pct": round(annualized_return * 100, 2),
        "max_drawdown_pct": round(max_drawdown * 100, 2),
        "trade_count": trade_count,
        "win_rate_pct": round(win_rate * 100, 2),
        "profit_factor": _safe_metric(profit_factor),
        "sharpe": round(returns_stats.sharpe(), 2),
    }


def _close_position_summary(
    cash: float,
    position: Position,
    raw_exit_price: float,
    fee_rate: float,
    liquidity_quote: float,
    bar_high: float,
    bar_low: float,
    execution_config: ExecutionConfig,
) -> tuple[float, float]:
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
    return cash, net_pnl


def _update_trade_stats(
    trade_net: float,
    trade_count: int,
    wins: int,
    gross_profit: float,
    gross_loss: float,
) -> tuple[int, int, float, float]:
    trade_count += 1
    if trade_net > 0:
        wins += 1
        gross_profit += trade_net
    elif trade_net < 0:
        gross_loss += trade_net
    return trade_count, wins, gross_profit, gross_loss


def _safe_metric(value: float) -> float | None:
    if not math.isfinite(value):
        return None
    return round(value, 2)


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


@dataclass
class _ReturnStats:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

    def sharpe(self) -> float:
        if self.count < 2:
            return 0.0
        variance = self.m2 / (self.count - 1)
        std = math.sqrt(variance)
        if std == 0:
            return 0.0
        minutes_per_year = 365 * 24 * 60
        return (self.mean / std) * math.sqrt(minutes_per_year)
