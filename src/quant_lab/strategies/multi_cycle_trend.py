from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from math import sqrt
from typing import Any

import pandas as pd


PRICE_COLUMNS = ("open", "high", "low", "close")


@dataclass(frozen=True)
class MultiCycleTrendParameters:
    synthetic_window: int = 4
    touch_ema_span: int = 12
    support_ema_span: int = 144
    trend_fast_ema_spans: tuple[int, int] = (144, 169)
    trend_slow_ema_spans: tuple[int, int] = (575, 676)
    monthly_ema_span: int = 12
    price_tick: float = 0.01
    margin_fraction: float = 0.20
    stop_loss_fraction_of_nominal: float = 0.50
    max_leverage: float = 3.0
    minimum_stop_distance_fraction: float = 0.02
    require_fresh_setup: bool = True
    require_current_close_above_touch_ema: bool = True
    fee_bps: float = 8.0
    contract_value: float = 0.01


def resample_intraday_to_daily(candles: pd.DataFrame) -> pd.DataFrame:
    return resample_intraday_to_timeframe(candles, rule="1D")


def resample_intraday_to_4h(candles: pd.DataFrame) -> pd.DataFrame:
    return resample_intraday_to_timeframe(candles, rule="4h")


def resample_intraday_to_timeframe(candles: pd.DataFrame, *, rule: str) -> pd.DataFrame:
    frame = _normalize_ohlcv_frame(candles)
    normalized_rule = rule.replace("H", "h")
    return (
        frame.set_index("timestamp")
        .resample(normalized_rule)
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "volume_ccy": "sum",
                "volume_quote": "sum",
            }
        )
        .dropna(subset=list(PRICE_COLUMNS))
        .reset_index()
    )


def generate_trading_signals(
    daily_bars: pd.DataFrame,
    *,
    params: MultiCycleTrendParameters | None = None,
) -> pd.DataFrame:
    cfg = params or MultiCycleTrendParameters()
    _validate_parameters(cfg)

    frame = _normalize_ohlcv_frame(daily_bars)
    frame["daily_bar_count"] = range(1, len(frame) + 1)

    required_ema_spans = {
        cfg.touch_ema_span,
        cfg.support_ema_span,
        cfg.monthly_ema_span,
        *cfg.trend_fast_ema_spans,
        *cfg.trend_slow_ema_spans,
    }
    for span in sorted(required_ema_spans):
        frame[f"ema_{span}"] = frame["close"].ewm(span=span, adjust=False).mean()

    monthly_context = _build_monthly_context(frame, cfg.monthly_ema_span)
    frame = pd.merge_asof(
        frame.sort_values("timestamp"),
        monthly_context.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )

    fast_a, fast_b = cfg.trend_fast_ema_spans
    slow_a, slow_b = cfg.trend_slow_ema_spans
    min_fast = frame[[f"ema_{fast_a}", f"ema_{fast_b}"]].min(axis=1)
    max_fast = frame[[f"ema_{fast_a}", f"ema_{fast_b}"]].max(axis=1)
    min_slow = frame[[f"ema_{slow_a}", f"ema_{slow_b}"]].min(axis=1)
    max_slow = frame[[f"ema_{slow_a}", f"ema_{slow_b}"]].max(axis=1)

    trend_ready = frame["daily_bar_count"] >= max(*cfg.trend_slow_ema_spans, cfg.synthetic_window)
    monthly_ready = frame["monthly_bar_count"].fillna(0) >= cfg.monthly_ema_span

    frame["monthly_bear_break"] = (
        monthly_ready
        & frame["monthly_close"].notna()
        & frame["monthly_ema"].notna()
        & (frame["monthly_close"] < frame["monthly_ema"])
    )
    frame["ema_bull_stack"] = trend_ready & min_fast.gt(max_slow)
    frame["ema_bear_stack"] = trend_ready & max_fast.lt(min_slow)

    frame["is_bull_trend"] = frame["ema_bull_stack"]
    frame["is_bear_trend"] = frame["monthly_bear_break"] | frame["ema_bear_stack"]
    frame["allow_long"] = frame["is_bull_trend"] & ~frame["is_bear_trend"]
    frame["trend_state"] = "neutral"
    frame.loc[frame["is_bear_trend"], "trend_state"] = "bear_trend"
    frame.loc[frame["allow_long"], "trend_state"] = "bull_trend"

    bar_valid = frame[list(PRICE_COLUMNS)].notna().all(axis=1)
    window_ready = (
        bar_valid.rolling(cfg.synthetic_window, min_periods=cfg.synthetic_window).min().fillna(0).astype(bool)
    )

    frame["synthetic_open"] = frame["open"].shift(cfg.synthetic_window - 1)
    frame["synthetic_high"] = frame["high"].rolling(cfg.synthetic_window, min_periods=cfg.synthetic_window).max()
    frame["synthetic_low"] = frame["low"].rolling(cfg.synthetic_window, min_periods=cfg.synthetic_window).min()
    frame["synthetic_close"] = frame["close"]

    close_above_touch = (
        frame["close"].notna()
        & frame[f"ema_{cfg.touch_ema_span}"].notna()
        & frame["close"].gt(frame[f"ema_{cfg.touch_ema_span}"])
    )
    frame["window_close_above_ema12"] = (
        close_above_touch.rolling(cfg.synthetic_window, min_periods=cfg.synthetic_window).max().fillna(0).astype(bool)
    )
    frame["synthetic_high_touches_ema12"] = (
        window_ready
        & frame["synthetic_high"].notna()
        & frame[f"ema_{cfg.touch_ema_span}"].notna()
        & frame["synthetic_high"].ge(frame[f"ema_{cfg.touch_ema_span}"])
    )
    frame["synthetic_low_touches_ema144"] = (
        window_ready
        & frame["synthetic_low"].notna()
        & frame[f"ema_{cfg.support_ema_span}"].notna()
        & frame["synthetic_low"].le(frame[f"ema_{cfg.support_ema_span}"])
    )
    frame["synthetic_setup"] = (
        frame["synthetic_high_touches_ema12"]
        & frame["synthetic_low_touches_ema144"]
        & frame["window_close_above_ema12"]
    )
    previous_setup = frame["synthetic_setup"].shift(1).fillna(False)
    frame["fresh_setup_signal"] = frame["synthetic_setup"] & previous_setup.ne(True)
    frame["current_close_above_ema12"] = (
        frame["close"].notna()
        & frame[f"ema_{cfg.touch_ema_span}"].notna()
        & frame["close"].gt(frame[f"ema_{cfg.touch_ema_span}"])
    )

    frame["take_profit_cross_ema144"] = _cross_under(
        frame[f"ema_{cfg.touch_ema_span}"],
        frame[f"ema_{cfg.support_ema_span}"],
    )
    frame["take_profit_cross_ema169"] = _cross_under(
        frame[f"ema_{cfg.touch_ema_span}"],
        frame[f"ema_{cfg.trend_fast_ema_spans[1]}"],
    )
    frame["take_profit_signal"] = frame["take_profit_cross_ema144"] | frame["take_profit_cross_ema169"]

    frame["stop_loss_price"] = frame["synthetic_low"].apply(lambda value: _stop_loss_price(value, cfg.price_tick))
    frame["signal_entry_price"] = frame["close"]
    frame["signal_leverage"] = frame.apply(
        lambda row: _safe_leverage(
            entry_price=row["signal_entry_price"],
            stop_price=row["stop_loss_price"],
            stop_loss_fraction_of_nominal=cfg.stop_loss_fraction_of_nominal,
            max_leverage=cfg.max_leverage,
            minimum_stop_distance_fraction=cfg.minimum_stop_distance_fraction,
        ),
        axis=1,
    )
    frame["nominal_margin_fraction"] = cfg.margin_fraction
    frame["max_loss_fraction_of_equity"] = cfg.margin_fraction * cfg.stop_loss_fraction_of_nominal

    entry_ready = frame["synthetic_setup"]
    if cfg.require_fresh_setup:
        entry_ready = entry_ready & frame["fresh_setup_signal"]
    if cfg.require_current_close_above_touch_ema:
        entry_ready = entry_ready & frame["current_close_above_ema12"]

    frame["open_long_signal"] = (
        frame["allow_long"]
        & entry_ready
        & ~frame["take_profit_signal"]
        & frame["signal_leverage"].notna()
        & frame["signal_leverage"].gt(0)
    )

    for column in ("stop_loss_price", "signal_entry_price", "signal_leverage"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    return frame


def run_multi_cycle_backtest(
    daily_bars: pd.DataFrame,
    *,
    params: MultiCycleTrendParameters | None = None,
    initial_equity: float = 10_000.0,
    symbol: str = "BTC-USDT-SWAP",
) -> dict[str, Any]:
    cfg = params or MultiCycleTrendParameters()
    signals = generate_trading_signals(daily_bars, params=cfg).reset_index(drop=True)

    fee_rate = cfg.fee_bps / 10_000.0
    cash = float(initial_equity)
    position: dict[str, Any] | None = None
    pending_entry: dict[str, Any] | None = None
    trades: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []

    for index, row in signals.iterrows():
        timestamp = pd.Timestamp(row["timestamp"])
        open_price = _coerce_float(row.get("open"))
        close_price = _coerce_float(row.get("close"))

        if pending_entry is not None and position is None and open_price is not None:
            stop_price = _coerce_float(pending_entry.get("stop_loss_price"))
            leverage = _safe_leverage(
                entry_price=open_price,
                stop_price=stop_price,
                stop_loss_fraction_of_nominal=cfg.stop_loss_fraction_of_nominal,
                max_leverage=cfg.max_leverage,
                minimum_stop_distance_fraction=cfg.minimum_stop_distance_fraction,
            )
            if stop_price is not None and leverage is not None and leverage > 0:
                equity_before_entry = cash
                margin_used = equity_before_entry * cfg.margin_fraction
                notional = margin_used * leverage
                quantity_btc = notional / open_price if open_price > 0 else 0.0
                entry_fee = notional * fee_rate
                cash -= entry_fee
                position = {
                    "signal_time": pending_entry["signal_time"],
                    "entry_time": timestamp,
                    "entry_price": open_price,
                    "stop_loss_price": stop_price,
                    "leverage": leverage,
                    "margin_used": margin_used,
                    "notional": notional,
                    "quantity_btc": quantity_btc,
                    "contracts": quantity_btc / cfg.contract_value if cfg.contract_value > 0 else quantity_btc,
                    "entry_fee": entry_fee,
                }
            pending_entry = None

        exit_reason: str | None = None
        exit_price: float | None = None
        unrealized_pnl = 0.0

        if position is not None and close_price is not None:
            unrealized_pnl = position["quantity_btc"] * (close_price - position["entry_price"])
            if close_price < position["stop_loss_price"]:
                exit_reason = "stop_loss_close"
                exit_price = close_price
            elif bool(row.get("take_profit_signal")):
                exit_reason = "take_profit_cross"
                exit_price = close_price

        if position is not None and exit_reason is not None and exit_price is not None:
            cash = _close_position(
                trades=trades,
                cash=cash,
                position=position,
                exit_time=timestamp,
                exit_price=exit_price,
                exit_reason=exit_reason,
                fee_rate=fee_rate,
                symbol=symbol,
            )
            position = None
            unrealized_pnl = 0.0

        equity_rows.append(_equity_row(timestamp=timestamp, cash=cash, position=position, unrealized_pnl=unrealized_pnl))

        if position is None and bool(row.get("open_long_signal")) and index + 1 < len(signals):
            pending_entry = {
                "signal_time": timestamp,
                "stop_loss_price": row.get("stop_loss_price"),
            }

    if position is not None:
        last_row = signals.iloc[-1]
        exit_timestamp = pd.Timestamp(last_row["timestamp"])
        exit_price = _coerce_float(last_row.get("close")) or float(position["entry_price"])
        cash = _close_position(
            trades=trades,
            cash=cash,
            position=position,
            exit_time=exit_timestamp,
            exit_price=exit_price,
            exit_reason="end_of_data",
            fee_rate=fee_rate,
            symbol=symbol,
        )
        if equity_rows:
            equity_rows[-1] = _equity_row(timestamp=exit_timestamp, cash=cash, position=None, unrealized_pnl=0.0)

    trades_frame = _ensure_trade_frame(trades)
    equity_curve = pd.DataFrame(equity_rows)
    summary = _build_summary(
        equity_curve=equity_curve,
        trades=trades_frame,
        initial_equity=initial_equity,
        signals=signals,
        params=cfg,
    )
    return {
        "signals": signals,
        "trades": trades_frame,
        "equity_curve": equity_curve,
        "summary": summary,
    }


def run_multi_cycle_backtest_on_4h(
    execution_bars: pd.DataFrame,
    *,
    params: MultiCycleTrendParameters | None = None,
    initial_equity: float = 10_000.0,
    symbol: str = "BTC-USDT-SWAP",
) -> dict[str, Any]:
    cfg = params or MultiCycleTrendParameters()
    execution = _normalize_ohlcv_frame(execution_bars)
    daily_signals = generate_trading_signals(resample_intraday_to_daily(execution), params=cfg).reset_index(drop=True)
    daily_signals["action_time"] = daily_signals["timestamp"] + pd.Timedelta(days=1)

    entry_actions = daily_signals.loc[daily_signals["open_long_signal"]].copy()
    take_profit_actions = daily_signals.loc[daily_signals["take_profit_signal"]].copy()

    fee_rate = cfg.fee_bps / 10_000.0
    cash = float(initial_equity)
    position: dict[str, Any] | None = None
    trades: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    entry_pointer = 0
    take_profit_pointer = 0

    for bar in execution.itertuples(index=False):
        timestamp = pd.Timestamp(bar.timestamp)
        open_price = _coerce_float(bar.open)
        close_price = _coerce_float(bar.close)

        if position is not None:
            while take_profit_pointer < len(take_profit_actions):
                action = take_profit_actions.iloc[take_profit_pointer]
                if pd.Timestamp(action["action_time"]) > timestamp:
                    break
                take_profit_pointer += 1
                if pd.Timestamp(action["timestamp"]) <= position["signal_time"]:
                    continue
                exit_price = open_price or position["entry_price"]
                cash = _close_position(
                    trades=trades,
                    cash=cash,
                    position=position,
                    exit_time=timestamp,
                    exit_price=exit_price,
                    exit_reason="take_profit_cross",
                    fee_rate=fee_rate,
                    symbol=symbol,
                )
                position = None
                break
        else:
            while take_profit_pointer < len(take_profit_actions):
                if pd.Timestamp(take_profit_actions.iloc[take_profit_pointer]["action_time"]) > timestamp:
                    break
                take_profit_pointer += 1

        if position is None:
            while entry_pointer < len(entry_actions):
                action = entry_actions.iloc[entry_pointer]
                if pd.Timestamp(action["action_time"]) > timestamp:
                    break
                entry_pointer += 1
                if open_price is None:
                    continue
                stop_price = _coerce_float(action["stop_loss_price"])
                leverage = _safe_leverage(
                    entry_price=open_price,
                    stop_price=stop_price,
                    stop_loss_fraction_of_nominal=cfg.stop_loss_fraction_of_nominal,
                    max_leverage=cfg.max_leverage,
                    minimum_stop_distance_fraction=cfg.minimum_stop_distance_fraction,
                )
                if stop_price is None or leverage is None or leverage <= 0:
                    continue
                margin_used = cash * cfg.margin_fraction
                notional = margin_used * leverage
                quantity_btc = notional / open_price if open_price > 0 else 0.0
                entry_fee = notional * fee_rate
                cash -= entry_fee
                position = {
                    "signal_time": pd.Timestamp(action["timestamp"]),
                    "entry_time": timestamp,
                    "entry_price": open_price,
                    "stop_loss_price": stop_price,
                    "leverage": leverage,
                    "margin_used": margin_used,
                    "notional": notional,
                    "quantity_btc": quantity_btc,
                    "contracts": quantity_btc / cfg.contract_value if cfg.contract_value > 0 else quantity_btc,
                    "entry_fee": entry_fee,
                }
                break
        else:
            while entry_pointer < len(entry_actions):
                if pd.Timestamp(entry_actions.iloc[entry_pointer]["action_time"]) > timestamp:
                    break
                entry_pointer += 1

        unrealized_pnl = 0.0
        if position is not None and close_price is not None:
            unrealized_pnl = position["quantity_btc"] * (close_price - position["entry_price"])
            if close_price < position["stop_loss_price"]:
                cash = _close_position(
                    trades=trades,
                    cash=cash,
                    position=position,
                    exit_time=timestamp,
                    exit_price=close_price,
                    exit_reason="stop_loss_close_4h",
                    fee_rate=fee_rate,
                    symbol=symbol,
                )
                position = None
                unrealized_pnl = 0.0

        equity_rows.append(_equity_row(timestamp=timestamp, cash=cash, position=position, unrealized_pnl=unrealized_pnl))

    if position is not None:
        last_bar = execution.iloc[-1]
        exit_timestamp = pd.Timestamp(last_bar["timestamp"])
        exit_price = _coerce_float(last_bar["close"]) or float(position["entry_price"])
        cash = _close_position(
            trades=trades,
            cash=cash,
            position=position,
            exit_time=exit_timestamp,
            exit_price=exit_price,
            exit_reason="end_of_data",
            fee_rate=fee_rate,
            symbol=symbol,
        )
        if equity_rows:
            equity_rows[-1] = _equity_row(timestamp=exit_timestamp, cash=cash, position=None, unrealized_pnl=0.0)

    trades_frame = _ensure_trade_frame(trades)
    equity_curve = pd.DataFrame(equity_rows)
    summary = _build_summary(
        equity_curve=equity_curve,
        trades=trades_frame,
        initial_equity=initial_equity,
        signals=daily_signals,
        params=cfg,
    )
    summary["execution_timeframe"] = "4H"
    summary["execution_bar_count"] = int(len(execution))
    return {
        "signals": daily_signals,
        "execution_bars": execution,
        "trades": trades_frame,
        "equity_curve": equity_curve,
        "summary": summary,
    }


def _close_position(
    *,
    trades: list[dict[str, Any]],
    cash: float,
    position: dict[str, Any],
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
    fee_rate: float,
    symbol: str,
) -> float:
    exit_notional = position["quantity_btc"] * exit_price
    exit_fee = exit_notional * fee_rate
    gross_pnl = position["quantity_btc"] * (exit_price - position["entry_price"])
    net_pnl = gross_pnl - position["entry_fee"] - exit_fee
    cash += gross_pnl - exit_fee
    trades.append(
        {
            "signal_time": position["signal_time"].isoformat(),
            "entry_time": position["entry_time"].isoformat(),
            "exit_time": exit_time.isoformat(),
            "side": "long",
            "contracts": round(position["contracts"], 8),
            "quantity_btc": round(position["quantity_btc"], 8),
            "entry_price": round(position["entry_price"], 8),
            "exit_price": round(exit_price, 8),
            "stop_price": round(position["stop_loss_price"], 8),
            "leverage": round(position["leverage"], 8),
            "margin_used": round(position["margin_used"], 8),
            "notional": round(position["notional"], 8),
            "gross_pnl": round(gross_pnl, 8),
            "funding_pnl": 0.0,
            "fee_paid": round(position["entry_fee"] + exit_fee, 8),
            "net_pnl": round(net_pnl, 8),
            "exit_reason": exit_reason,
            "symbol": symbol,
        }
    )
    return cash


def _equity_row(
    *,
    timestamp: pd.Timestamp,
    cash: float,
    position: dict[str, Any] | None,
    unrealized_pnl: float,
) -> dict[str, Any]:
    contracts = float(position["contracts"]) if position is not None else 0.0
    return {
        "timestamp": timestamp,
        "cash": round(cash, 8),
        "equity": round(cash + unrealized_pnl, 8),
        "unrealized_pnl": round(unrealized_pnl, 8),
        "halted": False,
        "position_side": 1 if position is not None else 0,
        "contracts": round(contracts, 8),
    }


def _ensure_trade_frame(trades: list[dict[str, Any]]) -> pd.DataFrame:
    if trades:
        return pd.DataFrame(trades)
    return pd.DataFrame(
        columns=[
            "signal_time",
            "entry_time",
            "exit_time",
            "side",
            "contracts",
            "quantity_btc",
            "entry_price",
            "exit_price",
            "stop_price",
            "leverage",
            "margin_used",
            "notional",
            "gross_pnl",
            "funding_pnl",
            "fee_paid",
            "net_pnl",
            "exit_reason",
            "symbol",
        ]
    )


def _build_summary(
    *,
    equity_curve: pd.DataFrame,
    trades: pd.DataFrame,
    initial_equity: float,
    signals: pd.DataFrame,
    params: MultiCycleTrendParameters,
) -> dict[str, Any]:
    final_equity = float(equity_curve["equity"].iloc[-1]) if not equity_curve.empty else float(initial_equity)
    total_return = ((final_equity / initial_equity) - 1.0) * 100.0 if initial_equity > 0 else 0.0

    equity_series = equity_curve["equity"].astype(float) if not equity_curve.empty else pd.Series(dtype=float)
    running_peak = equity_series.cummax()
    drawdown = ((equity_series / running_peak) - 1.0).fillna(0.0)
    max_drawdown_pct = abs(float(drawdown.min() * 100.0)) if not drawdown.empty else 0.0

    annualized_return_pct = None
    sharpe = None
    if len(equity_curve) >= 2 and initial_equity > 0 and final_equity > 0:
        timestamps = pd.to_datetime(equity_curve["timestamp"], utc=True)
        elapsed_days = max((timestamps.iloc[-1] - timestamps.iloc[0]).total_seconds() / 86_400.0, 1.0)
        annualized_return_pct = (((final_equity / initial_equity) ** (365.25 / elapsed_days)) - 1.0) * 100.0
        returns = equity_series.pct_change().dropna()
        if not returns.empty:
            volatility = returns.std(ddof=0)
            if volatility > 0:
                sharpe = float((returns.mean() / volatility) * sqrt(365.25))

    winning = trades.loc[trades["net_pnl"] > 0, "net_pnl"].sum() if not trades.empty else 0.0
    losing = trades.loc[trades["net_pnl"] < 0, "net_pnl"].sum() if not trades.empty else 0.0
    profit_factor = float(winning / abs(losing)) if losing < 0 else None
    win_rate_pct = float((trades["net_pnl"] > 0).mean() * 100.0) if not trades.empty else 0.0
    leverage_used = trades["leverage"].astype(float) if not trades.empty else pd.Series(dtype=float)

    return {
        "initial_equity": round(float(initial_equity), 8),
        "final_equity": round(final_equity, 8),
        "total_return_pct": round(total_return, 4),
        "annualized_return_pct": round(annualized_return_pct, 4) if annualized_return_pct is not None else None,
        "max_drawdown_pct": round(max_drawdown_pct, 4),
        "trade_count": int(len(trades)),
        "win_rate_pct": round(win_rate_pct, 4),
        "profit_factor": round(profit_factor, 4) if profit_factor is not None else None,
        "sharpe": round(sharpe, 4) if sharpe is not None else None,
        "signal_count": int(signals["open_long_signal"].fillna(False).sum()),
        "bull_trend_bar_count": int(signals["allow_long"].fillna(False).sum()),
        "take_profit_signal_count": int(signals["take_profit_signal"].fillna(False).sum()),
        "max_leverage_used": round(float(leverage_used.max()), 8) if not leverage_used.empty else None,
        "avg_leverage_used": round(float(leverage_used.mean()), 8) if not leverage_used.empty else None,
        "price_tick": params.price_tick,
        "margin_fraction": params.margin_fraction,
        "stop_loss_fraction_of_nominal": params.stop_loss_fraction_of_nominal,
        "minimum_stop_distance_fraction": params.minimum_stop_distance_fraction,
        "require_fresh_setup": params.require_fresh_setup,
        "require_current_close_above_touch_ema": params.require_current_close_above_touch_ema,
    }


def _build_monthly_context(frame: pd.DataFrame, monthly_ema_span: int) -> pd.DataFrame:
    monthly = (
        frame.set_index("timestamp")["close"]
        .resample("ME")
        .last()
        .dropna()
        .to_frame(name="monthly_close")
        .reset_index()
    )
    monthly["monthly_ema"] = monthly["monthly_close"].ewm(span=monthly_ema_span, adjust=False).mean()
    monthly["monthly_bar_count"] = range(1, len(monthly) + 1)
    return monthly


def _normalize_ohlcv_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if "timestamp" not in normalized.columns:
        raise ValueError("input frame must contain a timestamp column")
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True, errors="coerce")
    normalized = (
        normalized.dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .drop_duplicates("timestamp")
        .reset_index(drop=True)
    )
    for column in PRICE_COLUMNS:
        if column not in normalized.columns:
            raise ValueError(f"input frame must contain {column}")
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    for optional_column in ("volume", "volume_ccy", "volume_quote"):
        if optional_column in normalized.columns:
            normalized[optional_column] = pd.to_numeric(normalized[optional_column], errors="coerce").fillna(0.0)
        else:
            normalized[optional_column] = 0.0
    return normalized


def _cross_under(left: pd.Series, right: pd.Series) -> pd.Series:
    return (
        left.shift(1).notna()
        & right.shift(1).notna()
        & left.notna()
        & right.notna()
        & left.shift(1).ge(right.shift(1))
        & left.lt(right)
    )


def _stop_loss_price(value: object, tick_size: float) -> float | None:
    if value is None or pd.isna(value):
        return None
    low = Decimal(str(value))
    tick = Decimal(str(tick_size))
    return float((low - tick).quantize(tick, rounding=ROUND_DOWN))


def _safe_leverage(
    *,
    entry_price: object,
    stop_price: object,
    stop_loss_fraction_of_nominal: float,
    max_leverage: float,
    minimum_stop_distance_fraction: float,
) -> float | None:
    entry = _coerce_float(entry_price)
    stop = _coerce_float(stop_price)
    if entry is None or stop is None or entry <= 0 or stop >= entry:
        return None
    risk_fraction = (entry - stop) / entry
    if risk_fraction <= 0:
        return None
    effective_risk_fraction = max(risk_fraction, minimum_stop_distance_fraction)
    leverage = stop_loss_fraction_of_nominal / effective_risk_fraction
    return round(min(leverage, max_leverage), 8)


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _validate_parameters(params: MultiCycleTrendParameters) -> None:
    if params.synthetic_window < 1:
        raise ValueError("synthetic_window must be >= 1")
    if params.touch_ema_span < 1 or params.support_ema_span < 1 or params.monthly_ema_span < 1:
        raise ValueError("EMA spans must be >= 1")
    if any(span < 1 for span in (*params.trend_fast_ema_spans, *params.trend_slow_ema_spans)):
        raise ValueError("trend EMA spans must be >= 1")
    if params.price_tick <= 0:
        raise ValueError("price_tick must be > 0")
    if not (0 < params.margin_fraction <= 1):
        raise ValueError("margin_fraction must be within (0, 1]")
    if not (0 < params.stop_loss_fraction_of_nominal <= 1):
        raise ValueError("stop_loss_fraction_of_nominal must be within (0, 1]")
    if params.max_leverage <= 0:
        raise ValueError("max_leverage must be > 0")
    if params.minimum_stop_distance_fraction < 0:
        raise ValueError("minimum_stop_distance_fraction must be >= 0")
