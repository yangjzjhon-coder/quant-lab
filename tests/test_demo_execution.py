from __future__ import annotations

import pandas as pd

from quant_lab.config import ExecutionConfig, InstrumentConfig, RiskConfig, StrategyConfig, TradingConfig
from quant_lab.execution.planner import (
    AccountSnapshot,
    PositionSnapshot,
    build_account_snapshot,
    build_order_plan,
    build_position_snapshot,
    build_signal_snapshot,
)


def test_build_signal_snapshot_waits_for_latency_window() -> None:
    signal_bars = _signal_bars(range(100, 220))
    last_signal_time = signal_bars["timestamp"].iloc[-1]
    execution_bars = pd.DataFrame(
        {
            "timestamp": [
                last_signal_time + pd.Timedelta(hours=3, minutes=59),
                last_signal_time + pd.Timedelta(hours=4),
            ],
            "open": [218.0, 219.0],
            "high": [219.0, 220.0],
            "low": [217.0, 218.0],
            "close": [218.5, 219.5],
            "volume": [1.0, 1.0],
        }
    )

    signal = build_signal_snapshot(
        signal_bars=signal_bars,
        execution_bars=execution_bars,
        strategy_config=StrategyConfig(),
        execution_config=ExecutionConfig(latency_minutes=1),
    )

    assert signal.desired_side == 1
    assert signal.ready is False
    assert signal.effective_time == last_signal_time + pd.Timedelta(hours=4, minutes=1)
    assert signal.alpha_signal is not None
    assert signal.risk_signal is not None
    assert signal.alpha_signal.side == 1
    assert signal.risk_signal.stop_distance == signal.stop_distance


def test_build_order_plan_opens_long_from_flat() -> None:
    signal = build_signal_snapshot(
        signal_bars=_signal_bars(range(100, 220)),
        execution_bars=_execution_bars("2025-01-21 00:00", [219.0, 220.0, 221.0, 222.0]),
        strategy_config=StrategyConfig(),
        execution_config=ExecutionConfig(latency_minutes=1),
    )

    plan = build_order_plan(
        signal=signal,
        account=AccountSnapshot(total_equity=10_000.0, available_equity=10_000.0, currency="USDT", source="config"),
        position=PositionSnapshot(side=0, contracts=0.0),
        instrument_config=InstrumentConfig(
            symbol="BTC-USDT-SWAP",
            contract_value=0.01,
            lot_size=1.0,
            min_size=1.0,
        ),
        execution_config=ExecutionConfig(),
        risk_config=RiskConfig(),
        trading_config=TradingConfig(),
    )

    assert plan.action == "open"
    assert len(plan.instructions) == 1
    assert plan.instructions[0].side == "buy"
    assert plan.instructions[0].reduce_only is False
    assert len(plan.instructions[0].attach_algo_orders) == 1
    assert plan.instructions[0].attach_algo_orders[0].sl_trigger_px_type == "mark"
    assert plan.stop_price is not None
    assert plan.target_contracts > 0


def test_build_order_plan_flips_short_to_long_in_net_mode() -> None:
    signal = build_signal_snapshot(
        signal_bars=_signal_bars(range(100, 220)),
        execution_bars=_execution_bars("2025-01-21 00:00", [219.0, 220.0, 221.0, 222.0]),
        strategy_config=StrategyConfig(),
        execution_config=ExecutionConfig(latency_minutes=1),
    )

    plan = build_order_plan(
        signal=signal,
        account=AccountSnapshot(total_equity=10_000.0, available_equity=10_000.0, currency="USDT", source="config"),
        position=PositionSnapshot(side=-1, contracts=15.0),
        instrument_config=InstrumentConfig(
            symbol="BTC-USDT-SWAP",
            contract_value=0.01,
            lot_size=1.0,
            min_size=1.0,
        ),
        execution_config=ExecutionConfig(),
        risk_config=RiskConfig(),
        trading_config=TradingConfig(position_mode="net_mode"),
    )

    assert plan.action == "flip"
    assert len(plan.instructions) == 2
    assert plan.instructions[0].purpose == "close_existing"
    assert plan.instructions[0].side == "buy"
    assert plan.instructions[0].reduce_only is True
    assert plan.instructions[1].purpose == "open_target"
    assert plan.instructions[1].side == "buy"


def test_build_account_snapshot_uses_balance_payload() -> None:
    account = build_account_snapshot(
        balance_payload={
            "data": [
                {
                    "totalEq": "12500",
                    "details": [
                        {
                            "ccy": "USDT",
                            "eq": "12480",
                            "availEq": "12000",
                        }
                    ],
                }
            ]
        },
        account_config_payload={"data": [{"posMode": "net_mode", "perm": "read,trade"}]},
        settle_currency="USDT",
        fallback_equity=10_000,
    )

    assert account.total_equity == 12500.0
    assert account.available_equity == 12000.0
    assert account.account_mode == "net_mode"
    assert account.can_trade is True


def test_build_position_snapshot_parses_short_position() -> None:
    position = build_position_snapshot(
        positions_payload={
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "pos": "-12",
                    "posSide": "net",
                    "avgPx": "100000",
                    "markPx": "99500",
                }
            ]
        },
        inst_id="BTC-USDT-SWAP",
        position_mode="net_mode",
    )

    assert position.side == -1
    assert position.contracts == 12.0
    assert position.avg_price == 100000.0


def test_build_order_plan_can_disable_attached_stop() -> None:
    signal = build_signal_snapshot(
        signal_bars=_signal_bars(range(100, 220)),
        execution_bars=_execution_bars("2025-01-21 00:00", [219.0, 220.0, 221.0, 222.0]),
        strategy_config=StrategyConfig(),
        execution_config=ExecutionConfig(latency_minutes=1),
    )

    plan = build_order_plan(
        signal=signal,
        account=AccountSnapshot(total_equity=10_000.0, available_equity=10_000.0, currency="USDT", source="config"),
        position=PositionSnapshot(side=0, contracts=0.0),
        instrument_config=InstrumentConfig(
            symbol="BTC-USDT-SWAP",
            contract_value=0.01,
            lot_size=1.0,
            min_size=1.0,
        ),
        execution_config=ExecutionConfig(),
        risk_config=RiskConfig(),
        trading_config=TradingConfig(attach_stop_loss_on_entry=False),
    )

    assert len(plan.instructions) == 1
    assert plan.instructions[0].attach_algo_orders == []


def _signal_bars(closes) -> pd.DataFrame:
    closes = list(closes)
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(closes), freq="4h", tz="UTC"),
            "open": [value - 0.8 for value in closes],
            "high": [value + 0.1 for value in closes],
            "low": [value - 1 for value in closes],
            "close": closes,
            "volume": [10.0] * len(closes),
            "volume_quote": [500.0] * (len(closes) - 1) + [2_000.0],
        }
    )


def _execution_bars(start: str, closes: list[float]) -> pd.DataFrame:
    closes = list(closes)
    opens = [closes[0], *closes[:-1]]
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(start, periods=len(closes), freq="min", tz="UTC"),
            "open": opens,
            "high": [value + 0.5 for value in closes],
            "low": [value - 0.5 for value in closes],
            "close": closes,
            "volume": [1.0] * len(closes),
            "volume_quote": [1_000.0] * len(closes),
        }
    )
