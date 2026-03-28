from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from typer.testing import CliRunner

from quant_lab.cli import _build_leverage_alignment_requests, app
from quant_lab.config import (
    AppConfig,
    DatabaseConfig,
    ExecutionConfig,
    InstrumentConfig,
    PortfolioConfig,
    StorageConfig,
    StrategyConfig,
    TradingConfig,
)
from quant_lab.execution.planner import AccountSnapshot, OrderPlan, PositionSnapshot, SignalSnapshot


def test_build_leverage_alignment_requests_dedupes_cross_long_short_rows(tmp_path: Path) -> None:
    cfg = _runtime_config(tmp_path)

    requests = _build_leverage_alignment_requests(
        cfg,
        [
            {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "100"},
            {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "100"},
        ],
    )

    assert requests == [
        {
            "inst_id": "BTC-USDT-SWAP",
            "lever": 3.0,
            "mgn_mode": "cross",
        }
    ]


def test_demo_align_leverage_apply_outputs_before_and_after(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    cfg = _runtime_config(tmp_path)
    signal = _signal_snapshot()
    plan = _order_plan(signal)
    account = AccountSnapshot(
        total_equity=10_000.0,
        available_equity=10_000.0,
        currency="USDT",
        source="test",
        account_mode="long_short_mode",
        can_trade=True,
    )
    position = PositionSnapshot(side=-1, contracts=12.0, position_mode="long_short_mode")

    states = [
        (
            account,
            position,
            {
                "signal": signal,
                "plan": plan,
                "leverage_payload": {
                    "data": [
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "100"},
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "100"},
                    ]
                },
                "pending_orders_payload": {"data": []},
                "pending_algo_orders_payload": {"data": []},
            },
        ),
        (
            account,
            position,
            {
                "signal": signal,
                "plan": plan,
                "leverage_payload": {
                    "data": [
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "3"},
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "3"},
                    ]
                },
                "pending_orders_payload": {"data": []},
                "pending_algo_orders_payload": {"data": []},
            },
        ),
    ]

    monkeypatch.setattr("quant_lab.cli._load_app_context", lambda config, project_root: cfg)
    monkeypatch.setattr("quant_lab.cli._require_private_credentials", lambda current_cfg: None)
    monkeypatch.setattr("quant_lab.cli._load_executor_state", lambda path: {})
    monkeypatch.setattr("quant_lab.cli._load_demo_state", lambda current_cfg: states.pop(0))
    monkeypatch.setattr(
        "quant_lab.cli._align_demo_leverage",
        lambda current_cfg, leverage_rows: {
            "target": 3.0,
            "already_aligned": False,
            "request_count": 1,
            "requests": [{"inst_id": "BTC-USDT-SWAP", "lever": 3.0, "mgn_mode": "cross"}],
            "responses": [{"response": {"code": "0"}}],
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["demo-align-leverage", "--config", str(config_path), "--apply", "--confirm", "OKX_DEMO"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["before"]["checks"]["leverage_match"] is False
    assert payload["after"]["checks"]["leverage_match"] is True
    assert payload["alignment"]["request_count"] == 1


def test_demo_align_leverage_apply_refuses_when_cross_algo_orders_are_live(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    cfg = _runtime_config(tmp_path)
    signal = _signal_snapshot()
    plan = _order_plan(signal)
    account = AccountSnapshot(
        total_equity=10_000.0,
        available_equity=10_000.0,
        currency="USDT",
        source="test",
        account_mode="long_short_mode",
        can_trade=True,
    )
    position = PositionSnapshot(side=-1, contracts=12.0, position_mode="long_short_mode")

    monkeypatch.setattr("quant_lab.cli._load_app_context", lambda config, project_root: cfg)
    monkeypatch.setattr("quant_lab.cli._require_private_credentials", lambda current_cfg: None)
    monkeypatch.setattr("quant_lab.cli._load_executor_state", lambda path: {})
    monkeypatch.setattr(
        "quant_lab.cli._load_demo_state",
        lambda current_cfg: (
            account,
            position,
            {
                "signal": signal,
                "plan": plan,
                "leverage_payload": {
                    "data": [
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "100"},
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "100"},
                    ]
                },
                "pending_orders_payload": {"data": []},
                "pending_algo_orders_payload": {
                    "data": [{"algoId": "algo-1", "instId": "BTC-USDT-SWAP", "state": "live"}]
                },
            },
        ),
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["demo-align-leverage", "--config", str(config_path), "--apply", "--confirm", "OKX_DEMO"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["applied"] is False
    assert "Cancel and later re-arm the protective stop" in payload["blockers"][0]
    assert "rearm-protective-stop" in payload["hint"]


def test_demo_align_leverage_apply_can_use_stop_rearm_flow(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    cfg = _runtime_config(tmp_path)
    signal = _signal_snapshot()
    plan = _order_plan(signal)
    account = AccountSnapshot(
        total_equity=10_000.0,
        available_equity=10_000.0,
        currency="USDT",
        source="test",
        account_mode="long_short_mode",
        can_trade=True,
    )
    position = PositionSnapshot(side=-1, contracts=12.0, position_mode="long_short_mode")

    states = [
        (
            account,
            position,
            {
                "signal": signal,
                "plan": plan,
                "leverage_payload": {
                    "data": [
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "100"},
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "100"},
                    ]
                },
                "pending_orders_payload": {"data": []},
                "pending_algo_orders_payload": {
                    "data": [
                        {
                            "algoId": "algo-1",
                            "instId": "BTC-USDT-SWAP",
                            "side": "buy",
                            "posSide": "short",
                            "sz": "12",
                            "slTriggerPx": "101400",
                            "slOrdPx": "-1",
                            "slTriggerPxType": "mark",
                            "state": "live",
                        }
                    ]
                },
            },
        ),
        (
            account,
            position,
            {
                "signal": signal,
                "plan": plan,
                "leverage_payload": {
                    "data": [
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "3"},
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "3"},
                    ]
                },
                "pending_orders_payload": {"data": []},
                "pending_algo_orders_payload": {
                    "data": [
                        {
                            "algoId": "algo-2",
                            "instId": "BTC-USDT-SWAP",
                            "side": "buy",
                            "posSide": "short",
                            "sz": "12",
                            "slTriggerPx": "101400",
                            "slOrdPx": "-1",
                            "slTriggerPxType": "mark",
                            "state": "live",
                        }
                    ]
                },
            },
        ),
    ]

    monkeypatch.setattr("quant_lab.cli._load_app_context", lambda config, project_root: cfg)
    monkeypatch.setattr("quant_lab.cli._require_private_credentials", lambda current_cfg: None)
    monkeypatch.setattr("quant_lab.cli._load_executor_state", lambda path: {})
    monkeypatch.setattr("quant_lab.cli._load_demo_state", lambda current_cfg: states.pop(0))
    monkeypatch.setattr(
        "quant_lab.cli._align_demo_leverage_with_stop_rearm",
        lambda current_cfg, leverage_rows, stop_orders: {
            "target": 3.0,
            "already_aligned": False,
            "request_count": 1,
            "cancel": {"code": "0"},
            "responses": [{"response": {"code": "0"}}],
            "rearm": [{"response": {"code": "0"}}],
            "verified_stop_absence": True,
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "demo-align-leverage",
            "--config",
            str(config_path),
            "--apply",
            "--confirm",
            "OKX_DEMO",
            "--rearm-protective-stop",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["used_stop_rearm"] is True
    assert payload["after"]["checks"]["leverage_match"] is True


def test_demo_align_leverage_dry_run_in_portfolio_mode_targets_only_misaligned_symbol(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    cfg = _runtime_config(tmp_path)
    cfg.portfolio = PortfolioConfig(symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"])
    signal = _signal_snapshot()
    plan = _order_plan(signal)
    account = AccountSnapshot(
        total_equity=20_000.0,
        available_equity=20_000.0,
        currency="USDT",
        source="test",
        account_mode="long_short_mode",
        can_trade=True,
    )

    monkeypatch.setattr("quant_lab.cli._load_app_context", lambda config, project_root: cfg)
    monkeypatch.setattr("quant_lab.cli._load_executor_state", lambda path: {})
    monkeypatch.setattr(
        "quant_lab.cli._load_demo_portfolio_state",
        lambda current_cfg, symbols: (
            account,
            {
                "BTC-USDT-SWAP": {
                    "account": account,
                    "position": PositionSnapshot(side=0, contracts=0.0, position_mode="long_short_mode"),
                    "planning_account": account,
                    "signal": signal,
                    "plan": plan,
                    "leverage_payload": {
                        "data": [
                            {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "3"},
                            {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "3"},
                        ]
                    },
                    "pending_orders_payload": {"data": []},
                    "pending_algo_orders_payload": {"data": []},
                },
                "ETH-USDT-SWAP": {
                    "account": account,
                    "position": PositionSnapshot(side=0, contracts=0.0, position_mode="long_short_mode"),
                    "planning_account": account,
                    "signal": signal,
                    "plan": plan,
                    "leverage_payload": {
                        "data": [
                            {"instId": "ETH-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "100"},
                            {"instId": "ETH-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "100"},
                        ]
                    },
                    "pending_orders_payload": {"data": []},
                    "pending_algo_orders_payload": {"data": []},
                },
            },
        ),
    )

    runner = CliRunner()
    result = runner.invoke(app, ["demo-align-leverage", "--config", str(config_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["mode"] == "portfolio"
    assert payload["planned_requests"] == [
        {"inst_id": "ETH-USDT-SWAP", "lever": 3.0, "mgn_mode": "cross"}
    ]
    assert payload["symbol_plans"]["BTC-USDT-SWAP"]["planned_requests"] == []
    assert payload["before"]["symbol_states"]["ETH-USDT-SWAP"]["checks"]["leverage_match"] is False


def test_demo_align_leverage_apply_in_portfolio_mode_refreshes_after_state(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    cfg = _runtime_config(tmp_path)
    cfg.portfolio = PortfolioConfig(symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"])
    signal = _signal_snapshot()
    plan = _order_plan(signal)
    account = AccountSnapshot(
        total_equity=20_000.0,
        available_equity=20_000.0,
        currency="USDT",
        source="test",
        account_mode="long_short_mode",
        can_trade=True,
    )

    portfolio_states = [
        {
            "BTC-USDT-SWAP": {
                "account": account,
                "position": PositionSnapshot(side=0, contracts=0.0, position_mode="long_short_mode"),
                "planning_account": account,
                "signal": signal,
                "plan": plan,
                "leverage_payload": {
                    "data": [
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "3"},
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "3"},
                    ]
                },
                "pending_orders_payload": {"data": []},
                "pending_algo_orders_payload": {"data": []},
            },
            "ETH-USDT-SWAP": {
                "account": account,
                "position": PositionSnapshot(side=0, contracts=0.0, position_mode="long_short_mode"),
                "planning_account": account,
                "signal": signal,
                "plan": plan,
                "leverage_payload": {
                    "data": [
                        {"instId": "ETH-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "100"},
                        {"instId": "ETH-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "100"},
                    ]
                },
                "pending_orders_payload": {"data": []},
                "pending_algo_orders_payload": {"data": []},
            },
        },
        {
            "BTC-USDT-SWAP": {
                "account": account,
                "position": PositionSnapshot(side=0, contracts=0.0, position_mode="long_short_mode"),
                "planning_account": account,
                "signal": signal,
                "plan": plan,
                "leverage_payload": {
                    "data": [
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "3"},
                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "3"},
                    ]
                },
                "pending_orders_payload": {"data": []},
                "pending_algo_orders_payload": {"data": []},
            },
            "ETH-USDT-SWAP": {
                "account": account,
                "position": PositionSnapshot(side=0, contracts=0.0, position_mode="long_short_mode"),
                "planning_account": account,
                "signal": signal,
                "plan": plan,
                "leverage_payload": {
                    "data": [
                        {"instId": "ETH-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "3"},
                        {"instId": "ETH-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "3"},
                    ]
                },
                "pending_orders_payload": {"data": []},
                "pending_algo_orders_payload": {"data": []},
            },
        },
    ]

    monkeypatch.setattr("quant_lab.cli._load_app_context", lambda config, project_root: cfg)
    monkeypatch.setattr("quant_lab.cli._require_private_credentials", lambda current_cfg: None)
    monkeypatch.setattr("quant_lab.cli._load_executor_state", lambda path: {})
    monkeypatch.setattr(
        "quant_lab.cli._load_demo_portfolio_state",
        lambda current_cfg, symbols: (account, portfolio_states.pop(0)),
    )
    monkeypatch.setattr(
        "quant_lab.cli._align_demo_leverage",
        lambda current_cfg, leverage_rows: {
            "target": 3.0,
            "already_aligned": False,
            "request_count": 1,
            "requests": [{"inst_id": "ETH-USDT-SWAP", "lever": 3.0, "mgn_mode": "cross"}],
            "responses": [{"response": {"code": "0"}}],
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["demo-align-leverage", "--config", str(config_path), "--apply", "--confirm", "OKX_DEMO"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["mode"] == "portfolio"
    assert payload["applied"] is True
    assert payload["symbol_results"]["BTC-USDT-SWAP"]["status"] == "already_aligned"
    assert payload["symbol_results"]["ETH-USDT-SWAP"]["status"] == "aligned"
    assert payload["after"]["symbol_states"]["ETH-USDT-SWAP"]["checks"]["leverage_match"] is True


def _runtime_config(tmp_path: Path) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        okx={"use_demo": True},
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP", settle_currency="USDT"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        execution=ExecutionConfig(max_leverage=3.0),
        trading=TradingConfig(td_mode="cross", position_mode="long_short_mode"),
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
    )


def _signal_snapshot() -> SignalSnapshot:
    signal_time = _utc("2025-01-21T00:00:00+00:00")
    return SignalSnapshot(
        signal_time=signal_time,
        effective_time=signal_time,
        latest_execution_time=signal_time,
        latest_price=100_000.0,
        latest_high=100_500.0,
        latest_low=99_500.0,
        latest_liquidity_quote=1_500_000.0,
        desired_side=-1,
        previous_side=0,
        stop_distance=1_500.0,
        ready=True,
    )


def _order_plan(signal: SignalSnapshot) -> OrderPlan:
    return OrderPlan(
        action="hold",
        reason="already aligned",
        desired_side=-1,
        current_side=-1,
        current_contracts=12.0,
        target_contracts=12.0,
        equity_reference=10_000.0,
        latest_price=100_000.0,
        entry_price_estimate=99_900.0,
        stop_price=101_400.0,
        stop_distance=1_500.0,
        signal_time=signal.signal_time,
        effective_time=signal.effective_time,
        position_mode="long_short_mode",
    )


def _utc(raw: str):
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
