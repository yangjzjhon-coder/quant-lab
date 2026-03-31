from __future__ import annotations

from pathlib import Path

from quant_lab.application.runtime_policy import aggregate_execution_loop_status, build_rollout_policy_payload
from quant_lab.config import (
    AppConfig,
    DatabaseConfig,
    InstrumentConfig,
    OkxConfig,
    RolloutConfig,
    StorageConfig,
    StrategyConfig,
    TradingConfig,
)


def test_live_single_rollout_policy_ready_when_binding_is_locked(tmp_path: Path) -> None:
    config = _build_live_rollout_config(tmp_path)

    payload = build_rollout_policy_payload(config=config)

    assert payload["decision_source"] == "quant_lab.application.runtime_policy"
    assert payload["status"] == "ready"
    assert payload["active"] is True
    assert payload["ready"] is True
    assert payload["execution_mode"] == "live"
    assert payload["symbol_mode"] == "single"
    assert payload["checks"]["single_candidate_binding"] is True
    assert payload["checks"]["required_candidate_bound"] is True
    assert payload["checks"]["router_disabled"] is True


def test_live_single_rollout_policy_blocks_router_and_multi_symbol(tmp_path: Path) -> None:
    config = _build_live_rollout_config(
        tmp_path,
        trading=TradingConfig(
            require_approved_candidate=True,
            execution_candidate_id=101,
            execution_candidate_name="btc_breakout_live_v1",
            strategy_router_enabled=True,
            execution_candidate_map={"bull_trend": 101},
        ),
        portfolio_symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
    )

    payload = build_rollout_policy_payload(config=config)

    assert payload["status"] == "blocked"
    assert payload["ready"] is False
    assert payload["checks"]["router_disabled"] is False
    assert payload["checks"]["single_symbol_mode"] is False
    assert any("single-symbol mode" in reason for reason in payload["reasons"])
    assert any("forbids trading.strategy_router_enabled=true" in reason for reason in payload["reasons"])


def test_research_phase_rollout_policy_is_inactive(tmp_path: Path) -> None:
    config = _build_live_rollout_config(
        tmp_path,
        okx=OkxConfig(use_demo=True, profile="okx-demo"),
        rollout=RolloutConfig(),
    )

    payload = build_rollout_policy_payload(config=config)

    assert payload["status"] == "inactive"
    assert payload["active"] is False
    assert payload["ready"] is False
    assert payload["execution_mode"] == "demo"


def test_aggregate_execution_loop_status_uses_shared_priority_order() -> None:
    assert aggregate_execution_loop_status(["idle", "plan_only"]) == "plan_only"
    assert aggregate_execution_loop_status(["duplicate", "idle"]) == "duplicate"
    assert aggregate_execution_loop_status(["warning", "duplicate", "plan_only"]) == "warning"
    assert aggregate_execution_loop_status(["submitted", "warning", "idle"]) == "submitted"
    assert aggregate_execution_loop_status([]) == "ok"


def _build_live_rollout_config(
    tmp_path: Path,
    *,
    okx: OkxConfig | None = None,
    trading: TradingConfig | None = None,
    rollout: RolloutConfig | None = None,
    portfolio_symbols: list[str] | None = None,
) -> AppConfig:
    data_dir = tmp_path / "data"
    return AppConfig(
        okx=okx
        or OkxConfig(
            profile="okx-live-btc-single",
            use_demo=False,
        ),
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        portfolio={"symbols": portfolio_symbols or ["BTC-USDT-SWAP"]},
        strategy=StrategyConfig(
            name="breakout_retest_4h",
            variant="breakout_retest_regime",
            signal_bar="4H",
            execution_bar="1m",
        ),
        trading=trading
        or TradingConfig(
            require_approved_candidate=True,
            execution_candidate_id=101,
            execution_candidate_name="btc_breakout_live_v1",
            strategy_router_enabled=False,
            strategy_router_fallback_to_config=False,
        ),
        storage=StorageConfig(
            data_dir=data_dir,
            raw_dir=data_dir / "raw",
            report_dir=data_dir / "reports",
        ),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
        rollout=rollout
        or RolloutConfig(
            phase="live_single",
            account_profile="okx-live-btc-single",
            allowed_symbol="BTC-USDT-SWAP",
            required_candidate_id=101,
            required_candidate_name="btc_breakout_live_v1",
            required_signal_bar="4H",
            required_execution_bar="1m",
        ),
    )
