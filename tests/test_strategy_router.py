from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_lab.config import (
    AppConfig,
    DatabaseConfig,
    InstrumentConfig,
    StorageConfig,
    StrategyConfig,
    TradingConfig,
)
from quant_lab.execution.strategy_router import build_strategy_router_status, resolve_strategy_route
from quant_lab.service.database import StrategyCandidate, init_db, make_session_factory, session_scope


def test_strategy_router_selects_bull_trend_candidate(tmp_path: Path) -> None:
    candidate_config = tmp_path / "bull_candidate.yaml"
    candidate_config.write_text(
        """
strategy:
  name: trend_breakout_4h
  variant: trend_breakout_long
  signal_bar: 4H
  execution_bar: 1m
""".strip(),
        encoding="utf-8",
    )
    config = _runtime_config(
        tmp_path,
        trading=TradingConfig(
            strategy_router_enabled=True,
            strategy_router_fallback_to_config=True,
            execution_candidate_map={"bull_trend": 1},
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    with session_scope(session_factory) as session:
        session.add(
            StrategyCandidate(
                id=1,
                candidate_name="btc_bull_breakout",
                strategy_name="trend_breakout_4h",
                variant="trend_breakout_long",
                timeframe="4H",
                symbol_scope=["BTC-USDT-SWAP"],
                config_path=str(candidate_config),
                author_role="strategy_builder",
                status="approved",
                thesis="Bull trend breakout candidate.",
                tags=["bull"],
                details={},
                latest_score=82.4,
                latest_evaluation_status="evaluation_passed",
                latest_decision="approve",
                approval_scope="demo",
            )
        )

    decision = resolve_strategy_route(
        session_factory=session_factory,
        config=config,
        project_root=tmp_path,
        symbol="BTC-USDT-SWAP",
        signal_bars=_bull_signal_bars(),
        required_scope="demo",
    )

    assert decision.enabled is True
    assert decision.ready is True
    assert decision.regime == "bull_trend"
    assert decision.route_key == "bull_trend"
    assert decision.selected_strategy_source == "candidate_config"
    assert decision.selected_strategy_name == "trend_breakout_4h"
    assert decision.selected_variant == "trend_breakout_long"
    assert decision.candidate is not None
    assert decision.candidate["candidate_name"] == "btc_bull_breakout"


def test_strategy_router_falls_back_when_regime_route_is_missing(tmp_path: Path) -> None:
    config = _runtime_config(
        tmp_path,
        trading=TradingConfig(
            strategy_router_enabled=True,
            strategy_router_fallback_to_config=True,
            execution_candidate_map={"bull_trend": 1},
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    decision = resolve_strategy_route(
        session_factory=session_factory,
        config=config,
        project_root=tmp_path,
        symbol="BTC-USDT-SWAP",
        signal_bars=_range_signal_bars(),
        required_scope="demo",
    )

    assert decision.enabled is True
    assert decision.ready is False
    assert decision.fallback_used is True
    assert decision.regime != "bull_trend"
    assert decision.route_key is None
    assert decision.selected_strategy_source == "base_config_fallback"
    assert any("no routed candidate configured" in reason for reason in decision.reasons)


def test_strategy_router_status_reports_route_pool_readiness(tmp_path: Path) -> None:
    candidate_config = tmp_path / "route_candidate.yaml"
    candidate_config.write_text(
        """
strategy:
  name: ema_trend_4h
  variant: ema_cross
  signal_bar: 4H
  execution_bar: 1m
""".strip(),
        encoding="utf-8",
    )
    config = _runtime_config(
        tmp_path,
        trading=TradingConfig(
            strategy_router_enabled=True,
            execution_candidate_map={"bull_trend": 1, "range": 999},
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    with session_scope(session_factory) as session:
        session.add(
            StrategyCandidate(
                id=1,
                candidate_name="btc_default",
                strategy_name="ema_trend_4h",
                variant="ema_cross",
                timeframe="4H",
                symbol_scope=["BTC-USDT-SWAP"],
                config_path=str(candidate_config),
                author_role="strategy_builder",
                status="approved",
                thesis="Default candidate.",
                tags=["default"],
                details={},
                latest_score=74.1,
                latest_evaluation_status="evaluation_passed",
                latest_decision="approve",
                approval_scope="demo",
            )
        )

    status = build_strategy_router_status(session_factory=session_factory, config=config, required_scope="demo")

    assert status["enabled"] is True
    assert status["ready"] is False
    assert status["routes"][0]["route_key"] == "bull_trend"
    assert status["routes"][0]["ready"] is True
    assert status["routes"][1]["route_key"] == "range"
    assert status["routes"][1]["ready"] is False


def _runtime_config(tmp_path: Path, *, trading: TradingConfig) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h", variant="ema_cross", signal_bar="4H", execution_bar="1m"),
        trading=trading,
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
    )


def _bull_signal_bars() -> pd.DataFrame:
    timestamps = pd.date_range("2026-01-01", periods=260, freq="4h", tz="UTC")
    close = pd.Series(range(100, 360), dtype="float64")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close - 1.0,
            "high": close + 2.0,
            "low": close - 2.0,
            "close": close,
            "volume": 1000.0,
        }
    )
    return frame


def _range_signal_bars() -> pd.DataFrame:
    timestamps = pd.date_range("2026-01-01", periods=260, freq="4h", tz="UTC")
    values = [100.0 + ((index % 6) - 3) * 0.6 for index in range(260)]
    close = pd.Series(values, dtype="float64")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close + 0.1,
            "high": close + 0.8,
            "low": close - 0.8,
            "close": close,
            "volume": 1000.0,
        }
    )
    return frame
