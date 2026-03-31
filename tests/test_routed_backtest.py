from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from quant_lab.backtest.routed import run_routed_backtest
from quant_lab.artifacts import read_artifact_manifest
from quant_lab.cli import app
from quant_lab.config import (
    AppConfig,
    DatabaseConfig,
    ExecutionConfig,
    InstrumentConfig,
    StorageConfig,
    StrategyConfig,
    TradingConfig,
)
from quant_lab.service.database import StrategyCandidate, init_db, make_session_factory, session_scope


def test_run_routed_backtest_uses_candidate_for_mapped_bull_regime(tmp_path: Path) -> None:
    candidate_config = _write_candidate_config(tmp_path, variant="ema_cross")
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
    _insert_candidate(
        session_factory=session_factory,
        candidate_id=1,
        candidate_name="btc_bull_router",
        config_path=candidate_config,
    )

    routed = run_routed_backtest(
        session_factory=session_factory,
        config=config,
        project_root=tmp_path,
        symbol="BTC-USDT-SWAP",
        signal_bars=_bull_signal_bars(),
        execution_bars=_bull_execution_bars(),
        funding_rates=_empty_funding(),
        execution_config=config.execution,
        risk_config=config.risk,
        instrument_config=config.instrument,
        required_scope="demo",
    )

    assert routed.route_summary["candidate_bar_pct"] > 95.0
    assert routed.route_summary["route_status_counts"]["candidate_config"] >= len(routed.route_frame) - 4
    assert routed.route_frame["selected_candidate_name"].dropna().iloc[-1] == "btc_bull_router"
    assert routed.route_frame["selected_strategy_source"].iloc[-1] == "candidate_config"
    assert not routed.artifacts.equity_curve.empty


def test_run_routed_backtest_surfaces_contract_fields_in_route_outputs(tmp_path: Path) -> None:
    candidate_config = _write_candidate_config(tmp_path, variant="ema_cross")
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
    _insert_candidate(
        session_factory=session_factory,
        candidate_id=1,
        candidate_name="btc_bull_router",
        config_path=candidate_config,
    )

    routed = run_routed_backtest(
        session_factory=session_factory,
        config=config,
        project_root=tmp_path,
        symbol="BTC-USDT-SWAP",
        signal_bars=_bull_signal_bars(),
        execution_bars=_bull_execution_bars(),
        funding_rates=_empty_funding(),
        execution_config=config.execution,
        risk_config=config.risk,
        instrument_config=config.instrument,
        required_scope="demo",
    )

    for column in (
        "execution_desired_side",
        "alpha_side",
        "alpha_score",
        "alpha_regime",
        "risk_stop_distance",
        "risk_stop_price",
        "risk_multiplier",
        "contract_strategy_name",
        "contract_strategy_variant",
    ):
        assert column in routed.route_frame.columns
        assert column in routed.artifacts.signal_frame.columns

    latest = routed.route_frame.iloc[-1]
    assert latest["execution_desired_side"] == latest["desired_side"]
    assert latest["risk_stop_distance"] == latest["stop_distance"]
    assert latest["risk_multiplier"] == latest["strategy_risk_multiplier"]
    assert latest["contract_strategy_name"] == "ema_trend_4h"
    assert latest["contract_strategy_variant"] == "ema_cross"
    assert latest["alpha_regime"] == "bull_trend"


def test_run_routed_backtest_falls_back_when_regime_route_is_missing(tmp_path: Path) -> None:
    candidate_config = _write_candidate_config(tmp_path, variant="ema_cross")
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
    _insert_candidate(
        session_factory=session_factory,
        candidate_id=1,
        candidate_name="btc_bull_router",
        config_path=candidate_config,
    )

    routed = run_routed_backtest(
        session_factory=session_factory,
        config=config,
        project_root=tmp_path,
        symbol="BTC-USDT-SWAP",
        signal_bars=_range_signal_bars(),
        execution_bars=_range_execution_bars(),
        funding_rates=_empty_funding(),
        execution_config=config.execution,
        risk_config=config.risk,
        instrument_config=config.instrument,
        required_scope="demo",
    )

    assert routed.route_summary["fallback_bar_pct"] == 100.0
    assert routed.route_summary["route_status_counts"]["base_config_fallback"] == len(routed.route_frame)
    assert routed.route_frame["selected_strategy_source"].iloc[-1] == "base_config_fallback"
    assert routed.route_frame["selected_candidate_id"].isna().all()


def test_run_routed_backtest_keeps_market_columns_for_dashboard_outputs(tmp_path: Path) -> None:
    candidate_config = _write_candidate_config(tmp_path, variant="ema_cross")
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
    _insert_candidate(
        session_factory=session_factory,
        candidate_id=1,
        candidate_name="btc_bull_router",
        config_path=candidate_config,
    )

    routed = run_routed_backtest(
        session_factory=session_factory,
        config=config,
        project_root=tmp_path,
        symbol="BTC-USDT-SWAP",
        signal_bars=_bull_signal_bars(),
        execution_bars=_bull_execution_bars(),
        funding_rates=_empty_funding(),
        execution_config=config.execution,
        risk_config=config.risk,
        instrument_config=config.instrument,
        required_scope="demo",
    )

    signal_frame = routed.artifacts.signal_frame
    for column in ("open", "high", "low", "close"):
        assert column in signal_frame.columns
        assert signal_frame[column].notna().all()


def test_research_routed_backtest_cli_writes_report_and_routing_artifacts(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.yaml"
    candidate_config = _write_candidate_config(tmp_path, variant="ema_cross")
    config_path.write_text(
        """
instrument:
  symbol: BTC-USDT-SWAP
  settle_currency: USDT
strategy:
  name: ema_trend_4h
  variant: ema_cross
  signal_bar: 4H
  execution_bar: 1H
trading:
  strategy_router_enabled: true
  strategy_router_fallback_to_config: true
  execution_candidate_map:
    bull_trend: 1
storage:
  data_dir: data
  raw_dir: data/raw
  report_dir: data/reports
database:
  url: sqlite:///data/quant_lab.db
""".strip(),
        encoding="utf-8",
    )
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    _bull_signal_bars().to_parquet(raw_dir / "BTC-USDT-SWAP_4H.parquet", index=False)
    _bull_execution_bars().to_parquet(raw_dir / "BTC-USDT-SWAP_1H.parquet", index=False)
    _empty_funding().to_parquet(raw_dir / "BTC-USDT-SWAP_funding.parquet", index=False)

    runtime_config = _runtime_config(
        tmp_path,
        trading=TradingConfig(
            strategy_router_enabled=True,
            strategy_router_fallback_to_config=True,
            execution_candidate_map={"bull_trend": 1},
        ),
    )
    init_db(runtime_config.database.url)
    session_factory = make_session_factory(runtime_config.database.url)
    _insert_candidate(
        session_factory=session_factory,
        candidate_id=1,
        candidate_name="btc_bull_router",
        config_path=candidate_config,
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "research-routed-backtest",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    report_dir = tmp_path / "data" / "reports"
    prefix = "BTC-USDT-SWAP_ema_trend_4h_routed"
    manifest = read_artifact_manifest(report_dir, prefix)

    assert manifest is not None
    assert manifest["artifact_kind"] == "routed_backtest"
    assert manifest["logical_prefix"] == prefix

    artifacts = manifest["artifacts"]
    for key in ("summary", "equity_curve", "trades", "dashboard", "routes", "routing_summary"):
        assert Path(artifacts[key]).exists()
        assert "__" in Path(artifacts[key]).name

    aliases = manifest["aliases"]
    assert Path(aliases[f"{prefix}_summary.json"]).name == Path(artifacts["summary"]).name
    assert Path(aliases[f"{prefix}_dashboard.html"]).name == Path(artifacts["dashboard"]).name
    assert Path(aliases[f"{prefix}_routes.csv"]).name == Path(artifacts["routes"]).name
    assert Path(aliases[f"{prefix}_routing_summary.json"]).name == Path(artifacts["routing_summary"]).name

    summary_payload = json.loads(Path(artifacts["summary"]).read_text(encoding="utf-8"))
    assert summary_payload["routing_mode"] == "candidate_router"
    assert summary_payload["routing_candidate_bar_pct"] > 95.0


def _runtime_config(tmp_path: Path, *, trading: TradingConfig) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP", settle_currency="USDT"),
        strategy=StrategyConfig(name="ema_trend_4h", variant="ema_cross", signal_bar="4H", execution_bar="1H"),
        execution=ExecutionConfig(initial_equity=10_000.0, max_leverage=3.0),
        trading=trading,
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'data' / 'quant_lab.db').as_posix()}"),
    )


def _insert_candidate(*, session_factory, candidate_id: int, candidate_name: str, config_path: Path) -> None:
    with session_scope(session_factory) as session:
        session.add(
            StrategyCandidate(
                id=candidate_id,
                candidate_name=candidate_name,
                strategy_name="ema_trend_4h",
                variant="ema_cross",
                timeframe="4H",
                symbol_scope=["BTC-USDT-SWAP"],
                config_path=str(config_path),
                author_role="strategy_builder",
                status="approved",
                thesis="Bull routing candidate.",
                tags=["bull"],
                details={},
                latest_score=80.0,
                latest_evaluation_status="evaluation_passed",
                latest_decision="approve",
                approval_scope="demo",
            )
        )


def _write_candidate_config(tmp_path: Path, *, variant: str) -> Path:
    config_path = tmp_path / f"candidate_{variant}.yaml"
    config_path.write_text(
        f"""
instrument:
  symbol: BTC-USDT-SWAP
  settle_currency: USDT
strategy:
  name: ema_trend_4h
  variant: {variant}
  signal_bar: 4H
  execution_bar: 1H
""".strip(),
        encoding="utf-8",
    )
    return config_path


def _bull_signal_bars() -> pd.DataFrame:
    timestamps = pd.date_range("2025-01-01", periods=260, freq="4h", tz="UTC")
    close = pd.Series(range(100, 360), dtype="float64")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close - 1.0,
            "high": close + 2.0,
            "low": close - 2.0,
            "close": close,
            "volume": 1_000.0,
        }
    )


def _bull_execution_bars() -> pd.DataFrame:
    timestamps = pd.date_range("2025-01-01", periods=1040, freq="1h", tz="UTC")
    close = pd.Series([100.0 + (index * 0.25) for index in range(1040)], dtype="float64")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close - 0.1,
            "high": close + 0.4,
            "low": close - 0.4,
            "close": close,
            "volume": 5_000.0,
        }
    )


def _range_signal_bars() -> pd.DataFrame:
    timestamps = pd.date_range("2025-01-01", periods=260, freq="4h", tz="UTC")
    close = pd.Series([100.0 for _ in range(260)], dtype="float64")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": close + 0.2,
            "low": close - 0.2,
            "close": close,
            "volume": 1_000.0,
        }
    )


def _range_execution_bars() -> pd.DataFrame:
    timestamps = pd.date_range("2025-01-01", periods=1040, freq="1h", tz="UTC")
    close = pd.Series([100.0 for _ in range(1040)], dtype="float64")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": close + 0.1,
            "low": close - 0.1,
            "close": close,
            "volume": 5_000.0,
        }
    )


def _empty_funding() -> pd.DataFrame:
    return pd.DataFrame(columns=["timestamp", "realized_rate"])
