from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from quant_lab.application.project_tasks import default_project_research_artifact_resolution
from quant_lab.artifacts import backtest_artifact_resolution, canonical_artifact_path, update_artifact_manifest
from quant_lab.cli import app
from quant_lab.config import AppConfig, DatabaseConfig, InstrumentConfig, ServiceConfig, StorageConfig, StrategyConfig
from quant_lab.service.research_ops import infer_candidate_artifacts
from quant_lab.service.database import init_db, make_session_factory
from quant_lab.service.monitor import build_service_app


def test_research_cli_workflow_registers_evaluates_and_approves_candidate(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    _write_summary_artifact(tmp_path)
    runner = CliRunner()

    task_result = runner.invoke(
        app,
        [
            "research-create-task",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
            "--title",
            "Build BTC regime research plan",
            "--hypothesis",
            "Trend plus regime filter should outperform plain EMA cross.",
        ],
    )
    assert task_result.exit_code == 0
    task_payload = json.loads(task_result.stdout)
    assert task_payload["status"] == "proposed"
    assert task_payload["owner_role"] == "research_lead"

    candidate_result = runner.invoke(
        app,
        [
            "research-register-candidate",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
            "--name",
            "btc_regime_v1",
            "--task-id",
            str(task_payload["id"]),
            "--strategy-name",
            "ema_trend_4h",
            "--variant",
            "high_weight_long",
            "--timeframe",
            "4H",
            "--thesis",
            "Use high-weight BTC trend factors only.",
        ],
    )
    assert candidate_result.exit_code == 0
    candidate_payload = json.loads(candidate_result.stdout)
    candidate_id = candidate_payload["id"]
    assert candidate_payload["status"] == "draft"

    evaluate_result = runner.invoke(
        app,
        [
            "research-evaluate-candidate",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
            "--candidate-id",
            str(candidate_id),
        ],
    )
    assert evaluate_result.exit_code == 0
    evaluate_payload = json.loads(evaluate_result.stdout)
    assert evaluate_payload["candidate"]["latest_evaluation_status"] == "evaluation_passed"
    assert evaluate_payload["evaluation_report"]["score_total"] >= 70.0
    assert evaluate_payload["evaluation_report"]["artifact_payload"]["resolved_via"] == "legacy_fixed_name"
    assert evaluate_payload["evaluation_report"]["artifact_payload"]["artifact_fingerprint"]
    assert (
        evaluate_payload["evaluation_report"]["artifact_payload"]["summary_file"]["path"]
        == evaluate_payload["evaluation_report"]["artifact_payload"]["summary_path"]
    )

    approve_result = runner.invoke(
        app,
        [
            "research-approve-candidate",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
            "--candidate-id",
            str(candidate_id),
            "--decision",
            "approve",
            "--scope",
            "demo",
            "--reason",
            "Good enough for demo observation.",
        ],
    )
    assert approve_result.exit_code == 0
    approve_payload = json.loads(approve_result.stdout)
    assert approve_payload["candidate"]["status"] == "approved"
    assert approve_payload["approval"]["decision"] == "approve"

    overview_result = runner.invoke(
        app,
        [
            "research-overview",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
        ],
    )
    assert overview_result.exit_code == 0
    overview_payload = json.loads(overview_result.stdout)
    assert overview_payload["task_counts"]["proposed"] == 1
    assert overview_payload["candidate_counts"]["approved"] == 1
    assert overview_payload["approved_candidates"][0]["candidate_name"] == "btc_regime_v1"


def test_research_approve_candidate_cli_returns_structured_not_found_payload(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research-approve-candidate",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
            "--candidate-id",
            "999",
            "--decision",
            "approve",
            "--scope",
            "demo",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["source"] == "cli"
    assert payload["command"] == "research-approve-candidate"
    assert payload["error_code"] == "strategy_candidate_not_found"
    assert payload["error_type"] == "not_found"


def test_service_api_exposes_research_workflow_endpoints(tmp_path: Path) -> None:
    _write_summary_artifact(tmp_path)
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app_instance = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app_instance) as client:
        create_task = client.post(
            "/research/tasks",
            json={
                "title": "Evaluate BTC trend stack",
                "hypothesis": "Trend filters should reduce bear-market drawdown.",
                "symbols": ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
            },
        )
        assert create_task.status_code == 200
        create_task_payload = create_task.json()
        assert create_task_payload["task"]["created_at"].endswith("+00:00")
        assert create_task_payload["task"]["updated_at"].endswith("+00:00")
        task_id = create_task_payload["task"]["id"]

        create_candidate = client.post(
            "/research/candidates",
            json={
                "candidate_name": "btc_eth_regime_v2",
                "task_id": task_id,
                "strategy_name": "ema_trend_4h",
                "variant": "trend_regime_long",
                "timeframe": "4H",
                "symbol_scope": ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
                "thesis": "Use regime confirmation before taking trend entries.",
            },
        )
        assert create_candidate.status_code == 200
        create_candidate_payload = create_candidate.json()
        assert create_candidate_payload["candidate"]["created_at"].endswith("+00:00")
        assert create_candidate_payload["candidate"]["updated_at"].endswith("+00:00")
        candidate_id = create_candidate_payload["candidate"]["id"]

        evaluate = client.post(f"/research/candidates/{candidate_id}/evaluate", json={})
        assert evaluate.status_code == 200
        evaluate_payload = evaluate.json()
        assert evaluate_payload["candidate"]["latest_score"] >= 70.0
        assert evaluate_payload["evaluation_report"]["status"] == "evaluation_passed"
        assert evaluate_payload["candidate"]["last_evaluated_at"].endswith("+00:00")
        assert evaluate_payload["candidate"]["updated_at"].endswith("+00:00")
        assert evaluate_payload["evaluation_report"]["created_at"].endswith("+00:00")

        approve = client.post(
            f"/research/candidates/{candidate_id}/approve",
            json={"decision": "approve", "scope": "demo", "reason": "Promote to demo pool."},
        )
        assert approve.status_code == 200
        approve_payload = approve.json()
        assert approve_payload["candidate"]["status"] == "approved"
        assert approve_payload["candidate"]["updated_at"].endswith("+00:00")
        assert approve_payload["approval"]["created_at"].endswith("+00:00")

        tasks = client.get("/research/tasks?limit=10")
        assert tasks.status_code == 200
        tasks_payload = tasks.json()
        assert len(tasks_payload["tasks"]) == 1
        assert tasks_payload["tasks"][0]["created_at"].endswith("+00:00")
        assert tasks_payload["tasks"][0]["updated_at"].endswith("+00:00")

        candidates = client.get("/research/candidates?approved_only=true")
        assert candidates.status_code == 200
        candidates_payload = candidates.json()
        assert candidates_payload["candidates"][0]["candidate_name"] == "btc_eth_regime_v2"
        assert candidates_payload["candidates"][0]["last_evaluated_at"].endswith("+00:00")
        assert candidates_payload["candidates"][0]["created_at"].endswith("+00:00")
        assert candidates_payload["candidates"][0]["updated_at"].endswith("+00:00")

        overview = client.get("/research/overview?limit=10")
        assert overview.status_code == 200
        overview_payload = overview.json()
        assert overview_payload["candidate_counts"]["approved"] == 1
        assert overview_payload["latest_approvals"][0]["scope"] == "demo"
        assert overview_payload["tasks"][0]["created_at"].endswith("+00:00")
        assert overview_payload["approved_candidates"][0]["created_at"].endswith("+00:00")
        assert overview_payload["latest_evaluations"][0]["created_at"].endswith("+00:00")
        assert overview_payload["latest_approvals"][0]["created_at"].endswith("+00:00")


def test_research_materialize_top_cli_creates_candidate_configs(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    _write_trend_research_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research-materialize-top",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
            "--top-n",
            "2",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["created_count"] == 2
    assert payload["task"]["status"] == "proposed"
    first = payload["candidates"][0]
    generated_path = Path(first["config_path"])
    assert generated_path.exists()

    generated = yaml.safe_load(generated_path.read_text(encoding="utf-8"))
    assert generated["instrument"]["symbol"] == "BTC-USDT-SWAP"
    assert generated["portfolio"]["symbols"] == ["BTC-USDT-SWAP"]
    assert generated["strategy"]["variant"] == "trend_regime_long"
    assert generated["strategy"]["fast_ema"] == 12
    assert generated["strategy"]["slow_ema"] == 36
    assert generated["strategy"]["atr_stop_multiple"] == 3.0


def test_research_materialize_top_cli_prefers_manifest_research_csv(tmp_path: Path) -> None:
    config_path = _write_research_runtime_config(tmp_path)
    _write_trend_research_manifest_csv(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research-materialize-top",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
            "--top-n",
            "1",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["created_count"] == 1
    assert "__" in Path(payload["results_path"]).name
    assert payload["results_artifact"]["resolved_via"] == "manifest"
    assert payload["results_artifact"]["artifact_fingerprint"]
    assert payload["results_artifact"]["path"] == payload["results_path"]


def test_service_api_can_materialize_top_research_candidates(tmp_path: Path) -> None:
    _write_trend_research_csv(tmp_path)
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app_instance = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app_instance) as client:
        response = client.post("/research/materialize-top", json={"top_n": 2})
        assert response.status_code == 200
        payload = response.json()
        assert payload["created_count"] == 2
        assert payload["candidates"][0]["candidate"]["status"] == "draft"
        assert Path(payload["candidates"][0]["config_path"]).exists()


def test_service_api_can_promote_top_research_candidates_from_manifest_csv(tmp_path: Path) -> None:
    _write_trend_research_manifest_csv(tmp_path)
    _write_candidate_backtest_datasets(tmp_path)
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app_instance = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app_instance) as client:
        response = client.post("/research/promote-top", json={"top_n": 1})
        assert response.status_code == 200
        payload = response.json()
        assert payload["created_count"] == 1
        assert payload["evaluated_count"] == 1
        assert "__" in Path(payload["results_path"]).name
        assert payload["results_artifact"]["resolved_via"] == "manifest"


def test_research_promote_top_cli_materializes_backtests_and_evaluates(tmp_path: Path) -> None:
    config_path = _write_research_runtime_config(tmp_path)
    _write_trend_research_csv(tmp_path)
    _write_candidate_backtest_datasets(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research-promote-top",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
            "--top-n",
            "1",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["created_count"] == 1
    assert payload["evaluated_count"] == 1
    evaluation = payload["evaluations"][0]
    assert evaluation["candidate"]["latest_evaluation_status"] == evaluation["evaluation_report"]["status"]
    assert Path(evaluation["backtest"]["artifacts"]["summary_path"]).exists()
    assert Path(evaluation["backtest"]["artifacts"]["report_path"]).exists()
    candidate_id = evaluation["candidate"]["id"]
    assert f"candidate_{candidate_id}_" in Path(evaluation["backtest"]["artifacts"]["summary_path"]).name
    assert "__" in Path(evaluation["backtest"]["artifacts"]["summary_path"]).name
    assert evaluation["evaluation_report"]["artifact_payload"]["logical_prefix"].startswith(f"candidate_{candidate_id}_")
    assert (
        evaluation["evaluation_report"]["artifact_payload"]["summary_file"]["canonical_path"]
        == evaluation["evaluation_report"]["artifact_payload"]["summary_path"]
    )


def test_service_api_can_promote_top_research_candidates(tmp_path: Path) -> None:
    _write_trend_research_csv(tmp_path)
    _write_candidate_backtest_datasets(tmp_path)
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app_instance = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app_instance) as client:
        response = client.post("/research/promote-top", json={"top_n": 1})
        assert response.status_code == 200
        payload = response.json()
        assert payload["created_count"] == 1
        assert payload["evaluated_count"] == 1
        assert payload["evaluations"][0]["evaluation_report"]["status"] in {
            "evaluation_passed",
            "evaluation_review",
            "evaluation_failed",
        }
        summary_path = Path(payload["evaluations"][0]["backtest"]["artifacts"]["summary_path"])
        assert summary_path.exists()
        assert f"candidate_{payload['evaluations'][0]['candidate']['id']}_" in summary_path.name
        assert "__" in summary_path.name


def test_infer_candidate_artifacts_prefers_manifest_backtest_paths(tmp_path: Path) -> None:
    config = _runtime_config(tmp_path)
    report_dir = config.storage.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    identity, _ = backtest_artifact_resolution(config=config, project_root=tmp_path)
    prefix = str(identity["logical_prefix"])
    fingerprint = str(identity["artifact_fingerprint"])
    summary_path = canonical_artifact_path(report_dir, prefix, fingerprint, "summary.json")
    dashboard_path = canonical_artifact_path(report_dir, prefix, fingerprint, "dashboard.html")
    equity_path = canonical_artifact_path(report_dir, prefix, fingerprint, "equity_curve.csv")
    trades_path = canonical_artifact_path(report_dir, prefix, fingerprint, "trades.csv")

    for path in (summary_path, dashboard_path, equity_path, trades_path):
        path.write_text("{}", encoding="utf-8")

    update_artifact_manifest(
        report_dir=report_dir,
        identity=identity,
        artifacts={
            "summary": summary_path,
            "dashboard": dashboard_path,
            "equity_curve": equity_path,
            "trades": trades_path,
        },
    )

    payload = infer_candidate_artifacts(config=config, project_root=tmp_path)

    assert payload["summary_path"] == str(summary_path)
    assert payload["report_path"] == str(dashboard_path)
    assert payload["equity_curve_path"] == str(equity_path)
    assert payload["trades_path"] == str(trades_path)


def _runtime_config(tmp_path: Path) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h", signal_bar="4H", execution_bar="1H"),
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
    )


def _write_research_runtime_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "instrument": {"symbol": "BTC-USDT-SWAP"},
                "strategy": {"name": "ema_trend_4h", "signal_bar": "4H", "execution_bar": "1H"},
                "storage": {
                    "data_dir": "data",
                    "raw_dir": "data/raw",
                    "report_dir": "data/reports",
                },
                "database": {"url": f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"},
                "service": {"heartbeat_interval_seconds": 3600, "report_stale_minutes": 180},
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return config_path


def _write_summary_artifact(tmp_path: Path) -> None:
    report_dir = tmp_path / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    summary_path = report_dir / "BTC-USDT-SWAP_ema_trend_4h_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "initial_equity": 10000.0,
                "final_equity": 13250.0,
                "total_return_pct": 32.5,
                "annualized_return_pct": 9.6,
                "max_drawdown_pct": 8.4,
                "trade_count": 42,
                "win_rate_pct": 40.5,
                "profit_factor": 1.76,
                "sharpe": 1.08,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_trend_research_csv(tmp_path: Path) -> None:
    report_dir = tmp_path / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    results_path = report_dir / "BTC-USDT-SWAP_ema_trend_4h_trend_research.csv"
    results_path.write_text(_trend_research_csv_content(), encoding="utf-8")


def _write_trend_research_manifest_csv(tmp_path: Path) -> None:
    config = _runtime_config(tmp_path)
    report_dir = config.storage.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)
    identity, _ = default_project_research_artifact_resolution(config=config, project_root=tmp_path)
    results_path = canonical_artifact_path(
        report_dir,
        str(identity["logical_prefix"]),
        str(identity["artifact_fingerprint"]),
        "research.csv",
    )
    results_path.write_text(_trend_research_csv_content(), encoding="utf-8")
    update_artifact_manifest(
        report_dir=report_dir,
        identity=identity,
        artifacts={"research_csv": results_path},
    )


def _trend_research_csv_content() -> str:
    return (
        "strategy_name,variant,fast_ema,slow_ema,atr_stop_multiple,trend_ema,adx_threshold,research_score,bear_return_pct,max_drawdown_pct,sharpe,total_return_pct\n"
        "ema_trend_4h,trend_regime_long,12,36,3.0,200,20,0.91,24.5,11.8,1.72,68.0\n"
        "ema_trend_4h,breakout_retest_regime,16,48,3.5,200,25,0.83,18.1,10.4,1.55,61.0\n"
    )


def _write_candidate_backtest_datasets(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    _bull_signal_bars().to_parquet(raw_dir / "BTC-USDT-SWAP_4H.parquet", index=False)
    _bull_execution_bars().to_parquet(raw_dir / "BTC-USDT-SWAP_1H.parquet", index=False)
    _empty_funding().to_parquet(raw_dir / "BTC-USDT-SWAP_funding.parquet", index=False)
    _bull_signal_bars().assign(close=lambda frame: frame["close"] * 1.001).to_parquet(
        raw_dir / "BTC-USDT-SWAP_mark_price_4H.parquet",
        index=False,
    )
    _bull_signal_bars().assign(close=lambda frame: frame["close"] * 0.999).to_parquet(
        raw_dir / "BTC-USDT-SWAP_index_4H.parquet",
        index=False,
    )


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


def _empty_funding() -> pd.DataFrame:
    return pd.DataFrame(columns=["timestamp", "realized_rate"])
