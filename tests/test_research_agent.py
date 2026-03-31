from __future__ import annotations

import json
import subprocess
from pathlib import Path

from fastapi.testclient import TestClient
from typer.testing import CliRunner

from quant_lab.cli import app
from quant_lab.config import (
    AppConfig,
    DatabaseConfig,
    InstrumentConfig,
    ResearchAgentConfig,
    ServiceConfig,
    StorageConfig,
    StrategyConfig,
    load_config,
)
from quant_lab.service.database import init_db, make_session_factory
from quant_lab.service.monitor import build_service_app
from quant_lab.service.research_agent import (
    ResearchAgentRequest,
    build_research_agent_status,
    run_research_agent_workflow,
    supported_research_agent_providers,
)


def test_load_config_applies_research_agent_env_overrides(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setenv("RESEARCH_AGENT_ENABLED", "true")
    monkeypatch.setenv("RESEARCH_AGENT_PROVIDER", "http_json")
    monkeypatch.setenv("RESEARCH_AGENT_BASE_URL", "https://agents.example.com")
    monkeypatch.setenv("RESEARCH_AGENT_API_KEY", "agent-key")
    monkeypatch.setenv("RESEARCH_AGENT_TIMEOUT_SECONDS", "75")
    monkeypatch.setenv("RESEARCH_AGENT_MAX_RETRIES", "2")
    monkeypatch.setenv("RESEARCH_AGENT_WORKFLOW_PATH", "/api/run")
    monkeypatch.setenv("RESEARCH_AGENT_PROBE_PATH", "/api/health")
    monkeypatch.setenv("RESEARCH_AGENT_LOCAL_REPO_PATH", str(tmp_path / "TradingAgents"))
    monkeypatch.setenv("RESEARCH_AGENT_PYTHON_EXECUTABLE", "python-ta")
    monkeypatch.setenv(
        "RESEARCH_AGENT_PROVIDER_OPTIONS_JSON",
        json.dumps({"debug": True, "selected_analysts": ["market", "news"]}),
    )

    cfg = load_config(config_path)

    assert cfg.research_agent.enabled is True
    assert cfg.research_agent.provider == "http_json"
    assert cfg.research_agent.base_url == "https://agents.example.com"
    assert cfg.research_agent.api_key == "agent-key"
    assert cfg.research_agent.timeout_seconds == 75.0
    assert cfg.research_agent.max_retries == 2
    assert cfg.research_agent.workflow_path == "/api/run"
    assert cfg.research_agent.probe_path == "/api/health"
    assert cfg.research_agent.local_repo_path == tmp_path / "TradingAgents"
    assert cfg.research_agent.python_executable == "python-ta"
    assert cfg.research_agent.provider_options == {"debug": True, "selected_analysts": ["market", "news"]}


def test_load_config_resolves_research_agent_relative_paths(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "external" / "TradingAgents").mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "settings.yaml"
    config_path.write_text(
        """
research_agent:
  enabled: true
  provider: "tradingagents"
  local_repo_path: "external/TradingAgents"
  python_executable: ".venvs/tradingagents/bin/python"
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(config_path)

    assert cfg.research_agent.local_repo_path == (tmp_path / "external" / "TradingAgents").resolve()
    assert cfg.research_agent.python_executable == str((tmp_path / ".venvs" / "tradingagents" / "bin" / "python").resolve())


def test_research_agent_status_cli_reports_disabled_by_default(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research-agent-status",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["enabled"] is False
    assert payload["provider"] == "disabled"
    assert payload["ready"] is False
    assert payload["supported_providers"] == supported_research_agent_providers()
    assert payload["provider_help"]["available"] == supported_research_agent_providers()
    assert "research_agent is disabled" in payload["warnings"]


def test_research_agent_run_cli_returns_structured_error_when_disabled(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research-agent-run",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
            "--task",
            "check disabled external agent",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["source"] == "cli"
    assert payload["command"] == "research-agent-run"
    assert payload["error_code"] == "research_agent_disabled"
    assert payload["error_type"] == "configuration_error"


def test_service_api_exposes_research_agent_status_and_run_workflow(tmp_path: Path, monkeypatch) -> None:
    config = _runtime_config(
        tmp_path,
        research_agent=ResearchAgentConfig(
            enabled=True,
            provider="http_json",
            base_url="https://agents.example.com",
            api_key="secret",
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    monkeypatch.setattr(
        "quant_lab.service.research_agent._request_json",
        lambda **kwargs: (
            {"ok": True}
            if kwargs["method"] == "GET"
            else {
                "summary": "Use a regime filter before breakout entries on BTC and ETH.",
                "task": {
                    "title": "Agent-generated breakout regime study",
                    "hypothesis": "Adding regime filters should improve drawdown behavior.",
                    "notes": "Generated by external agent.",
                },
                "candidate": {
                    "candidate_name": "agent_breakout_regime_v1",
                    "strategy_name": "ema_trend_4h",
                    "variant": "breakout_retest_regime",
                    "timeframe": "4H",
                    "symbol_scope": ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
                    "thesis": "Trade only when the agent confirms a supportive regime.",
                    "tags": ["agent", "regime", "breakout"],
                    "details": {"source": "stub-agent"},
                },
            }
        ),
    )

    app_instance = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app_instance) as client:
        status = client.get("/research/agent/status")
        assert status.status_code == 200
        assert status.json()["ready"] is True
        assert status.json()["capabilities"] == ["structured_research_workflow"]

        response = client.post(
            "/research/agent/run",
            json={
                "role": "research_lead",
                "task": "Study breakout plus regime confirmation",
                "symbols": ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
                "notes": "Created from service API",
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["task"]["status"] == "proposed"
        assert payload["candidate"]["status"] == "draft"
        assert payload["candidate"]["candidate_name"] == "agent_breakout_regime_v1"
        assert payload["candidate"]["symbol_scope"] == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
        details = payload["candidate"]["details"]["research_agent"]
        assert details["provider"] == "http_json"
        assert "Use a regime filter" in details["summary"]


def test_research_agent_status_supports_tradingagents_probe(tmp_path: Path, monkeypatch) -> None:
    repo_dir = tmp_path / "TradingAgents"
    repo_dir.mkdir(parents=True, exist_ok=True)
    config = _runtime_config(
        tmp_path,
        research_agent=ResearchAgentConfig(
            enabled=True,
            provider="tradingagents",
            local_repo_path=repo_dir,
            python_executable="python-ta",
        ),
    )

    def fake_run(command, **kwargs):
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=json.dumps({"ok": True, "provider": "tradingagents", "default_config_keys": ["llm_provider"]}),
            stderr="",
        )

    monkeypatch.setattr("quant_lab.service.research_agent.subprocess.run", fake_run)

    payload = build_research_agent_status(config=config, probe=True)

    assert payload["provider"] == "tradingagents"
    assert payload["configured"] is True
    assert payload["ready"] is True
    assert payload["local_repo_path"] == str(repo_dir)
    assert payload["python_executable"] == "python-ta"
    assert payload["capabilities"] == ["structured_research_workflow", "local_repo_subprocess"]
    assert payload["probe"]["ok"] is True
    assert payload["provider_help"]["required"] == ["local_repo_path"]
    assert payload["provider_help"]["symbol_examples"][0]["provider_symbol"] == "BTC-USD"
    assert Path(payload["provider_help"]["runner_path"]).name == "tradingagents_runner.py"
    assert Path(payload["provider_help"]["bootstrap_script"]).name == "bootstrap_tradingagents_env.py"


def test_run_research_agent_workflow_supports_tradingagents_subprocess(tmp_path: Path, monkeypatch) -> None:
    repo_dir = tmp_path / "TradingAgents"
    repo_dir.mkdir(parents=True, exist_ok=True)
    config = _runtime_config(
        tmp_path,
        research_agent=ResearchAgentConfig(
            enabled=True,
            provider="tradingagents",
            local_repo_path=repo_dir,
            python_executable="python-ta",
            provider_options={
                "debug": True,
                "selected_analysts": ["market", "news"],
                "config_overrides": {"llm_provider": "openai"},
                "environment": {"OPENAI_API_KEY": "test-openai-key"},
            },
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)
    calls: list[dict[str, object]] = []

    def fake_run(command, **kwargs):
        request_payload = json.loads(kwargs["input"])
        calls.append({"command": command, "kwargs": kwargs, "payload": request_payload})
        decision = "BUY" if request_payload["symbol"] == "BTC-USDT-SWAP" else "HOLD"
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=json.dumps(
                {
                    "ok": True,
                    "provider": "tradingagents",
                    "company_name": request_payload["company_name"],
                    "trade_date": request_payload["trade_date"],
                    "decision": decision,
                    "selected_analysts": request_payload["selected_analysts"],
                    "final_state": {
                        "company_of_interest": request_payload["company_name"],
                        "trade_date": request_payload["trade_date"],
                        "final_trade_decision": decision,
                        "investment_plan": f"Plan for {request_payload['company_name']}",
                    },
                }
            ),
            stderr="",
        )

    monkeypatch.setattr("quant_lab.service.research_agent.subprocess.run", fake_run)

    payload = run_research_agent_workflow(
        config=config,
        session_factory=session_factory,
        request=ResearchAgentRequest(
            task="Run TradingAgents workflow for BTC and ETH",
            symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
            context={"trade_date": "2026-03-29"},
        ),
    )

    assert payload["task"]["status"] == "proposed"
    assert payload["candidate"]["status"] == "draft"
    assert payload["candidate"]["strategy_name"] == "ema_trend_4h"
    assert payload["candidate"]["symbol_scope"] == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    details = payload["candidate"]["details"]["research_agent"]
    assert details["provider"] == "tradingagents"
    assert "TradingAgents decisions:" in details["summary"]
    assert len(details["raw_response"]["results"]) == 2
    assert details["raw_response"]["results"][0]["company_name"] == "BTC-USD"
    assert details["raw_response"]["results"][1]["company_name"] == "ETH-USD"

    assert len(calls) == 2
    assert calls[0]["command"][0] == "python-ta"
    assert Path(calls[0]["command"][1]).name == "tradingagents_runner.py"
    assert calls[0]["kwargs"]["cwd"] == str(repo_dir)
    assert calls[0]["kwargs"]["env"]["OPENAI_API_KEY"] == "test-openai-key"
    assert calls[0]["payload"]["config_overrides"] == {"llm_provider": "openai"}
    assert calls[0]["payload"]["trade_date"] == "2026-03-29"


def _runtime_config(tmp_path: Path, *, research_agent: ResearchAgentConfig | None = None) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
        research_agent=research_agent or ResearchAgentConfig(),
    )
