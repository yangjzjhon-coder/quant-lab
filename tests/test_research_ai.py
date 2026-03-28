from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient
from typer.testing import CliRunner

from quant_lab.cli import app
from quant_lab.config import (
    AppConfig,
    DatabaseConfig,
    InstrumentConfig,
    ResearchAIConfig,
    ServiceConfig,
    StorageConfig,
    StrategyConfig,
    load_config,
)
from quant_lab.service.database import init_db, make_session_factory
from quant_lab.service.monitor import build_service_app


def test_load_config_applies_research_ai_env_overrides(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setenv("RESEARCH_AI_ENABLED", "true")
    monkeypatch.setenv("RESEARCH_AI_PROVIDER", "openai_compatible")
    monkeypatch.setenv("RESEARCH_AI_BASE_URL", "https://api.example.com/v1")
    monkeypatch.setenv("RESEARCH_AI_API_KEY", "test-key")
    monkeypatch.setenv("RESEARCH_AI_MODEL", "gpt-test")
    monkeypatch.setenv("RESEARCH_AI_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("RESEARCH_AI_TEMPERATURE", "0.15")
    monkeypatch.setenv("RESEARCH_AI_MAX_OUTPUT_TOKENS", "1500")
    monkeypatch.setenv("RESEARCH_AI_MAX_RETRIES", "3")
    monkeypatch.setenv("RESEARCH_AI_SYSTEM_PROMPT", "Research prompt")

    cfg = load_config(config_path)

    assert cfg.research_ai.enabled is True
    assert cfg.research_ai.provider == "openai_compatible"
    assert cfg.research_ai.base_url == "https://api.example.com/v1"
    assert cfg.research_ai.api_key == "test-key"
    assert cfg.research_ai.model == "gpt-test"
    assert cfg.research_ai.timeout_seconds == 45.0
    assert cfg.research_ai.temperature == 0.15
    assert cfg.research_ai.max_output_tokens == 1500
    assert cfg.research_ai.max_retries == 3
    assert cfg.research_ai.default_system_prompt == "Research prompt"


def test_research_ai_status_cli_reports_disabled_by_default(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research-ai-status",
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
    assert "research_ai is disabled" in payload["warnings"]


def test_service_api_exposes_research_ai_status(tmp_path: Path) -> None:
    config = _runtime_config(
        tmp_path,
        research_ai=ResearchAIConfig(
            enabled=True,
            provider="openai_compatible",
            base_url="https://api.example.com/v1",
            api_key="secret",
            model="gpt-test",
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    app_instance = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app_instance) as client:
        response = client.get("/research/ai/status")
        assert response.status_code == 200
        payload = response.json()
        assert payload["enabled"] is True
        assert payload["ready"] is True
        assert payload["api_key_configured"] is True
        assert payload["capabilities"] == ["chat_completion"]
        assert payload["role_models"]["research_lead"] == "gpt-test"


def test_service_api_exposes_research_ai_run(tmp_path: Path, monkeypatch) -> None:
    config = _runtime_config(
        tmp_path,
        research_ai=ResearchAIConfig(
            enabled=True,
            provider="openai_compatible",
            base_url="https://api.example.com/v1",
            api_key="secret",
            model="gpt-test",
        ),
    )
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    monkeypatch.setattr(
        "quant_lab.service.research_ai._run_openai_compatible_request",
        lambda **kwargs: {
            "provider": "openai_compatible",
            "model": "gpt-test",
            "role": kwargs["request"].role,
            "output_text": "Validated",
            "usage": {"total_tokens": 42},
            "raw_response": {"id": "stub"},
        },
    )

    app_instance = build_service_app(config=config, session_factory=session_factory, project_root=tmp_path)
    with TestClient(app_instance) as client:
        response = client.post(
            "/research/ai/run",
            json={
                "role": "backtest_validator",
                "task": "Check the candidate for optimistic assumptions",
                "context": {"candidate_id": 12},
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["role"] == "backtest_validator"
        assert payload["model"] == "gpt-test"
        assert payload["output_text"] == "Validated"


def _runtime_config(tmp_path: Path, *, research_ai: ResearchAIConfig | None = None) -> AppConfig:
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
        research_ai=research_ai or ResearchAIConfig(),
    )
