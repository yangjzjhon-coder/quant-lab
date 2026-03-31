from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from quant_lab.cli import app
from quant_lab.config import AppConfig, MarketDataConfig, ResearchAIConfig, load_config
from quant_lab.providers.market_data import build_market_data_provider, register_market_data_provider
from quant_lab.service.integrations import build_integration_overview
from quant_lab.service.market_data import build_market_data_status
from quant_lab.service.research_ai import (
    ResearchAIRequest,
    build_research_ai_status,
    register_research_ai_provider,
    run_research_ai_request,
)


def test_load_config_applies_market_data_env_overrides(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setenv("MARKET_DATA_PROVIDER", "stub_feed")
    monkeypatch.setenv("MARKET_DATA_BASE_URL", "https://feed.example.com")
    monkeypatch.setenv("MARKET_DATA_PROXY_URL", "http://127.0.0.1:8899")
    monkeypatch.setenv("MARKET_DATA_TIMEOUT_SECONDS", "12.5")

    cfg = load_config(config_path)

    assert cfg.market_data.provider == "stub_feed"
    assert cfg.market_data.base_url == "https://feed.example.com"
    assert cfg.market_data.proxy_url is not None
    assert cfg.market_data.proxy_url.startswith("http://")
    assert cfg.market_data.proxy_url.endswith(":8899")
    assert cfg.market_data.timeout_seconds == 12.5


def test_market_data_provider_registry_supports_custom_provider() -> None:
    class _StubProvider:
        provider_name = "stub_feed"

        def __init__(self, config: AppConfig) -> None:
            self.config = config

        def close(self) -> None:
            return None

    register_market_data_provider("stub_feed", lambda config: _StubProvider(config))

    config = AppConfig(market_data=MarketDataConfig(provider="stub_feed"))
    provider = build_market_data_provider(config)

    assert provider.provider_name == "stub_feed"
    assert provider.config.market_data.provider == "stub_feed"
    provider.close()


def test_market_data_status_supports_custom_provider_probe_and_help() -> None:
    class _StubProvider:
        provider_name = "stub_feed_status"

        def __init__(self, config: AppConfig) -> None:
            self.config = config

        def close(self) -> None:
            return None

        def capabilities(self, *, cfg, configured: bool) -> list[str]:
            return ["history_candles", "books_snapshot"] if configured else []

        def missing_configuration(self, *, cfg) -> list[str]:
            return []

        def warnings(self, *, cfg) -> list[str]:
            return ["using stub provider for tests"]

        def probe(self, *, cfg) -> dict[str, object]:
            return {"ok": True, "endpoint": "feed://health"}

        def provider_help(self, *, cfg) -> dict[str, object]:
            return {"required": [], "available": ["stub_feed_status"], "notes": ["test stub"]}

    register_market_data_provider("stub_feed_status", lambda config: _StubProvider(config))

    config = AppConfig(
        market_data=MarketDataConfig(
            provider="stub_feed_status",
            api_key="stub-key",
            extra_headers={"X-Test-Feed": "quant-lab"},
            provider_options={"symbol_map": {"BTC-USDT-SWAP": "BTC-PERP"}},
        )
    )

    status = build_market_data_status(config=config, probe=True)

    assert status["provider"] == "stub_feed_status"
    assert status["ready"] is True
    assert status["api_key_configured"] is True
    assert status["extra_headers_keys"] == ["X-Test-Feed"]
    assert status["provider_options_keys"] == ["symbol_map"]
    assert status["capabilities"] == ["history_candles", "books_snapshot"]
    assert status["warnings"] == ["using stub provider for tests"]
    assert status["probe"]["ok"] is True
    assert status["provider_help"]["notes"] == ["test stub"]


def test_integration_overview_summarizes_provider_statuses() -> None:
    class _StubProvider:
        provider_name = "stub_feed_overview"

        def __init__(self, config: AppConfig) -> None:
            self.config = config

        def close(self) -> None:
            return None

        def capabilities(self, *, cfg, configured: bool) -> list[str]:
            return ["history_candles"] if configured else []

        def missing_configuration(self, *, cfg) -> list[str]:
            return []

        def warnings(self, *, cfg) -> list[str]:
            return []

        def probe(self, *, cfg) -> dict[str, object]:
            return {"ok": True}

    class _StubResearchProvider:
        provider_name = "agent_api_overview"

        def capabilities(self, *, cfg, configured: bool) -> list[str]:
            return ["agent_run"] if configured else []

        def missing_configuration(self, *, cfg) -> list[str]:
            return []

        def warnings(self, *, cfg) -> list[str]:
            return []

        def probe(self, *, cfg) -> dict[str, object]:
            return {"ok": True}

        def run(self, *, cfg, request: ResearchAIRequest) -> dict[str, object]:
            return {
                "provider": "agent_api_overview",
                "model": cfg.model or "agent-default",
                "role": request.role,
                "output_text": "ok",
                "usage": {},
                "raw_response": {},
            }

    register_market_data_provider("stub_feed_overview", lambda config: _StubProvider(config))
    register_research_ai_provider("agent_api_overview", lambda: _StubResearchProvider())

    config = AppConfig(
        market_data=MarketDataConfig(provider="stub_feed_overview"),
        research_ai=ResearchAIConfig(enabled=True, provider="agent_api_overview", model="agent-default"),
    )

    payload = build_integration_overview(config=config, probe=True)

    assert payload["summary"]["total"] == 3
    assert payload["summary"]["ready_count"] == 2
    assert payload["statuses"]["market_data"]["ready"] is True
    assert payload["statuses"]["research_ai"]["ready"] is True
    assert payload["statuses"]["research_agent"]["ready"] is False


def test_market_data_status_cli_reports_probe_payload(tmp_path: Path) -> None:
    class _StubProvider:
        provider_name = "stub_feed_cli"

        def __init__(self, config: AppConfig) -> None:
            self.config = config

        def close(self) -> None:
            return None

        def missing_configuration(self, *, cfg) -> list[str]:
            return []

        def warnings(self, *, cfg) -> list[str]:
            return []

        def probe(self, *, cfg) -> dict[str, object]:
            return {"ok": True, "endpoint": "feed://cli"}

    register_market_data_provider("stub_feed_cli", lambda config: _StubProvider(config))
    config_path = tmp_path / "settings.yaml"
    config_path.write_text('market_data:\n  provider: "stub_feed_cli"\n', encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "market-data-status",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
            "--probe",
        ],
    )

    assert result.exit_code == 0
    assert '"provider": "stub_feed_cli"' in result.stdout
    assert '"endpoint": "feed://cli"' in result.stdout


def test_integration_status_cli_returns_all_integration_sections(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "integration-status",
            "--config",
            str(config_path),
            "--project-root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    assert '"market_data"' in result.stdout
    assert '"research_ai"' in result.stdout
    assert '"research_agent"' in result.stdout


def test_research_ai_provider_registry_supports_custom_agent_provider() -> None:
    class _StubResearchProvider:
        provider_name = "agent_api"

        def capabilities(self, *, cfg, configured: bool) -> list[str]:
            return ["agent_run"] if configured else []

        def missing_configuration(self, *, cfg) -> list[str]:
            return []

        def warnings(self, *, cfg) -> list[str]:
            return []

        def probe(self, *, cfg) -> dict[str, object]:
            return {"ok": True, "endpoint": "agent://health"}

        def run(self, *, cfg, request: ResearchAIRequest) -> dict[str, object]:
            return {
                "provider": "agent_api",
                "model": cfg.model or "agent-default",
                "role": request.role,
                "output_text": f"agent handled: {request.task}",
                "usage": {},
                "raw_response": {"request_role": request.role},
            }

    register_research_ai_provider("agent_api", lambda: _StubResearchProvider())

    config = AppConfig(
        research_ai=ResearchAIConfig(
            enabled=True,
            provider="agent_api",
            model="agent-default",
        )
    )

    status = build_research_ai_status(config=config, probe=True)
    assert status["ready"] is True
    assert "agent_api" in status["supported_providers"]
    assert "disabled" in status["supported_providers"]
    assert "openai_compatible" in status["supported_providers"]
    assert status["capabilities"] == ["agent_run"]
    assert status["probe"]["ok"] is True
    assert "agent_api" in status["provider_help"]["available"]
    assert "disabled" in status["provider_help"]["available"]
    assert "openai_compatible" in status["provider_help"]["available"]

    response = run_research_ai_request(
        config=config,
        request=ResearchAIRequest(
            role="research_lead",
            task="check alternate data feed integration",
        ),
    )
    assert response["provider"] == "agent_api"
    assert response["output_text"] == "agent handled: check alternate data feed integration"
