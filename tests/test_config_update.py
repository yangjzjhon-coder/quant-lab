from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from quant_lab.config import AppConfig, TradingConfig, configured_symbols, load_config, update_instrument_section


def test_update_instrument_section_writes_expected_values(tmp_path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "okx": {"rest_base_url": "https://www.okx.com"},
                "instrument": {"symbol": "BTC-USDT-SWAP", "instrument_type": "SWAP"},
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    update_instrument_section(
        config_path,
        {
            "symbol": "BTC-USDT-SWAP",
            "instrument_type": "SWAP",
            "contract_value": 0.01,
            "contract_value_currency": "BTC",
            "lot_size": 0.01,
            "min_size": 0.01,
            "tick_size": 0.1,
            "settle_currency": "USDT",
        },
    )

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert raw["instrument"]["lot_size"] == 0.01
    assert raw["instrument"]["min_size"] == 0.01
    assert raw["instrument"]["tick_size"] == 0.1
    assert raw["instrument"]["settle_currency"] == "USDT"


def test_load_config_reads_okx_profile_from_shared_config(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "okx": {
                    "profile": "okx-demo",
                }
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    okx_root = tmp_path / "home"
    okx_root.mkdir(parents=True, exist_ok=True)
    okx_dir = okx_root / ".okx"
    okx_dir.mkdir(parents=True, exist_ok=True)
    (okx_dir / "config.toml").write_text(
        (
            'default_profile = "okx-demo"\n\n'
            "[profiles.okx-demo]\n"
            'api_key = "demo-key"\n'
            'secret_key = "demo-secret"\n'
            'passphrase = "demo-pass"\n'
            "demo = true\n"
            'proxy_url = "http://127.0.0.1:7897"\n'
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(Path, "home", lambda: okx_root)
    monkeypatch.setattr("quant_lab.config._is_wsl_runtime", lambda: False)

    cfg = load_config(config_path)

    assert cfg.okx.profile == "okx-demo"
    assert cfg.okx.api_key == "demo-key"
    assert cfg.okx.secret_key == "demo-secret"
    assert cfg.okx.passphrase == "demo-pass"
    assert cfg.okx.use_demo is True
    assert cfg.okx.proxy_url == "http://127.0.0.1:7897"


def test_load_config_rewrites_localhost_proxy_for_wsl(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "okx": {
                    "profile": "okx-demo",
                }
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    okx_root = tmp_path / "home"
    okx_root.mkdir(parents=True, exist_ok=True)
    okx_dir = okx_root / ".okx"
    okx_dir.mkdir(parents=True, exist_ok=True)
    (okx_dir / "config.toml").write_text(
        (
            'default_profile = "okx-demo"\n\n'
            "[profiles.okx-demo]\n"
            'proxy_url = "http://127.0.0.1:7897"\n'
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(Path, "home", lambda: okx_root)
    monkeypatch.setattr("quant_lab.config._is_wsl_runtime", lambda: True)
    monkeypatch.setattr("quant_lab.config._wsl_default_gateway", lambda: "172.19.0.1")

    cfg = load_config(config_path)

    assert cfg.okx.proxy_url == "http://172.19.0.1:7897"


def test_configured_symbols_prefers_portfolio_list_and_dedupes(tmp_path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "instrument": {
                    "symbol": "BTC-USDT-SWAP",
                    "instrument_type": "SWAP",
                },
                "portfolio": {
                    "symbols": [
                        "BTC-USDT-SWAP",
                        "ETH-USDT-SWAP",
                        "BTC-USDT-SWAP",
                    ]
                },
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    cfg = load_config(config_path)

    assert configured_symbols(cfg) == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]


def test_load_config_rejects_coarser_execution_bar_than_signal_bar(tmp_path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "strategy": {
                    "signal_bar": "1H",
                    "execution_bar": "4H",
                }
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError, match="execution_bar cannot be coarser"):
        load_config(config_path)


def test_load_config_rejects_invalid_risk_relationships(tmp_path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "risk": {
                    "risk_per_trade": 0.05,
                    "portfolio_max_total_risk": 0.03,
                    "portfolio_max_same_direction_risk": 0.025,
                }
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError, match="risk.risk_per_trade must be <= risk.portfolio_max_total_risk"):
        load_config(config_path)


def test_load_config_rejects_conflicting_alert_tls_modes(tmp_path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "alerts": {
                    "smtp_use_tls": True,
                    "smtp_use_ssl": True,
                }
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError, match="smtp_use_tls and alerts.smtp_use_ssl cannot both be true"):
        load_config(config_path)


def test_load_config_revalidates_env_overrides(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("MARKET_DATA_TIMEOUT_SECONDS", "-5")

    with pytest.raises(ValidationError, match="market_data.timeout_seconds must be > 0"):
        load_config(config_path)


def test_load_config_applies_market_data_extended_env_overrides(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("MARKET_DATA_API_KEY", "feed-key")
    monkeypatch.setenv("MARKET_DATA_EXTRA_HEADERS_JSON", '{"X-Test-Feed":"quant-lab"}')
    monkeypatch.setenv(
        "MARKET_DATA_PROVIDER_OPTIONS_JSON",
        '{"symbol_map":{"BTC-USDT-SWAP":"BTC-PERP"},"depth":50}',
    )

    cfg = load_config(config_path)

    assert cfg.market_data.api_key == "feed-key"
    assert cfg.market_data.extra_headers == {"X-Test-Feed": "quant-lab"}
    assert cfg.market_data.provider_options == {
        "symbol_map": {"BTC-USDT-SWAP": "BTC-PERP"},
        "depth": 50,
    }


def test_load_config_applies_research_ai_provider_options_env_override(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv(
        "RESEARCH_AI_PROVIDER_OPTIONS_JSON",
        '{"workspace":"desk-a","features":["summary","critique"]}',
    )

    cfg = load_config(config_path)

    assert cfg.research_ai.provider_options == {
        "workspace": "desk-a",
        "features": ["summary", "critique"],
    }


def test_app_config_rejects_isolated_long_short_mode() -> None:
    with pytest.raises(
        ValidationError,
        match="trading.position_mode=long_short_mode is not supported with trading.td_mode=isolated",
    ):
        AppConfig(trading=TradingConfig(td_mode="isolated", position_mode="long_short_mode"))
