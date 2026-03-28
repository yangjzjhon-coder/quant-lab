from __future__ import annotations

from pathlib import Path

import yaml

from quant_lab.config import configured_symbols, load_config, update_instrument_section


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
