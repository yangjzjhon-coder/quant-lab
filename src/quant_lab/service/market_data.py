from __future__ import annotations

import inspect
from typing import Any

from quant_lab.config import AppConfig
from quant_lab.errors import ExternalServiceError
from quant_lab.providers.market_data import (
    build_market_data_provider,
    market_data_provider_name,
    supported_market_data_providers,
)

_CAPABILITY_METHODS = (
    ("fetch_history_candles", "history_candles"),
    ("fetch_funding_rate_history", "funding_rate_history"),
    ("fetch_open_interest", "open_interest"),
    ("fetch_mark_price", "mark_price"),
    ("fetch_index_ticker", "index_ticker"),
    ("fetch_books_full_snapshot", "books_snapshot"),
    ("fetch_history_trades", "history_trades"),
    ("fetch_history_mark_price_candles", "mark_price_candles"),
    ("fetch_history_index_candles", "index_candles"),
    ("fetch_instrument_details", "instrument_details"),
)


def build_market_data_status(*, config: AppConfig, probe: bool = False) -> dict[str, Any]:
    provider = market_data_provider_name(config)
    supported = provider in supported_market_data_providers()
    provider_impl = None
    provider_init_error: str | None = None

    if supported:
        try:
            provider_impl = build_market_data_provider(config)
        except Exception as exc:
            provider_init_error = f"{type(exc).__name__}: {exc}"

    missing = _missing_configuration(
        config=config,
        provider=provider,
        provider_impl=provider_impl,
        provider_init_error=provider_init_error,
        supported=supported,
    )
    configured = bool(supported and not missing and provider_init_error is None)

    payload: dict[str, Any] = {
        "provider": provider,
        "supported": supported,
        "configured": configured,
        "ready": configured,
        "supported_providers": supported_market_data_providers(),
        "base_url": str(config.market_data.base_url or config.okx.rest_base_url or "").strip() or None,
        "proxy_url": config.market_data.proxy_url if config.market_data.proxy_url is not None else config.okx.proxy_url,
        "timeout_seconds": config.market_data.timeout_seconds,
        "api_key_configured": bool(config.market_data.api_key),
        "extra_headers_keys": sorted((config.market_data.extra_headers or {}).keys()),
        "provider_options_keys": sorted((config.market_data.provider_options or {}).keys()),
        "capabilities": _capabilities(
            config=config,
            provider=provider,
            provider_impl=provider_impl,
            configured=configured,
        ),
        "missing": missing,
        "warnings": _warnings(
            config=config,
            provider=provider,
            provider_impl=provider_impl,
            provider_init_error=provider_init_error,
            supported=supported,
        ),
        "provider_help": _provider_help(
            config=config,
            provider=provider,
            provider_impl=provider_impl,
        ),
        "probe": None,
    }

    if probe and configured:
        payload["probe"] = _probe_provider(
            config=config,
            provider=provider,
            provider_impl=provider_impl,
        )
        payload["ready"] = bool((payload["probe"] or {}).get("ok"))
    elif probe and provider_init_error is not None:
        payload["probe"] = {
            "ok": False,
            "provider": provider,
            "error": provider_init_error,
        }
        payload["ready"] = False

    _safe_close(provider_impl)
    return payload


def _capabilities(*, config: AppConfig, provider: str, provider_impl: Any, configured: bool) -> list[str]:
    if provider_impl is not None:
        payload = _call_optional(
            provider_impl,
            "capabilities",
            config=config,
            cfg=config.market_data,
            configured=configured,
        )
        if isinstance(payload, list):
            return [str(item).strip() for item in payload if str(item).strip()]

    if provider == "okx":
        return [label for _, label in _CAPABILITY_METHODS]

    if provider_impl is None:
        return []

    capabilities: list[str] = []
    for method_name, capability in _CAPABILITY_METHODS:
        if callable(getattr(provider_impl, method_name, None)):
            capabilities.append(capability)
    return capabilities


def _missing_configuration(
    *,
    config: AppConfig,
    provider: str,
    provider_impl: Any,
    provider_init_error: str | None,
    supported: bool,
) -> list[str]:
    if not supported:
        return ["provider"]
    if provider_init_error is not None and provider_impl is None:
        return ["provider_init"]
    payload = _call_optional(
        provider_impl,
        "missing_configuration",
        config=config,
        cfg=config.market_data,
    )
    if isinstance(payload, list):
        return [str(item).strip() for item in payload if str(item).strip()]
    return []


def _warnings(
    *,
    config: AppConfig,
    provider: str,
    provider_impl: Any,
    provider_init_error: str | None,
    supported: bool,
) -> list[str]:
    warnings: list[str] = []
    if not supported:
        warnings.append(f"unsupported provider configured: {provider}")
        return warnings
    if provider_init_error is not None:
        warnings.append(f"provider initialization failed: {provider_init_error}")
    payload = _call_optional(
        provider_impl,
        "warnings",
        config=config,
        cfg=config.market_data,
    )
    if isinstance(payload, list):
        warnings.extend(str(item).strip() for item in payload if str(item).strip())
    return warnings


def _provider_help(*, config: AppConfig, provider: str, provider_impl: Any) -> dict[str, Any]:
    payload = _call_optional(
        provider_impl,
        "provider_help",
        config=config,
        cfg=config.market_data,
    )
    if isinstance(payload, dict):
        return payload

    if provider == "okx":
        return {
            "required": [],
            "symbol_examples": [
                {
                    "quant_lab_symbol": "BTC-USDT-SWAP",
                    "provider_symbol": "BTC-USDT-SWAP",
                },
                {
                    "quant_lab_symbol": "ETH-USDT-SWAP",
                    "provider_symbol": "ETH-USDT-SWAP",
                },
            ],
            "notes": [
                "Defaults to okx.rest_base_url when market_data.base_url is unset.",
                "Defaults to okx.proxy_url when market_data.proxy_url is unset.",
                "Custom providers can consume market_data.api_key, market_data.extra_headers, and market_data.provider_options.",
            ],
            "example_env": {
                "MARKET_DATA_PROVIDER": "okx",
                "MARKET_DATA_BASE_URL": "https://www.okx.com",
                "MARKET_DATA_PROXY_URL": "http://127.0.0.1:7897",
            },
            "example_provider_options": {},
        }

    return {
        "required": ["provider"],
        "available": supported_market_data_providers(),
        "notes": [
            "Custom market data providers can use market_data.api_key, market_data.extra_headers, and market_data.provider_options.",
        ],
    }


def _probe_provider(*, config: AppConfig, provider: str, provider_impl: Any) -> dict[str, Any]:
    if provider_impl is None:
        return {
            "ok": False,
            "provider": provider,
            "error": "provider is not initialized",
        }

    payload = _call_optional(
        provider_impl,
        "probe",
        config=config,
        cfg=config.market_data,
    )
    if isinstance(payload, dict):
        normalized = dict(payload)
        normalized.setdefault("provider", provider)
        return normalized

    try:
        if provider == "okx":
            details = provider_impl.fetch_instrument_details(
                inst_type=config.instrument.instrument_type,
                inst_id=config.instrument.symbol,
            )
            return {
                "ok": True,
                "provider": provider,
                "instrument": config.instrument.symbol,
                "instrument_type": config.instrument.instrument_type,
                "detail_keys": sorted(details.keys()) if isinstance(details, dict) else [],
            }
    except Exception as exc:
        error = ExternalServiceError(
            f"{type(exc).__name__}: {exc}",
            error_code="market_data_probe_failed",
        )
        return {
            "ok": False,
            "provider": provider,
            "error": error.detail,
            "error_code": error.error_code,
            "retryable": error.retryable,
        }

    return {
        "ok": True,
        "provider": provider,
        "note": "provider does not implement an explicit probe() method",
    }


def _call_optional(target: Any, method_name: str, **kwargs: Any) -> Any:
    if target is None:
        return None
    method = getattr(target, method_name, None)
    if not callable(method):
        return None
    signature = inspect.signature(method)
    accepted_kwargs = {
        key: value
        for key, value in kwargs.items()
        if key in signature.parameters
    }
    return method(**accepted_kwargs)


def _safe_close(provider_impl: Any) -> None:
    if provider_impl is None:
        return
    close = getattr(provider_impl, "close", None)
    if callable(close):
        close()
