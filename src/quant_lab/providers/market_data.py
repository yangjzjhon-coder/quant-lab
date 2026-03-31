from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

import pandas as pd

from quant_lab.config import AppConfig
from quant_lab.data.okx_public_client import OkxPublicClient


class MarketDataProvider(Protocol):
    provider_name: str

    def close(self) -> None: ...

    def fetch_history_candles(
        self,
        inst_id: str,
        bar: str,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        limit: int = 300,
        max_pages: int = 10000,
        pause_seconds: float = 0.12,
    ) -> pd.DataFrame: ...

    def fetch_funding_rate_history(
        self,
        inst_id: str,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        limit: int = 400,
        max_pages: int = 1000,
        pause_seconds: float = 0.2,
    ) -> pd.DataFrame: ...

    def fetch_open_interest(self, inst_type: str, inst_id: str) -> dict[str, Any]: ...

    def fetch_mark_price(self, inst_type: str, inst_id: str) -> dict[str, Any]: ...

    def fetch_index_ticker(self, index_inst_id: str) -> dict[str, Any]: ...

    def fetch_books_full_snapshot(self, inst_id: str, depth: int = 50) -> dict[str, Any]: ...

    def fetch_history_trades(
        self,
        inst_id: str,
        *,
        limit: int = 100,
        max_pages: int = 10,
        pause_seconds: float = 0.12,
    ) -> pd.DataFrame: ...

    def fetch_history_mark_price_candles(
        self,
        inst_id: str,
        bar: str,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        limit: int = 300,
        max_pages: int = 10000,
        pause_seconds: float = 0.12,
    ) -> pd.DataFrame: ...

    def fetch_history_index_candles(
        self,
        index_inst_id: str,
        bar: str,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        limit: int = 300,
        max_pages: int = 10000,
        pause_seconds: float = 0.12,
    ) -> pd.DataFrame: ...

    def fetch_instrument_details(self, inst_type: str, inst_id: str) -> dict[str, Any]: ...


MarketDataProviderFactory = Callable[[AppConfig], MarketDataProvider]


class OkxMarketDataProvider:
    provider_name = "okx"

    def __init__(self, *, base_url: str, timeout_seconds: float, proxy_url: str | None) -> None:
        self._client = OkxPublicClient(
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            proxy_url=proxy_url,
        )

    def close(self) -> None:
        self._client.close()

    def fetch_history_candles(
        self,
        inst_id: str,
        bar: str,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        limit: int = 300,
        max_pages: int = 10000,
        pause_seconds: float = 0.12,
    ) -> pd.DataFrame:
        return self._client.fetch_history_candles(
            inst_id=inst_id,
            bar=bar,
            start=start,
            end=end,
            limit=limit,
            max_pages=max_pages,
            pause_seconds=pause_seconds,
        )

    def fetch_funding_rate_history(
        self,
        inst_id: str,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        limit: int = 400,
        max_pages: int = 1000,
        pause_seconds: float = 0.2,
    ) -> pd.DataFrame:
        return self._client.fetch_funding_rate_history(
            inst_id=inst_id,
            start=start,
            end=end,
            limit=limit,
            max_pages=max_pages,
            pause_seconds=pause_seconds,
        )

    def fetch_open_interest(self, inst_type: str, inst_id: str) -> dict[str, Any]:
        return self._client.fetch_open_interest(inst_type=inst_type, inst_id=inst_id)

    def fetch_mark_price(self, inst_type: str, inst_id: str) -> dict[str, Any]:
        return self._client.fetch_mark_price(inst_type=inst_type, inst_id=inst_id)

    def fetch_index_ticker(self, index_inst_id: str) -> dict[str, Any]:
        return self._client.fetch_index_ticker(index_inst_id=index_inst_id)

    def fetch_books_full_snapshot(self, inst_id: str, depth: int = 50) -> dict[str, Any]:
        return self._client.fetch_books_full_snapshot(inst_id=inst_id, depth=depth)

    def fetch_history_trades(
        self,
        inst_id: str,
        *,
        limit: int = 100,
        max_pages: int = 10,
        pause_seconds: float = 0.12,
    ) -> pd.DataFrame:
        return self._client.fetch_history_trades(
            inst_id=inst_id,
            limit=limit,
            max_pages=max_pages,
            pause_seconds=pause_seconds,
        )

    def fetch_history_mark_price_candles(
        self,
        inst_id: str,
        bar: str,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        limit: int = 300,
        max_pages: int = 10000,
        pause_seconds: float = 0.12,
    ) -> pd.DataFrame:
        return self._client.fetch_history_mark_price_candles(
            inst_id=inst_id,
            bar=bar,
            start=start,
            end=end,
            limit=limit,
            max_pages=max_pages,
            pause_seconds=pause_seconds,
        )

    def fetch_history_index_candles(
        self,
        index_inst_id: str,
        bar: str,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        limit: int = 300,
        max_pages: int = 10000,
        pause_seconds: float = 0.12,
    ) -> pd.DataFrame:
        return self._client.fetch_history_index_candles(
            index_inst_id=index_inst_id,
            bar=bar,
            start=start,
            end=end,
            limit=limit,
            max_pages=max_pages,
            pause_seconds=pause_seconds,
        )

    def fetch_instrument_details(self, inst_type: str, inst_id: str) -> dict[str, Any]:
        return self._client.fetch_instrument_details(inst_type=inst_type, inst_id=inst_id)


def _build_okx_provider(config: AppConfig) -> MarketDataProvider:
    return OkxMarketDataProvider(
        base_url=str(config.market_data.base_url or config.okx.rest_base_url),
        timeout_seconds=float(config.market_data.timeout_seconds),
        proxy_url=config.market_data.proxy_url if config.market_data.proxy_url is not None else config.okx.proxy_url,
    )


_MARKET_DATA_PROVIDER_REGISTRY: dict[str, MarketDataProviderFactory] = {
    "okx": _build_okx_provider,
}


def register_market_data_provider(name: str, factory: MarketDataProviderFactory) -> None:
    normalized = _normalize_provider_name(name)
    if not normalized:
        raise ValueError("market data provider name must be non-empty")
    _MARKET_DATA_PROVIDER_REGISTRY[normalized] = factory


def build_market_data_provider(config: AppConfig) -> MarketDataProvider:
    provider_name = market_data_provider_name(config)
    factory = _MARKET_DATA_PROVIDER_REGISTRY.get(provider_name)
    if factory is None:
        supported = ", ".join(sorted(_MARKET_DATA_PROVIDER_REGISTRY))
        raise ValueError(f"unsupported market_data provider: {provider_name}. supported: {supported}")
    return factory(config)


def market_data_provider_name(config: AppConfig) -> str:
    return _normalize_provider_name(config.market_data.provider) or "okx"


def supported_market_data_providers() -> list[str]:
    return sorted(_MARKET_DATA_PROVIDER_REGISTRY)


def _normalize_provider_name(name: str | None) -> str:
    return str(name or "").strip().lower()
