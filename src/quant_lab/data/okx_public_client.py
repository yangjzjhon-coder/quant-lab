from __future__ import annotations

import json
import shutil
import subprocess
import time
from typing import Any
from urllib.parse import urlencode

import httpx
import pandas as pd
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed


class OkxApiError(RuntimeError):
    pass


class OkxPublicClient:
    def __init__(
        self,
        base_url: str = "https://www.okx.com",
        timeout_seconds: float = 20.0,
        proxy_url: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(base_url=self.base_url, timeout=timeout_seconds, proxy=proxy_url)
        self._use_windows_fallback = False

    def close(self) -> None:
        self.client.close()

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
        frames: list[pd.DataFrame] = []
        cursor = _timestamp_ms(end) if end is not None else None

        for _ in range(max_pages):
            params: dict[str, Any] = {"instId": inst_id, "bar": bar, "limit": str(limit)}
            if cursor is not None:
                params["after"] = str(cursor)

            payload = self._get_json("/api/v5/market/history-candles", params=params)
            rows = payload.get("data", [])
            if not rows:
                break

            frame = _parse_candles(rows)
            if frame.empty:
                break

            frames.append(frame)

            oldest_ts = int(frame["timestamp"].min().timestamp() * 1000)
            if start is not None and oldest_ts <= _timestamp_ms(start):
                break
            if cursor is not None and oldest_ts == cursor:
                break

            cursor = oldest_ts
            time.sleep(pause_seconds)

        if not frames:
            return pd.DataFrame(columns=_candle_columns())

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(
            drop=True
        )

        if start is not None:
            combined = combined.loc[combined["timestamp"] >= start]
        if end is not None:
            combined = combined.loc[combined["timestamp"] <= end]

        return combined.reset_index(drop=True)

    def fetch_funding_rate_history(
        self,
        inst_id: str,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        limit: int = 400,
        max_pages: int = 1000,
        pause_seconds: float = 0.2,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        cursor = _timestamp_ms(end) if end is not None else None

        for _ in range(max_pages):
            params: dict[str, Any] = {"instId": inst_id, "limit": str(limit)}
            if cursor is not None:
                params["after"] = str(cursor)

            payload = self._get_json("/api/v5/public/funding-rate-history", params=params)
            rows = payload.get("data", [])
            if not rows:
                break

            frame = _parse_funding(rows)
            if frame.empty:
                break

            frames.append(frame)

            oldest_ts = int(frame["timestamp"].min().timestamp() * 1000)
            if start is not None and oldest_ts <= _timestamp_ms(start):
                break
            if cursor is not None and oldest_ts == cursor:
                break

            cursor = oldest_ts
            time.sleep(pause_seconds)

        if not frames:
            return pd.DataFrame(columns=["timestamp", "funding_rate", "realized_rate"])

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(
            drop=True
        )

        if start is not None:
            combined = combined.loc[combined["timestamp"] >= start]
        if end is not None:
            combined = combined.loc[combined["timestamp"] <= end]

        return combined.reset_index(drop=True)

    def fetch_open_interest(self, inst_type: str, inst_id: str) -> dict[str, Any]:
        payload = self._get_json(
            "/api/v5/public/open-interest",
            params={"instType": inst_type, "instId": inst_id},
        )
        rows = payload.get("data", [])
        if not rows:
            raise OkxApiError(f"No open interest returned for {inst_type} {inst_id}.")

        row = rows[0]
        return {
            "timestamp": _parse_timestamp_ms(row.get("ts")),
            "symbol": row.get("instId") or inst_id,
            "open_interest_contracts": _to_float(row.get("oi")),
            "open_interest_currency": _to_float(row.get("oiCcy")),
        }

    def fetch_mark_price(self, inst_type: str, inst_id: str) -> dict[str, Any]:
        payload = self._get_json(
            "/api/v5/public/mark-price",
            params={"instType": inst_type, "instId": inst_id},
        )
        rows = payload.get("data", [])
        if not rows:
            raise OkxApiError(f"No mark price returned for {inst_type} {inst_id}.")

        row = rows[0]
        return {
            "timestamp": _parse_timestamp_ms(row.get("ts")),
            "symbol": row.get("instId") or inst_id,
            "mark_price": _to_float(row.get("markPx")),
        }

    def fetch_index_ticker(self, index_inst_id: str) -> dict[str, Any]:
        payload = self._get_json(
            "/api/v5/market/index-tickers",
            params={"instId": index_inst_id},
        )
        rows = payload.get("data", [])
        if not rows:
            raise OkxApiError(f"No index ticker returned for {index_inst_id}.")

        row = rows[0]
        return {
            "timestamp": _parse_timestamp_ms(row.get("ts")),
            "index_inst_id": row.get("instId") or index_inst_id,
            "index_price": _to_float(row.get("idxPx")),
            "high_24h": _to_float(row.get("high24h")),
            "low_24h": _to_float(row.get("low24h")),
            "open_24h": _to_float(row.get("open24h")),
        }

    def fetch_books_full_snapshot(self, inst_id: str, depth: int = 50) -> dict[str, Any]:
        payload = self._get_json(
            "/api/v5/market/books-full",
            params={"instId": inst_id, "sz": str(depth)},
        )
        rows = payload.get("data", [])
        if not rows:
            raise OkxApiError(f"No books-full snapshot returned for {inst_id}.")

        row = rows[0]
        return {
            "timestamp": _parse_timestamp_ms(row.get("ts")),
            "symbol": inst_id,
            "depth": depth,
            "bids": _parse_book_levels(row.get("bids", [])),
            "asks": _parse_book_levels(row.get("asks", [])),
        }

    def fetch_history_trades(
        self,
        inst_id: str,
        *,
        limit: int = 100,
        max_pages: int = 10,
        pause_seconds: float = 0.12,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        cursor: str | None = None

        for _ in range(max_pages):
            params: dict[str, Any] = {"instId": inst_id, "limit": str(limit)}
            if cursor is not None:
                params["after"] = cursor

            payload = self._get_json("/api/v5/market/history-trades", params=params)
            rows = payload.get("data", [])
            if not rows:
                break

            frame = _parse_trades(rows)
            if frame.empty:
                break

            frames.append(frame)
            oldest_trade_id = str(frame["trade_id"].iloc[-1])
            if cursor is not None and oldest_trade_id == cursor:
                break
            cursor = oldest_trade_id
            time.sleep(pause_seconds)

        if not frames:
            return pd.DataFrame(columns=["timestamp", "symbol", "trade_id", "price", "size", "side", "count"])

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=["symbol", "trade_id"]).sort_values(
            ["timestamp", "trade_id"]
        )
        return combined.reset_index(drop=True)

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
        return self._fetch_reference_candles(
            path="/api/v5/market/history-mark-price-candles",
            params={"instId": inst_id, "bar": bar},
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
        return self._fetch_reference_candles(
            path="/api/v5/market/history-index-candles",
            params={"instId": index_inst_id, "bar": bar},
            start=start,
            end=end,
            limit=limit,
            max_pages=max_pages,
            pause_seconds=pause_seconds,
        )

    def fetch_instrument_details(self, inst_type: str, inst_id: str) -> dict[str, Any]:
        payload = self._get_json(
            "/api/v5/public/instruments",
            params={"instType": inst_type, "instId": inst_id},
        )
        rows = payload.get("data", [])
        if not rows:
            raise OkxApiError(f"No instrument metadata returned for {inst_type} {inst_id}.")

        record = rows[0]
        return {
            "symbol": record["instId"],
            "instrument_type": record["instType"],
            "contract_value": float(record["ctVal"]),
            "contract_value_currency": record["ctValCcy"],
            "lot_size": float(record["lotSz"]),
            "min_size": float(record["minSz"]),
            "tick_size": float(record["tickSz"]),
            "settle_currency": record.get("settleCcy") or None,
        }

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, OkxApiError)),
        stop=stop_after_attempt(5),
        wait=wait_fixed(1),
        reraise=True,
    )
    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        if self._use_windows_fallback:
            payload = self._get_json_via_windows(path=path, params=params)
        else:
            try:
                response = self.client.get(path, params=params)
                response.raise_for_status()
                payload = response.json()
            except httpx.HTTPError:
                self._use_windows_fallback = True
                payload = self._get_json_via_windows(path=path, params=params)

        if payload.get("code") not in {None, "0"}:
            raise OkxApiError(f"OKX returned error: {payload}")
        return payload

    def _get_json_via_windows(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        powershell = shutil.which("powershell.exe")
        if not powershell:
            raise RuntimeError("powershell.exe is not available for the Windows network fallback.")

        query = urlencode({key: value for key, value in params.items() if value is not None})
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"

        command = [
            powershell,
            "-NoProfile",
            "-Command",
            (
                "$ProgressPreference='SilentlyContinue'; "
                "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; "
                f"(Invoke-WebRequest -Uri '{url}' -TimeoutSec 60).Content"
            ),
        ]

        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=90,
        )
        return json.loads(result.stdout)

    def _fetch_reference_candles(
        self,
        *,
        path: str,
        params: dict[str, Any],
        start: pd.Timestamp | None,
        end: pd.Timestamp | None,
        limit: int,
        max_pages: int,
        pause_seconds: float,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        cursor = _timestamp_ms(end) if end is not None else None

        for _ in range(max_pages):
            request_params = dict(params)
            request_params["limit"] = str(limit)
            if cursor is not None:
                request_params["after"] = str(cursor)

            payload = self._get_json(path, params=request_params)
            rows = payload.get("data", [])
            if not rows:
                break

            frame = _parse_reference_candles(rows)
            if frame.empty:
                break

            frames.append(frame)

            oldest_ts = int(frame["timestamp"].min().timestamp() * 1000)
            if start is not None and oldest_ts <= _timestamp_ms(start):
                break
            if cursor is not None and oldest_ts == cursor:
                break

            cursor = oldest_ts
            time.sleep(pause_seconds)

        if not frames:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "confirm"])

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(
            drop=True
        )

        if start is not None:
            combined = combined.loc[combined["timestamp"] >= start]
        if end is not None:
            combined = combined.loc[combined["timestamp"] <= end]

        return combined.reset_index(drop=True)


def _parse_candles(rows: list[list[str]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=_candle_columns())
    if frame.empty:
        return frame

    numeric_columns = ["open", "high", "low", "close", "volume", "volume_ccy", "volume_quote"]
    for column in numeric_columns:
        frame[column] = frame[column].astype(float)

    frame["timestamp"] = pd.to_datetime(frame["timestamp"].astype("int64"), unit="ms", utc=True)
    frame["confirm"] = frame["confirm"].astype(str)
    frame = frame.loc[frame["confirm"] == "1"].reset_index(drop=True)
    return frame


def _parse_funding(rows: list[dict[str, str]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    frame["timestamp"] = pd.to_datetime(frame["fundingTime"].astype("int64"), unit="ms", utc=True)
    frame["funding_rate"] = frame["fundingRate"].astype(float)
    frame["realized_rate"] = frame["realizedRate"].astype(float)
    return frame[["timestamp", "funding_rate", "realized_rate"]]


def _parse_reference_candles(rows: list[list[str]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "confirm"])

    normalized_rows = [row[:6] if len(row) >= 6 else [*row[:5], "1"] for row in rows]
    frame = pd.DataFrame(normalized_rows, columns=["timestamp", "open", "high", "low", "close", "confirm"])
    for column in ("open", "high", "low", "close"):
        frame[column] = frame[column].astype(float)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"].astype("int64"), unit="ms", utc=True)
    frame["confirm"] = frame["confirm"].astype(str)
    frame = frame.loc[frame["confirm"] == "1"].reset_index(drop=True)
    return frame


def _parse_trades(rows: list[dict[str, str]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "symbol", "trade_id", "price", "size", "side", "count"])

    frame["timestamp"] = pd.to_datetime(frame["ts"].astype("int64"), unit="ms", utc=True)
    frame["symbol"] = frame["instId"].astype(str)
    frame["trade_id"] = frame["tradeId"].astype(str)
    frame["price"] = frame["px"].astype(float)
    frame["size"] = frame["sz"].astype(float)
    frame["side"] = frame["side"].astype(str)
    if "count" in frame.columns:
        frame["count"] = frame["count"].apply(_to_float)
    else:
        frame["count"] = None
    return frame[["timestamp", "symbol", "trade_id", "price", "size", "side", "count"]]


def _parse_book_levels(levels: list[list[str]]) -> list[dict[str, float | int | None]]:
    parsed: list[dict[str, float | int | None]] = []
    for level in levels:
        if not level:
            continue
        parsed.append(
            {
                "price": _to_float(level[0]) if len(level) > 0 else None,
                "size": _to_float(level[1]) if len(level) > 1 else None,
                "liquidated_orders": _to_float(level[2]) if len(level) > 2 else None,
                "order_count": int(float(level[3])) if len(level) > 3 and level[3] not in {"", None} else None,
            }
        )
    return parsed


def _parse_timestamp_ms(raw: str | None) -> pd.Timestamp | None:
    if raw in {None, "", " "}:
        return None
    return pd.to_datetime(int(str(raw)), unit="ms", utc=True)


def _to_float(value: Any) -> float | None:
    if value in {None, "", " "}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _timestamp_ms(value: pd.Timestamp | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.tz_localize("UTC")
    return int(value.timestamp() * 1000)


def _candle_columns() -> list[str]:
    return [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "volume_ccy",
        "volume_quote",
        "confirm",
    ]
