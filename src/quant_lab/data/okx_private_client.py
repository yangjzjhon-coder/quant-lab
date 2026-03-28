from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import UTC, datetime
from typing import Any

import httpx

from quant_lab.data.okx_public_client import OkxApiError


class OkxPrivateClient:
    def __init__(
        self,
        api_key: str,
        secret_key: str,
        passphrase: str,
        base_url: str = "https://www.okx.com",
        use_demo: bool = False,
        timeout_seconds: float = 20.0,
        proxy_url: str | None = None,
    ) -> None:
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.base_url = base_url.rstrip("/")
        self.use_demo = use_demo
        self.client = httpx.Client(base_url=self.base_url, timeout=timeout_seconds, proxy=proxy_url)

    def close(self) -> None:
        self.client.close()

    def get_account_config(self) -> dict[str, Any]:
        return self._request("GET", "/api/v5/account/config")

    def get_balance(self, ccy: str | None = None) -> dict[str, Any]:
        params = {"ccy": ccy} if ccy else None
        return self._request("GET", "/api/v5/account/balance", params=params)

    def get_positions(
        self,
        inst_type: str | None = None,
        inst_id: str | None = None,
    ) -> dict[str, Any]:
        params = {key: value for key, value in {"instType": inst_type, "instId": inst_id}.items() if value}
        return self._request("GET", "/api/v5/account/positions", params=params or None)

    def get_max_order_size(
        self,
        inst_id: str,
        td_mode: str,
        ccy: str | None = None,
        px: float | None = None,
        leverage: float | None = None,
    ) -> dict[str, Any]:
        params = {
            "instId": inst_id,
            "tdMode": td_mode,
        }
        if ccy:
            params["ccy"] = ccy
        if px is not None:
            params["px"] = _format_decimal(px)
        if leverage is not None:
            params["lever"] = _format_decimal(leverage)
        return self._request("GET", "/api/v5/account/max-size", params=params)

    def get_leverage_info(self, *, inst_id: str, mgn_mode: str) -> dict[str, Any]:
        return self._request(
            "GET",
            "/api/v5/account/leverage-info",
            params={"instId": inst_id, "mgnMode": mgn_mode},
        )

    def get_pending_orders(
        self,
        *,
        inst_type: str | None = None,
        inst_id: str | None = None,
    ) -> dict[str, Any]:
        params = {key: value for key, value in {"instType": inst_type, "instId": inst_id}.items() if value}
        return self._request("GET", "/api/v5/trade/orders-pending", params=params or None)

    def get_pending_algo_orders(
        self,
        *,
        ord_type: str = "conditional",
        inst_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"ordType": ord_type}
        if inst_id:
            params["instId"] = inst_id
        return self._request("GET", "/api/v5/trade/orders-algo-pending", params=params)

    def set_leverage(
        self,
        *,
        lever: float,
        mgn_mode: str,
        inst_id: str | None = None,
        ccy: str | None = None,
        pos_side: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "lever": _format_decimal(lever),
            "mgnMode": mgn_mode,
        }
        if inst_id:
            payload["instId"] = inst_id
        if ccy:
            payload["ccy"] = ccy
        if pos_side:
            payload["posSide"] = pos_side
        return self._request("POST", "/api/v5/account/set-leverage", data=payload)

    def place_algo_order(
        self,
        *,
        inst_id: str,
        td_mode: str,
        side: str,
        ord_type: str,
        size: float,
        pos_side: str | None = None,
        algo_cl_ord_id: str | None = None,
        tag: str | None = None,
        sl_trigger_px: float | None = None,
        sl_ord_px: float | None = None,
        sl_trigger_px_type: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": ord_type,
            "sz": _format_decimal(size),
        }
        if pos_side:
            payload["posSide"] = pos_side
        if algo_cl_ord_id:
            payload["algoClOrdId"] = algo_cl_ord_id
        if tag:
            payload["tag"] = tag
        if sl_trigger_px is not None:
            payload["slTriggerPx"] = _format_decimal(sl_trigger_px)
        if sl_ord_px is not None:
            payload["slOrdPx"] = _format_decimal(sl_ord_px)
        if sl_trigger_px_type:
            payload["slTriggerPxType"] = sl_trigger_px_type
        return self._request("POST", "/api/v5/trade/order-algo", data=payload)

    def cancel_algo_orders(self, orders: list[dict[str, Any]]) -> dict[str, Any]:
        return self._request("POST", "/api/v5/trade/cancel-algos", data=orders)

    def set_position_mode(self, position_mode: str) -> dict[str, Any]:
        return self._request(
            "POST",
            "/api/v5/account/set-position-mode",
            data={"posMode": position_mode},
        )

    def place_order(
        self,
        *,
        inst_id: str,
        td_mode: str,
        side: str,
        ord_type: str,
        size: float,
        pos_side: str | None = None,
        reduce_only: bool | None = None,
        cl_ord_id: str | None = None,
        tag: str | None = None,
        attach_algo_ords: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": ord_type,
            "sz": _format_decimal(size),
        }
        if pos_side:
            payload["posSide"] = pos_side
        if reduce_only is not None:
            payload["reduceOnly"] = str(reduce_only).lower()
        if cl_ord_id:
            payload["clOrdId"] = cl_ord_id
        if tag:
            payload["tag"] = tag
        if attach_algo_ords:
            payload["attachAlgoOrds"] = attach_algo_ords
        return self._request("POST", "/api/v5/trade/order", data=payload)

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        query = httpx.QueryParams({key: value for key, value in (params or {}).items() if value is not None})
        request_path = path if not query else f"{path}?{query}"
        body = json.dumps(_normalize_payload(data), separators=(",", ":")) if data else ""
        headers = self._build_headers(method=method, request_path=request_path, body=body)
        response = self.client.request(method, path, params=params, content=body or None, headers=headers)
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") not in {None, "0"}:
            raise OkxApiError(f"OKX returned error: {payload}")
        return payload

    def _build_headers(self, method: str, request_path: str, body: str) -> dict[str, str]:
        timestamp = _okx_timestamp()
        prehash = f"{timestamp}{method.upper()}{request_path}{body}"
        signature = base64.b64encode(
            hmac.new(
                self.secret_key.encode("utf-8"),
                prehash.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        if self.use_demo:
            headers["x-simulated-trading"] = "1"
        return headers


def _okx_timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _format_decimal(value: float) -> str:
    return format(value, "f").rstrip("0").rstrip(".") or "0"


def _normalize_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_payload(item) for item in value]
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        return _format_decimal(value)
    return value
