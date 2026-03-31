from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import click
import httpx


@dataclass(eq=False)
class QuantLabError(Exception):
    detail: str
    error_code: str = "quant_lab_error"
    error_type: str = "application_error"
    status_code: int = 400
    retryable: bool = False
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.detail)

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "detail": self.detail,
            "error_code": self.error_code,
            "error_type": self.error_type,
            "retryable": self.retryable,
        }
        if self.meta:
            payload["meta"] = self.meta
        return payload


class InvalidRequestError(QuantLabError):
    def __init__(
        self,
        detail: str,
        *,
        error_code: str = "invalid_request",
        retryable: bool = False,
        meta: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            detail=detail,
            error_code=error_code,
            error_type="invalid_request",
            status_code=400,
            retryable=retryable,
            meta=meta or {},
        )


class NotFoundError(QuantLabError):
    def __init__(
        self,
        detail: str,
        *,
        error_code: str = "not_found",
        retryable: bool = False,
        meta: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            detail=detail,
            error_code=error_code,
            error_type="not_found",
            status_code=404,
            retryable=retryable,
            meta=meta or {},
        )


class ConflictError(QuantLabError):
    def __init__(
        self,
        detail: str,
        *,
        error_code: str = "conflict",
        retryable: bool = False,
        meta: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            detail=detail,
            error_code=error_code,
            error_type="conflict",
            status_code=409,
            retryable=retryable,
            meta=meta or {},
        )


class ConfigurationError(QuantLabError):
    def __init__(
        self,
        detail: str,
        *,
        error_code: str = "configuration_error",
        retryable: bool = False,
        meta: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            detail=detail,
            error_code=error_code,
            error_type="configuration_error",
            status_code=409,
            retryable=retryable,
            meta=meta or {},
        )


class ExternalServiceError(QuantLabError):
    def __init__(
        self,
        detail: str,
        *,
        error_code: str = "external_service_error",
        retryable: bool = True,
        meta: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            detail=detail,
            error_code=error_code,
            error_type="external_service_error",
            status_code=502,
            retryable=retryable,
            meta=meta or {},
        )


class ServiceOperationError(QuantLabError):
    def __init__(
        self,
        detail: str,
        *,
        error_code: str = "service_operation_error",
        retryable: bool = False,
        meta: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            detail=detail,
            error_code=error_code,
            error_type="service_operation_error",
            status_code=500,
            retryable=retryable,
            meta=meta or {},
        )


def normalize_error(exc: Exception) -> QuantLabError:
    if isinstance(exc, QuantLabError):
        return exc

    message = str(exc).strip() or type(exc).__name__
    lowered = message.lower()

    if isinstance(exc, FileNotFoundError):
        return NotFoundError(message, error_code="file_not_found")
    if isinstance(exc, PermissionError):
        return ConflictError(message, error_code="permission_denied")
    if isinstance(exc, click.ClickException):
        return InvalidRequestError(message, error_code="cli_validation_error")
    if isinstance(exc, httpx.HTTPError):
        return ExternalServiceError(f"{type(exc).__name__}: {message}", error_code="external_http_error")
    if isinstance(exc, ValueError):
        if "does not exist" in lowered or "not found" in lowered:
            return NotFoundError(message, error_code="resource_not_found")
        return InvalidRequestError(message, error_code="invalid_request")
    if isinstance(exc, RuntimeError):
        if "already active" in lowered or "already exists" in lowered or "conflict" in lowered:
            return ConflictError(message, error_code="runtime_conflict")
        return ServiceOperationError(message, error_code="runtime_error")
    return ServiceOperationError(f"{type(exc).__name__}: {message}", error_code="internal_error")
