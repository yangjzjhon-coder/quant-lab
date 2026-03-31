from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from typing import Protocol

import httpx
from pydantic import BaseModel, Field

from quant_lab.config import AppConfig, ResearchAIConfig
from quant_lab.errors import ConfigurationError, ExternalServiceError, InvalidRequestError

SUPPORTED_RESEARCH_AI_PROVIDERS = {"disabled", "openai_compatible"}

DEFAULT_RESEARCH_AI_ROLE_PROMPTS = {
    "research_lead": (
        "You are the research lead for a crypto systematic trading desk. "
        "Turn raw evidence into a precise research plan with explicit assumptions, risks, and next checks."
    ),
    "factor_analyst": (
        "You are a factor analyst for BTC and ETH trend-following research. "
        "Explain which drivers are measurable, which are missing, and how each one should be validated."
    ),
    "strategy_builder": (
        "You are a systematic strategy builder. "
        "Convert research findings into concrete entry, exit, sizing, and invalidation rules."
    ),
    "backtest_validator": (
        "You are a backtest validator. "
        "Look for future leakage, optimistic fill assumptions, regime bias, and missing real-world frictions."
    ),
    "risk_officer": (
        "You are a trading risk officer. "
        "Stress-test the proposal, identify failure modes, and define risk limits before deployment."
    ),
}


class ResearchAIRequest(BaseModel):
    role: str = "research_lead"
    task: str
    context: dict[str, Any] = Field(default_factory=dict)
    system_prompt: str | None = None
    temperature: float | None = None
    max_output_tokens: int | None = None


class ResearchAIResponse(BaseModel):
    provider: str
    model: str
    role: str
    output_text: str
    usage: dict[str, Any] = Field(default_factory=dict)
    raw_response: dict[str, Any] = Field(default_factory=dict)


class ResearchAIProvider(Protocol):
    provider_name: str

    def capabilities(self, *, cfg: ResearchAIConfig, configured: bool) -> list[str]: ...

    def missing_configuration(self, *, cfg: ResearchAIConfig) -> list[str]: ...

    def warnings(self, *, cfg: ResearchAIConfig) -> list[str]: ...

    def probe(self, *, cfg: ResearchAIConfig) -> dict[str, Any]: ...

    def run(self, *, cfg: ResearchAIConfig, request: ResearchAIRequest) -> dict[str, Any]: ...


ResearchAIProviderFactory = Callable[[], ResearchAIProvider]


class _OpenAICompatibleResearchAIProvider:
    provider_name = "openai_compatible"

    def capabilities(self, *, cfg: ResearchAIConfig, configured: bool) -> list[str]:
        return ["chat_completion"] if configured else []

    def missing_configuration(self, *, cfg: ResearchAIConfig) -> list[str]:
        missing: list[str] = []
        if not str(cfg.base_url or "").strip():
            missing.append("base_url")
        if not str(cfg.api_key or "").strip():
            missing.append("api_key")
        if not str(cfg.model or "").strip() and not cfg.role_models:
            missing.append("model")
        return missing

    def warnings(self, *, cfg: ResearchAIConfig) -> list[str]:
        warnings: list[str] = []
        if not str(cfg.base_url or "").strip():
            warnings.append("openai_compatible provider requires research_ai.base_url")
        return warnings

    def probe(self, *, cfg: ResearchAIConfig) -> dict[str, Any]:
        endpoint = _build_endpoint(cfg.base_url, "models")
        try:
            payload = _request_json(
                method="GET",
                url=endpoint,
                headers=_request_headers(cfg),
                timeout_seconds=min(cfg.timeout_seconds, 15.0),
                max_retries=0,
            )
            models = payload.get("data") if isinstance(payload.get("data"), list) else []
            return {
                "ok": True,
                "endpoint": endpoint,
                "model_count": len(models),
            }
        except Exception as exc:
            return {
                "ok": False,
                "endpoint": endpoint,
                "error": f"{type(exc).__name__}: {exc}",
            }

    def run(self, *, cfg: ResearchAIConfig, request: ResearchAIRequest) -> dict[str, Any]:
        return _run_openai_compatible_request(cfg=cfg, request=request)


_RESEARCH_AI_PROVIDER_REGISTRY: dict[str, ResearchAIProviderFactory] = {
    "openai_compatible": _OpenAICompatibleResearchAIProvider,
}


def register_research_ai_provider(name: str, factory: ResearchAIProviderFactory) -> None:
    normalized = _normalize_provider(name)
    if normalized in {"", "disabled"}:
        raise InvalidRequestError(
            "research_ai provider name must be non-empty and cannot be 'disabled'",
            error_code="invalid_research_ai_provider_name",
        )
    _RESEARCH_AI_PROVIDER_REGISTRY[normalized] = factory


def get_research_ai_provider(provider: str | None) -> ResearchAIProvider | None:
    normalized = _normalize_provider(provider)
    if normalized == "disabled":
        return None
    factory = _RESEARCH_AI_PROVIDER_REGISTRY.get(normalized)
    return None if factory is None else factory()


def supported_research_ai_providers() -> list[str]:
    return sorted({"disabled", *list(_RESEARCH_AI_PROVIDER_REGISTRY)})


def build_research_ai_status(*, config: AppConfig, probe: bool = False) -> dict[str, Any]:
    cfg = config.research_ai
    provider = _normalize_provider(cfg.provider)
    provider_impl = get_research_ai_provider(provider)
    missing = _missing_configuration(cfg=cfg, provider=provider)
    supported = provider == "disabled" or provider_impl is not None
    configured = bool(cfg.enabled and supported and not missing)

    payload: dict[str, Any] = {
        "enabled": bool(cfg.enabled),
        "provider": provider,
        "supported": supported,
        "configured": configured,
        "ready": configured,
        "supported_providers": supported_research_ai_providers(),
        "model": cfg.model,
        "base_url": cfg.base_url,
        "timeout_seconds": cfg.timeout_seconds,
        "temperature": cfg.temperature,
        "max_output_tokens": cfg.max_output_tokens,
        "max_retries": cfg.max_retries,
        "api_key_configured": bool(cfg.api_key),
        "default_system_prompt_configured": bool(cfg.default_system_prompt),
        "provider_options_keys": sorted((cfg.provider_options or {}).keys()),
        "role_models": _effective_role_models(cfg),
        "role_system_prompts": sorted((cfg.role_system_prompts or {}).keys()),
        "capabilities": provider_impl.capabilities(cfg=cfg, configured=configured) if provider_impl is not None else [],
        "missing": missing,
        "warnings": _status_warnings(cfg=cfg, provider=provider),
        "provider_help": _provider_help(cfg=cfg, provider=provider),
        "probe": None,
    }

    if probe and configured:
        payload["probe"] = _probe_provider(cfg=cfg, provider=provider)
        payload["ready"] = bool(payload["probe"].get("ok"))

    return payload


def run_research_ai_request(*, config: AppConfig, request: ResearchAIRequest) -> dict[str, Any]:
    cfg = config.research_ai
    provider = _normalize_provider(cfg.provider)
    provider_impl = get_research_ai_provider(provider)
    missing = _missing_configuration(cfg=cfg, provider=provider)
    if not cfg.enabled:
        raise ConfigurationError("research_ai is disabled", error_code="research_ai_disabled")
    if provider == "disabled" or provider_impl is None:
        raise ConfigurationError(
            f"unsupported research_ai provider: {provider}",
            error_code="research_ai_provider_unsupported",
        )
    if missing:
        raise ConfigurationError(
            f"research_ai is not fully configured: {', '.join(missing)}",
            error_code="research_ai_not_configured",
        )
    return provider_impl.run(cfg=cfg, request=request)


def _run_openai_compatible_request(*, cfg: ResearchAIConfig, request: ResearchAIRequest) -> dict[str, Any]:
    model = _resolve_role_model(cfg=cfg, role=request.role)
    if not model:
        raise ConfigurationError(
            f"no model configured for role {request.role}",
            error_code="research_ai_role_model_missing",
        )

    payload: dict[str, Any] = {
        "model": model,
        "messages": _build_messages(cfg=cfg, request=request),
        "temperature": cfg.temperature if request.temperature is None else request.temperature,
    }
    max_output_tokens = cfg.max_output_tokens if request.max_output_tokens is None else request.max_output_tokens
    if max_output_tokens is not None:
        payload["max_tokens"] = int(max_output_tokens)

    endpoint = _build_endpoint(cfg.base_url, "chat/completions")
    headers = _request_headers(cfg)
    raw_response = _request_json(
        method="POST",
        url=endpoint,
        headers=headers,
        timeout_seconds=cfg.timeout_seconds,
        max_retries=cfg.max_retries,
        json_payload=payload,
    )
    response = ResearchAIResponse(
        provider="openai_compatible",
        model=model,
        role=request.role,
        output_text=_extract_output_text(raw_response),
        usage=raw_response.get("usage") if isinstance(raw_response.get("usage"), dict) else {},
        raw_response=raw_response,
    )
    return response.model_dump()


def _probe_provider(*, cfg: ResearchAIConfig, provider: str) -> dict[str, Any]:
    provider_impl = get_research_ai_provider(provider)
    if provider_impl is not None:
        return provider_impl.probe(cfg=cfg)
    return {"ok": False, "error": f"unsupported research_ai provider: {provider}"}


def _request_json(
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    timeout_seconds: float,
    max_retries: int,
    json_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    attempts = max(0, int(max_retries)) + 1
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            with httpx.Client(timeout=timeout_seconds) as client:
                response = client.request(method=method, url=url, headers=headers, json=json_payload)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ExternalServiceError(
                        "research_ai response must be a JSON object",
                        error_code="research_ai_invalid_response",
                        retryable=False,
                    )
                return payload
        except ExternalServiceError as exc:
            last_error = exc
        except httpx.HTTPError as exc:
            last_error = ExternalServiceError(
                f"{type(exc).__name__}: {exc}",
                error_code="research_ai_http_error",
            )
        except Exception as exc:
            last_error = ExternalServiceError(
                f"{type(exc).__name__}: {exc}",
                error_code="research_ai_request_failed",
                retryable=False,
            )
    if last_error is None:
        raise ExternalServiceError(
            "research_ai request failed without an explicit exception",
            error_code="research_ai_request_failed",
        )
    raise last_error


def _build_messages(*, cfg: ResearchAIConfig, request: ResearchAIRequest) -> list[dict[str, str]]:
    system_prompt = (
        request.system_prompt
        or _resolve_system_prompt(cfg=cfg, role=request.role)
        or "You are a quantitative crypto research assistant."
    )
    user_content = request.task.strip()
    if request.context:
        user_content = (
            f"{user_content}\n\n"
            f"Structured context:\n{json.dumps(request.context, ensure_ascii=False, indent=2, default=str)}"
        )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]


def _resolve_role_model(*, cfg: ResearchAIConfig, role: str) -> str | None:
    role_key = str(role).strip()
    if cfg.role_models and role_key in cfg.role_models:
        return str(cfg.role_models[role_key]).strip()
    if cfg.model:
        return str(cfg.model).strip()
    return None


def _resolve_system_prompt(*, cfg: ResearchAIConfig, role: str) -> str | None:
    role_key = str(role).strip()
    if cfg.role_system_prompts and role_key in cfg.role_system_prompts:
        return str(cfg.role_system_prompts[role_key]).strip()
    if cfg.default_system_prompt:
        return str(cfg.default_system_prompt).strip()
    return DEFAULT_RESEARCH_AI_ROLE_PROMPTS.get(role_key)


def _effective_role_models(cfg: ResearchAIConfig) -> dict[str, str | None]:
    roles = sorted(set(DEFAULT_RESEARCH_AI_ROLE_PROMPTS) | set((cfg.role_models or {}).keys()))
    return {role: _resolve_role_model(cfg=cfg, role=role) for role in roles}


def _request_headers(cfg: ResearchAIConfig) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
    }
    for key, value in (cfg.extra_headers or {}).items():
        normalized_key = str(key).strip()
        normalized_value = str(value).strip()
        if normalized_key and normalized_value:
            headers[normalized_key] = normalized_value
    return headers


def _build_endpoint(base_url: str | None, path: str) -> str:
    normalized_base_url = str(base_url or "").strip().rstrip("/")
    normalized_path = str(path).strip().lstrip("/")
    if not normalized_base_url:
        raise ConfigurationError(
            "research_ai.base_url is not configured",
            error_code="research_ai_base_url_missing",
        )
    return f"{normalized_base_url}/{normalized_path}"


def _extract_output_text(raw_response: dict[str, Any]) -> str:
    choices = raw_response.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0]
    if not isinstance(first, dict):
        return ""
    message = first.get("message")
    if isinstance(message, dict):
        content = message.get("content")
        return _content_to_text(content)
    delta = first.get("delta")
    if isinstance(delta, dict):
        return _content_to_text(delta.get("content"))
    return ""


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        chunks: list[str] = []
        for item in content:
            if isinstance(item, str):
                text = item.strip()
                if text:
                    chunks.append(text)
                continue
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                chunks.append(text.strip())
                continue
            nested = item.get("content")
            if isinstance(nested, str) and nested.strip():
                chunks.append(nested.strip())
        return "\n".join(chunks).strip()
    return ""


def _missing_configuration(*, cfg: ResearchAIConfig, provider: str) -> list[str]:
    missing: list[str] = []
    if not cfg.enabled:
        return missing
    if provider == "disabled":
        missing.append("provider")
        return missing
    provider_impl = get_research_ai_provider(provider)
    if provider_impl is None:
        missing.append("provider")
        return missing
    return provider_impl.missing_configuration(cfg=cfg)


def _status_warnings(*, cfg: ResearchAIConfig, provider: str) -> list[str]:
    warnings: list[str] = []
    if not cfg.enabled:
        warnings.append("research_ai is disabled")
    provider_impl = get_research_ai_provider(provider)
    if provider not in {"disabled"} and provider_impl is None:
        warnings.append(f"unsupported provider configured: {provider}")
    if provider_impl is not None:
        warnings.extend(provider_impl.warnings(cfg=cfg))
    return warnings


def _normalize_provider(provider: str | None) -> str:
    normalized = str(provider or "").strip().lower()
    return normalized or "disabled"


def _provider_help(*, cfg: ResearchAIConfig, provider: str) -> dict[str, Any]:
    if provider == "openai_compatible":
        return {
            "required": ["base_url", "api_key", "model"],
            "example_env": {
                "RESEARCH_AI_ENABLED": "true",
                "RESEARCH_AI_PROVIDER": "openai_compatible",
                "RESEARCH_AI_BASE_URL": "https://api.openai.com/v1",
                "RESEARCH_AI_API_KEY": "set-in-env",
                "RESEARCH_AI_MODEL": "gpt-5-mini",
            },
            "example_context": {
                "candidate_id": 12,
                "symbols": ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
            },
            "notes": [
                "Custom providers can consume research_ai.provider_options.",
                "Role-specific model overrides are configured through research_ai.role_models.",
            ],
        }
    return {
        "required": ["provider"],
        "available": supported_research_ai_providers(),
    }
