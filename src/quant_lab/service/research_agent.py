from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import httpx
from pydantic import BaseModel, Field

from quant_lab.config import AppConfig, ResearchAgentConfig, configured_symbols
from quant_lab.errors import ConfigurationError, ExternalServiceError, InvalidRequestError
from quant_lab.logging_utils import get_logger
from quant_lab.service.research_ops import (
    create_research_task,
    register_strategy_candidate,
    serialize_research_task,
    serialize_strategy_candidate,
)

SUPPORTED_RESEARCH_AGENT_PROVIDERS = {"disabled", "http_json", "tradingagents"}
LOGGER = get_logger(__name__)


class ResearchAgentRequest(BaseModel):
    role: str = "research_lead"
    task: str
    title: str | None = None
    hypothesis: str = ""
    symbols: list[str] = Field(default_factory=list)
    context: dict[str, Any] = Field(default_factory=dict)
    task_id: int | None = None
    create_task: bool = True
    register_candidate: bool = True
    owner_role: str = "research_lead"
    author_role: str = "strategy_builder"
    priority: str = "high"
    notes: str = ""
    candidate_name: str | None = None
    strategy_name: str | None = None
    variant: str | None = None
    timeframe: str | None = None
    thesis: str | None = None
    tags: list[str] = Field(default_factory=list)


class ResearchAgentProvider(Protocol):
    provider_name: str

    def capabilities(self, *, cfg: ResearchAgentConfig, configured: bool) -> list[str]: ...

    def missing_configuration(self, *, cfg: ResearchAgentConfig) -> list[str]: ...

    def warnings(self, *, cfg: ResearchAgentConfig) -> list[str]: ...

    def probe(self, *, cfg: ResearchAgentConfig) -> dict[str, Any]: ...

    def run(self, *, cfg: ResearchAgentConfig, request: ResearchAgentRequest) -> dict[str, Any]: ...


ResearchAgentProviderFactory = Callable[[], ResearchAgentProvider]


class _HttpJsonResearchAgentProvider:
    provider_name = "http_json"

    def capabilities(self, *, cfg: ResearchAgentConfig, configured: bool) -> list[str]:
        return ["structured_research_workflow"] if configured else []

    def missing_configuration(self, *, cfg: ResearchAgentConfig) -> list[str]:
        missing: list[str] = []
        if not str(cfg.base_url or "").strip():
            missing.append("base_url")
        return missing

    def warnings(self, *, cfg: ResearchAgentConfig) -> list[str]:
        warnings: list[str] = []
        if not str(cfg.base_url or "").strip():
            warnings.append("http_json provider requires research_agent.base_url")
        return warnings

    def probe(self, *, cfg: ResearchAgentConfig) -> dict[str, Any]:
        endpoint = _build_endpoint(cfg.base_url, cfg.probe_path)
        try:
            payload = _request_json(
                method="GET",
                url=endpoint,
                headers=_request_headers(cfg),
                timeout_seconds=min(cfg.timeout_seconds, 15.0),
                max_retries=0,
            )
            return {
                "ok": True,
                "endpoint": endpoint,
                "payload": payload,
            }
        except Exception as exc:
            return {
                "ok": False,
                "endpoint": endpoint,
                "error": f"{type(exc).__name__}: {exc}",
            }

    def run(self, *, cfg: ResearchAgentConfig, request: ResearchAgentRequest) -> dict[str, Any]:
        endpoint = _build_endpoint(cfg.base_url, cfg.workflow_path)
        payload = _request_json(
            method="POST",
            url=endpoint,
            headers=_request_headers(cfg),
            timeout_seconds=cfg.timeout_seconds,
            max_retries=cfg.max_retries,
            json_payload={
                "role": request.role,
                "task": request.task,
                "title": request.title,
                "hypothesis": request.hypothesis,
                "symbols": request.symbols,
                "context": request.context,
            },
        )
        return _normalize_provider_payload(provider="http_json", request=request, payload=payload)


class _TradingAgentsResearchAgentProvider:
    provider_name = "tradingagents"

    def capabilities(self, *, cfg: ResearchAgentConfig, configured: bool) -> list[str]:
        return ["structured_research_workflow", "local_repo_subprocess"] if configured else []

    def missing_configuration(self, *, cfg: ResearchAgentConfig) -> list[str]:
        missing: list[str] = []
        repo_path = cfg.local_repo_path
        if repo_path is None or not str(repo_path).strip():
            missing.append("local_repo_path")
        elif not Path(repo_path).expanduser().exists():
            missing.append("local_repo_path")
        return missing

    def warnings(self, *, cfg: ResearchAgentConfig) -> list[str]:
        warnings: list[str] = []
        repo_path = cfg.local_repo_path
        if repo_path is None or not str(repo_path).strip():
            warnings.append("tradingagents provider requires research_agent.local_repo_path")
        elif not Path(repo_path).expanduser().exists():
            warnings.append(f"tradingagents local_repo_path does not exist: {repo_path}")
        if not str(cfg.python_executable or "").strip():
            warnings.append("tradingagents provider will use 'python' from PATH")
        return warnings

    def probe(self, *, cfg: ResearchAgentConfig) -> dict[str, Any]:
        repo_path = _resolve_tradingagents_repo_path(cfg)
        try:
            payload = _run_tradingagents_subprocess(
                cfg=cfg,
                runner_payload={"action": "probe"},
                timeout_seconds=max(30.0, cfg.timeout_seconds),
                max_retries=0,
            )
            payload["repo_path"] = str(repo_path)
            payload["python_executable"] = _tradingagents_python_executable(cfg)
            return payload
        except Exception as exc:
            return {
                "ok": False,
                "repo_path": str(repo_path),
                "python_executable": _tradingagents_python_executable(cfg),
                "error": f"{type(exc).__name__}: {exc}",
            }

    def run(self, *, cfg: ResearchAgentConfig, request: ResearchAgentRequest) -> dict[str, Any]:
        return _run_tradingagents_request(cfg=cfg, request=request)


_RESEARCH_AGENT_PROVIDER_REGISTRY: dict[str, ResearchAgentProviderFactory] = {
    "http_json": _HttpJsonResearchAgentProvider,
    "tradingagents": _TradingAgentsResearchAgentProvider,
}


def register_research_agent_provider(name: str, factory: ResearchAgentProviderFactory) -> None:
    normalized = _normalize_provider(name)
    if normalized in {"", "disabled"}:
        raise InvalidRequestError(
            "research_agent provider name must be non-empty and cannot be 'disabled'",
            error_code="invalid_research_agent_provider_name",
        )
    _RESEARCH_AGENT_PROVIDER_REGISTRY[normalized] = factory


def get_research_agent_provider(provider: str | None) -> ResearchAgentProvider | None:
    normalized = _normalize_provider(provider)
    if normalized == "disabled":
        return None
    factory = _RESEARCH_AGENT_PROVIDER_REGISTRY.get(normalized)
    return None if factory is None else factory()


def supported_research_agent_providers() -> list[str]:
    return sorted({"disabled", *list(_RESEARCH_AGENT_PROVIDER_REGISTRY)})


def build_research_agent_status(*, config: AppConfig, probe: bool = False) -> dict[str, Any]:
    cfg = config.research_agent
    provider = _normalize_provider(cfg.provider)
    provider_impl = get_research_agent_provider(provider)
    missing = _missing_configuration(cfg=cfg, provider=provider)
    supported = provider == "disabled" or provider_impl is not None
    configured = bool(cfg.enabled and supported and not missing)

    payload: dict[str, Any] = {
        "enabled": bool(cfg.enabled),
        "provider": provider,
        "supported": supported,
        "configured": configured,
        "ready": configured,
        "supported_providers": supported_research_agent_providers(),
        "base_url": cfg.base_url,
        "timeout_seconds": cfg.timeout_seconds,
        "max_retries": cfg.max_retries,
        "workflow_path": cfg.workflow_path,
        "probe_path": cfg.probe_path,
        "api_key_configured": bool(cfg.api_key),
        "local_repo_path": str(cfg.local_repo_path) if cfg.local_repo_path is not None else None,
        "python_executable": cfg.python_executable or "python",
        "provider_options_keys": sorted((cfg.provider_options or {}).keys()),
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


def run_research_agent_request(*, config: AppConfig, request: ResearchAgentRequest) -> dict[str, Any]:
    cfg = config.research_agent
    provider = _normalize_provider(cfg.provider)
    provider_impl = get_research_agent_provider(provider)
    missing = _missing_configuration(cfg=cfg, provider=provider)
    if not cfg.enabled:
        raise ConfigurationError("research_agent is disabled", error_code="research_agent_disabled")
    if provider == "disabled" or provider_impl is None:
        raise ConfigurationError(
            f"unsupported research_agent provider: {provider}",
            error_code="research_agent_provider_unsupported",
        )
    if missing:
        raise ConfigurationError(
            f"research_agent is not fully configured: {', '.join(missing)}",
            error_code="research_agent_not_configured",
        )
    LOGGER.info(
        "research agent request provider=%s role=%s symbols=%s task=%s",
        provider,
        request.role,
        ",".join(request.symbols),
        request.task,
    )
    return provider_impl.run(cfg=cfg, request=_normalize_request(config=config, request=request))


def run_research_agent_workflow(*, config: AppConfig, session_factory, request: ResearchAgentRequest) -> dict[str, Any]:
    normalized_request = _normalize_request(config=config, request=request)
    LOGGER.info(
        "research agent workflow start provider=%s role=%s symbols=%s create_task=%s register_candidate=%s",
        config.research_agent.provider,
        normalized_request.role,
        ",".join(normalized_request.symbols),
        normalized_request.create_task,
        normalized_request.register_candidate,
    )
    agent_payload = run_research_agent_request(config=config, request=normalized_request)
    task_payload = agent_payload.get("task")
    candidate_payload = agent_payload.get("candidate")
    created_task = None
    created_candidate = None

    effective_task_id = normalized_request.task_id
    if normalized_request.create_task and effective_task_id is None:
        task_title = str((task_payload or {}).get("title") or normalized_request.title or normalized_request.task).strip()
        hypothesis = str((task_payload or {}).get("hypothesis") or normalized_request.hypothesis or "").strip()
        notes_parts = [
            str(normalized_request.notes or "").strip(),
            str((task_payload or {}).get("notes") or "").strip(),
            str(agent_payload.get("summary") or "").strip(),
        ]
        created_task = create_research_task(
            session_factory=session_factory,
            title=task_title,
            hypothesis=hypothesis,
            owner_role=normalized_request.owner_role,
            priority=normalized_request.priority,
            symbols=normalized_request.symbols,
            notes="\n\n".join(part for part in notes_parts if part),
        )
        effective_task_id = created_task.id
        LOGGER.info("research agent workflow created task task_id=%s title=%s", created_task.id, task_title)

    if normalized_request.register_candidate:
        candidate_details = dict((candidate_payload or {}).get("details") or {})
        candidate_details["research_agent"] = {
            "provider": agent_payload.get("provider"),
            "role": agent_payload.get("role"),
            "summary": agent_payload.get("summary"),
            "context": normalized_request.context,
            "raw_response": agent_payload.get("raw_response"),
        }
        created_candidate = register_strategy_candidate(
            session_factory=session_factory,
            candidate_name=str(
                (candidate_payload or {}).get("candidate_name")
                or normalized_request.candidate_name
                or _default_candidate_name(normalized_request)
            ).strip(),
            strategy_name=str(
                (candidate_payload or {}).get("strategy_name")
                or normalized_request.strategy_name
                or config.strategy.name
            ).strip(),
            variant=str(
                (candidate_payload or {}).get("variant")
                or normalized_request.variant
                or config.strategy.variant
            ).strip(),
            timeframe=str(
                (candidate_payload or {}).get("timeframe")
                or normalized_request.timeframe
                or config.strategy.signal_bar
            ).strip(),
            symbol_scope=_clean_text_list(
                (candidate_payload or {}).get("symbol_scope") or normalized_request.symbols
            ),
            config_path=None,
            author_role=normalized_request.author_role,
            thesis=str(
                (candidate_payload or {}).get("thesis")
                or normalized_request.thesis
                or agent_payload.get("summary")
                or normalized_request.task
            ).strip(),
            tags=_clean_text_list((candidate_payload or {}).get("tags") or normalized_request.tags),
            task_id=effective_task_id,
            details=candidate_details,
        )
        LOGGER.info(
            "research agent workflow created candidate candidate_id=%s name=%s",
            created_candidate.id,
            created_candidate.candidate_name,
        )

    LOGGER.info(
        "research agent workflow completed provider=%s task_id=%s candidate_created=%s",
        agent_payload.get("provider"),
        effective_task_id,
        created_candidate is not None,
    )
    return {
        "agent_result": agent_payload,
        "task": serialize_research_task(created_task) if created_task is not None else None,
        "candidate": serialize_strategy_candidate(created_candidate) if created_candidate is not None else None,
        "task_id": effective_task_id,
    }


def _probe_provider(*, cfg: ResearchAgentConfig, provider: str) -> dict[str, Any]:
    provider_impl = get_research_agent_provider(provider)
    if provider_impl is not None:
        return provider_impl.probe(cfg=cfg)
    return {"ok": False, "error": f"unsupported research_agent provider: {provider}"}


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
                        "research_agent response must be a JSON object",
                        error_code="research_agent_invalid_response",
                        retryable=False,
                    )
                return payload
        except ExternalServiceError as exc:
            last_error = exc
        except httpx.HTTPError as exc:
            last_error = ExternalServiceError(
                f"{type(exc).__name__}: {exc}",
                error_code="research_agent_http_error",
            )
        except Exception as exc:
            last_error = ExternalServiceError(
                f"{type(exc).__name__}: {exc}",
                error_code="research_agent_request_failed",
                retryable=False,
            )
    if last_error is None:
        raise ExternalServiceError(
            "research_agent request failed without an explicit exception",
            error_code="research_agent_request_failed",
        )
    raise last_error


def _normalize_provider_payload(*, provider: str, request: ResearchAgentRequest, payload: dict[str, Any]) -> dict[str, Any]:
    task_payload = payload.get("task")
    task_payload = task_payload if isinstance(task_payload, dict) else {}
    candidate_payload = payload.get("candidate")
    candidate_payload = candidate_payload if isinstance(candidate_payload, dict) else {}
    return {
        "provider": provider,
        "workflow": "external_research_agent",
        "role": request.role,
        "summary": str(payload.get("summary") or payload.get("output_text") or "").strip(),
        "task": {
            "title": str(task_payload.get("title") or request.title or request.task).strip(),
            "hypothesis": str(task_payload.get("hypothesis") or request.hypothesis or "").strip(),
            "notes": str(task_payload.get("notes") or "").strip(),
        },
        "candidate": {
            "candidate_name": candidate_payload.get("candidate_name"),
            "strategy_name": candidate_payload.get("strategy_name"),
            "variant": candidate_payload.get("variant"),
            "timeframe": candidate_payload.get("timeframe"),
            "symbol_scope": _clean_text_list(candidate_payload.get("symbol_scope") or request.symbols),
            "thesis": str(candidate_payload.get("thesis") or "").strip(),
            "tags": _clean_text_list(candidate_payload.get("tags") or []),
            "details": candidate_payload.get("details") if isinstance(candidate_payload.get("details"), dict) else {},
        },
        "raw_response": payload,
    }


def _normalize_request(*, config: AppConfig, request: ResearchAgentRequest) -> ResearchAgentRequest:
    payload = request.model_dump()
    payload["symbols"] = _clean_text_list(payload.get("symbols") or configured_symbols(config))
    return ResearchAgentRequest.model_validate(payload)


def _request_headers(cfg: ResearchAgentConfig) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if cfg.api_key:
        headers["Authorization"] = f"Bearer {cfg.api_key}"
    headers.update({str(key): str(value) for key, value in (cfg.extra_headers or {}).items()})
    return headers


def _build_endpoint(base_url: str | None, path: str) -> str:
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        raise ConfigurationError(
            "research_agent.base_url is required",
            error_code="research_agent_base_url_missing",
        )
    suffix = "/" + str(path or "").strip().lstrip("/")
    return f"{base}{suffix}"


def _missing_configuration(*, cfg: ResearchAgentConfig, provider: str) -> list[str]:
    missing: list[str] = []
    if not cfg.enabled:
        return missing
    if provider == "disabled":
        missing.append("provider")
        return missing
    provider_impl = get_research_agent_provider(provider)
    if provider_impl is None:
        missing.append("provider")
        return missing
    return provider_impl.missing_configuration(cfg=cfg)


def _status_warnings(*, cfg: ResearchAgentConfig, provider: str) -> list[str]:
    warnings: list[str] = []
    if not cfg.enabled:
        warnings.append("research_agent is disabled")
        return warnings
    provider_impl = get_research_agent_provider(provider)
    if provider not in {"disabled"} and provider_impl is None:
        warnings.append(f"unsupported provider configured: {provider}")
    if provider_impl is not None:
        warnings.extend(provider_impl.warnings(cfg=cfg))
    return warnings


def _normalize_provider(provider: str | None) -> str:
    normalized = str(provider or "").strip().lower()
    return normalized or "disabled"


def _clean_text_list(raw_values: Any) -> list[str]:
    if isinstance(raw_values, str):
        raw_values = [raw_values]
    if not isinstance(raw_values, list):
        return []
    values: list[str] = []
    for item in raw_values:
        cleaned = str(item or "").strip()
        if cleaned and cleaned not in values:
            values.append(cleaned)
    return values


def _default_candidate_name(request: ResearchAgentRequest) -> str:
    first_symbol = request.symbols[0].split("-")[0].lower() if request.symbols else "research"
    role_slug = request.role.replace("_", "-").strip("-") or "agent"
    return f"{first_symbol}_{role_slug}_draft"


def _provider_help(*, cfg: ResearchAgentConfig, provider: str) -> dict[str, Any]:
    if provider == "http_json":
        return {
            "required": ["base_url"],
            "example_env": {
                "RESEARCH_AGENT_ENABLED": "true",
                "RESEARCH_AGENT_PROVIDER": "http_json",
                "RESEARCH_AGENT_BASE_URL": "https://agents.example.com",
                "RESEARCH_AGENT_API_KEY": "set-in-env",
            },
            "example_context": {
                "trade_date": "2026-03-29",
                "research_scope": "breakout_regime_review",
            },
        }
    if provider == "tradingagents":
        runtime_options = dict(cfg.provider_options or {})
        symbol_examples = [
            {
                "quant_lab_symbol": symbol,
                "provider_symbol": _map_tradingagents_symbol(symbol=symbol, runtime_options=runtime_options),
            }
            for symbol in ("BTC-USDT-SWAP", "ETH-USDT-SWAP")
        ]
        return {
            "required": ["local_repo_path"],
            "runner_path": str(_tradingagents_runner_path()),
            "bootstrap_script": str(Path(__file__).resolve().parents[3] / "tools" / "bootstrap_tradingagents_env.py"),
            "symbol_examples": symbol_examples,
            "trade_date_resolution": "context.trade_date -> provider_options.trade_date -> utc_today",
            "example_env": {
                "RESEARCH_AGENT_ENABLED": "true",
                "RESEARCH_AGENT_PROVIDER": "tradingagents",
                "RESEARCH_AGENT_LOCAL_REPO_PATH": "/abs/path/to/TradingAgents",
                "RESEARCH_AGENT_PYTHON_EXECUTABLE": "/abs/path/to/python",
            },
            "example_provider_options": {
                "debug": True,
                "selected_analysts": ["market", "news"],
                "symbol_map": {
                    "BTC-USDT-SWAP": "BTC-USD",
                    "ETH-USDT-SWAP": "ETH-USD",
                },
                "config_overrides": {
                    "llm_provider": "openai",
                    "deep_think_llm": "gpt-5.2",
                    "quick_think_llm": "gpt-5-mini",
                },
                "environment": {
                    "OPENAI_API_KEY": "set-in-shell-or-env",
                },
            },
            "example_context": {
                "trade_date": "2026-03-29",
                "provider_options": {
                    "selected_analysts": ["market", "news"],
                },
            },
        }
    return {
        "required": ["provider"],
        "available": supported_research_agent_providers(),
    }


def _run_tradingagents_request(*, cfg: ResearchAgentConfig, request: ResearchAgentRequest) -> dict[str, Any]:
    runtime_options = _merge_dicts(dict(cfg.provider_options or {}), _context_provider_options(request.context))
    selected_analysts = _clean_text_list(
        runtime_options.get("selected_analysts") or ["market", "social", "news", "fundamentals"]
    )
    trade_date = _resolve_trade_date(request=request, runtime_options=runtime_options)
    symbol_results: list[dict[str, Any]] = []

    for symbol in request.symbols:
        company_name = _map_tradingagents_symbol(symbol=symbol, runtime_options=runtime_options)
        result = _run_tradingagents_subprocess(
            cfg=cfg,
            runner_payload={
                "action": "run",
                "symbol": symbol,
                "company_name": company_name,
                "trade_date": trade_date,
                "selected_analysts": selected_analysts,
                "debug": bool(runtime_options.get("debug")),
                "config_overrides": runtime_options.get("config_overrides")
                if isinstance(runtime_options.get("config_overrides"), dict)
                else {},
                "environment": runtime_options.get("environment")
                if isinstance(runtime_options.get("environment"), dict)
                else {},
            },
            timeout_seconds=cfg.timeout_seconds,
            max_retries=cfg.max_retries,
        )
        symbol_results.append(
            {
                "symbol": symbol,
                "company_name": str(result.get("company_name") or company_name).strip(),
                "trade_date": str(result.get("trade_date") or trade_date).strip(),
                "decision": str(result.get("decision") or "").strip(),
                "selected_analysts": _clean_text_list(result.get("selected_analysts") or selected_analysts),
                "final_state": _safe_tradingagents_final_state(result.get("final_state")),
            }
        )

    summary_parts = [
        f"{item['symbol']}={item['decision'] or 'UNKNOWN'} ({item['company_name']}, {item['trade_date']})"
        for item in symbol_results
    ]
    summary = "TradingAgents decisions: " + "; ".join(summary_parts) if summary_parts else "TradingAgents returned no results."
    task_notes = f"TradingAgents subprocess workflow analyzed {len(symbol_results)} symbol(s)."
    candidate_tags = _clean_text_list(
        ["agent", "tradingagents"] + [item["decision"].lower() for item in symbol_results if item.get("decision")]
    )

    return {
        "provider": "tradingagents",
        "workflow": "tradingagents_subprocess",
        "role": request.role,
        "summary": summary,
        "task": {
            "title": str(request.title or request.task).strip(),
            "hypothesis": str(request.hypothesis or "").strip(),
            "notes": task_notes,
        },
        "candidate": {
            "candidate_name": request.candidate_name,
            "strategy_name": request.strategy_name,
            "variant": request.variant,
            "timeframe": request.timeframe,
            "symbol_scope": request.symbols,
            "thesis": str(request.thesis or summary).strip(),
            "tags": candidate_tags,
            "details": {
                "trade_date": trade_date,
                "selected_analysts": selected_analysts,
                "results": symbol_results,
            },
        },
        "raw_response": {
            "trade_date": trade_date,
            "selected_analysts": selected_analysts,
            "results": symbol_results,
        },
    }


def _run_tradingagents_subprocess(
    *,
    cfg: ResearchAgentConfig,
    runner_payload: dict[str, Any],
    timeout_seconds: float,
    max_retries: int,
) -> dict[str, Any]:
    repo_path = _resolve_tradingagents_repo_path(cfg)
    command = [
        _tradingagents_python_executable(cfg),
        str(_tradingagents_runner_path()),
    ]
    payload = dict(runner_payload)
    payload["repo_path"] = str(repo_path)
    env = os.environ.copy()
    env.update(_string_dict(payload.get("environment")))

    attempts = max(0, int(max_retries)) + 1
    last_error: Exception | None = None
    action = str(runner_payload.get("action") or "run").strip().lower()
    for attempt in range(1, attempts + 1):
        try:
            LOGGER.info(
                "tradingagents subprocess start action=%s attempt=%s/%s repo=%s python=%s",
                action,
                attempt,
                attempts,
                repo_path,
                command[0],
            )
            result = subprocess.run(
                command,
                input=json.dumps(payload),
                capture_output=True,
                text=True,
                cwd=str(repo_path),
                env=env,
                timeout=timeout_seconds,
                check=False,
            )
            if result.returncode != 0:
                raise ExternalServiceError(
                    _subprocess_error_message(result),
                    error_code="tradingagents_subprocess_failed",
                )
            response_payload = _parse_subprocess_json(result.stdout)
            if bool(response_payload.get("ok")) is False:
                message = str(response_payload.get("error") or "tradingagents runner returned ok=false").strip()
                raise ExternalServiceError(
                    message,
                    error_code="tradingagents_subprocess_failed",
                    retryable=False,
                )
            LOGGER.info(
                "tradingagents subprocess completed action=%s attempt=%s/%s ok=%s",
                action,
                attempt,
                attempts,
                response_payload.get("ok"),
            )
            return response_payload
        except ExternalServiceError as exc:
            last_error = exc
            LOGGER.warning(
                "tradingagents subprocess failed action=%s attempt=%s/%s error=%s:%s",
                action,
                attempt,
                attempts,
                type(exc).__name__,
                exc,
            )
        except Exception as exc:
            last_error = ExternalServiceError(
                f"{type(exc).__name__}: {exc}",
                error_code="tradingagents_subprocess_failed",
                retryable=False,
            )
            LOGGER.warning(
                "tradingagents subprocess failed action=%s attempt=%s/%s error=%s:%s",
                action,
                attempt,
                attempts,
                type(last_error).__name__,
                last_error,
            )
    if last_error is None:
        raise ExternalServiceError(
            "tradingagents runner failed without an explicit exception",
            error_code="tradingagents_subprocess_failed",
        )
    raise last_error


def _resolve_tradingagents_repo_path(cfg: ResearchAgentConfig) -> Path:
    if cfg.local_repo_path is None or not str(cfg.local_repo_path).strip():
        raise ConfigurationError(
            "research_agent.local_repo_path is required for tradingagents provider",
            error_code="research_agent_repo_path_missing",
        )
    repo_path = Path(cfg.local_repo_path).expanduser().resolve()
    if not repo_path.exists():
        raise ConfigurationError(
            f"research_agent.local_repo_path does not exist: {repo_path}",
            error_code="research_agent_repo_path_missing",
        )
    return repo_path


def _tradingagents_python_executable(cfg: ResearchAgentConfig) -> str:
    return str(cfg.python_executable or "python").strip() or "python"


def _tradingagents_runner_path() -> Path:
    return Path(__file__).resolve().parents[3] / "tools" / "tradingagents_runner.py"


def _context_provider_options(context: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(context, dict):
        return {}
    payload = context.get("provider_options")
    return dict(payload) if isinstance(payload, dict) else {}


def _resolve_trade_date(*, request: ResearchAgentRequest, runtime_options: dict[str, Any]) -> str:
    raw_value = request.context.get("trade_date") if isinstance(request.context, dict) else None
    if raw_value in {None, ""}:
        raw_value = runtime_options.get("trade_date")
    cleaned = str(raw_value or "").strip()
    if cleaned:
        return cleaned[:10]
    return datetime.now(timezone.utc).date().isoformat()


def _map_tradingagents_symbol(*, symbol: str, runtime_options: dict[str, Any]) -> str:
    symbol_map = runtime_options.get("symbol_map")
    if isinstance(symbol_map, dict):
        mapped = symbol_map.get(symbol)
        if mapped is not None and str(mapped).strip():
            return str(mapped).strip()
    parts = [part.strip().upper() for part in str(symbol).split("-") if part.strip()]
    if len(parts) >= 2 and parts[1] in {"USDT", "USDC", "USD"}:
        return f"{parts[0]}-USD"
    return parts[0] if parts else str(symbol).strip()


def _safe_tradingagents_final_state(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    keys = (
        "company_of_interest",
        "trade_date",
        "market_report",
        "sentiment_report",
        "news_report",
        "fundamentals_report",
        "trader_investment_plan",
        "investment_plan",
        "final_trade_decision",
    )
    return {key: payload.get(key) for key in keys if key in payload}


def _merge_dicts(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def _string_dict(raw_payload: Any) -> dict[str, str]:
    if not isinstance(raw_payload, dict):
        return {}
    payload: dict[str, str] = {}
    for key, value in raw_payload.items():
        normalized_key = str(key or "").strip()
        if normalized_key:
            payload[normalized_key] = str(value)
    return payload


def _parse_subprocess_json(raw_text: str) -> dict[str, Any]:
    cleaned = str(raw_text or "").strip()
    if not cleaned:
        raise ExternalServiceError(
            "tradingagents runner returned empty stdout",
            error_code="tradingagents_invalid_response",
            retryable=False,
        )
    payload = json.loads(cleaned)
    if not isinstance(payload, dict):
        raise ExternalServiceError(
            "tradingagents runner must return a JSON object",
            error_code="tradingagents_invalid_response",
            retryable=False,
        )
    return payload


def _subprocess_error_message(result: subprocess.CompletedProcess[str]) -> str:
    try:
        payload = _parse_subprocess_json(result.stdout)
        if str(payload.get("error") or "").strip():
            return str(payload["error"]).strip()
    except Exception:
        pass
    stderr = str(result.stderr or "").strip()
    stdout = str(result.stdout or "").strip()
    if stderr:
        return stderr
    if stdout:
        return stdout
    return f"tradingagents runner exited with code {result.returncode}"
