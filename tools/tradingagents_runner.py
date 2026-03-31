from __future__ import annotations

import copy
import importlib
import json
import sys
from pathlib import Path
from typing import Any

SAFE_FINAL_STATE_KEYS = (
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


def main() -> int:
    action = "run"
    try:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict):
            raise ValueError("runner payload must be a JSON object")

        action = str(payload.get("action") or "run").strip().lower()
        repo_path = _resolve_repo_path(payload.get("repo_path"))
        sys.path.insert(0, str(repo_path))

        if action == "probe":
            _emit(_probe_runtime(repo_path))
            return 0

        if action != "run":
            raise ValueError(f"unsupported action: {action}")

        from tradingagents.default_config import DEFAULT_CONFIG
        from tradingagents.graph.trading_graph import TradingAgentsGraph

        company_name = str(payload.get("company_name") or "").strip()
        trade_date = str(payload.get("trade_date") or "").strip()
        if not company_name:
            raise ValueError("company_name is required")
        if not trade_date:
            raise ValueError("trade_date is required")

        selected_analysts = _clean_text_list(payload.get("selected_analysts"))
        effective_selected_analysts = selected_analysts or ["market", "social", "news", "fundamentals"]
        config_overrides = payload.get("config_overrides") if isinstance(payload.get("config_overrides"), dict) else {}
        config = _build_runtime_config(default_config=DEFAULT_CONFIG, repo_path=repo_path, overrides=config_overrides)

        graph = TradingAgentsGraph(
            selected_analysts=effective_selected_analysts,
            debug=bool(payload.get("debug")),
            config=config,
        )
        final_state, decision = graph.propagate(company_name, trade_date)
        final_state = final_state if isinstance(final_state, dict) else {}

        _emit(
            {
                "ok": True,
                "provider": "tradingagents",
                "company_name": company_name,
                "trade_date": trade_date,
                "decision": str(decision or "").strip(),
                "selected_analysts": effective_selected_analysts,
                "final_state": {key: final_state.get(key) for key in SAFE_FINAL_STATE_KEYS if key in final_state},
            }
        )
        return 0
    except Exception as exc:
        _emit({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
        return 0 if action == "probe" else 1


def _probe_runtime(repo_path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "provider": "tradingagents",
        "repo_path": str(repo_path),
        "python_executable": sys.executable,
        "python_version": sys.version.split()[0],
        "repo_markers": {
            "pyproject_toml": (repo_path / "pyproject.toml").exists(),
            "default_config": (repo_path / "tradingagents" / "default_config.py").exists(),
            "trading_graph": (repo_path / "tradingagents" / "graph" / "trading_graph.py").exists(),
        },
        "imports": {
            "default_config": {"ok": False},
            "trading_graph": {"ok": False},
        },
        "missing_modules": [],
        "warnings": [],
        "default_config_keys": [],
        "install_hint": (
            "Create a dedicated TradingAgents venv and point research_agent.python_executable to it."
        ),
    }

    payload["imports"]["default_config"] = _import_diagnostic("tradingagents.default_config")
    if payload["imports"]["default_config"]["ok"]:
        try:
            module = importlib.import_module("tradingagents.default_config")
            default_config = getattr(module, "DEFAULT_CONFIG", None)
            if isinstance(default_config, dict):
                payload["default_config_keys"] = sorted(default_config.keys())
        except Exception as exc:
            payload["warnings"].append(f"DEFAULT_CONFIG read failed: {type(exc).__name__}: {exc}")

    payload["imports"]["trading_graph"] = _import_diagnostic("tradingagents.graph.trading_graph")

    missing_modules = [
        item.get("missing_module")
        for item in payload["imports"].values()
        if isinstance(item, dict) and str(item.get("missing_module") or "").strip()
    ]
    payload["missing_modules"] = sorted({str(name) for name in missing_modules if str(name).strip()})
    payload["ok"] = bool(payload["imports"]["trading_graph"]["ok"])
    return payload


def _import_diagnostic(module_name: str) -> dict[str, Any]:
    try:
        importlib.import_module(module_name)
        return {"ok": True}
    except ModuleNotFoundError as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "missing_module": str(exc.name or "").strip() or None,
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _resolve_repo_path(raw_value: Any) -> Path:
    raw_path = str(raw_value or "").strip()
    if not raw_path:
        raise ValueError("repo_path is required")
    repo_path = Path(raw_path).expanduser()
    return repo_path.resolve()


def _build_runtime_config(*, default_config: dict[str, Any], repo_path: Path, overrides: dict[str, Any]) -> dict[str, Any]:
    config = _merge_dicts(copy.deepcopy(default_config), overrides)
    config["project_dir"] = str(repo_path)
    config["results_dir"] = _resolve_repo_relative_dir(
        repo_path=repo_path,
        raw_value=config.get("results_dir"),
        default_relative="results",
    )
    config["data_cache_dir"] = _resolve_repo_relative_dir(
        repo_path=repo_path,
        raw_value=config.get("data_cache_dir"),
        default_relative="dataflows/data_cache",
    )
    return config


def _resolve_repo_relative_dir(*, repo_path: Path, raw_value: Any, default_relative: str) -> str:
    candidate = Path(str(raw_value or default_relative).strip() or default_relative)
    if not candidate.is_absolute():
        candidate = repo_path / candidate
    return str(candidate.resolve())


def _merge_dicts(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


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


def _emit(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=True))


if __name__ == "__main__":
    raise SystemExit(main())
