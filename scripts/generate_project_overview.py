from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


MODULE_DESCRIPTIONS = {
    "alerts": "Alert delivery and notification adapters.",
    "application": "Shared application-level helpers used by CLI and services.",
    "backtest": "Signal replay, portfolio aggregation, metrics, sweep, and routed backtests.",
    "data": "OKX clients, public factors, and dataset helpers.",
    "execution": "Signal snapshot building, order planning, and strategy routing.",
    "providers": "Pluggable provider registry for market data sources.",
    "reporting": "HTML dashboard and report rendering.",
    "risk": "Single-symbol and portfolio risk controls.",
    "service": "FastAPI service, client UI, runtime monitor, research service flows.",
    "strategies": "Strategy signal frame generation.",
    "utils": "Timeframe, filesystem, and generic utility helpers.",
}

FLOW_ROWS = [
    {
        "flow": "A. Data Acquisition",
        "entry_points": "download, download-public-factors, sync-instrument",
        "core_files": "src/quant_lab/cli.py | src/quant_lab/providers/market_data.py | src/quant_lab/data/*",
        "upstream_truth": "config/settings.yaml + market_data provider config",
        "outputs": "data/raw/*.parquet | data/raw/*.json",
    },
    {
        "flow": "B. Backtest And Reports",
        "entry_points": "backtest, report, sweep, research-trend, research-routed-backtest",
        "core_files": "src/quant_lab/backtest/* | src/quant_lab/reporting/* | src/quant_lab/application/report_runtime.py",
        "upstream_truth": "raw data + strategy/execution/risk config",
        "outputs": "data/reports/*summary.json | *equity_curve.csv | *trades.csv | *.html",
    },
    {
        "flow": "C. Research Governance",
        "entry_points": "research-* CLI commands | /research/* API",
        "core_files": "src/quant_lab/service/research_ops.py | src/quant_lab/service/research_ai.py | src/quant_lab/service/research_agent.py",
        "upstream_truth": "SQLite approval/candidate/evaluation tables + candidate configs",
        "outputs": "research_tasks | strategy_candidates | evaluation_reports | approval_decisions",
    },
    {
        "flow": "D. Demo Execution",
        "entry_points": "demo-preflight, demo-plan, demo-reconcile, demo-execute, demo-loop, demo-portfolio-*",
        "core_files": "src/quant_lab/application/demo_support.py | src/quant_lab/service/demo_runtime.py | src/quant_lab/execution/* | src/quant_lab/risk/*",
        "upstream_truth": "live market snapshots + config gates + approval/router state",
        "outputs": "executor state json | service heartbeats | alerts | demo logs",
    },
    {
        "flow": "E. Service And Client",
        "entry_points": "service-api | FastAPI routes under /runtime, /client, /project, /research",
        "core_files": "src/quant_lab/service/monitor.py | src/quant_lab/service/dashboard.py | src/quant_lab/service/client_dashboard.py",
        "upstream_truth": "runtime snapshot DB rows + report artifacts + demo runtime payloads",
        "outputs": "local HTML dashboards + JSON APIs",
    },
]

STATE_SOURCE_ROWS = [
    {
        "source": "config/settings.yaml",
        "kind": "yaml",
        "role": "Primary runtime configuration for strategy, execution, risk, service, and integrations.",
    },
    {
        "source": ".env",
        "kind": "env",
        "role": "Runtime overrides layered on top of YAML.",
    },
    {
        "source": "OKX profile/config.toml",
        "kind": "credentials",
        "role": "Optional private credential and proxy injection.",
    },
    {
        "source": "data/raw/*",
        "kind": "files",
        "role": "Historical candles, funding, mark/index, order-book and public factor source data.",
    },
    {
        "source": "data/reports/*",
        "kind": "files",
        "role": "Backtest, sweep, research, and dashboard artifacts.",
    },
    {
        "source": "data/demo_executor_state*.json",
        "kind": "files",
        "role": "Demo executor dedupe state and recent submit/error memory.",
    },
    {
        "source": "data/quant_lab.db",
        "kind": "sqlite",
        "role": "Runtime snapshots, heartbeats, alerts, project tasks, and research governance state.",
    },
]

GATE_DESCRIPTIONS = {
    "okx.use_demo": "Whether private OKX actions target the OKX demo environment.",
    "trading.allow_order_placement": "Master switch for any order placement path.",
    "trading.require_approved_candidate": "Requires a bound/approved strategy candidate before execution.",
    "trading.strategy_router_enabled": "Enables regime-based candidate routing instead of a single static config.",
    "trading.strategy_router_fallback_to_config": "Allows planning fallback to base config when router misses; should not imply submit readiness.",
    "research_ai.enabled": "Enables research AI provider integration.",
    "research_agent.enabled": "Enables research agent integration, including TradingAgents.",
    "alerts.email_enabled": "Enables SMTP alert delivery.",
    "alerts.telegram_enabled": "Enables Telegram alert delivery.",
}


@dataclass
class ProjectSnapshot:
    generated_at: str
    repo_root: Path
    config_path: Path
    config: dict[str, Any]
    cli_commands: list[dict[str, Any]]
    api_endpoints: list[dict[str, Any]]
    module_rows: list[dict[str, Any]]
    repo_stats: list[dict[str, Any]]
    raw_file_rows: list[dict[str, Any]]
    report_file_rows: list[dict[str, Any]]
    raw_summary_rows: list[dict[str, Any]]
    report_summary_rows: list[dict[str, Any]]
    db_table_rows: list[dict[str, Any]]
    docs_rows: list[dict[str, Any]]
    config_rows: list[dict[str, Any]]
    gate_rows: list[dict[str, Any]]
    overview_rows: list[dict[str, Any]]
    default_artifact_rows: list[dict[str, Any]]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate an Excel and HTML overview of the quant-lab project.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output-prefix", type=str, default=None)
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    generated_at = datetime.now(timezone.utc).isoformat()
    output_prefix = args.output_prefix or f"quant_lab_project_overview_{datetime.now().date().isoformat()}"
    report_dir = repo_root / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    output_xlsx = report_dir / f"{output_prefix}.xlsx"
    output_html = report_dir / f"{output_prefix}.html"

    snapshot = build_snapshot(repo_root=repo_root, generated_at=generated_at)
    write_workbook(snapshot=snapshot, output_path=output_xlsx)
    write_html(snapshot=snapshot, output_path=output_html)
    print(json.dumps({"xlsx": str(output_xlsx), "html": str(output_html)}, ensure_ascii=False, indent=2))


def build_snapshot(*, repo_root: Path, generated_at: str) -> ProjectSnapshot:
    config_path = choose_config_path(repo_root)
    config = load_yaml(config_path)
    cli_commands = extract_cli_commands(repo_root / "src" / "quant_lab" / "cli.py")
    api_endpoints = extract_api_endpoints(repo_root / "src" / "quant_lab" / "service" / "monitor.py")
    module_rows = build_module_rows(repo_root / "src" / "quant_lab")
    repo_stats = build_repo_stats(repo_root)
    raw_file_rows, raw_summary_rows = build_file_inventory(repo_root / "data" / "raw", kind="raw")
    report_file_rows, report_summary_rows = build_file_inventory(repo_root / "data" / "reports", kind="reports")
    db_table_rows = build_db_rows(repo_root / "data" / "quant_lab.db")
    docs_rows = build_docs_rows(repo_root / "docs")
    config_rows = flatten_config(config)
    gate_rows = build_gate_rows(config)
    overview_rows = build_overview_rows(
        repo_root=repo_root,
        generated_at=generated_at,
        config_path=config_path,
        config=config,
        cli_commands=cli_commands,
        api_endpoints=api_endpoints,
        repo_stats=repo_stats,
        raw_file_rows=raw_file_rows,
        report_file_rows=report_file_rows,
        db_table_rows=db_table_rows,
    )
    default_artifact_rows = build_default_artifact_rows(repo_root=repo_root, config=config)
    return ProjectSnapshot(
        generated_at=generated_at,
        repo_root=repo_root,
        config_path=config_path,
        config=config,
        cli_commands=cli_commands,
        api_endpoints=api_endpoints,
        module_rows=module_rows,
        repo_stats=repo_stats,
        raw_file_rows=raw_file_rows,
        report_file_rows=report_file_rows,
        raw_summary_rows=raw_summary_rows,
        report_summary_rows=report_summary_rows,
        db_table_rows=db_table_rows,
        docs_rows=docs_rows,
        config_rows=config_rows,
        gate_rows=gate_rows,
        overview_rows=overview_rows,
        default_artifact_rows=default_artifact_rows,
    )


def choose_config_path(repo_root: Path) -> Path:
    settings = repo_root / "config" / "settings.yaml"
    if settings.exists():
        return settings
    return repo_root / "config" / "settings.example.yaml"


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def extract_cli_commands(path: Path) -> list[dict[str, Any]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    commands: list[dict[str, Any]] = []
    pattern = re.compile(r'@app\.command(?:\("([^"]+)"\))?')
    def_pattern = re.compile(r"def\s+([a-zA-Z_][a-zA-Z0-9_]*)\(")
    for index, line in enumerate(lines):
        match = pattern.search(line)
        if not match:
            continue
        explicit = match.group(1)
        func_name = None
        for probe in range(index + 1, min(index + 8, len(lines))):
            def_match = def_pattern.search(lines[probe])
            if def_match:
                func_name = def_match.group(1)
                break
        if func_name is None:
            continue
        command_name = explicit or func_name.replace("_", "-")
        commands.append(
            {
                "command": command_name,
                "function": func_name,
                "group": command_group(command_name),
                "line": index + 1,
            }
        )
    return sorted(commands, key=lambda item: (item["group"], item["command"]))


def extract_api_endpoints(path: Path) -> list[dict[str, Any]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    endpoints: list[dict[str, Any]] = []
    route_pattern = re.compile(r'@app\.(get|post|put|delete)\("([^"]+)"')
    def_pattern = re.compile(r"def\s+([a-zA-Z_][a-zA-Z0-9_]*)\(")
    for index, line in enumerate(lines):
        match = route_pattern.search(line)
        if not match:
            continue
        method, route = match.groups()
        func_name = None
        for probe in range(index + 1, min(index + 8, len(lines))):
            def_match = def_pattern.search(lines[probe])
            if def_match:
                func_name = def_match.group(1)
                break
        endpoints.append(
            {
                "method": method.upper(),
                "path": route,
                "function": func_name or "",
                "group": endpoint_group(route),
                "line": index + 1,
            }
        )
    return sorted(endpoints, key=lambda item: (item["group"], item["path"], item["method"]))


def build_module_rows(src_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for directory in sorted(path for path in src_root.iterdir() if path.is_dir() and path.name != "__pycache__"):
        py_files = list(directory.rglob("*.py"))
        rows.append(
            {
                "module": directory.name,
                "python_files": len(py_files),
                "description": MODULE_DESCRIPTIONS.get(directory.name, "Project module."),
                "path": str(directory.relative_to(src_root.parent.parent)),
            }
        )
    return rows


def build_repo_stats(repo_root: Path) -> list[dict[str, Any]]:
    src_py = list((repo_root / "src").rglob("*.py"))
    test_py = list((repo_root / "tests").rglob("*.py"))
    docs_files = list((repo_root / "docs").rglob("*")) if (repo_root / "docs").exists() else []
    report_files = list((repo_root / "data" / "reports").rglob("*")) if (repo_root / "data" / "reports").exists() else []
    raw_files = list((repo_root / "data" / "raw").rglob("*")) if (repo_root / "data" / "raw").exists() else []
    test_functions = 0
    test_pattern = re.compile(r"^\s*def\s+test_[a-zA-Z0-9_]+\(", re.MULTILINE)
    for file_path in test_py:
        test_functions += len(test_pattern.findall(file_path.read_text(encoding="utf-8", errors="replace")))
    return [
        {"metric": "source_python_files", "value": len(src_py)},
        {"metric": "test_python_files", "value": len(test_py)},
        {"metric": "test_functions", "value": test_functions},
        {"metric": "docs_files", "value": len([item for item in docs_files if item.is_file()])},
        {"metric": "report_files", "value": len([item for item in report_files if item.is_file()])},
        {"metric": "raw_data_files", "value": len([item for item in raw_files if item.is_file()])},
    ]


def build_file_inventory(root: Path, *, kind: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not root.exists():
        return [], []
    rows: list[dict[str, Any]] = []
    summary_counter: Counter[str] = Counter()
    summary_bytes: Counter[str] = Counter()
    for file_path in sorted(path for path in root.rglob("*") if path.is_file()):
        relative = file_path.relative_to(root.parent)
        category = classify_file(file_path.name, kind=kind)
        symbol = infer_symbol(file_path.name)
        size_bytes = file_path.stat().st_size
        rows.append(
            {
                "relative_path": str(relative),
                "name": file_path.name,
                "symbol_scope": symbol,
                "category": category,
                "size_mb": round(size_bytes / (1024 * 1024), 3),
            }
        )
        summary_counter[category] += 1
        summary_bytes[category] += size_bytes
    summary_rows = [
        {
            "category": category,
            "file_count": summary_counter[category],
            "total_size_mb": round(summary_bytes[category] / (1024 * 1024), 3),
        }
        for category in sorted(summary_counter)
    ]
    return rows, summary_rows


def build_db_rows(db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with sqlite3.connect(db_path) as connection:
        table_names = [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
        ]
        for table_name in table_names:
            row_count = connection.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            rows.append({"table": table_name, "row_count": row_count})
    return rows


def build_docs_rows(docs_dir: Path) -> list[dict[str, Any]]:
    if not docs_dir.exists():
        return []
    return [
        {"document": file_path.name, "relative_path": str(file_path.relative_to(docs_dir.parent))}
        for file_path in sorted(path for path in docs_dir.iterdir() if path.is_file())
    ]


def flatten_config(config: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def walk(prefix: str, value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                next_prefix = f"{prefix}.{key}" if prefix else str(key)
                walk(next_prefix, child)
            return
        rows.append({"key": prefix, "value": format_cell(value)})

    walk("", config)
    return rows


def build_gate_rows(config: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, description in GATE_DESCRIPTIONS.items():
        rows.append(
            {
                "gate": key,
                "current_value": format_cell(nested_get(config, key)),
                "meaning": description,
            }
        )
    return rows


def build_overview_rows(
    *,
    repo_root: Path,
    generated_at: str,
    config_path: Path,
    config: dict[str, Any],
    cli_commands: list[dict[str, Any]],
    api_endpoints: list[dict[str, Any]],
    repo_stats: list[dict[str, Any]],
    raw_file_rows: list[dict[str, Any]],
    report_file_rows: list[dict[str, Any]],
    db_table_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    symbols = configured_symbols(config)
    strategy_name = nested_get(config, "strategy.name")
    strategy_variant = nested_get(config, "strategy.variant")
    mode = "portfolio" if len(symbols) > 1 else "single"
    repo_stat_map = {item["metric"]: item["value"] for item in repo_stats}
    return [
        {"item": "generated_at_utc", "value": generated_at},
        {"item": "repo_root", "value": str(repo_root)},
        {"item": "active_config", "value": str(config_path)},
        {"item": "runtime_mode", "value": mode},
        {"item": "symbols", "value": ", ".join(symbols)},
        {"item": "strategy", "value": f"{strategy_name} | {strategy_variant}"},
        {"item": "okx_use_demo", "value": format_cell(nested_get(config, "okx.use_demo"))},
        {"item": "allow_order_placement", "value": format_cell(nested_get(config, "trading.allow_order_placement"))},
        {
            "item": "require_approved_candidate",
            "value": format_cell(nested_get(config, "trading.require_approved_candidate")),
        },
        {
            "item": "strategy_router_enabled",
            "value": format_cell(nested_get(config, "trading.strategy_router_enabled")),
        },
        {
            "item": "research_agent",
            "value": f"{nested_get(config, 'research_agent.provider')} | enabled={nested_get(config, 'research_agent.enabled')}",
        },
        {
            "item": "research_ai",
            "value": f"{nested_get(config, 'research_ai.provider')} | enabled={nested_get(config, 'research_ai.enabled')}",
        },
        {"item": "cli_commands", "value": len(cli_commands)},
        {"item": "service_api_endpoints", "value": len(api_endpoints)},
        {"item": "source_python_files", "value": repo_stat_map.get("source_python_files", 0)},
        {"item": "test_functions", "value": repo_stat_map.get("test_functions", 0)},
        {"item": "raw_data_files", "value": len(raw_file_rows)},
        {"item": "report_files", "value": len(report_file_rows)},
        {"item": "db_tables", "value": len(db_table_rows)},
    ]


def build_default_artifact_rows(*, repo_root: Path, config: dict[str, Any]) -> list[dict[str, Any]]:
    report_dir = repo_root / "data" / "reports"
    prefix = default_report_prefix(config)
    artifacts = [
        ("summary", report_dir / f"{prefix}_summary.json"),
        ("equity_curve", report_dir / f"{prefix}_equity_curve.csv"),
        ("trades", report_dir / f"{prefix}_trades.csv"),
        ("dashboard", report_dir / f"{prefix}_dashboard.html"),
        ("allocation_overlay", report_dir / f"{prefix}_allocation_overlay.csv"),
    ]
    rows: list[dict[str, Any]] = []
    for artifact_type, path in artifacts:
        rows.append(
            {
                "artifact_type": artifact_type,
                "path": str(path.relative_to(repo_root)),
                "exists": path.exists(),
            }
        )
    summary_path = report_dir / f"{prefix}_summary.json"
    if summary_path.exists():
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {}
        for field in ("final_equity", "total_return_pct", "max_drawdown_pct", "trade_count"):
            rows.append(
                {
                    "artifact_type": f"summary_metric:{field}",
                    "path": str(summary_path.relative_to(repo_root)),
                    "exists": payload.get(field),
                }
            )
    return rows


def default_report_prefix(config: dict[str, Any]) -> str:
    symbols = configured_symbols(config)
    strategy_name = str(nested_get(config, "strategy.name") or "strategy")
    if len(symbols) == 1:
        return f"{symbols[0]}_{strategy_name}"
    bases = "_".join(symbol.split("-")[0].lower() for symbol in symbols)
    return f"portfolio_{bases}_{strategy_name}"


def configured_symbols(config: dict[str, Any]) -> list[str]:
    portfolio_symbols = nested_get(config, "portfolio.symbols")
    if isinstance(portfolio_symbols, list) and portfolio_symbols:
        return [str(item) for item in portfolio_symbols]
    symbol = nested_get(config, "instrument.symbol")
    return [str(symbol)] if symbol else []


def command_group(command_name: str) -> str:
    if command_name.startswith("research-"):
        return "research"
    if command_name.startswith("demo-"):
        return "demo"
    if command_name.startswith("service-"):
        return "service"
    if command_name in {"download", "download-public-factors", "sync-instrument"}:
        return "data"
    if command_name in {"backtest", "report", "sweep", "research-trend", "research-routed-backtest"}:
        return "backtest_reporting"
    if command_name in {
        "market-data-status",
        "integration-status",
        "research-ai-status",
        "research-ai-run",
        "research-agent-status",
        "research-agent-run",
    }:
        return "integrations"
    if command_name == "alert-test":
        return "alerts"
    return "misc"


def endpoint_group(route: str) -> str:
    if route.startswith("/runtime"):
        return "runtime"
    if route.startswith("/client"):
        return "client"
    if route.startswith("/research"):
        return "research"
    if route.startswith("/project"):
        return "project"
    if route.startswith("/reports"):
        return "reports"
    if route.startswith("/artifacts"):
        return "artifacts"
    if route.startswith("/market-data") or route.startswith("/integrations"):
        return "integrations"
    return "system"


def classify_file(name: str, *, kind: str) -> str:
    if kind == "raw":
        if name.endswith(".parquet"):
            stem = name[: -len(".parquet")]
            suffix = stem.split("_", 1)[1] if "_" in stem else stem
            return suffix
        return Path(name).suffix.lstrip(".") or "other"
    if name.endswith("_dashboard.html"):
        return "dashboard_html"
    if name.endswith("_summary.json"):
        return "summary_json"
    if name.endswith("_equity_curve.csv"):
        return "equity_curve_csv"
    if name.endswith("_trades.csv"):
        return "trades_csv"
    if name.endswith("_allocation_overlay.csv"):
        return "allocation_overlay_csv"
    if name.endswith("_sweep.csv"):
        return "sweep_csv"
    if name.endswith("_sweep_dashboard.html"):
        return "sweep_dashboard_html"
    suffix = Path(name).suffix.lower()
    if suffix == ".xlsx":
        return "xlsx"
    if suffix == ".md":
        return "markdown"
    if suffix == ".html":
        return "html"
    if suffix == ".csv":
        return "csv"
    if suffix == ".json":
        return "json"
    return suffix.lstrip(".") or "other"


def infer_symbol(name: str) -> str:
    if name.startswith("portfolio_"):
        return "portfolio"
    match = re.match(r"([A-Z0-9]+-USDT-SWAP)", name)
    if match:
        return match.group(1)
    return "n/a"


def nested_get(payload: dict[str, Any], dotted_key: str) -> Any:
    current: Any = payload
    for part in dotted_key.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def format_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def write_workbook(*, snapshot: ProjectSnapshot, output_path: Path) -> None:
    sheets: list[tuple[str, pd.DataFrame]] = [
        ("Overview", pd.DataFrame(snapshot.overview_rows)),
        ("DefaultArtifacts", pd.DataFrame(snapshot.default_artifact_rows)),
        ("Flows", pd.DataFrame(FLOW_ROWS)),
        ("Modules", pd.DataFrame(snapshot.module_rows)),
        ("CLICommands", pd.DataFrame(snapshot.cli_commands)),
        ("ServiceAPI", pd.DataFrame(snapshot.api_endpoints)),
        ("StateSources", pd.DataFrame(STATE_SOURCE_ROWS)),
        ("Gates", pd.DataFrame(snapshot.gate_rows)),
        ("RepoStats", pd.DataFrame(snapshot.repo_stats)),
        ("Config", pd.DataFrame(snapshot.config_rows)),
        ("RawSummary", pd.DataFrame(snapshot.raw_summary_rows)),
        ("ReportSummary", pd.DataFrame(snapshot.report_summary_rows)),
        ("DBTables", pd.DataFrame(snapshot.db_table_rows)),
        ("Docs", pd.DataFrame(snapshot.docs_rows)),
        ("RawFiles", pd.DataFrame(snapshot.raw_file_rows)),
        ("ReportFiles", pd.DataFrame(snapshot.report_file_rows)),
    ]
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, frame in sheets:
            frame.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        workbook = writer.book
        for worksheet in workbook.worksheets:
            worksheet.freeze_panes = "A2" if worksheet.max_row > 1 else "A1"
            for column_cells in worksheet.columns:
                max_length = 0
                column_letter = column_cells[0].column_letter
                for cell in column_cells:
                    text = "" if cell.value is None else str(cell.value)
                    max_length = max(max_length, len(text))
                worksheet.column_dimensions[column_letter].width = min(max(max_length + 2, 12), 60)


def write_html(*, snapshot: ProjectSnapshot, output_path: Path) -> None:
    overview = {row["item"]: row["value"] for row in snapshot.overview_rows}
    cli_groups = summarize_group_counts(snapshot.cli_commands, key="group")
    api_groups = summarize_group_counts(snapshot.api_endpoints, key="group")
    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>quant-lab project overview</title>
  <style>
    :root {{
      --bg: #f4f1ea; --paper: #fffdf8; --ink: #17212b; --muted: #6d7a88;
      --line: #d9d0c1; --accent: #0b7285; --accent-2: #c97b2a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; font-family: "Segoe UI", "PingFang SC", "Noto Sans SC", sans-serif; color: var(--ink);
      background: radial-gradient(circle at top left, rgba(11,114,133,.08), transparent 28%), linear-gradient(180deg, #f8f4ec 0%, var(--bg) 100%);
    }}
    .shell {{ max-width: 1480px; margin: 0 auto; padding: 28px; }}
    .hero, .grid-2 {{ display: grid; grid-template-columns: 1.4fr .8fr; gap: 18px; margin-bottom: 18px; }}
    .grid-2 {{ grid-template-columns: 1fr 1fr; }}
    .panel, .card {{
      background: var(--paper); border: 1px solid var(--line); border-radius: 20px;
      padding: 20px 22px; box-shadow: 0 12px 40px rgba(23,33,43,.06);
    }}
    .hero h1 {{ margin: 0 0 8px; font-size: 34px; letter-spacing: -.02em; }}
    .hero p, .meta, .flow span, .foot {{ color: var(--muted); line-height: 1.6; }}
    .meta {{ font-size: 13px; word-break: break-all; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 14px; margin-bottom: 18px; }}
    .card small {{ display: block; margin-bottom: 8px; color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }}
    .card strong {{ font-size: 24px; display: block; line-height: 1.2; }}
    .section-title {{ margin: 0 0 12px; font-size: 18px; }}
    .flow {{ border-left: 4px solid var(--accent); padding-left: 14px; margin-bottom: 16px; }}
    .flow strong {{ display: block; margin-bottom: 6px; font-size: 16px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ text-align: left; vertical-align: top; padding: 9px 10px; border-bottom: 1px solid #eee5d8; word-break: break-word; }}
    th {{ color: var(--muted); font-weight: 600; background: rgba(201,123,42,.06); }}
    .pill-row {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px; }}
    .pill {{ padding: 8px 12px; border-radius: 999px; background: rgba(11,114,133,.08); color: var(--accent); font-size: 12px; }}
    .group-list {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; }}
    .group-item {{ border: 1px solid var(--line); border-radius: 14px; padding: 12px 14px; background: rgba(255,255,255,.65); }}
    .group-item strong {{ display: block; font-size: 18px; }}
    @media (max-width: 980px) {{ .hero, .grid-2 {{ grid-template-columns: 1fr; }} .shell {{ padding: 18px; }} }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="panel">
        <h1>quant-lab current project map</h1>
        <p>This file combines the active config, source structure, CLI/API surface, shared state sources, and current data/report/database footprints into one readable view.</p>
        <div class="pill-row">
          <div class="pill">mode: {overview.get("runtime_mode", "--")}</div>
          <div class="pill">symbols: {overview.get("symbols", "--")}</div>
          <div class="pill">strategy: {overview.get("strategy", "--")}</div>
          <div class="pill">research agent: {overview.get("research_agent", "--")}</div>
        </div>
      </div>
      <div class="panel meta">
        <div><strong>generated_at_utc</strong>: {snapshot.generated_at}</div>
        <div><strong>repo_root</strong>: {snapshot.repo_root}</div>
        <div><strong>active_config</strong>: {snapshot.config_path}</div>
        <div><strong>default_report_prefix</strong>: {default_report_prefix(snapshot.config)}</div>
      </div>
    </section>
    <section class="cards">
      <div class="card"><small>CLI Commands</small><strong>{overview.get("cli_commands", 0)}</strong></div>
      <div class="card"><small>Service APIs</small><strong>{overview.get("service_api_endpoints", 0)}</strong></div>
      <div class="card"><small>Source Python</small><strong>{overview.get("source_python_files", 0)}</strong></div>
      <div class="card"><small>Test Functions</small><strong>{overview.get("test_functions", 0)}</strong></div>
      <div class="card"><small>Raw Files</small><strong>{overview.get("raw_data_files", 0)}</strong></div>
      <div class="card"><small>Report Files</small><strong>{overview.get("report_files", 0)}</strong></div>
      <div class="card"><small>DB Tables</small><strong>{overview.get("db_tables", 0)}</strong></div>
      <div class="card"><small>OKX Demo</small><strong>{overview.get("okx_use_demo", "--")}</strong></div>
    </section>
    <section class="grid-2">
      <div class="panel"><h2 class="section-title">System Flows</h2>{"".join(render_flow_cards())}</div>
      <div class="panel"><h2 class="section-title">Current Gates</h2>{dataframe_html(pd.DataFrame(snapshot.gate_rows))}</div>
    </section>
    <section class="grid-2">
      <div class="panel"><h2 class="section-title">Module Map</h2>{dataframe_html(pd.DataFrame(snapshot.module_rows))}</div>
      <div class="panel"><h2 class="section-title">Default Artifact Presence</h2>{dataframe_html(pd.DataFrame(snapshot.default_artifact_rows))}</div>
    </section>
    <section class="grid-2">
      <div class="panel"><h2 class="section-title">CLI Groups</h2>{render_group_items(cli_groups)}</div>
      <div class="panel"><h2 class="section-title">API Groups</h2>{render_group_items(api_groups)}</div>
    </section>
    <section class="grid-2">
      <div class="panel"><h2 class="section-title">Repo Stats</h2>{dataframe_html(pd.DataFrame(snapshot.repo_stats))}</div>
      <div class="panel"><h2 class="section-title">State Sources</h2>{dataframe_html(pd.DataFrame(STATE_SOURCE_ROWS))}</div>
    </section>
    <section class="grid-2">
      <div class="panel"><h2 class="section-title">Raw Data Summary</h2>{dataframe_html(pd.DataFrame(snapshot.raw_summary_rows))}</div>
      <div class="panel"><h2 class="section-title">Report Artifact Summary</h2>{dataframe_html(pd.DataFrame(snapshot.report_summary_rows))}</div>
    </section>
    <section class="grid-2">
      <div class="panel"><h2 class="section-title">Service API Surface</h2>{dataframe_html(pd.DataFrame(snapshot.api_endpoints))}</div>
      <div class="panel"><h2 class="section-title">CLI Surface</h2>{dataframe_html(pd.DataFrame(snapshot.cli_commands))}</div>
    </section>
    <section class="grid-2">
      <div class="panel"><h2 class="section-title">Database Tables</h2>{dataframe_html(pd.DataFrame(snapshot.db_table_rows))}</div>
      <div class="panel"><h2 class="section-title">Docs</h2>{dataframe_html(pd.DataFrame(snapshot.docs_rows))}</div>
    </section>
    <div class="foot">Excel workbook companion: same prefix in data/reports. Use the workbook for raw tables and this HTML for the visual map.</div>
  </div>
</body>
</html>
"""
    output_path.write_text(html, encoding="utf-8")


def summarize_group_counts(rows: list[dict[str, Any]], *, key: str) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for row in rows:
        counter[str(row.get(key) or "other")] += 1
    return sorted(counter.items(), key=lambda item: item[0])


def render_group_items(groups: list[tuple[str, int]]) -> str:
    items = [
        f'<div class="group-item"><small>{name}</small><strong>{count}</strong></div>'
        for name, count in groups
    ]
    return f'<div class="group-list">{"".join(items)}</div>'


def render_flow_cards() -> list[str]:
    cards: list[str] = []
    for row in FLOW_ROWS:
        cards.append(
            "<div class=\"flow\">"
            f"<strong>{row['flow']}</strong>"
            f"<span><b>Entry:</b> {row['entry_points']}</span>"
            f"<span><b>Core files:</b> {row['core_files']}</span>"
            f"<span><b>Truth source:</b> {row['upstream_truth']}</span>"
            f"<span><b>Outputs:</b> {row['outputs']}</span>"
            "</div>"
        )
    return cards


def dataframe_html(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "<div>No data.</div>"
    return frame.to_html(index=False, border=0, classes="dataframe", justify="left")


if __name__ == "__main__":
    main()
