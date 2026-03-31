# TradingAgents Research Provider

## Goal

Integrate `TradingAgents` into `quant-lab` as a research-layer provider, while keeping `quant-lab` as the system of record.

This means:

- `quant-lab` still owns research tasks, candidates, approvals, execution gates, and reports
- `TradingAgents` is used as an external research engine
- integration happens through a subprocess adapter, not an in-process dependency

## Why This Shape

This boundary keeps the system stable:

- heavy `TradingAgents` dependencies stay outside the main `quant-lab` runtime
- failures in the agent framework do not directly corrupt demo execution or service runtime
- new agent frameworks can reuse the same provider pattern later

## Minimal Config

Add a `research_agent` block to your runtime config:

```yaml
research_agent:
  enabled: true
  provider: "tradingagents"
  timeout_seconds: 120.0
  max_retries: 1
  local_repo_path: "/abs/path/to/TradingAgents"
  python_executable: "/abs/path/to/python"
  provider_options:
    debug: true
    selected_analysts:
      - "market"
      - "news"
    symbol_map:
      BTC-USDT-SWAP: "BTC-USD"
      ETH-USDT-SWAP: "ETH-USD"
    config_overrides:
      llm_provider: "openai"
      deep_think_llm: "gpt-5.2"
      quick_think_llm: "gpt-5-mini"
```

## Environment Override Option

You can also inject the runtime from environment variables:

```bash
export RESEARCH_AGENT_ENABLED=true
export RESEARCH_AGENT_PROVIDER=tradingagents
export RESEARCH_AGENT_LOCAL_REPO_PATH=/abs/path/to/TradingAgents
export RESEARCH_AGENT_PYTHON_EXECUTABLE=/abs/path/to/python
export RESEARCH_AGENT_PROVIDER_OPTIONS_JSON='{"debug": true, "selected_analysts": ["market", "news"]}'
```

## Dedicated Python Environment

`TradingAgents` has a much heavier dependency set than `quant-lab`.

Do not assume the `quant-lab` runtime interpreter can import it. The intended shape is:

- `quant-lab` keeps its own environment
- `TradingAgents` runs in its own dedicated virtualenv
- `research_agent.python_executable` points to that dedicated interpreter

Bootstrap helper:

```bash
python tools/bootstrap_tradingagents_env.py \
  /abs/path/to/TradingAgents \
  /abs/path/to/.venvs/tradingagents
```

Then set:

- `research_agent.local_repo_path` to the TradingAgents repo
- `research_agent.python_executable` to that venv's Python

## Probe The Integration

Check whether `quant-lab` can reach the local repo and runner:

```bash
quant-lab research-agent-status \
  --config config/settings.yaml \
  --project-root . \
  --probe
```

Important fields in the response:

- `provider`
- `supported_providers`
- `configured`
- `ready`
- `local_repo_path`
- `python_executable`
- `provider_help`
- `probe`

For `tradingagents`, `probe` also reports:

- `repo_markers`
- `imports.default_config`
- `imports.trading_graph`
- `missing_modules`
- `install_hint`

## Run A Research Workflow

Example:

```bash
quant-lab research-agent-run \
  --config config/settings.yaml \
  --project-root . \
  --task "Review BTC and ETH breakout conditions with external debate" \
  --symbols BTC-USDT-SWAP,ETH-USDT-SWAP \
  --context-json "{\"trade_date\": \"2026-03-29\"}"
```

What happens next:

1. `quant-lab` normalizes symbols and request context
2. the `tradingagents` provider maps symbols to agent-facing names such as `BTC-USD`
3. the runner launches one subprocess workflow per symbol
4. results are normalized back into `quant-lab`
5. `research_tasks` and `strategy_candidates` can be created from the result

## Current Mapping Rules

Default symbol mapping:

- `BTC-USDT-SWAP -> BTC-USD`
- `ETH-USDT-SWAP -> ETH-USD`

Fallback rule:

- take the first asset code and map it to `<ASSET>-USD`

Override this through `provider_options.symbol_map` when needed.

## What This Does Not Do

This provider does not:

- place orders
- bypass candidate approval
- replace the existing research governance flow
- make `TradingAgents` the owner of runtime truth

It only feeds structured research output back into the existing governance pipeline.
