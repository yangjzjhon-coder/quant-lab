# Research Governance Workflow

## Purpose

This layer turns strategy research into a controlled workflow instead of a pile of ad hoc reports.

The intended flow is:

1. Create a research task
2. Register one or more strategy candidates under that task
3. Evaluate each candidate against backtest artifacts
4. Record an approval decision
5. Treat only approved candidates as promotion-ready for demo or live

## Role Model

Supported research roles:

- `research_lead`
- `factor_analyst`
- `strategy_builder`
- `backtest_validator`
- `risk_officer`

These roles are organizational labels. They do not place orders.

## Database Entities

- `research_tasks`
  - The queue of research hypotheses and work items
- `strategy_candidates`
  - Candidate strategies or strategy revisions under evaluation
- `evaluation_reports`
  - Structured evaluation results tied to a candidate
- `approval_decisions`
  - Final decisions such as `approve`, `reject`, or `watchlist`

## Candidate Status Flow

- `draft`
- `evaluation_passed`
- `evaluation_review`
- `evaluation_failed`
- `approved`
- `rejected`
- `watchlist`

## CLI Commands

Create a task:

```bash
quant-lab research-create-task \
  --config config/settings.yaml \
  --title "BTC regime research" \
  --hypothesis "Trend plus regime filter should reduce drawdown"
```

List tasks:

```bash
quant-lab research-list-tasks --config config/settings.yaml
```

Register a candidate:

```bash
quant-lab research-register-candidate \
  --config config/settings.yaml \
  --name btc_regime_v1 \
  --strategy-name ema_trend_4h \
  --variant high_weight_long \
  --timeframe 4H \
  --author-role strategy_builder
```

List candidates:

```bash
quant-lab research-list-candidates --config config/settings.yaml
```

Evaluate a candidate with the latest artifacts inferred from the current config:

```bash
quant-lab research-evaluate-candidate \
  --config config/settings.yaml \
  --candidate-id 1
```

Approve a candidate for demo:

```bash
quant-lab research-approve-candidate \
  --config config/settings.yaml \
  --candidate-id 1 \
  --decision approve \
  --scope demo \
  --reason "Promote to demo observation"
```

Bind the approved candidate into the execution config:

```bash
quant-lab research-bind-candidate \
  --config config/settings.yaml \
  --candidate-id 1
```

Enable regime routing and bind a candidate to a route key:

```bash
quant-lab research-set-route \
  --config config/settings.yaml \
  --route-key bull_trend \
  --candidate-id 1
```

Examples of route keys:

- `bull_trend`
- `bear_trend`
- `range`
- `BTC-USDT-SWAP:bull_trend`
- `ETH-USDT-SWAP:range`
- `default`

Show the whole research overview:

```bash
quant-lab research-overview --config config/settings.yaml
```

Run a historical routed backtest against the approved candidate pool:

```bash
quant-lab research-routed-backtest \
  --config config/settings.yaml \
  --project-root . \
  --required-scope demo
```

This command:

- loads the configured `execution_candidate_map`
- validates each routed candidate is approved and config-compatible
- classifies each historical bar as `bull_trend`, `bear_trend`, or `range`
- switches signal generation by route key across history
- writes normal backtest artifacts plus routing artifacts:
  - `*_summary.json`
  - `*_equity_curve.csv`
  - `*_trades.csv`
  - `*_dashboard.html`
  - `*_routes.csv`
  - `*_routing_summary.json`

## Service API

Available endpoints:

- `GET /research/overview`
- `GET /research/tasks`
- `POST /research/tasks`
- `GET /research/candidates`
- `POST /research/candidates`
- `POST /research/candidates/{candidate_id}/evaluate`
- `POST /research/candidates/{candidate_id}/approve`

## Evaluation Notes

The current evaluation helper is deliberately simple:

- Reads `summary.json`
- Extracts return, drawdown, trade count, profit factor, and Sharpe
- Produces a `0-100` composite score
- Maps the candidate to:
  - `evaluation_passed`
  - `evaluation_review`
  - `evaluation_failed`

This is a governance scaffold, not the final institutional ranking model.

## Execution Gate

Execution is gated to the approved candidate pool so demo and live promotion can only select strategies with:

- latest decision = `approve`
- explicit scope = `demo` or `live`
- latest evaluation status not stale

This is now partially in place for demo execution:

- `trading.require_approved_candidate`
- `trading.execution_candidate_id`
- `trading.execution_candidate_name`
- `trading.strategy_router_enabled`
- `trading.strategy_router_fallback_to_config`
- `trading.execution_candidate_map`

When the gate is enabled, demo submit paths will refuse to place orders unless:

- the bound candidate exists
- candidate status is `approved`
- candidate scope is compatible with `demo`
- candidate strategy name, variant, timeframe, and symbol scope match the current config

When regime routing is enabled:

- the system classifies each symbol as `bull_trend`, `bear_trend`, or `range`
- the router searches `execution_candidate_map`
- the selected candidate must still be `approved`
- if no matching route exists, the system can fall back to the base config for planning only
- actual submit will still be blocked for that symbol until the router resolves an executable approved candidate

## Current Gap

The governance and routed-backtest layers are now in place.

The next material step is not more routing code. It is research discipline:

- create and register multiple candidate configs for `bull_trend`, `bear_trend`, and `range`
- run routed historical comparisons against the same BTC/ETH datasets
- promote only candidates that survive both standalone and routed evaluation
