# Quant Lab

Backtesting and paper-trading scaffolding for a trend-following system on OKX perpetual futures.

Quant Lab is a local research workbench: it pulls public market data, runs a
realistic backtest with fees, slippage, execution delay and funding costs, scores
the result, and then carries the same strategy through a guarded execution layer
against an OKX demo account. It is built to answer one question honestly — *does
this strategy survive contact with costs?* — before any capital is at risk.

```
data ──▶ backtest ──▶ metrics + HTML report
             │
             └──▶ execution planner ──▶ OKX demo account
                        (dry-run by default)
```

## Highlights

- **Realistic fills.** Signals are taken on `4h` closes; entries are filled one
  minute later at the `1m` open, with a configurable fixed slippage. Taker fees,
  funding rate and execution latency are all charged against the curve.
- **Risk-first design.** Per-trade risk is capped (`<= 2%` by default) and the
  system stops opening new positions when weekly drawdown reaches `6%`.
- **Parameter sweeps.** Grid search over fast/slow EMA and ATR multipliers,
  rendered as a sortable HTML dashboard rather than a wall of numbers.
- **Multi-symbol portfolios.** BTC and ETH can be backtested as separate sleeves
  and aggregated into one equal-weight equity curve.
- **Guarded execution.** Order submission requires four independent conditions to
  be true at once, so a misconfigured environment cannot place a live-looking order.
- **Observability.** Run snapshots, heartbeats and alerts land in SQLite or
  PostgreSQL and are served by a small FastAPI dashboard.

## Verified results

A full backtest over `2023-01-01` → `2026-03-01` on `BTC-USDT-SWAP`:

| Metric | Default `EMA 20/50` | Sweep best `EMA 10/50`, `ATR 2.5` |
| --- | --- | --- |
| Total return | 493.49% | 778.71% |
| Max drawdown | 19.84% | 16.04% |
| Sharpe | 1.83 | 2.38 |
| Trades | 98 | — |
| Final equity (start 10,000) | 59,348.7 | — |

These are backtest figures, not live results, and the limitations below apply —
in particular, funding-rate history does not fully cover 2023 through 2025.

## Repository layout

```
src/quant_lab/
  alerts/       email and Telegram delivery
  backtest/     engine, metrics, portfolio, realism model, grid sweep
  data/         OKX public and private clients, factor collection
  execution/    order planner, strategy router
  reporting/    backtest, sweep and research dashboards (Plotly → HTML)
  risk/         portfolio risk, rule evaluation
  service/      FastAPI app, monitor, database, research agent
  strategies/   ema_trend, multi_cycle_trend
config/         example settings for several strategy variants
tests/          40+ unit tests across the package
tools/          research environment bootstrap
```

## Quick start

Requires Python 3.11+. Linux or WSL2 is recommended; Windows works but TA and
Docker dependencies are smoother under WSL2.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"

cp .env.example .env
cp config/settings.example.yaml config/settings.yaml
```

Sync contract metadata and download history:

```bash
quant-lab sync-instrument --config config/settings.yaml
quant-lab download --config config/settings.yaml --start 2023-01-01 --end 2026-03-01
```

Run a backtest and render the report:

```bash
quant-lab backtest --config config/settings.yaml
quant-lab report   --config config/settings.yaml
```

Grid search:

```bash
quant-lab sweep --config config/settings.yaml \
  --fast 10,20,30 --slow 50,80,120 --atr 1.5,2.0,2.5
```

Artifacts land in `data/reports/`:

```
<SYMBOL>_ema_trend_4h_summary.json
<SYMBOL>_ema_trend_4h_equity_curve.csv
<SYMBOL>_ema_trend_4h_trades.csv
<SYMBOL>_ema_trend_4h_dashboard.html
<SYMBOL>_ema_trend_4h_sweep.csv
<SYMBOL>_ema_trend_4h_sweep_dashboard.html
```

## Default trading assumptions

| Parameter | Value |
| --- | --- |
| Instrument | `BTC-USDT-SWAP` |
| Strategy | `EMA(20/50)` trend switch |
| Signal | `4h` close only |
| Entry | `1m` open after the signal, plus slippage |
| Fees | taker, modelled conservatively on both sides |
| Stop | `ATR × multiple` |

## Execution layer

The execution path is deliberately awkward to trigger. Orders are submitted only
when all of the following hold:

- `OKX_USE_DEMO=true`
- `QUANT_LAB_ALLOW_ORDER_PLACEMENT=true`
- the command passes `--submit`
- the command passes `--confirm OKX_DEMO`

Without them the command prints an order **plan** and exits. Supporting commands:

```bash
quant-lab demo-preflight    --config config/settings.yaml --live-plan
quant-lab demo-account      --config config/settings.yaml
quant-lab demo-plan         --config config/settings.yaml
quant-lab demo-drill        --config config/settings.yaml
quant-lab demo-reconcile    --config config/settings.yaml
quant-lab demo-align-leverage --config config/settings.yaml
```

- `demo-preflight` checks whether submission conditions are met and reports which
  alert channels are ready.
- `demo-reconcile` compares local state (`data/demo_executor_state.json`) against
  the exchange's positions, open orders, conditional orders and leverage, which is
  where most "I thought I placed that order" bugs surface.
- `demo-align-leverage` is dry-run by default; `--apply --confirm OKX_DEMO` is
  required to actually change leverage, and it refuses when protective conditional
  orders are still live in cross mode.

## Service and dashboard

```bash
quant-lab service-init-db --config config/settings.yaml
quant-lab service-step    --config config/settings.yaml
quant-lab service-api     --config config/settings.yaml
```

Then on `127.0.0.1:18080`:

| Path | Contents |
| --- | --- |
| `/` | runtime dashboard |
| `/client` | local trading client (reconcile, leverage dry-run, alert test) |
| `/health` | health check |
| `/runtime/latest` | latest run snapshot |
| `/heartbeats` | recent service heartbeats |
| `/alerts` | recent alerts |
| `/reports/backtest` | backtest HTML report |
| `/reports/sweep` | sweep HTML report |

Storage defaults to SQLite at `sqlite:///data/quant_lab.db`; set
`QUANT_LAB_DATABASE_URL` for PostgreSQL. A `docker-compose.yml` is included for
PostgreSQL and Grafana.

## Testing

```bash
pytest
```

The suite covers backtest funding alignment, portfolio risk overlay, strategy
routing, execution approval gates, demo reconcile, alert delivery, reporting and
the service client API.

## Known limitations

These are real constraints, not disclaimers:

- **Funding-rate history is incomplete** for 2023 through 2025, so funding cost in
  that window is under-modelled.
- **Protective stops are plan-only.** They are attached to entry plans but not yet
  submitted as standalone algo stop orders to OKX.
- **The dashboard is intentionally lightweight.** Grafana is scaffolded but not
  wired into the runtime yet.
- **Single exchange.** Only OKX is supported; there is no venue abstraction.
- **No order-book simulation.** Slippage is a fixed parameter, not a function of
  depth, so results are optimistic on large size.

## Security notes

- Never commit API keys, secrets or passphrases. `.env` is gitignored.
- Backtesting needs only public market data; no private key is required to run the
  research half of this repository.
- If a key was ever exposed, revoke and rotate it before anything else.

## License

MIT — see [LICENSE](LICENSE).

---

中文文档：[README.zh-CN.md](README.zh-CN.md)
