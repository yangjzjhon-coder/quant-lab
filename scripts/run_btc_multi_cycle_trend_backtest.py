from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from quant_lab.reporting.dashboard import render_dashboard
from quant_lab.strategies.multi_cycle_trend import (
    MultiCycleTrendParameters,
    resample_intraday_to_4h,
    run_multi_cycle_backtest_on_4h,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the BTC multi-cycle trend backtest over all local BTC data.")
    parser.add_argument(
        "--source",
        default=str(PROJECT_ROOT / "data" / "raw" / "BTC-USDT-SWAP_1m.parquet"),
        help="Path to the BTC intraday parquet source used to build 4H execution bars.",
    )
    parser.add_argument(
        "--report-prefix",
        default="BTC-USDT-SWAP_multi_cycle_trend_prompt",
        help="Prefix used for report artifacts under data/reports.",
    )
    parser.add_argument("--initial-equity", type=float, default=10_000.0)
    parser.add_argument("--fee-bps", type=float, default=8.0)
    parser.add_argument("--price-tick", type=float, default=0.01)
    parser.add_argument("--margin-fraction", type=float, default=0.20)
    parser.add_argument("--max-leverage", type=float, default=3.0)
    parser.add_argument(
        "--min-stop-distance-pct",
        type=float,
        default=2.0,
        help="Conservative floor applied to stop distance when deriving leverage.",
    )
    args = parser.parse_args()

    source_path = Path(args.source)
    if not source_path.exists():
        raise FileNotFoundError(f"BTC source parquet not found: {source_path}")

    intraday = pd.read_parquet(source_path)
    execution_bars = resample_intraday_to_4h(intraday)
    params = MultiCycleTrendParameters(
        fee_bps=args.fee_bps,
        price_tick=args.price_tick,
        margin_fraction=args.margin_fraction,
        max_leverage=args.max_leverage,
        minimum_stop_distance_fraction=max(args.min_stop_distance_pct, 0.0) / 100.0,
    )
    results = run_multi_cycle_backtest_on_4h(
        execution_bars,
        params=params,
        initial_equity=args.initial_equity,
        symbol="BTC-USDT-SWAP",
    )

    report_dir = PROJECT_ROOT / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    prefix = args.report_prefix
    summary_path = report_dir / f"{prefix}_summary.json"
    equity_curve_path = report_dir / f"{prefix}_equity_curve.csv"
    trades_path = report_dir / f"{prefix}_trades.csv"
    signals_path = report_dir / f"{prefix}_signals.csv"
    execution_bars_path = report_dir / f"{prefix}_execution_bars.csv"
    dashboard_path = report_dir / f"{prefix}_dashboard.html"

    summary_path.write_text(
        json.dumps(results["summary"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    results["equity_curve"].to_csv(equity_curve_path, index=False)
    results["trades"].to_csv(trades_path, index=False)
    results["signals"].to_csv(signals_path, index=False)
    results["execution_bars"].to_csv(execution_bars_path, index=False)
    render_dashboard(
        summary_path=summary_path,
        equity_curve_path=equity_curve_path,
        trades_path=trades_path,
        output_path=dashboard_path,
        title="BTC Multi-Cycle Trend Prompt Backtest",
    )

    summary = results["summary"]
    print("Execution timeframe: 4H")
    print(f"Signals: {summary['signal_count']}")
    print(f"Trades: {summary['trade_count']}")
    print(f"Final equity: {summary['final_equity']}")
    print(f"Total return %: {summary['total_return_pct']}")
    print(f"Max drawdown %: {summary['max_drawdown_pct']}")
    print(f"Summary: {summary_path}")
    print(f"Signals CSV: {signals_path}")
    print(f"Trades CSV: {trades_path}")
    print(f"Equity CSV: {equity_curve_path}")
    print(f"Execution Bars CSV: {execution_bars_path}")
    print(f"Dashboard: {dashboard_path}")


if __name__ == "__main__":
    main()
