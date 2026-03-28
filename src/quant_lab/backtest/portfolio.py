from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from quant_lab.backtest.metrics import build_summary
from quant_lab.models import TradeRecord


def combine_portfolio_equity_curves(equity_curves: dict[str, pd.DataFrame]) -> pd.DataFrame:
    if not equity_curves:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "cash",
                "equity",
                "unrealized_pnl",
                "halted",
                "position_side",
                "contracts",
                "active_positions",
            ]
        )

    merged: pd.DataFrame | None = None
    initial_values: dict[str, dict[str, float | bool]] = {}

    for symbol, equity_curve in equity_curves.items():
        normalized = equity_curve.copy()
        normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], utc=True)
        normalized = normalized.sort_values("timestamp").reset_index(drop=True)
        slug = _symbol_slug(symbol)
        renamed = normalized.rename(
            columns={
                "cash": f"cash__{slug}",
                "equity": f"equity__{slug}",
                "unrealized_pnl": f"unrealized__{slug}",
                "halted": f"halted__{slug}",
                "position_side": f"position_side__{slug}",
                "contracts": f"contracts__{slug}",
            }
        )[
            [
                "timestamp",
                f"cash__{slug}",
                f"equity__{slug}",
                f"unrealized__{slug}",
                f"halted__{slug}",
                f"position_side__{slug}",
                f"contracts__{slug}",
            ]
        ]
        initial_values[slug] = {
            "cash": float(renamed.iloc[0][f"cash__{slug}"]),
            "equity": float(renamed.iloc[0][f"equity__{slug}"]),
            "unrealized": float(renamed.iloc[0][f"unrealized__{slug}"]),
            "halted": bool(renamed.iloc[0][f"halted__{slug}"]),
            "position_side": float(renamed.iloc[0][f"position_side__{slug}"]),
            "contracts": float(renamed.iloc[0][f"contracts__{slug}"]),
        }
        merged = renamed if merged is None else merged.merge(renamed, on="timestamp", how="outer")

    assert merged is not None
    merged = merged.sort_values("timestamp").reset_index(drop=True)

    cash_columns: list[str] = []
    equity_columns: list[str] = []
    unrealized_columns: list[str] = []
    halted_columns: list[str] = []
    contracts_columns: list[str] = []
    position_columns: list[str] = []

    for slug, initial in initial_values.items():
        cash_column = f"cash__{slug}"
        equity_column = f"equity__{slug}"
        unrealized_column = f"unrealized__{slug}"
        halted_column = f"halted__{slug}"
        position_column = f"position_side__{slug}"
        contracts_column = f"contracts__{slug}"

        merged[cash_column] = merged[cash_column].ffill().fillna(initial["cash"])
        merged[equity_column] = merged[equity_column].ffill().fillna(initial["equity"])
        merged[unrealized_column] = merged[unrealized_column].ffill().fillna(initial["unrealized"])
        merged[halted_column] = merged[halted_column].ffill().fillna(initial["halted"]).astype(bool)
        merged[position_column] = merged[position_column].ffill().fillna(initial["position_side"])
        merged[contracts_column] = merged[contracts_column].ffill().fillna(initial["contracts"])

        cash_columns.append(cash_column)
        equity_columns.append(equity_column)
        unrealized_columns.append(unrealized_column)
        halted_columns.append(halted_column)
        contracts_columns.append(contracts_column)
        position_columns.append(position_column)

    active_positions = (
        merged[position_columns]
        .fillna(0.0)
        .astype(float)
        .ne(0.0)
        .sum(axis=1)
    )
    combined = pd.DataFrame(
        {
            "timestamp": merged["timestamp"],
            "cash": merged[cash_columns].sum(axis=1),
            "equity": merged[equity_columns].sum(axis=1),
            "unrealized_pnl": merged[unrealized_columns].sum(axis=1),
            "halted": merged[halted_columns].any(axis=1),
            "position_side": 0,
            "contracts": merged[contracts_columns].sum(axis=1),
            "active_positions": active_positions,
        }
    )
    return combined.reset_index(drop=True)


def build_portfolio_trade_frame(trades_by_symbol: dict[str, Sequence[TradeRecord]]) -> pd.DataFrame:
    columns = [
        "signal_time",
        "entry_time",
        "exit_time",
        "side",
        "contracts",
        "entry_price",
        "exit_price",
        "stop_price",
        "gross_pnl",
        "funding_pnl",
        "fee_paid",
        "net_pnl",
        "exit_reason",
        "symbol",
    ]
    rows: list[dict[str, object]] = []
    for symbol, trades in trades_by_symbol.items():
        for trade in trades:
            row = trade.to_dict()
            row["symbol"] = row.get("symbol") or symbol
            rows.append(row)

    if not rows:
        return pd.DataFrame(columns=columns)

    frame = pd.DataFrame(rows)
    missing_columns = [column for column in columns if column not in frame.columns]
    for column in missing_columns:
        frame[column] = None
    return frame[columns].sort_values(["entry_time", "exit_time", "symbol"]).reset_index(drop=True)


def build_portfolio_summary(
    *,
    equity_curve: pd.DataFrame,
    trades: Sequence[TradeRecord],
    initial_equity: float,
    symbols: Sequence[str],
) -> dict[str, object]:
    summary = build_summary(
        equity_curve=equity_curve,
        trades=trades,
        initial_equity=initial_equity,
    )
    summary["symbol_count"] = len(symbols)
    summary["symbols"] = list(symbols)
    return summary


def _symbol_slug(symbol: str) -> str:
    return symbol.replace("/", "-").replace(":", "-").replace(".", "-")
