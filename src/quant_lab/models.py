from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


@dataclass
class Position:
    side: int
    contracts: float
    contract_value: float
    entry_time: pd.Timestamp
    signal_time: pd.Timestamp
    entry_price: float
    stop_price: float
    entry_fee: float
    funding_paid: float = 0.0

    def notional(self, mark_price: float) -> float:
        return self.contracts * self.contract_value * mark_price

    def unrealized_pnl(self, mark_price: float) -> float:
        return self.side * self.contracts * self.contract_value * (mark_price - self.entry_price)


@dataclass
class TradeRecord:
    signal_time: pd.Timestamp
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    side: str
    contracts: float
    entry_price: float
    exit_price: float
    stop_price: float
    gross_pnl: float
    funding_pnl: float
    fee_paid: float
    net_pnl: float
    exit_reason: str
    symbol: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BacktestArtifacts:
    trades: list[TradeRecord]
    equity_curve: pd.DataFrame
    signal_frame: pd.DataFrame
