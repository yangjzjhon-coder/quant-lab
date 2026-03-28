from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class PublicFactorSnapshot:
    symbol: str
    asof: pd.Timestamp | None = None
    basis_bps: float | None = None
    basis_change_bps: float | None = None
    oi_change_pct: float | None = None
    orderbook_imbalance: float | None = None
    trade_buy_notional_ratio: float | None = None
    score: float = 0.5
    confidence: float = 0.0
    risk_multiplier: float = 1.0
    components: dict[str, float] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.asof is not None:
            payload["asof"] = self.asof.isoformat()
        return payload


def load_public_factor_snapshot(
    *,
    raw_dir: Path,
    symbol: str,
    signal_bar: str,
    asof: pd.Timestamp | None = None,
) -> PublicFactorSnapshot:
    snapshot = PublicFactorSnapshot(symbol=symbol, asof=_coerce_timestamp(asof))
    symbol_slug = symbol.replace("/", "-")

    mark_candles = _read_parquet(raw_dir / f"{symbol_slug}_mark_price_{signal_bar}.parquet")
    index_candles = _read_parquet(raw_dir / f"{symbol_slug}_index_{signal_bar}.parquet")
    oi_frame = _read_parquet(raw_dir / f"{symbol_slug}_open_interest.parquet")
    book_frame = _read_parquet(raw_dir / f"{symbol_slug}_books_full_summary.parquet")
    trades = _read_parquet(raw_dir / f"{symbol_slug}_history_trades.parquet")

    target_time = snapshot.asof
    if target_time is None:
        target_time = _latest_timestamp(mark_candles, index_candles, oi_frame, book_frame, trades)
        snapshot.asof = target_time

    component_scores: dict[str, float] = {}

    mark_row = _latest_row_before(mark_candles, target_time)
    index_row = _latest_row_before(index_candles, target_time)
    if mark_row is not None and index_row is not None:
        mark_close = _safe_float(mark_row.get("close"))
        index_close = _safe_float(index_row.get("close"))
        if mark_close and index_close:
            snapshot.basis_bps = round(((mark_close / index_close) - 1.0) * 10_000, 4)
            previous_basis = _previous_basis_bps(mark_candles, index_candles, target_time)
            if previous_basis is not None:
                snapshot.basis_change_bps = round(snapshot.basis_bps - previous_basis, 4)
            component_scores["basis"] = _basis_score(snapshot.basis_bps, snapshot.basis_change_bps)
    else:
        snapshot.notes.append("mark/index candles are missing")

    oi_rows = _rows_before(oi_frame, target_time).tail(2)
    if len(oi_rows) >= 2:
        previous_oi = _safe_float(oi_rows.iloc[-2].get("open_interest_contracts"))
        current_oi = _safe_float(oi_rows.iloc[-1].get("open_interest_contracts"))
        if previous_oi and current_oi and previous_oi > 0:
            snapshot.oi_change_pct = round(((current_oi / previous_oi) - 1.0) * 100, 4)
            component_scores["oi"] = _oi_score(snapshot.oi_change_pct)
    else:
        snapshot.notes.append("open interest history is too sparse")

    book_row = _latest_row_before(book_frame, target_time)
    if book_row is not None:
        bid_notional = _safe_float(book_row.get("bid_top5_notional"))
        ask_notional = _safe_float(book_row.get("ask_top5_notional"))
        if bid_notional is not None and ask_notional is not None and (bid_notional + ask_notional) > 0:
            snapshot.orderbook_imbalance = round(
                (bid_notional - ask_notional) / (bid_notional + ask_notional),
                4,
            )
            component_scores["orderbook"] = _orderbook_score(snapshot.orderbook_imbalance)
    else:
        snapshot.notes.append("books-full summary is missing")

    trade_rows = _recent_trade_rows(trades, target_time)
    if not trade_rows.empty:
        buy_notional = (
            trade_rows.loc[trade_rows["side"] == "buy", "price"].astype(float)
            * trade_rows.loc[trade_rows["side"] == "buy", "size"].astype(float)
        ).sum()
        total_notional = (trade_rows["price"].astype(float) * trade_rows["size"].astype(float)).sum()
        if total_notional > 0:
            snapshot.trade_buy_notional_ratio = round(float(buy_notional / total_notional), 4)
            component_scores["trade_flow"] = _trade_flow_score(snapshot.trade_buy_notional_ratio)
    else:
        snapshot.notes.append("history trades are missing")

    confidence = len(component_scores) / 4.0
    snapshot.confidence = round(confidence, 4)
    snapshot.components = {key: round(value, 4) for key, value in component_scores.items()}

    if component_scores:
        raw_score = sum(component_scores.values()) / len(component_scores)
        blended_score = (raw_score * confidence) + (0.5 * (1.0 - confidence))
    else:
        blended_score = 0.5

    snapshot.score = round(max(0.0, min(1.0, blended_score)), 4)
    snapshot.risk_multiplier = round(max(0.35, min(1.15, 0.35 + snapshot.score)), 4)
    return snapshot


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path)
    if "timestamp" in frame.columns:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    return frame.sort_values("timestamp").reset_index(drop=True) if "timestamp" in frame.columns else frame


def _latest_timestamp(*frames: pd.DataFrame) -> pd.Timestamp | None:
    timestamps: list[pd.Timestamp] = []
    for frame in frames:
        if not frame.empty and "timestamp" in frame.columns:
            timestamps.append(pd.Timestamp(frame["timestamp"].max()))
    return max(timestamps) if timestamps else None


def _coerce_timestamp(value: pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None:
        return None
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _rows_before(frame: pd.DataFrame, asof: pd.Timestamp | None) -> pd.DataFrame:
    if frame.empty or "timestamp" not in frame.columns:
        return pd.DataFrame()
    if asof is None:
        return frame
    return frame.loc[frame["timestamp"] <= asof].copy()


def _latest_row_before(frame: pd.DataFrame, asof: pd.Timestamp | None) -> dict[str, Any] | None:
    rows = _rows_before(frame, asof)
    if rows.empty:
        return None
    row = rows.iloc[-1]
    return row.to_dict()


def _previous_basis_bps(mark_candles: pd.DataFrame, index_candles: pd.DataFrame, asof: pd.Timestamp | None) -> float | None:
    mark_rows = _rows_before(mark_candles, asof)
    index_rows = _rows_before(index_candles, asof)
    if len(mark_rows) < 2 or len(index_rows) < 2:
        return None

    mark_prev = _safe_float(mark_rows.iloc[-2].get("close"))
    index_prev = _safe_float(index_rows.iloc[-2].get("close"))
    if mark_prev is None or index_prev is None or index_prev == 0:
        return None
    return ((mark_prev / index_prev) - 1.0) * 10_000


def _recent_trade_rows(frame: pd.DataFrame, asof: pd.Timestamp | None, max_rows: int = 200) -> pd.DataFrame:
    rows = _rows_before(frame, asof)
    if rows.empty:
        return rows
    return rows.tail(max_rows).copy()


def _basis_score(basis_bps: float | None, basis_change_bps: float | None) -> float:
    if basis_bps is None:
        return 0.5
    if -4.0 <= basis_bps <= 12.0:
        score = 0.9
    elif -10.0 <= basis_bps <= 20.0:
        score = 0.7
    elif basis_bps > 35.0 or basis_bps < -20.0:
        score = 0.2
    else:
        score = 0.45

    if basis_change_bps is not None:
        if basis_change_bps > 8.0:
            score -= 0.15
        elif basis_change_bps < -8.0:
            score -= 0.1
    return max(0.0, min(1.0, score))


def _oi_score(oi_change_pct: float | None) -> float:
    if oi_change_pct is None:
        return 0.5
    if oi_change_pct >= 3.0:
        return 0.9
    if oi_change_pct >= 1.0:
        return 0.75
    if oi_change_pct >= -0.5:
        return 0.55
    if oi_change_pct >= -2.0:
        return 0.35
    return 0.2


def _orderbook_score(imbalance: float | None) -> float:
    if imbalance is None:
        return 0.5
    return max(0.0, min(1.0, 0.5 + (imbalance * 1.6)))


def _trade_flow_score(buy_ratio: float | None) -> float:
    if buy_ratio is None:
        return 0.5
    return max(0.0, min(1.0, 0.5 + ((buy_ratio - 0.5) * 2.2)))


def _safe_float(value: Any) -> float | None:
    if value in {None, "", " "}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
