from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_lab.data.public_factors import load_public_factor_snapshot


def test_load_public_factor_snapshot_builds_score_from_local_artifacts(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"timestamp": "2026-03-28T00:00:00Z", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "confirm": "1"},
            {"timestamp": "2026-03-28T04:00:00Z", "open": 100.5, "high": 102.0, "low": 100.0, "close": 101.5, "confirm": "1"},
        ]
    ).to_parquet(raw_dir / "BTC-USDT-SWAP_mark_price_4H.parquet", index=False)
    pd.DataFrame(
        [
            {"timestamp": "2026-03-28T00:00:00Z", "open": 99.8, "high": 100.8, "low": 99.5, "close": 100.0, "confirm": "1"},
            {"timestamp": "2026-03-28T04:00:00Z", "open": 100.1, "high": 101.5, "low": 99.8, "close": 100.7, "confirm": "1"},
        ]
    ).to_parquet(raw_dir / "BTC-USDT-SWAP_index_4H.parquet", index=False)
    pd.DataFrame(
        [
            {"timestamp": "2026-03-28T00:00:00Z", "symbol": "BTC-USDT-SWAP", "open_interest_contracts": 1000.0},
            {"timestamp": "2026-03-28T04:00:00Z", "symbol": "BTC-USDT-SWAP", "open_interest_contracts": 1035.0},
        ]
    ).to_parquet(raw_dir / "BTC-USDT-SWAP_open_interest.parquet", index=False)
    pd.DataFrame(
        [
            {
                "timestamp": "2026-03-28T04:00:00Z",
                "symbol": "BTC-USDT-SWAP",
                "depth": 50,
                "best_bid_price": 101.4,
                "best_bid_size": 10.0,
                "best_ask_price": 101.6,
                "best_ask_size": 8.0,
                "spread": 0.2,
                "bid_top5_notional": 100000.0,
                "ask_top5_notional": 80000.0,
            }
        ]
    ).to_parquet(raw_dir / "BTC-USDT-SWAP_books_full_summary.parquet", index=False)
    pd.DataFrame(
        [
            {"timestamp": "2026-03-28T03:59:00Z", "symbol": "BTC-USDT-SWAP", "trade_id": "1", "price": 101.0, "size": 3.0, "side": "buy", "count": 1.0},
            {"timestamp": "2026-03-28T03:59:30Z", "symbol": "BTC-USDT-SWAP", "trade_id": "2", "price": 101.2, "size": 2.0, "side": "buy", "count": 1.0},
            {"timestamp": "2026-03-28T03:59:50Z", "symbol": "BTC-USDT-SWAP", "trade_id": "3", "price": 101.1, "size": 1.0, "side": "sell", "count": 1.0},
        ]
    ).to_parquet(raw_dir / "BTC-USDT-SWAP_history_trades.parquet", index=False)

    snapshot = load_public_factor_snapshot(
        raw_dir=raw_dir,
        symbol="BTC-USDT-SWAP",
        signal_bar="4H",
        asof=pd.Timestamp("2026-03-28T04:00:00Z"),
    )

    assert snapshot.basis_bps is not None
    assert snapshot.oi_change_pct == 3.5
    assert snapshot.orderbook_imbalance is not None and snapshot.orderbook_imbalance > 0
    assert snapshot.trade_buy_notional_ratio is not None and snapshot.trade_buy_notional_ratio > 0.5
    assert snapshot.confidence == 1.0
    assert snapshot.score > 0.6
    assert snapshot.risk_multiplier > 0.9
