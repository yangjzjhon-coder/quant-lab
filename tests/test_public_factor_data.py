from __future__ import annotations

from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from quant_lab.cli import _index_inst_id, app
from quant_lab.config import AppConfig, DatabaseConfig, InstrumentConfig, StorageConfig, StrategyConfig
from quant_lab.data.okx_public_client import OkxPublicClient


def test_index_inst_id_maps_swap_to_spot_index() -> None:
    assert _index_inst_id("BTC-USDT-SWAP") == "BTC-USDT"
    assert _index_inst_id("ETH-USDT-SWAP") == "ETH-USDT"


def test_fetch_history_trades_paginates_and_dedupes(monkeypatch) -> None:
    client = OkxPublicClient(base_url="https://www.okx.com")
    payloads = [
        {
            "code": "0",
            "data": [
                {"instId": "BTC-USDT-SWAP", "tradeId": "200", "px": "101", "sz": "2", "side": "buy", "ts": "2000"},
                {"instId": "BTC-USDT-SWAP", "tradeId": "199", "px": "100", "sz": "1", "side": "sell", "ts": "1000"},
            ],
        },
        {
            "code": "0",
            "data": [
                {"instId": "BTC-USDT-SWAP", "tradeId": "199", "px": "100", "sz": "1", "side": "sell", "ts": "1000"},
                {"instId": "BTC-USDT-SWAP", "tradeId": "198", "px": "99", "sz": "3", "side": "buy", "ts": "500"},
            ],
        },
    ]

    monkeypatch.setattr(client, "_get_json", lambda path, params: payloads.pop(0))

    frame = client.fetch_history_trades("BTC-USDT-SWAP", max_pages=2, pause_seconds=0.0)

    assert list(frame["trade_id"]) == ["198", "199", "200"]
    assert list(frame["price"]) == [99.0, 100.0, 101.0]
    client.close()


def test_download_public_factors_writes_expected_artifacts(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("{}", encoding="utf-8")

    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    cfg = AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP", instrument_type="SWAP"),
        strategy=StrategyConfig(name="breakout_retest_4h", signal_bar="4H"),
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
    )

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def close(self) -> None:
            return None

        def fetch_open_interest(self, inst_type: str, inst_id: str) -> dict[str, object]:
            return {
                "timestamp": pd.Timestamp("2026-03-28T00:00:00Z"),
                "symbol": inst_id,
                "open_interest_contracts": 12345.0,
                "open_interest_currency": 123.45,
            }

        def fetch_mark_price(self, inst_type: str, inst_id: str) -> dict[str, object]:
            return {
                "timestamp": pd.Timestamp("2026-03-28T00:00:00Z"),
                "symbol": inst_id,
                "mark_price": 65000.0,
            }

        def fetch_index_ticker(self, index_inst_id: str) -> dict[str, object]:
            return {
                "timestamp": pd.Timestamp("2026-03-28T00:00:00Z"),
                "index_inst_id": index_inst_id,
                "index_price": 64980.0,
                "high_24h": 66000.0,
                "low_24h": 64000.0,
                "open_24h": 64500.0,
            }

        def fetch_history_trades(self, inst_id: str, *, limit: int, max_pages: int) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "timestamp": pd.Timestamp("2026-03-28T00:00:00Z"),
                        "symbol": inst_id,
                        "trade_id": "1001",
                        "price": 65001.0,
                        "size": 2.0,
                        "side": "buy",
                        "count": 1.0,
                    }
                ]
            )

        def fetch_books_full_snapshot(self, inst_id: str, depth: int = 50) -> dict[str, object]:
            return {
                "timestamp": pd.Timestamp("2026-03-28T00:00:00Z"),
                "symbol": inst_id,
                "depth": depth,
                "bids": [{"price": 64999.0, "size": 3.0, "liquidated_orders": None, "order_count": 2}],
                "asks": [{"price": 65001.0, "size": 4.0, "liquidated_orders": None, "order_count": 2}],
            }

        def fetch_history_mark_price_candles(
            self,
            inst_id: str,
            bar: str,
            start: pd.Timestamp,
            end: pd.Timestamp,
        ) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "timestamp": pd.Timestamp("2026-03-28T00:00:00Z"),
                        "open": 64800.0,
                        "high": 65100.0,
                        "low": 64750.0,
                        "close": 65000.0,
                        "confirm": "1",
                    }
                ]
            )

        def fetch_history_index_candles(
            self,
            index_inst_id: str,
            bar: str,
            start: pd.Timestamp,
            end: pd.Timestamp,
        ) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "timestamp": pd.Timestamp("2026-03-28T00:00:00Z"),
                        "open": 64790.0,
                        "high": 65090.0,
                        "low": 64740.0,
                        "close": 64980.0,
                        "confirm": "1",
                    }
                ]
            )

    monkeypatch.setattr("quant_lab.cli.load_config", lambda _path: cfg)
    monkeypatch.setattr("quant_lab.cli.OkxPublicClient", FakeClient)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "download-public-factors",
            "--config",
            str(config_path),
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-28",
        ],
    )

    assert result.exit_code == 0
    assert (raw_dir / "BTC-USDT-SWAP_open_interest.parquet").exists()
    assert (raw_dir / "BTC-USDT-SWAP_mark_price.parquet").exists()
    assert (raw_dir / "BTC-USDT-SWAP_index_ticker.parquet").exists()
    assert (raw_dir / "BTC-USDT-SWAP_history_trades.parquet").exists()
    assert (raw_dir / "BTC-USDT-SWAP_books_full_summary.parquet").exists()
    assert (raw_dir / "BTC-USDT-SWAP_books_full_latest.json").exists()
    assert (raw_dir / "BTC-USDT-SWAP_mark_price_4H.parquet").exists()
    assert (raw_dir / "BTC-USDT-SWAP_index_4H.parquet").exists()

    trade_frame = pd.read_parquet(raw_dir / "BTC-USDT-SWAP_history_trades.parquet")
    assert len(trade_frame) == 1
    assert trade_frame.iloc[0]["trade_id"] == "1001"
