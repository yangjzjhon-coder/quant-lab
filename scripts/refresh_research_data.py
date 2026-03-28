from __future__ import annotations

import time
from pathlib import Path

import pandas as pd

from quant_lab.config import ensure_storage_dirs, load_config
from quant_lab.data.okx_public_client import OkxPublicClient


def fetch_segment(
    client: OkxPublicClient,
    *,
    inst_id: str,
    bar: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> pd.DataFrame:
    for attempt in range(1, 4):
        try:
            frame = client.fetch_history_candles(
                inst_id=inst_id,
                bar=bar,
                start=start_ts,
                end=end_ts,
            )
            print(f"[{bar}] ok {start_ts} -> {end_ts} rows={len(frame)}", flush=True)
            return frame
        except Exception as exc:  # pragma: no cover - network dependent helper
            print(
                f"[{bar}] retry {attempt} failed for {start_ts} -> {end_ts}: {type(exc).__name__}: {exc}",
                flush=True,
            )
            if attempt >= 3:
                raise
            time.sleep(2)
    raise RuntimeError("unreachable")


def fetch_segmented(
    client: OkxPublicClient,
    *,
    inst_id: str,
    bar: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    chunk_days: int,
    step: pd.Timedelta,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    cursor = start_ts
    while cursor <= end_ts:
        chunk_end = min(cursor + pd.Timedelta(days=chunk_days) - step, end_ts)
        frames.append(fetch_segment(client, inst_id=inst_id, bar=bar, start_ts=cursor, end_ts=chunk_end))
        cursor = chunk_end + step

    if not frames:
        return pd.DataFrame()

    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def main() -> None:
    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / "config" / "settings_1h_research.yaml"
    cfg = load_config(config_path)
    storage = cfg.storage.resolved(project_root)
    ensure_storage_dirs(storage)

    symbol_slug = cfg.instrument.symbol.replace("/", "-")
    raw_dir = storage.raw_dir
    execution_existing_path = raw_dir / f"{symbol_slug}_1m.parquet"

    client = OkxPublicClient(base_url=cfg.okx.rest_base_url, proxy_url=cfg.okx.proxy_url)
    try:
        signal_full = fetch_segmented(
            client,
            inst_id=cfg.instrument.symbol,
            bar="1H",
            start_ts=pd.Timestamp("2020-01-01", tz="UTC"),
            end_ts=pd.Timestamp("2026-03-25 23:00:00", tz="UTC"),
            chunk_days=540,
            step=pd.Timedelta(hours=1),
        )

        existing_execution = pd.read_parquet(execution_existing_path)
        existing_execution["timestamp"] = pd.to_datetime(existing_execution["timestamp"], utc=True)
        print(
            "[1m] existing "
            f"rows={len(existing_execution)} "
            f"range={existing_execution['timestamp'].min()} -> {existing_execution['timestamp'].max()}",
            flush=True,
        )

        historical_execution = fetch_segmented(
            client,
            inst_id=cfg.instrument.symbol,
            bar="1m",
            start_ts=pd.Timestamp("2020-01-01", tz="UTC"),
            end_ts=pd.Timestamp("2022-12-31 23:59:00", tz="UTC"),
            chunk_days=60,
            step=pd.Timedelta(minutes=1),
        )
        latest_gap_execution = fetch_segment(
            client,
            inst_id=cfg.instrument.symbol,
            bar="1m",
            start_ts=pd.Timestamp("2026-03-01 00:00:00", tz="UTC"),
            end_ts=pd.Timestamp("2026-03-25 23:59:00", tz="UTC"),
        )
        execution_full = (
            pd.concat(
                [historical_execution, existing_execution, latest_gap_execution],
                ignore_index=True,
            )
            .drop_duplicates(subset=["timestamp"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

        funding = client.fetch_funding_rate_history(
            inst_id=cfg.instrument.symbol,
            start=pd.Timestamp("2020-01-01", tz="UTC"),
            end=pd.Timestamp("2026-03-25 23:59:00", tz="UTC"),
        )
        print(f"[funding] rows={len(funding)}", flush=True)
    finally:
        client.close()

    signal_path = raw_dir / f"{symbol_slug}_1H.parquet"
    execution_path = raw_dir / f"{symbol_slug}_1m.parquet"
    funding_path = raw_dir / f"{symbol_slug}_funding.parquet"

    signal_full.to_parquet(signal_path, index=False)
    execution_full.to_parquet(execution_path, index=False)
    funding.to_parquet(funding_path, index=False)

    print(
        f"saved signal rows={len(signal_full)} range={signal_full['timestamp'].min()} -> {signal_full['timestamp'].max()}",
        flush=True,
    )
    print(
        "saved execution "
        f"rows={len(execution_full)} "
        f"range={execution_full['timestamp'].min()} -> {execution_full['timestamp'].max()}",
        flush=True,
    )
    print(
        "saved funding "
        f"rows={len(funding)} "
        f"range={funding['timestamp'].min() if not funding.empty else None} -> "
        f"{funding['timestamp'].max() if not funding.empty else None}",
        flush=True,
    )


if __name__ == "__main__":
    main()
