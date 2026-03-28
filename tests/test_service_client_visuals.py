from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from quant_lab.config import AppConfig, DatabaseConfig, InstrumentConfig, PortfolioConfig, ServiceConfig, StorageConfig, StrategyConfig
from quant_lab.service.client_ops import _build_autotrade_status
from quant_lab.service.client_ops import build_client_snapshot
from quant_lab.service.client_ops import build_demo_visuals_payload
from quant_lab.service.database import AlertEvent, ServiceHeartbeat, init_db, make_session_factory, session_scope


def test_build_demo_visuals_payload_summarizes_demo_history(tmp_path: Path) -> None:
    database_url = f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"
    init_db(database_url)
    session_factory = make_session_factory(database_url)

    with session_scope(session_factory) as session:
        session.add_all(
            [
                ServiceHeartbeat(
                    service_name="quant-lab-demo-loop",
                    status="plan_only",
                    details={
                        "cycle": 1,
                        "action": "hold",
                        "desired_side": -1,
                        "current_side": 0,
                        "target_contracts": 18.5,
                        "current_contracts": 0.0,
                        "submitted": False,
                        "response_count": 0,
                        "warning_count": 0,
                        "latest_price": 71000.0,
                    },
                    created_at=_utc("2026-03-25T12:00:00+00:00"),
                ),
                ServiceHeartbeat(
                    service_name="quant-lab-demo-loop",
                    status="submitted",
                    details={
                        "cycle": 2,
                        "action": "open",
                        "desired_side": -1,
                        "current_side": 0,
                        "target_contracts": 27.96,
                        "current_contracts": 0.0,
                        "submitted": True,
                        "response_count": 1,
                        "warning_count": 0,
                        "latest_price": 71305.0,
                        "total_equity": 31438.45,
                        "available_equity": 31438.45,
                    },
                    created_at=_utc("2026-03-25T12:05:00+00:00"),
                ),
                ServiceHeartbeat(
                    service_name="quant-lab-demo-loop",
                    status="warning",
                    details={
                        "cycle": 3,
                        "action": "hold",
                        "desired_side": -1,
                        "current_side": -1,
                        "target_contracts": 21.83,
                        "current_contracts": 27.96,
                        "submitted": False,
                        "response_count": 0,
                        "warning_count": 1,
                        "latest_price": 71600.0,
                    },
                    created_at=_utc("2026-03-25T12:10:00+00:00"),
                ),
                AlertEvent(
                    event_key="demo_order_submitted",
                    channel="email",
                    level="info",
                    title="Demo order submitted",
                    message="submission test",
                    status="sent",
                    delivered_at=_utc("2026-03-25T12:05:05+00:00"),
                    created_at=_utc("2026-03-25T12:05:05+00:00"),
                ),
            ]
        )

    reconcile = {
        "position": {"contracts": 27.96, "side": -1},
        "plan": {"target_contracts": 21.83},
        "signal": {"desired_side": -1},
    }
    payload = build_demo_visuals_payload(session_factory=session_factory, reconcile=reconcile)

    assert payload["summary"]["total_cycles"] == 3
    assert payload["summary"]["submitted_count"] == 1
    assert payload["summary"]["warning_count"] == 1
    assert payload["summary"]["current_contracts"] == 27.96
    assert payload["summary"]["target_contracts"] == 21.83
    assert payload["summary"]["contract_gap"] == 6.13
    assert payload["chart"]["points"][1]["status"] == "submitted"
    assert payload["chart"]["points"][2]["current_contracts"] == 27.96
    assert payload["recent_events"][0]["cycle"] == 3
    assert payload["recent_alerts"][0]["event_key"] == "demo_order_submitted"


def test_build_client_snapshot_falls_back_to_cached_state_when_live_fetch_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    (config.storage.data_dir / "demo_executor_state.json").write_text(
        """
{
  "last_plan": {
    "action": "flip",
    "desired_side": 1,
    "current_side": -1,
    "current_contracts": 27.96,
    "target_contracts": 17.51,
    "latest_price": 71344.0,
    "position_mode": "long_short_mode",
    "warnings": []
  },
  "last_signal": {
    "signal_time": "2026-03-25T08:00:00+00:00",
    "effective_time": "2026-03-25T08:01:00+00:00",
    "latest_price": 71344.0,
    "desired_side": 1,
    "ready": true
  }
}
        """.strip(),
        encoding="utf-8",
    )

    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="error",
                details={
                    "cycle": 8,
                    "action": "flip",
                    "desired_side": 1,
                    "current_side": -1,
                    "target_contracts": 17.51,
                    "current_contracts": 27.96,
                    "submitted": False,
                    "response_count": 0,
                    "warning_count": 1,
                    "latest_price": 71344.0,
                    "signal_time": "2026-03-25T08:00:00+00:00",
                    "effective_time": "2026-03-25T08:01:00+00:00",
                },
                created_at=_utc("2026-03-25T12:10:00+00:00"),
            )
        )

    def _boom(_config):
        raise RuntimeError("network down")

    monkeypatch.setattr("quant_lab.cli._load_demo_state", _boom)

    payload = build_client_snapshot(config=config, session_factory=session_factory, project_root=tmp_path)

    assert payload["snapshot_source"] == "cached_local_state"
    assert payload["live_error"] == "RuntimeError: network down"
    assert payload["reconcile"]["snapshot_source"] == "cached_local_state"
    assert payload["reconcile"]["plan"]["target_contracts"] == 17.51
    assert payload["reconcile"]["position"]["contracts"] == 27.96
    assert payload["demo_visuals"]["summary"]["current_contracts"] == 27.96
    assert payload["demo_visuals"]["summary"]["target_contracts"] == 17.51
    assert payload["demo_visuals"]["summary"]["last_status_label"] == "错误"


def test_build_demo_visuals_payload_summarizes_portfolio_history(tmp_path: Path) -> None:
    database_url = f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"
    init_db(database_url)
    session_factory = make_session_factory(database_url)

    with session_scope(session_factory) as session:
        session.add_all(
            [
                ServiceHeartbeat(
                    service_name="quant-lab-demo-loop",
                    status="submitted",
                    details={
                        "mode": "portfolio",
                        "cycle": 1,
                        "action": "1/2 submitted",
                        "symbol_count": 2,
                        "submitted_symbol_count": 1,
                        "actionable_symbol_count": 2,
                        "active_position_symbol_count": 1,
                        "response_count": 1,
                        "warning_count": 0,
                    },
                    created_at=_utc("2026-03-25T12:00:00+00:00"),
                ),
                ServiceHeartbeat(
                    service_name="quant-lab-demo-loop",
                    status="warning",
                    details={
                        "mode": "portfolio",
                        "cycle": 2,
                        "action": "0/2 submitted",
                        "symbol_count": 2,
                        "submitted_symbol_count": 0,
                        "actionable_symbol_count": 1,
                        "active_position_symbol_count": 2,
                        "response_count": 0,
                        "warning_count": 1,
                    },
                    created_at=_utc("2026-03-25T12:05:00+00:00"),
                ),
            ]
        )

    reconcile = {
        "mode": "portfolio",
        "symbol_states": {
            "ETH-USDT-SWAP": {
                "position": {"contracts": 6.0, "side": 1},
                "signal": {"desired_side": 1},
                "plan": {"action": "hold", "target_contracts": 5.0},
                "checks": {"leverage_match": True, "size_match": False, "protective_stop_ready": True},
            },
            "BTC-USDT-SWAP": {
                "position": {"contracts": 10.0, "side": 1},
                "signal": {"desired_side": 1},
                "plan": {"action": "open", "target_contracts": 8.0},
                "checks": {"leverage_match": True, "size_match": True, "protective_stop_ready": True},
            },
        },
    }

    payload = build_demo_visuals_payload(session_factory=session_factory, reconcile=reconcile)

    assert payload["summary"]["mode"] == "portfolio"
    assert payload["summary"]["symbol_count"] == 2
    assert payload["summary"]["current_contracts"] == 16.0
    assert payload["summary"]["target_contracts"] == 13.0
    assert payload["chart"]["mode"] == "portfolio"
    assert payload["chart"]["latest_target_contracts"] == 1
    assert payload["chart"]["latest_live_contracts"] == 2
    assert payload["per_symbol_states"][0]["symbol"] == "BTC-USDT-SWAP"
    assert payload["per_symbol_states"][1]["symbol"] == "ETH-USDT-SWAP"
    assert payload["recent_events"][0]["mode"] == "portfolio"


def test_build_client_snapshot_portfolio_falls_back_to_cached_state_when_live_fetch_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _portfolio_runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    (config.storage.data_dir / "demo_executor_state.json").write_text(
        """
{
  "symbols": {
    "BTC-USDT-SWAP": {
      "last_plan": {
        "action": "open",
        "desired_side": 1,
        "current_side": 0,
        "current_contracts": 0.0,
        "target_contracts": 8.0,
        "latest_price": 71344.0,
        "position_mode": "net_mode",
        "warnings": []
      },
      "last_signal": {
        "signal_time": "2026-03-25T08:00:00+00:00",
        "effective_time": "2026-03-25T08:01:00+00:00",
        "latest_price": 71344.0,
        "desired_side": 1,
        "ready": true
      }
    },
    "ETH-USDT-SWAP": {
      "last_plan": {
        "action": "hold",
        "desired_side": 1,
        "current_side": 1,
        "current_contracts": 5.0,
        "target_contracts": 4.0,
        "latest_price": 3820.0,
        "position_mode": "net_mode",
        "warnings": []
      },
      "last_signal": {
        "signal_time": "2026-03-25T08:00:00+00:00",
        "effective_time": "2026-03-25T08:01:00+00:00",
        "latest_price": 3820.0,
        "desired_side": 1,
        "ready": true
      }
    }
  }
}
        """.strip(),
        encoding="utf-8",
    )

    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="warning",
                details={
                    "mode": "portfolio",
                    "cycle": 3,
                    "action": "1/2 submitted",
                    "symbol_count": 2,
                    "submitted_symbol_count": 1,
                    "actionable_symbol_count": 2,
                    "active_position_symbol_count": 1,
                    "response_count": 1,
                    "warning_count": 1,
                    "symbol_states": {
                        "BTC-USDT-SWAP": {
                            "action": "open",
                            "desired_side": 1,
                            "current_side": 0,
                            "current_contracts": 0.0,
                            "target_contracts": 8.0,
                            "latest_price": 71344.0,
                            "signal_time": "2026-03-25T08:00:00+00:00",
                            "effective_time": "2026-03-25T08:01:00+00:00",
                        },
                        "ETH-USDT-SWAP": {
                            "action": "hold",
                            "desired_side": 1,
                            "current_side": 1,
                            "current_contracts": 5.0,
                            "target_contracts": 4.0,
                            "latest_price": 3820.0,
                            "signal_time": "2026-03-25T08:00:00+00:00",
                            "effective_time": "2026-03-25T08:01:00+00:00",
                        },
                    },
                },
                created_at=_utc("2026-03-25T12:10:00+00:00"),
            )
        )

    def _boom(_config, _symbols):
        raise RuntimeError("portfolio network down")

    monkeypatch.setattr("quant_lab.cli._load_demo_portfolio_state", _boom)

    payload = build_client_snapshot(config=config, session_factory=session_factory, project_root=tmp_path)

    assert payload["snapshot_source"] == "cached_local_state"
    assert payload["live_error"] == "RuntimeError: portfolio network down"
    assert payload["reconcile"]["mode"] == "portfolio"
    assert payload["reconcile"]["summary"]["symbol_count"] == 2
    assert payload["reconcile"]["summary"]["leverage_ready_symbol_count"] == 0
    assert payload["reconcile"]["symbol_states"]["BTC-USDT-SWAP"]["plan"]["target_contracts"] == 8.0
    assert payload["reconcile"]["symbol_states"]["ETH-USDT-SWAP"]["position"]["contracts"] == 5.0
    assert payload["demo_visuals"]["summary"]["mode"] == "portfolio"
    assert payload["demo_visuals"]["summary"]["current_contracts"] == 5.0
    assert payload["demo_visuals"]["summary"]["target_contracts"] == 12.0


def test_build_autotrade_status_reports_portfolio_idle_state() -> None:
    payload = _build_autotrade_status(
        preflight={
            "demo_trading": {"mode": "submit_ready", "ready": True, "reasons": []},
            "execution_loop": {"latest_heartbeat": {"status": "idle", "created_at": "2026-03-27T12:00:00+00:00"}},
        },
        reconcile={
            "mode": "portfolio",
            "summary": {"actionable_symbol_count": 0, "active_position_symbol_count": 0},
            "symbol_states": {
                "BTC-USDT-SWAP": {
                    "plan": {"action": "hold", "reason": "Strategy is flat on the latest confirmed signal bar."},
                    "position": {"side": 0, "contracts": 0},
                    "checks": {"leverage_match": True, "protective_stop_ready": True, "open_orders_idle": True},
                },
                "ETH-USDT-SWAP": {
                    "plan": {"action": "hold", "reason": "Strategy is flat on the latest confirmed signal bar."},
                    "position": {"side": 0, "contracts": 0},
                    "checks": {"leverage_match": True, "protective_stop_ready": True, "open_orders_idle": True},
                },
            },
        },
        demo_visuals={"summary": {"mode": "portfolio", "last_event_time": "2026-03-27T12:00:00+00:00"}},
        snapshot_source="live_okx",
        live_error=None,
    )

    assert payload["state_code"] == "idle"
    assert payload["will_submit_now"] is False
    assert payload["headline"] == "当前没有可执行信号"
    assert "BTC-USDT-SWAP" in payload["reasons"][0]


def test_build_autotrade_status_reports_portfolio_blockers() -> None:
    payload = _build_autotrade_status(
        preflight={
            "demo_trading": {"mode": "submit_ready", "ready": True, "reasons": []},
            "execution_loop": {"latest_heartbeat": {"status": "warning", "created_at": "2026-03-27T12:00:00+00:00"}},
        },
        reconcile={
            "mode": "portfolio",
            "summary": {"actionable_symbol_count": 1, "active_position_symbol_count": 1},
            "symbol_states": {
                "BTC-USDT-SWAP": {
                    "plan": {"action": "open", "instructions": [{"ordType": "market"}]},
                    "position": {"side": 1, "contracts": 3},
                    "checks": {"leverage_match": False, "protective_stop_ready": True, "open_orders_idle": True},
                }
            },
        },
        demo_visuals={"summary": {"mode": "portfolio", "last_event_time": "2026-03-27T12:00:00+00:00"}},
        snapshot_source="live_okx",
        live_error=None,
    )

    assert payload["state_code"] == "blocked_exchange"
    assert payload["will_submit_now"] is False
    assert "BTC-USDT-SWAP：杠杆未对齐" in payload["reasons"]
    assert "杠杆对齐" in payload["next_hint"]


def test_build_autotrade_status_reports_submit_blocked() -> None:
    payload = _build_autotrade_status(
        preflight={
            "demo_trading": {
                "mode": "submit_blocked",
                "ready": False,
                "reasons": ["trading.allow_order_placement=false"],
            },
            "execution_loop": {"latest_heartbeat": {"status": "idle", "created_at": "2026-03-27T12:00:00+00:00"}},
        },
        reconcile={"plan": {"action": "open", "instructions": [{"ordType": "market"}]}, "position": {"side": 0, "contracts": 0}},
        demo_visuals={"summary": {"mode": "single", "last_event_time": "2026-03-27T12:00:00+00:00"}},
        snapshot_source="live_okx",
        live_error=None,
    )

    assert payload["state_code"] == "blocked_config"
    assert payload["can_submit"] is False
    assert payload["headline"] == "自动下单未就绪"
    assert payload["reasons"] == ["当前运行未开启自动下单。"]


def _utc(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)


def _runtime_config(tmp_path: Path) -> AppConfig:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    report_dir = data_dir / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    return AppConfig(
        instrument=InstrumentConfig(symbol="BTC-USDT-SWAP"),
        strategy=StrategyConfig(name="ema_trend_4h"),
        storage=StorageConfig(data_dir=data_dir, raw_dir=raw_dir, report_dir=report_dir),
        database=DatabaseConfig(url=f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"),
        service=ServiceConfig(heartbeat_interval_seconds=3600, report_stale_minutes=180),
    )


def _portfolio_runtime_config(tmp_path: Path) -> AppConfig:
    config = _runtime_config(tmp_path)
    config.portfolio = PortfolioConfig(symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"])
    return config
