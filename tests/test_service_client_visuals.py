from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from quant_lab.config import AppConfig, DatabaseConfig, InstrumentConfig, OkxConfig, PortfolioConfig, ServiceConfig, StorageConfig, StrategyConfig
from quant_lab.service.client_ops import build_client_snapshot
from quant_lab.service.database import AlertEvent, ServiceHeartbeat, init_db, make_session_factory, session_scope
from quant_lab.service.demo_runtime import (
    autotrade_status_label,
    build_autotrade_status,
    build_client_checks_summary,
    build_client_exchange_summary,
    build_client_headline_summary,
    build_client_plan_summary,
    build_client_symbol_summary,
    build_client_warning_summary,
    build_demo_visuals_payload,
    client_side_label,
    demo_history_status_label,
)


def test_autotrade_status_label_uses_shared_runtime_mapping() -> None:
    assert autotrade_status_label("submitted") == "已提交"
    assert autotrade_status_label("duplicate") == "已跳过重复计划"
    assert autotrade_status_label("plan_only") == "仅演练"
    assert autotrade_status_label("error") == "错误"


def test_client_side_label_uses_shared_runtime_mapping() -> None:
    assert client_side_label(1) == "做多"
    assert client_side_label(-1) == "做空"
    assert client_side_label(0) == "空仓"
    assert client_side_label(None) == "--"


def test_demo_history_status_label_prefers_serialized_status_label() -> None:
    assert demo_history_status_label({"status": "warning", "status_label": "serialized-warning"}) == "serialized-warning"
    assert demo_history_status_label({"status": "warning"}) == autotrade_status_label("warning")
    assert demo_history_status_label(None) == "--"


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
    assert payload["recent_events"][0]["action_label"] == "持有"
    assert payload["recent_events"][0]["status_label"] == "警告"
    assert payload["recent_events"][0]["current_side_label"] == "做空"
    assert payload["recent_events"][0]["desired_side_label"] == "做空"
    assert payload["recent_alerts"][0]["event_key"] == "demo_order_submitted"
    assert payload["recent_alerts"][0]["status_label"] == "已发送"
    assert payload["recent_alerts"][0]["delivered_at"].endswith("+00:00")


def test_build_demo_visuals_payload_summary_prefers_serialized_history_status_label(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{(tmp_path / 'quant_lab.db').as_posix()}"
    init_db(database_url)
    session_factory = make_session_factory(database_url)

    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="error",
                details={
                    "cycle": 1,
                    "action": "hold",
                    "desired_side": 0,
                    "current_side": 0,
                    "target_contracts": 0.0,
                    "current_contracts": 0.0,
                    "submitted": False,
                    "response_count": 0,
                    "warning_count": 0,
                    "latest_price": 71000.0,
                },
                created_at=_utc("2026-03-25T12:10:00+00:00"),
            )
        )

    original = build_demo_visuals_payload.__globals__["_demo_visual_heartbeat_point"]

    def _fake_demo_visual_heartbeat_point(row):
        point = original(row)
        point["status_label"] = "serialized-error"
        return point

    monkeypatch.setitem(
        build_demo_visuals_payload.__globals__,
        "_demo_visual_heartbeat_point",
        _fake_demo_visual_heartbeat_point,
    )

    payload = build_demo_visuals_payload(
        session_factory=session_factory,
        reconcile={
            "mode": "single",
            "position": {"side": 0, "contracts": 0.0},
            "plan": {"target_contracts": 0.0},
            "signal": {"desired_side": 0},
        },
    )

    assert payload["summary"]["last_status"] == "error"
    assert payload["recent_events"][0]["status_label"] == "serialized-error"
    assert payload["summary"]["last_status_label"] == "serialized-error"


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

    monkeypatch.setattr("quant_lab.service.demo_runtime.load_demo_state", _boom)

    payload = build_client_snapshot(config=config, session_factory=session_factory, project_root=tmp_path)

    assert payload["snapshot_source"] == "cached_local_state"
    assert payload["live_error"] == "RuntimeError: network down"
    assert payload["checks_summary"]["mode"] == "single"
    assert payload["checks_summary"]["cards"]["demo"]["level"] == "danger"
    assert payload["plan_summary"]["mode"] == "single"
    assert payload["symbol_summary"]["mode"] == "single"
    assert payload["warning_summary"]["has_warnings"] is True
    assert "实时抓取失败：RuntimeError: network down" in payload["warning_summary"]["messages"]
    assert payload["reconcile"]["snapshot_source"] == "cached_local_state"
    assert payload["reconcile"]["plan"]["target_contracts"] == 17.51
    assert payload["reconcile"]["position"]["contracts"] == 27.96
    assert payload["reconcile"]["signal"]["alpha_signal"]["side"] == 1
    assert payload["reconcile"]["signal"]["risk_signal"]["risk_multiplier"] == 1.0
    assert payload["demo_visuals"]["summary"]["current_contracts"] == 27.96
    assert payload["demo_visuals"]["summary"]["target_contracts"] == 17.51
    assert payload["demo_visuals"]["summary"]["last_status_label"] == "错误"
    assert payload["headline_summary"]["title"] == payload["autotrade_status"]["headline"]
    assert payload["headline_summary"]["source_label"] == "本地缓存"
    assert payload["headline_summary"]["submit"]["value"] == "不允许"


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
    assert payload["recent_events"][0]["action_label"] == "0/2 submitted"
    assert payload["recent_events"][0]["status_label"] == "警告"


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

    monkeypatch.setattr("quant_lab.service.demo_runtime.load_demo_portfolio_state", _boom)

    payload = build_client_snapshot(config=config, session_factory=session_factory, project_root=tmp_path)

    assert payload["snapshot_source"] == "cached_local_state"
    assert payload["live_error"] == "RuntimeError: portfolio network down"
    assert payload["checks_summary"]["mode"] == "portfolio"
    assert payload["checks_summary"]["cards"]["demo"]["level"] == "danger"
    assert payload["plan_summary"]["mode"] == "portfolio"
    assert payload["symbol_summary"]["mode"] == "portfolio"
    assert "实时抓取失败：RuntimeError: portfolio network down" in payload["warning_summary"]["messages"]
    assert payload["reconcile"]["mode"] == "portfolio"
    assert payload["reconcile"]["summary"]["symbol_count"] == 2
    assert payload["reconcile"]["summary"]["allocation_mode"] == "priority_risk_budget"
    assert payload["reconcile"]["summary"]["leverage_ready_symbol_count"] == 0
    assert payload["reconcile"]["symbol_states"]["BTC-USDT-SWAP"]["plan"]["target_contracts"] == 8.0
    assert payload["reconcile"]["symbol_states"]["ETH-USDT-SWAP"]["position"]["contracts"] == 5.0
    assert payload["reconcile"]["symbol_states"]["BTC-USDT-SWAP"]["signal"]["alpha_signal"]["side"] == 1
    assert payload["reconcile"]["symbol_states"]["ETH-USDT-SWAP"]["signal"]["risk_signal"]["risk_multiplier"] == 1.0
    assert payload["demo_visuals"]["summary"]["mode"] == "portfolio"
    assert payload["demo_visuals"]["summary"]["current_contracts"] == 5.0
    assert payload["demo_visuals"]["summary"]["target_contracts"] == 12.0


def test_build_client_snapshot_falls_back_to_structured_single_heartbeat_when_executor_state_is_empty(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    (config.storage.data_dir / "demo_executor_state.json").write_text("{}", encoding="utf-8")

    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="warning",
                details={
                    "mode": "single",
                    "summary": {
                        "mode": "single",
                        "cycle": 4,
                        "status": "warning",
                        "submitted": False,
                        "response_count": 0,
                        "warning_count": 1,
                    },
                    "account": {
                        "total_equity": 12_000.0,
                        "available_equity": 10_500.0,
                        "currency": "USDT",
                    },
                    "position": {
                        "side": -1,
                        "contracts": 3.0,
                        "position_mode": "net_mode",
                    },
                    "signal": {
                        "signal_time": "2026-03-25T08:00:00+00:00",
                        "effective_time": "2026-03-25T08:01:00+00:00",
                        "latest_price": 71344.0,
                        "desired_side": 1,
                        "ready": False,
                        "alpha_signal": {
                            "side": 1,
                            "score": 0.7,
                            "regime": "bull_trend",
                            "strategy_name": "ema_trend_4h",
                            "strategy_variant": "base",
                        },
                        "risk_signal": {
                            "stop_distance": 1500.0,
                            "stop_price": 69844.0,
                            "risk_multiplier": 1.0,
                        },
                    },
                    "plan": {
                        "action": "flip",
                        "reason": "structured cached heartbeat",
                        "desired_side": 1,
                        "current_side": -1,
                        "current_contracts": 3.0,
                        "target_contracts": 5.0,
                        "latest_price": 71344.0,
                        "signal_time": "2026-03-25T08:00:00+00:00",
                        "effective_time": "2026-03-25T08:01:00+00:00",
                        "position_mode": "net_mode",
                        "instructions": [],
                        "warnings": [],
                    },
                },
                created_at=_utc("2026-03-25T12:10:00+00:00"),
            )
        )

    monkeypatch.setattr("quant_lab.service.demo_runtime.load_demo_state", lambda _config: (_ for _ in ()).throw(RuntimeError("network down")))

    payload = build_client_snapshot(config=config, session_factory=session_factory, project_root=tmp_path)

    assert payload["snapshot_source"] == "cached_local_state"
    assert payload["reconcile"]["plan"]["action"] == "flip"
    assert payload["reconcile"]["plan"]["target_contracts"] == 5.0
    assert payload["reconcile"]["position"]["contracts"] == 3.0
    assert payload["reconcile"]["signal"]["alpha_signal"]["side"] == 1
    assert payload["demo_visuals"]["summary"]["current_contracts"] == 3.0
    assert payload["demo_visuals"]["chart"]["points"][-1]["target_contracts"] == 5.0


def test_build_client_snapshot_falls_back_to_flat_single_heartbeat_when_executor_state_is_empty(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    (config.storage.data_dir / "demo_executor_state.json").write_text("{}", encoding="utf-8")

    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="warning",
                details={
                    "cycle": 5,
                    "action": "flip",
                    "desired_side": 1,
                    "current_side": -1,
                    "current_contracts": 3.0,
                    "target_contracts": 5.0,
                    "latest_price": 71344.0,
                    "signal_time": "2026-03-25T08:00:00+00:00",
                    "effective_time": "2026-03-25T08:01:00+00:00",
                    "position_mode": "net_mode",
                    "response_count": 0,
                    "warning_count": 1,
                    "submitted": False,
                    "total_equity": 12_000.0,
                    "available_equity": 10_500.0,
                    "currency": "USDT",
                },
                created_at=_utc("2026-03-25T12:10:00+00:00"),
            )
        )

    monkeypatch.setattr(
        "quant_lab.service.demo_runtime.load_demo_state",
        lambda _config: (_ for _ in ()).throw(RuntimeError("network down")),
    )

    payload = build_client_snapshot(config=config, session_factory=session_factory, project_root=tmp_path)

    assert payload["snapshot_source"] == "cached_local_state"
    assert payload["reconcile"]["account"]["total_equity"] == 12_000.0
    assert payload["reconcile"]["account"]["available_equity"] == 10_500.0
    assert payload["reconcile"]["plan"]["action"] == "flip"
    assert payload["reconcile"]["plan"]["target_contracts"] == 5.0
    assert payload["reconcile"]["position"]["contracts"] == 3.0
    assert payload["reconcile"]["signal"]["alpha_signal"]["side"] == 1
    assert payload["reconcile"]["signal"]["risk_signal"]["risk_multiplier"] == 1.0
    assert payload["demo_visuals"]["summary"]["current_contracts"] == 3.0
    assert payload["demo_visuals"]["chart"]["points"][-1]["target_contracts"] == 5.0


def test_build_client_snapshot_portfolio_falls_back_to_structured_heartbeat_when_executor_state_is_empty(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _portfolio_runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    (config.storage.data_dir / "demo_executor_state.json").write_text("{}", encoding="utf-8")

    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="warning",
                details={
                    "mode": "portfolio",
                    "summary": {
                        "mode": "portfolio",
                        "cycle": 6,
                        "status": "warning",
                        "symbol_count": 2,
                        "submitted_symbol_count": 1,
                        "actionable_symbol_count": 2,
                        "active_position_symbol_count": 1,
                        "response_count": 1,
                        "warning_count": 1,
                    },
                    "account": {
                        "total_equity": 20_000.0,
                        "available_equity": 19_000.0,
                        "currency": "USDT",
                    },
                    "symbol_states": {
                        "BTC-USDT-SWAP": {
                            "summary": {"status": "submitted", "response_count": 1, "warning_count": 0},
                            "position": {"side": 0, "contracts": 0.0, "position_mode": "net_mode"},
                            "signal": {
                                "signal_time": "2026-03-25T08:00:00+00:00",
                                "effective_time": "2026-03-25T08:01:00+00:00",
                                "latest_price": 71344.0,
                                "desired_side": 1,
                                "ready": False,
                                "alpha_signal": {"side": 1, "score": 0.8, "regime": "bull_trend"},
                                "risk_signal": {"stop_distance": 1500.0, "stop_price": 69844.0, "risk_multiplier": 1.0},
                            },
                            "plan": {
                                "action": "open",
                                "reason": "structured portfolio heartbeat",
                                "desired_side": 1,
                                "current_side": 0,
                                "current_contracts": 0.0,
                                "target_contracts": 8.0,
                                "latest_price": 71344.0,
                                "signal_time": "2026-03-25T08:00:00+00:00",
                                "effective_time": "2026-03-25T08:01:00+00:00",
                                "position_mode": "net_mode",
                                "instructions": [],
                                "warnings": [],
                            },
                        },
                        "ETH-USDT-SWAP": {
                            "summary": {"status": "warning", "response_count": 0, "warning_count": 1},
                            "position": {"side": 1, "contracts": 5.0, "position_mode": "net_mode"},
                            "signal": {
                                "signal_time": "2026-03-25T08:00:00+00:00",
                                "effective_time": "2026-03-25T08:01:00+00:00",
                                "latest_price": 3820.0,
                                "desired_side": 1,
                                "ready": False,
                                "alpha_signal": {"side": 1, "score": 0.5, "regime": "range"},
                                "risk_signal": {"stop_distance": 120.0, "stop_price": 3700.0, "risk_multiplier": 1.0},
                            },
                            "plan": {
                                "action": "hold",
                                "reason": "structured portfolio heartbeat",
                                "desired_side": 1,
                                "current_side": 1,
                                "current_contracts": 5.0,
                                "target_contracts": 4.0,
                                "latest_price": 3820.0,
                                "signal_time": "2026-03-25T08:00:00+00:00",
                                "effective_time": "2026-03-25T08:01:00+00:00",
                                "position_mode": "net_mode",
                                "instructions": [],
                                "warnings": [],
                            },
                        },
                    },
                },
                created_at=_utc("2026-03-25T12:10:00+00:00"),
            )
        )

    monkeypatch.setattr("quant_lab.service.demo_runtime.load_demo_portfolio_state", lambda _config, _symbols: (_ for _ in ()).throw(RuntimeError("portfolio network down")))

    payload = build_client_snapshot(config=config, session_factory=session_factory, project_root=tmp_path)

    assert payload["snapshot_source"] == "cached_local_state"
    assert payload["reconcile"]["summary"]["symbol_count"] == 2
    assert payload["reconcile"]["symbol_states"]["BTC-USDT-SWAP"]["plan"]["target_contracts"] == 8.0
    assert payload["reconcile"]["symbol_states"]["ETH-USDT-SWAP"]["position"]["contracts"] == 5.0
    assert payload["demo_visuals"]["summary"]["mode"] == "portfolio"
    assert payload["demo_visuals"]["chart"]["points"][-1]["target_contracts"] == 2
    assert payload["demo_visuals"]["chart"]["points"][-1]["current_contracts"] == 1


def test_build_client_snapshot_portfolio_falls_back_to_flat_heartbeat_when_executor_state_is_empty(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _portfolio_runtime_config(tmp_path)
    init_db(config.database.url)
    session_factory = make_session_factory(config.database.url)

    (config.storage.data_dir / "demo_executor_state.json").write_text("{}", encoding="utf-8")

    with session_scope(session_factory) as session:
        session.add(
            ServiceHeartbeat(
                service_name="quant-lab-demo-loop",
                status="warning",
                details={
                    "mode": "portfolio",
                    "cycle": 7,
                    "action": "1/2 submitted",
                    "symbol_count": 2,
                    "submitted_symbol_count": 1,
                    "actionable_symbol_count": 2,
                    "active_position_symbol_count": 1,
                    "response_count": 1,
                    "warning_count": 1,
                    "total_equity": 20_000.0,
                    "available_equity": 19_000.0,
                    "currency": "USDT",
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
                            "position_mode": "net_mode",
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
                            "position_mode": "net_mode",
                        },
                    },
                },
                created_at=_utc("2026-03-25T12:10:00+00:00"),
            )
        )

    monkeypatch.setattr(
        "quant_lab.service.demo_runtime.load_demo_portfolio_state",
        lambda _config, _symbols: (_ for _ in ()).throw(RuntimeError("portfolio network down")),
    )

    payload = build_client_snapshot(config=config, session_factory=session_factory, project_root=tmp_path)

    assert payload["snapshot_source"] == "cached_local_state"
    assert payload["reconcile"]["summary"]["symbol_count"] == 2
    assert payload["reconcile"]["symbol_states"]["BTC-USDT-SWAP"]["plan"]["target_contracts"] == 8.0
    assert payload["reconcile"]["symbol_states"]["ETH-USDT-SWAP"]["position"]["contracts"] == 5.0
    assert payload["reconcile"]["symbol_states"]["BTC-USDT-SWAP"]["signal"]["alpha_signal"]["side"] == 1
    assert payload["reconcile"]["symbol_states"]["ETH-USDT-SWAP"]["signal"]["risk_signal"]["risk_multiplier"] == 1.0
    assert payload["demo_visuals"]["summary"]["mode"] == "portfolio"
    assert payload["demo_visuals"]["summary"]["current_contracts"] == 5.0
    assert payload["demo_visuals"]["summary"]["target_contracts"] == 12.0
    assert payload["demo_visuals"]["chart"]["points"][-1]["target_contracts"] == 2
    assert payload["demo_visuals"]["chart"]["points"][-1]["current_contracts"] == 1


def test_build_autotrade_status_reports_portfolio_idle_state() -> None:
    payload = build_autotrade_status(
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
    payload = build_autotrade_status(
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
    payload = build_autotrade_status(
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


def test_build_headline_summary_reflects_autotrade_and_snapshot_state() -> None:
    payload = build_client_headline_summary(
        preflight={
            "demo_trading": {"mode": "submit_ready", "ready": True},
            "execution_loop": {"latest_heartbeat": {"status": "warning", "created_at": "2026-03-27T12:00:00+00:00"}},
        },
        autotrade_status={
            "headline": "自动下单被账户或交易所状态阻塞",
            "latest_loop_status": "warning",
            "latest_loop_status_label": autotrade_status_label("warning"),
            "will_submit_now": False,
        },
        demo_visuals={"summary": {"last_event_time": "2026-03-27T12:00:00+00:00"}},
        snapshot_source="cached_local_state",
    )

    assert payload["level"] == "warn"
    assert payload["pill_text"] == "通道已开，等待信号"
    assert payload["title"] == "自动下单被账户或交易所状态阻塞"
    assert payload["mode_label"] == "可提交"
    assert payload["source_label"] == "本地缓存"
    assert payload["submit"]["value"] == "允许"
    assert payload["actionable"]["value"] == "无动作"
    assert payload["loop"]["value"] == "警告"


def test_build_headline_summary_prefers_serialized_loop_status_label() -> None:
    payload = build_client_headline_summary(
        preflight={
            "demo_trading": {"mode": "submit_ready", "ready": True},
            "execution_loop": {
                "latest_heartbeat": {
                    "status": "warning",
                    "status_label": "heartbeat-warning",
                    "created_at": "2026-03-27T12:00:00+00:00",
                }
            },
        },
        autotrade_status={
            "headline": "blocked",
            "latest_loop_status": "warning",
            "latest_loop_status_label": "serialized-warning",
            "will_submit_now": False,
        },
        demo_visuals={"summary": {"last_event_time": "2026-03-27T12:00:00+00:00"}},
        snapshot_source="cached_local_state",
    )

    assert payload["loop"]["status"] == "warning"
    assert payload["loop"]["value"] == "serialized-warning"


def test_build_checks_summary_for_single_mode() -> None:
    payload = build_client_checks_summary(
        preflight={
            "demo_trading": {"mode": "submit_ready", "ready": True},
        },
        reconcile={
            "checks": {
                "leverage_match": True,
                "size_match": False,
                "protective_stop_ready": False,
            }
        },
    )

    assert payload["mode"] == "single"
    assert payload["cards"]["demo"]["level"] == "ok"
    assert payload["cards"]["leverage"]["level"] == "ok"
    assert payload["cards"]["size"]["level"] == "warn"
    assert payload["cards"]["stop"]["level"] == "danger"


def test_build_checks_summary_for_portfolio_mode() -> None:
    payload = build_client_checks_summary(
        preflight={
            "demo_trading": {"mode": "submit_blocked", "ready": False},
        },
        reconcile={
            "mode": "portfolio",
            "symbol_states": {
                "BTC-USDT-SWAP": {
                    "position": {"side": 1, "contracts": 2},
                    "checks": {
                        "leverage_match": True,
                        "size_match": False,
                        "protective_stop_ready": True,
                    },
                },
                "ETH-USDT-SWAP": {
                    "position": {"side": 0, "contracts": 0},
                    "checks": {
                        "leverage_match": False,
                        "size_match": True,
                        "protective_stop_ready": False,
                    },
                },
            },
        },
    )

    assert payload["mode"] == "portfolio"
    assert payload["counts"] == {
        "total": 2,
        "active": 1,
        "leverage_ready": 1,
        "size_ready": 1,
        "stop_ready": 1,
    }
    assert payload["cards"]["demo"]["level"] == "danger"
    assert payload["cards"]["leverage"]["value"] == "1/2"
    assert payload["cards"]["size"]["value"] == "1/2"
    assert payload["cards"]["stop"]["value"] == "1/1"


def test_build_plan_summary_for_single_and_portfolio_modes() -> None:
    single_payload = build_client_plan_summary(
        reconcile={
            "account": {"total_equity": 12000.0, "available_equity": 10500.0, "currency": "USDT"},
            "position": {"side": -1, "contracts": 3.0},
            "signal": {"desired_side": 1, "latest_price": 71344.0},
            "plan": {"action": "flip", "target_contracts": 5.0, "reason": "structured cached heartbeat"},
        }
    )
    portfolio_payload = build_client_plan_summary(
        reconcile={
            "mode": "portfolio",
            "account": {"total_equity": 20000.0, "available_equity": 19000.0, "currency": "USDT"},
            "summary": {
                "allocation_mode": "priority_risk_budget",
                "symbol_count": 2,
                "actionable_symbol_count": 1,
                "active_position_symbol_count": 1,
                "requested_total_risk_pct": 3.5,
                "allocated_total_risk_pct": 2.2,
                "portfolio_total_risk_cap_pct": 4.0,
                "same_direction_risk_cap_pct": 3.0,
                "budgeted_equity_total": 15000.0,
                "budgeted_symbol_count": 2,
                "per_symbol_planning_equity": 7500.0,
                "planning_equity_reference": 19000.0,
            },
        }
    )

    assert single_payload["mode"] == "single"
    assert "账户权益：12000.00 USDT" in single_payload["items"]
    assert "当前持仓：做空 | 3.00 张" in single_payload["items"]
    assert "计划动作：flip" in single_payload["items"]
    assert portfolio_payload["mode"] == "portfolio"
    assert "allocation_mode：priority_risk_budget" in portfolio_payload["items"]
    assert "组合标的数：2" in portfolio_payload["items"]
    assert "requested_total_risk_pct：3.50%" in portfolio_payload["items"]
    assert portfolio_payload["items"][-1] == "最近循环模式：组合模式"


def test_build_symbol_summary_for_single_and_portfolio_modes() -> None:
    single_payload = build_client_symbol_summary(
        reconcile={
            "instrument": "BTC-USDT-SWAP",
            "position": {"side": -1, "contracts": 3.0},
            "signal": {"desired_side": 1},
            "plan": {"action": "flip", "target_contracts": 5.0, "reason": "structured cached heartbeat"},
        }
    )
    portfolio_payload = build_client_symbol_summary(
        reconcile={
            "mode": "portfolio",
            "symbol_states": {
                "BTC-USDT-SWAP": {
                    "position": {"side": 0, "contracts": 0.0},
                    "signal": {"desired_side": 1},
                    "plan": {"action": "open", "target_contracts": 8.0},
                    "checks": {
                        "leverage_match": True,
                        "size_match": False,
                        "protective_stop_ready": True,
                    },
                    "planning_account": {"available_equity": 10000.0, "currency": "USDT"},
                    "portfolio_risk": {
                        "base_risk_fraction": 0.02,
                        "scaled_risk_fraction": 0.015,
                        "requested_target_contracts": 10.0,
                        "final_target_contracts": 8.0,
                        "applied_scale": 0.8,
                        "reasons": ["risk scaled"],
                    },
                    "router_decision": {
                        "ready": False,
                        "route_key": "trend_follow",
                        "reasons": ["route not ready"],
                        "display": {"route_label": "bull trend"},
                    },
                }
            },
        }
    )

    assert single_payload["mode"] == "single"
    assert single_payload["title"] == "当前标的状态"
    assert single_payload["cards"][0]["title"] == "BTC-USDT-SWAP"
    assert "当前方向：做空 | 策略方向：做多" in single_payload["cards"][0]["lines"]
    assert portfolio_payload["mode"] == "portfolio"
    assert portfolio_payload["title"] == "组合标的状态"
    assert portfolio_payload["cards"][0]["title"] == "BTC-USDT-SWAP"
    assert any("规划权益：10000.00 USDT | 路由：bull trend（已阻塞）" in line for line in portfolio_payload["cards"][0]["lines"])
    assert any("申请风险：2.00% | 分配风险：1.50%" in line for line in portfolio_payload["cards"][0]["lines"])
    assert any("组合风控：risk scaled | 路由阻塞：bull trend | 路由未就绪。" in line for line in portfolio_payload["cards"][0]["lines"])


def test_build_exchange_summary_collects_runtime_connectivity_and_errors() -> None:
    payload = build_client_exchange_summary(
        preflight={
            "okx_connectivity": {
                "profile": "okx-demo",
                "proxy_url": "http://127.0.0.1:7897",
                "egress_ip": "1.2.3.4",
                "notes": ["proxy in use"],
            }
        },
        reconcile={
            "account": {"account_mode": "cross"},
            "position": {"position_mode": "net_mode"},
            "exchange": {
                "pending_orders": {"count": 2},
                "pending_algo_orders": {"count": 1},
                "leverage": {"values": ["3", "5"]},
                "protection_stop": {"ready": True},
            },
        },
        snapshot_source="cached_local_state",
        live_error="RuntimeError: network down",
    )

    assert payload["items"][0] == {"label": "数据来源", "value": "本地缓存"}
    assert payload["items"][1] == {"label": "账户模式", "value": "cross"}
    assert {"label": "普通挂单数", "value": "2"} in payload["items"]
    assert {"label": "条件单数", "value": "1"} in payload["items"]
    assert {"label": "杠杆值", "value": "3, 5"} in payload["items"]
    assert {"label": "保护止损", "value": "已就绪"} in payload["items"]
    assert {"label": "OKX Profile", "value": "okx-demo"} in payload["items"]
    assert {"label": "代理", "value": "http://127.0.0.1:7897"} in payload["items"]
    assert {"label": "出口 IP", "value": "1.2.3.4"} in payload["items"]
    assert {"label": "连接提示", "value": "proxy in use"} in payload["items"]
    assert {"label": "实时错误", "value": "RuntimeError: network down"} in payload["items"]


def test_build_warning_summary_deduplicates_cross_layer_messages() -> None:
    payload = build_client_warning_summary(
        preflight={
            "demo_trading": {
                "reasons": [
                    "trading.allow_order_placement=false",
                    "missing OKX_API_KEY",
                ]
            }
        },
        reconcile={
            "warnings": [
                "杠杆未对齐",
                "缺少保护止损",
            ]
        },
        autotrade_status={
            "blocking_reasons": [
                "杠杆未对齐",
                "缺少保护止损",
            ]
        },
        live_error="RuntimeError: network down",
    )

    assert payload["has_warnings"] is True
    assert payload["count"] == 5
    assert payload["messages"] == [
        "当前运行未开启自动下单。",
        "缺少 OKX_API_KEY。",
        "杠杆未对齐",
        "缺少保护止损",
        "实时抓取失败：RuntimeError: network down",
    ]
    assert payload["items"][0]["source"] == "preflight"
    assert payload["items"][-1]["source"] == "live_error"


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
        okx=OkxConfig(use_demo=True),
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
