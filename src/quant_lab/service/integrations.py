from __future__ import annotations

from typing import Any

from quant_lab.config import AppConfig
from quant_lab.service.market_data import build_market_data_status
from quant_lab.service.research_agent import build_research_agent_status
from quant_lab.service.research_ai import build_research_ai_status


def build_integration_overview(*, config: AppConfig, probe: bool = False) -> dict[str, Any]:
    market_data = build_market_data_status(config=config, probe=probe)
    research_ai = build_research_ai_status(config=config, probe=probe)
    research_agent = build_research_agent_status(config=config, probe=probe)
    statuses = {
        "market_data": market_data,
        "research_ai": research_ai,
        "research_agent": research_agent,
    }
    ready_count = sum(1 for payload in statuses.values() if payload.get("ready") is True)
    configured_count = sum(1 for payload in statuses.values() if payload.get("configured") is True)
    return {
        "statuses": statuses,
        "summary": {
            "ready_count": ready_count,
            "configured_count": configured_count,
            "total": len(statuses),
            "probe_enabled": bool(probe),
        },
    }
