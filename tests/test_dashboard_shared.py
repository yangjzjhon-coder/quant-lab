from __future__ import annotations

from quant_lab.service.dashboard_shared import (
    render_shared_dashboard_base_js,
    render_shared_report_metrics_js,
    render_shared_visual_reports_js,
)


def test_render_shared_dashboard_base_js_exposes_common_helpers() -> None:
    js = render_shared_dashboard_base_js()

    assert "const byId = (id) => document.getElementById(id);" in js
    assert "const $ = byId;" in js
    assert "function apiErrorPayload(error)" in js
    assert "function apiErrorSummary(error)" in js
    assert "function buildApiError(payload, status, path)" in js
    assert "async function requestJson(path, options = {})" in js
    assert "function heartbeatSummary(heartbeat)" in js


def test_render_shared_visual_reports_js_exposes_configurable_renderer() -> None:
    js = render_shared_visual_reports_js()

    assert 'function renderVisualReports(payload)' in js
    assert 'globalThis.VISUAL_REPORTS_NOTE_TEXT' in js
    assert 'globalThis.VISUAL_REPORTS_KIND_CLASS' in js
    assert 'globalThis.VISUAL_REPORTS_MULTI_CYCLE_KIND' in js
    assert 'const kind = useMultiCycleKind && item.kind' in js


def test_render_shared_report_metrics_js_exposes_optional_win_rate_formatting() -> None:
    js = render_shared_report_metrics_js()

    assert 'function formatMetricLines(metrics, options = {})' in js
    assert 'const includeWinRate = options.includeWinRate === true;' in js
    assert 'if (includeWinRate && num(payload.win_rate_pct, null) !== null)' in js
