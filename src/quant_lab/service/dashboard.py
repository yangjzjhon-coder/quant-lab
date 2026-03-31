from __future__ import annotations

import html as html_lib

from fastapi.responses import HTMLResponse

from quant_lab.config import AppConfig, configured_symbols
from quant_lab.service.dashboard_shared import (
    render_shared_dashboard_base_js,
    render_shared_report_metrics_js,
    render_shared_visual_reports_js,
)

NO_CACHE_HTML_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


def render_runtime_dashboard(
    config: AppConfig,
    *,
    initial_visual_reports_note: str = "正在加载可视化回测报表...",
    initial_visual_reports_html: str = '<div class="empty">正在加载可视化回测报表...</div>',
    initial_portfolio_sleeves_html: str = '<div class="empty">正在加载组合子报表...</div>',
) -> HTMLResponse:
    symbols = " / ".join(configured_symbols(config))
    mode = "组合模式" if len(configured_symbols(config)) > 1 else "单标的模式"
    strategy = config.strategy.name
    refresh_ms = max(config.service.heartbeat_interval_seconds, 15) * 1000
    html = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Cache-Control" content="no-store, no-cache, must-revalidate, max-age=0">
<meta http-equiv="Pragma" content="no-cache">
<meta http-equiv="Expires" content="0">
<title>quant-lab 运行总览</title>
<style>
:root{
  --bg:#f4efe6;
  --panel:#fffdfa;
  --line:rgba(17,24,39,.10);
  --text:#14212b;
  --muted:#63707b;
  --accent:#0a6c74;
  --accent-2:#d97b29;
  --danger:#b94b1f;
  --shadow:0 18px 42px rgba(15,23,42,.06);
}
*{box-sizing:border-box}
body{
  margin:0;
  padding:24px;
  background:linear-gradient(180deg,#f1ece2 0%,#faf7f2 360px);
  color:var(--text);
  font:14px/1.65 "Segoe UI","PingFang SC","Noto Sans SC",sans-serif;
}
main{max-width:1440px;margin:0 auto;display:grid;gap:16px}
section{
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:22px;
  padding:20px;
  box-shadow:var(--shadow);
}
h1,h2,h3{margin:0 0 10px}
.muted,.note,small{color:var(--muted)}
.hero{display:flex;align-items:flex-end;justify-content:space-between;gap:18px;flex-wrap:wrap}
.hero h1{font-size:34px}
code{
  padding:2px 8px;
  border-radius:999px;
  background:#edf5f6;
  font-size:12px;
}
a{color:var(--accent)}
.cards,.toolbar,.report-grid,.feed,.dual{display:grid;gap:12px}
.cards{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}
.toolbar{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}
.dual{grid-template-columns:repeat(2,minmax(0,1fr))}
.card,.feed-item,.report-card{
  border:1px solid var(--line);
  border-radius:18px;
  background:rgba(255,255,255,.86);
  padding:16px;
}
.card strong{
  display:block;
  font-size:1.45rem;
  margin-bottom:6px;
}
.toolbar a,.toolbar button,.report-card a{
  display:inline-flex;
  min-height:44px;
  align-items:center;
  justify-content:center;
  border:0;
  border-radius:14px;
  text-decoration:none;
  color:#fff;
  font-weight:700;
  cursor:pointer;
}
.toolbar a,.toolbar button.primary{background:linear-gradient(135deg,#0a6c74,#0f8d96)}
.toolbar button.alt{background:linear-gradient(135deg,#b96b18,#d5842d)}
.toolbar button.warn{background:linear-gradient(135deg,#98491e,#c25b25)}
.report-grid{grid-template-columns:repeat(auto-fit,minmax(360px,1fr))}
.report-card{display:grid;gap:12px}
.report-card.primary{grid-column:1/-1}
.report-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap}
.report-tags{display:flex;flex-wrap:wrap;gap:8px}
.tag{
  display:inline-flex;
  align-items:center;
  padding:6px 10px;
  border-radius:999px;
  background:#edf5f6;
  color:var(--accent);
  font-size:12px;
  font-weight:700;
}
.tag.warn{background:#fff1e2;color:#b96b18}
.report-preview{
  border:1px solid var(--line);
  border-radius:14px;
  overflow:hidden;
  background:#fff;
  min-height:420px;
}
.report-card.primary .report-preview{min-height:680px}
.report-preview iframe{width:100%;height:420px;border:0}
.report-card.primary .report-preview iframe{height:680px}
.feed{max-height:380px;overflow:auto}
.empty{
  padding:24px 16px;
  border:1px dashed var(--line);
  border-radius:16px;
  text-align:center;
  color:var(--muted);
}
pre{
  margin:0;
  padding:16px;
  border-radius:18px;
  border:1px solid #203140;
  background:#11212c;
  color:#e6f0f2;
  max-height:360px;
  overflow:auto;
  font:12px/1.55 "IBM Plex Mono","Consolas",monospace;
}
@media (max-width:1120px){
  .dual{grid-template-columns:1fr}
  .report-card.primary .report-preview,
  .report-card.primary .report-preview iframe{min-height:460px;height:460px}
}
</style>
</head>
<body>
<main>
<section>
  <div class="hero">
    <div>
      <div class="muted">运行总览</div>
      <h1>__SYMBOLS__</h1>
      <div class="note">策略 <code>__STRATEGY__</code>，当前运行在 <strong>__MODE__</strong>。</div>
    </div>
    <div class="toolbar">
      <a href="/client" target="_blank" rel="noreferrer">打开交易客户端</a>
      <a href="/reports/backtest" target="_blank" rel="noreferrer">打开当前回测报表</a>
      <a href="/reports/sweep" target="_blank" rel="noreferrer">打开参数扫描报表</a>
    </div>
  </div>
</section>

<section>
  <div class="cards">
    <div class="card">
      <small>演示交易模式</small>
      <strong id="card-demo-mode">--</strong>
      <div class="note" id="card-demo-mode-note">--</div>
    </div>
    <div class="card">
      <small>告警通道</small>
      <strong id="card-alerts-ready">--</strong>
      <div class="note" id="card-alerts-note">--</div>
    </div>
    <div class="card">
      <small>执行循环</small>
      <strong id="card-loop-status">--</strong>
      <div class="note" id="card-loop-note">--</div>
    </div>
    <div class="card">
      <small>刷新节奏</small>
      <strong id="meta-refresh">--</strong>
      <div class="note" id="meta-updated">--</div>
    </div>
  </div>
</section>

<section>
  <div>
    <h2>策略可视化回测</h2>
    <div class="note">主页直接嵌入多周期 K 线回放、开平仓标记、止损线和逻辑复盘。当前主回测优先置顶，其余策略或组合报表按最近产物补充展示。</div>
  </div>
  <small id="visual-reports-note">__INITIAL_VISUAL_REPORTS_NOTE__</small>
  <div id="visual-reports-feed" class="report-grid">
    __INITIAL_VISUAL_REPORTS_HTML__
  </div>
</section>

<section>
  <div>
    <h2>组合子报表</h2>
    <div class="note">如果当前是组合模式，这里会展示每个标的的子报表摘要与对应回测入口。</div>
  </div>
  <div id="portfolio-sleeves-feed" class="feed">
    __INITIAL_PORTFOLIO_SLEEVES_HTML__
  </div>
</section>

<section class="dual">
  <div>
    <h2>项目任务</h2>
    <div class="toolbar">
      <button id="task-backtest" class="primary">运行回测</button>
      <button id="task-report" class="alt">生成报表</button>
      <button id="task-sweep" class="alt">参数扫描</button>
      <button id="task-research" class="warn">运行研究</button>
    </div>
    <div id="project-task-feed" class="feed">
      <div class="empty">还没有任务执行记录。</div>
    </div>
  </div>
  <div>
    <h2>任务输出</h2>
    <small id="project-task-status">空闲</small>
    <pre id="project-task-output">当前还没有触发新的项目任务。</pre>
  </div>
</section>

<section class="dual">
  <div>
    <h2>最近告警</h2>
    <div id="alerts-feed" class="feed"><div class="empty">正在加载告警...</div></div>
  </div>
  <div>
    <h2>最近心跳</h2>
    <div id="heartbeats-feed" class="feed"><div class="empty">正在加载心跳...</div></div>
  </div>
</section>

<section class="dual">
  <div>
    <h2>运行前检查</h2>
    <div class="note">调试 JSON，字段名保留英文用于排障。</div>
    <pre id="preflight-json">正在加载...</pre>
  </div>
  <div>
    <h2>最新运行快照</h2>
    <div class="note">调试 JSON，字段名保留英文用于排障。</div>
    <pre id="snapshot-json">正在加载...</pre>
  </div>
</section>
</main>

<script>
__SHARED_DASHBOARD_BASE_JS__
__SHARED_REPORT_METRICS_JS__
globalThis.VISUAL_REPORTS_NOTE_TEXT = "首个报表作为主页主图展示，其余最近策略回测继续在下方并列预览。";
globalThis.VISUAL_REPORTS_KIND_CLASS = "note";
globalThis.VISUAL_REPORTS_MULTI_CYCLE_KIND = true;
__SHARED_VISUAL_REPORTS_JS__
const REFRESH_MS = __REFRESH_MS__;
function heartbeatMetric(heartbeat, key) {
  return obj(heartbeatSummary(heartbeat))[key];
}

function renderPortfolioSleeves(payload) {
  const feed = byId("portfolio-sleeves-feed");
  const sleeves = arr(payload?.sleeve_reports);
  if (!sleeves.length) {
    feed.innerHTML = '<div class="empty">当前没有可展示的组合子报表。</div>';
    return;
  }
  feed.innerHTML = sleeves.map((item) => {
    const metricsText = formatMetricLines(item.metrics, { includeWinRate: true });
    const url = item.dashboard?.url ? bust(item.dashboard.url) : "";
    const link = url ? `<a href="${url}" target="_blank" rel="noreferrer">打开子报表</a>` : "暂无报表";
    return `<div class="feed-item">
      <strong>${esc(txt(item.symbol, "标的"))}</strong>
      <div>${esc(metricsText)}</div>
      <div class="note">${link}</div>
    </div>`;
  }).join("");
}

function renderProjectTasks(tasks) {
  const feed = byId("project-task-feed");
  const rows = arr(tasks);
  if (!rows.length) {
    feed.innerHTML = '<div class="empty">还没有任务执行记录。</div>';
    return;
  }
  feed.innerHTML = rows.map((task) => {
    const payload = obj(task.request_payload);
    const prefix = txt(payload.logical_prefix, "--");
    return `<div class="feed-item">
      <strong>${esc(txt(task.task_label, txt(task.task_name || task.task, "--")))}</strong>
      <div>${esc(txt(task.status_label, txt(task.status, "--")))} | ${esc(when(task.created_at))}</div>
      <div class="note">逻辑前缀：${esc(prefix)}</div>
    </div>`;
  }).join("");
}

function renderPreflight(preflight) {
  const demo = obj(preflight.demo_trading);
  const alerts = obj(preflight.alerts);
  const summary = obj(preflight.dashboard_summary);
  const demoSummary = obj(summary.demo_mode);
  const alertsSummary = obj(summary.alerts);
  const loopSummary = obj(summary.loop);
  const statusSummary = obj(summary.status);
  const loop = obj(obj(preflight.execution_loop).latest_heartbeat);
  const channels = Object.entries(obj(alerts.channels))
    .filter(([, item]) => item && item.ready)
    .map(([name]) => name);

  byId("card-demo-mode").textContent = txt(demoSummary.label, txt(demo.mode, "--"));
  byId("card-demo-mode-note").textContent = txt(demoSummary.note, arr(demo.reasons).join(" | ") || "无额外说明");
  byId("card-alerts-ready").textContent = txt(alertsSummary.label, channels.length ? channels.join(", ") : "未就绪");
  byId("card-alerts-note").textContent = txt(alertsSummary.note, txt(obj(preflight.okx_connectivity).profile, "未配置"));
  byId("card-loop-status").textContent = txt(loopSummary.label, txt(loop.status_label, txt(loop.status, "缺失")));
  byId("card-loop-note").textContent = txt(loopSummary.note, txt(obj(preflight.execution_loop).namespace, "--"));
  byId("meta-refresh").textContent = `${Math.round(REFRESH_MS / 1000)} 秒`;
  byId("meta-updated").textContent = `页面刷新：${when(new Date().toISOString())} | 状态：${txt(statusSummary.display_label, txt(statusSummary.label, "--"))}`;
  byId("preflight-json").textContent = JSON.stringify(preflight, null, 2);
}

function renderSnapshot(snapshot) {
  byId("snapshot-json").textContent = JSON.stringify(snapshot || {}, null, 2);
}

async function runProjectTask(task) {
  byId("project-task-status").textContent = `正在提交任务：${task}`;
  try {
    const payload = await requestJson("/project/submit", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ task }),
    });
    byId("project-task-status").textContent = `任务已排队：${task}`;
    byId("project-task-output").textContent = JSON.stringify(payload.task_run || payload, null, 2);
    await refresh();
  } catch (error) {
    byId("project-task-status").textContent = `任务失败：${task}`;
    byId("project-task-output").textContent = JSON.stringify(error.payload || { detail: error.message || String(error) }, null, 2);
  }
}

async function refresh() {
  const [latest, alerts, heartbeats, artifacts, preflight, tasks] = await Promise.all([
    requestJson("/runtime/latest"),
    requestJson("/alerts?limit=12"),
    requestJson("/heartbeats?limit=12"),
    requestJson("/artifacts"),
    requestJson("/runtime/preflight"),
    requestJson("/project/tasks?limit=10"),
  ]);

  renderPreflight(preflight);
  renderSnapshot(latest.snapshot);
  renderVisualReports(artifacts);
  renderPortfolioSleeves(artifacts);
  renderProjectTasks(tasks.tasks);

  byId("alerts-feed").innerHTML = arr(alerts.alerts).length
    ? arr(alerts.alerts).map((item) => `<div class="feed-item">
        <strong>${esc(txt(item.title || item.event_key, "--"))}</strong>
        <div>${esc(txt(item.status_label, txt(item.status, "--")))} | ${esc(when(item.created_at))}</div>
        <div>${esc(txt(item.message, "--"))}</div>
      </div>`).join("")
    : '<div class="empty">最近没有告警。</div>';

  byId("heartbeats-feed").innerHTML = arr(heartbeats.heartbeats).length
    ? arr(heartbeats.heartbeats).map((item) => `<div class="feed-item">
        <strong>${esc(txt(item.service_name, "--"))}</strong>
        <div>${esc(txt(item.status_label, txt(item.status, "--")))} | ${esc(when(item.created_at))}</div>
        <div>${esc(JSON.stringify(obj(item.details)))}</div>
      </div>`).join("")
    : '<div class="empty">最近没有心跳。</div>';
}

byId("task-backtest")?.addEventListener("click", () => runProjectTask("backtest"));
byId("task-report")?.addEventListener("click", () => runProjectTask("report"));
byId("task-sweep")?.addEventListener("click", () => runProjectTask("sweep"));
byId("task-research")?.addEventListener("click", () => runProjectTask("research"));

refresh();
setInterval(refresh, REFRESH_MS);
</script>
</body>
</html>"""
    for old, new in {
        "__SYMBOLS__": html_lib.escape(symbols),
        "__STRATEGY__": html_lib.escape(strategy),
        "__MODE__": html_lib.escape(mode),
        "__REFRESH_MS__": str(refresh_ms),
        "__INITIAL_VISUAL_REPORTS_NOTE__": html_lib.escape(initial_visual_reports_note),
        "__INITIAL_VISUAL_REPORTS_HTML__": initial_visual_reports_html,
        "__INITIAL_PORTFOLIO_SLEEVES_HTML__": initial_portfolio_sleeves_html,
    }.items():
        html = html.replace(old, new)
    html = html.replace("__SHARED_DASHBOARD_BASE_JS__", render_shared_dashboard_base_js())
    html = html.replace("__SHARED_REPORT_METRICS_JS__", render_shared_report_metrics_js())
    html = html.replace("__SHARED_VISUAL_REPORTS_JS__", render_shared_visual_reports_js())
    return HTMLResponse(html, headers=NO_CACHE_HTML_HEADERS)
