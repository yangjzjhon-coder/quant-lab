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


def render_client_dashboard(
    config: AppConfig,
    *,
    initial_visual_reports_note: str = "正在加载可视化回测报表...",
    initial_visual_reports_html: str = '<div class="empty">正在加载可视化回测报表...</div>',
) -> HTMLResponse:
    symbols = " / ".join(configured_symbols(config))
    mode = "组合模式" if len(configured_symbols(config)) > 1 else "单标的模式"
    strategy = config.strategy.name
    html = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Cache-Control" content="no-store, no-cache, must-revalidate, max-age=0">
<meta http-equiv="Pragma" content="no-cache">
<meta http-equiv="Expires" content="0">
<title>quant-lab 本地客户端</title>
<style>
:root{
  --bg:#f4efe6;
  --panel:#fffdfa;
  --line:rgba(18,25,38,.10);
  --text:#18232d;
  --muted:#687684;
  --accent:#0b7285;
  --accent-2:#cd7b25;
  --danger:#c4542d;
  --ok:#16825d;
  --warn:#bd7a14;
  --shadow:0 18px 42px rgba(18,25,38,.06);
}
*{box-sizing:border-box}
body{
  margin:0;
  padding:24px;
  background:linear-gradient(180deg,#f1ece2 0%,#faf7f2 360px);
  color:var(--text);
  font:14px/1.65 "Segoe UI","PingFang SC","Noto Sans SC",sans-serif;
}
main{max-width:1480px;margin:0 auto;display:grid;gap:16px}
section{
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:22px;
  padding:20px;
  box-shadow:var(--shadow);
}
h1,h2,h3{margin:0 0 10px}
.muted,.hint,small{color:var(--muted)}
a{color:var(--accent)}
code{
  padding:2px 8px;
  border-radius:999px;
  background:#edf5f6;
  font-size:12px;
}
.row,.cards,.report-grid,.controls,.feed,.dual{display:grid;gap:12px}
.row{grid-template-columns:minmax(0,1fr) auto;align-items:flex-end}
.cards{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}
.dual{grid-template-columns:repeat(2,minmax(0,1fr))}
.card,.item,.report-card{
  background:rgba(255,255,255,.86);
  border:1px solid var(--line);
  border-radius:18px;
  padding:16px;
}
.value{font-size:1.6rem;font-weight:700}
.pill{
  display:inline-flex;
  align-items:center;
  padding:8px 14px;
  border-radius:999px;
  font-size:12px;
  font-weight:700;
}
.pill-ok{background:#e8f7f0;color:var(--ok)}
.pill-warn{background:#fff4e3;color:var(--warn)}
.pill-danger{background:#fbeae4;color:var(--danger)}
.pill-neutral{background:#eef2f5;color:#586675}
.value.ok{color:var(--ok)}
.value.warn{color:var(--warn)}
.value.danger{color:var(--danger)}
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
  min-height:520px;
}
.report-card.primary .report-preview{min-height:720px}
.report-preview iframe{width:100%;height:520px;border:0}
.report-card.primary .report-preview iframe{height:720px}
.controls{grid-template-columns:repeat(3,minmax(0,1fr))}
.control-card{
  display:grid;
  gap:10px;
  min-height:100%;
}
button,.report-card a{
  min-height:44px;
  padding:10px 14px;
  border:0;
  border-radius:14px;
  background:linear-gradient(135deg,#0b7285,#1098ad);
  color:#fff;
  font-weight:700;
  cursor:pointer;
  text-decoration:none;
}
button.alt{background:linear-gradient(135deg,#b96b18,#d5852d)}
button.warn{background:linear-gradient(135deg,#aa640f,#cf7a11)}
button.danger{background:linear-gradient(135deg,#a94722,#cc5a2b)}
input[type="text"],textarea{
  width:100%;
  padding:11px 12px;
  border:1px solid var(--line);
  border-radius:12px;
  background:#fff;
  color:#111827;
  font:inherit;
}
textarea{min-height:88px;resize:vertical}
.list{max-height:340px;overflow:auto}
.item{white-space:pre-wrap;word-break:break-word}
pre{
  margin:0;
  padding:16px;
  border-radius:18px;
  border:1px solid #203140;
  background:#11212c;
  color:#e6f0f2;
  max-height:420px;
  overflow:auto;
  font:12px/1.55 "IBM Plex Mono","Consolas",monospace;
}
svg{
  width:100%;
  height:220px;
  display:block;
  border-radius:16px;
  border:1px solid var(--line);
  background:linear-gradient(180deg,#f7fbfc,#fffdf9);
}
.empty{
  padding:18px;
  border:1px dashed var(--line);
  border-radius:16px;
  text-align:center;
  color:var(--muted);
}
.actions{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}
@media (max-width:1200px){
  .dual,.controls,.row{grid-template-columns:1fr}
  .report-card.primary .report-preview,
  .report-card.primary .report-preview iframe{min-height:460px;height:460px}
}
</style>
</head>
<body>
<main>
<section>
  <div class="row">
    <div>
      <div class="muted">quant-lab 本地客户端</div>
      <h1>__SYMBOLS__</h1>
      <div class="hint">策略 <code>__STRATEGY__</code>，当前运行在 <strong>__MODE__</strong>。</div>
    </div>
    <span id="headline-pill" class="pill pill-neutral">等待刷新</span>
  </div>
  <div class="actions">
    <a href="/">打开运行总览</a>
    <a href="/reports/backtest" target="_blank" rel="noreferrer">打开当前回测报表</a>
    <a href="/reports/sweep" target="_blank" rel="noreferrer">打开参数扫描报表</a>
  </div>
</section>

<section>
  <div class="cards">
    <div class="card">
      <small>自动交易状态</small>
      <div class="value" id="headline-title">正在加载</div>
      <div class="hint" id="headline-subtitle">--</div>
    </div>
    <div class="card">
      <small>提交门禁</small>
      <div class="value" id="headline-submit">--</div>
      <div class="hint" id="headline-submit-note">--</div>
    </div>
    <div class="card">
      <small>当前动作</small>
      <div class="value" id="headline-actionable">--</div>
      <div class="hint" id="headline-actionable-note">--</div>
    </div>
    <div class="card">
      <small>最近更新时间</small>
      <div class="value" id="meta-updated">--</div>
      <div class="hint" id="meta-source">--</div>
    </div>
  </div>
</section>

<section>
  <div>
    <h2>策略可视化回测</h2>
    <div class="hint">这里直接嵌入多周期 K 线、开平仓标记、止损线和逻辑复盘页面，便于一边看运行态一边核对历史回测。</div>
  </div>
  <small id="visual-reports-note">__INITIAL_VISUAL_REPORTS_NOTE__</small>
  <div id="visual-reports-feed" class="report-grid">
    __INITIAL_VISUAL_REPORTS_HTML__
  </div>
</section>

<section>
  <h2>操作面板</h2>
  <div class="controls">
    <div class="card control-card">
      <label>提交确认口令
        <input id="confirm-input" type="text" placeholder="OKX_DEMO">
      </label>
      <label>
        <input id="rearm-stop" type="checkbox">
        允许临时重挂保护止损
      </label>
      <label>
        <input id="auto-refresh" type="checkbox" checked>
        每 30 秒自动刷新
      </label>
    </div>
    <div class="card control-card">
      <label>测试告警内容
        <textarea id="alert-message">quant-lab 本地客户端测试告警</textarea>
      </label>
    </div>
    <div class="card control-card">
      <small>说明</small>
      <div class="hint">杠杆校准分为演练与实际应用两条路径；若执行真实变更，仍然要求确认口令通过。</div>
    </div>
  </div>
  <div class="actions">
    <button id="btn-refresh">刷新快照</button>
    <button id="btn-reconcile" class="alt">执行对账</button>
    <button id="btn-align-dry" class="warn">杠杆校准演练</button>
    <button id="btn-align-apply" class="danger">应用杠杆校准</button>
    <button id="btn-alert" class="alt">发送测试告警</button>
  </div>
</section>

<section class="dual">
  <div>
    <h2>核心检查</h2>
    <div class="cards">
      <div class="card">
        <small>模拟盘</small>
        <div class="value" id="check-demo">--</div>
        <div class="hint" id="check-demo-note">--</div>
      </div>
      <div class="card">
        <small>杠杆</small>
        <div class="value" id="check-leverage">--</div>
        <div class="hint" id="check-leverage-note">--</div>
      </div>
      <div class="card">
        <small>仓位</small>
        <div class="value" id="check-size">--</div>
        <div class="hint" id="check-size-note">--</div>
      </div>
      <div class="card">
        <small>止损</small>
        <div class="value" id="check-stop">--</div>
        <div class="hint" id="check-stop-note">--</div>
      </div>
    </div>
    <div id="plan-summary" class="list"><div class="empty">等待刷新...</div></div>
  </div>
  <div>
    <h2>阻塞与警告</h2>
    <div id="warning-list" class="list"><div class="empty">等待刷新...</div></div>
  </div>
</section>

<section class="dual">
  <div>
    <h2 id="symbols-title">标的状态</h2>
    <div class="hint" id="symbols-note">等待刷新...</div>
    <div id="symbol-list" class="list"><div class="empty">等待刷新...</div></div>
  </div>
  <div>
    <h2>模拟执行历史</h2>
    <div class="hint" id="history-note">等待刷新...</div>
    <div class="cards">
      <div class="card"><small>循环次数</small><div class="value" id="history-cycles">--</div></div>
      <div class="card"><small>提交次数</small><div class="value" id="history-submitted">--</div></div>
      <div class="card"><small>提交率</small><div class="value" id="history-rate">--</div></div>
      <div class="card"><small>最近状态</small><div class="value" id="history-status">--</div></div>
    </div>
    <small id="history-updated">--</small>
    <small id="chart-summary">--</small>
    <svg id="history-chart" viewBox="0 0 760 220" preserveAspectRatio="none"></svg>
    <div class="hint" id="chart-note">--</div>
  </div>
</section>

<section class="dual">
  <div>
    <h2>最近事件</h2>
    <div id="event-feed" class="list"><div class="empty">等待刷新...</div></div>
  </div>
  <div>
    <h2>原始载荷</h2>
    <small id="result-stamp">--</small>
    <div class="hint">调试 JSON，字段名保留英文用于排障。</div>
    <pre id="raw-json">等待刷新...</pre>
  </div>
</section>
</main>

<script>
__SHARED_DASHBOARD_BASE_JS__
__SHARED_REPORT_METRICS_JS__
globalThis.VISUAL_REPORTS_NOTE_TEXT = "主报表置顶显示，便于直接查看 K 线回放、交易标记与止损线。";
globalThis.VISUAL_REPORTS_KIND_CLASS = "hint";
globalThis.VISUAL_REPORTS_MULTI_CYCLE_KIND = false;
__SHARED_VISUAL_REPORTS_JS__
const PORTFOLIO_DEBUG_KEYS = ["requested_total_risk_pct", "allocated_total_risk_pct", "budgeted_equity_total", "portfolio_risk", "planning_account"];
let timer = null;
const fmt = (value, digits=2) => {
  const parsed = num(value, null);
  return parsed === null ? "--" : parsed.toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
};
const pct = (value) => {
  const parsed = num(value, null);
  return parsed === null ? "--" : `${fmt(parsed, 2)}%`;
};

function pillClass(value) {
  const raw = String(value || "").trim();
  if (raw === "ok") {
    return "pill-ok";
  }
  if (raw === "warn") {
    return "pill-warn";
  }
  if (raw === "danger") {
    return "pill-danger";
  }
  return "pill-neutral";
}

function level(node, value, cssClass, note) {
  if (!$(node)) {
    return;
  }
  $(node).textContent = value;
  $(node).className = `value ${cssClass || ""}`.trim();
  const noteNode = $(`${node}-note`);
  if (noteNode) {
    noteNode.textContent = note || "--";
  }
}

function renderHistory(snapshot) {
  const visuals = obj(snapshot.demo_visuals);
  const summary = obj(visuals.summary);
  const chart = obj(visuals.chart);
  const points = arr(chart.points);
  const svg = $("history-chart");

  $("history-note").textContent = summary.mode === "portfolio" ? "当前展示组合执行循环历史。" : "当前展示单标的执行循环历史。";
  $("history-updated").textContent = `最近事件时间：${when(summary.last_event_time)}`;
  $("history-cycles").textContent = fmt(summary.total_cycles, 0);
  $("history-submitted").textContent = fmt(summary.submitted_count, 0);
  $("history-rate").textContent = pct(summary.submission_rate_pct);
  $("history-status").textContent = txt(summary.last_status_label, "--");

  if (!points.length) {
    svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" fill="#687684" font-size="16">暂无执行历史</text>';
    $("chart-summary").textContent = "--";
    $("chart-note").textContent = "等待演示执行循环产生历史记录。";
  } else {
    const values = points.flatMap((point) => [
      Number(point.target_contracts),
      ...(num(point.current_contracts, null) === null ? [] : [Number(point.current_contracts)]),
    ]);
    const minimum = Math.min(...values);
    const maximum = Math.max(...values);
    const span = Math.max(maximum - minimum, 1);
    const width = 760;
    const height = 220;
    const left = 40;
    const right = 12;
    const top = 14;
    const bottom = 24;
    const x = (index) => points.length === 1 ? ((width - left - right) / 2) + left : left + (((width - left - right) * index) / (points.length - 1));
    const y = (value) => height - bottom - (((Number(value) - minimum) / span) * (height - top - bottom));
    const targetPath = points.map((point, index) => `${index === 0 ? "M" : "L"} ${x(index).toFixed(2)} ${y(point.target_contracts).toFixed(2)}`).join(" ");
    const livePath = points
      .filter((point) => num(point.current_contracts, null) !== null)
      .map((point, index, source) => {
        const originalIndex = points.indexOf(point);
        return `${index === 0 ? "M" : "L"} ${x(originalIndex).toFixed(2)} ${y(point.current_contracts).toFixed(2)}`;
      })
      .join(" ");

    svg.innerHTML = `<path d="${targetPath}" fill="none" stroke="#0b7285" stroke-width="3"></path>${livePath ? `<path d="${livePath}" fill="none" stroke="#94a3b8" stroke-width="2" stroke-dasharray="8 6"></path>` : ""}`;
    $("chart-summary").textContent = "实线表示目标仓位，虚线表示实时仓位。";
    $("chart-note").textContent = `采样点数：${points.length}`;
  }

  $("event-feed").innerHTML = arr(visuals.recent_events).length
    ? arr(visuals.recent_events).map((item) => `<div class="item"><strong>循环 ${esc(txt(item.cycle, "--"))} | ${esc(txt(item.action_label, item.action))}</strong>
状态：${esc(txt(item.status_label, item.status))} | 时间：${esc(when(item.created_at))}
当前方向：${esc(txt(item.current_side_label, "--"))} | 目标方向：${esc(txt(item.desired_side_label, "--"))}
目标张数：${esc(fmt(item.target_contracts, 2))} | 实时张数：${esc(fmt(item.current_contracts, 2))}</div>`).join("")
    : '<div class="empty">最近没有演示执行循环事件。</div>';
}

function renderSnapshot(snapshot, payload) {
  const preflight = obj(snapshot.preflight);
  const demo = obj(preflight.demo_trading);
  const autotrade = obj(snapshot.autotrade_status);
  const headline = obj(snapshot.headline_summary);
  const headlineSubmit = obj(headline.submit);
  const headlineActionable = obj(headline.actionable);
  const headlineLoop = obj(headline.loop);
  const cards = obj(obj(snapshot.checks_summary).cards);
  const plan = arr(obj(snapshot.plan_summary).items);
  const warnings = arr(obj(snapshot.warning_summary).items);
  const symbols = arr(obj(snapshot.symbol_summary).cards);
  const latestEventTime = headline.latest_event_time || obj(snapshot.demo_visuals).summary?.last_event_time;
  const fallbackModeLabel = demo.ready === true ? "可提交" : (demo.mode === "plan_only" ? "仅规划" : txt(demo.mode, "--"));

  $("meta-updated").textContent = when(latestEventTime || new Date().toISOString());
  $("meta-source").textContent = `数据来源：${txt(headline.source_label, txt(snapshot.snapshot_source, "--"))} | 循环：${txt(headlineLoop.value, txt(autotrade.latest_loop_status_label, "--"))}`;
  $("headline-title").textContent = txt(headline.title, txt(autotrade.headline, demo.ready === true ? "自动交易可执行" : "自动交易被阻塞"));
  $("headline-subtitle").textContent = `模式：${txt(headline.mode_label, fallbackModeLabel)} | 最近事件：${when(latestEventTime)}`;
  $("headline-pill").className = `pill ${pillClass(headline.level || (demo.ready === true ? "ok" : "danger"))}`;
  $("headline-pill").textContent = txt(headline.pill_text, demo.ready === true ? "可提交" : "已阻塞");

  level(
    "headline-submit",
    txt(headlineSubmit.value, demo.ready === true ? "允许" : "阻塞"),
    txt(headlineSubmit.level, demo.ready === true ? "ok" : "danger"),
    txt(headlineSubmit.note, arr(demo.reasons).join(" | ") || "无"),
  );
  level(
    "headline-actionable",
    txt(headlineActionable.value, autotrade.will_submit_now === true ? "有动作" : "无动作"),
    txt(headlineActionable.level, autotrade.will_submit_now === true ? "ok" : "warn"),
    txt(headlineActionable.note, arr(autotrade.reasons).join(" | ") || "无"),
  );
  level("check-demo", txt(obj(cards.demo).value, "--"), txt(obj(cards.demo).level, "warn"), txt(obj(cards.demo).note, "--"));
  level("check-leverage", txt(obj(cards.leverage).value, "--"), txt(obj(cards.leverage).level, "warn"), txt(obj(cards.leverage).note, "--"));
  level("check-size", txt(obj(cards.size).value, "--"), txt(obj(cards.size).level, "warn"), txt(obj(cards.size).note, "--"));
  level("check-stop", txt(obj(cards.stop).value, "--"), txt(obj(cards.stop).level, "warn"), txt(obj(cards.stop).note, "--"));

  $("plan-summary").innerHTML = plan.length
    ? plan.map((item) => `<div class="item">${esc(item)}</div>`).join("")
    : '<div class="empty">当前没有计划摘要。</div>';
  $("warning-list").innerHTML = warnings.length
    ? warnings.map((item) => `<div class="item">${esc(item.text || item.message || item)}</div>`).join("")
    : '<div class="empty">当前没有阻塞告警。</div>';

  $("symbols-title").textContent = txt(obj(snapshot.symbol_summary).title, "标的状态");
  $("symbols-note").textContent = txt(obj(snapshot.symbol_summary).note, "--");
  $("symbol-list").innerHTML = symbols.length
    ? symbols.map((card) => `<div class="item"><strong>${esc(txt(card.title, "--"))}</strong>
${arr(card.lines).map((line) => esc(line)).join("\n")}</div>`).join("")
    : `<div class="empty">${esc(txt(obj(snapshot.symbol_summary).empty_text, "当前没有标的状态。"))}</div>`;

  renderHistory(snapshot);
  $("result-stamp").textContent = `载荷时间：${when(new Date().toISOString())}`;
  $("raw-json").textContent = JSON.stringify(payload, null, 2);
}

function busy(value) {
  ["btn-refresh", "btn-reconcile", "btn-align-dry", "btn-align-apply", "btn-alert"].forEach((id) => {
    if ($(id)) {
      $(id).disabled = value;
    }
  });
}

function alignBody(apply) {
  return {
    apply,
    confirm: $("confirm-input").value || "",
    rearm_protective_stop: $("rearm-stop").checked,
  };
}

function renderClientLoadError(error) {
  const summary = apiErrorSummary(error);
  const payload = apiErrorPayload(error) || { detail: summary };
  $("headline-pill").className = "pill pill-danger";
  $("headline-pill").textContent = "请求失败";
  $("headline-title").textContent = "客户端加载失败";
  $("headline-subtitle").textContent = summary;
  $("raw-json").textContent = JSON.stringify(payload, null, 2);
}

async function loadArtifactsSafe() {
  try {
    return await requestJson("/artifacts");
  } catch {
    return null;
  }
}

async function runClientRequest(url="/client/snapshot", options, refreshArtifacts=false) {
  busy(true);
  try {
    const payload = await requestJson(url, options || {});
    const snapshot = obj(payload.snapshot || payload);
    const artifacts = refreshArtifacts ? await loadArtifactsSafe() : null;
    renderSnapshot(snapshot, payload);
    if (artifacts) {
      renderVisualReports(artifacts);
    }
  } catch (error) {
    renderClientLoadError(error);
  } finally {
    busy(false);
  }
}

$("btn-refresh").addEventListener("click", () => runClientRequest("/client/snapshot", undefined, true));
$("btn-reconcile").addEventListener("click", () => runClientRequest("/client/reconcile", { method: "POST" }, true));
$("btn-align-dry").addEventListener("click", () => runClientRequest("/client/align-leverage", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(alignBody(false)),
}, true));
$("btn-align-apply").addEventListener("click", () => runClientRequest("/client/align-leverage", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(alignBody(true)),
}, true));
$("btn-alert").addEventListener("click", () => runClientRequest("/client/alert-test", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ message: $("alert-message").value || "quant-lab 本地客户端测试告警" }),
}, false));
$("auto-refresh").addEventListener("change", () => {
  if (timer) {
    clearInterval(timer);
  }
  timer = $("auto-refresh").checked ? setInterval(() => runClientRequest("/client/snapshot", undefined, true), 30000) : null;
});

timer = $("auto-refresh").checked ? setInterval(() => runClientRequest("/client/snapshot", undefined, true), 30000) : null;
runClientRequest("/client/snapshot", undefined, true);
</script>
</body>
</html>"""
    for old, new in {
        "__SYMBOLS__": html_lib.escape(symbols),
        "__STRATEGY__": html_lib.escape(strategy),
        "__MODE__": html_lib.escape(mode),
        "__INITIAL_VISUAL_REPORTS_NOTE__": html_lib.escape(initial_visual_reports_note),
        "__INITIAL_VISUAL_REPORTS_HTML__": initial_visual_reports_html,
    }.items():
        html = html.replace(old, new)
    html = html.replace("__SHARED_DASHBOARD_BASE_JS__", render_shared_dashboard_base_js())
    html = html.replace("__SHARED_REPORT_METRICS_JS__", render_shared_report_metrics_js())
    html = html.replace("__SHARED_VISUAL_REPORTS_JS__", render_shared_visual_reports_js())
    return HTMLResponse(html, headers=NO_CACHE_HTML_HEADERS)
