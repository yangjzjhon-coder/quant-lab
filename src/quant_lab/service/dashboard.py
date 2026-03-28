from __future__ import annotations

import html as html_lib

from fastapi.responses import HTMLResponse

from quant_lab.config import AppConfig, configured_symbols


def render_runtime_dashboard(config: AppConfig) -> HTMLResponse:
    refresh_ms = max(config.service.heartbeat_interval_seconds, 15) * 1000
    symbols = configured_symbols(config)
    symbol_label = " / ".join(symbols)
    mode_label = "组合模式" if len(symbols) > 1 else "单标的模式"
    strategy = config.strategy.name

    html = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>quant-lab runtime</title>
  <style>
    :root{--bg:#f3efe6;--panel:#fffdf9;--ink:#16202a;--muted:#5e6a72;--line:rgba(22,32,42,.1);--accent:#006d77;--ok:#2a9d8f;--warn:#bb3e03}
    *{box-sizing:border-box}body{margin:0;padding:24px;font:14px/1.55 "IBM Plex Sans","Segoe UI",sans-serif;color:var(--ink);background:linear-gradient(180deg,var(--bg),#faf7f1)}
    code{padding:0 .28rem;border-radius:6px;background:rgba(22,32,42,.08)}main{max-width:1340px;margin:0 auto;display:grid;gap:16px}
    section{background:rgba(255,251,245,.9);border:1px solid rgba(255,255,255,.65);border-radius:18px;padding:20px;box-shadow:0 16px 36px rgba(22,32,42,.07)}
    h1,h2,h3{margin:.1rem 0 .6rem;font-family:"Space Grotesk","Segoe UI",sans-serif}.muted,small,.note{color:var(--muted)}
    .hero,.board,.mini-grid,.cards,.status-cards,.toolbar,.feed{display:grid;gap:12px}.hero,.board{grid-template-columns:1.2fr .8fr}.mini-grid{grid-template-columns:1fr 1fr}.cards,.status-cards{grid-template-columns:repeat(auto-fit,minmax(170px,1fr))}
    .pill,.card,.feed-item,.chart-shell{border:1px solid var(--line);border-radius:14px;background:rgba(255,255,255,.66)}.pill,.card,.feed-item{padding:14px}.pill span,.card span{display:block;font-size:.82rem;color:var(--muted);margin-bottom:6px}
    .card strong{display:block;font-size:1.45rem;letter-spacing:-.04em}.status-value{font-weight:700;font-size:1.08rem}.ok{color:var(--ok)}.warn{color:var(--warn)}
    .toolbar,.task-grid{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}.toolbar a,.task-grid button{display:inline-flex;min-height:44px;align-items:center;justify-content:center;border-radius:12px;text-decoration:none;background:linear-gradient(135deg,#006d77,#0a9396);color:#fff;font-weight:700;border:0;cursor:pointer}.toolbar a.disabled,.task-grid button:disabled{opacity:.45;pointer-events:none}
    .task-grid button.alt{background:linear-gradient(135deg,#ae5f00,#ca6702)}.task-grid button.warn{background:linear-gradient(135deg,#9b2226,#bb3e03)}pre.task-output{margin:0;padding:14px;border-radius:14px;border:1px solid var(--line);background:#13242e;color:#e6f1f2;max-height:320px;overflow:auto}
    .chart-shell{padding:14px}svg{width:100%;height:300px;display:block;border-radius:12px;background:linear-gradient(180deg,rgba(0,109,119,.08),rgba(255,255,255,.75))}
    .feed{max-height:420px;overflow:auto}.feed-item{white-space:pre-wrap;word-break:break-word}.mono{font-family:"IBM Plex Mono","Consolas",monospace}.empty{padding:24px 14px;border:1px dashed var(--line);border-radius:14px;text-align:center;color:var(--muted)}
    .footer{text-align:right;font-size:.84rem;color:var(--muted)}@media(max-width:1040px){.hero,.board,.mini-grid{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <main>
    <section>
      <div class="hero">
        <div>
          <div class="muted">quant-lab runtime</div>
          <h1>__SYMBOL__</h1>
          <div class="muted">策略 <code>__STRATEGY__</code>，当前运行模式为 <strong>__MODE__</strong>。这个页面持续读取运行快照、告警、心跳和报告产物。</div>
          <div class="cards" style="margin-top:14px">
            <div class="pill"><span>策略</span><strong>__STRATEGY__</strong></div>
            <div class="pill"><span>模式</span><strong>__MODE__</strong></div>
            <div class="pill"><span>刷新周期</span><strong id="meta-refresh">--</strong></div>
            <div class="pill"><span>最后刷新</span><strong id="meta-updated">--</strong></div>
          </div>
        </div>
        <div class="status-cards">
          <div class="card"><span>运行状态</span><div class="status-value" id="status-runtime">加载中</div><small id="status-runtime-note"></small></div>
          <div class="card"><span>风控暂停</span><div class="status-value" id="status-halt">加载中</div><small id="status-halt-note"></small></div>
          <div class="card"><span>报告新鲜度</span><div class="status-value" id="status-stale">加载中</div><small id="status-stale-note"></small></div>
          <div class="card"><span>Demo Loop</span><div class="status-value" id="status-demo">加载中</div><small id="status-demo-note"></small></div>
        </div>
      </div>
    </section>

    <section>
      <div class="toolbar">
        <a href="/client" target="_blank" rel="noreferrer">打开交易客户端</a>
        <a href="/reports/backtest" id="backtest-link" class="disabled" target="_blank" rel="noreferrer">打开回测报告</a>
        <a href="/reports/sweep" id="sweep-link" class="disabled" target="_blank" rel="noreferrer">打开参数扫描报告</a>
      </div>
    </section>

    <section>
      <div class="cards">
        <div class="card"><span>最新净值</span><strong id="card-equity">--</strong><small id="card-equity-note">--</small></div>
        <div class="card"><span>累计收益</span><strong id="card-return">--</strong><small>来自当前最新 summary</small></div>
        <div class="card"><span>最大回撤</span><strong id="card-drawdown">--</strong><small>越低越稳</small></div>
        <div class="card"><span>交易次数</span><strong id="card-trades">--</strong><small>累计已完成交易</small></div>
        <div class="card"><span>最新现金</span><strong id="card-cash">--</strong><small>未实现盈亏 <span id="card-unrealized">--</span></small></div>
        <div class="card"><span>Runtime Mode</span><strong>__MODE__</strong><small>Tracking __SYMBOL_COUNT__ symbols</small></div>
        <div class="card"><span>Demo Mode</span><strong id="card-demo-mode">--</strong><small id="card-demo-mode-note">--</small></div>
        <div class="card"><span>Alert Channels</span><strong id="card-alerts-ready">--</strong><small id="card-alerts-note">--</small></div>
        <div class="card"><span>Loop Status</span><strong id="card-loop-status">--</strong><small id="card-loop-note">--</small></div>
      </div>
    </section>

    <section class="board">
      <div>
        <h2>净值轨迹</h2>
        <small id="chart-range">--</small>
        <div class="chart-shell" style="margin-top:10px">
          <svg id="equity-chart" viewBox="0 0 1000 300" preserveAspectRatio="none" aria-label="equity history"></svg>
        </div>
      </div>
      <div>
        <h2>最近告警</h2>
        <small id="alerts-count">0 条</small>
        <div class="feed" id="alerts-feed" style="margin-top:10px"><div class="empty">还没有告警记录。</div></div>
      </div>
    </section>

    <section class="mini-grid">
      <div>
        <h2>服务心跳</h2>
        <small id="heartbeats-count">0 条</small>
        <div class="feed" id="heartbeats-feed" style="margin-top:10px"><div class="empty">还没有心跳记录。</div></div>
      </div>
      <div>
        <h2>报告文件</h2>
        <small>本地服务直接代理</small>
        <div class="feed" id="artifacts-feed" style="margin-top:10px"><div class="empty">正在读取产物信息。</div></div>
      </div>
    </section>

    <section>
      <h2>Portfolio Sleeves</h2>
      <small id="portfolio-sleeves-note">Per-symbol sleeve metrics and direct report links will appear here when portfolio mode is enabled.</small>
      <div class="feed" id="portfolio-sleeves-feed" style="margin-top:10px"><div class="empty">Loading portfolio sleeve artifacts...</div></div>
    </section>

    <section class="mini-grid">
      <div>
        <h2>Project Tasks</h2>
        <small>Run the core research pipeline with the current config.</small>
        <div class="task-grid" style="margin-top:10px">
          <button id="task-backtest">Run Backtest</button>
          <button id="task-report" class="alt">Build Report</button>
          <button id="task-sweep" class="alt">Run Sweep</button>
          <button id="task-research" class="warn">Run Research</button>
        </div>
        <div class="feed" id="project-task-preflight" style="margin-top:10px"><div class="empty">Loading project task readiness...</div></div>
      </div>
      <div>
        <h2>Task Output</h2>
        <small id="project-task-status">Idle</small>
        <pre id="project-task-output" class="task-output mono">No project task has been triggered yet.</pre>
        <div class="feed" id="project-task-feed" style="margin-top:10px"><div class="empty">No project task history yet.</div></div>
      </div>
    </section>

    <div class="footer">自动刷新中。如果你刚刚重新跑完回测，手动刷新浏览器也可以立即看到新数据。</div>
  </main>

  <script>
    const REFRESH_MS = __REFRESH_MS__;
    const byId = (id) => document.getElementById(id);
    const obj = (v) => v && typeof v === 'object' && !Array.isArray(v) ? v : {};
    const arr = (v) => Array.isArray(v) ? v : [];
    const num = (v, fb = null) => (v === null || v === undefined || v === '' || Number.isNaN(Number(v))) ? fb : Number(v);
    function formatNumber(value, digits = 2) {
      const parsed = num(value, null);
      if (parsed === null) return '--';
      return parsed.toLocaleString('zh-CN', { minimumFractionDigits: digits, maximumFractionDigits: digits });
    }
    function formatPct(value) {
      const parsed = num(value, null);
      return parsed === null ? '--' : `${formatNumber(parsed, 2)}%`;
    }
    function formatTime(value) {
      if (!value) return '--';
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString('zh-CN', { hour12: false });
    }
    function setStatus(id, label, ok = true, note = '') {
      const node = byId(id);
      node.textContent = label;
      node.classList.remove('ok', 'warn');
      node.classList.add(ok ? 'ok' : 'warn');
      const noteNode = byId(`${id}-note`);
      if (noteNode) noteNode.textContent = note;
    }
    function setProjectTaskBusy(busy) {
      ['task-backtest', 'task-report', 'task-sweep', 'task-research'].forEach((id) => {
        const node = byId(id);
        if (node) node.disabled = busy;
      });
    }
    async function postJson(path, payload) {
      const response = await fetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify(payload || {}),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(data.detail || `Request failed: ${path}`);
      }
      return data;
    }
    async function runProjectTask(task) {
      byId('project-task-status').textContent = `Submitting ${task}...`;
      byId('project-task-output').textContent = 'Task submission is in progress. Please wait...';
      setProjectTaskBusy(true);
      try {
        const payload = await postJson('/project/submit', { task });
        const taskRun = payload.task_run || {};
        byId('project-task-status').textContent = `Queued: ${task}`;
        byId('project-task-output').textContent = JSON.stringify(taskRun, null, 2);
        const tasksPayload = await fetchJson('/project/tasks?limit=10');
        renderProjectTasks(tasksPayload.tasks);
        await refresh();
      } catch (error) {
        byId('project-task-status').textContent = `Failed: ${task}`;
        byId('project-task-output').textContent = String(error);
      } finally {
        setProjectTaskBusy(false);
      }
    }
    function renderArtifacts(payload) {
      const items = [payload?.backtest_report, payload?.sweep_report, payload?.summary, payload?.equity_curve, payload?.trades, payload?.sweep_csv].filter(Boolean);
      const feed = byId('artifacts-feed');
      if (!items.length) {
        feed.innerHTML = '<div class="empty">还没有可展示的报告产物。</div>';
        return;
      }
      feed.innerHTML = items.map((item) => `
        <article class="feed-item">
          <strong>${item.label}</strong>
          <div>${item.exists ? '已存在' : '未生成'}${item.url ? ` | <a href="${item.url}" target="_blank" rel="noreferrer">打开</a>` : ''}</div>
          <div class="mono">${item.path || '--'}</div>
        </article>
      `).join('');
      const backtest = payload?.backtest_report;
      const sweep = payload?.sweep_report;
      const backtestLink = byId('backtest-link');
      const sweepLink = byId('sweep-link');
      if (backtest?.exists && backtest?.url) {
        backtestLink.classList.remove('disabled');
        backtestLink.href = backtest.url;
      } else {
        backtestLink.classList.add('disabled');
      }
      if (sweep?.exists && sweep?.url) {
        sweepLink.classList.remove('disabled');
        sweepLink.href = sweep.url;
      } else {
        sweepLink.classList.add('disabled');
      }
    }
    function renderFeed(targetId, countId, items, mapper, emptyText) {
      byId(countId).textContent = `${items.length} 条`;
      byId(targetId).innerHTML = items.length ? items.map(mapper).join('') : `<div class="empty">${emptyText}</div>`;
    }
    function findHeartbeat(heartbeats, serviceName) {
      return (heartbeats || []).find((item) => item.service_name === serviceName) || null;
    }
    function renderPreflight(preflight, heartbeats) {
      const demo = obj(preflight?.demo_trading);
      const alerts = obj(preflight?.alerts);
      const okx = obj(preflight?.okx_connectivity);
      const loop = preflight?.execution_loop?.latest_heartbeat || findHeartbeat(heartbeats, 'quant-lab-demo-loop');
      const loopStatus = loop?.status || 'missing';
      const loopDetails = obj(loop?.details);
      const loopMode = loopDetails.mode || (loopDetails.symbol_count ? 'portfolio' : 'single');
      const loopCycle = loopDetails.cycle;
      const loopAction = loopDetails.action;
      const loopSymbolCount = loopDetails.symbol_count;
      const loopActionableCount = loopDetails.actionable_symbol_count;
      const loopActiveCount = loopDetails.active_position_symbol_count;
      const demoReasons = arr(demo.reasons);
      const okxNotes = arr(okx.notes);
      const channelEntries = Object.entries(obj(alerts.channels));
      const readyChannels = channelEntries.filter(([, value]) => value.ready).map(([name]) => name);
      const enabledChannels = channelEntries.filter(([, value]) => value.enabled).map(([name]) => name);

      byId('card-demo-mode').textContent = demo.mode || '--';
      byId('card-demo-mode-note').textContent = demo.ready
        ? 'Ready for OKX demo auto-submit'
        : (okxNotes[okxNotes.length - 1] || demoReasons[0] || 'Running in safe planning mode');

      byId('card-alerts-ready').textContent = readyChannels.length ? readyChannels.join(', ') : 'disabled';
      byId('card-alerts-note').textContent = readyChannels.length
        ? `Ready: ${readyChannels.join(', ')}`
        : (enabledChannels.length ? `Incomplete: ${enabledChannels.join(', ')}` : 'No alert channel configured');

      byId('card-loop-status').textContent = loopStatus;
      byId('card-loop-note').textContent = loop
        ? (loopMode === 'portfolio'
          ? `Cycle ${loopCycle ?? '--'} · portfolio ${loopSymbolCount ?? '--'} symbols · actionable ${loopActionableCount ?? '--'} · active ${loopActiveCount ?? '--'}`
          : `Cycle ${loopCycle ?? '--'} · ${loopAction || 'n/a'}`)
        : 'No demo-loop heartbeat yet';

      const demoBaseLabel = demo.ready ? 'submit ready' : (loopStatus === 'error' ? 'loop error' : (demo.mode || 'plan_only'));
      const demoLabel = loopMode === 'portfolio' ? `portfolio ${demoBaseLabel}` : demoBaseLabel;
      const demoOk = demo.ready || !['error', 'missing'].includes(loopStatus);
      setStatus('status-demo', demoLabel, demoOk, loop
        ? (loopMode === 'portfolio' ? '组合 demo-loop 心跳已接入。' : '单标的 demo-loop 心跳已接入。')
        : '当前还没有 demo-loop 心跳。');
    }
    function renderChart(snapshots) {
      const svg = byId('equity-chart');
      if (!snapshots.length) {
        svg.innerHTML = '';
        byId('chart-range').textContent = '--';
        return;
      }
      const ordered = [...snapshots].reverse();
      const values = ordered.map((item) => Number(item.latest_equity));
      const max = Math.max(...values);
      const min = Math.min(...values);
      const span = max - min || Math.max(max * 0.02, 1);
      const width = 1000, height = 300, left = 34, right = 18, top = 18, bottom = 28;
      const chartWidth = width - left - right;
      const chartHeight = height - top - bottom;
      const points = ordered.map((item, index) => {
        const x = left + (ordered.length === 1 ? chartWidth / 2 : (index / (ordered.length - 1)) * chartWidth);
        const y = top + ((max - Number(item.latest_equity)) / span) * chartHeight;
        return `${x.toFixed(2)},${y.toFixed(2)}`;
      }).join(' ');
      const guides = Array.from({ length: 5 }, (_, index) => {
        const y = top + (chartHeight / 4) * index;
        const value = max - (span / 4) * index;
        return `
          <line x1="${left}" y1="${y}" x2="${width - right}" y2="${y}" stroke="rgba(22,32,42,0.08)" stroke-width="1" />
          <text x="8" y="${y + 4}" fill="rgba(94,106,114,0.9)" font-size="11">${formatNumber(value, 0)}</text>
        `;
      }).join('');
      const baseline = top + chartHeight;
      svg.innerHTML = `
        ${guides}
        <polyline fill="none" stroke="#0a9396" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" points="${points}" />
        <line x1="${left}" y1="${baseline}" x2="${width - right}" y2="${baseline}" stroke="rgba(22,32,42,0.2)" stroke-width="1.4" />
      `;
      byId('chart-range').textContent = `${formatTime(ordered[0]?.created_at || ordered[0]?.report_timestamp)} -> ${formatTime(ordered[ordered.length - 1]?.created_at || ordered[ordered.length - 1]?.report_timestamp)}`;
    }
    function renderArtifactCatalog(payload) {
      const featured = [payload?.backtest_report, payload?.sweep_report, payload?.summary, payload?.equity_curve, payload?.trades, payload?.sweep_csv].filter(Boolean);
      const seenPaths = new Set(featured.map((item) => item.path));
      const sleevePaths = new Set(
        arr(payload?.sleeve_reports).flatMap((item) => [
          item?.dashboard?.path,
          item?.summary_file?.path,
          item?.equity_curve?.path,
          item?.trades?.path,
        ].filter(Boolean))
      );
      const catalog = arr(payload?.catalog)
        .filter((item) => !seenPaths.has(item.path) && !sleevePaths.has(item.path))
        .slice(0, 18)
        .map((item) => ({
          ...item,
          label: `${item.group_label || 'Artifact'} | ${item.label || item.name}`,
          exists: item.exists !== false,
        }));
      const items = [...featured, ...catalog];
      const feed = byId('artifacts-feed');
      if (!items.length) {
        feed.innerHTML = '<div class="empty">杩樻病鏈夊彲灞曠ず鐨勬姤鍛婁骇鐗┿€?/div>';
      } else {
        feed.innerHTML = items.map((item) => `
          <article class="feed-item">
            <strong>${item.label}</strong>
            <div>${item.exists ? '宸插瓨鍦? : '鏈敓鎴?}${item.url ? ` | <a href="${item.url}" target="_blank" rel="noreferrer">鎵撳紑</a>` : ''}</div>
            <div class="mono">${item.path || '--'}</div>
            <div class="note">${item.modified_at ? `updated ${formatTime(item.modified_at)} | ` : ''}${item.size_bytes !== undefined ? `${formatNumber(item.size_bytes, 0)} bytes` : ''}</div>
          </article>
        `).join('');
      }
      const backtest = payload?.backtest_report;
      const sweep = payload?.sweep_report;
      const backtestLink = byId('backtest-link');
      const sweepLink = byId('sweep-link');
      if (backtest?.exists && backtest?.url) {
        backtestLink.classList.remove('disabled');
        backtestLink.href = backtest.url;
      } else {
        backtestLink.classList.add('disabled');
      }
      if (sweep?.exists && sweep?.url) {
        sweepLink.classList.remove('disabled');
        sweepLink.href = sweep.url;
      } else {
        sweepLink.classList.add('disabled');
      }
    }
    function renderPortfolioSleeves(payload) {
      const feed = byId('portfolio-sleeves-feed');
      const note = byId('portfolio-sleeves-note');
      const sleeves = arr(payload?.sleeve_reports);
      const mode = payload?.mode || 'single';
      if (!sleeves.length) {
        note.textContent = mode === 'portfolio'
          ? 'Portfolio mode is enabled, but sleeve artifacts are not available yet. Run backtest and report first.'
          : 'Current config is running in single-instrument mode. Sleeve cards appear only for portfolio backtests.';
        feed.innerHTML = `<div class="empty">${mode === 'portfolio' ? 'No sleeve artifacts found yet.' : 'Sleeve view is inactive in single-instrument mode.'}</div>`;
        return;
      }
      note.textContent = `Tracking ${sleeves.length} sleeve reports for ${arr(payload?.symbols).join(' / ') || 'portfolio mode'}.`;
      feed.innerHTML = sleeves.map((item) => {
        const metrics = obj(item?.metrics);
        const links = [
          item?.dashboard?.url ? `<a href="${item.dashboard.url}" target="_blank" rel="noreferrer">HTML</a>` : '',
          item?.summary_file?.url ? `<a href="${item.summary_file.url}" target="_blank" rel="noreferrer">Summary</a>` : '',
          item?.equity_curve?.url ? `<a href="${item.equity_curve.url}" target="_blank" rel="noreferrer">Equity CSV</a>` : '',
          item?.trades?.url ? `<a href="${item.trades.url}" target="_blank" rel="noreferrer">Trades CSV</a>` : '',
        ].filter(Boolean).join(' | ');
        return `
          <article class="feed-item">
            <strong>${item?.symbol || '--'}</strong>
            <div>Final Equity ${formatNumber(metrics.final_equity, 2)} | Return ${formatPct(metrics.total_return_pct)} | Max DD ${formatPct(metrics.max_drawdown_pct)}</div>
            <div>Trades ${formatNumber(metrics.trade_count, 0)} | Win Rate ${formatPct(metrics.win_rate_pct)} | Sharpe ${formatNumber(metrics.sharpe, 2)}</div>
            <div>${links || 'Artifact links are not ready yet.'}</div>
            <div class="note">Capital Allocation ${formatPct(metrics.capital_allocation_pct)}</div>
          </article>
        `;
      }).join('');
    }
    function renderProjectTasks(tasks) {
      const items = arr(tasks);
      const feed = byId('project-task-feed');
      if (!items.length) {
        byId('project-task-status').textContent = 'Idle';
        byId('project-task-output').textContent = 'No project task has been triggered yet.';
        feed.innerHTML = '<div class="empty">No project task history yet.</div>';
        return;
      }
      const latest = items[0];
      byId('project-task-status').textContent = `${latest.status || '--'}: ${latest.task_name || '--'}`;
      byId('project-task-output').textContent = latest.error_message || JSON.stringify(latest.result_payload || latest.artifact_payload || latest.request_payload || {}, null, 2);
      feed.innerHTML = items.map((item) => `
        <article class="feed-item">
          <strong>${item.task_name || '--'}</strong>
          <div>${item.status || '--'} | started ${formatTime(item.started_at)}${item.finished_at ? ` | finished ${formatTime(item.finished_at)}` : ''}</div>
          <div class="mono">${item.error_message || JSON.stringify(item.artifact_payload || item.result_payload || {}, null, 2)}</div>
        </article>
      `).join('');
    }
    function renderProjectPreflight(payload) {
      const feed = byId('project-task-preflight');
      const tasks = obj(payload?.tasks);
      const names = Object.keys(tasks);
      if (!names.length) {
        feed.innerHTML = '<div class="empty">Project task readiness is unavailable.</div>';
        return;
      }
      feed.innerHTML = names.map((name) => {
        const item = obj(tasks[name]);
        const missing = arr(item.missing);
        const state = item.ready ? 'ready' : 'missing deps';
        const detail = item.ready
          ? `${item.present_count || 0}/${item.required_count || 0} dependencies ready`
          : `${missing.length} missing | ${item.present_count || 0}/${item.required_count || 0} ready`;
        const body = item.ready
          ? '<div class="note">No blocking files detected.</div>'
          : `<div class="mono">${missing.slice(0, 4).join('\n')}${missing.length > 4 ? `\n... +${missing.length - 4} more` : ''}</div><div class="note">${item.hint || ''}</div>`;
        return `
        <article class="feed-item">
          <strong>${name}</strong>
          <div>${state} | ${detail}</div>
          ${body}
        </article>
      `;
      }).join('');
    }
    async function fetchJson(path) {
      const response = await fetch(path, { headers: { Accept: 'application/json' } });
      if (!response.ok) throw new Error(`Request failed: ${path}`);
      return response.json();
    }
    async function refresh() {
      try {
        const [latestPayload, historyPayload, alertsPayload, heartbeatsPayload, artifactsPayload, preflightPayload, projectTasksPayload, projectPreflightPayload] = await Promise.all([
          fetchJson('/runtime/latest'),
          fetchJson('/runtime/history?limit=120'),
          fetchJson('/alerts?limit=12'),
          fetchJson('/heartbeats?limit=12'),
          fetchJson('/artifacts'),
          fetchJson('/runtime/preflight'),
          fetchJson('/project/tasks?limit=10'),
          fetchJson('/project/preflight'),
        ]);
        const snapshot = latestPayload.snapshot;
        const history = arr(historyPayload.snapshots);
        const alerts = arr(alertsPayload.alerts);
        const heartbeats = arr(heartbeatsPayload.heartbeats);
        byId('meta-refresh').textContent = `${Math.round(REFRESH_MS / 1000)} 秒`;
        byId('meta-updated').textContent = formatTime(new Date().toISOString());

        if (!snapshot) {
          byId('card-equity').textContent = '--';
          byId('card-return').textContent = '--';
          byId('card-drawdown').textContent = '--';
          byId('card-trades').textContent = '--';
          byId('card-cash').textContent = '--';
          byId('card-unrealized').textContent = '--';
          byId('card-equity-note').textContent = '还没有运行快照。';
          setStatus('status-runtime', '等待首次监控', false, '数据库中还没有可展示的 runtime snapshot。');
          setStatus('status-halt', '未知', false, '当前还没有风控状态。');
          setStatus('status-stale', '未知', false, '当前还没有报告新鲜度信息。');
        } else {
          byId('card-equity').textContent = formatNumber(snapshot.latest_equity, 2);
          byId('card-equity-note').textContent = `报告时间 ${formatTime(snapshot.report_timestamp)}`;
          byId('card-return').textContent = formatPct(snapshot.total_return_pct);
          byId('card-drawdown').textContent = formatPct(snapshot.max_drawdown_pct);
          byId('card-trades').textContent = formatNumber(snapshot.trade_count, 0);
          byId('card-cash').textContent = formatNumber(snapshot.latest_cash, 2);
          byId('card-unrealized').textContent = formatNumber(snapshot.latest_unrealized_pnl, 2);
          setStatus('status-runtime', snapshot.report_stale ? '运行中，报告过期' : '运行正常', !snapshot.report_stale, snapshot.report_stale ? '监控服务正常，但最新报告已经过期。' : '监控服务正在读取最新报告。');
          setStatus('status-halt', snapshot.halted ? '已暂停开新仓' : '正常', !snapshot.halted, snapshot.halted ? '风控熔断已触发。' : '当前未触发风控暂停。');
          setStatus('status-stale', snapshot.report_stale ? '已过期' : '新鲜', !snapshot.report_stale, snapshot.report_stale ? '报告新鲜度低于服务要求。' : '报告更新时间在允许范围内。');
        }

        renderChart(history);
        renderPreflight(preflightPayload, heartbeats);
        renderArtifactCatalog(artifactsPayload);
        renderPortfolioSleeves(artifactsPayload);
        renderProjectTasks(projectTasksPayload.tasks);
        renderProjectPreflight(projectPreflightPayload);
        renderFeed('alerts-feed', 'alerts-count', alerts, (item) => `<article class="feed-item"><strong>${item.title}</strong><div>${item.level} | ${item.status} | ${formatTime(item.created_at)}</div><div>${item.message}</div></article>`, '还没有告警记录。');
        renderFeed('heartbeats-feed', 'heartbeats-count', heartbeats, (item) => `<article class="feed-item"><strong>${item.service_name}</strong><div>${item.status} | ${formatTime(item.created_at)}</div><div class="mono">${JSON.stringify(item.details, null, 2)}</div></article>`, '还没有心跳记录。');
      } catch (_error) {
        byId('meta-updated').textContent = '读取失败';
        setStatus('status-runtime', '服务请求失败', false, '前端无法从 runtime 接口读取数据。');
        setStatus('status-demo', 'request failed', false, '前端无法读取 demo-loop 预检信息。');
      }
    }
    byId('task-backtest')?.addEventListener('click', () => runProjectTask('backtest'));
    byId('task-report')?.addEventListener('click', () => runProjectTask('report'));
    byId('task-sweep')?.addEventListener('click', () => runProjectTask('sweep'));
    byId('task-research')?.addEventListener('click', () => runProjectTask('research'));
    refresh();
    setInterval(refresh, REFRESH_MS);
  </script>
</body>
</html>"""

    for old, new in {
        "__REFRESH_MS__": str(refresh_ms),
        "__SYMBOL__": html_lib.escape(symbol_label),
        "__STRATEGY__": html_lib.escape(strategy),
        "__MODE__": html_lib.escape(mode_label),
        "__SYMBOL_COUNT__": str(len(symbols)),
    }.items():
        html = html.replace(old, new)
    return HTMLResponse(html)
