from __future__ import annotations


def render_shared_dashboard_base_js() -> str:
    return """const byId = (id) => document.getElementById(id);
const $ = byId;
const obj = (value) => value && typeof value === "object" && !Array.isArray(value) ? value : {};
const arr = (value) => Array.isArray(value) ? value : [];
const txt = (value, fallback="--") => value === null || value === undefined || value === "" ? fallback : String(value);
const num = (value, fallback=null) => value === null || value === undefined || value === "" || Number.isNaN(Number(value)) ? fallback : Number(value);
const esc = (value) => String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#39;");
const bust = (url) => {
  const source = String(url || "").trim();
  return source ? `${source}${source.includes("?") ? "&" : "?"}_ts=${Date.now()}` : source;
};
const when = (value) => {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString("zh-CN", { hour12: false });
};

function apiErrorPayload(error) {
  if (error && typeof error === "object" && error.payload && typeof error.payload === "object") {
    return error.payload;
  }
  if (error && typeof error === "object" && !Array.isArray(error) && (error.detail !== undefined || error.error_code !== undefined)) {
    return error;
  }
  return null;
}

function apiErrorSummary(error) {
  const payload = apiErrorPayload(error) || {};
  const detail = txt(payload.detail, error && error.message ? error.message : "请求失败").trim();
  const code = txt(payload.error_code, "").trim();
  const retry = payload.retryable === true ? " | 可重试" : "";
  return code ? `[${code}] ${detail}${retry}` : `${detail}${retry}`;
}

function buildApiError(payload, status, path) {
  const normalized = obj(payload);
  const detail = txt(normalized.detail, `请求失败：${path}`);
  return {
    message: apiErrorSummary({ payload: { ...normalized, detail } }),
    payload: { ...normalized, detail },
    status,
    path,
  };
}

async function requestJson(path, options = {}) {
  const response = await fetch(bust(path), {
    ...options,
    cache: "no-store",
    headers: { Accept: "application/json", ...obj(options).headers },
  });
  const raw = await response.text();
  let data = {};
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch {
    data = { detail: raw || `请求失败：${path}` };
  }
  if (!response.ok) {
    throw buildApiError(data, response.status, path);
  }
  return data;
}

function heartbeatSummary(heartbeat) {
  return obj(obj(heartbeat).details).summary;
}"""


def render_shared_visual_reports_js() -> str:
    return """function renderVisualReports(payload) {
  const feed = byId("visual-reports-feed");
  const note = byId("visual-reports-note");
  const seen = new Set();
  const cards = [];
  const preferred = ["backtest_dashboard", "portfolio_dashboard", "sweep_dashboard"];
  const noteText = typeof globalThis.VISUAL_REPORTS_NOTE_TEXT === "string"
    ? globalThis.VISUAL_REPORTS_NOTE_TEXT
    : "首个报表作为主页主图展示，其余最近策略回测继续在下方并列预览。";
  const kindClass = typeof globalThis.VISUAL_REPORTS_KIND_CLASS === "string"
    ? globalThis.VISUAL_REPORTS_KIND_CLASS
    : "note";
  const useMultiCycleKind = globalThis.VISUAL_REPORTS_MULTI_CYCLE_KIND === true;

  const push = (item) => {
    if (!item?.exists || !item?.url) {
      return;
    }
    const path = txt(item.path, "");
    if (path && seen.has(path)) {
      return;
    }
    if (path) {
      seen.add(path);
    }
    cards.push(item);
  };

  const pushBacktest = () => {
    if (payload?.backtest_report?.exists && payload?.backtest_report?.url) {
      push({
        label: payload?.mode === "portfolio" ? "当前组合总览" : "当前主回测",
        path: payload.backtest_report.path,
        url: payload.backtest_report.url,
        kind: "主报表",
        exists: true,
      });
    }
  };

  const pushSleeves = () => arr(payload?.sleeve_reports).forEach((item) => {
    if (item?.dashboard?.exists && item?.dashboard?.url) {
      push({
        label: `${item.symbol || "标的"} 子报表`,
        path: item.dashboard.path,
        url: item.dashboard.url,
        kind: "组合子报表",
        exists: true,
      });
    }
  });

  if (payload?.mode === "portfolio") {
    pushSleeves();
    pushBacktest();
  } else {
    pushBacktest();
    pushSleeves();
  }

  arr(payload?.catalog)
    .filter((item) => String(item?.category || "").includes("dashboard") && item?.url)
    .sort((left, right) => {
      const leftCategory = String(left?.category || "");
      const rightCategory = String(right?.category || "");
      const leftName = String(left?.name || "");
      const rightName = String(right?.name || "");
      const leftWeight = (leftName.includes("multi_cycle") ? -2 : 0) + (preferred.includes(leftCategory) ? -1 : 0);
      const rightWeight = (rightName.includes("multi_cycle") ? -2 : 0) + (preferred.includes(rightCategory) ? -1 : 0);
      return leftWeight - rightWeight;
    })
    .slice(0, 10)
    .forEach((item) => push({
      label: item.group_label || item.label || item.name || "网页报表",
      path: item.path,
      url: item.url,
      kind: item.name || "最近产物",
      exists: item.exists !== false,
    }));

  if (!cards.length) {
    note.textContent = "当前还没有可嵌入的网页回测报表，请先运行回测或生成报表任务。";
    feed.innerHTML = '<div class="empty">还没有可视化回测预览。</div>';
    return;
  }

  note.textContent = noteText;
  feed.innerHTML = cards.map((item, index) => {
    const url = bust(item.url);
    const cardClass = index === 0 ? "report-card primary" : "report-card";
    const kind = useMultiCycleKind && item.kind && String(item.kind).includes("multi_cycle")
      ? "多周期回测"
      : txt(item.kind, "回测报表");
    return `<article class="${cardClass}">
      <div class="report-head">
        <div>
          <strong>${esc(txt(item.label, "网页报表"))}</strong>
          <div class="${kindClass}">${esc(kind)}</div>
          <div class="${kindClass}">${esc(txt(item.path, "--"))}</div>
        </div>
        <div class="report-tags">
          <span class="tag">K 线回放</span>
          <span class="tag">开平仓标记</span>
          <span class="tag warn">止损线</span>
        </div>
      </div>
      <div class="report-preview">
        <iframe src="${url}" loading="lazy" referrerpolicy="no-referrer"></iframe>
      </div>
      <a href="${url}" target="_blank" rel="noreferrer">新窗口打开</a>
    </article>`;
  }).join("");
}"""


def render_shared_report_metrics_js() -> str:
    return """function formatMetricLines(metrics, options = {}) {
  const payload = obj(metrics);
  const lines = [];
  const includeWinRate = options.includeWinRate === true;
  if (num(payload.final_equity, null) !== null) {
    lines.push(`最终权益：${num(payload.final_equity).toLocaleString("zh-CN", { maximumFractionDigits: 2 })}`);
  }
  if (num(payload.total_return_pct, null) !== null) {
    lines.push(`总收益：${num(payload.total_return_pct).toFixed(2)}%`);
  }
  if (num(payload.max_drawdown_pct, null) !== null) {
    lines.push(`最大回撤：${num(payload.max_drawdown_pct).toFixed(2)}%`);
  }
  if (num(payload.trade_count, null) !== null) {
    lines.push(`交易笔数：${num(payload.trade_count).toFixed(0)}`);
  }
  if (includeWinRate && num(payload.win_rate_pct, null) !== null) {
    lines.push(`胜率：${num(payload.win_rate_pct).toFixed(2)}%`);
  }
  return lines.length ? lines.join(" | ") : JSON.stringify(payload, null, 0);
}"""
