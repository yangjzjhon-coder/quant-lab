from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from quant_lab.reporting.market_chart import (
    PLOTLY_CONFIG,
    build_market_structure_figure,
    build_trade_logic_review_html,
    find_companion_path,
    read_market_bars,
    read_signals,
)


def render_dashboard(
    summary_path: Path,
    equity_curve_path: Path,
    trades_path: Path,
    output_path: Path,
    title: str,
) -> None:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    equity_curve = pd.read_csv(equity_curve_path, parse_dates=["timestamp"])
    trades = _read_trades(trades_path)
    signals_path = find_companion_path(summary_path, "signals.csv")
    execution_bars_path = find_companion_path(summary_path, "execution_bars.csv")
    allocation_overlay_path = find_companion_path(summary_path, "allocation_overlay.csv")
    signals = read_signals(signals_path) if signals_path is not None else None
    execution_bars = read_market_bars(execution_bars_path) if execution_bars_path is not None else None
    allocation_overlay = _read_allocation_overlay(allocation_overlay_path) if allocation_overlay_path is not None else None

    equity_curve = equity_curve.sort_values("timestamp").reset_index(drop=True)
    equity_vis = _downsample_equity_curve(equity_curve)
    equity_vis["drawdown_pct"] = ((equity_vis["equity"] / equity_vis["equity"].cummax()) - 1.0) * 100

    # Keep dashboards self-contained so browser previews and iframe embeds do
    # not depend on cdn.plot.ly being reachable.
    include_plotly = True
    market_chart_html = ""
    trade_logic_review_html = ""
    if signals is not None and execution_bars is not None:
        market_chart_html = build_market_structure_figure(
            signals=signals,
            execution_bars=execution_bars,
            trades=trades,
            title=title,
        ).to_html(
            full_html=False,
            include_plotlyjs=include_plotly,
            config=PLOTLY_CONFIG,
        )
        trade_logic_review_html = build_trade_logic_review_html(signals=signals, trades=trades)
        include_plotly = False

    equity_html = _build_equity_figure(equity_vis, title).to_html(
        full_html=False,
        include_plotlyjs=include_plotly,
        config=PLOTLY_CONFIG,
    )
    drawdown_html = _build_drawdown_figure(equity_vis).to_html(
        full_html=False,
        include_plotlyjs=False,
        config=PLOTLY_CONFIG,
    )
    trades_html = _build_trade_pnl_figure(trades).to_html(
        full_html=False,
        include_plotlyjs=False,
        config=PLOTLY_CONFIG,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        _dashboard_document(
            title=title,
            summary=summary,
            summary_path=summary_path,
            equity_curve_path=equity_curve_path,
            trades_path=trades_path,
            trades=trades,
            signals_path=signals_path,
            execution_bars_path=execution_bars_path,
            market_chart_html=market_chart_html,
            trade_logic_review_html=trade_logic_review_html,
            allocation_overlay_path=allocation_overlay_path,
            allocation_overlay=allocation_overlay,
            equity_html=equity_html,
            drawdown_html=drawdown_html,
            trades_html=trades_html,
        ),
        encoding="utf-8",
    )


def _build_equity_figure(equity_curve: pd.DataFrame, title: str) -> go.Figure:
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=equity_curve["timestamp"],
            y=equity_curve["equity"],
            mode="lines",
            name="权益",
            line={"color": "#1f77b4", "width": 2},
        )
    )
    figure.add_trace(
        go.Scatter(
            x=equity_curve["timestamp"],
            y=equity_curve["cash"],
            mode="lines",
            name="现金",
            line={"color": "#7f8c8d", "width": 1, "dash": "dot"},
        )
    )
    figure.update_layout(
        title=f"{title} 权益曲线",
        template="plotly_white",
        height=440,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
    )
    figure.update_yaxes(title="USDT")
    figure.update_xaxes(title="时间（UTC）")
    return figure


def _build_drawdown_figure(equity_curve: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=equity_curve["timestamp"],
            y=equity_curve["drawdown_pct"],
            mode="lines",
            name="回撤",
            fill="tozeroy",
            line={"color": "#c0392b", "width": 2},
        )
    )
    figure.update_layout(
        title="回撤曲线",
        template="plotly_white",
        height=340,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
        showlegend=False,
    )
    figure.update_yaxes(title="%", ticksuffix="%")
    figure.update_xaxes(title="时间（UTC）")
    return figure


def _build_trade_pnl_figure(trades: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if trades.empty:
        figure.add_annotation(
            text="当前报表区间内没有成交记录。",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font={"size": 16},
        )
    else:
        symbol_labels = trades["symbol"] if "symbol" in trades.columns else pd.Series([""] * len(trades))
        colors = ["#2ecc71" if value >= 0 else "#e74c3c" for value in trades["net_pnl"]]
        labels = [
            f"{symbol + ' | ' if symbol else ''}{side} | {entry} -> {exit}"
            for symbol, side, entry, exit in zip(
                symbol_labels.fillna("").astype(str),
                trades["side"],
                trades["entry_time"].dt.strftime("%Y-%m-%d %H:%M"),
                trades["exit_time"].dt.strftime("%Y-%m-%d %H:%M"),
                strict=False,
            )
        ]
        figure.add_trace(
            go.Bar(
                x=labels,
                y=trades["net_pnl"],
                marker_color=colors,
                name="净盈亏",
                hovertemplate="%{x}<br>净盈亏：%{y:.2f} USDT<extra></extra>",
            )
        )

    figure.update_layout(
        title="单笔交易净盈亏",
        template="plotly_white",
        height=360,
        margin={"l": 40, "r": 20, "t": 60, "b": 120},
        showlegend=False,
    )
    figure.update_yaxes(title="USDT")
    figure.update_xaxes(title="交易", tickangle=-35)
    return figure


def _dashboard_document(
    title: str,
    summary: dict[str, object],
    summary_path: Path,
    equity_curve_path: Path,
    trades_path: Path,
    trades: pd.DataFrame,
    signals_path: Path | None,
    execution_bars_path: Path | None,
    market_chart_html: str,
    trade_logic_review_html: str,
    allocation_overlay_path: Path | None,
    allocation_overlay: pd.DataFrame | None,
    equity_html: str,
    drawdown_html: str,
    trades_html: str,
) -> str:
    cards = "\n".join(_summary_cards(summary))
    summary_table = _summary_table(summary)

    path_lines = [
        f"<div>摘要：{summary_path}</div>",
        f"<div>权益曲线：{equity_curve_path}</div>",
        f"<div>成交明细：{trades_path}</div>",
    ]
    if signals_path is not None:
        path_lines.append(f"<div>信号明细：{signals_path}</div>")
    if execution_bars_path is not None:
        path_lines.append(f"<div>执行 K 线：{execution_bars_path}</div>")
    if allocation_overlay_path is not None:
        path_lines.append(f"<div>仓位覆盖：{allocation_overlay_path}</div>")
    paths_html = "\n        ".join(path_lines)

    market_section = ""
    if market_chart_html:
        market_section = f"""
    <section class="panel chart-panel">
      <div class="section-title">TradingView 风格多周期 K 线回放</div>
      <p class="panel-note">
        上半区展示信号周期逻辑，下半区回放执行周期 K 线。
        图中会标记做多触发、开仓、平仓与止损线。
        你可以通过顶部按钮在默认视图、完整逻辑层和纯回放视图之间切换。
      </p>
      <div class="logic-pills">
        <span>趋势过滤</span>
        <span>合成形态</span>
        <span>执行回放</span>
        <span>止损区间</span>
      </div>
      {market_chart_html}
    </section>
"""
    else:
        missing = []
        if signals_path is None:
            missing.append("signals.csv")
        if execution_bars_path is None:
            missing.append("execution_bars.csv")
        missing_text = "、".join(missing) if missing else "K 线伴随文件"
        market_section = f"""
    <section class="panel">
      <div class="section-title">多周期 K 线回放</div>
      <p class="panel-note">
        当前报表缺少 {missing_text}，因此暂时只能展示权益与成交统计，还不能生成 K 线回放。
        需要在回测产物中同时写出信号明细和执行 K 线，主页与报表页才会出现完整图表。
      </p>
    </section>
"""

    logic_review_section = ""
    if trade_logic_review_html:
        logic_review_section = f"""
    <section class="panel">
      <div class="section-title">交易逻辑复盘</div>
      <p class="logic-review-note">
        每一笔交易都会拆成趋势、形态、风险和退出四组门槛，方便逐行审计开仓决策与止损设置。
      </p>
      {trade_logic_review_html}
    </section>
"""

    trade_navigator_section = ""
    if market_chart_html and not trades.empty:
        trade_navigator_section = _trade_navigator_section_html(trades)

    allocation_section = ""
    if allocation_overlay is not None and not allocation_overlay.empty:
        allocation_section = _allocation_section_html(allocation_overlay)

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title} 回测看板</title>
  <style>
    :root {{
      --bg: #f7f8fa;
      --card: #ffffff;
      --border: #dfe4ea;
      --text: #1f2933;
      --muted: #6b7280;
      --accent: #0f766e;
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      padding: 32px;
      font-family: "Segoe UI", "PingFang SC", "Noto Sans SC", sans-serif;
      background: linear-gradient(180deg, #eef5f3 0%, var(--bg) 260px);
      color: var(--text);
    }}
    .shell {{
      max-width: 1320px;
      margin: 0 auto;
    }}
    .hero {{
      display: flex;
      justify-content: space-between;
      align-items: end;
      gap: 24px;
      margin-bottom: 24px;
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 32px;
    }}
    .hero p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.6;
    }}
    .paths {{
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
      text-align: right;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-bottom: 20px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 16px 18px;
      box-shadow: 0 8px 30px rgba(15, 23, 42, 0.05);
    }}
    .card .label {{
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 8px;
    }}
    .card .value {{
      font-size: 24px;
      font-weight: 700;
    }}
    .panel {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 20px;
      padding: 18px;
      margin-bottom: 20px;
      box-shadow: 0 8px 30px rgba(15, 23, 42, 0.05);
    }}
    .chart-panel {{
      background: linear-gradient(180deg, #222936 0%, #1e222d 100%);
      border-color: #394150;
      color: #b2b5be;
    }}
    .panel-note {{
      margin: 0 0 12px;
      color: inherit;
      opacity: 0.92;
      line-height: 1.6;
    }}
    .logic-pills {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 12px;
    }}
    .logic-pills span {{
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.08);
      border: 1px solid rgba(255, 255, 255, 0.08);
      font-size: 12px;
    }}
    .logic-review-note {{
      margin: 0 0 14px;
      color: var(--muted);
      line-height: 1.6;
    }}
    .logic-review-empty {{
      margin: 0;
      color: var(--muted);
    }}
    .trade-navigator {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 12px;
      margin-bottom: 14px;
    }}
    .trade-navigator select {{
      min-width: 280px;
      padding: 10px 12px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: #fff;
      color: var(--text);
    }}
    .trade-navigator label {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 13px;
    }}
    .trade-navigator-actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    .trade-navigator button {{
      border: 1px solid var(--border);
      background: #fff;
      color: var(--text);
      border-radius: 10px;
      padding: 10px 14px;
      cursor: pointer;
      font-weight: 600;
    }}
    .trade-focus-summary {{
      color: var(--muted);
      line-height: 1.6;
      font-size: 13px;
      padding: 12px 14px;
      border-radius: 14px;
      background: #f8fafc;
      border: 1px solid #e5e7eb;
    }}
    .logic-review-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    .logic-review-table th,
    .logic-review-table td {{
      padding: 12px 10px;
      border-bottom: 1px solid #edf2f7;
      vertical-align: top;
      text-align: left;
      line-height: 1.55;
    }}
    .logic-review-table th {{
      color: var(--muted);
      font-weight: 600;
      background: #fafbfc;
      position: sticky;
      top: 0;
    }}
    .logic-pass,
    .logic-fail {{
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.02em;
    }}
    .logic-pass {{
      background: rgba(34, 171, 148, 0.12);
      color: #0f766e;
    }}
    .logic-fail {{
      background: rgba(242, 54, 69, 0.12);
      color: #b42318;
    }}
    .trade-review-row-active {{
      background: #f0fdf4;
      box-shadow: inset 3px 0 0 #16a34a;
    }}
    .trade-review-row-hidden {{
      display: none;
    }}
    .summary-grid {{
      display: grid;
      grid-template-columns: minmax(0, 2fr) minmax(320px, 1fr);
      gap: 20px;
    }}
    .summary-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    .summary-table td {{
      padding: 10px 0;
      border-bottom: 1px solid #edf2f7;
    }}
    .summary-table td:first-child {{
      color: var(--muted);
      width: 45%;
    }}
    .section-title {{
      font-size: 18px;
      font-weight: 700;
      margin: 0 0 12px;
    }}
    @media (max-width: 900px) {{
      body {{
        padding: 20px;
      }}
      .hero {{
        flex-direction: column;
        align-items: start;
      }}
      .paths {{
        text-align: left;
      }}
      .summary-grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div>
        <h1>{title}</h1>
        <p>
          本地 HTML 回测看板。摘要、权益与成交统计会始终渲染；当伴随的信号明细和执行 K 线存在时，会自动补上多周期 K 线回放。
        </p>
      </div>
      <div class="paths">
        {paths_html}
      </div>
    </section>

    <section class="cards">
      {cards}
    </section>

    {market_section}

    {trade_navigator_section}

    <section class="summary-grid">
      <div class="panel">
        <div class="section-title">权益曲线</div>
        {equity_html}
      </div>
      <div class="panel">
        <div class="section-title">摘要</div>
        {summary_table}
      </div>
    </section>

    {logic_review_section}

    {allocation_section}

    <section class="panel">
      <div class="section-title">回撤</div>
      {drawdown_html}
    </section>

    <section class="panel">
      <div class="section-title">单笔交易净盈亏</div>
      {trades_html}
    </section>
  </div>
  {_trade_navigator_script(trades)}
</body>
</html>
"""


def _trade_navigator_section_html(trades: pd.DataFrame) -> str:
    options = ['<option value="">全部交易</option>']
    for trade_id, trade in _trade_navigator_records(trades).items():
        options.append(
            f'<option value="{trade_id}">{trade["label"]}</option>'
        )
    options_html = "\n          ".join(options)
    return f"""
    <section class="panel">
      <div class="section-title">交易定位器</div>
      <p class="logic-review-note">
        可以直接跳到单笔交易，缩放到持仓时间窗，并按需把复盘表过滤到该笔交易。
      </p>
      <div class="trade-navigator">
        <select id="trade-focus-select" aria-label="交易选择器">
          {options_html}
        </select>
        <label>
          <input type="checkbox" id="trade-filter-toggle" checked />
          仅显示选中交易的复盘行
        </label>
        <div class="trade-navigator-actions">
          <button type="button" id="trade-focus-button">聚焦交易</button>
          <button type="button" id="trade-locate-button">定位表格</button>
          <button type="button" id="trade-reset-button">重置视图</button>
        </div>
      </div>
      <div class="trade-focus-summary" id="trade-focus-summary">
        选择一笔交易后，这里会汇总它的开仓、平仓、止损与净盈亏。
      </div>
    </section>
"""


def _trade_navigator_script(trades: pd.DataFrame) -> str:
    if trades.empty:
        return ""
    payload = json.dumps(_trade_navigator_records(trades), ensure_ascii=False)
    return f"""
  <script>
    (() => {{
      const tradeMap = {payload};
      const select = document.getElementById("trade-focus-select");
      const filterToggle = document.getElementById("trade-filter-toggle");
      const focusButton = document.getElementById("trade-focus-button");
      const locateButton = document.getElementById("trade-locate-button");
      const resetButton = document.getElementById("trade-reset-button");
      const summary = document.getElementById("trade-focus-summary");
      const chartDiv = document.querySelector(".chart-panel .plotly-graph-div");
      const rows = Array.from(document.querySelectorAll(".trade-review-row"));

      if (!select || !summary) {{
        return;
      }}

      const asTime = (value) => {{
        if (!value) {{
          return null;
        }}
        const ts = new Date(value).getTime();
        return Number.isFinite(ts) ? ts : null;
      }};

      const updateSummary = (tradeId) => {{
        if (!tradeId || !tradeMap[tradeId]) {{
          summary.textContent = "选择一笔交易后，这里会汇总它的开仓、平仓、止损与净盈亏。";
          return;
        }}
        const trade = tradeMap[tradeId];
        summary.textContent =
          `${{trade.trade_id}} | ${{trade.symbol || "N/A"}} | ${{trade.side}} | ` +
          `开仓 ${{trade.entry_time_label}} @ ${{trade.entry_price}} | ` +
          `平仓 ${{trade.exit_time_label}} @ ${{trade.exit_price}} | ` +
          `止损 ${{trade.stop_price}} | 净盈亏 ${{trade.net_pnl}}`;
      }};

      const updateRows = (tradeId, scrollIntoView) => {{
        rows.forEach((row) => {{
          const active = Boolean(tradeId) && row.dataset.tradeId === tradeId;
          const shouldHide = Boolean(tradeId) && filterToggle && filterToggle.checked && !active;
          row.classList.toggle("trade-review-row-active", active);
          row.classList.toggle("trade-review-row-hidden", shouldHide);
        }});
        if (scrollIntoView && tradeId) {{
          const targetRow = document.getElementById(`trade-review-${{tradeId}}`);
          if (targetRow) {{
            targetRow.scrollIntoView({{ behavior: "smooth", block: "center" }});
          }}
        }}
      }};

      const focusChart = (tradeId, reset) => {{
        if (!chartDiv || typeof Plotly === "undefined") {{
          return;
        }}
        if (reset || !tradeId || !tradeMap[tradeId]) {{
          Plotly.relayout(chartDiv, {{
            "xaxis.autorange": true,
            "xaxis2.autorange": true,
            "yaxis.autorange": true,
            "yaxis2.autorange": true
          }});
          return;
        }}
        const trade = tradeMap[tradeId];
        const signalTime = asTime(trade.signal_time);
        const entryTime = asTime(trade.entry_time);
        const exitTime = asTime(trade.exit_time);
        const start = signalTime ?? entryTime;
        if (start === null || exitTime === null) {{
          return;
        }}
        const baseDuration = Math.max(exitTime - start, 4 * 60 * 60 * 1000);
        const pad = Math.max(Math.round(baseDuration * 0.75), 12 * 60 * 60 * 1000);
        const rangeStart = new Date(start - pad).toISOString();
        const rangeEnd = new Date(exitTime + pad).toISOString();
        Plotly.relayout(chartDiv, {{
          "xaxis.range": [rangeStart, rangeEnd],
          "xaxis2.range": [rangeStart, rangeEnd]
        }});
        chartDiv.scrollIntoView({{ behavior: "smooth", block: "center" }});
      }};

      const syncSelection = (options = {{}}) => {{
        const tradeId = select.value;
        updateSummary(tradeId);
        updateRows(tradeId, Boolean(options.scrollRow));
        if (options.focusChart) {{
          focusChart(tradeId, false);
        }}
      }};

      select.addEventListener("change", () => syncSelection({{ focusChart: Boolean(select.value), scrollRow: Boolean(select.value) }}));
      if (filterToggle) {{
        filterToggle.addEventListener("change", () => syncSelection());
      }}
      if (focusButton) {{
        focusButton.addEventListener("click", () => syncSelection({{ focusChart: Boolean(select.value), scrollRow: false }}));
      }}
      if (locateButton) {{
        locateButton.addEventListener("click", () => syncSelection({{ focusChart: false, scrollRow: Boolean(select.value) }}));
      }}
      if (resetButton) {{
        resetButton.addEventListener("click", () => {{
          select.value = "";
          updateSummary("");
          updateRows("", false);
          focusChart("", true);
        }});
      }}

      updateSummary("");
      updateRows("", false);
    }})();
  </script>
"""


def _trade_navigator_records(trades: pd.DataFrame) -> dict[str, dict[str, str]]:
    records: dict[str, dict[str, str]] = {}
    for index, trade in trades.iterrows():
        trade_id = f"T{index + 1}"
        side = _trade_side_label(trade.get("side"))
        symbol = str(trade.get("symbol") or "")
        entry_time = _to_iso_timestamp(trade.get("entry_time"))
        exit_time = _to_iso_timestamp(trade.get("exit_time"))
        signal_time = _to_iso_timestamp(trade.get("signal_time"))
        entry_label = _to_label_timestamp(trade.get("entry_time"))
        exit_label = _to_label_timestamp(trade.get("exit_time"))
        records[trade_id] = {
            "trade_id": trade_id,
            "label": f"{trade_id} | {symbol or 'N/A'} | {side} | {entry_label}",
            "symbol": symbol,
            "side": side,
            "signal_time": signal_time,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "entry_time_label": entry_label,
            "exit_time_label": exit_label,
            "entry_price": f"{_safe_trade_number(trade.get('entry_price')):.2f}",
            "exit_price": f"{_safe_trade_number(trade.get('exit_price')):.2f}",
            "stop_price": f"{_safe_trade_number(trade.get('stop_price')):.2f}",
            "net_pnl": f"{_safe_trade_number(trade.get('net_pnl')):.2f}",
        }
    return records


def _to_iso_timestamp(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return pd.Timestamp(value).isoformat()


def _to_label_timestamp(value: object) -> str:
    if value is None:
        return "无"
    try:
        if pd.isna(value):
            return "无"
    except TypeError:
        pass
    return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M")


def _trade_side_label(value: object) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"long", "buy"}:
        return "多头"
    if raw in {"short", "sell"}:
        return "空头"
    return str(value or "无")


def _safe_trade_number(value: object) -> float:
    try:
        if pd.isna(value):
            return 0.0
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _summary_cards(summary: dict[str, object]) -> list[str]:
    metrics = [
        ("最终权益", _format_number(summary.get("final_equity"), suffix=" USDT")),
        ("总收益", _format_number(summary.get("total_return_pct"), suffix="%")),
        ("最大回撤", _format_number(summary.get("max_drawdown_pct"), suffix="%")),
        ("交易笔数", _format_number(summary.get("trade_count"))),
        ("胜率", _format_number(summary.get("win_rate_pct"), suffix="%")),
        ("夏普比率", _format_number(summary.get("sharpe"))),
    ]
    if summary.get("symbol_count") is not None:
        metrics.append(("标的数量", _format_number(summary.get("symbol_count"))))
    return [
        (
            '<div class="card">'
            f'<div class="label">{label}</div>'
            f'<div class="value">{value}</div>'
            "</div>"
        )
        for label, value in metrics
    ]


def _summary_table(summary: dict[str, object]) -> str:
    rows = [
        ("初始权益", _format_number(summary.get("initial_equity"), suffix=" USDT")),
        ("最终权益", _format_number(summary.get("final_equity"), suffix=" USDT")),
        ("总收益", _format_number(summary.get("total_return_pct"), suffix="%")),
        ("年化收益", _format_number(summary.get("annualized_return_pct"), suffix="%")),
        ("最大回撤", _format_number(summary.get("max_drawdown_pct"), suffix="%")),
        ("交易笔数", _format_number(summary.get("trade_count"))),
        ("胜率", _format_number(summary.get("win_rate_pct"), suffix="%")),
        ("盈亏比", _format_number(summary.get("profit_factor"))),
        ("夏普比率", _format_number(summary.get("sharpe"))),
    ]
    if summary.get("symbol_count") is not None:
        rows.append(("标的数量", _format_number(summary.get("symbol_count"))))
    if summary.get("allocation_mode") is not None:
        rows.append(("资金分配模式", _format_number(summary.get("allocation_mode"))))
    if summary.get("portfolio_construction") is not None:
        rows.append(("组合构建方式", _format_number(summary.get("portfolio_construction"))))
    if summary.get("capital_allocator") is not None:
        rows.append(("资金分配器", _format_number(summary.get("capital_allocator"))))
    if summary.get("per_symbol_initial_equity") is not None:
        rows.append(
            (
                "单标的初始资金",
                _format_number(summary.get("per_symbol_initial_equity"), suffix=" USDT"),
            )
        )
    if summary.get("runtime_allocation_reference") is not None:
        rows.append(("运行时分配参考", _format_number(summary.get("runtime_allocation_reference"))))
    if summary.get("allocation_note") is not None:
        rows.append(("分配说明", _format_number(summary.get("allocation_note"))))
    if summary.get("historical_allocation_overlay") is not None:
        rows.append(("历史仓位覆盖", _format_number(summary.get("historical_allocation_overlay"))))
    if summary.get("historical_requested_risk_pct_avg") is not None:
        rows.append(("历史申请风险均值", _format_number(summary.get("historical_requested_risk_pct_avg"), suffix="%")))
    if summary.get("historical_allocated_risk_pct_avg") is not None:
        rows.append(("历史分配风险均值", _format_number(summary.get("historical_allocated_risk_pct_avg"), suffix="%")))
    if summary.get("historical_allocated_risk_pct_max") is not None:
        rows.append(("历史分配风险峰值", _format_number(summary.get("historical_allocated_risk_pct_max"), suffix="%")))
    if summary.get("historical_bull_trend_symbol_count_avg") is not None:
        rows.append(("历史多头标的均值", _format_number(summary.get("historical_bull_trend_symbol_count_avg"))))
    if summary.get("historical_range_symbol_count_avg") is not None:
        rows.append(("历史震荡标的均值", _format_number(summary.get("historical_range_symbol_count_avg"))))
    if summary.get("routing_mode") is not None:
        rows.append(("路由模式", _format_number(summary.get("routing_mode"))))
    if summary.get("routing_candidate_bar_pct") is not None:
        rows.append(("候选策略覆盖 K 线占比", _format_number(summary.get("routing_candidate_bar_pct"), suffix="%")))
    if summary.get("routing_fallback_bar_pct") is not None:
        rows.append(("回退配置覆盖 K 线占比", _format_number(summary.get("routing_fallback_bar_pct"), suffix="%")))
    if summary.get("routing_flat_bar_pct") is not None:
        rows.append(("空仓 K 线占比", _format_number(summary.get("routing_flat_bar_pct"), suffix="%")))
    body = "\n".join(
        f"<tr><td>{label}</td><td>{value}</td></tr>"
        for label, value in rows
    )
    return f'<table class="summary-table"><tbody>{body}</tbody></table>'


def _read_trades(trades_path: Path) -> pd.DataFrame:
    if trades_path.stat().st_size == 0:
        return pd.DataFrame(
            columns=[
                "signal_time",
                "entry_time",
                "exit_time",
                "side",
                "contracts",
                "entry_price",
                "exit_price",
                "stop_price",
                "gross_pnl",
                "funding_pnl",
                "fee_paid",
                "net_pnl",
                "exit_reason",
                "symbol",
            ]
        )

    trades = pd.read_csv(trades_path)
    if trades.empty:
        return trades

    for column in ("signal_time", "entry_time", "exit_time"):
        if column in trades.columns:
            trades[column] = pd.to_datetime(trades[column], utc=True)
    return trades


def _read_allocation_overlay(path: Path) -> pd.DataFrame:
    overlay = pd.read_csv(path)
    if overlay.empty:
        return overlay
    if "timestamp" in overlay.columns:
        overlay["timestamp"] = pd.to_datetime(overlay["timestamp"], utc=True)
    for column in (
        "requested_total_risk_fraction",
        "allocated_total_risk_fraction",
        "active_symbol_count",
        "allocated_symbol_count",
        "bull_trend_symbol_count",
        "bear_trend_symbol_count",
        "range_symbol_count",
    ):
        if column in overlay.columns:
            overlay[column] = pd.to_numeric(overlay[column], errors="coerce")
    return overlay.sort_values("timestamp").reset_index(drop=True)


def _allocation_section_html(allocation_overlay: pd.DataFrame) -> str:
    latest = allocation_overlay.iloc[-1]
    risk_rows = [
        ("申请风险", _format_number(_fraction_to_pct(latest.get("requested_total_risk_fraction")), suffix="%")),
        ("实际分配风险", _format_number(_fraction_to_pct(latest.get("allocated_total_risk_fraction")), suffix="%")),
        ("活跃标的数", _format_number(latest.get("active_symbol_count"))),
        ("已分配标的数", _format_number(latest.get("allocated_symbol_count"))),
    ]
    regime_rows = [
        ("多头标的数", _format_number(latest.get("bull_trend_symbol_count"))),
        ("空头标的数", _format_number(latest.get("bear_trend_symbol_count"))),
        ("震荡标的数", _format_number(latest.get("range_symbol_count"))),
        ("主导状态", _format_number(latest.get("dominant_regime"))),
    ]
    risk_body = "\n".join(
        f"<tr><td>{label}</td><td>{value}</td></tr>"
        for label, value in risk_rows
        if value != "N/A"
    )
    regime_body = "\n".join(
        f"<tr><td>{label}</td><td>{value}</td></tr>"
        for label, value in regime_rows
        if value != "N/A"
    )
    return f"""
    <section class="summary-grid">
      <div class="panel">
        <div class="section-title">组合风险预算</div>
        <p class="panel-note">
          已检测到 `allocation_overlay.csv`。这里展示的是回测过程中最近一次记录下来的组合风险预算状态。
        </p>
        <table class="summary-table"><tbody>{risk_body}</tbody></table>
      </div>
      <div class="panel">
        <div class="section-title">最新分配状态</div>
        <div class="section-title" style="font-size:14px;color:#6b7280;margin-top:12px;">状态分布</div>
        <table class="summary-table"><tbody>{regime_body}</tbody></table>
      </div>
    </section>
"""


def _downsample_equity_curve(equity_curve: pd.DataFrame) -> pd.DataFrame:
    if len(equity_curve) <= 12_000:
        return equity_curve.copy()

    frame = equity_curve.set_index("timestamp")
    if len(frame) > 400_000:
        rule = "4h"
    elif len(frame) > 150_000:
        rule = "1h"
    elif len(frame) > 50_000:
        rule = "15min"
    else:
        rule = "5min"

    reduced = frame.resample(rule).last().dropna().reset_index()
    return reduced


def _fraction_to_pct(value: object) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value) * 100.0
    except (TypeError, ValueError):
        return None


def _format_number(value: object, suffix: str = "") -> str:
    if value is None:
        return "N/A"
    if isinstance(value, int):
        return f"{value}{suffix}"
    if isinstance(value, float):
        return f"{value:,.2f}{suffix}"
    return f"{value}{suffix}"
