from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go


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

    equity_curve = equity_curve.sort_values("timestamp").reset_index(drop=True)
    equity_vis = _downsample_equity_curve(equity_curve)
    equity_vis["drawdown_pct"] = ((equity_vis["equity"] / equity_vis["equity"].cummax()) - 1.0) * 100

    equity_html = _build_equity_figure(equity_vis, title).to_html(
        full_html=False,
        include_plotlyjs="cdn",
        config={"displaylogo": False, "responsive": True},
    )
    drawdown_html = _build_drawdown_figure(equity_vis).to_html(
        full_html=False,
        include_plotlyjs=False,
        config={"displaylogo": False, "responsive": True},
    )
    trades_html = _build_trade_pnl_figure(trades).to_html(
        full_html=False,
        include_plotlyjs=False,
        config={"displaylogo": False, "responsive": True},
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        _dashboard_document(
            title=title,
            summary=summary,
            summary_path=summary_path,
            equity_curve_path=equity_curve_path,
            trades_path=trades_path,
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
            name="Equity",
            line={"color": "#1f77b4", "width": 2},
        )
    )
    figure.add_trace(
        go.Scatter(
            x=equity_curve["timestamp"],
            y=equity_curve["cash"],
            mode="lines",
            name="Cash",
            line={"color": "#7f8c8d", "width": 1, "dash": "dot"},
        )
    )
    figure.update_layout(
        title=f"{title} Equity Curve",
        template="plotly_white",
        height=440,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
    )
    figure.update_yaxes(title="USDT")
    figure.update_xaxes(title="Time (UTC)")
    return figure


def _build_drawdown_figure(equity_curve: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=equity_curve["timestamp"],
            y=equity_curve["drawdown_pct"],
            mode="lines",
            name="Drawdown",
            fill="tozeroy",
            line={"color": "#c0392b", "width": 2},
        )
    )
    figure.update_layout(
        title="Drawdown",
        template="plotly_white",
        height=340,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
        showlegend=False,
    )
    figure.update_yaxes(title="%", ticksuffix="%")
    figure.update_xaxes(title="Time (UTC)")
    return figure


def _build_trade_pnl_figure(trades: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if trades.empty:
        figure.add_annotation(
            text="No trades in this report range.",
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
                name="Net PnL",
                hovertemplate="%{x}<br>Net PnL: %{y:.2f} USDT<extra></extra>",
            )
        )

    figure.update_layout(
        title="Trade Net PnL",
        template="plotly_white",
        height=360,
        margin={"l": 40, "r": 20, "t": 60, "b": 120},
        showlegend=False,
    )
    figure.update_yaxes(title="USDT")
    figure.update_xaxes(title="Trade", tickangle=-35)
    return figure


def _dashboard_document(
    title: str,
    summary: dict[str, object],
    summary_path: Path,
    equity_curve_path: Path,
    trades_path: Path,
    equity_html: str,
    drawdown_html: str,
    trades_html: str,
) -> str:
    cards = "\n".join(_summary_cards(summary))
    summary_table = _summary_table(summary)

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title} Dashboard</title>
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
        <p>本地回测 HTML 仪表盘。当前数据来自已生成的 summary / equity / trades 报表文件。</p>
      </div>
      <div class="paths">
        <div>summary: {summary_path}</div>
        <div>equity: {equity_curve_path}</div>
        <div>trades: {trades_path}</div>
      </div>
    </section>

    <section class="cards">
      {cards}
    </section>

    <section class="summary-grid">
      <div class="panel">
        <div class="section-title">Equity Curve</div>
        {equity_html}
      </div>
      <div class="panel">
        <div class="section-title">Summary</div>
        {summary_table}
      </div>
    </section>

    <section class="panel">
      <div class="section-title">Drawdown</div>
      {drawdown_html}
    </section>

    <section class="panel">
      <div class="section-title">Trade PnL</div>
      {trades_html}
    </section>
  </div>
</body>
</html>
"""


def _summary_cards(summary: dict[str, object]) -> list[str]:
    metrics = [
        ("Final Equity", _format_number(summary.get("final_equity"), suffix=" USDT")),
        ("Total Return", _format_number(summary.get("total_return_pct"), suffix="%")),
        ("Max Drawdown", _format_number(summary.get("max_drawdown_pct"), suffix="%")),
        ("Trade Count", _format_number(summary.get("trade_count"))),
        ("Win Rate", _format_number(summary.get("win_rate_pct"), suffix="%")),
        ("Sharpe", _format_number(summary.get("sharpe"))),
    ]
    if summary.get("symbol_count") is not None:
        metrics.append(("Symbols", _format_number(summary.get("symbol_count"))))
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
        ("Initial Equity", _format_number(summary.get("initial_equity"), suffix=" USDT")),
        ("Final Equity", _format_number(summary.get("final_equity"), suffix=" USDT")),
        ("Total Return", _format_number(summary.get("total_return_pct"), suffix="%")),
        ("Annualized Return", _format_number(summary.get("annualized_return_pct"), suffix="%")),
        ("Max Drawdown", _format_number(summary.get("max_drawdown_pct"), suffix="%")),
        ("Trade Count", _format_number(summary.get("trade_count"))),
        ("Win Rate", _format_number(summary.get("win_rate_pct"), suffix="%")),
        ("Profit Factor", _format_number(summary.get("profit_factor"))),
        ("Sharpe", _format_number(summary.get("sharpe"))),
    ]
    if summary.get("symbol_count") is not None:
        rows.append(("Symbol Count", _format_number(summary.get("symbol_count"))))
    if summary.get("allocation_mode") is not None:
        rows.append(("Allocation Mode", _format_number(summary.get("allocation_mode"))))
    if summary.get("per_symbol_initial_equity") is not None:
        rows.append(
            (
                "Per Symbol Capital",
                _format_number(summary.get("per_symbol_initial_equity"), suffix=" USDT"),
            )
        )
    if summary.get("routing_mode") is not None:
        rows.append(("Routing Mode", _format_number(summary.get("routing_mode"))))
    if summary.get("routing_candidate_bar_pct") is not None:
        rows.append(("Candidate Bars", _format_number(summary.get("routing_candidate_bar_pct"), suffix="%")))
    if summary.get("routing_fallback_bar_pct") is not None:
        rows.append(("Fallback Bars", _format_number(summary.get("routing_fallback_bar_pct"), suffix="%")))
    if summary.get("routing_flat_bar_pct") is not None:
        rows.append(("Flat Bars", _format_number(summary.get("routing_flat_bar_pct"), suffix="%")))
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


def _format_number(value: object, suffix: str = "") -> str:
    if value is None:
        return "N/A"
    if isinstance(value, int):
        return f"{value}{suffix}"
    if isinstance(value, float):
        return f"{value:,.2f}{suffix}"
    return f"{value}{suffix}"
