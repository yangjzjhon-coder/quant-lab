from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go


def render_sweep_dashboard(results: pd.DataFrame, output_path: Path, title: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if results.empty:
        output_path.write_text("<html><body><h1>No sweep results.</h1></body></html>", encoding="utf-8")
        return

    top = results.head(12).copy()
    scatter_html = _build_scatter(results, title).to_html(
        full_html=False,
        include_plotlyjs="cdn",
        config={"displaylogo": False, "responsive": True},
    )
    heatmap_html = _build_heatmap(results).to_html(
        full_html=False,
        include_plotlyjs=False,
        config={"displaylogo": False, "responsive": True},
    )
    table_html = _build_table(top).to_html(
        full_html=False,
        include_plotlyjs=False,
        config={"displaylogo": False, "responsive": True},
    )

    output_path.write_text(
        _document(title=title, results=results, scatter_html=scatter_html, heatmap_html=heatmap_html, table_html=table_html),
        encoding="utf-8",
    )


def _build_scatter(results: pd.DataFrame, title: str) -> go.Figure:
    figure = go.Figure(
        data=[
            go.Scatter(
                x=results["max_drawdown_pct"],
                y=results["total_return_pct"],
                mode="markers",
                marker={
                    "size": results["trade_count"].clip(lower=1) * 3 + 8,
                    "color": results["sharpe"],
                    "colorscale": "Viridis",
                    "showscale": True,
                    "colorbar": {"title": "Sharpe"},
                },
                text=results.apply(
                    lambda row: (
                        f"EMA {int(row.fast_ema)}/{int(row.slow_ema)} | "
                        f"ATRx {row.atr_stop_multiple}<br>"
                        f"Return: {row.total_return_pct:.2f}%<br>"
                        f"DD: {row.max_drawdown_pct:.2f}%<br>"
                        f"Trades: {int(row.trade_count)}"
                    ),
                    axis=1,
                ),
                hovertemplate="%{text}<extra></extra>",
            )
        ]
    )
    figure.update_layout(
        title=f"{title} Sweep: Return vs Drawdown",
        template="plotly_white",
        height=430,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    figure.update_xaxes(title="Max Drawdown %")
    figure.update_yaxes(title="Total Return %")
    return figure


def _build_heatmap(results: pd.DataFrame) -> go.Figure:
    best = (
        results.sort_values(["sharpe", "total_return_pct"], ascending=[False, False])
        .drop_duplicates(subset=["fast_ema", "slow_ema"])
        .pivot(index="fast_ema", columns="slow_ema", values="sharpe")
        .sort_index()
    )

    figure = go.Figure(
        data=[
            go.Heatmap(
                z=best.values,
                x=[str(value) for value in best.columns],
                y=[str(value) for value in best.index],
                colorscale="YlGnBu",
                colorbar={"title": "Sharpe"},
                hovertemplate="Fast EMA %{y}<br>Slow EMA %{x}<br>Sharpe %{z:.2f}<extra></extra>",
            )
        ]
    )
    figure.update_layout(
        title="Best Sharpe by EMA Pair",
        template="plotly_white",
        height=380,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    figure.update_xaxes(title="Slow EMA")
    figure.update_yaxes(title="Fast EMA")
    return figure


def _build_table(results: pd.DataFrame) -> go.Figure:
    display = results.copy()
    for column in ("total_return_pct", "max_drawdown_pct", "win_rate_pct", "sharpe", "score_return_over_dd"):
        display[column] = display[column].map(lambda value: f"{value:.2f}")

    figure = go.Figure(
        data=[
            go.Table(
                header={
                    "values": [
                        "Fast",
                        "Slow",
                        "ATRx",
                        "Return %",
                        "Max DD %",
                        "Sharpe",
                        "Trades",
                        "Win Rate %",
                        "Score",
                    ],
                    "fill_color": "#0f766e",
                    "font": {"color": "white", "size": 13},
                    "align": "left",
                },
                cells={
                    "values": [
                        display["fast_ema"],
                        display["slow_ema"],
                        display["atr_stop_multiple"],
                        display["total_return_pct"],
                        display["max_drawdown_pct"],
                        display["sharpe"],
                        display["trade_count"],
                        display["win_rate_pct"],
                        display["score_return_over_dd"],
                    ],
                    "fill_color": "#ffffff",
                    "align": "left",
                    "height": 28,
                },
            )
        ]
    )
    figure.update_layout(
        title="Top Sweep Results",
        template="plotly_white",
        height=420,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
    )
    return figure


def _document(title: str, results: pd.DataFrame, scatter_html: str, heatmap_html: str, table_html: str) -> str:
    best = results.iloc[0]
    cards = [
        ("Combinations", f"{len(results)}"),
        ("Best EMA", f"{int(best.fast_ema)}/{int(best.slow_ema)}"),
        ("Best ATRx", f"{best.atr_stop_multiple:.2f}"),
        ("Best Return", f"{best.total_return_pct:.2f}%"),
        ("Best Sharpe", f"{best.sharpe:.2f}"),
        ("Best Max DD", f"{best.max_drawdown_pct:.2f}%"),
    ]
    cards_html = "\n".join(
        (
            '<div class="card">'
            f'<div class="label">{label}</div>'
            f'<div class="value">{value}</div>'
            "</div>"
        )
        for label, value in cards
    )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title} Sweep Dashboard</title>
  <style>
    body {{
      margin: 0;
      padding: 32px;
      font-family: "Segoe UI", "PingFang SC", "Noto Sans SC", sans-serif;
      color: #1f2933;
      background: linear-gradient(180deg, #f4faf8 0%, #f7f8fa 260px);
    }}
    .shell {{
      max-width: 1360px;
      margin: 0 auto;
    }}
    .hero {{
      margin-bottom: 20px;
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 32px;
    }}
    .hero p {{
      margin: 0;
      color: #6b7280;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-bottom: 20px;
    }}
    .card, .panel {{
      background: #ffffff;
      border: 1px solid #dfe4ea;
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 8px 30px rgba(15, 23, 42, 0.05);
    }}
    .label {{
      color: #6b7280;
      font-size: 13px;
      margin-bottom: 8px;
    }}
    .value {{
      font-size: 24px;
      font-weight: 700;
    }}
    .grid {{
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(0, 1fr);
      gap: 20px;
      margin-bottom: 20px;
    }}
    .section-title {{
      font-size: 18px;
      font-weight: 700;
      margin: 0 0 12px;
    }}
    @media (max-width: 960px) {{
      body {{
        padding: 20px;
      }}
      .grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>{title} Sweep Dashboard</h1>
      <p>用于比较 EMA 趋势参数组合的本地研究面板。</p>
    </section>
    <section class="cards">
      {cards_html}
    </section>
    <section class="grid">
      <div class="panel">
        <div class="section-title">Return vs Drawdown</div>
        {scatter_html}
      </div>
      <div class="panel">
        <div class="section-title">EMA Heatmap</div>
        {heatmap_html}
      </div>
    </section>
    <section class="panel">
      <div class="section-title">Top Results</div>
      {table_html}
    </section>
  </div>
</body>
</html>
"""
