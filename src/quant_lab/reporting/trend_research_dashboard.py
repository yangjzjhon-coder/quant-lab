from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go


def render_trend_research_dashboard(results: pd.DataFrame, output_path: Path, title: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if results.empty:
        output_path.write_text("<html><body><h1>暂无研究结果。</h1></body></html>", encoding="utf-8")
        return

    top = results.head(15).copy()
    scatter_html = _build_scatter(results, title).to_html(
        full_html=False,
        include_plotlyjs=True,
        config={"displaylogo": False, "responsive": True},
    )
    variant_html = _build_variant_bars(results).to_html(
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
        _document(title=title, results=results, scatter_html=scatter_html, variant_html=variant_html, table_html=table_html),
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
                    "size": results["research_score"] * 28 + 10,
                    "color": results["bear_return_pct"],
                    "colorscale": "Tealgrn",
                    "showscale": True,
                    "colorbar": {"title": "熊市收益 %"},
                    "line": {"width": 1, "color": "rgba(15,23,42,0.18)"},
                },
                customdata=results[
                    [
                        "variant",
                        "fast_ema",
                        "slow_ema",
                        "atr_stop_multiple",
                        "trend_ema",
                        "adx_threshold",
                        "research_score",
                        "sharpe",
                    ]
                ].values,
                hovertemplate=(
                    "变体 %{customdata[0]}<br>"
                    "EMA %{customdata[1]}/%{customdata[2]} | ATR 倍数 %{customdata[3]}<br>"
                    "趋势 EMA %{customdata[4]} | ADX %{customdata[5]}<br>"
                    "总收益 %{y:.2f}% | 最大回撤 %{x:.2f}%<br>"
                    "熊市收益 %{marker.color:.2f}%<br>"
                    "夏普 %{customdata[7]:.2f} | 研究评分 %{customdata[6]:.4f}<extra></extra>"
                ),
            )
        ]
    )
    figure.update_layout(
        title=f"{title}：收益 / 回撤 / 熊市收益",
        template="plotly_white",
        height=440,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
    )
    figure.update_xaxes(title="最大回撤 %")
    figure.update_yaxes(title="总收益 %")
    return figure


def _build_variant_bars(results: pd.DataFrame) -> go.Figure:
    best_by_variant = (
        results.sort_values(["research_score", "bear_return_pct"], ascending=[False, False])
        .drop_duplicates(subset=["variant"])
        .sort_values("research_score", ascending=False)
    )

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=best_by_variant["variant"],
            y=best_by_variant["research_score"],
            name="研究评分",
            marker_color="#0f766e",
            hovertemplate="变体 %{x}<br>研究评分 %{y:.4f}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Bar(
            x=best_by_variant["variant"],
            y=best_by_variant["bear_return_pct"],
            name="熊市收益 %",
            marker_color="#ca6702",
            hovertemplate="变体 %{x}<br>熊市收益 %{y:.2f}%<extra></extra>",
        )
    )
    figure.update_layout(
        title="各变体最佳候选",
        template="plotly_white",
        height=380,
        margin={"l": 40, "r": 20, "t": 60, "b": 40},
        barmode="group",
    )
    return figure


def _build_table(results: pd.DataFrame) -> go.Figure:
    display = results.copy()
    for column in (
        "atr_stop_multiple",
        "adx_threshold",
        "total_return_pct",
        "bear_return_pct",
        "max_drawdown_pct",
        "sharpe",
        "research_score",
    ):
        display[column] = display[column].map(lambda value: f"{value:.2f}" if value is not None else "--")

    figure = go.Figure(
        data=[
            go.Table(
                header={
                    "values": [
                        "变体",
                        "快 EMA",
                        "慢 EMA",
                        "ATR 倍数",
                        "趋势 EMA",
                        "ADX",
                        "收益 %",
                        "熊市收益 %",
                        "最大回撤 %",
                        "夏普",
                        "研究评分",
                    ],
                    "fill_color": "#0f766e",
                    "font": {"color": "white", "size": 13},
                    "align": "left",
                },
                cells={
                    "values": [
                        display["variant"],
                        display["fast_ema"],
                        display["slow_ema"],
                        display["atr_stop_multiple"],
                        display["trend_ema"],
                        display["adx_threshold"],
                        display["total_return_pct"],
                        display["bear_return_pct"],
                        display["max_drawdown_pct"],
                        display["sharpe"],
                        display["research_score"],
                    ],
                    "fill_color": "#ffffff",
                    "align": "left",
                    "height": 28,
                },
            )
        ]
    )
    figure.update_layout(
        title="最佳研究结果",
        template="plotly_white",
        height=460,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
    )
    return figure


def _document(title: str, results: pd.DataFrame, scatter_html: str, variant_html: str, table_html: str) -> str:
    best = results.iloc[0]
    cards = [
        ("候选数量", f"{len(results)}"),
        ("最佳变体", str(best.variant)),
        ("最佳 EMA", f"{int(best.fast_ema)}/{int(best.slow_ema)}"),
        ("最佳 ATR 倍数", f"{best.atr_stop_multiple:.2f}"),
        ("最佳熊市收益", f"{best.bear_return_pct:.2f}%"),
        ("最佳最大回撤", f"{best.max_drawdown_pct:.2f}%"),
        ("研究评分", f"{best.research_score:.4f}"),
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
  <title>{title} 趋势研究看板</title>
  <style>
    body {{
      margin: 0;
      padding: 32px;
      font-family: "Segoe UI", "PingFang SC", "Noto Sans SC", sans-serif;
      color: #1f2933;
      background: linear-gradient(180deg, #f4faf8 0%, #f7f8fa 280px);
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
      line-height: 1.7;
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
      grid-template-columns: minmax(0, 1.2fr) minmax(0, 1fr);
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
      <h1>{title}</h1>
      <p>趋势研究看板会同时强调收益、最大回撤、熊市收益与研究评分，适合用来挑选更稳健的候选策略。</p>
    </section>
    <section class="cards">
      {cards_html}
    </section>
    <section class="grid">
      <div class="panel">
        <div class="section-title">收益 / 回撤 / 熊市收益</div>
        {scatter_html}
      </div>
      <div class="panel">
        <div class="section-title">变体对比</div>
        {variant_html}
      </div>
    </section>
    <section class="panel">
      <div class="section-title">最佳结果</div>
      {table_html}
    </section>
  </div>
</body>
</html>
"""
