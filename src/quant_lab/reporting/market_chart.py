from __future__ import annotations

import html
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


TV_COLORS = {
    "bg": "#131722",
    "panel": "#1e222d",
    "grid": "#2a2e39",
    "text": "#b2b5be",
    "bull": "#22ab94",
    "bear": "#f23645",
    "blue": "#2962ff",
    "violet": "#7c3aed",
    "amber": "#f59e0b",
    "sky": "#38bdf8",
    "green": "#4ade80",
}

PLOTLY_CONFIG = {
    "displaylogo": False,
    "responsive": True,
    "scrollZoom": True,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
}


def find_companion_path(summary_path: Path, suffix: str) -> Path | None:
    candidates: list[Path] = []
    if summary_path.name.endswith("_summary.json"):
        prefix = summary_path.name[: -len("_summary.json")]
        candidates.append(summary_path.parent / f"{prefix}_{suffix}")
    candidates.append(summary_path.parent / suffix)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def read_signals(signals_path: Path) -> pd.DataFrame:
    signals = pd.read_csv(signals_path)
    if signals.empty:
        return signals

    for column in ("timestamp", "action_time"):
        if column in signals.columns:
            signals[column] = pd.to_datetime(signals[column], utc=True)
    for column in (
        "open",
        "high",
        "low",
        "close",
        "stop_loss_price",
        "signal_entry_price",
        "signal_leverage",
        "ema_12",
        "ema_144",
        "ema_169",
        "ema_575",
        "ema_676",
    ):
        if column in signals.columns:
            signals[column] = pd.to_numeric(signals[column], errors="coerce")
    for column in (
        "open_long_signal",
        "take_profit_signal",
        "synthetic_setup",
        "synthetic_high_touches_ema12",
        "synthetic_low_touches_ema144",
        "window_close_above_ema12",
        "allow_long",
        "fresh_setup_signal",
        "current_close_above_ema12",
    ):
        if column in signals.columns:
            signals[column] = signals[column].map(_coerce_bool)
    return signals.sort_values("timestamp").reset_index(drop=True)


def read_market_bars(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if frame.empty:
        return frame
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return (
        frame.dropna(subset=["timestamp", "open", "high", "low", "close"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def build_market_structure_figure(
    *,
    signals: pd.DataFrame,
    execution_bars: pd.DataFrame,
    trades: pd.DataFrame,
    title: str,
) -> go.Figure:
    daily = signals.sort_values("timestamp").reset_index(drop=True)
    execution = execution_bars.sort_values("timestamp").reset_index(drop=True)
    projected_daily = _project_daily_indicators(execution, daily)
    trace_groups: list[str] = []

    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.44, 0.56],
        subplot_titles=("信号周期逻辑", "执行周期回放"),
    )
    
    def add_grouped_trace(trace: go.BaseTraceType, *, row: int, col: int, group: str) -> None:
        figure.add_trace(trace, row=row, col=col)
        trace_groups.append(group)

    add_grouped_trace(
        go.Candlestick(
            x=daily["timestamp"],
            open=daily["open"],
            high=daily["high"],
            low=daily["low"],
            close=daily["close"],
            name="信号周期 K 线",
            increasing_line_color=TV_COLORS["bull"],
            increasing_fillcolor=TV_COLORS["bull"],
            decreasing_line_color=TV_COLORS["bear"],
            decreasing_fillcolor=TV_COLORS["bear"],
            showlegend=False,
        ),
        row=1,
        col=1,
        group="core",
    )

    daily_ema_styles = {
        "ema_12": ("EMA12", TV_COLORS["amber"], True, "ema_primary"),
        "ema_144": ("EMA144", TV_COLORS["sky"], True, "ema_primary"),
        "ema_169": ("EMA169", TV_COLORS["violet"], "legendonly", "ema_secondary"),
        "ema_575": ("EMA575", "#64748b", "legendonly", "ema_secondary"),
        "ema_676": ("EMA676", "#94a3b8", "legendonly", "ema_secondary"),
    }
    for column, (label, color, visible, group) in daily_ema_styles.items():
        if column not in daily.columns:
            continue
        add_grouped_trace(
            go.Scatter(
                x=daily["timestamp"],
                y=daily[column],
                mode="lines",
                name=label,
                line={"color": color, "width": 1.3},
                visible=visible,
                legendgroup="daily-ema",
            ),
            row=1,
            col=1,
            group=group,
        )

    _add_signal_logic_trace(
        figure=figure,
        trace_groups=trace_groups,
        frame=daily,
        flag_column="open_long_signal",
        y_column="close",
        name="做多触发",
        color=TV_COLORS["green"],
        symbol="triangle-up",
        row=1,
        logic_hover=True,
        group="core",
    )
    _add_signal_logic_trace(
        figure=figure,
        trace_groups=trace_groups,
        frame=daily,
        flag_column="take_profit_signal",
        y_column="close",
        name="止盈触发",
        color=TV_COLORS["bear"],
        symbol="x",
        row=1,
        logic_hover=False,
        group="core",
    )
    _add_signal_logic_trace(
        figure=figure,
        trace_groups=trace_groups,
        frame=daily,
        flag_column="synthetic_high_touches_ema12",
        y_column="high",
        name="触碰 EMA12",
        color=TV_COLORS["amber"],
        symbol="circle-open",
        row=1,
        visible="legendonly",
        logic_hover=True,
        group="logic",
    )
    _add_signal_logic_trace(
        figure=figure,
        trace_groups=trace_groups,
        frame=daily,
        flag_column="synthetic_low_touches_ema144",
        y_column="low",
        name="触碰 EMA144",
        color=TV_COLORS["sky"],
        symbol="circle-open",
        row=1,
        visible="legendonly",
        logic_hover=True,
        group="logic",
    )
    _add_signal_logic_trace(
        figure=figure,
        trace_groups=trace_groups,
        frame=daily,
        flag_column="window_close_above_ema12",
        y_column="close",
        name="窗口收盘高于 EMA12",
        color=TV_COLORS["green"],
        symbol="diamond-open",
        row=1,
        visible="legendonly",
        logic_hover=True,
        group="logic",
    )
    _add_signal_logic_trace(
        figure=figure,
        trace_groups=trace_groups,
        frame=daily,
        flag_column="allow_long",
        y_column="low",
        name="趋势过滤通过",
        color=TV_COLORS["violet"],
        symbol="square-open",
        row=1,
        visible="legendonly",
        logic_hover=True,
        group="logic",
    )

    add_grouped_trace(
        go.Candlestick(
            x=execution["timestamp"],
            open=execution["open"],
            high=execution["high"],
            low=execution["low"],
            close=execution["close"],
            name="执行周期 K 线",
            increasing_line_color=TV_COLORS["bull"],
            increasing_fillcolor=TV_COLORS["bull"],
            decreasing_line_color=TV_COLORS["bear"],
            decreasing_fillcolor=TV_COLORS["bear"],
            showlegend=False,
        ),
        row=2,
        col=1,
        group="core",
    )

    execution_overlay_styles = {
        "ema_12": ("EMA12 投影", TV_COLORS["amber"], True, "ema_primary"),
        "ema_144": ("EMA144 投影", TV_COLORS["sky"], True, "ema_primary"),
        "ema_169": ("EMA169 投影", TV_COLORS["violet"], "legendonly", "ema_secondary"),
    }
    for column, (label, color, visible, group) in execution_overlay_styles.items():
        if column not in projected_daily.columns:
            continue
        add_grouped_trace(
            go.Scatter(
                x=projected_daily["timestamp"],
                y=projected_daily[column],
                mode="lines",
                name=label,
                line={"color": color, "width": 1.2, "dash": "dot"},
                visible=visible,
                hovertemplate=f"{label}: %{{y:.2f}}<extra></extra>",
                legendgroup="execution-ema",
            ),
            row=2,
            col=1,
            group=group,
        )

    if not trades.empty:
        trade_ids = [f"T{i + 1}" for i in range(len(trades))]
        entry_hover = [
            _trade_entry_hover_text(trade_id=trade_id, row=row)
            for trade_id, (_, row) in zip(trade_ids, trades.iterrows(), strict=False)
        ]
        exit_hover = [
            _trade_exit_hover_text(trade_id=trade_id, row=row)
            for trade_id, (_, row) in zip(trade_ids, trades.iterrows(), strict=False)
        ]
        add_grouped_trace(
            go.Scatter(
                x=trades["entry_time"],
                y=trades["entry_price"],
                mode="markers+text",
                name="开仓",
                text=trade_ids,
                textposition="top center",
                marker={
                    "size": 13,
                    "symbol": "triangle-up",
                    "color": TV_COLORS["green"],
                    "line": {"color": TV_COLORS["panel"], "width": 1},
                },
                hovertext=entry_hover,
                hovertemplate="%{hovertext}<extra></extra>",
                legendgroup="trades",
            ),
            row=2,
            col=1,
            group="core",
        )
        add_grouped_trace(
            go.Scatter(
                x=trades["exit_time"],
                y=trades["exit_price"],
                mode="markers+text",
                name="平仓",
                text=trade_ids,
                textposition="bottom center",
                marker={
                    "size": 12,
                    "symbol": "x",
                    "color": TV_COLORS["bear"],
                    "line": {"color": TV_COLORS["panel"], "width": 1},
                },
                hovertext=exit_hover,
                hovertemplate="%{hovertext}<extra></extra>",
                legendgroup="trades",
            ),
            row=2,
            col=1,
            group="core",
        )
        for index, (_, row) in enumerate(trades.iterrows()):
            stop_price = pd.to_numeric(row.get("stop_price"), errors="coerce")
            if pd.isna(stop_price):
                continue
            add_grouped_trace(
                go.Scatter(
                    x=[row["entry_time"], row["exit_time"]],
                    y=[stop_price, stop_price],
                    mode="lines",
                    name="止损线",
                    line={"color": "#ff6b6b", "width": 1.4, "dash": "dash"},
                    showlegend=index == 0,
                    hovertemplate=(
                        f"T{index + 1}<br>止损：{float(stop_price):.2f}"
                        f"<br>区间：{pd.Timestamp(row['entry_time']).strftime('%Y-%m-%d %H:%M')} -> "
                        f"{pd.Timestamp(row['exit_time']).strftime('%Y-%m-%d %H:%M')}"
                        "<extra></extra>"
                    ),
                    legendgroup="trades",
                ),
                row=2,
                col=1,
                group="core",
            )

    figure.update_layout(
        updatemenus=_build_visibility_buttons(trace_groups),
    )

    figure.update_layout(
        title=f"{title} | TradingView 风格多周期 K 线回放",
        height=1100,
        margin={"l": 50, "r": 25, "t": 70, "b": 30},
        dragmode="pan",
        hovermode="x unified",
        paper_bgcolor=TV_COLORS["panel"],
        plot_bgcolor=TV_COLORS["bg"],
        font={"color": TV_COLORS["text"], "family": "Trebuchet MS, Segoe UI, sans-serif"},
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.01,
            "xanchor": "left",
            "x": 0,
            "bgcolor": "rgba(0,0,0,0)",
        },
    )
    figure.update_xaxes(
        showgrid=True,
        gridcolor=TV_COLORS["grid"],
        rangeslider_visible=False,
        showspikes=True,
        spikemode="across",
        spikesnap="cursor",
        spikecolor=TV_COLORS["text"],
        spikethickness=1,
        row=1,
        col=1,
    )
    figure.update_xaxes(
        showgrid=True,
        gridcolor=TV_COLORS["grid"],
        rangeslider_visible=False,
        showspikes=True,
        spikemode="across",
        spikesnap="cursor",
        spikecolor=TV_COLORS["text"],
        spikethickness=1,
        row=2,
        col=1,
    )
    figure.update_yaxes(
        showgrid=True,
        gridcolor=TV_COLORS["grid"],
        zeroline=False,
        title="信号周期价格",
        row=1,
        col=1,
    )
    figure.update_yaxes(
        showgrid=True,
        gridcolor=TV_COLORS["grid"],
        zeroline=False,
        title="执行周期价格",
        row=2,
        col=1,
    )
    return figure


def _project_daily_indicators(execution_bars: pd.DataFrame, signals: pd.DataFrame) -> pd.DataFrame:
    columns = [column for column in ("ema_12", "ema_144", "ema_169") if column in signals.columns]
    if not columns:
        return execution_bars[["timestamp"]].copy()
    return pd.merge_asof(
        execution_bars[["timestamp"]].sort_values("timestamp"),
        signals[["timestamp", *columns]].sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )


def _add_signal_logic_trace(
    *,
    figure: go.Figure,
    trace_groups: list[str],
    frame: pd.DataFrame,
    flag_column: str,
    y_column: str,
    name: str,
    color: str,
    symbol: str,
    row: int,
    visible: bool | str = True,
    logic_hover: bool,
    group: str,
) -> None:
    if flag_column not in frame.columns or y_column not in frame.columns:
        return
    mask = frame[flag_column].fillna(False)
    if not mask.any():
        return
    subset = frame.loc[mask].copy()
    hover_text = (
        subset.apply(_signal_hover_text, axis=1)
        if logic_hover
        else subset.apply(lambda item: _simple_signal_hover_text(name=name, row=item), axis=1)
    )
    figure.add_trace(
        go.Scatter(
            x=subset["timestamp"],
            y=subset[y_column],
            mode="markers",
            name=name,
            visible=visible,
            legendgroup="logic" if group == "logic" else "core-logic",
            marker={
                "size": 10,
                "symbol": symbol,
                "color": color,
                "line": {"color": TV_COLORS["panel"], "width": 1},
            },
            hovertext=hover_text,
            hovertemplate="%{hovertext}<extra></extra>",
        ),
        row=row,
        col=1,
    )
    trace_groups.append(group)


def build_trade_logic_review_html(*, signals: pd.DataFrame, trades: pd.DataFrame) -> str:
    if trades.empty or signals.empty or "timestamp" not in signals.columns:
        return "<p class=\"logic-review-empty\">当前报表没有可复盘的交易逻辑明细。</p>"

    signal_frame = signals.sort_values("timestamp").reset_index(drop=True)
    rows: list[str] = []
    for index, trade in trades.iterrows():
        signal_row = _match_signal_row(signal_frame, trade)
        if signal_row is None:
            continue
        rows.append(
            _logic_review_row_html(
                trade_id=f"T{index + 1}",
                trade=trade,
                signal_row=signal_row,
            )
        )
    if not rows:
        return "<p class=\"logic-review-empty\">虽然存在成交记录，但没有找到对应的信号行。</p>"
    header = """
<thead>
  <tr>
    <th>交易</th>
    <th>信号 / 开仓</th>
    <th>趋势门槛</th>
    <th>形态门槛</th>
    <th>风险</th>
    <th>退出</th>
  </tr>
</thead>
"""
    body = "\n".join(rows)
    return f'<table class="logic-review-table">{header}<tbody>{body}</tbody></table>'


def _build_visibility_buttons(trace_groups: list[str]) -> list[dict[str, object]]:
    default_groups = {"core", "ema_primary"}
    logic_groups = {"core", "ema_primary", "ema_secondary", "logic"}
    replay_groups = {"core"}
    return [
        {
            "type": "buttons",
            "direction": "left",
            "x": 0,
            "y": 1.14,
            "xanchor": "left",
            "yanchor": "top",
            "showactive": True,
            "bgcolor": "rgba(30,34,45,0.85)",
            "bordercolor": "#394150",
            "font": {"color": TV_COLORS["text"]},
            "buttons": [
                {
                    "label": "默认视图",
                    "method": "update",
                    "args": [{"visible": _visibility_mask(trace_groups, default_groups)}],
                },
                {
                    "label": "完整逻辑层",
                    "method": "update",
                    "args": [{"visible": _visibility_mask(trace_groups, logic_groups)}],
                },
                {
                    "label": "纯回放视图",
                    "method": "update",
                    "args": [{"visible": _visibility_mask(trace_groups, replay_groups)}],
                },
            ],
        }
    ]


def _visibility_mask(trace_groups: list[str], active_groups: set[str]) -> list[bool]:
    return [group in active_groups for group in trace_groups]


def _match_signal_row(signals: pd.DataFrame, trade: pd.Series) -> pd.Series | None:
    if "signal_time" not in trade or pd.isna(trade["signal_time"]):
        return None
    signal_time = pd.Timestamp(trade["signal_time"])
    matches = signals.loc[signals["timestamp"] == signal_time]
    if not matches.empty:
        return matches.iloc[-1]
    prior = signals.loc[signals["timestamp"] <= signal_time]
    if prior.empty:
        return None
    return prior.iloc[-1]


def _logic_review_row_html(*, trade_id: str, trade: pd.Series, signal_row: pd.Series) -> str:
    signal_time = pd.Timestamp(signal_row["timestamp"]).strftime("%Y-%m-%d %H:%M")
    entry_time = pd.Timestamp(trade["entry_time"]).strftime("%Y-%m-%d %H:%M")
    exit_time = pd.Timestamp(trade["exit_time"]).strftime("%Y-%m-%d %H:%M")
    trend_gate = (
        f"趋势状态：{html.escape(_format_value(signal_row.get('trend_state')))}<br>"
        f"允许做多：{_logic_badge(_coerce_bool(signal_row.get('allow_long')))}"
    )
    setup_gate = (
        f"新鲜形态：{_logic_badge(_coerce_bool(signal_row.get('fresh_setup_signal')))}<br>"
        f"触碰 EMA12：{_logic_badge(_coerce_bool(signal_row.get('synthetic_high_touches_ema12')))}<br>"
        f"触碰 EMA144：{_logic_badge(_coerce_bool(signal_row.get('synthetic_low_touches_ema144')))}<br>"
        f"收盘高于 EMA12：{_logic_badge(_coerce_bool(signal_row.get('window_close_above_ema12')))}<br>"
        f"当前收盘高于 EMA12：{_logic_badge(_coerce_bool(signal_row.get('current_close_above_ema12')))}"
    )
    risk_block = (
        f"开仓价：{_safe_float(trade.get('entry_price')):.2f}<br>"
        f"止损价：{_safe_float(trade.get('stop_price')):.2f}<br>"
        f"杠杆：{_safe_float(trade.get('leverage')):.2f}x"
    )
    exit_block = (
        f"平仓价：{_safe_float(trade.get('exit_price')):.2f}<br>"
        f"退出原因：{html.escape(_format_value(trade.get('exit_reason')))}<br>"
        f"净盈亏：{_safe_float(trade.get('net_pnl')):.2f}"
    )
    return f"""
<tr class="trade-review-row" id="trade-review-{trade_id}" data-trade-id="{trade_id}">
  <td><strong>{trade_id}</strong></td>
  <td>信号：{signal_time}<br>开仓：{entry_time}<br>平仓：{exit_time}</td>
  <td>{trend_gate}</td>
  <td>{setup_gate}</td>
  <td>{risk_block}</td>
  <td>{exit_block}</td>
</tr>
"""


def _logic_badge(ok: bool) -> str:
    label = "通过" if ok else "失败"
    css = "logic-pass" if ok else "logic-fail"
    return f'<span class="{css}">{label}</span>'


def _signal_hover_text(row: pd.Series) -> str:
    timestamp = pd.Timestamp(row["timestamp"]).strftime("%Y-%m-%d %H:%M")
    parts = [
        f"<b>信号 K 线</b>：{timestamp}",
        f"趋势状态：{_format_value(row.get('trend_state'))}",
        f"允许做多：{_yes_no(row.get('allow_long'))}",
        f"形态成立：{_yes_no(row.get('synthetic_setup'))}",
        f"新鲜形态：{_yes_no(row.get('fresh_setup_signal'))}",
        f"触碰 EMA12：{_yes_no(row.get('synthetic_high_touches_ema12'))}",
        f"触碰 EMA144：{_yes_no(row.get('synthetic_low_touches_ema144'))}",
        f"窗口收盘高于 EMA12：{_yes_no(row.get('window_close_above_ema12'))}",
        f"当前收盘高于 EMA12：{_yes_no(row.get('current_close_above_ema12'))}",
    ]
    stop_price = pd.to_numeric(row.get("stop_loss_price"), errors="coerce")
    signal_leverage = pd.to_numeric(row.get("signal_leverage"), errors="coerce")
    if not pd.isna(stop_price):
        parts.append(f"止损价：{float(stop_price):.2f}")
    if not pd.isna(signal_leverage):
        parts.append(f"信号杠杆：{float(signal_leverage):.2f}x")
    return "<br>".join(parts)


def _simple_signal_hover_text(*, name: str, row: pd.Series) -> str:
    timestamp = pd.Timestamp(row["timestamp"]).strftime("%Y-%m-%d %H:%M")
    return f"<b>{name}</b><br>信号 K 线：{timestamp}<br>收盘价：{_format_value(row.get('close'))}"


def _trade_entry_hover_text(*, trade_id: str, row: pd.Series) -> str:
    parts = [
        f"<b>{trade_id} 开仓</b>",
        f"时间：{pd.Timestamp(row['entry_time']).strftime('%Y-%m-%d %H:%M')}",
        f"信号时间：{pd.Timestamp(row['signal_time']).strftime('%Y-%m-%d %H:%M')}",
        f"开仓价：{float(row['entry_price']):.2f}",
    ]
    stop_price = pd.to_numeric(row.get("stop_price"), errors="coerce")
    leverage = pd.to_numeric(row.get("leverage"), errors="coerce")
    if not pd.isna(stop_price):
        parts.append(f"止损价：{float(stop_price):.2f}")
    if not pd.isna(leverage):
        parts.append(f"杠杆：{float(leverage):.2f}x")
    return "<br>".join(parts)


def _trade_exit_hover_text(*, trade_id: str, row: pd.Series) -> str:
    parts = [
        f"<b>{trade_id} 平仓</b>",
        f"时间：{pd.Timestamp(row['exit_time']).strftime('%Y-%m-%d %H:%M')}",
        f"平仓价：{float(row['exit_price']):.2f}",
        f"退出原因：{_format_value(row.get('exit_reason'))}",
    ]
    net_pnl = pd.to_numeric(row.get("net_pnl"), errors="coerce")
    if not pd.isna(net_pnl):
        parts.append(f"净盈亏：{float(net_pnl):.2f} USDT")
    return "<br>".join(parts)


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except TypeError:
        pass
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _yes_no(value: object) -> str:
    return "是" if _coerce_bool(value) else "否"


def _format_value(value: object) -> str:
    if value is None:
        return "N/A"
    try:
        if pd.isna(value):
            return "N/A"
    except TypeError:
        pass
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)


def _safe_float(value: object) -> float:
    try:
        if pd.isna(value):
            return 0.0
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
