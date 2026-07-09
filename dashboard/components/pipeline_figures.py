"""Chart builders for the Pipeline Health dashboard page."""
from __future__ import annotations

import plotly.graph_objects as go
import polars as pl

from dashboard.components import theme_utils as tu
from dashboard.data.pipeline_transforms import (
    dag_avg_duration,
    dag_run_timeline,
    failing_tests,
    test_pass_rate_trend,
)


def _empty(msg: str, height: int = 400, template: str = "terminal_amber") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        **tu.base_layout(template),
        height=height,
        annotations=[dict(
            text=msg, xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color=tu.empty_color(template)),
        )],
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig


def dag_timeline_bar(pipeline: pl.DataFrame, template: str = "terminal_amber") -> go.Figure:
    """Stacked bar of successful vs failed runs per DAG per day."""
    if pipeline.is_empty():
        return _empty("No pipeline run data available", template=template)

    data = dag_run_timeline(pipeline)
    dag_ids = data["dag_id"].unique().sort().to_list()
    pal = tu.palette(template)
    fig = go.Figure()

    for i, dag_id in enumerate(dag_ids):
        subset = data.filter(pl.col("dag_id") == dag_id)
        fig.add_trace(go.Bar(
            name=f"{dag_id} ✓",
            x=subset["run_date"].to_list(),
            y=subset["successful_runs"].to_list(),
            marker_color=pal[i % len(pal)],
            legendgroup=dag_id,
            hovertemplate=f"{dag_id}<br>%{{x}}<br>%{{y}} successful<extra></extra>",
        ))
        fig.add_trace(go.Bar(
            name=f"{dag_id} ✗",
            x=subset["run_date"].to_list(),
            y=subset["failed_runs"].to_list(),
            marker_color=tu.bad_color(template),
            marker_opacity=0.6,
            legendgroup=dag_id,
            showlegend=False,
            hovertemplate=f"{dag_id}<br>%{{x}}<br>%{{y}} failed<extra></extra>",
        ))

    fig.update_layout(
        **tu.base_layout(template),
        title="DAG run outcomes — daily",
        barmode="stack", height=380,
        xaxis_title=None, yaxis_title="Runs",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def task_duration_bar(pipeline: pl.DataFrame, template: str = "terminal_amber") -> go.Figure:
    """Horizontal bar of avg run duration by DAG."""
    if pipeline.is_empty():
        return _empty("No duration data available", 300, template)

    data = dag_avg_duration(pipeline)
    fig = go.Figure(go.Bar(
        x=data["avg_duration_seconds"].to_list(),
        y=data["dag_id"].to_list(),
        orientation="h",
        marker_color=tu.palette(template)[0],
        hovertemplate="%{y}<br>avg %{x:.0f}s<extra></extra>",
    ))
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=180, r=20, t=50, b=40)),
        title="Avg run duration by DAG (seconds)",
        height=max(250, 60 + len(data) * 40),
        xaxis_title="Seconds", yaxis_title=None,
    )
    return fig


def test_pass_rate_line(tests: pl.DataFrame, template: str = "terminal_amber") -> go.Figure:
    """Line chart of dbt test pass rate per model over time."""
    if tests.is_empty():
        return _empty("No dbt test data available", template=template)

    data = test_pass_rate_trend(tests)
    if data.is_empty():
        return _empty("No dbt test data available", template=template)

    pal = tu.palette(template)
    models = data["model_name"].unique().sort().to_list()
    fig = go.Figure()
    for i, model in enumerate(models):
        subset = data.filter(pl.col("model_name") == model)
        fig.add_trace(go.Scatter(
            x=subset["run_date"].to_list(),
            y=subset["pass_rate_pct"].to_list(),
            name=model, mode="lines+markers",
            line={"color": pal[i % len(pal)], "width": 2},
            hovertemplate=f"{model}<br>%{{x}}<br>%{{y:.1f}}% pass<extra></extra>",
        ))

    fig.update_layout(
        **tu.base_layout(template),
        title="dbt test pass rate by model",
        height=380,
        yaxis=dict(title="Pass rate %", range=[0, 105], gridcolor=tu.grid_color(template)),
        xaxis_title=None,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def failing_tests_table(tests: pl.DataFrame, template: str = "terminal_amber") -> go.Figure:
    """Table of most recent test failures."""
    data = failing_tests(tests)
    if data.is_empty():
        return _empty("All tests passing ✓", 200, template)

    hdr = tu.table_header(template)
    cells = tu.table_cells(template)

    fig = go.Figure(go.Table(
        header=dict(
            values=["Date", "Test", "Model", "Column", "Failures", "30d pass%"],
            fill_color=hdr["fill_color"],
            font=dict(color=hdr["font_color"], size=11),
            align="left", line_color=hdr["line_color"],
        ),
        cells=dict(
            values=[
                data["run_date"].cast(pl.Utf8).to_list(),
                data["test_name"].to_list(),
                data["model_name"].to_list(),
                [c or "—" for c in data["column_name"].to_list()],
                data["failures"].to_list(),
                [f"{v:.1f}%" if v is not None else "—" for v in data["pass_rate_30d"].to_list()],
            ],
            fill_color=cells["fill_color"],
            font=dict(color=cells["font_color"], size=10),
            align="left", line_color=cells["line_color"],
        ),
    ))
    fig.update_layout(
        **tu.base_layout(template, margin=dict(l=10, r=10, t=50, b=10)),
        title="Recent test failures",
        height=max(200, 80 + len(data) * 30),
    )
    return fig
