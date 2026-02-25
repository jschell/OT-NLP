# pipeline/visualize/report.py
"""Pipeline run summary chart."""

from __future__ import annotations

import plotly.graph_objects as go


def pipeline_summary_chart(
    row_counts: dict[str, int],
    run_history: list[dict] | None = None,
    title: str = "Pipeline Row Counts by Table",
) -> go.Figure:
    """Bar chart of row counts with optional recent run history table.

    Args:
        row_counts: {table_name: row_count} mapping for all pipeline tables.
        run_history: Optional list of pipeline_runs rows as dicts; if provided
            a second trace or annotation can be added (currently reserved for
            future use, accepted but not rendered).
        title: Chart title.

    Returns:
        Plotly Figure containing one Bar trace, one bar per table in row_counts.
    """
    tables = list(row_counts.keys())
    counts = [row_counts[t] for t in tables]

    fig = go.Figure(
        data=go.Bar(
            x=tables,
            y=counts,
            marker_color="#0f3460",
            text=counts,
            textposition="outside",
            hovertemplate="%{x}: %{y:,} rows<extra></extra>",
        )
    )
    fig.update_layout(
        title=title,
        xaxis_title="Table",
        yaxis_title="Rows",
        template="plotly_white",
        height=350,
    )
    return fig
