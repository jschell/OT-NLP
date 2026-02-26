# pipeline/visualize/arcs.py
"""Arc diagram for chiastic structure analysis.

Renders a linear colon axis with quadratic Bezier arcs connecting matched
colon pairs. Arc colour encodes pattern type; stroke width encodes confidence.
"""

from __future__ import annotations

import plotly.graph_objects as go

_PATTERN_COLORS: dict[str, str] = {
    "ABBA": "#e94560",
    "ABCBA": "#0f3460",
    "AB": "#2e8b57",
}
_DEFAULT_COLOR = "#888888"


def chiasm_arc_figure(
    verse_labels: list[str],
    arc_pairs: list[tuple[int, int, float]],
    pattern_types: list[str],
    title: str = "Chiastic Structure Arc Diagram",
) -> go.Figure:
    """Arc diagram showing chiastic pattern matches with confidence overlay.

    Args:
        verse_labels: Label for each colon position on the x-axis
            (e.g. ["v1c1", "v1c2", "v2c1", ...]).
        arc_pairs: List of (start_idx, end_idx, confidence) tuples. Each
            tuple produces exactly one arc shape in the figure.
        pattern_types: Pattern type string per arc_pair in the same order
            (e.g. "ABBA", "ABCBA", "AB"). Must have same length as arc_pairs.
        title: Chart title.

    Returns:
        Plotly Figure. len(fig.layout.shapes) == len(arc_pairs).
    """
    fig = go.Figure()

    n = len(verse_labels)
    x_positions = list(range(n))

    # Baseline — use a Scatter trace so it does not count in fig.layout.shapes
    if n > 0:
        fig.add_trace(
            go.Scatter(
                x=[0, max(x_positions)],
                y=[0, 0],
                mode="lines",
                name="",
                showlegend=False,
                line=dict(color="lightgray", width=2),
                hoverinfo="skip",
            )
        )

    # Tick annotations for verse labels
    for i, label in enumerate(verse_labels):
        fig.add_annotation(
            x=i,
            y=-0.12,
            text=label,
            showarrow=False,
            font=dict(size=9, color="gray"),
        )

    # Arc shapes — one per arc_pair
    for (start_idx, end_idx, confidence), pattern in zip(
        arc_pairs, pattern_types, strict=True
    ):
        if start_idx >= n or end_idx >= n:
            continue
        color = _PATTERN_COLORS.get(pattern, _DEFAULT_COLOR)
        line_width = max(1, round(float(confidence) * 5))
        x_a = float(start_idx)
        x_b = float(end_idx)
        x_mid = (x_a + x_b) / 2.0
        arc_height = abs(x_b - x_a) * 0.35

        fig.add_shape(
            type="path",
            path=(f"M {x_a:.3f} 0 Q {x_mid:.3f} {arc_height:.3f} {x_b:.3f} 0"),
            line=dict(color=color, width=line_width),
            opacity=max(0.4, float(confidence)),
        )

    # Legend traces (invisible points, one per known pattern type)
    seen_patterns: set[str] = set(pattern_types)
    for pattern in sorted(seen_patterns):
        color = _PATTERN_COLORS.get(pattern, _DEFAULT_COLOR)
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="lines",
                name=pattern,
                line=dict(color=color, width=2),
            )
        )

    x_max = max(x_positions) + 1 if x_positions else 2
    fig.update_layout(
        title=title,
        xaxis=dict(
            range=[-0.5, x_max - 0.5],
            tickvals=list(range(n)),
            ticktext=verse_labels,
            title="Colon position",
        ),
        yaxis=dict(
            range=[-0.3, max(0.5, x_max * 0.35)],
            visible=False,
        ),
        template="plotly_white",
        height=350,
        showlegend=bool(seen_patterns),
    )
    return fig
