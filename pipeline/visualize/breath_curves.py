# pipeline/visualize/breath_curves.py
"""Breath curve overlay chart.

Shows the per-syllable breath weight of the Hebrew source alongside
one or more English translations (stress-mapped) and optional suggestions.
"""

from __future__ import annotations

import plotly.graph_objects as go


def breath_curve_figure(
    verse_id: int,
    hebrew_curve: list[float],
    translation_curves: dict[str, list[float]],
    suggestion_curves: dict[str, list[float]] | None = None,
    title: str = "",
) -> go.Figure:
    """Overlay breath weight curves for Hebrew and translations.

    Args:
        verse_id: Database verse_id (used for chart title when title not given).
        hebrew_curve: Per-syllable breath weights from breath_profiles.breath_curve.
        translation_curves: {translation_key: [per-syllable English weights]}.
        suggestion_curves: Optional {label: [per-syllable weights]} for LLM suggestions.
        title: Chart title; defaults to "Breath Curve — verse {verse_id}".

    Returns:
        Plotly Figure with one Scatter trace per curve.
    """
    fig = go.Figure()

    chart_title = title or f"Breath Curve — verse {verse_id}"

    # Hebrew trace — bold, dark
    n_heb = len(hebrew_curve)
    x_heb = [i / max(n_heb - 1, 1) for i in range(n_heb)] if n_heb else []
    fig.add_trace(
        go.Scatter(
            x=x_heb,
            y=hebrew_curve,
            mode="lines",
            name="Hebrew (source)",
            line=dict(color="#1a1a2e", width=3),
            hovertemplate=(
                "Position: %{x:.2f}<br>Weight: %{y:.3f}<extra>Hebrew</extra>"
            ),
        )
    )

    # Translation curves
    _COLORS = ["#e94560", "#0f3460", "#533483", "#2e8b57", "#cd853f"]
    for idx, (key, curve) in enumerate(translation_curves.items()):
        n = len(curve)
        if n == 0:
            continue
        x = [i / max(n - 1, 1) for i in range(n)]
        fig.add_trace(
            go.Scatter(
                x=x,
                y=curve,
                mode="lines",
                name=key,
                line=dict(
                    color=_COLORS[idx % len(_COLORS)],
                    width=2,
                    dash="dot",
                ),
                hovertemplate=(
                    f"Position: %{{x:.2f}}<br>Weight: %{{y:.3f}}<extra>{key}</extra>"
                ),
            )
        )

    # Suggestion curves
    if suggestion_curves:
        for _idx, (label, curve) in enumerate(suggestion_curves.items()):
            n = len(curve)
            if n == 0:
                continue
            x = [i / max(n - 1, 1) for i in range(n)]
            fig.add_trace(
                go.Scatter(
                    x=x,
                    y=curve,
                    mode="lines",
                    name=f"Suggestion: {label}",
                    line=dict(color="#ffd700", width=2, dash="dash"),
                )
            )

    fig.update_layout(
        title=chart_title,
        xaxis_title="Relative position in verse (0–1)",
        yaxis_title="Breath weight (0–1)",
        yaxis=dict(range=[0, 1.05]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
        template="plotly_white",
        height=400,
    )
    return fig
