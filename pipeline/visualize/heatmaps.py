# pipeline/visualize/heatmaps.py
"""Deviation heatmap and syllable openness heatmap."""

from __future__ import annotations

import plotly.graph_objects as go


def deviation_heatmap(
    psalm_chapters: list[int],
    translation_keys: list[str],
    scores: list[list[float]],
) -> go.Figure:
    """Heatmap of mean composite_deviation per Psalm chapter x translation.

    Args:
        psalm_chapters: Ordered list of Psalm chapter numbers (y-axis).
        translation_keys: Ordered list of translation identifiers (x-axis).
        scores: 2-D matrix [chapters][translations] of mean composite_deviation.

    Returns:
        Plotly Figure containing a single Heatmap trace.
    """
    if not psalm_chapters or not translation_keys or not scores:
        return go.Figure()

    y_labels = [f"Psalm {c}" for c in psalm_chapters]

    fig = go.Figure(
        data=go.Heatmap(
            z=scores,
            x=translation_keys,
            y=y_labels,
            colorscale="RdYlGn_r",
            colorbar=dict(title="Mean Deviation"),
            hovertemplate=(
                "Psalm %{y} | %{x}<br>Deviation: %{z:.4f}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        title="Style Deviation by Psalm x Translation",
        xaxis_title="Translation",
        yaxis_title="Psalm Chapter",
        template="plotly_white",
        height=max(400, len(psalm_chapters) * 8),
    )
    return fig


def syllable_openness_heatmap(
    verse_ids: list[int],
    syllable_openness_matrix: list[list[float]],
) -> go.Figure:
    """Heatmap of syllable openness across verse positions.

    Args:
        verse_ids: Ordered list of verse_id values (y-axis).
        syllable_openness_matrix: 2-D matrix [verses][syllable_positions]
            of vowel openness values (0.0–1.0).

    Returns:
        Plotly Figure containing a single Heatmap trace.
    """
    if not verse_ids or not syllable_openness_matrix:
        return go.Figure()

    fig = go.Figure(
        data=go.Heatmap(
            z=syllable_openness_matrix,
            y=[str(vid) for vid in verse_ids],
            colorscale="Blues",
            colorbar=dict(title="Openness"),
            hovertemplate=(
                "Verse %{y} | Position %{x}<br>Openness: %{z:.3f}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        title="Syllable Openness by Verse",
        xaxis_title="Syllable position",
        yaxis_title="Verse ID",
        template="plotly_white",
        height=max(350, len(verse_ids) * 10),
    )
    return fig
