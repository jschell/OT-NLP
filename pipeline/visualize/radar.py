# pipeline/visualize/radar.py
"""Style fingerprint radar chart comparing Hebrew source to translations."""

from __future__ import annotations

import plotly.graph_objects as go

_DIMENSIONS = [
    "syllable_density",
    "morpheme_ratio",
    "sonority_score",
    "clause_compression",
]
_LABELS = ["Syllable Density", "Morpheme Ratio", "Sonority", "Clause Compression"]
# Approximate per-dimension maxima used for 0–1 normalisation
_MAX_VALS = [4.0, 5.0, 1.0, 15.0]
_COLORS = ["#1a1a2e", "#e94560", "#0f3460", "#533483", "#2e8b57", "#cd853f"]


def _normalize(fp: dict[str, float]) -> list[float]:
    """Normalise a fingerprint dict to the [0, 1] range per dimension."""
    return [
        min(1.0, fp.get(dim, 0.0) / max_val)
        for dim, max_val in zip(_DIMENSIONS, _MAX_VALS, strict=True)
    ]


def fingerprint_radar(
    labels: list[str],
    fingerprints: list[dict[str, float]],
    hebrew_fingerprint: dict[str, float],
    title: str = "Style Fingerprint Comparison",
) -> go.Figure:
    """4-dimensional radar chart comparing translation fingerprints to Hebrew.

    Args:
        labels: Translation keys in the same order as ``fingerprints``
            (e.g. ["KJV", "ESV"]).
        fingerprints: One fingerprint dict per translation, each containing
            keys: syllable_density, morpheme_ratio, sonority_score,
            clause_compression.
        hebrew_fingerprint: Fingerprint dict for the Hebrew source verse.
        title: Chart title.

    Returns:
        Plotly Figure with len(labels) + 1 Scatterpolar traces
        (Hebrew trace first).
    """
    fig = go.Figure()

    # Hebrew trace
    heb_vals = _normalize(hebrew_fingerprint)
    fig.add_trace(
        go.Scatterpolar(
            r=heb_vals + [heb_vals[0]],
            theta=_LABELS + [_LABELS[0]],
            fill="toself",
            fillcolor="rgba(26,26,46,0.15)",
            line=dict(color=_COLORS[0], width=3),
            name="Hebrew (source)",
        )
    )

    # Translation traces
    for idx, (label, fp) in enumerate(zip(labels, fingerprints, strict=True)):
        vals = _normalize(fp)
        fig.add_trace(
            go.Scatterpolar(
                r=vals + [vals[0]],
                theta=_LABELS + [_LABELS[0]],
                fill="none",
                line=dict(
                    color=_COLORS[(idx + 1) % len(_COLORS)],
                    width=2,
                    dash="dot",
                ),
                name=label,
            )
        )

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1],
                tickfont=dict(size=9),
            ),
        ),
        title=title,
        template="plotly_white",
        height=400,
        showlegend=True,
    )
    return fig
