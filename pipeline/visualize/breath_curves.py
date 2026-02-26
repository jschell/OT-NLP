# pipeline/visualize/breath_curves.py
"""Breath curve overlay charts.

Shows the per-syllable breath weight of the Hebrew source alongside
one or more English translations (stress-mapped) and optional suggestions.

Single-verse:  breath_curve_figure()
Multi-verse:   multi_verse_breath_figure()  — contiguous chart with boundary lines.
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


_COLORS = ["#e94560", "#0f3460", "#533483", "#2e8b57", "#cd853f"]


def multi_verse_breath_figure(
    verse_labels: list[str],
    hebrew_curves: list[list[float]],
    translation_curves: dict[str, list[list[float]]],
    title: str = "",
) -> go.Figure:
    """Contiguous multi-verse breath curve with verse boundary markers.

    Concatenates Hebrew and translation breath curves from consecutive verses
    into a single chart.  Absolute syllable position is used on the x-axis so
    that verses with more syllables occupy proportionally more horizontal space.
    Dashed vertical lines mark verse boundaries; each verse is labelled at its
    start position.

    Args:
        verse_labels: Human-readable label per verse, e.g. ["23:1", "23:2"].
            Must have the same length as ``hebrew_curves``.
        hebrew_curves: One breath-weight list per verse (source text).
        translation_curves: {translation_key: [per-verse curve list]}.
            Inner lists must have the same length as ``verse_labels``.
        title: Chart title.

    Returns:
        Plotly Figure with one Scatter trace per curve, vertical boundary lines,
        and verse-start annotations.
    """
    fig = go.Figure()

    n_verses = len(verse_labels)

    # ── Build cumulative x-offsets ────────────────────────────────────────────
    # Each verse occupies syllable positions [offset, offset + len(curve) - 1].
    verse_start_positions: list[int] = []
    offset = 0
    for hc in hebrew_curves:
        verse_start_positions.append(offset)
        offset += max(len(hc), 1)
    total_len = offset  # total syllable span across all verses

    # ── Hebrew trace ─────────────────────────────────────────────────────────
    x_heb: list[int] = []
    y_heb: list[float] = []
    for i, hc in enumerate(hebrew_curves):
        start = verse_start_positions[i]
        x_heb.extend(start + j for j in range(len(hc)))
        y_heb.extend(hc)

    fig.add_trace(
        go.Scatter(
            x=x_heb,
            y=y_heb,
            mode="lines",
            name="Hebrew (source)",
            line=dict(color="#1a1a2e", width=3),
            hovertemplate="Syllable: %{x}<br>Weight: %{y:.3f}<extra>Hebrew</extra>",
        )
    )

    # ── Translation traces ────────────────────────────────────────────────────
    for t_idx, (key, per_verse) in enumerate(translation_curves.items()):
        x_t: list[int] = []
        y_t: list[float] = []
        for i, curve in enumerate(per_verse):
            if not curve:
                continue
            start = verse_start_positions[i]
            x_t.extend(start + j for j in range(len(curve)))
            y_t.extend(curve)
        if not x_t:
            continue
        fig.add_trace(
            go.Scatter(
                x=x_t,
                y=y_t,
                mode="lines",
                name=key,
                line=dict(
                    color=_COLORS[t_idx % len(_COLORS)],
                    width=2,
                    dash="dot",
                ),
                hovertemplate=(
                    f"Syllable: %{{x}}<br>Weight: %{{y:.3f}}<extra>{key}</extra>"
                ),
            )
        )

    # ── Verse boundary lines (skip first — that's x=0) ───────────────────────
    for i in range(1, n_verses):
        fig.add_vline(
            x=verse_start_positions[i],
            line_dash="dash",
            line_color="grey",
            opacity=0.5,
        )

    # ── Verse label annotations at each verse start ───────────────────────────
    for label, x_start in zip(verse_labels, verse_start_positions, strict=True):
        fig.add_annotation(
            x=x_start,
            y=1.05,
            text=label,
            showarrow=False,
            yref="paper",
            xanchor="left",
            font=dict(size=11, color="#444"),
        )

    fig.update_layout(
        title=title or "Breath Curve — multi-verse",
        xaxis_title="Syllable position",
        xaxis=dict(range=[0, total_len - 1]),
        yaxis_title="Breath weight (0–1)",
        yaxis=dict(range=[0, 1.05]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
        template="plotly_white",
        height=420,
    )
    return fig
