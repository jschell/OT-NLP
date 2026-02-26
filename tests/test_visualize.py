# tests/test_visualize.py
"""Unit tests for pipeline/visualize/ chart modules.

All tests assert on figure structure (trace count, layout keys).
No pixel values, no rendered output, no DB connections required.

Run with:
    uv run --frozen pytest tests/test_visualize.py -v
"""

from __future__ import annotations

import plotly.graph_objects as go
from visualize.arcs import chiasm_arc_figure
from visualize.breath_curves import breath_curve_figure
from visualize.heatmaps import deviation_heatmap
from visualize.radar import fingerprint_radar
from visualize.report import pipeline_summary_chart

# ── breath_curves ─────────────────────────────────────────────────────────────


def test_breath_curve_figure_returns_figure() -> None:
    """breath_curve_figure returns a go.Figure instance."""
    fig = breath_curve_figure(
        verse_id=1,
        hebrew_curve=[0.5, 0.8, 0.6, 0.9, 0.4],
        translation_curves={"KJV": [0.4, 0.7, 0.5, 0.8, 0.45]},
    )
    assert isinstance(fig, go.Figure)


def test_breath_curve_has_hebrew_trace() -> None:
    """Figure must contain at least one trace named 'Hebrew (source)'."""
    fig = breath_curve_figure(
        verse_id=42,
        hebrew_curve=[0.3, 0.7, 0.5],
        translation_curves={"ESV": [0.2, 0.6, 0.4]},
    )
    trace_names = [t.name for t in fig.data]
    assert any("Hebrew" in (name or "") for name in trace_names)


# ── heatmaps ──────────────────────────────────────────────────────────────────


def test_deviation_heatmap_shape() -> None:
    """Heatmap z-matrix rows == unique chapters, cols == unique translations."""
    fig = deviation_heatmap(
        psalm_chapters=[1, 23, 119],
        translation_keys=["KJV", "ESV", "YLT"],
        scores=[
            [0.10, 0.15, 0.20],
            [0.08, 0.12, 0.18],
            [0.22, 0.25, 0.30],
        ],
    )
    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 1  # single Heatmap trace
    z = fig.data[0].z
    assert len(z) == 3  # 3 chapters
    assert len(z[0]) == 3  # 3 translations


# ── arcs ──────────────────────────────────────────────────────────────────────


def test_chiasm_arc_count() -> None:
    """Number of arc shapes equals number of arc_pairs provided."""
    arc_pairs = [
        (0, 3, 0.85),
        (1, 2, 0.72),
    ]
    fig = chiasm_arc_figure(
        verse_labels=["v1c1", "v1c2", "v2c1", "v2c2"],
        arc_pairs=arc_pairs,
        pattern_types=["ABBA", "ABBA"],
    )
    assert isinstance(fig, go.Figure)
    # Each arc_pair becomes one shape in fig.layout.shapes
    assert len(fig.layout.shapes) == len(arc_pairs)


def test_empty_arc_pairs_no_error() -> None:
    """chiasm_arc_figure with empty inputs returns a valid empty figure."""
    fig = chiasm_arc_figure(
        verse_labels=[],
        arc_pairs=[],
        pattern_types=[],
    )
    assert isinstance(fig, go.Figure)
    assert len(fig.layout.shapes) == 0


# ── radar ─────────────────────────────────────────────────────────────────────


def test_fingerprint_radar_has_all_translations() -> None:
    """Radar figure has N+1 traces: one Hebrew + one per translation."""
    hebrew_fp = {
        "syllable_density": 2.5,
        "morpheme_ratio": 1.8,
        "sonority_score": 0.55,
        "clause_compression": 4.0,
    }
    trans_fps = [
        {
            "syllable_density": 1.8,
            "morpheme_ratio": 1.2,
            "sonority_score": 0.45,
            "clause_compression": 5.0,
        },
        {
            "syllable_density": 2.1,
            "morpheme_ratio": 1.5,
            "sonority_score": 0.50,
            "clause_compression": 4.5,
        },
    ]
    fig = fingerprint_radar(
        labels=["KJV", "ESV"],
        fingerprints=trans_fps,
        hebrew_fingerprint=hebrew_fp,
    )
    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 3  # Hebrew + KJV + ESV


def test_fingerprint_radar_single_translation() -> None:
    """Radar works correctly with exactly one translation."""
    fig = fingerprint_radar(
        labels=["KJV"],
        fingerprints=[
            {
                "syllable_density": 1.8,
                "morpheme_ratio": 1.2,
                "sonority_score": 0.45,
                "clause_compression": 5.0,
            }
        ],
        hebrew_fingerprint={
            "syllable_density": 2.5,
            "morpheme_ratio": 1.8,
            "sonority_score": 0.55,
            "clause_compression": 4.0,
        },
    )
    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 2  # Hebrew + KJV


# ── report ────────────────────────────────────────────────────────────────────


def test_pipeline_summary_bar_count() -> None:
    """Bar count in the summary chart equals number of tables provided."""
    counts = {
        "verses": 2527,
        "word_tokens": 43000,
        "syllable_tokens": 120000,
        "breath_profiles": 2527,
        "verse_fingerprints": 2527,
        "translation_scores": 12635,
    }
    fig = pipeline_summary_chart(
        row_counts=counts,
        run_history=[],
    )
    assert isinstance(fig, go.Figure)
    # The bar chart trace should have one bar per table
    bar_trace = next(t for t in fig.data if isinstance(t, go.Bar))
    assert len(bar_trace.x) == len(counts)
