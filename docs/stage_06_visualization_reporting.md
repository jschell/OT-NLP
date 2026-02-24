# Stage 6 — Visualization & Reporting
## Detailed Implementation Plan

> **Depends on:** Stage 4 (scores populated); Stage 5 optional (suggestions enhance explorer)  
> **Produces:** Auto-rebuilding Sphinx HTML report site; Typst PDF; Streamlit interactive explorer with breath curves, deviation heatmaps, arc diagrams, and chiasm viewer  
> **Estimated time:** 2–4 hours initial setup; subsequent runs auto-triggered by pipeline

---

## Objectives

1. Build `visualize/` module with all chart types as importable Python functions
2. Implement `modules/export.py` to orchestrate the full build pipeline
3. Build Sphinx documentation site with myst-nb
4. Author `docs/report.typ` for Typst PDF generation
5. Build full Streamlit explorer application

---

## Tool Summary

| Tool | Role | How added |
|---|---|---|
| `sphinx` + `myst-nb` | HTML report site | `pip install` — already in requirements.txt |
| `Typst` binary | PDF generation | Single binary in Dockerfile, version pinned |
| `Streamlit` | Interactive explorer | `pip install` — already in requirements.txt |
| `plotly` + `kaleido` | Charts + static export | `pip install` — already in requirements.txt |

No Node.js. No Grafana. No Quarto. All versions pinned in `requirements.txt` or `Dockerfile.pipeline`.

---

## File Structure

```
pipeline/
  visualize/
    __init__.py
    breath_curves.py     ← breath curve overlay charts
    heatmaps.py          ← deviation heatmap, openness heatmap
    arcs.py              ← arc diagram for parallelism + chiasm
    radar.py             ← style fingerprint radar charts
    report.py            ← pipeline run summary chart
  modules/
    export.py            ← orchestrates nbconvert → sphinx → typst
  docs/
    conf.py              ← Sphinx configuration
    index.md             ← table of contents
    analysis.ipynb       ← primary analysis notebook
    report.typ           ← Typst PDF source
  tests/
    test_visualize.py
streamlit/
  app.py                 ← full Streamlit explorer
```

---

## Step 1 — File: `visualize/__init__.py`

```python
"""Psalms NLP visualization module."""
from .breath_curves import breath_curve_overlay
from .heatmaps import deviation_heatmap, openness_heatmap
from .arcs import arc_diagram
from .radar import fingerprint_radar
from .report import pipeline_summary_chart

__all__ = [
    "breath_curve_overlay",
    "deviation_heatmap",
    "openness_heatmap",
    "arc_diagram",
    "fingerprint_radar",
    "pipeline_summary_chart",
]
```

---

## Step 2 — File: `visualize/breath_curves.py`

```python
"""
Breath curve overlay chart.

Shows the per-syllable breath weight of the Hebrew source alongside
one or more English translations (stress-mapped) and optional suggestions.
"""

from __future__ import annotations
from typing import List, Optional, Dict
import plotly.graph_objects as go


def breath_curve_overlay(
    hebrew_curve: List[float],
    translations: Dict[str, List[float]],
    suggestions: Optional[Dict[str, List[float]]] = None,
    title: str = "Breath Curve Overlay",
) -> go.Figure:
    """
    Args:
        hebrew_curve: per-syllable breath weights from breath_profiles.breath_curve
        translations: {translation_key: [per-syllable English weights]}
        suggestions: {label: [per-syllable weights]} optional
        title: chart title

    Returns:
        Plotly Figure with one trace per curve.
    """
    fig = go.Figure()

    # Hebrew curve — bold, dark
    n_heb = len(hebrew_curve)
    x_heb = [i / max(n_heb - 1, 1) for i in range(n_heb)]
    fig.add_trace(go.Scatter(
        x=x_heb,
        y=hebrew_curve,
        mode="lines",
        name="Hebrew (source)",
        line=dict(color="#1a1a2e", width=3),
        hovertemplate="Position: %{x:.2f}<br>Weight: %{y:.3f}<extra>Hebrew</extra>",
    ))

    # Translation curves
    colors = ["#e94560", "#0f3460", "#533483", "#2e8b57", "#cd853f"]
    for idx, (key, curve) in enumerate(translations.items()):
        n = len(curve)
        if n == 0:
            continue
        x = [i / max(n - 1, 1) for i in range(n)]
        fig.add_trace(go.Scatter(
            x=x, y=curve,
            mode="lines",
            name=key,
            line=dict(color=colors[idx % len(colors)], width=2, dash="dot"),
            hovertemplate=f"Position: %{{x:.2f}}<br>Weight: %{{y:.3f}}<extra>{key}</extra>",
        ))

    # Suggestion curves
    if suggestions:
        for idx, (label, curve) in enumerate(suggestions.items()):
            n = len(curve)
            if n == 0:
                continue
            x = [i / max(n - 1, 1) for i in range(n)]
            fig.add_trace(go.Scatter(
                x=x, y=curve,
                mode="lines",
                name=f"Suggestion: {label}",
                line=dict(color="#ffd700", width=2, dash="dash"),
            ))

    fig.update_layout(
        title=title,
        xaxis_title="Relative position in verse (0–1)",
        yaxis_title="Breath weight (0–1)",
        yaxis=dict(range=[0, 1.05]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
        template="plotly_white",
        height=400,
    )
    return fig
```

---

## Step 3 — File: `visualize/heatmaps.py`

```python
"""Deviation heatmap and syllable openness heatmap."""

from __future__ import annotations
from typing import List, Dict
import plotly.graph_objects as go
import pandas as pd


def deviation_heatmap(
    scores: List[Dict],
    title: str = "Style Deviation by Psalm × Translation",
) -> go.Figure:
    """
    Args:
        scores: list of dicts with keys: chapter, translation_key, composite_deviation
    """
    if not scores:
        return go.Figure()

    df = pd.DataFrame(scores)
    pivot = df.pivot_table(
        index="chapter",
        columns="translation_key",
        values="composite_deviation",
        aggfunc="mean",
    )

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=[f"Psalm {c}" for c in pivot.index.tolist()],
        colorscale="RdYlGn_r",   # red = high deviation, green = low
        colorbar=dict(title="Mean Deviation"),
        hovertemplate="Psalm %{y} | %{x}<br>Deviation: %{z:.4f}<extra></extra>",
    ))
    fig.update_layout(
        title=title,
        xaxis_title="Translation",
        yaxis_title="Psalm Chapter",
        template="plotly_white",
        height=max(400, len(pivot) * 8),
    )
    return fig


def openness_heatmap(
    syllables: List[Dict],
    chapter: int,
    verse_num: int,
    title: str = "",
) -> go.Figure:
    """
    Syllable-level openness heatmap for a single verse.

    Args:
        syllables: list of dicts with syllable_text, vowel_openness, colon_index
    """
    if not syllables:
        return go.Figure()

    texts = [s.get("syllable_text", "") for s in syllables]
    values = [s.get("vowel_openness", 0) for s in syllables]
    colons = [s.get("colon_index", 1) for s in syllables]

    fig = go.Figure(data=go.Bar(
        x=list(range(len(texts))),
        y=values,
        text=texts,
        textposition="outside",
        marker=dict(
            color=values,
            colorscale="Blues",
            cmin=0, cmax=1,
            colorbar=dict(title="Openness"),
        ),
        customdata=colons,
        hovertemplate="Syllable: %{text}<br>Openness: %{y:.3f}<br>Colon: %{customdata}<extra></extra>",
    ))

    # Add colon boundary markers
    seen_colons = set()
    for i, c in enumerate(colons):
        if c not in seen_colons and i > 0:
            fig.add_vline(x=i - 0.5, line_dash="dash", line_color="gray", opacity=0.5)
        seen_colons.add(c)

    fig.update_layout(
        title=title or f"Syllable Openness — Psalm {chapter}:{verse_num}",
        xaxis_title="Syllable position",
        yaxis_title="Vowel openness (0–1)",
        yaxis=dict(range=[0, 1.1]),
        template="plotly_white",
        height=350,
        showlegend=False,
    )
    return fig
```

---

## Step 4 — File: `visualize/arcs.py`

```python
"""
Arc diagram for structural analysis.

Shows:
  - Parallelism / root repetition arcs within a Psalm
  - Chiastic structure arcs (ABBA / ABCBA)
"""

from __future__ import annotations
from typing import List, Dict, Optional
import math
import plotly.graph_objects as go


def arc_diagram(
    verses: List[Dict],
    chiasm_candidates: List[Dict],
    title: str = "Structural Arc Diagram",
) -> go.Figure:
    """
    Args:
        verses: list of dicts with verse_num, colon_count
        chiasm_candidates: list of dicts with verse_id_start, verse_id_end,
                           pattern_type, confidence, colon_matches
        title: chart title

    Each arc connects the colon pair identified by chiasm detection.
    Arc color encodes pattern type; arc weight encodes confidence.
    """
    fig = go.Figure()

    if not verses:
        return fig

    # X positions: one per colon, laid out linearly
    colon_positions = []
    verse_labels = []
    x = 0
    for v in sorted(verses, key=lambda v: v["verse_num"]):
        v_num = v["verse_num"]
        n_colons = max(v.get("colon_count", 2), 1)
        for c in range(n_colons):
            colon_positions.append(x)
            verse_labels.append(f"v{v_num}c{c+1}")
            x += 1

    # Draw baseline
    fig.add_shape(
        type="line",
        x0=0, x1=max(colon_positions) if colon_positions else 1,
        y0=0, y1=0,
        line=dict(color="lightgray", width=2),
    )

    # Verse boundary tick marks
    for v in verses:
        v_num = v["verse_num"]
        # Approximate x position of verse start
        x_mark = sum(
            max(vv.get("colon_count", 2), 1)
            for vv in verses if vv["verse_num"] < v_num
        )
        fig.add_shape(
            type="line",
            x0=x_mark, x1=x_mark,
            y0=-0.05, y1=0.05,
            line=dict(color="gray", width=1),
        )
        fig.add_annotation(
            x=x_mark, y=-0.12,
            text=f"v{v_num}",
            showarrow=False,
            font=dict(size=9, color="gray"),
        )

    # Draw arcs for chiasm candidates
    COLORS = {
        "ABBA":  "#e94560",
        "ABCBA": "#0f3460",
        "AB":    "#2e8b57",
    }

    for cand in chiasm_candidates:
        pattern = cand.get("pattern_type", "AB")
        confidence = float(cand.get("confidence", 0.5))
        matches = cand.get("colon_matches", [])
        color = COLORS.get(pattern, "#888888")
        line_width = max(1, round(confidence * 5))

        for match in matches:
            if "pivot" in match:
                continue  # skip pivot marker
            a_idx = match.get("a", 0)
            b_idx = match.get("b", 1)
            sim = match.get("similarity", confidence)

            if a_idx >= len(colon_positions) or b_idx >= len(colon_positions):
                continue

            x_a = colon_positions[a_idx]
            x_b = colon_positions[b_idx]
            arc_height = abs(x_b - x_a) * 0.3

            # SVG-style arc via Bezier points
            x_mid = (x_a + x_b) / 2
            fig.add_shape(
                type="path",
                path=f"M {x_a} 0 Q {x_mid} {arc_height} {x_b} 0",
                line=dict(color=color, width=line_width),
                opacity=max(0.4, confidence),
            )
            fig.add_annotation(
                x=x_mid, y=arc_height + 0.02,
                text=f"{pattern} {sim:.2f}",
                showarrow=False,
                font=dict(size=8, color=color),
            )

    # Add legend entries manually
    for pattern, color in COLORS.items():
        fig.add_trace(go.Scatter(
            x=[None], y=[None],
            mode="lines",
            name=pattern,
            line=dict(color=color, width=2),
        ))

    n_pos = max(colon_positions) + 1 if colon_positions else 2
    fig.update_layout(
        title=title,
        xaxis=dict(
            range=[-0.5, n_pos + 0.5],
            ticktext=verse_labels[::2],
            tickvals=list(range(0, len(colon_positions), 2)),
            title="Colon position",
        ),
        yaxis=dict(range=[-0.25, max(0.5, n_pos * 0.35)], visible=False),
        template="plotly_white",
        height=350,
        showlegend=True,
    )
    return fig
```

---

## Step 5 — File: `visualize/radar.py`

```python
"""Style fingerprint radar chart comparing Hebrew source to translations."""

from __future__ import annotations
from typing import Dict
import plotly.graph_objects as go


DIMENSIONS = ["syllable_density", "morpheme_ratio", "sonority_score", "clause_compression"]
LABELS = ["Syllable Density", "Morpheme Ratio", "Sonority", "Clause Compression"]


def fingerprint_radar(
    hebrew_fp: Dict[str, float],
    translation_fps: Dict[str, Dict[str, float]],
    title: str = "Style Fingerprint Comparison",
) -> go.Figure:
    """
    Args:
        hebrew_fp: {dimension: value} for Hebrew source verse
        translation_fps: {translation_key: {dimension: value}}
    """
    fig = go.Figure()

    colors = ["#1a1a2e", "#e94560", "#0f3460", "#533483", "#2e8b57", "#cd853f"]

    def _normalize(fp: Dict[str, float]) -> list:
        """Normalize values to 0–1 range for radar display."""
        raw = [fp.get(d, 0) for d in DIMENSIONS]
        max_vals = [4.0, 5.0, 1.0, 15.0]   # approximate max per dimension
        return [min(1.0, v / m) for v, m in zip(raw, max_vals)]

    # Hebrew trace
    values = _normalize(hebrew_fp) + [_normalize(hebrew_fp)[0]]   # close the loop
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=LABELS + [LABELS[0]],
        fill="toself",
        fillcolor="rgba(26,26,46,0.15)",
        line=dict(color=colors[0], width=3),
        name="Hebrew (source)",
    ))

    # Translation traces
    for idx, (key, fp) in enumerate(translation_fps.items()):
        values = _normalize(fp) + [_normalize(fp)[0]]
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=LABELS + [LABELS[0]],
            fill="none",
            line=dict(color=colors[(idx + 1) % len(colors)], width=2, dash="dot"),
            name=key,
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 1], tickfont=dict(size=9)),
        ),
        title=title,
        template="plotly_white",
        height=400,
        showlegend=True,
    )
    return fig
```

---

## Step 6 — File: `visualize/report.py`

```python
"""Pipeline run summary chart."""

from __future__ import annotations
from typing import Dict, List
import plotly.graph_objects as go


def pipeline_summary_chart(row_counts: Dict[str, int]) -> go.Figure:
    """Bar chart of row counts per pipeline table."""
    tables = list(row_counts.keys())
    counts = [row_counts[t] for t in tables]

    fig = go.Figure(data=go.Bar(
        x=tables,
        y=counts,
        marker_color="#0f3460",
        text=counts,
        textposition="outside",
        hovertemplate="%{x}: %{y:,} rows<extra></extra>",
    ))
    fig.update_layout(
        title="Pipeline Row Counts by Table",
        xaxis_title="Table",
        yaxis_title="Rows",
        template="plotly_white",
        height=350,
    )
    return fig
```

---

## Step 7 — File: `modules/export.py`

```python
"""
Stage 6 — Export orchestrator.

Runs: nbconvert (execute notebook) → sphinx-build (HTML site) → typst (PDF)
All output to /data/outputs/.

Uses the freeze pattern: notebooks are executed once and saved with outputs;
sphinx-build reads static outputs without re-executing.
"""

from __future__ import annotations

import logging
import subprocess
import shutil
from pathlib import Path
from typing import Dict

import psycopg2

logger = logging.getLogger(__name__)


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    export_cfg = config.get("export", {})
    output_dir = Path(export_cfg.get("output_dir", "/data/outputs"))
    report_dir = Path(export_cfg.get("report_dir", "/data/outputs/report"))
    pdf_path   = Path(export_cfg.get("pdf_path", "/data/outputs/report.pdf"))
    typst_version = export_cfg.get("typst_version", "0.12.0")

    docs_dir   = Path("/pipeline/docs")
    notebook   = docs_dir / "analysis.ipynb"

    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    results = {}

    # ── Step 1: Execute notebook (freeze outputs) ────────────────
    executed_notebook = output_dir / "analysis_executed.ipynb"
    logger.info("Executing analysis notebook...")
    result = subprocess.run(
        [
            "jupyter", "nbconvert",
            "--to", "notebook",
            "--execute",
            "--ExecutePreprocessor.timeout=300",
            "--output", str(executed_notebook),
            str(notebook),
        ],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"Notebook execution failed:\n{result.stderr}")
        results["notebook_execute"] = "failed"
    else:
        logger.info("Notebook executed successfully")
        results["notebook_execute"] = "ok"
        # Copy executed notebook to docs dir for sphinx to pick up
        shutil.copy(executed_notebook, docs_dir / "analysis_executed.ipynb")

    # ── Step 2: Sphinx HTML build ────────────────────────────────
    logger.info("Building Sphinx HTML site...")
    result = subprocess.run(
        [
            "sphinx-build",
            "-b", "html",
            "-q",          # quiet — only warnings/errors
            str(docs_dir),
            str(report_dir),
        ],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"Sphinx build failed:\n{result.stderr}")
        results["sphinx"] = "failed"
    else:
        logger.info(f"Sphinx HTML site written to {report_dir}")
        results["sphinx"] = "ok"

    # ── Step 3: Typst PDF ────────────────────────────────────────
    typ_file = docs_dir / "report.typ"
    if typ_file.exists() and shutil.which("typst"):
        logger.info("Compiling Typst PDF...")
        result = subprocess.run(
            ["typst", "compile", str(typ_file), str(pdf_path)],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            logger.warning(f"Typst compilation failed:\n{result.stderr}")
            results["typst"] = "failed"
        else:
            logger.info(f"PDF written to {pdf_path}")
            results["typst"] = "ok"
    else:
        logger.info("Typst not available or report.typ not found — skipping PDF")
        results["typst"] = "skipped"

    return results
```

---

## Step 8 — File: `docs/conf.py`

```python
# Sphinx configuration for Psalms NLP report site

project   = "Psalms NLP Analysis"
author    = "Psalms NLP Pipeline"
release   = "0.1"

extensions = [
    "myst_nb",
    "sphinx.ext.autodoc",
    "sphinxcontrib.bibtex",
]

myst_enable_extensions = ["colon_fence", "deflist", "dollarmath"]

# Do not execute notebooks — use frozen outputs
nb_execution_mode = "off"

html_theme = "sphinx_book_theme"
html_theme_options = {
    "repository_url": "",
    "use_repository_button": False,
    "show_navbar_depth": 2,
}

bibtex_bibfiles = ["references.bib"]

suppress_warnings = ["myst.header"]
```

`docs/index.md`:
```markdown
# Psalms NLP Analysis

A computational analysis of phonetic and structural properties in the Hebrew Psalms
and their English translations.

```{toctree}
:maxdepth: 2
analysis_executed
```
```

`docs/references.bib` (starter bibliography):
```bibtex
@book{alter2007,
  author    = {Alter, Robert},
  title     = {The Book of Psalms: A Translation with Commentary},
  publisher = {W.W. Norton},
  year      = {2007}
}

@online{bhsa2021,
  author = {{ETCBC}},
  title  = {Biblia Hebraica Stuttgartensia Amstelodamensis},
  year   = {2021},
  url    = {https://github.com/ETCBC/bhsa}
}
```

`docs/report.typ` (Typst PDF template):
```typst
#import "@preview/charged-ieee:0.1.3": ieee

#show: ieee.with(
  title: [Psalms NLP: Quantifying Translation Fidelity to Hebrew Phonetic Structure],
  authors: (
    (name: "Psalms NLP Pipeline", organization: "Self-hosted"),
  ),
  abstract: [
    This report presents computational analysis of the Hebrew Psalms corpus
    using morphological and phonetic fingerprinting to quantify style deviation
    and breath alignment across five English translations.
  ],
)

= Introduction

Analysis conducted using the BHSA morphological database and a four-dimensional
style fingerprint (syllable density, morpheme ratio, sonority score, clause compression).

= Results

#figure(
  image("/data/outputs/figures/deviation_heatmap.png", width: 100%),
  caption: [Mean style deviation by Psalm and translation],
)

#figure(
  image("/data/outputs/figures/breath_sample.png", width: 80%),
  caption: [Breath curve overlay for Psalm 23:1],
)

= Methods

The BHSA Hebrew Bible Syntactic Analysis corpus @bhsa2021 provides morphological
annotation at the word level. Style fingerprints are 4-dimensional vectors computed
per verse...

#bibliography("references.bib")
```

---

## Step 9 — Full Streamlit Explorer: `streamlit/app.py`

```python
"""
Psalms NLP Interactive Explorer

Multi-page Streamlit application providing:
  Page 1 — Breath Curve Overlay
  Page 2 — Deviation Heatmap
  Page 3 — Chiasm Viewer
  Page 4 — Translation Comparison Table
"""

import os
import sys
import json
import streamlit as st
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import pandas as pd

sys.path.insert(0, "/pipeline")

from visualize.breath_curves import breath_curve_overlay
from visualize.heatmaps import deviation_heatmap, openness_heatmap
from visualize.arcs import arc_diagram
from visualize.radar import fingerprint_radar
from adapters.phoneme_adapter import english_breath_weights

st.set_page_config(
    page_title="Psalms NLP Explorer",
    page_icon="📖",
    layout="wide",
)


@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "db"),
        dbname=os.environ.get("POSTGRES_DB", "psalms"),
        user=os.environ.get("POSTGRES_USER", "psalms"),
        password=os.environ.get("POSTGRES_PASSWORD", "psalms_dev"),
    )


def query(sql, params=()):
    conn = get_connection()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


# ── Sidebar ──────────────────────────────────────────────────────

st.sidebar.title("📖 Psalms NLP")
page = st.sidebar.radio(
    "Navigate",
    ["Breath Curves", "Deviation Heatmap", "Chiasm Viewer", "Translation Comparison", "Pipeline Summary"],
)

chapters = query(
    "SELECT DISTINCT chapter FROM verses WHERE book_num = 19 ORDER BY chapter"
)
chapter_options = [r["chapter"] for r in chapters]

selected_chapter = st.sidebar.selectbox(
    "Psalm (chapter)", chapter_options, index=22 if 23 in chapter_options else 0
)

verse_options_raw = query(
    "SELECT verse_num FROM verses WHERE book_num = 19 AND chapter = %s ORDER BY verse_num",
    (selected_chapter,)
)
verse_options = [r["verse_num"] for r in verse_options_raw]
selected_verse = st.sidebar.selectbox("Verse", verse_options)

translation_keys_raw = query(
    "SELECT DISTINCT translation_key FROM translations ORDER BY translation_key"
)
all_keys = [r["translation_key"] for r in translation_keys_raw]
selected_translations = st.sidebar.multiselect(
    "Translations", all_keys, default=all_keys[:3] if len(all_keys) >= 3 else all_keys
)


# ── Helper: fetch verse data ─────────────────────────────────────

def get_verse(chapter, verse_num):
    rows = query(
        """
        SELECT v.verse_id, v.hebrew_text,
               bp.breath_curve, bp.stress_positions, bp.colon_count, bp.mean_weight
        FROM verses v
        LEFT JOIN breath_profiles bp ON bp.verse_id = v.verse_id
        WHERE v.book_num = 19 AND v.chapter = %s AND v.verse_num = %s
        """,
        (chapter, verse_num)
    )
    return rows[0] if rows else None


def get_translation_texts(verse_id, keys):
    rows = query(
        "SELECT translation_key, verse_text FROM translations WHERE verse_id = %s AND translation_key = ANY(%s)",
        (verse_id, keys)
    )
    return {r["translation_key"]: r["verse_text"] for r in rows}


def get_scores(verse_id, keys):
    rows = query(
        """
        SELECT translation_key, composite_deviation, breath_alignment,
               density_deviation, morpheme_deviation, sonority_deviation, compression_deviation
        FROM translation_scores WHERE verse_id = %s AND translation_key = ANY(%s)
        """,
        (verse_id, keys)
    )
    return {r["translation_key"]: dict(r) for r in rows}


def get_fingerprints(verse_id, keys):
    heb = query(
        "SELECT syllable_density, morpheme_ratio, sonority_score, clause_compression FROM verse_fingerprints WHERE verse_id = %s",
        (verse_id,)
    )
    heb_fp = dict(heb[0]) if heb else {}

    eng_fps = {}
    for key in keys:
        text_rows = query(
            "SELECT verse_text FROM translations WHERE verse_id = %s AND translation_key = %s",
            (verse_id, key)
        )
        if text_rows:
            from adapters.phoneme_adapter import english_fingerprint
            eng_fps[key] = english_fingerprint(text_rows[0]["verse_text"])

    return heb_fp, eng_fps


# ── Page: Breath Curves ──────────────────────────────────────────

if page == "Breath Curves":
    st.header(f"Breath Curve Overlay — Psalm {selected_chapter}:{selected_verse}")

    verse = get_verse(selected_chapter, selected_verse)
    if not verse:
        st.error("No data for this verse. Run the pipeline first.")
        st.stop()

    heb_curve = verse["breath_curve"] or []
    texts = get_translation_texts(verse["verse_id"], selected_translations)

    # Compute English breath weights per translation
    eng_curves = {
        key: english_breath_weights(text)
        for key, text in texts.items()
    }

    # Suggestions
    sug_rows = query(
        "SELECT suggested_text, translation_key FROM suggestions WHERE verse_id = %s",
        (verse["verse_id"],)
    )
    sug_curves = {
        f"{r['translation_key']}*": english_breath_weights(r["suggested_text"])
        for r in sug_rows
    }

    fig = breath_curve_overlay(
        hebrew_curve=heb_curve,
        translations={k: v for k, v in eng_curves.items() if k in selected_translations},
        suggestions=sug_curves if sug_curves else None,
        title=f"Psalm {selected_chapter}:{selected_verse} — Breath Curve",
    )
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Hebrew text")
        st.text(verse["hebrew_text"])
        st.metric("Mean breath weight", f"{(verse.get('mean_weight') or 0):.3f}")
        st.metric("Colon count", verse.get("colon_count") or "—")

    with col2:
        st.subheader("Translation texts")
        for key, text in texts.items():
            st.markdown(f"**{key}:** {text}")

    # Openness heatmap
    syl_rows = query(
        """
        SELECT st.syllable_text, st.vowel_openness, st.colon_index, st.onset_class
        FROM syllable_tokens st
        JOIN word_tokens wt ON st.token_id = wt.token_id
        JOIN verses v ON st.verse_id = v.verse_id
        WHERE v.book_num = 19 AND v.chapter = %s AND v.verse_num = %s
        ORDER BY wt.position, st.syllable_index
        """,
        (selected_chapter, selected_verse)
    )
    if syl_rows:
        heatmap_fig = openness_heatmap(
            [dict(r) for r in syl_rows],
            selected_chapter, selected_verse
        )
        st.plotly_chart(heatmap_fig, use_container_width=True)


# ── Page: Deviation Heatmap ──────────────────────────────────────

elif page == "Deviation Heatmap":
    st.header("Style Deviation by Psalm × Translation")

    score_rows = query(
        """
        SELECT v.chapter, ts.translation_key, ts.composite_deviation
        FROM translation_scores ts
        JOIN verses v ON ts.verse_id = v.verse_id
        WHERE v.book_num = 19 AND ts.translation_key = ANY(%s)
        """,
        (selected_translations,)
    )
    if not score_rows:
        st.warning("No scores found. Run Stage 4 first.")
    else:
        fig = deviation_heatmap([dict(r) for r in score_rows])
        st.plotly_chart(fig, use_container_width=True)

        # Summary table
        st.subheader("Mean deviation by translation")
        summary = pd.DataFrame(score_rows).groupby("translation_key")["composite_deviation"].agg(
            ["mean", "median", "std"]
        ).round(4).sort_values("mean")
        st.dataframe(summary, use_container_width=True)


# ── Page: Chiasm Viewer ──────────────────────────────────────────

elif page == "Chiasm Viewer":
    st.header(f"Chiasm Viewer — Psalm {selected_chapter}")

    verse_rows = query(
        "SELECT verse_id, verse_num, colon_count FROM verses WHERE book_num = 19 AND chapter = %s ORDER BY verse_num",
        (selected_chapter,)
    )
    if not verse_rows:
        st.warning("No verses found.")
        st.stop()

    verse_id_list = [r["verse_id"] for r in verse_rows]

    chiasm_rows = query(
        """
        SELECT cc.verse_id_start, cc.verse_id_end, cc.pattern_type,
               cc.colon_matches, cc.confidence, cc.is_reviewed, cc.reviewer_note
        FROM chiasm_candidates cc
        WHERE cc.verse_id_start = ANY(%s) OR cc.verse_id_end = ANY(%s)
        ORDER BY cc.confidence DESC
        """,
        (verse_id_list, verse_id_list)
    )

    if not chiasm_rows:
        st.info(
            f"No chiasm candidates found for Psalm {selected_chapter}. "
            "This may mean no patterns above the confidence threshold were detected, "
            "or Stage 2 (second pass) has not yet run."
        )
    else:
        st.metric("Candidates found", len(chiasm_rows))

        fig = arc_diagram(
            verses=[dict(r) for r in verse_rows],
            chiasm_candidates=[dict(r) for r in chiasm_rows],
            title=f"Psalm {selected_chapter} — Chiastic Structure Candidates",
        )
        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            "⚠ These are computational candidates flagged for interpretive review. "
            "Phonetic similarity does not confirm chiasm — scholarly judgment required."
        )

        # Candidate table
        df = pd.DataFrame(chiasm_rows)
        df["confidence"] = df["confidence"].round(4)
        st.dataframe(
            df[["verse_id_start", "verse_id_end", "pattern_type", "confidence", "is_reviewed"]],
            use_container_width=True,
        )


# ── Page: Translation Comparison ────────────────────────────────

elif page == "Translation Comparison":
    st.header(f"Translation Comparison — Psalm {selected_chapter}:{selected_verse}")

    verse = get_verse(selected_chapter, selected_verse)
    if not verse:
        st.error("No verse data.")
        st.stop()

    scores = get_scores(verse["verse_id"], selected_translations)
    texts  = get_translation_texts(verse["verse_id"], selected_translations)
    heb_fp, eng_fps = get_fingerprints(verse["verse_id"], selected_translations)

    # Score table
    st.subheader("Deviation Scores")
    rows = []
    for key in selected_translations:
        s = scores.get(key, {})
        rows.append({
            "Translation": key,
            "Text": texts.get(key, "—")[:60] + "…",
            "Composite Deviation": round(s.get("composite_deviation", 0), 4),
            "Breath Alignment":    round(s.get("breath_alignment", 0), 4),
            "Density Dev":         round(s.get("density_deviation", 0), 4),
            "Morpheme Dev":        round(s.get("morpheme_deviation", 0), 4),
        })
    st.dataframe(pd.DataFrame(rows).set_index("Translation"), use_container_width=True)

    # Radar chart
    if heb_fp and eng_fps:
        radar_fig = fingerprint_radar(heb_fp, eng_fps,
                                      title=f"Psalm {selected_chapter}:{selected_verse} — Fingerprint")
        st.plotly_chart(radar_fig, use_container_width=True)

    # Suggestions
    sug_rows = query(
        """
        SELECT translation_key, suggested_text, composite_deviation,
               improvement_delta, llm_provider, llm_model
        FROM suggestions WHERE verse_id = %s
        ORDER BY improvement_delta DESC
        """,
        (verse["verse_id"],)
    )
    if sug_rows:
        st.subheader("LLM Suggestions")
        for s in sug_rows:
            delta = s["improvement_delta"]
            badge = "✅" if delta > 0 else "⚠️"
            st.markdown(
                f"{badge} **{s['translation_key']}** improvement Δ={delta:.4f} "
                f"(via {s['llm_provider']}/{s['llm_model']})"
            )
            st.markdown(f"> {s['suggested_text']}")


# ── Page: Pipeline Summary ───────────────────────────────────────

elif page == "Pipeline Summary":
    st.header("Pipeline Summary")

    counts_raw = query(
        """
        SELECT 'verses' as tbl, COUNT(*) as cnt FROM verses WHERE book_num = 19
        UNION ALL SELECT 'word_tokens', COUNT(*) FROM word_tokens wt JOIN verses v ON wt.verse_id = v.verse_id WHERE v.book_num = 19
        UNION ALL SELECT 'syllable_tokens', COUNT(*) FROM syllable_tokens st JOIN verses v ON st.verse_id = v.verse_id WHERE v.book_num = 19
        UNION ALL SELECT 'breath_profiles', COUNT(*) FROM breath_profiles bp JOIN verses v ON bp.verse_id = v.verse_id WHERE v.book_num = 19
        UNION ALL SELECT 'fingerprints', COUNT(*) FROM verse_fingerprints vf JOIN verses v ON vf.verse_id = v.verse_id WHERE v.book_num = 19
        UNION ALL SELECT 'translation_scores', COUNT(*) FROM translation_scores ts JOIN verses v ON ts.verse_id = v.verse_id WHERE v.book_num = 19
        UNION ALL SELECT 'suggestions', COUNT(*) FROM suggestions s JOIN verses v ON s.verse_id = v.verse_id WHERE v.book_num = 19
        UNION ALL SELECT 'chiasm_candidates', COUNT(*) FROM chiasm_candidates
        """
    )
    counts = {r["tbl"]: r["cnt"] for r in counts_raw}

    from visualize.report import pipeline_summary_chart
    fig = pipeline_summary_chart(counts)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Recent pipeline runs")
    runs = query(
        "SELECT started_at, finished_at, status, stages_run, error_message FROM pipeline_runs ORDER BY started_at DESC LIMIT 10"
    )
    if runs:
        st.dataframe(pd.DataFrame(runs), use_container_width=True)
    else:
        st.info("No pipeline runs recorded yet.")
```

---

## Step 10 — Test Cases

```python
# tests/test_visualize.py

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import plotly.graph_objects as go
from visualize.breath_curves import breath_curve_overlay
from visualize.heatmaps import deviation_heatmap, openness_heatmap
from visualize.arcs import arc_diagram
from visualize.radar import fingerprint_radar
from visualize.report import pipeline_summary_chart


def test_breath_curve_returns_figure():
    fig = breath_curve_overlay(
        hebrew_curve=[0.5, 0.8, 0.6, 0.9, 0.4],
        translations={"KJV": [0.4, 0.7, 0.5, 0.8, 0.45]},
    )
    assert isinstance(fig, go.Figure)
    assert len(fig.data) >= 2


def test_breath_curve_with_suggestions():
    fig = breath_curve_overlay(
        hebrew_curve=[0.5, 0.8],
        translations={"KJV": [0.4, 0.7]},
        suggestions={"KJV*": [0.55, 0.75]},
    )
    assert len(fig.data) == 3


def test_deviation_heatmap_returns_figure():
    scores = [
        {"chapter": 23, "translation_key": "KJV", "composite_deviation": 0.15},
        {"chapter": 23, "translation_key": "YLT", "composite_deviation": 0.08},
        {"chapter": 1,  "translation_key": "KJV", "composite_deviation": 0.22},
    ]
    fig = deviation_heatmap(scores)
    assert isinstance(fig, go.Figure)


def test_deviation_heatmap_empty():
    fig = deviation_heatmap([])
    assert isinstance(fig, go.Figure)


def test_openness_heatmap_returns_figure():
    syllables = [
        {"syllable_text": "הָ", "vowel_openness": 1.0, "colon_index": 1},
        {"syllable_text": "אָ", "vowel_openness": 0.9, "colon_index": 2},
    ]
    fig = openness_heatmap(syllables, 23, 1)
    assert isinstance(fig, go.Figure)


def test_arc_diagram_returns_figure():
    verses = [{"verse_num": 1, "colon_count": 2}, {"verse_num": 2, "colon_count": 3}]
    candidates = [{
        "verse_id_start": 1, "verse_id_end": 2,
        "pattern_type": "ABBA", "confidence": 0.85,
        "colon_matches": [{"a": 0, "b": 3, "similarity": 0.88}, {"a": 1, "b": 2, "similarity": 0.82}]
    }]
    fig = arc_diagram(verses, candidates)
    assert isinstance(fig, go.Figure)


def test_fingerprint_radar_returns_figure():
    heb = {"syllable_density": 2.5, "morpheme_ratio": 1.8, "sonority_score": 0.55, "clause_compression": 4.0}
    trans = {
        "KJV": {"syllable_density": 1.8, "morpheme_ratio": 1.2, "sonority_score": 0.45, "clause_compression": 5.0},
    }
    fig = fingerprint_radar(heb, trans)
    assert isinstance(fig, go.Figure)


def test_pipeline_summary_chart():
    counts = {"verses": 2527, "word_tokens": 43000, "breath_profiles": 2527}
    fig = pipeline_summary_chart(counts)
    assert isinstance(fig, go.Figure)
```

---

## Acceptance Criteria

- [ ] `python -m pytest /pipeline/tests/test_visualize.py -v` — all 8 tests pass
- [ ] `modules/export.py` runs without error when called with valid `config` and `conn`
- [ ] Sphinx HTML site is generated at `/data/outputs/report/index.html`
- [ ] Typst PDF is generated at `/data/outputs/report.pdf` (or skipped with log message if typst not available)
- [ ] Streamlit app accessible at http://localhost:8501 with all 5 pages rendering
- [ ] Each Streamlit page loads without Python exceptions for valid Psalm/verse selections
- [ ] Breath curve page shows Hebrew trace + at least one translation trace
- [ ] Chiasm page shows appropriate empty state when no candidates exist
