# Plan: Stage 06a — Visualization Library

> **Depends on:** Plan 04 (translation_scores populated)
> **Status:** active

## Goal

Build five pure-function Plotly chart modules in `pipeline/visualize/` that accept pre-fetched
data as arguments and return `plotly.graph_objects.Figure` objects — with no database
dependency of their own.

## Acceptance Criteria

- `uv run --frozen pytest tests/test_visualize.py -v` reports 8 tests passed, 0 failed
- Each of the five visualize modules imports cleanly with no side effects
- `pipeline/visualize/__init__.py` exports all public functions
- `uv run --frozen ruff check .` reports no errors
- `uv run --frozen pyright` reports no type errors in the new modules

## Architecture

The `pipeline/visualize/` package contains five modules — `breath_curves`, `heatmaps`,
`arcs`, `radar`, and `report` — each exporting one or more pure functions that take
plain Python data structures (lists, dicts, floats) and return a `go.Figure`. Callers
(Streamlit pages, notebooks, the export module) are responsible for all DB queries and
pass the results in. This separation makes the chart logic fully unit-testable without a
database connection and reusable across all display contexts.

## Tech Stack

- Python 3.11
- `plotly` (`plotly.graph_objects`) — figure construction
- `pandas` — pivot table in `heatmaps.deviation_heatmap`
- `uv` — package management
- `pytest` — test runner
- `ruff` — lint and format
- `pyright` — type checking

---

## Tasks

### Task 1: Write failing tests for all five chart modules

**Files:** `tests/test_visualize.py`

**Steps:**

1. Write test file with all 8 tests. The imports will fail immediately because none of the
   source modules exist yet — that is the expected failure.

   ```python
   # tests/test_visualize.py
   """Unit tests for pipeline/visualize/ chart modules.

   All tests assert on figure structure (trace count, layout keys).
   No pixel values, no rendered output, no DB connections required.
   """
   from __future__ import annotations

   import sys
   from pathlib import Path

   sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

   import plotly.graph_objects as go
   import pytest

   from visualize.breath_curves import breath_curve_figure
   from visualize.heatmaps import deviation_heatmap, syllable_openness_heatmap
   from visualize.arcs import chiasm_arc_figure
   from visualize.radar import fingerprint_radar
   from visualize.report import pipeline_summary_chart


   # ── breath_curves ────────────────────────────────────────────────


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


   # ── heatmaps ────────────────────────────────────────────────────


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
       assert len(z) == 3          # 3 chapters
       assert len(z[0]) == 3       # 3 translations


   # ── arcs ────────────────────────────────────────────────────────


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


   # ── radar ────────────────────────────────────────────────────────


   def test_fingerprint_radar_has_all_translations() -> None:
       """Radar figure has N+1 traces: one Hebrew + one per translation."""
       hebrew_fp = {
           "syllable_density": 2.5,
           "morpheme_ratio": 1.8,
           "sonority_score": 0.55,
           "clause_compression": 4.0,
       }
       trans_fps = [
           {"syllable_density": 1.8, "morpheme_ratio": 1.2,
            "sonority_score": 0.45, "clause_compression": 5.0},
           {"syllable_density": 2.1, "morpheme_ratio": 1.5,
            "sonority_score": 0.50, "clause_compression": 4.5},
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
           fingerprints=[{
               "syllable_density": 1.8,
               "morpheme_ratio": 1.2,
               "sonority_score": 0.45,
               "clause_compression": 5.0,
           }],
           hebrew_fingerprint={
               "syllable_density": 2.5,
               "morpheme_ratio": 1.8,
               "sonority_score": 0.55,
               "clause_compression": 4.0,
           },
       )
       assert isinstance(fig, go.Figure)
       assert len(fig.data) == 2  # Hebrew + KJV


   # ── report ───────────────────────────────────────────────────────


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
   ```

2. Run and confirm FAILED (import errors are the expected failure):

   ```bash
   uv run --frozen pytest tests/test_visualize.py -v
   # Expected: ERROR — ModuleNotFoundError: No module named 'visualize'
   ```

3. No implementation yet — proceed to Task 2.

4. N/A — tests will not pass until all modules are implemented.

5. N/A — lint after implementation.

6. Commit: `"test: add 8 failing tests for visualize library (TDD red phase)"`

---

### Task 2: Create `pipeline/visualize/__init__.py`

**Files:** `pipeline/visualize/__init__.py`

**Steps:**

1. No new tests for this task — the Task 1 test file already imports from `visualize.*`.
   Creating the package allows imports to resolve.

2. Verify tests still fail after creating the package (modules are still absent):

   ```bash
   uv run --frozen pytest tests/test_visualize.py -v
   # Expected: ERROR — ImportError: cannot import name 'breath_curve_figure' from 'visualize.breath_curves'
   ```

3. Implement the package `__init__.py`. Note: the source document uses different function
   names (`breath_curve_overlay`, `arc_diagram`, etc.) — the plan uses the **new canonical
   names** from the module signatures specified in the task prompt. The `__init__.py` must
   match the names actually exported by each module.

   ```python
   # pipeline/visualize/__init__.py
   """Psalms NLP visualization package.

   Exports pure Plotly figure-factory functions.
   All functions accept pre-fetched data; no DB access.
   """
   from __future__ import annotations

   from .arcs import chiasm_arc_figure
   from .breath_curves import breath_curve_figure
   from .heatmaps import deviation_heatmap, syllable_openness_heatmap
   from .radar import fingerprint_radar
   from .report import pipeline_summary_chart

   __all__ = [
       "breath_curve_figure",
       "chiasm_arc_figure",
       "deviation_heatmap",
       "fingerprint_radar",
       "pipeline_summary_chart",
       "syllable_openness_heatmap",
   ]
   ```

4. Tests still fail (individual modules absent) — expected:

   ```bash
   uv run --frozen pytest tests/test_visualize.py -v
   # Expected: ERROR — cannot import name 'breath_curve_figure' from 'visualize.breath_curves'
   ```

5. N/A — lint after all modules exist.

6. Commit: `"feat: add visualize package __init__ (imports not yet resolvable)"`

---

### Task 3: Implement `pipeline/visualize/breath_curves.py`

**Files:** `pipeline/visualize/breath_curves.py`

**Steps:**

1. Tests `test_breath_curve_figure_returns_figure` and `test_breath_curve_has_hebrew_trace`
   already written in Task 1.

2. Run only those two tests and confirm they fail:

   ```bash
   uv run --frozen pytest tests/test_visualize.py::test_breath_curve_figure_returns_figure \
       tests/test_visualize.py::test_breath_curve_has_hebrew_trace -v
   # Expected: ERROR — cannot import name 'breath_curve_figure'
   ```

3. Implement:

   ```python
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
           title: Chart title; defaults to "Psalm — Breath Curve (verse {verse_id})".

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
                       f"Position: %{{x:.2f}}<br>Weight: %{{y:.3f}}"
                       f"<extra>{key}</extra>"
                   ),
               )
           )

       # Suggestion curves
       if suggestion_curves:
           for idx, (label, curve) in enumerate(suggestion_curves.items()):
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
           legend=dict(
               orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
           ),
           hovermode="x unified",
           template="plotly_white",
           height=400,
       )
       return fig
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_visualize.py::test_breath_curve_figure_returns_figure \
       tests/test_visualize.py::test_breath_curve_has_hebrew_trace -v
   # Expected: PASSED PASSED
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check pipeline/visualize/breath_curves.py --fix
   uv run --frozen pyright pipeline/visualize/breath_curves.py
   ```

6. Commit: `"feat: implement visualize/breath_curves.py with breath_curve_figure"`

---

### Task 4: Implement `pipeline/visualize/heatmaps.py`

**Files:** `pipeline/visualize/heatmaps.py`

**Steps:**

1. Test `test_deviation_heatmap_shape` already written in Task 1.

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_visualize.py::test_deviation_heatmap_shape -v
   # Expected: ERROR — cannot import name 'deviation_heatmap'
   ```

3. Implement. Note: the module signature uses positional list arguments rather than a
   list-of-dicts approach, which allows the test to assert on z-matrix dimensions directly.
   The `syllable_openness_heatmap` function is also included here because it is tested
   indirectly via the Streamlit explorer (plan 06b) and exported from `__init__.py`.

   ```python
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
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_visualize.py::test_deviation_heatmap_shape -v
   # Expected: PASSED
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check pipeline/visualize/heatmaps.py --fix
   uv run --frozen pyright pipeline/visualize/heatmaps.py
   ```

6. Commit: `"feat: implement visualize/heatmaps.py with deviation and openness heatmaps"`

---

### Task 5: Implement `pipeline/visualize/arcs.py`

**Files:** `pipeline/visualize/arcs.py`

**Steps:**

1. Tests `test_chiasm_arc_count` and `test_empty_arc_pairs_no_error` already written.

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_visualize.py::test_chiasm_arc_count \
       tests/test_visualize.py::test_empty_arc_pairs_no_error -v
   # Expected: ERROR — cannot import name 'chiasm_arc_figure'
   ```

3. Implement. The key contract tested is that `len(fig.layout.shapes) == len(arc_pairs)`.
   Each `(start_idx, end_idx, confidence)` tuple must produce exactly one SVG path shape.

   ```python
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

       # Baseline
       if n > 0:
           fig.add_shape(
               type="line",
               x0=0,
               x1=max(x_positions),
               y0=0,
               y1=0,
               line=dict(color="lightgray", width=2),
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
           arc_pairs, pattern_types
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
               path=(
                   f"M {x_a:.3f} 0 "
                   f"Q {x_mid:.3f} {arc_height:.3f} "
                   f"{x_b:.3f} 0"
               ),
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
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_visualize.py::test_chiasm_arc_count \
       tests/test_visualize.py::test_empty_arc_pairs_no_error -v
   # Expected: PASSED PASSED
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check pipeline/visualize/arcs.py --fix
   uv run --frozen pyright pipeline/visualize/arcs.py
   ```

6. Commit: `"feat: implement visualize/arcs.py with chiasm_arc_figure"`

---

### Task 6: Implement `pipeline/visualize/radar.py`

**Files:** `pipeline/visualize/radar.py`

**Steps:**

1. Tests `test_fingerprint_radar_has_all_translations` and
   `test_fingerprint_radar_single_translation` already written.

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_visualize.py::test_fingerprint_radar_has_all_translations \
       tests/test_visualize.py::test_fingerprint_radar_single_translation -v
   # Expected: ERROR — cannot import name 'fingerprint_radar'
   ```

3. Implement:

   ```python
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
           for dim, max_val in zip(_DIMENSIONS, _MAX_VALS)
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
       for idx, (label, fp) in enumerate(zip(labels, fingerprints)):
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
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest \
       tests/test_visualize.py::test_fingerprint_radar_has_all_translations \
       tests/test_visualize.py::test_fingerprint_radar_single_translation -v
   # Expected: PASSED PASSED
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check pipeline/visualize/radar.py --fix
   uv run --frozen pyright pipeline/visualize/radar.py
   ```

6. Commit: `"feat: implement visualize/radar.py with fingerprint_radar"`

---

### Task 7: Implement `pipeline/visualize/report.py`

**Files:** `pipeline/visualize/report.py`

**Steps:**

1. Test `test_pipeline_summary_bar_count` already written.

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_visualize.py::test_pipeline_summary_bar_count -v
   # Expected: ERROR — cannot import name 'pipeline_summary_chart'
   ```

3. Implement. The test asserts that `len(bar_trace.x) == len(counts)`, so the function
   must produce a `go.Bar` trace with one bar per table in `row_counts`.
   The `run_history` parameter is accepted (for Streamlit use) but the test passes an
   empty list, so it must not be required.

   ```python
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
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_visualize.py::test_pipeline_summary_bar_count -v
   # Expected: PASSED
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check pipeline/visualize/report.py --fix
   uv run --frozen pyright pipeline/visualize/report.py
   ```

6. Commit: `"feat: implement visualize/report.py with pipeline_summary_chart"`

---

### Task 8: Run full test suite and confirm green

**Files:** `tests/test_visualize.py`, all `pipeline/visualize/*.py`

**Steps:**

1. No new tests.

2. Run complete test file:

   ```bash
   uv run --frozen pytest tests/test_visualize.py -v
   # Expected: all 8 PASSED
   ```

   If any test fails, diagnose before proceeding.

3. No implementation changes — fix only if a test is failing.

4. Confirm all 8 pass:

   ```bash
   uv run --frozen pytest tests/test_visualize.py -v
   # Expected output:
   # tests/test_visualize.py::test_breath_curve_figure_returns_figure PASSED
   # tests/test_visualize.py::test_breath_curve_has_hebrew_trace PASSED
   # tests/test_visualize.py::test_deviation_heatmap_shape PASSED
   # tests/test_visualize.py::test_chiasm_arc_count PASSED
   # tests/test_visualize.py::test_empty_arc_pairs_no_error PASSED
   # tests/test_visualize.py::test_fingerprint_radar_has_all_translations PASSED
   # tests/test_visualize.py::test_fingerprint_radar_single_translation PASSED
   # tests/test_visualize.py::test_pipeline_summary_bar_count PASSED
   # 8 passed
   ```

5. Full lint + typecheck across the package:

   ```bash
   uv run --frozen ruff check pipeline/visualize/ --fix
   uv run --frozen ruff format pipeline/visualize/
   uv run --frozen pyright pipeline/visualize/
   ```

6. Commit: `"feat: visualization library complete — 8 tests green (Stage 06a)"`
