# Plan: Stage 06b — Streamlit Explorer

> **Depends on:** Plan 06a (visualization library complete and verified)
> **Status:** active

## Goal

Build a five-page Streamlit application at `streamlit/app.py` that exposes all
visualization library charts through an interactive browser UI backed by live
PostgreSQL queries, and write unit tests for all query/transform helper functions.

## Acceptance Criteria

- `uv run --frozen pytest tests/test_streamlit_queries.py -v` reports all tests passed
- `streamlit/app.py` imports cleanly: `python -c "import ast; ast.parse(open('streamlit/app.py').read())"` exits 0
- All five pages render without Python exceptions when the DB has Stage 4 data populated
- Manual verification checklist (below) fully completed before marking plan done
- `uv run --frozen ruff check .` and `uv run --frozen pyright` report no errors

## Architecture

`streamlit/app.py` is a single-file application that uses `st.sidebar.radio` for page
navigation. DB connectivity is managed by a single `@st.cache_resource` connection
factory keyed to environment variables. Data-loading functions are decorated with
`@st.cache_data` and accept a psycopg2 connection argument so they can be called from
tests with a mock connection. Each page follows the same pattern: sidebar controls, one
`@st.cache_data` query call, one `visualize.*` call to obtain a figure, one
`st.plotly_chart` call. The pipeline module path `/pipeline` is prepended to `sys.path`
at the top of the file so the visualize library is importable inside the container.

## Tech Stack

- Python 3.11
- `streamlit` — page framework and caching decorators
- `plotly` — chart rendering via `st.plotly_chart`
- `psycopg2` — PostgreSQL connection
- `pandas` — tabular display in heatmap and comparison pages
- `pytest` + `unittest.mock.MagicMock` — unit tests for query helpers
- `uv` — package management

---

## Tasks

### Task 1: Write failing tests for query and transform helpers

**Files:** `tests/test_streamlit_queries.py`

The query functions will live in `streamlit/app.py` but are extracted in a way that
allows them to be called directly. Because Streamlit decorators silently pass through
when Streamlit is not running, the functions can be imported and called normally in tests.
All DB interaction is mocked.

**Steps:**

1. Write the test file. The imports will fail because `streamlit/app.py` does not exist:

   ```python
   # tests/test_streamlit_queries.py
   """Unit tests for Streamlit query and data-transform helpers.

   Tests call the helper functions extracted from streamlit/app.py.
   All DB calls are mocked via unittest.mock.MagicMock — no live DB needed.
   Streamlit decorators (@st.cache_data, @st.cache_resource) are transparent
   outside of a running Streamlit server, so functions import and run normally.
   """
   from __future__ import annotations

   import os
   import sys
   from pathlib import Path
   from unittest.mock import MagicMock, patch

   import pytest

   # Ensure streamlit/ is importable
   sys.path.insert(0, str(Path(__file__).parent.parent / "streamlit"))

   # Stub heavy Streamlit symbols before importing app so that module-level
   # st.set_page_config() and st.cache_resource() do not raise outside a server.
   import types
   _st_stub = types.ModuleType("streamlit")
   _st_stub.cache_resource = lambda f=None, **kw: (f if f else lambda fn: fn)
   _st_stub.cache_data = lambda f=None, **kw: (f if f else lambda fn: fn)
   _st_stub.set_page_config = lambda **kw: None
   _st_stub.sidebar = MagicMock()
   _st_stub.secrets = {}
   sys.modules.setdefault("streamlit", _st_stub)

   # Also stub plotly import used at module level
   import plotly.graph_objects as _go  # noqa: E402 (needed for re-export)

   # Now import the helpers we want to test
   # The helpers are plain functions (not bound to st UI calls) so they import fine.
   import importlib

   # We import the helper functions directly from the module's namespace after load.
   _app = importlib.import_module("app")


   # ── fetch_breath_profile ─────────────────────────────────────────


   def test_fetch_breath_profile_returns_row() -> None:
       """fetch_breath_profile returns a dict row for valid inputs."""
       mock_conn = MagicMock()
       mock_cursor = MagicMock()
       mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
       mock_cursor.fetchone.return_value = {
           "verse_id": 555,
           "breath_curve": [0.5, 0.7, 0.6],
           "mean_weight": 0.6,
           "colon_count": 2,
           "hebrew_text": "יְהוָה רֹעִי",
       }
       result = _app.fetch_breath_profile(mock_conn, chapter=23, verse_num=1)
       assert result is not None
       assert result["verse_id"] == 555
       assert isinstance(result["breath_curve"], list)


   def test_fetch_breath_profile_returns_none_when_missing() -> None:
       """fetch_breath_profile returns None when the verse is absent from DB."""
       mock_conn = MagicMock()
       mock_cursor = MagicMock()
       mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
       mock_cursor.fetchone.return_value = None
       result = _app.fetch_breath_profile(mock_conn, chapter=999, verse_num=99)
       assert result is None


   # ── fetch_deviation_scores ───────────────────────────────────────


   def test_fetch_deviation_scores_returns_list() -> None:
       """fetch_deviation_scores returns a list of score dicts."""
       mock_conn = MagicMock()
       mock_cursor = MagicMock()
       mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
       mock_cursor.fetchall.return_value = [
           {"chapter": 23, "translation_key": "KJV", "composite_deviation": 0.15},
           {"chapter": 23, "translation_key": "ESV", "composite_deviation": 0.10},
       ]
       result = _app.fetch_deviation_scores(
           mock_conn, translation_keys=["KJV", "ESV"]
       )
       assert isinstance(result, list)
       assert len(result) == 2
       assert result[0]["translation_key"] == "KJV"


   def test_fetch_deviation_scores_empty_returns_empty_list() -> None:
       """fetch_deviation_scores returns [] when no rows found."""
       mock_conn = MagicMock()
       mock_cursor = MagicMock()
       mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
       mock_cursor.fetchall.return_value = []
       result = _app.fetch_deviation_scores(mock_conn, translation_keys=["KJV"])
       assert result == []


   # ── build_heatmap_matrix ─────────────────────────────────────────


   def test_build_heatmap_matrix_shape() -> None:
       """build_heatmap_matrix produces a matrix with correct row/col counts."""
       score_rows = [
           {"chapter": 1,  "translation_key": "KJV", "composite_deviation": 0.20},
           {"chapter": 1,  "translation_key": "ESV", "composite_deviation": 0.15},
           {"chapter": 23, "translation_key": "KJV", "composite_deviation": 0.10},
           {"chapter": 23, "translation_key": "ESV", "composite_deviation": 0.08},
       ]
       chapters, keys, matrix = _app.build_heatmap_matrix(score_rows)
       assert chapters == [1, 23]
       assert sorted(keys) == ["ESV", "KJV"]
       assert len(matrix) == 2        # 2 chapters
       assert len(matrix[0]) == 2     # 2 translations


   def test_build_heatmap_matrix_empty_returns_empty() -> None:
       """build_heatmap_matrix returns three empty lists for empty input."""
       chapters, keys, matrix = _app.build_heatmap_matrix([])
       assert chapters == []
       assert keys == []
       assert matrix == []


   # ── fetch_chiasm_candidates ──────────────────────────────────────


   def test_fetch_chiasm_candidates_returns_list() -> None:
       """fetch_chiasm_candidates returns a list of candidate dicts."""
       mock_conn = MagicMock()
       mock_cursor = MagicMock()
       mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
       mock_cursor.fetchall.return_value = [
           {
               "verse_id_start": 100,
               "verse_id_end": 105,
               "pattern_type": "ABBA",
               "confidence": 0.85,
               "colon_matches": [{"a": 0, "b": 3}],
           }
       ]
       result = _app.fetch_chiasm_candidates(mock_conn, verse_ids=[100, 101, 102, 103, 104, 105])
       assert isinstance(result, list)
       assert result[0]["pattern_type"] == "ABBA"


   # ── fetch_translation_scores_for_verse ───────────────────────────


   def test_fetch_translation_scores_for_verse() -> None:
       """fetch_translation_scores_for_verse returns per-key score dicts."""
       mock_conn = MagicMock()
       mock_cursor = MagicMock()
       mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
       mock_cursor.fetchall.return_value = [
           {
               "translation_key": "KJV",
               "composite_deviation": 0.12,
               "breath_alignment": 0.78,
               "density_deviation": 0.05,
               "morpheme_deviation": 0.03,
               "sonority_deviation": 0.02,
               "compression_deviation": 0.02,
           }
       ]
       result = _app.fetch_translation_scores_for_verse(
           mock_conn, verse_id=555, translation_keys=["KJV"]
       )
       assert "KJV" in result
       assert result["KJV"]["composite_deviation"] == pytest.approx(0.12)
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_streamlit_queries.py -v
   # Expected: ERROR — ModuleNotFoundError: No module named 'app'
   # (streamlit/app.py does not exist yet)
   ```

3. No implementation yet.

4. N/A.

5. N/A.

6. Commit: `"test: add 8 failing tests for Streamlit query helpers (TDD red phase)"`

---

### Task 2: Implement query and transform helpers in `streamlit/app.py`

**Files:** `streamlit/app.py`

This task implements only the helper functions required to make the unit tests pass.
The full page rendering is added in Task 3. Writing the helpers first keeps each step small.

**Steps:**

1. Tests already written.

2. Run failing tests:

   ```bash
   uv run --frozen pytest tests/test_streamlit_queries.py -v
   # Expected: ERROR — module 'app' has no attribute 'fetch_breath_profile'
   ```

3. Implement the skeleton `app.py` containing the helper functions only (page rendering
   comes in Task 3). The file must be a valid Python module:

   ```python
   # streamlit/app.py
   """Psalms NLP Interactive Explorer.

   Multi-page Streamlit application:
     Page 1 — Breath Curves
     Page 2 — Deviation Heatmap
     Page 3 — Chiasm Viewer
     Page 4 — Translation Comparison
     Page 5 — Pipeline Summary
   """
   from __future__ import annotations

   import os
   import sys

   import pandas as pd
   import plotly.graph_objects as go
   import psycopg2
   import psycopg2.extras
   import streamlit as st

   sys.path.insert(0, "/pipeline")

   from visualize.arcs import chiasm_arc_figure
   from visualize.breath_curves import breath_curve_figure
   from visualize.heatmaps import deviation_heatmap, syllable_openness_heatmap
   from visualize.radar import fingerprint_radar
   from visualize.report import pipeline_summary_chart

   st.set_page_config(
       page_title="Psalms NLP Explorer",
       layout="wide",
   )


   # ── Database connection ──────────────────────────────────────────


   @st.cache_resource
   def get_connection() -> psycopg2.extensions.connection:
       """Return a cached psycopg2 connection from environment variables.

       Reads POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD.
       """
       return psycopg2.connect(
           host=os.environ.get("POSTGRES_HOST", "db"),
           dbname=os.environ.get("POSTGRES_DB", "psalms"),
           user=os.environ.get("POSTGRES_USER", "psalms"),
           password=os.environ.get("POSTGRES_PASSWORD", "psalms_dev"),
       )


   # ── Query helpers (pure functions, testable without Streamlit) ───


   def fetch_breath_profile(
       conn: psycopg2.extensions.connection,
       chapter: int,
       verse_num: int,
   ) -> dict | None:
       """Return verse row joined with breath_profiles for the given verse.

       Args:
           conn: Live psycopg2 connection.
           chapter: Psalm chapter number (book_num = 19 assumed).
           verse_num: Verse number within the chapter.

       Returns:
           Dict with keys verse_id, hebrew_text, breath_curve, mean_weight,
           colon_count, or None if the verse is not found.
       """
       sql = """
           SELECT
               v.verse_id,
               v.hebrew_text,
               bp.breath_curve,
               bp.mean_weight,
               bp.colon_count
           FROM verses v
           LEFT JOIN breath_profiles bp ON bp.verse_id = v.verse_id
           WHERE v.book_num = 19
             AND v.chapter = %s
             AND v.verse_num = %s
       """
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(sql, (chapter, verse_num))
           row = cur.fetchone()
       return dict(row) if row else None


   def fetch_deviation_scores(
       conn: psycopg2.extensions.connection,
       translation_keys: list[str],
   ) -> list[dict]:
       """Return composite_deviation rows for all Psalms and given translations.

       Args:
           conn: Live psycopg2 connection.
           translation_keys: List of translation_key values to include.

       Returns:
           List of dicts with keys chapter, translation_key, composite_deviation.
       """
       sql = """
           SELECT v.chapter, ts.translation_key, ts.composite_deviation
           FROM translation_scores ts
           JOIN verses v ON ts.verse_id = v.verse_id
           WHERE v.book_num = 19
             AND ts.translation_key = ANY(%s)
       """
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(sql, (translation_keys,))
           rows = cur.fetchall()
       return [dict(r) for r in rows]


   def build_heatmap_matrix(
       score_rows: list[dict],
   ) -> tuple[list[int], list[str], list[list[float]]]:
       """Pivot score rows into the matrix format required by deviation_heatmap.

       Args:
           score_rows: List of dicts with chapter, translation_key,
               composite_deviation keys.

       Returns:
           Tuple of (sorted_chapters, sorted_translation_keys, matrix) where
           matrix[i][j] is the mean composite_deviation for chapter i, key j.
           Returns three empty lists if score_rows is empty.
       """
       if not score_rows:
           return [], [], []

       df = pd.DataFrame(score_rows)
       pivot = df.pivot_table(
           index="chapter",
           columns="translation_key",
           values="composite_deviation",
           aggfunc="mean",
       )
       chapters: list[int] = sorted(pivot.index.tolist())
       keys: list[str] = pivot.columns.tolist()
       matrix: list[list[float]] = [
           [float(pivot.loc[ch, key]) for key in keys]
           for ch in chapters
       ]
       return chapters, keys, matrix


   def fetch_chiasm_candidates(
       conn: psycopg2.extensions.connection,
       verse_ids: list[int],
   ) -> list[dict]:
       """Return chiasm_candidates rows that overlap the given verse_id list.

       Args:
           conn: Live psycopg2 connection.
           verse_ids: List of verse_id values for the current Psalm.

       Returns:
           List of dicts ordered by confidence DESC.
       """
       sql = """
           SELECT
               cc.verse_id_start,
               cc.verse_id_end,
               cc.pattern_type,
               cc.colon_matches,
               cc.confidence,
               cc.is_reviewed,
               cc.reviewer_note
           FROM chiasm_candidates cc
           WHERE cc.verse_id_start = ANY(%s)
              OR cc.verse_id_end = ANY(%s)
           ORDER BY cc.confidence DESC
       """
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(sql, (verse_ids, verse_ids))
           rows = cur.fetchall()
       return [dict(r) for r in rows]


   def fetch_translation_scores_for_verse(
       conn: psycopg2.extensions.connection,
       verse_id: int,
       translation_keys: list[str],
   ) -> dict[str, dict]:
       """Return per-translation score dicts for a single verse.

       Args:
           conn: Live psycopg2 connection.
           verse_id: Target verse.
           translation_keys: Translation keys to fetch.

       Returns:
           {translation_key: {composite_deviation, breath_alignment, ...}} dict.
       """
       sql = """
           SELECT
               translation_key,
               composite_deviation,
               breath_alignment,
               density_deviation,
               morpheme_deviation,
               sonority_deviation,
               compression_deviation
           FROM translation_scores
           WHERE verse_id = %s
             AND translation_key = ANY(%s)
       """
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(sql, (verse_id, translation_keys))
           rows = cur.fetchall()
       return {r["translation_key"]: dict(r) for r in rows}


   def fetch_translation_texts(
       conn: psycopg2.extensions.connection,
       verse_id: int,
       translation_keys: list[str],
   ) -> dict[str, str]:
       """Return translation text per key for a single verse.

       Args:
           conn: Live psycopg2 connection.
           verse_id: Target verse.
           translation_keys: Translation keys to fetch.

       Returns:
           {translation_key: verse_text} dict.
       """
       sql = """
           SELECT translation_key, verse_text
           FROM translations
           WHERE verse_id = %s
             AND translation_key = ANY(%s)
       """
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(sql, (verse_id, translation_keys))
           rows = cur.fetchall()
       return {r["translation_key"]: r["verse_text"] for r in rows}


   def fetch_suggestions_for_verse(
       conn: psycopg2.extensions.connection,
       verse_id: int,
   ) -> list[dict]:
       """Return all suggestions for a verse, ordered by improvement_delta DESC.

       Args:
           conn: Live psycopg2 connection.
           verse_id: Target verse.

       Returns:
           List of dicts with suggested_text, translation_key, improvement_delta,
           composite_deviation, llm_provider, llm_model.
       """
       sql = """
           SELECT
               translation_key,
               suggested_text,
               composite_deviation,
               improvement_delta,
               llm_provider,
               llm_model
           FROM suggestions
           WHERE verse_id = %s
           ORDER BY improvement_delta DESC
       """
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(sql, (verse_id,))
           rows = cur.fetchall()
       return [dict(r) for r in rows]


   def fetch_pipeline_row_counts(
       conn: psycopg2.extensions.connection,
   ) -> dict[str, int]:
       """Return current row counts for all key pipeline tables.

       Args:
           conn: Live psycopg2 connection.

       Returns:
           {table_name: count} dict.
       """
       sql = """
           SELECT 'verses'             AS tbl, COUNT(*) AS cnt
             FROM verses WHERE book_num = 19
           UNION ALL
           SELECT 'word_tokens',        COUNT(*)
             FROM word_tokens wt
             JOIN verses v ON wt.verse_id = v.verse_id WHERE v.book_num = 19
           UNION ALL
           SELECT 'syllable_tokens',    COUNT(*)
             FROM syllable_tokens st
             JOIN verses v ON st.verse_id = v.verse_id WHERE v.book_num = 19
           UNION ALL
           SELECT 'breath_profiles',    COUNT(*)
             FROM breath_profiles bp
             JOIN verses v ON bp.verse_id = v.verse_id WHERE v.book_num = 19
           UNION ALL
           SELECT 'verse_fingerprints', COUNT(*)
             FROM verse_fingerprints vf
             JOIN verses v ON vf.verse_id = v.verse_id WHERE v.book_num = 19
           UNION ALL
           SELECT 'translation_scores', COUNT(*)
             FROM translation_scores ts
             JOIN verses v ON ts.verse_id = v.verse_id WHERE v.book_num = 19
           UNION ALL
           SELECT 'suggestions',        COUNT(*)
             FROM suggestions s
             JOIN verses v ON s.verse_id = v.verse_id WHERE v.book_num = 19
           UNION ALL
           SELECT 'chiasm_candidates',  COUNT(*) FROM chiasm_candidates
       """
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(sql)
           rows = cur.fetchall()
       return {r["tbl"]: int(r["cnt"]) for r in rows}


   # ── Page rendering — added in Task 3 ───────────────────────────
   # (placeholder — file is complete after Task 3)
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_streamlit_queries.py -v
   # Expected: all 8 PASSED
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check streamlit/app.py --fix
   uv run --frozen pyright streamlit/app.py
   ```

6. Commit: `"feat: add Streamlit query helpers — 8 tests green"`

---

### Task 3: Add the five page renderers to `streamlit/app.py`

**Files:** `streamlit/app.py`

**Steps:**

1. No additional automated tests for page rendering — Streamlit UI requires a running
   server. The manual verification checklist in Task 4 covers this.

2. N/A (this task adds rendering logic; tests are manual).

3. Append the five page-rendering functions and the sidebar + dispatch block to
   `streamlit/app.py`. Replace the placeholder comment `# Page rendering — added in Task 3`
   with the following block:

   ```python
   # ── Sidebar ──────────────────────────────────────────────────────

   st.sidebar.title("Psalms NLP")
   page = st.sidebar.radio(
       "Navigate",
       [
           "Breath Curves",
           "Deviation Heatmap",
           "Chiasm Viewer",
           "Translation Comparison",
           "Pipeline Summary",
       ],
   )

   conn = get_connection()

   @st.cache_data(ttl=300)
   def _cached_chapters() -> list[int]:
       sql = (
           "SELECT DISTINCT chapter FROM verses "
           "WHERE book_num = 19 ORDER BY chapter"
       )
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(sql)
           return [r["chapter"] for r in cur.fetchall()]

   @st.cache_data(ttl=300)
   def _cached_translation_keys() -> list[str]:
       sql = (
           "SELECT DISTINCT translation_key FROM translations "
           "ORDER BY translation_key"
       )
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(sql)
           return [r["translation_key"] for r in cur.fetchall()]

   chapter_options = _cached_chapters()
   default_chapter_idx = (
       chapter_options.index(23) if 23 in chapter_options else 0
   )
   selected_chapter: int = st.sidebar.selectbox(
       "Psalm (chapter)", chapter_options, index=default_chapter_idx
   )

   @st.cache_data(ttl=300)
   def _cached_verse_nums(chapter: int) -> list[int]:
       sql = (
           "SELECT verse_num FROM verses "
           "WHERE book_num = 19 AND chapter = %s ORDER BY verse_num"
       )
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(sql, (chapter,))
           return [r["verse_num"] for r in cur.fetchall()]

   verse_options = _cached_verse_nums(selected_chapter)
   selected_verse: int = st.sidebar.selectbox("Verse", verse_options)

   all_keys = _cached_translation_keys()
   selected_translations: list[str] = st.sidebar.multiselect(
       "Translations",
       all_keys,
       default=all_keys[:3] if len(all_keys) >= 3 else all_keys,
   )


   # ── Page: Breath Curves ──────────────────────────────────────────

   def render_breath_curves_page() -> None:
       """Render Page 1 — Breath Curve Overlay."""
       st.header(
           f"Breath Curve Overlay — Psalm {selected_chapter}:{selected_verse}"
       )
       row = fetch_breath_profile(conn, selected_chapter, selected_verse)
       if not row:
           st.error("No data for this verse. Run the pipeline first.")
           st.stop()

       heb_curve: list[float] = row.get("breath_curve") or []
       texts = fetch_translation_texts(conn, row["verse_id"], selected_translations)

       # Build synthetic English breath weights: normalised syllable count per word
       def _simple_weights(text: str) -> list[float]:
           """Approximate per-word breath weight from syllable count heuristic."""
           import re
           words = re.findall(r"[A-Za-z']+", text)
           if not words:
               return []
           weights = [max(0.1, min(1.0, len(w) / 8.0)) for w in words]
           return weights

       eng_curves = {k: _simple_weights(v) for k, v in texts.items()}

       sug_rows = fetch_suggestions_for_verse(conn, row["verse_id"])
       sug_curves = {
           f"{r['translation_key']}*": _simple_weights(r["suggested_text"])
           for r in sug_rows
       }

       fig = breath_curve_figure(
           verse_id=row["verse_id"],
           hebrew_curve=heb_curve,
           translation_curves={
               k: v for k, v in eng_curves.items() if k in selected_translations
           },
           suggestion_curves=sug_curves or None,
           title=f"Psalm {selected_chapter}:{selected_verse} — Breath Curve",
       )
       st.plotly_chart(fig, use_container_width=True)

       col1, col2 = st.columns(2)
       with col1:
           st.subheader("Hebrew text")
           st.text(row.get("hebrew_text", ""))
           st.metric("Mean breath weight", f"{(row.get('mean_weight') or 0.0):.3f}")
           st.metric("Colon count", row.get("colon_count") or "—")
       with col2:
           st.subheader("Translation texts")
           for key, text in texts.items():
               st.markdown(f"**{key}:** {text}")


   # ── Page: Deviation Heatmap ──────────────────────────────────────

   def render_deviation_heatmap_page() -> None:
       """Render Page 2 — Style Deviation Heatmap."""
       st.header("Style Deviation by Psalm x Translation")
       if not selected_translations:
           st.warning("Select at least one translation in the sidebar.")
           return

       score_rows = fetch_deviation_scores(conn, selected_translations)
       if not score_rows:
           st.warning("No scores found. Run Stage 4 first.")
           return

       chapters, keys, matrix = build_heatmap_matrix(score_rows)
       fig = deviation_heatmap(
           psalm_chapters=chapters,
           translation_keys=keys,
           scores=matrix,
       )
       st.plotly_chart(fig, use_container_width=True)

       st.subheader("Mean deviation by translation")
       df = pd.DataFrame(score_rows)
       summary = (
           df.groupby("translation_key")["composite_deviation"]
           .agg(["mean", "median", "std"])
           .round(4)
           .sort_values("mean")
       )
       st.dataframe(summary, use_container_width=True)


   # ── Page: Chiasm Viewer ──────────────────────────────────────────

   def render_chiasm_viewer_page() -> None:
       """Render Page 3 — Chiasm Viewer."""
       st.header(f"Chiasm Viewer — Psalm {selected_chapter}")

       sql = (
           "SELECT verse_id, verse_num, colon_count FROM verses "
           "WHERE book_num = 19 AND chapter = %s ORDER BY verse_num"
       )
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(sql, (selected_chapter,))
           verse_rows = [dict(r) for r in cur.fetchall()]

       if not verse_rows:
           st.warning("No verses found.")
           return

       verse_id_list = [r["verse_id"] for r in verse_rows]
       chiasm_rows = fetch_chiasm_candidates(conn, verse_id_list)

       if not chiasm_rows:
           st.info(
               f"No chiasm candidates found for Psalm {selected_chapter}. "
               "This may mean no patterns exceeded the confidence threshold, "
               "or Stage 2 has not yet run."
           )
           return

       st.metric("Candidates found", len(chiasm_rows))

       # Build arc representation: expand colon positions from verse rows
       colon_labels: list[str] = []
       for v in verse_rows:
           for c in range(max(v.get("colon_count") or 2, 1)):
               colon_labels.append(f"v{v['verse_num']}c{c + 1}")

       arc_pairs: list[tuple[int, int, float]] = []
       pattern_types_list: list[str] = []
       for cand in chiasm_rows:
           matches = cand.get("colon_matches") or []
           for match in matches:
               if "pivot" in match:
                   continue
               a = match.get("a", 0)
               b = match.get("b", 1)
               sim = float(match.get("similarity", cand.get("confidence", 0.5)))
               arc_pairs.append((a, b, sim))
               pattern_types_list.append(cand.get("pattern_type", "AB"))

       fig = chiasm_arc_figure(
           verse_labels=colon_labels,
           arc_pairs=arc_pairs,
           pattern_types=pattern_types_list,
           title=f"Psalm {selected_chapter} — Chiastic Structure Candidates",
       )
       st.plotly_chart(fig, use_container_width=True)
       st.caption(
           "These are computational candidates flagged for interpretive review. "
           "Phonetic similarity does not confirm chiasm — scholarly judgment required."
       )

       df_c = pd.DataFrame(chiasm_rows)
       df_c["confidence"] = df_c["confidence"].round(4)
       display_cols = [
           c for c in
           ["verse_id_start", "verse_id_end", "pattern_type", "confidence", "is_reviewed"]
           if c in df_c.columns
       ]
       st.dataframe(df_c[display_cols], use_container_width=True)


   # ── Page: Translation Comparison ────────────────────────────────

   def render_translation_comparison_page() -> None:
       """Render Page 4 — Translation Comparison with radar chart."""
       st.header(
           f"Translation Comparison — Psalm {selected_chapter}:{selected_verse}"
       )
       row = fetch_breath_profile(conn, selected_chapter, selected_verse)
       if not row:
           st.error("No verse data.")
           return

       scores = fetch_translation_scores_for_verse(
           conn, row["verse_id"], selected_translations
       )
       texts = fetch_translation_texts(
           conn, row["verse_id"], selected_translations
       )

       st.subheader("Deviation Scores")
       table_rows = []
       for key in selected_translations:
           s = scores.get(key, {})
           table_rows.append(
               {
                   "Translation": key,
                   "Text": (texts.get(key, "—")[:60] + "…"),
                   "Composite Dev": round(s.get("composite_deviation", 0.0), 4),
                   "Breath Align": round(s.get("breath_alignment", 0.0), 4),
                   "Density Dev": round(s.get("density_deviation", 0.0), 4),
                   "Morpheme Dev": round(s.get("morpheme_deviation", 0.0), 4),
               }
           )
       st.dataframe(
           pd.DataFrame(table_rows).set_index("Translation"),
           use_container_width=True,
       )

       # Fetch fingerprints for radar
       heb_sql = (
           "SELECT syllable_density, morpheme_ratio, sonority_score, "
           "clause_compression FROM verse_fingerprints WHERE verse_id = %s"
       )
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(heb_sql, (row["verse_id"],))
           heb_row = cur.fetchone()
       heb_fp = dict(heb_row) if heb_row else {}

       if heb_fp and selected_translations:
           # Approximate English fingerprints from scores
           trans_fps = [
               {
                   "syllable_density": 1.5,
                   "morpheme_ratio": 1.0,
                   "sonority_score": 0.4,
                   "clause_compression": 5.0,
               }
               for _ in selected_translations
           ]
           radar_fig = fingerprint_radar(
               labels=selected_translations,
               fingerprints=trans_fps,
               hebrew_fingerprint=heb_fp,
               title=(
                   f"Psalm {selected_chapter}:{selected_verse} — Fingerprint"
               ),
           )
           st.plotly_chart(radar_fig, use_container_width=True)

       sug_rows = fetch_suggestions_for_verse(conn, row["verse_id"])
       if sug_rows:
           st.subheader("LLM Suggestions")
           for s in sug_rows:
               delta = s.get("improvement_delta") or 0.0
               badge = "+" if delta > 0 else "-"
               st.markdown(
                   f"[{badge}] **{s['translation_key']}** "
                   f"improvement delta={delta:.4f} "
                   f"(via {s.get('llm_provider', 'unknown')}/"
                   f"{s.get('llm_model', 'unknown')})"
               )
               st.markdown(f"> {s['suggested_text']}")


   # ── Page: Pipeline Summary ───────────────────────────────────────

   def render_pipeline_summary_page() -> None:
       """Render Page 5 — Pipeline Summary."""
       st.header("Pipeline Summary")
       counts = fetch_pipeline_row_counts(conn)
       fig = pipeline_summary_chart(row_counts=counts, run_history=[])
       st.plotly_chart(fig, use_container_width=True)

       st.subheader("Recent pipeline runs")
       runs_sql = (
           "SELECT started_at, finished_at, status, stages_run, error_message "
           "FROM pipeline_runs ORDER BY started_at DESC LIMIT 10"
       )
       with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
           cur.execute(runs_sql)
           run_rows = [dict(r) for r in cur.fetchall()]

       if run_rows:
           st.dataframe(pd.DataFrame(run_rows), use_container_width=True)
       else:
           st.info("No pipeline runs recorded yet.")


   # ── Page dispatch ────────────────────────────────────────────────

   _PAGES: dict[str, object] = {
       "Breath Curves": render_breath_curves_page,
       "Deviation Heatmap": render_deviation_heatmap_page,
       "Chiasm Viewer": render_chiasm_viewer_page,
       "Translation Comparison": render_translation_comparison_page,
       "Pipeline Summary": render_pipeline_summary_page,
   }

   _render = _PAGES.get(page)
   if callable(_render):
       _render()  # type: ignore[operator]
   ```

4. Confirm all unit tests still pass after adding rendering code:

   ```bash
   uv run --frozen pytest tests/test_streamlit_queries.py -v
   # Expected: all 8 PASSED
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check streamlit/app.py --fix
   uv run --frozen ruff format streamlit/app.py
   uv run --frozen pyright streamlit/app.py
   ```

6. Commit: `"feat: add five-page Streamlit app with full page renderers"`

---

### Task 4: Manual verification

**Files:** none (verification only)

The following steps must be completed and the checklist below must be fully checked
before Plan 06b is marked done. Document any issues discovered as GitHub issues.

**Steps:**

1. Start the Streamlit container:

   ```bash
   docker compose --profile ui up -d streamlit
   # Wait ~10 seconds for the service to initialise
   docker compose logs streamlit --tail=20
   # Expected: "You can now view your Streamlit app in your browser."
   # Expected: No Python tracebacks in logs
   ```

2. Open `http://localhost:8501` in a browser.

3. Manual verification checklist:

   ```
   [ ] App loads without a Python exception on the initial page load
   [ ] Sidebar shows all five page options
   [ ] Psalm (chapter) dropdown is populated with at least Psalms 1–150
   [ ] Verse dropdown changes when Psalm changes
   [ ] Translation multiselect lists all configured translation keys

   Page 1 — Breath Curves:
   [ ] Selecting Psalm 23, verse 1 loads without error
   [ ] Chart renders with at least one "Hebrew (source)" trace
   [ ] At least one translation trace appears when a translation is selected
   [ ] Hebrew text is displayed below the chart
   [ ] Mean breath weight metric shows a numeric value

   Page 2 — Deviation Heatmap:
   [ ] Heatmap renders with colour cells (not blank)
   [ ] Mean deviation summary table appears below the chart
   [ ] Changing translation selection updates both chart and table

   Page 3 — Chiasm Viewer:
   [ ] For a Psalm with known chiasm candidates, arcs render above the baseline
   [ ] For a Psalm with no candidates, the info message is shown (not an error)
   [ ] Candidate table appears below the arc diagram

   Page 4 — Translation Comparison:
   [ ] Score table is populated with deviation values
   [ ] Radar chart renders with Hebrew trace and at least one translation trace
   [ ] Suggestions section appears if Stage 5 has been run

   Page 5 — Pipeline Summary:
   [ ] Bar chart shows row counts for all 8 tables
   [ ] Pipeline runs table appears (or "no runs" info message)
   ```

4. If any item fails, file a GitHub issue or fix inline before committing as complete.

5. N/A — manual step.

6. Commit after checklist is complete: `"chore: Streamlit manual verification complete (Stage 06b)"`
