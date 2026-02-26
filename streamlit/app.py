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
import plotly.graph_objects as go  # noqa: F401 (used in page renderers)
import psycopg2
import psycopg2.extras

import streamlit as st

# Allow the container's /pipeline mount to shadow the local path; conftest.py
# handles the test path separately via sys.path.insert for the pipeline dir.
sys.path.insert(0, "/pipeline")

from visualize.arcs import chiasm_arc_figure  # noqa: E402
from visualize.breath_curves import breath_curve_figure  # noqa: E402
from visualize.heatmaps import deviation_heatmap  # noqa: E402
from visualize.radar import fingerprint_radar  # noqa: E402
from visualize.report import pipeline_summary_chart  # noqa: E402

st.set_page_config(
    page_title="Psalms NLP Explorer",
    layout="wide",
)


# ── Database connection ───────────────────────────────────────────────────────


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


# ── Query helpers (pure functions, testable without Streamlit) ────────────────


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
        [float(pivot.loc[ch, key]) for key in keys] for ch in chapters
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


def build_translation_fingerprints(
    heb_fp: dict[str, float],
    scores: dict[str, dict],
    translation_keys: list[str],
) -> list[dict[str, float]]:
    """Reconstruct approximate English fingerprints from stored deviation scores.

    Each dimension is estimated as ``max(0, heb_value - |heb - eng|)``.  Because
    the DB stores unsigned deviations (``|heb − eng|``) rather than the raw
    English values, this gives the lower-bound reconstruction; the result is
    sufficient to show meaningful differences between translations on the radar.

    Args:
        heb_fp: Hebrew fingerprint with keys syllable_density, morpheme_ratio,
            sonority_score, clause_compression.
        scores: Per-translation score dicts keyed by translation_key, each
            containing density_deviation, morpheme_deviation, sonority_deviation,
            compression_deviation.
        translation_keys: Ordered list of translation keys to include.

    Returns:
        List of fingerprint dicts, one per translation key, in the same order.
    """
    fps: list[dict[str, float]] = []
    for key in translation_keys:
        s = scores.get(key, {})
        fps.append(
            {
                "syllable_density": max(
                    0.0,
                    heb_fp.get("syllable_density", 0.0)
                    - s.get("density_deviation", 0.0),
                ),
                "morpheme_ratio": max(
                    0.0,
                    heb_fp.get("morpheme_ratio", 0.0)
                    - s.get("morpheme_deviation", 0.0),
                ),
                "sonority_score": max(
                    0.0,
                    heb_fp.get("sonority_score", 0.0)
                    - s.get("sonority_deviation", 0.0),
                ),
                "clause_compression": max(
                    0.0,
                    heb_fp.get("clause_compression", 0.0)
                    - s.get("compression_deviation", 0.0),
                ),
            }
        )
    return fps


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


# ── Sidebar + page dispatch (only when a Streamlit runtime is active) ─────────
# Guard prevents psycopg2.connect() from firing when the module is imported
# in unit tests or via static analysis tools.

if st.runtime.exists():  # type: ignore[attr-defined]
    # ── Sidebar ───────────────────────────────────────────────────────────────

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
        sql = "SELECT DISTINCT chapter FROM verses WHERE book_num = 19 ORDER BY chapter"
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [r["chapter"] for r in cur.fetchall()]

    @st.cache_data(ttl=300)
    def _cached_translation_keys() -> list[str]:
        sql = (
            "SELECT DISTINCT translation_key FROM translations ORDER BY translation_key"
        )
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [r["translation_key"] for r in cur.fetchall()]

    chapter_options = _cached_chapters()
    default_chapter_idx = chapter_options.index(23) if 23 in chapter_options else 0
    _ch_sel = st.sidebar.selectbox(
        "Psalm (chapter)", chapter_options, index=default_chapter_idx
    )
    selected_chapter: int = _ch_sel or (
        chapter_options[default_chapter_idx] if chapter_options else 1
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
    _v_sel = st.sidebar.selectbox("Verse", verse_options)
    selected_verse: int = _v_sel or (verse_options[0] if verse_options else 1)

    all_keys = _cached_translation_keys()
    selected_translations: list[str] = st.sidebar.multiselect(
        "Translations",
        all_keys,
        default=all_keys[:3] if len(all_keys) >= 3 else all_keys,
    )

    # ── Page 1: Breath Curves ─────────────────────────────────────────────────

    def render_breath_curves_page() -> None:
        """Render Page 1 — Breath Curve Overlay."""
        import re

        st.header(f"Breath Curve Overlay — Psalm {selected_chapter}:{selected_verse}")
        row = fetch_breath_profile(conn, selected_chapter, selected_verse)
        if not row:
            st.error("No data for this verse. Run the pipeline first.")
            st.stop()

        heb_curve: list[float] = row.get("breath_curve") or []
        texts = fetch_translation_texts(conn, row["verse_id"], selected_translations)

        def _simple_weights(text: str) -> list[float]:
            """Approximate per-word breath weight from syllable count heuristic."""
            words = re.findall(r"[A-Za-z']+", text)
            if not words:
                return []
            return [max(0.1, min(1.0, len(w) / 8.0)) for w in words]

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

    # ── Page 2: Deviation Heatmap ─────────────────────────────────────────────

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
            .sort_values("mean")  # type: ignore[call-overload]
        )
        st.dataframe(summary, use_container_width=True)

    # ── Page 3: Chiasm Viewer ─────────────────────────────────────────────────

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
            c
            for c in [
                "verse_id_start",
                "verse_id_end",
                "pattern_type",
                "confidence",
                "is_reviewed",
            ]
            if c in df_c.columns
        ]
        st.dataframe(df_c[display_cols], use_container_width=True)

    # ── Page 4: Translation Comparison ───────────────────────────────────────

    def render_translation_comparison_page() -> None:
        """Render Page 4 — Translation Comparison with radar chart."""
        st.header(f"Translation Comparison — Psalm {selected_chapter}:{selected_verse}")
        row = fetch_breath_profile(conn, selected_chapter, selected_verse)
        if not row:
            st.error("No verse data.")
            return

        scores = fetch_translation_scores_for_verse(
            conn, row["verse_id"], selected_translations
        )
        texts = fetch_translation_texts(conn, row["verse_id"], selected_translations)

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

        heb_sql = (
            "SELECT syllable_density, morpheme_ratio, sonority_score, "
            "clause_compression FROM verse_fingerprints WHERE verse_id = %s"
        )
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(heb_sql, (row["verse_id"],))
            heb_row = cur.fetchone()
        # Cast Decimal values from PostgreSQL NUMERIC columns to float
        heb_fp = {k: float(v) for k, v in dict(heb_row).items()} if heb_row else {}

        if heb_fp and selected_translations:
            trans_fps = build_translation_fingerprints(
                heb_fp, scores, selected_translations
            )
            radar_fig = fingerprint_radar(
                labels=selected_translations,
                fingerprints=trans_fps,
                hebrew_fingerprint=heb_fp,
                title=f"Psalm {selected_chapter}:{selected_verse} — Fingerprint",
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

    # ── Page 5: Pipeline Summary ──────────────────────────────────────────────

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

    # ── Page dispatch ─────────────────────────────────────────────────────────

    _PAGES: dict[str, object] = {
        "Breath Curves": render_breath_curves_page,
        "Deviation Heatmap": render_deviation_heatmap_page,
        "Chiasm Viewer": render_chiasm_viewer_page,
        "Translation Comparison": render_translation_comparison_page,
        "Pipeline Summary": render_pipeline_summary_page,
    }

    _render = _PAGES.get(page or "")
    if callable(_render):
        _render()  # type: ignore[operator]
