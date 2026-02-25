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


# ── Page rendering — added in Task 3 ─────────────────────────────────────────
# (placeholder — file is complete after Task 3)
