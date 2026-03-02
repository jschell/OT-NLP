# pipeline/modules/genre_baseline.py
"""Stage 8 — Genre-level aggregate fingerprint baselines.

Aggregates verse_fingerprints grouped by the genre assigned to each book,
producing one summary row per genre in the genre_baselines table.  These
baselines enable cross-corpus comparison (e.g. hebrew_poetry vs
hebrew_prophecy) and serve as reference anchors for future expansion books.

Entry point follows the standard pipeline module interface:
    run(conn, config) -> dict
"""

from __future__ import annotations

import logging
import time

import psycopg2

logger = logging.getLogger(__name__)

# Query: aggregate fingerprint dimensions by books.genre
_AGGREGATE_SQL = """
    SELECT
        b.genre,
        COUNT(vf.verse_id)                AS verse_count,
        AVG(vf.syllable_density)          AS syllable_density_mean,
        STDDEV(vf.syllable_density)       AS syllable_density_stddev,
        AVG(vf.morpheme_ratio)            AS morpheme_ratio_mean,
        STDDEV(vf.morpheme_ratio)         AS morpheme_ratio_stddev,
        AVG(vf.sonority_score)            AS sonority_mean,
        STDDEV(vf.sonority_score)         AS sonority_stddev,
        AVG(vf.clause_compression)        AS clause_compression_mean,
        STDDEV(vf.clause_compression)     AS clause_compression_stddev
    FROM verse_fingerprints vf
    JOIN verses  v ON vf.verse_id = v.verse_id
    JOIN books   b ON v.book_num  = b.book_num
    WHERE b.genre IS NOT NULL
    GROUP BY b.genre
    ORDER BY b.genre
"""

_UPSERT_SQL = """
    INSERT INTO genre_baselines (
        genre, verse_count,
        syllable_density_mean,   syllable_density_stddev,
        morpheme_ratio_mean,     morpheme_ratio_stddev,
        sonority_mean,           sonority_stddev,
        clause_compression_mean, clause_compression_stddev
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (genre) DO UPDATE SET
        verse_count               = EXCLUDED.verse_count,
        syllable_density_mean     = EXCLUDED.syllable_density_mean,
        syllable_density_stddev   = EXCLUDED.syllable_density_stddev,
        morpheme_ratio_mean       = EXCLUDED.morpheme_ratio_mean,
        morpheme_ratio_stddev     = EXCLUDED.morpheme_ratio_stddev,
        sonority_mean             = EXCLUDED.sonority_mean,
        sonority_stddev           = EXCLUDED.sonority_stddev,
        clause_compression_mean   = EXCLUDED.clause_compression_mean,
        clause_compression_stddev = EXCLUDED.clause_compression_stddev,
        computed_at               = NOW()
"""


def run(
    conn: psycopg2.extensions.connection,
    config: dict,  # noqa: ARG001 — kept for interface consistency
) -> dict:
    """Compute per-genre aggregate fingerprints and persist to genre_baselines.

    Args:
        conn: Live psycopg2 connection.
        config: Full parsed config.yml (not used; retained for interface
            consistency with all other pipeline stages).

    Returns:
        Dict with "rows_written" (number of genres upserted) and "elapsed_s".
    """
    t0 = time.monotonic()

    rows = _aggregate_by_genre(conn)

    if not rows:
        logger.info(
            "No genre data found — ensure books.genre is populated "
            "and verse_fingerprints exist"
        )
        return {"rows_written": 0, "elapsed_s": round(time.monotonic() - t0, 2)}

    _upsert_baselines(conn, rows)

    elapsed = round(time.monotonic() - t0, 2)
    logger.info(
        "Upserted %d genre_baselines rows in %.2fs",
        len(rows),
        elapsed,
    )
    return {"rows_written": len(rows), "elapsed_s": elapsed}


def _aggregate_by_genre(
    conn: psycopg2.extensions.connection,
) -> list[dict]:
    """Run the aggregation query and return one dict per genre.

    Args:
        conn: Live psycopg2 connection.

    Returns:
        List of dicts keyed by column name, one entry per genre.
        Empty list when no fingerprints with a non-null genre exist.
    """
    with conn.cursor() as cur:
        cur.execute(_AGGREGATE_SQL)
        raw = cur.fetchall()

    return [
        {
            "genre": row[0],
            "verse_count": int(row[1]),
            "syllable_density_mean": float(row[2] or 0),
            "syllable_density_stddev": float(row[3] or 0),
            "morpheme_ratio_mean": float(row[4] or 0),
            "morpheme_ratio_stddev": float(row[5] or 0),
            "sonority_mean": float(row[6] or 0),
            "sonority_stddev": float(row[7] or 0),
            "clause_compression_mean": float(row[8] or 0),
            "clause_compression_stddev": float(row[9] or 0),
        }
        for row in raw
    ]


def _upsert_baselines(
    conn: psycopg2.extensions.connection,
    rows: list[dict],
) -> None:
    """Upsert a list of genre aggregate dicts into genre_baselines.

    Args:
        conn: Live psycopg2 connection.
        rows: Output of :func:`_aggregate_by_genre`.
    """
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                _UPSERT_SQL,
                (
                    row["genre"],
                    row["verse_count"],
                    row["syllable_density_mean"],
                    row["syllable_density_stddev"],
                    row["morpheme_ratio_mean"],
                    row["morpheme_ratio_stddev"],
                    row["sonority_mean"],
                    row["sonority_stddev"],
                    row["clause_compression_mean"],
                    row["clause_compression_stddev"],
                ),
            )
    conn.commit()
