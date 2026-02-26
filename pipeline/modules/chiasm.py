# pipeline/modules/chiasm.py
"""
Stage 2 (second pass) — Chiasm detection.

Detects ABBA and ABCBA chiastic patterns across colon sequences.
Requires colon_fingerprints in verse_fingerprints, which is populated
by Stage 3 (breath.py back-population step).

DEFERRED: Do not call run() until Stage 3 completes.

All output is stored as candidates with confidence scores. These are
observations for interpretive review, not asserted findings.
"""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict

import numpy as np
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def run(
    conn: psycopg2.extensions.connection,
    config: dict,
) -> dict:
    """Detect ABBA and ABCBA chiastic patterns and store candidates.

    NOTE: This function requires verse_fingerprints.colon_fingerprints
    to be populated. Run Stage 3 (breath.py) before calling this.

    Args:
        conn: Live psycopg2 connection.
        config: Full parsed config.yml.

    Returns:
        Dict with "rows_written", "elapsed_s", "candidates_found".
    """
    t0 = time.perf_counter()
    chiasm_config = config.get("chiasm", {})
    similarity_threshold: float = chiasm_config.get("similarity_threshold", 0.75)
    min_confidence: float = chiasm_config.get("min_confidence", 0.65)
    max_stanza_verses: int = chiasm_config.get("max_stanza_verses", 8)

    corpus = config.get("corpus", {})
    book_nums: list[int] = [b["book_num"] for b in corpus.get("books", [])]

    colon_fps = _load_colon_fingerprints(conn, book_nums)
    if not colon_fps:
        logger.warning("No colon fingerprints found. Run Stage 3 first.")
        return {
            "rows_written": 0,
            "elapsed_s": round(time.perf_counter() - t0, 2),
            "candidates_found": 0,
        }

    chapters = _load_chapter_groupings(conn, book_nums)
    candidates: list[dict] = []

    for (_book_num, _chapter), verse_ids in chapters.items():
        if len(verse_ids) > max_stanza_verses:
            verse_ids = verse_ids[:max_stanza_verses]

        chapter_colons: list[tuple[int, int, np.ndarray]] = []
        for v_id in verse_ids:
            for colon_idx, vec in colon_fps.get(v_id, []):
                chapter_colons.append((v_id, colon_idx, vec))

        if len(chapter_colons) < 4:
            continue

        found = _detect_patterns(chapter_colons, similarity_threshold, min_confidence)
        candidates.extend(found)

    _store_candidates(conn, candidates)
    elapsed = round(time.perf_counter() - t0, 2)
    logger.info(
        "Chiasm detection complete: %d candidates in %.1fs",
        len(candidates),
        elapsed,
    )
    return {
        "rows_written": len(candidates),
        "elapsed_s": elapsed,
        "candidates_found": len(candidates),
    }


def _load_colon_fingerprints(
    conn: psycopg2.extensions.connection,
    book_nums: list[int],
) -> dict[int, list[tuple[int, np.ndarray]]]:
    """Load colon-level fingerprint vectors from verse_fingerprints.

    Args:
        conn: Live psycopg2 connection.
        book_nums: Books to load.

    Returns:
        Dict mapping verse_id → list of (colon_index, 4D vector).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT vf.verse_id, vf.colon_fingerprints
            FROM verse_fingerprints vf
            JOIN verses v ON vf.verse_id = v.verse_id
            WHERE v.book_num = ANY(%s)
              AND vf.colon_fingerprints IS NOT NULL
            """,
            (book_nums,),
        )
        rows = cur.fetchall()

    result: dict[int, list[tuple[int, np.ndarray]]] = {}
    for verse_id, colon_fps_json in rows:
        if not colon_fps_json:
            continue
        colon_data = (
            colon_fps_json
            if isinstance(colon_fps_json, list)
            else json.loads(colon_fps_json)
        )
        colon_vecs: list[tuple[int, np.ndarray]] = []
        for item in colon_data:
            vec = np.array(
                [
                    item.get("density", 0.0),
                    item.get("morpheme_ratio", 0.0),
                    item.get("sonority", 0.0),
                    item.get("mean_weight", 0.0),
                ],
                dtype=float,
            )
            colon_vecs.append((item["colon"], vec))
        result[verse_id] = colon_vecs

    return result


def _load_chapter_groupings(
    conn: psycopg2.extensions.connection,
    book_nums: list[int],
) -> dict[tuple[int, int], list[int]]:
    """Return verse_ids grouped by (book_num, chapter) in verse order.

    Args:
        conn: Live psycopg2 connection.
        book_nums: Books to include.

    Returns:
        Dict mapping (book_num, chapter) → sorted list of verse_ids.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT book_num, chapter, verse_id
            FROM verses
            WHERE book_num = ANY(%s)
            ORDER BY book_num, chapter, verse_num
            """,
            (book_nums,),
        )
        chapters: dict[tuple[int, int], list[int]] = defaultdict(list)
        for book_num, chapter, verse_id in cur.fetchall():
            chapters[(book_num, chapter)].append(verse_id)
    return dict(chapters)


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Compute cosine similarity between two vectors.

    Args:
        a: First vector.
        b: Second vector.

    Returns:
        Float in [0.0, 1.0], or 0.0 if either vector is zero-length.
    """
    norm_a = float(np.linalg.norm(a))
    norm_b = float(np.linalg.norm(b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def _detect_patterns(
    colons: list[tuple[int, int, np.ndarray]],
    threshold: float,
    min_confidence: float,
) -> list[dict]:
    """Scan a colon sequence for ABBA and ABCBA chiastic patterns.

    ABBA: 4-element window i..i+3 where outer colons match and inner
    colons match.

    ABCBA: 5-element window i..i+4 where outer colons match, inner
    colons match, and colon i+2 is the unconstrained pivot.

    Args:
        colons: List of (verse_id, colon_index, 4D vector) tuples.
        threshold: Minimum cosine similarity to qualify a match pair.
        min_confidence: Minimum mean similarity to store a candidate.

    Returns:
        List of candidate dicts ready for _store_candidates().
    """
    candidates: list[dict] = []
    n = len(colons)

    for i in range(n - 3):
        # ABBA window: i, i+1, i+2, i+3
        if i + 3 < n:
            v_ids_abba = [colons[j][0] for j in range(i, i + 4)]
            outer_sim = _cosine_similarity(colons[i][2], colons[i + 3][2])
            inner_sim = _cosine_similarity(colons[i + 1][2], colons[i + 2][2])
            if outer_sim >= threshold and inner_sim >= threshold:
                confidence = (outer_sim + inner_sim) / 2
                if confidence >= min_confidence:
                    candidates.append(
                        {
                            "verse_id_start": min(v_ids_abba),
                            "verse_id_end": max(v_ids_abba),
                            "pattern_type": "ABBA",
                            "colon_matches": [
                                {
                                    "a": i,
                                    "b": i + 3,
                                    "similarity": round(outer_sim, 4),
                                },
                                {
                                    "a": i + 1,
                                    "b": i + 2,
                                    "similarity": round(inner_sim, 4),
                                },
                            ],
                            "confidence": round(confidence, 4),
                        }
                    )

        # ABCBA window: i, i+1, i+2 (pivot), i+3, i+4
        if i + 4 < n:
            v_ids_abcba = [colons[j][0] for j in range(i, i + 5)]
            outer_sim = _cosine_similarity(colons[i][2], colons[i + 4][2])
            inner_sim = _cosine_similarity(colons[i + 1][2], colons[i + 3][2])
            if outer_sim >= threshold and inner_sim >= threshold:
                confidence = (outer_sim + inner_sim) / 2
                if confidence >= min_confidence:
                    candidates.append(
                        {
                            "verse_id_start": min(v_ids_abcba),
                            "verse_id_end": max(v_ids_abcba),
                            "pattern_type": "ABCBA",
                            "colon_matches": [
                                {
                                    "a": i,
                                    "b": i + 4,
                                    "similarity": round(outer_sim, 4),
                                },
                                {
                                    "a": i + 1,
                                    "b": i + 3,
                                    "similarity": round(inner_sim, 4),
                                },
                                {"pivot": i + 2},
                            ],
                            "confidence": round(confidence, 4),
                        }
                    )

    return candidates


def _store_candidates(
    conn: psycopg2.extensions.connection,
    candidates: list[dict],
) -> None:
    """Insert chiasm candidates into chiasm_candidates table.

    Uses ON CONFLICT DO NOTHING for idempotency.

    Args:
        conn: Live psycopg2 connection.
        candidates: List of candidate dicts from _detect_patterns().
    """
    if not candidates:
        return
    rows = [
        (
            c["verse_id_start"],
            c["verse_id_end"],
            c["pattern_type"],
            json.dumps(c["colon_matches"]),
            c["confidence"],
        )
        for c in candidates
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO chiasm_candidates
              (verse_id_start, verse_id_end, pattern_type,
               colon_matches, confidence)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
    conn.commit()
