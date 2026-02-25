# pipeline/modules/score.py
"""
Stage 4 — Translation scoring.

For each verse × translation pair:
  1. Compute English style fingerprint via phoneme_adapter
  2. Compute weighted absolute deviation from the Hebrew fingerprint
  3. Compute breath alignment against the Hebrew breath curve
  4. Batch-upsert results into translation_scores
"""

from __future__ import annotations

import logging
import time

import numpy as np
import psycopg2
import psycopg2.extras
from adapters.db_adapter import batch_upsert
from adapters.phoneme_adapter import english_breath_weights, english_fingerprint

logger = logging.getLogger(__name__)


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    """Score all verse × translation pairs and persist results.

    Args:
        conn: Live psycopg2 connection.
        config: Full parsed config.yml.

    Returns:
        Dict with "scored" (int) and "elapsed_s" (float).
    """
    t0 = time.monotonic()

    corpus = config.get("corpus", {})
    book_nums: list[int] = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters: list[int] = corpus.get("debug_chapters", [])
    batch_size: int = config.get("scoring", {}).get("batch_size", 100)

    w = config.get("scoring", {}).get("deviation_weights", {})
    dev_weights = np.array([
        w.get("density", 0.35),
        w.get("morpheme", 0.25),
        w.get("sonority", 0.20),
        w.get("compression", 0.20),
    ])

    bw = config.get("scoring", {}).get("breath_alignment_weights", {})
    stress_weight: float = bw.get("stress", 0.60)
    wm_weight: float = bw.get("weight", 0.40)

    sources = config.get("translations", {}).get("sources", [])
    translation_keys: list[str] = [s["id"] for s in sources]

    heb_fps = _load_hebrew_fingerprints(conn, book_nums, debug_chapters)
    heb_breath = _load_hebrew_breath(conn, book_nums, debug_chapters)
    trans_texts = _load_translation_texts(
        conn, translation_keys, book_nums, debug_chapters
    )

    if not heb_fps:
        logger.error("No Hebrew fingerprints found — run Stage 2 first.")
        return {"scored": 0, "elapsed_s": 0.0}

    logger.info(
        "Scoring %d verses × %d translations = %d pairs",
        len(heb_fps),
        len(translation_keys),
        len(heb_fps) * len(translation_keys),
    )

    score_rows: list[tuple] = []
    for verse_id, heb_fp in heb_fps.items():
        heb_vec = np.array([
            heb_fp["syllable_density"],
            heb_fp["morpheme_ratio"],
            heb_fp["sonority_score"],
            heb_fp["clause_compression"],
        ])
        heb_curve = heb_breath.get(verse_id, {})

        for key in translation_keys:
            text = trans_texts.get((verse_id, key))
            if not text:
                continue

            eng_fp = english_fingerprint(text)
            eng_vec = np.array([
                eng_fp["syllable_density"],
                eng_fp["morpheme_ratio"],
                eng_fp["sonority_score"],
                eng_fp["clause_compression"],
            ])

            diffs = np.abs(heb_vec - eng_vec)
            density_dev = float(diffs[0])
            morpheme_dev = float(diffs[1])
            sonority_dev = float(diffs[2])
            compression_dev = float(diffs[3])
            composite_dev = float(np.dot(dev_weights, diffs))

            stress_align, weight_match = _compute_breath_alignment(
                heb_curve, text
            )
            breath_align = round(
                stress_align * stress_weight + weight_match * wm_weight, 4
            )

            score_rows.append((
                verse_id,
                key,
                round(density_dev, 4),
                round(morpheme_dev, 4),
                round(sonority_dev, 4),
                round(compression_dev, 4),
                round(composite_dev, 4),
                round(stress_align, 4),
                round(weight_match, 4),
                round(breath_align, 4),
            ))

    batch_upsert(
        conn,
        """
        INSERT INTO translation_scores
          (verse_id, translation_key,
           density_deviation, morpheme_deviation, sonority_deviation,
           compression_deviation, composite_deviation,
           stress_alignment, weight_match, breath_alignment)
        VALUES %s
        ON CONFLICT (verse_id, translation_key) DO UPDATE
          SET density_deviation     = EXCLUDED.density_deviation,
              morpheme_deviation    = EXCLUDED.morpheme_deviation,
              sonority_deviation    = EXCLUDED.sonority_deviation,
              compression_deviation = EXCLUDED.compression_deviation,
              composite_deviation   = EXCLUDED.composite_deviation,
              stress_alignment      = EXCLUDED.stress_alignment,
              weight_match          = EXCLUDED.weight_match,
              breath_alignment      = EXCLUDED.breath_alignment,
              scored_at             = NOW()
        """,
        score_rows,
        batch_size=batch_size,
    )

    elapsed = round(time.monotonic() - t0, 2)
    logger.info(
        "Scored %d verse × translation pairs in %.2fs", len(score_rows), elapsed
    )
    return {"scored": len(score_rows), "elapsed_s": elapsed}


def _load_hebrew_fingerprints(
    conn: psycopg2.extensions.connection,
    book_nums: list[int],
    debug_chapters: list[int],
) -> dict[int, dict[str, float]]:
    """Load verse_fingerprints rows for the configured corpus.

    Args:
        conn: Live psycopg2 connection.
        book_nums: Book numbers in scope.
        debug_chapters: Optional chapter filter.

    Returns:
        Dict mapping verse_id → fingerprint dimension dict.
    """
    q = """
        SELECT vf.verse_id, vf.syllable_density, vf.morpheme_ratio,
               vf.sonority_score, vf.clause_compression
        FROM verse_fingerprints vf
        JOIN verses v ON vf.verse_id = v.verse_id
        WHERE v.book_num = ANY(%s)
    """
    params: list = [book_nums]
    if debug_chapters:
        q += " AND v.chapter = ANY(%s)"
        params.append(debug_chapters)
    with conn.cursor() as cur:
        cur.execute(q, params)
        return {
            row[0]: {
                "syllable_density": float(row[1] or 0),
                "morpheme_ratio":   float(row[2] or 0),
                "sonority_score":   float(row[3] or 0),
                "clause_compression": float(row[4] or 0),
            }
            for row in cur.fetchall()
        }


def _load_hebrew_breath(
    conn: psycopg2.extensions.connection,
    book_nums: list[int],
    debug_chapters: list[int],
) -> dict[int, dict]:
    """Load breath_profiles rows for the configured corpus.

    Args:
        conn: Live psycopg2 connection.
        book_nums: Book numbers in scope.
        debug_chapters: Optional chapter filter.

    Returns:
        Dict mapping verse_id → breath profile dict.
    """
    q = """
        SELECT bp.verse_id, bp.breath_curve, bp.stress_positions, bp.mean_weight
        FROM breath_profiles bp
        JOIN verses v ON bp.verse_id = v.verse_id
        WHERE v.book_num = ANY(%s)
    """
    params: list = [book_nums]
    if debug_chapters:
        q += " AND v.chapter = ANY(%s)"
        params.append(debug_chapters)
    with conn.cursor() as cur:
        cur.execute(q, params)
        return {
            row[0]: {
                # Cast Decimal elements to float (psycopg2 returns NUMERIC[] as Decimal[])
                "breath_curve":     [float(x) for x in (row[1] or [])],
                "stress_positions": [float(x) for x in (row[2] or [])],
                "mean_weight":      float(row[3] or 0),
            }
            for row in cur.fetchall()
        }


def _load_translation_texts(
    conn: psycopg2.extensions.connection,
    translation_keys: list[str],
    book_nums: list[int],
    debug_chapters: list[int],
) -> dict[tuple[int, str], str]:
    """Load translations rows for the configured corpus and translation keys.

    Args:
        conn: Live psycopg2 connection.
        translation_keys: Translation IDs to load (e.g. ["KJV", "YLT"]).
        book_nums: Book numbers in scope.
        debug_chapters: Optional chapter filter.

    Returns:
        Dict mapping (verse_id, translation_key) → verse text.
    """
    q = """
        SELECT t.verse_id, t.translation_key, t.verse_text
        FROM translations t
        JOIN verses v ON t.verse_id = v.verse_id
        WHERE v.book_num = ANY(%s)
          AND t.translation_key = ANY(%s)
    """
    params: list = [book_nums, translation_keys]
    if debug_chapters:
        q += " AND v.chapter = ANY(%s)"
        params.append(debug_chapters)
    with conn.cursor() as cur:
        cur.execute(q, params)
        return {(row[0], row[1]): row[2] for row in cur.fetchall()}


def _compute_breath_alignment(
    heb_data: dict,
    english_text: str,
) -> tuple[float, float]:
    """Compute (stress_alignment, weight_match) for a verse × translation pair.

    stress_alignment: 1 minus mean minimum distance between Hebrew and English
    stressed syllable positions (both normalized 0–1).

    weight_match: similarity between mean Hebrew breath weight and mean English
    syllable weight, scaled so a difference of 0.5 maps to 0.0.

    Args:
        heb_data: Dict with "stress_positions" (list[float]) and "mean_weight".
        english_text: English translation verse text.

    Returns:
        Tuple of (stress_alignment, weight_match), both floats in [0.0, 1.0].
    """
    heb_stress: list[float] = heb_data.get("stress_positions", [])
    heb_mean_weight: float = heb_data.get("mean_weight", 0.5)

    eng_weights = english_breath_weights(english_text)
    n_eng = len(eng_weights)

    if not eng_weights:
        return 0.5, 0.5

    # Stress alignment
    if not heb_stress or n_eng == 0:
        stress_align: float = 0.5
    else:
        eng_positions = [i / max(n_eng - 1, 1) for i in range(n_eng)]
        mean_eng = sum(eng_weights) / len(eng_weights)
        eng_stress = [
            eng_positions[i]
            for i, weight in enumerate(eng_weights)
            if weight > mean_eng * 1.1
        ]

        if not eng_stress:
            stress_align = 0.5
        else:
            distances = [
                min(abs(h_pos - e_pos) for e_pos in eng_stress)
                for h_pos in heb_stress
            ]
            stress_align = max(0.0, 1.0 - sum(distances) / len(distances))

    # Weight match: penalise mean-weight difference
    mean_eng_weight = sum(eng_weights) / len(eng_weights)
    weight_diff = abs(heb_mean_weight - mean_eng_weight)
    weight_match = max(0.0, 1.0 - weight_diff * 2)

    return round(stress_align, 4), round(weight_match, 4)
