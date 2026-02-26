# pipeline/modules/fingerprint.py
"""
Stage 2 — Style fingerprinting.

Computes the 4-dimensional style fingerprint for each verse:
  syllable_density    = mean syllables per word
  morpheme_ratio      = mean morphemes per word
  sonority_score      = mean consonant sonority of word onsets (0–1)
  clause_compression  = mean words per clause boundary
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict

import psycopg2
import psycopg2.extras
from adapters.db_adapter import batch_upsert, verse_ids_for_stage

logger = logging.getLogger(__name__)

# Hebrew consonant set (includes final forms)
CONSONANTS = frozenset("אבגדהוזחטיכלמנסעפצקרשת" + "ךםןףץ")

# Full vowel point Unicode characters (each = 1 syllable nucleus)
FULL_VOWEL_POINTS = frozenset(
    "\u05b4\u05b5\u05b6\u05b7\u05b8\u05b9\u05ba\u05bb\u05c1\u05c2"
)

# Half/ultra-short vowel points (shewa + hataf forms)
HALF_VOWEL_POINTS = frozenset("\u05b0\u05b1\u05b2\u05b3")

# Onset consonant sonority weights (0.0 = least sonorous, 1.0 = most)
SONORITY: dict[str, float] = {
    # Gutturals
    "א": 0.60,
    "ה": 0.65,
    "ח": 0.45,
    "ע": 0.55,
    # Liquids
    "ל": 0.90,
    "ר": 0.85,
    # Nasals
    "מ": 0.80,
    "נ": 0.80,
    # Sibilants
    "ש": 0.40,
    "ס": 0.40,
    "ז": 0.45,
    "צ": 0.35,
    # Approximants
    "י": 0.75,
    "ו": 0.70,
    # Voiced stops
    "ב": 0.30,
    "ג": 0.30,
    "ד": 0.30,
    # Voiceless stops
    "כ": 0.20,
    "ך": 0.20,
    "פ": 0.20,
    "ף": 0.20,
    "ת": 0.20,
    "ק": 0.15,
    "ט": 0.20,
}
DEFAULT_SONORITY = 0.35


def run(
    conn: psycopg2.extensions.connection,
    config: dict,
) -> dict:
    """Compute 4D style fingerprints for all pending verses.

    Args:
        conn: Live psycopg2 connection.
        config: Full parsed config.yml.

    Returns:
        Dict with "rows_written", "elapsed_s", "fingerprints_computed".
    """
    t0 = time.perf_counter()
    corpus = config.get("corpus", {})
    book_nums: list[int] = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters: list[int] = corpus.get("debug_chapters", [])
    batch_size: int = config.get("fingerprint", {}).get("batch_size", 100)

    pending = verse_ids_for_stage(conn, "verse_fingerprints", book_nums, debug_chapters)
    if not pending:
        logger.info("All verse fingerprints already computed.")
        return {
            "rows_written": 0,
            "elapsed_s": round(time.perf_counter() - t0, 2),
            "fingerprints_computed": 0,
        }

    logger.info("Computing fingerprints for %d verses", len(pending))

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT verse_id, position, surface_form,
                   morpheme_count, part_of_speech
            FROM word_tokens
            WHERE verse_id = ANY(%s)
            ORDER BY verse_id, position
            """,
            (pending,),
        )
        rows = cur.fetchall()

    by_verse: dict[int, list] = defaultdict(list)
    for row in rows:
        by_verse[row[0]].append(row)

    fingerprint_rows: list[tuple] = []
    for verse_id in pending:
        tokens = by_verse.get(verse_id, [])
        if not tokens:
            continue
        fp = _compute_fingerprint(tokens)
        fingerprint_rows.append(
            (
                verse_id,
                fp["syllable_density"],
                fp["morpheme_ratio"],
                fp["sonority_score"],
                fp["clause_compression"],
            )
        )

    batch_upsert(
        conn,
        """
        INSERT INTO verse_fingerprints
          (verse_id, syllable_density, morpheme_ratio,
           sonority_score, clause_compression)
        VALUES %s
        ON CONFLICT (verse_id) DO UPDATE
          SET syllable_density   = EXCLUDED.syllable_density,
              morpheme_ratio     = EXCLUDED.morpheme_ratio,
              sonority_score     = EXCLUDED.sonority_score,
              clause_compression = EXCLUDED.clause_compression,
              computed_at        = NOW()
        """,
        fingerprint_rows,
        batch_size=batch_size,
    )

    # Update word_count on verses table
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE verses v
            SET word_count = sub.cnt
            FROM (
                SELECT verse_id, COUNT(*) AS cnt
                FROM word_tokens
                WHERE verse_id = ANY(%s)
                GROUP BY verse_id
            ) sub
            WHERE v.verse_id = sub.verse_id
            """,
            (pending,),
        )
    conn.commit()

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info(
        "Fingerprints computed: %d in %.1fs",
        len(fingerprint_rows),
        elapsed,
    )
    return {
        "rows_written": len(fingerprint_rows),
        "elapsed_s": elapsed,
        "fingerprints_computed": len(fingerprint_rows),
    }


def _compute_fingerprint(tokens: list) -> dict[str, float]:
    """Compute 4D fingerprint from a list of word token rows.

    Args:
        tokens: List of tuples
            (verse_id, position, surface_form, morpheme_count, part_of_speech).

    Returns:
        Dict with keys: syllable_density, morpheme_ratio,
        sonority_score, clause_compression.
    """
    surface_forms = [r[2] for r in tokens]
    morpheme_counts = [r[3] or 1 for r in tokens]
    pos_tags = [r[4] or "" for r in tokens]
    n = len(tokens)

    syllable_counts = [count_hebrew_syllables(f) for f in surface_forms]
    syllable_density = sum(syllable_counts) / n if n > 0 else 0.0

    morpheme_ratio = sum(morpheme_counts) / n if n > 0 else 0.0

    sonority_values = [_onset_sonority(f) for f in surface_forms]
    sonority_score = (
        sum(sonority_values) / len(sonority_values) if sonority_values else 0.0
    )

    conjunction_count = sum(1 for p in pos_tags if p == "conjunction")
    clause_compression = n / (conjunction_count + 1)

    return {
        "syllable_density": round(syllable_density, 4),
        "morpheme_ratio": round(morpheme_ratio, 4),
        "sonority_score": round(sonority_score, 4),
        "clause_compression": round(clause_compression, 4),
    }


def count_hebrew_syllables(word: str) -> int:
    """Count syllables in a Hebrew word with niqqud.

    A syllable nucleus is a full vowel point. Shewa and hataf forms
    (half-vowels) count as 1 when they are the only vowel in a word
    (monosyllabic function words). Unvocalized words are assumed
    monosyllabic.

    Args:
        word: Hebrew word string, possibly with niqqud (combining chars).

    Returns:
        Integer syllable count, minimum 1.
    """
    full = sum(1 for c in word if c in FULL_VOWEL_POINTS)
    half = sum(1 for c in word if c in HALF_VOWEL_POINTS)
    if full == 0 and half > 0:
        return 1
    if full == 0 and half == 0:
        return 1
    return full


def _onset_sonority(word: str) -> float:
    """Return the sonority score of the onset consonant of a word.

    Args:
        word: Hebrew word string (surface form with or without niqqud).

    Returns:
        Float sonority score in [0.0, 1.0].
    """
    for ch in word:
        if ch in CONSONANTS:
            return SONORITY.get(ch, DEFAULT_SONORITY)
    return DEFAULT_SONORITY
