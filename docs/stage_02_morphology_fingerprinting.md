# Stage 2 — Core Pipeline: Morphology & Fingerprinting
## Detailed Implementation Plan

> **Depends on:** Stage 0 (schema), Stage 1 (BHSA downloaded, translation files present)  
> **Produces:** `verses` and `word_tokens` populated; 4D style fingerprints in `verse_fingerprints`; `chiasm_candidates` populated (second pass, after Stage 3)  
> **Estimated time:** 30–90 minutes (Psalms corpus)

---

## Objectives

1. Extract morphological data from BHSA via text-fabric into `verses` and `word_tokens`
2. Compute the 4-dimensional style fingerprint per verse into `verse_fingerprints`
3. Implement `modules/chiasm.py` for ABBA/ABCBA pattern detection (runs as second pass after Stage 3 supplies colon boundary data)
4. Implement `adapters/db_adapter.py` for resumable upsert operations

---

## Conceptual Overview

### The 4-Dimensional Style Fingerprint

Each Psalm verse is represented as a vector in four dimensions derived entirely from the Hebrew source text:

| Dimension | Definition | Range | What it captures |
|---|---|---|---|
| `syllable_density` | mean syllables per word | ~1.5–3.5 | Word length / phonetic weight |
| `morpheme_ratio` | mean morphemes per word | ~1.0–4.0 | Morphological complexity |
| `sonority_score` | mean onset consonant sonority | 0.0–1.0 | Acoustic openness |
| `clause_compression` | words per clause boundary | ~2.0–12.0 | Syntactic density |

When a translation is scored (Stage 4), the same four dimensions are computed from the English text and the Euclidean distance to the Hebrew vector is the deviation score.

### Chiasm Detection Architecture

Chiasm detection requires colon boundaries, which come from Stage 3. The module is defined here but executed as a second pass after Stage 3 completes:

```
Stage 2: populate verses, word_tokens, verse_fingerprints
Stage 3: populate syllable_tokens, breath_profiles (colon boundaries stored here)
Stage 2 (second pass): modules/chiasm.py reads colon data → chiasm_candidates
```

The `run.py` orchestrator manages this sequencing.

---

## File Structure for This Stage

```
pipeline/
  modules/
    ingest.py               ← BHSA → verses + word_tokens
    fingerprint.py          ← verses → verse_fingerprints
    chiasm.py               ← colon data → chiasm_candidates
  adapters/
    db_adapter.py           ← resumable upsert utilities
  tests/
    test_fingerprint.py
    test_chiasm.py
    test_db_adapter.py
```

---

## Step 1 — File: `adapters/db_adapter.py`

```python
"""
Database adapter utilities.

Provides resumable upsert operations used by all pipeline modules.
The core pattern: before processing, query which items already have
target table rows, then skip those. This makes every stage
idempotent and resumable after interruption.
"""

from __future__ import annotations

import logging
from typing import List, Set

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def get_processed_verse_ids(
    conn: psycopg2.extensions.connection,
    table: str,
    id_column: str = "verse_id",
) -> Set[int]:
    """
    Return the set of verse_ids that already have rows in `table`.
    Used to skip already-computed verses when resuming.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT {id_column} FROM {table}")
        return {row[0] for row in cur.fetchall()}


def get_all_verse_ids(
    conn: psycopg2.extensions.connection,
    book_nums: List[int],
    debug_chapters: List[int] = None,
) -> List[int]:
    """Return all verse_ids for the given books, optionally filtered by chapter."""
    with conn.cursor() as cur:
        if debug_chapters:
            cur.execute(
                """
                SELECT verse_id FROM verses
                WHERE book_num = ANY(%s) AND chapter = ANY(%s)
                ORDER BY verse_id
                """,
                (book_nums, debug_chapters)
            )
        else:
            cur.execute(
                "SELECT verse_id FROM verses WHERE book_num = ANY(%s) ORDER BY verse_id",
                (book_nums,)
            )
        return [row[0] for row in cur.fetchall()]


def verse_ids_for_stage(
    conn: psycopg2.extensions.connection,
    target_table: str,
    book_nums: List[int],
    debug_chapters: List[int] = None,
) -> List[int]:
    """
    Return verse_ids that need processing for a given stage.

    Subtracts already-processed from the full corpus set. Use this at
    the start of each module's run() to implement resumability.
    """
    all_ids = set(get_all_verse_ids(conn, book_nums, debug_chapters))
    done_ids = get_processed_verse_ids(conn, target_table)
    pending = sorted(all_ids - done_ids)
    logger.info(
        f"Stage targeting {target_table}: "
        f"{len(all_ids)} total, {len(done_ids)} done, {len(pending)} pending"
    )
    return pending


def batch_upsert(
    conn: psycopg2.extensions.connection,
    query: str,
    rows: List[tuple],
    batch_size: int = 100,
) -> int:
    """
    Execute a psycopg2 execute_values upsert in batches.
    Returns total rows processed.
    """
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, query, batch)
        conn.commit()
        total += len(batch)
    return total
```

---

## Step 2 — File: `modules/ingest.py`

```python
"""
Stage 2 — BHSA ingest.

Extracts morphological data from the BHSA text-fabric corpus and writes
to `verses` and `word_tokens` tables.
"""

from __future__ import annotations

import logging
import sys
from typing import List, Tuple

import psycopg2
import psycopg2.extras

sys.path.insert(0, "/pipeline")
from adapters.db_adapter import verse_ids_for_stage, batch_upsert

logger = logging.getLogger(__name__)

# BHSA part-of-speech codes → human-readable
POS_MAP = {
    "subs": "noun",
    "verb": "verb",
    "adjv": "adjective",
    "advb": "adverb",
    "prep": "preposition",
    "conj": "conjunction",
    "prps": "pronoun_personal",
    "prde": "pronoun_demonstrative",
    "prin": "pronoun_interrogative",
    "nmpr": "proper_noun",
    "intj": "interjection",
    "nega": "negative_particle",
    "inrg": "interrogative_particle",
    "art":  "article",
}

# Verb stems
STEM_MAP = {
    "qal": "qal", "nif": "niphal", "piel": "piel", "pual": "pual",
    "hif": "hiphil", "hof": "hophal", "hit": "hitpael",
    "htpo": "hitpolel", "nit": "nitpael",
}


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    """
    Ingest BHSA morphological data into `verses` and `word_tokens`.
    Returns summary dict with row counts.
    """
    from tf.fabric import Fabric

    bhsa_path = config["bhsa"]["data_path"]
    corpus = config.get("corpus", {})
    book_nums = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters = corpus.get("debug_chapters", [])
    batch_size = config.get("fingerprint", {}).get("batch_size", 100)

    logger.info(f"Loading BHSA from {bhsa_path}")
    TF = Fabric(
        locations=[f"{bhsa_path}/github/ETCBC/bhsa/tf/c"],
        silent=True
    )
    api = TF.load(
        "book chapter verse "
        "g_word_utf8 lex sp "
        "prs uvf vt vs "
        "nme pfm suffix "
        "label"  # clause labels for compression scoring
    )
    F, T, L = api.F, api.T, api.L

    # Book name lookup (book_num → BHSA internal name)
    BOOK_NAMES = {19: "Psalms", 23: "Isaiah", 18: "Job", 25: "Lamentations"}

    for book_num in book_nums:
        bhsa_book_name = BOOK_NAMES.get(book_num)
        if not bhsa_book_name:
            logger.warning(f"No BHSA book name mapping for book_num={book_num}")
            continue

        _ingest_book(conn, api, book_num, bhsa_book_name, debug_chapters, batch_size)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM verses WHERE book_num = ANY(%s)", (book_nums,))
        verse_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM word_tokens t JOIN verses v ON t.verse_id = v.verse_id WHERE v.book_num = ANY(%s)", (book_nums,))
        token_count = cur.fetchone()[0]

    return {"verses": verse_count, "word_tokens": token_count}


def _ingest_book(conn, api, book_num, bhsa_book_name, debug_chapters, batch_size):
    F, T, L = api.F, api.T, api.L

    # Collect all verse nodes for this book
    book_node = next(
        n for n in F.otype.s("book") if T.bookName(n) == bhsa_book_name
    )
    verse_nodes = L.d(book_node, "verse")

    verse_rows = []
    token_rows_by_verse = {}

    for v_node in verse_nodes:
        chapter = int(F.chapter.v(v_node))
        verse_num = int(F.verse.v(v_node))

        if debug_chapters and chapter not in debug_chapters:
            continue

        # Collect surface forms to build Hebrew text
        words = L.d(v_node, "word")
        surface_forms = [F.g_word_utf8.v(w) for w in words]
        hebrew_text = " ".join(surface_forms)

        verse_rows.append((book_num, chapter, verse_num, hebrew_text))

        tokens = []
        for pos, w_node in enumerate(words, start=1):
            pos_tag = F.sp.v(w_node) or ""
            human_pos = POS_MAP.get(pos_tag, pos_tag)
            is_verb = (pos_tag == "verb")
            is_noun = (pos_tag in ("subs", "nmpr"))
            stem = STEM_MAP.get(F.vs.v(w_node), None) if is_verb else None

            # Morpheme count: prefix count + 1 (stem) + suffix indicator
            prefix_count = _count_prefixes(w_node, F)
            has_suffix = bool(F.prs.v(w_node) or F.uvf.v(w_node))
            morpheme_count = prefix_count + 1 + (1 if has_suffix else 0)

            tokens.append((
                pos,                        # position
                F.g_word_utf8.v(w_node),    # surface_form
                F.lex.v(w_node),            # lexeme
                human_pos,                  # part_of_speech
                morpheme_count,             # morpheme_count
                is_verb,
                is_noun,
                stem,                       # verb stem or None
            ))

        token_rows_by_verse[(chapter, verse_num)] = tokens

    # Upsert verses
    logger.info(f"Upserting {len(verse_rows)} verse rows for book {book_num}")
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO verses (book_num, chapter, verse_num, hebrew_text)
            VALUES %s
            ON CONFLICT (book_num, chapter, verse_num) DO UPDATE
              SET hebrew_text = EXCLUDED.hebrew_text
            RETURNING chapter, verse_num, verse_id
            """,
            verse_rows,
            fetch=True
        )
        verse_id_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    conn.commit()

    # Upsert word tokens
    all_token_rows = []
    for (chapter, verse_num), tokens in token_rows_by_verse.items():
        verse_id = verse_id_map.get((chapter, verse_num))
        if verse_id is None:
            continue
        for t in tokens:
            all_token_rows.append((verse_id,) + t)

    logger.info(f"Upserting {len(all_token_rows)} token rows for book {book_num}")
    batch_upsert(
        conn,
        """
        INSERT INTO word_tokens
          (verse_id, position, surface_form, lexeme, part_of_speech,
           morpheme_count, is_verb, is_noun, stem)
        VALUES %s
        ON CONFLICT (verse_id, position) DO UPDATE
          SET surface_form = EXCLUDED.surface_form,
              lexeme = EXCLUDED.lexeme,
              part_of_speech = EXCLUDED.part_of_speech,
              morpheme_count = EXCLUDED.morpheme_count
        """,
        all_token_rows,
        batch_size=batch_size,
    )

    logger.info(f"Book {book_num}: done")


def _count_prefixes(word_node, F) -> int:
    """Count prefix morphemes. BHSA stores conjunction/preposition as part of word."""
    # pfm = prefix morpheme indicator
    pfm = F.pfm.v(word_node) or ""
    # Simple heuristic: any non-empty pfm = 1 prefix
    return 1 if pfm and pfm != "absent" else 0
```

---

## Step 3 — File: `modules/fingerprint.py`

```python
"""
Stage 2 — Style fingerprinting.

Computes the 4-dimensional style fingerprint for each verse:
  syllable_density    = mean syllables per word
  morpheme_ratio      = mean morphemes per word
  sonority_score      = mean consonant sonority (0–1)
  clause_compression  = mean words per clause boundary
"""

from __future__ import annotations

import logging
import re
from typing import List, Tuple

import psycopg2
import psycopg2.extras

from adapters.db_adapter import verse_ids_for_stage, batch_upsert

logger = logging.getLogger(__name__)

# Hebrew Unicode ranges
CONSONANTS   = set("אבגדהוזחטיכלמנסעפצקרשת" + "ךםןףץ")  # includes finals
VOWEL_POINTS = set("\u05B0\u05B1\u05B2\u05B3\u05B4\u05B5\u05B6\u05B7\u05B8\u05B9\u05BA\u05BB\u05C1\u05C2")
# Shewa and hataf vowels are not full syllable nuclei for density counting

# Consonant sonority scale (0.0 = least sonorous, 1.0 = most)
SONORITY = {
    # Glottals / gutturals — breathy but fricative
    "א": 0.60, "ה": 0.65, "ח": 0.45, "ע": 0.55,
    # Liquids — highly sonorous
    "ל": 0.90, "ר": 0.85, "מ": 0.80, "נ": 0.80,
    # Sibilants
    "שׁ": 0.40, "שׂ": 0.40, "ס": 0.40, "ז": 0.45, "צ": 0.35,
    "ש": 0.40,
    # Voiced stops
    "ב": 0.30, "ג": 0.30, "ד": 0.30,
    # Voiceless stops
    "כ": 0.20, "פ": 0.20, "ת": 0.20, "ק": 0.15, "ט": 0.20,
    # Approximants
    "י": 0.75, "ו": 0.70,
}
DEFAULT_SONORITY = 0.35


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    corpus = config.get("corpus", {})
    book_nums = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters = corpus.get("debug_chapters", [])
    batch_size = config.get("fingerprint", {}).get("batch_size", 100)

    pending = verse_ids_for_stage(conn, "verse_fingerprints", book_nums, debug_chapters)
    if not pending:
        logger.info("All verse fingerprints already computed.")
        return {"fingerprints_computed": 0}

    logger.info(f"Computing fingerprints for {len(pending)} verses")

    # Fetch token data for all pending verses in one query
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT verse_id, position, surface_form, morpheme_count, part_of_speech
            FROM word_tokens
            WHERE verse_id = ANY(%s)
            ORDER BY verse_id, position
            """,
            (pending,)
        )
        rows = cur.fetchall()

    # Fetch clause counts per verse (from word_tokens POS — clause boundaries
    # approximated by counting conjunction-initial words)
    # Group by verse
    from collections import defaultdict
    by_verse = defaultdict(list)
    for row in rows:
        by_verse[row[0]].append(row)

    fingerprint_rows = []
    for verse_id in pending:
        tokens = by_verse.get(verse_id, [])
        if not tokens:
            continue
        fp = _compute_fingerprint(tokens)
        fingerprint_rows.append((
            verse_id,
            fp["syllable_density"],
            fp["morpheme_ratio"],
            fp["sonority_score"],
            fp["clause_compression"],
        ))

    batch_upsert(
        conn,
        """
        INSERT INTO verse_fingerprints
          (verse_id, syllable_density, morpheme_ratio, sonority_score, clause_compression)
        VALUES %s
        ON CONFLICT (verse_id) DO UPDATE
          SET syllable_density    = EXCLUDED.syllable_density,
              morpheme_ratio      = EXCLUDED.morpheme_ratio,
              sonority_score      = EXCLUDED.sonority_score,
              clause_compression  = EXCLUDED.clause_compression,
              computed_at         = NOW()
        """,
        fingerprint_rows,
        batch_size=batch_size,
    )

    logger.info(f"Fingerprints computed: {len(fingerprint_rows)}")

    # Update word_count on verses table
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE verses v
            SET word_count = sub.cnt
            FROM (
                SELECT verse_id, COUNT(*) as cnt
                FROM word_tokens
                WHERE verse_id = ANY(%s)
                GROUP BY verse_id
            ) sub
            WHERE v.verse_id = sub.verse_id
            """,
            (pending,)
        )
    conn.commit()

    return {"fingerprints_computed": len(fingerprint_rows)}


def _compute_fingerprint(tokens: list) -> dict:
    """
    Compute 4D fingerprint from a list of word token rows.
    Each row: (verse_id, position, surface_form, morpheme_count, part_of_speech)
    """
    surface_forms = [r[2] for r in tokens]
    morpheme_counts = [r[3] or 1 for r in tokens]
    pos_tags = [r[4] or "" for r in tokens]

    n = len(tokens)

    # syllable_density
    syllable_counts = [count_hebrew_syllables(f) for f in surface_forms]
    syllable_density = sum(syllable_counts) / n if n > 0 else 0.0

    # morpheme_ratio
    morpheme_ratio = sum(morpheme_counts) / n if n > 0 else 0.0

    # sonority_score — mean sonority of first (onset) consonant of each word
    sonority_values = [_onset_sonority(f) for f in surface_forms]
    sonority_score = sum(sonority_values) / len(sonority_values) if sonority_values else 0.0

    # clause_compression — words per clause
    # Approximate clause boundaries by conjunction-initial words + start
    clause_starts = 1 + sum(
        1 for p in pos_tags if p in ("conjunction",)
    )
    clause_compression = n / clause_starts if clause_starts > 0 else float(n)

    return {
        "syllable_density":   round(syllable_density, 4),
        "morpheme_ratio":     round(morpheme_ratio, 4),
        "sonority_score":     round(sonority_score, 4),
        "clause_compression": round(clause_compression, 4),
    }


def count_hebrew_syllables(word: str) -> int:
    """
    Count syllables in a Hebrew word with niqqud.

    A syllable nucleus is a non-shewa vowel point. Shewa (U+05B0) and
    hataf variants (U+05B1–U+05B3) count as half-syllables; we round up
    when they are the only vowel in a word (monosyllabic function words).
    """
    FULL_VOWELS = set("\u05B4\u05B5\u05B6\u05B7\u05B8\u05B9\u05BA\u05BB\u05C1\u05C2")
    HALF_VOWELS = set("\u05B0\u05B1\u05B2\u05B3")

    full = sum(1 for c in word if c in FULL_VOWELS)
    half = sum(1 for c in word if c in HALF_VOWELS)

    if full == 0 and half > 0:
        return 1   # function word with only shewa
    if full == 0 and half == 0:
        return 1   # unvocalized word — assume monosyllabic
    return full


def _onset_sonority(word: str) -> float:
    """Return sonority score for the onset consonant of the word."""
    for ch in word:
        if ch in CONSONANTS:
            return SONORITY.get(ch, DEFAULT_SONORITY)
    return DEFAULT_SONORITY
```

---

## Step 4 — File: `modules/chiasm.py`

```python
"""
Stage 2 (second pass) — Chiasm detection.

Detects ABBA and ABCBA chiastic patterns across colon sequences.
Requires colon boundary data from Stage 3 (breath_profiles.colon_boundaries).

All output is stored as candidates with confidence scores.
These are observations for interpretive review, not asserted findings.
"""

from __future__ import annotations

import json
import logging
from typing import List, Dict, Tuple, Optional

import numpy as np
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    """
    Detect chiastic patterns within stanzas.

    A stanza is a contiguous sequence of verses within a Psalm chapter.
    For Psalms, we treat each chapter as a single stanza candidate.
    """
    chiasm_config = config.get("chiasm", {})
    similarity_threshold = chiasm_config.get("similarity_threshold", 0.75)
    min_confidence = chiasm_config.get("min_confidence", 0.65)
    max_stanza_verses = chiasm_config.get("max_stanza_verses", 8)

    corpus = config.get("corpus", {})
    book_nums = [b["book_num"] for b in corpus.get("books", [])]

    # Fetch colon-level fingerprint data
    colon_fps = _load_colon_fingerprints(conn, book_nums)
    if not colon_fps:
        logger.warning("No colon fingerprints found. Run Stage 3 first.")
        return {"candidates_found": 0}

    # Fetch chapter groupings
    chapters = _load_chapter_groupings(conn, book_nums)

    candidates = []
    for (book_num, chapter), verse_ids in chapters.items():
        if len(verse_ids) > max_stanza_verses:
            verse_ids = verse_ids[:max_stanza_verses]

        # Get colon fingerprint vectors for this chapter in verse order
        chapter_colons = []  # List of (verse_id, colon_idx, vector)
        for v_id in verse_ids:
            for colon_idx, vec in colon_fps.get(v_id, []):
                chapter_colons.append((v_id, colon_idx, vec))

        if len(chapter_colons) < 4:
            continue

        # Detect patterns
        found = _detect_patterns(
            chapter_colons, similarity_threshold, min_confidence
        )
        candidates.extend(found)

    # Store results
    _store_candidates(conn, candidates)
    logger.info(f"Chiasm detection complete: {len(candidates)} candidates stored")
    return {"candidates_found": len(candidates)}


def _load_colon_fingerprints(
    conn: psycopg2.extensions.connection,
    book_nums: List[int]
) -> Dict[int, List[Tuple[int, np.ndarray]]]:
    """Load colon-level fingerprint arrays from verse_fingerprints.colon_fingerprints."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT vf.verse_id, vf.colon_fingerprints
            FROM verse_fingerprints vf
            JOIN verses v ON vf.verse_id = v.verse_id
            WHERE v.book_num = ANY(%s)
              AND vf.colon_fingerprints IS NOT NULL
            """,
            (book_nums,)
        )
        rows = cur.fetchall()

    result = {}
    for verse_id, colon_fps_json in rows:
        if not colon_fps_json:
            continue
        colon_data = colon_fps_json if isinstance(colon_fps_json, list) else json.loads(colon_fps_json)
        colon_vecs = []
        for item in colon_data:
            vec = np.array([
                item.get("density", 0.0),
                item.get("morpheme_ratio", 0.0),
                item.get("sonority", 0.0),
                item.get("compression", 0.0),
            ], dtype=float)
            colon_vecs.append((item["colon"], vec))
        result[verse_id] = colon_vecs

    return result


def _load_chapter_groupings(
    conn: psycopg2.extensions.connection,
    book_nums: List[int]
) -> Dict[Tuple[int, int], List[int]]:
    """Return {(book_num, chapter): [verse_id, ...]} in verse order."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT book_num, chapter, verse_id
            FROM verses
            WHERE book_num = ANY(%s)
            ORDER BY book_num, chapter, verse_num
            """,
            (book_nums,)
        )
        from collections import defaultdict
        chapters = defaultdict(list)
        for book_num, chapter, verse_id in cur.fetchall():
            chapters[(book_num, chapter)].append(verse_id)
    return dict(chapters)


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two vectors. Returns 0.0 if either is zero."""
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def _detect_patterns(
    colons: List[Tuple[int, int, np.ndarray]],
    threshold: float,
    min_confidence: float,
) -> List[dict]:
    """
    Scan colon sequence for ABBA and ABCBA patterns.

    For ABBA: colons at positions i, i+1, i+2, i+3 where
      sim(i, i+3) >= threshold and sim(i+1, i+2) >= threshold

    For ABCBA: colons at positions i..i+4 where
      sim(i, i+4) >= threshold and sim(i+1, i+3) >= threshold
      (colon i+2 is the pivot — C — no constraint)
    """
    candidates = []
    n = len(colons)

    for i in range(n - 3):
        # ABBA (4-element)
        if i + 3 < n:
            v_ids = [colons[j][0] for j in range(i, i+4)]
            sims = {
                "outer": _cosine_similarity(colons[i][2], colons[i+3][2]),
                "inner": _cosine_similarity(colons[i+1][2], colons[i+2][2]),
            }
            if sims["outer"] >= threshold and sims["inner"] >= threshold:
                confidence = (sims["outer"] + sims["inner"]) / 2
                if confidence >= min_confidence:
                    candidates.append({
                        "verse_id_start": min(v_ids),
                        "verse_id_end":   max(v_ids),
                        "pattern_type": "ABBA",
                        "colon_matches": [
                            {"a": i,   "b": i+3, "similarity": round(sims["outer"], 4)},
                            {"a": i+1, "b": i+2, "similarity": round(sims["inner"], 4)},
                        ],
                        "confidence": round(confidence, 4),
                    })

        # ABCBA (5-element)
        if i + 4 < n:
            v_ids = [colons[j][0] for j in range(i, i+5)]
            sims = {
                "outer": _cosine_similarity(colons[i][2], colons[i+4][2]),
                "inner": _cosine_similarity(colons[i+1][2], colons[i+3][2]),
            }
            if sims["outer"] >= threshold and sims["inner"] >= threshold:
                confidence = (sims["outer"] * 0.5 + sims["inner"] * 0.5)
                if confidence >= min_confidence:
                    candidates.append({
                        "verse_id_start": min(v_ids),
                        "verse_id_end":   max(v_ids),
                        "pattern_type": "ABCBA",
                        "colon_matches": [
                            {"a": i,   "b": i+4, "similarity": round(sims["outer"], 4)},
                            {"a": i+1, "b": i+3, "similarity": round(sims["inner"], 4)},
                            {"pivot": i+2},
                        ],
                        "confidence": round(confidence, 4),
                    })

    return candidates


def _store_candidates(conn: psycopg2.extensions.connection, candidates: List[dict]):
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
              (verse_id_start, verse_id_end, pattern_type, colon_matches, confidence)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
    conn.commit()
```

---

## Step 5 — Test Cases

Save as `tests/test_fingerprint.py`:

```python
"""Tests for Stage 2 fingerprint module."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.fingerprint import count_hebrew_syllables, _compute_fingerprint


def test_syllable_count_basic():
    # יְהוָה — 2 syllables (full vowels: patah + qamets)
    word = "יְהוָה"
    assert count_hebrew_syllables(word) == 2


def test_syllable_count_single():
    # כִּי — 1 syllable
    word = "כִּי"
    assert count_hebrew_syllables(word) == 1


def test_syllable_count_no_vowels():
    # Consonantal text only — assume 1
    word = "כי"
    assert count_hebrew_syllables(word) == 1


def test_fingerprint_returns_four_dimensions():
    # Minimal token list: (verse_id, position, surface_form, morpheme_count, pos)
    tokens = [
        (1, 1, "הָאִישׁ", 1, "noun"),
        (1, 2, "הַהוּא", 2, "pronoun_demonstrative"),
        (1, 3, "הָלַךְ", 2, "verb"),
    ]
    fp = _compute_fingerprint(tokens)
    assert "syllable_density" in fp
    assert "morpheme_ratio" in fp
    assert "sonority_score" in fp
    assert "clause_compression" in fp
    for v in fp.values():
        assert isinstance(v, float)
        assert 0.0 <= v <= 20.0  # rough sanity bound


def test_fingerprint_morpheme_ratio():
    tokens = [
        (1, 1, "אֶת", 1, "preposition"),
        (1, 2, "הָאֱלֹהִים", 2, "noun"),
    ]
    fp = _compute_fingerprint(tokens)
    # Mean of [1, 2] = 1.5
    assert abs(fp["morpheme_ratio"] - 1.5) < 0.01
```

```python
# tests/test_chiasm.py
"""Tests for chiasm detection logic."""

import numpy as np
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.chiasm import _cosine_similarity, _detect_patterns


def test_cosine_similarity_identical():
    v = np.array([1.0, 2.0, 3.0, 4.0])
    assert abs(_cosine_similarity(v, v) - 1.0) < 1e-6


def test_cosine_similarity_orthogonal():
    a = np.array([1.0, 0.0, 0.0, 0.0])
    b = np.array([0.0, 1.0, 0.0, 0.0])
    assert abs(_cosine_similarity(a, b)) < 1e-6


def test_cosine_similarity_zero_vector():
    a = np.zeros(4)
    b = np.array([1.0, 2.0, 3.0, 4.0])
    assert _cosine_similarity(a, b) == 0.0


def test_detect_abba_pattern():
    # Colons: A B B' A' (colons 0 and 3 similar; 1 and 2 similar)
    a = np.array([1.0, 0.0, 0.0, 0.0])
    b = np.array([0.0, 1.0, 0.0, 0.0])
    # Each pair has similarity 1.0 with itself
    colons = [
        (100, 1, a),   # verse 100, colon 1, vector A
        (100, 2, b),   # verse 100, colon 2, vector B
        (100, 3, b),   # verse 100, colon 3, vector B (matches colon 2)
        (100, 4, a),   # verse 100, colon 4, vector A (matches colon 1)
    ]
    results = _detect_patterns(colons, threshold=0.8, min_confidence=0.6)
    abba = [r for r in results if r["pattern_type"] == "ABBA"]
    assert len(abba) >= 1
    assert abba[0]["confidence"] >= 0.8


def test_detect_abcba_pattern():
    a = np.array([1.0, 0.0, 0.0, 0.0])
    b = np.array([0.0, 1.0, 0.0, 0.0])
    c = np.array([0.0, 0.0, 1.0, 0.0])  # pivot
    colons = [
        (101, 1, a),
        (101, 2, b),
        (101, 3, c),   # pivot C
        (102, 1, b),   # matches colon 2
        (102, 2, a),   # matches colon 1
    ]
    results = _detect_patterns(colons, threshold=0.8, min_confidence=0.6)
    abcba = [r for r in results if r["pattern_type"] == "ABCBA"]
    assert len(abcba) >= 1


def test_no_pattern_when_dissimilar():
    vectors = [np.random.rand(4) for _ in range(6)]
    colons = [(200, i, v) for i, v in enumerate(vectors)]
    # With very high threshold, should find nothing on random vectors
    results = _detect_patterns(colons, threshold=0.999, min_confidence=0.999)
    assert len(results) == 0
```

Run:
```bash
docker compose --profile pipeline run --rm pipeline python -m pytest /pipeline/tests/test_fingerprint.py /pipeline/tests/test_chiasm.py -v
```

---

## Acceptance Criteria

- [ ] `verses` table contains exactly 2,527 rows for Psalms (or subset if `debug_chapters` set)
- [ ] `word_tokens` table contains ~43,000 rows for Psalms
- [ ] `verse_fingerprints` contains one row per verse
- [ ] All four fingerprint columns are non-null and within expected ranges:
  - `syllable_density`: 1.5–4.0
  - `morpheme_ratio`: 1.0–4.5
  - `sonority_score`: 0.2–0.8
  - `clause_compression`: 2.0–15.0
- [ ] After Stage 3: `chiasm_candidates` contains at least some candidates (expect 20–200 for Psalms depending on thresholds)
- [ ] All unit tests pass
- [ ] Module is resumable: running twice with same config produces same row counts (no duplicates)

---

## SQL Validation Queries

```sql
-- Verse count
SELECT COUNT(*) FROM verses WHERE book_num = 19;
-- Expected: 2527

-- Fingerprint coverage
SELECT COUNT(*) FROM verse_fingerprints vf
JOIN verses v ON vf.verse_id = v.verse_id WHERE v.book_num = 19;
-- Expected: 2527

-- Fingerprint range check
SELECT
  MIN(syllable_density), MAX(syllable_density),
  MIN(morpheme_ratio),   MAX(morpheme_ratio),
  MIN(sonority_score),   MAX(sonority_score),
  MIN(clause_compression), MAX(clause_compression)
FROM verse_fingerprints vf
JOIN verses v ON vf.verse_id = v.verse_id WHERE v.book_num = 19;

-- Sample Psalm 23 tokens
SELECT position, surface_form, lexeme, part_of_speech, morpheme_count
FROM word_tokens wt
JOIN verses v ON wt.verse_id = v.verse_id
WHERE v.book_num = 19 AND v.chapter = 23 AND v.verse_num = 1
ORDER BY position;
```
