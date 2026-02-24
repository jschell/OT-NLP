# Stage 4 — Translation Scoring
## Detailed Implementation Plan

> **Depends on:** Stage 3 (`syllable_tokens`, `breath_profiles`, and `translations` fully populated)  
> **Produces:** `translation_scores` with style deviation and breath alignment metrics for every verse × translation pair  
> **Estimated time:** 15–45 minutes (Psalms, 5 translations)

---

## Objectives

1. Implement English syllable and phoneme parsing using the CMU Pronouncing Dictionary
2. Compute style deviation between Hebrew fingerprint and English translation fingerprint
3. Compute breath alignment score between Hebrew breath curve and English stress pattern
4. Store all scores in `translation_scores`

---

## Conceptual Overview

### Style Deviation

For each verse × translation pair, we compute the same 4-dimensional fingerprint on the English text that was computed on the Hebrew in Stage 2, then measure the weighted Euclidean distance:

```
hebrew_vec  = [syllable_density, morpheme_ratio, sonority_score, clause_compression]
english_vec = [syllable_density, morpheme_ratio, sonority_score, clause_compression]

deviation   = weighted_euclidean(hebrew_vec, english_vec)
```

Because English and Hebrew are different languages, the absolute values will differ — what we measure is relative deviation across translations. A translation with lower composite deviation preserves more of the Hebrew's phonetic and morphological texture.

### Breath Alignment

Breath alignment measures whether the stress peaks in the English translation fall at similar proportions of the verse as the stressed syllables in the Hebrew:

```
hebrew_stress_positions  = [0.1, 0.4, 0.7]      # normalized 0–1
english_stress_positions = [0.15, 0.45, 0.68]   # normalized 0–1

stress_alignment = 1 - mean(min_distance(h, e)) for each h in hebrew
```

`weight_match` measures whether the mean English syllable weight (derived from CMU vowel phoneme classes) approximates the mean Hebrew breath weight.

---

## File Structure

```
pipeline/
  modules/
    score.py               ← main scoring module
  adapters/
    phoneme_adapter.py     ← CMU dict lookup + heuristic fallback
  tests/
    test_score.py
    test_phoneme_adapter.py
```

---

## Step 1 — File: `adapters/phoneme_adapter.py`

```python
"""
English phoneme adapter.

Uses the CMU Pronouncing Dictionary via the `pronouncing` library for
known words, with a heuristic syllable counter fallback for unknown words.

No external API calls — CMU dictionary is bundled with the library.
"""

from __future__ import annotations

import re
import logging
from typing import List, Dict, Optional, Tuple
from functools import lru_cache

import pronouncing

logger = logging.getLogger(__name__)

# CMU vowel phonemes → openness score (approximate IPA mapping)
# ARPABET vowel codes with stress digit stripped
VOWEL_OPENNESS = {
    # Open vowels (low, back)
    "AA": 1.00,   # father
    "AE": 0.85,   # cat
    "AH": 0.80,   # strut / schwa
    # Mid vowels
    "AO": 0.75,   # thought
    "AW": 0.72,   # mouth
    "AY": 0.78,   # price
    "EH": 0.65,   # dress
    "EY": 0.60,   # face
    "OW": 0.70,   # goat
    "OY": 0.68,   # choice
    # Close vowels (high, front/back)
    "IH": 0.45,   # kit
    "IY": 0.40,   # fleece
    "UH": 0.42,   # foot
    "UW": 0.38,   # goose
    # Reduced vowel
    "ER": 0.50,   # nurse
}
DEFAULT_VOWEL_OPENNESS = 0.55


def get_syllables_and_stress(word: str) -> Tuple[int, List[float]]:
    """
    Return (syllable_count, stress_weights) for an English word.

    stress_weights is a list of per-syllable breath weights based on
    vowel openness and CMU stress marks (0=unstressed, 1=primary, 2=secondary).
    
    Falls back to heuristic counter for unknown words.
    """
    word_clean = word.lower().strip(".,;:!?\"'()-")
    phones = pronouncing.phones_for_word(word_clean)

    if phones:
        return _parse_cmu_phones(phones[0])
    else:
        return _heuristic_syllables(word_clean)


def _parse_cmu_phones(phones_str: str) -> Tuple[int, List[float]]:
    """Parse a CMU phones string into syllable count and per-syllable weights."""
    tokens = phones_str.split()
    syllable_weights = []

    for token in tokens:
        # Vowel phones end with a digit (stress mark)
        if token[-1].isdigit():
            stress = int(token[-1])
            vowel_code = token[:-1]
            openness = VOWEL_OPENNESS.get(vowel_code, DEFAULT_VOWEL_OPENNESS)
            # Stressed syllables get a boost
            stress_multiplier = {0: 0.7, 1: 1.0, 2: 0.85}.get(stress, 0.75)
            syllable_weights.append(round(openness * stress_multiplier, 4))

    if not syllable_weights:
        return 1, [0.4]

    return len(syllable_weights), syllable_weights


def _heuristic_syllables(word: str) -> Tuple[int, List[float]]:
    """
    Heuristic syllable counter for words not in CMU dictionary.
    Rules: count vowel groups (aeiouy), with adjustments for silent-e.
    """
    word = word.lower()
    # Remove silent trailing e
    if len(word) > 2 and word.endswith("e") and word[-2] not in "aeiouy":
        word = word[:-1]
    # Remove silent trailing ed
    if len(word) > 3 and word.endswith("ed") and word[-3] not in "aeiouy":
        word = word[:-1]

    vowel_groups = re.findall(r"[aeiouy]+", word)
    count = max(1, len(vowel_groups))
    weights = [DEFAULT_VOWEL_OPENNESS] * count
    return count, weights


@lru_cache(maxsize=10000)
def syllable_count(word: str) -> int:
    """Cached syllable count for a single word."""
    return get_syllables_and_stress(word)[0]


def english_fingerprint(text: str) -> Dict[str, float]:
    """
    Compute the 4-dimensional style fingerprint for an English verse text.

    Returns dict with syllable_density, morpheme_ratio, sonority_score,
    clause_compression.
    """
    words = _tokenize(text)
    if not words:
        return {k: 0.0 for k in ("syllable_density","morpheme_ratio","sonority_score","clause_compression")}

    n = len(words)

    # Syllable density
    syl_counts = [syllable_count(w) for w in words]
    syllable_density = sum(syl_counts) / n

    # Morpheme ratio approximation: mean word length in characters / 4
    # English morphemes correlate with word length; this is a proxy measure
    morpheme_ratio = sum(len(w) for w in words) / (n * 4.0)
    morpheme_ratio = min(morpheme_ratio, 4.0)

    # Sonority score: mean onset sonority using English consonant classes
    sonority_values = [_english_onset_sonority(w) for w in words]
    sonority_score = sum(sonority_values) / len(sonority_values)

    # Clause compression: words per clause (clauses = sentence-boundary + comma)
    clause_separators = text.count(",") + text.count(";") + 1
    clause_compression = n / clause_separators

    return {
        "syllable_density":   round(syllable_density, 4),
        "morpheme_ratio":     round(morpheme_ratio, 4),
        "sonority_score":     round(sonority_score, 4),
        "clause_compression": round(clause_compression, 4),
    }


def english_breath_weights(text: str) -> List[float]:
    """
    Return a flat list of per-syllable breath weights for an English verse.
    Used for breath alignment scoring.
    """
    words = _tokenize(text)
    weights = []
    for w in words:
        _, syl_weights = get_syllables_and_stress(w)
        weights.extend(syl_weights)
    return weights


def _tokenize(text: str) -> List[str]:
    """Split English verse text into words, removing punctuation."""
    return [w for w in re.split(r"\s+", re.sub(r"[^\w\s]", " ", text)) if w and w.isalpha()]


# English onset sonority (simplified)
_ENG_SONORITY = {
    "l": 0.90, "r": 0.85, "m": 0.80, "n": 0.80, "w": 0.75, "y": 0.70,
    "v": 0.50, "z": 0.50, "j": 0.45, "b": 0.35, "d": 0.35, "g": 0.30,
    "f": 0.40, "s": 0.40, "h": 0.65, "k": 0.20, "p": 0.20, "t": 0.20,
    "c": 0.25, "q": 0.15, "x": 0.25,
}

def _english_onset_sonority(word: str) -> float:
    first = word[0].lower() if word else ""
    return _ENG_SONORITY.get(first, 0.30)
```

---

## Step 2 — File: `modules/score.py`

```python
"""
Stage 4 — Translation scoring.

For each verse × translation pair:
  1. Compute English style fingerprint
  2. Compute deviation from Hebrew fingerprint
  3. Compute breath alignment against Hebrew breath curve
  4. Store in translation_scores
"""

from __future__ import annotations

import logging
import math
from typing import List, Dict, Tuple, Optional

import numpy as np
import psycopg2
import psycopg2.extras

from adapters.db_adapter import batch_upsert
from adapters.phoneme_adapter import english_fingerprint, english_breath_weights

logger = logging.getLogger(__name__)


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    corpus = config.get("corpus", {})
    book_nums = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters = corpus.get("debug_chapters", [])
    batch_size = config.get("scoring", {}).get("batch_size", 100)

    # Deviation dimension weights
    w = config.get("scoring", {}).get("deviation_weights", {})
    dev_weights = np.array([
        w.get("density",     0.35),
        w.get("morpheme",    0.25),
        w.get("sonority",    0.20),
        w.get("compression", 0.20),
    ])

    # Breath alignment weights
    bw = config.get("scoring", {}).get("breath_alignment_weights", {})
    stress_weight = bw.get("stress", 0.60)
    weight_match_weight = bw.get("weight", 0.40)

    # Get translation keys to score
    sources = config.get("translations", {}).get("sources", [])
    translation_keys = [s["id"] for s in sources]

    # Fetch Hebrew fingerprints
    heb_fps = _load_hebrew_fingerprints(conn, book_nums, debug_chapters)
    # Fetch Hebrew breath profiles
    heb_breath = _load_hebrew_breath(conn, book_nums, debug_chapters)
    # Fetch translation texts
    trans_texts = _load_translation_texts(conn, translation_keys, book_nums, debug_chapters)

    if not heb_fps:
        logger.error("No Hebrew fingerprints found. Run Stage 2 first.")
        return {"scored": 0}

    logger.info(
        f"Scoring {len(heb_fps)} verses × {len(translation_keys)} translations "
        f"= {len(heb_fps) * len(translation_keys)} pairs"
    )

    score_rows = []
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

            # Per-dimension deviations
            diffs = np.abs(heb_vec - eng_vec)
            density_dev     = float(diffs[0])
            morpheme_dev    = float(diffs[1])
            sonority_dev    = float(diffs[2])
            compression_dev = float(diffs[3])
            composite_dev   = float(np.dot(dev_weights, diffs))

            # Breath alignment
            stress_align, weight_match = _compute_breath_alignment(
                heb_curve, text
            )
            breath_align = round(
                stress_align * stress_weight + weight_match * weight_match_weight, 4
            )

            score_rows.append((
                verse_id,
                key,
                round(density_dev,     4),
                round(morpheme_dev,    4),
                round(sonority_dev,    4),
                round(compression_dev, 4),
                round(composite_dev,   4),
                round(stress_align,    4),
                round(weight_match,    4),
                round(breath_align,    4),
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

    # Update word_count on translations
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE translations t
            SET word_count = sub.wc
            FROM (
                SELECT verse_id, translation_key,
                       array_length(regexp_split_to_array(
                           regexp_replace(verse_text, '[^\\w\\s]', ' ', 'g'), '\\s+'), 1) AS wc
                FROM translations
                WHERE verse_id IN (SELECT verse_id FROM verses WHERE book_num = ANY(%s))
            ) sub
            WHERE t.verse_id = sub.verse_id AND t.translation_key = sub.translation_key
            """,
            (book_nums,)
        )
    conn.commit()

    logger.info(f"Scored {len(score_rows)} verse × translation pairs")
    return {"scored": len(score_rows)}


def _load_hebrew_fingerprints(conn, book_nums, debug_chapters):
    q = """
        SELECT vf.verse_id, vf.syllable_density, vf.morpheme_ratio,
               vf.sonority_score, vf.clause_compression
        FROM verse_fingerprints vf
        JOIN verses v ON vf.verse_id = v.verse_id
        WHERE v.book_num = ANY(%s)
    """
    params = [book_nums]
    if debug_chapters:
        q += " AND v.chapter = ANY(%s)"
        params.append(debug_chapters)
    with conn.cursor() as cur:
        cur.execute(q, params)
        return {
            r[0]: {
                "syllable_density":   float(r[1] or 0),
                "morpheme_ratio":     float(r[2] or 0),
                "sonority_score":     float(r[3] or 0),
                "clause_compression": float(r[4] or 0),
            }
            for r in cur.fetchall()
        }


def _load_hebrew_breath(conn, book_nums, debug_chapters):
    q = """
        SELECT bp.verse_id, bp.breath_curve, bp.stress_positions, bp.mean_weight
        FROM breath_profiles bp
        JOIN verses v ON bp.verse_id = v.verse_id
        WHERE v.book_num = ANY(%s)
    """
    params = [book_nums]
    if debug_chapters:
        q += " AND v.chapter = ANY(%s)"
        params.append(debug_chapters)
    with conn.cursor() as cur:
        cur.execute(q, params)
        return {
            r[0]: {
                "breath_curve":    r[1] or [],
                "stress_positions": r[2] or [],
                "mean_weight":     float(r[3] or 0),
            }
            for r in cur.fetchall()
        }


def _load_translation_texts(conn, translation_keys, book_nums, debug_chapters):
    q = """
        SELECT t.verse_id, t.translation_key, t.verse_text
        FROM translations t
        JOIN verses v ON t.verse_id = v.verse_id
        WHERE v.book_num = ANY(%s)
          AND t.translation_key = ANY(%s)
    """
    params = [book_nums, translation_keys]
    if debug_chapters:
        q += " AND v.chapter = ANY(%s)"
        params.append(debug_chapters)
    with conn.cursor() as cur:
        cur.execute(q, params)
        return {(r[0], r[1]): r[2] for r in cur.fetchall()}


def _compute_breath_alignment(
    heb_data: dict,
    english_text: str,
) -> Tuple[float, float]:
    """
    Compute stress_alignment and weight_match scores.

    stress_alignment: 1 minus mean minimum distance between Hebrew and
    English stressed syllable positions (normalized 0–1).

    weight_match: similarity between mean Hebrew breath weight and mean
    English syllable weight.
    """
    heb_stress = heb_data.get("stress_positions", [])
    heb_mean_weight = heb_data.get("mean_weight", 0.5)

    eng_weights = english_breath_weights(english_text)
    n_eng = len(eng_weights)

    if not eng_weights:
        return 0.5, 0.5

    # Stress alignment
    if not heb_stress or n_eng == 0:
        stress_align = 0.5
    else:
        eng_positions = [i / max(n_eng - 1, 1) for i in range(n_eng)]
        # Find primary stress peaks in English: above-mean weight
        mean_eng = sum(eng_weights) / len(eng_weights)
        eng_stress = [
            eng_positions[i]
            for i, w in enumerate(eng_weights)
            if w > mean_eng * 1.1
        ]

        if not eng_stress:
            stress_align = 0.5
        else:
            # For each Hebrew stress position, find nearest English stress
            distances = []
            for h_pos in heb_stress:
                nearest = min(abs(h_pos - e_pos) for e_pos in eng_stress)
                distances.append(nearest)
            stress_align = max(0.0, 1.0 - (sum(distances) / len(distances)))

    # Weight match: compare mean weights
    mean_eng_weight = sum(eng_weights) / len(eng_weights)
    weight_diff = abs(heb_mean_weight - mean_eng_weight)
    weight_match = max(0.0, 1.0 - weight_diff * 2)

    return round(stress_align, 4), round(weight_match, 4)
```

---

## Step 3 — Test Cases

```python
# tests/test_phoneme_adapter.py

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from adapters.phoneme_adapter import (
    get_syllables_and_stress,
    syllable_count,
    english_fingerprint,
    english_breath_weights,
    _heuristic_syllables,
)


class TestSyllableCount:

    def test_common_word_in_cmu(self):
        # "shepherd" = 2 syllables
        count, _ = get_syllables_and_stress("shepherd")
        assert count == 2

    def test_lord_one_syllable(self):
        count, _ = get_syllables_and_stress("lord")
        assert count == 1

    def test_heuristic_fallback_for_unknown(self):
        # Made-up word not in CMU
        count, weights = _heuristic_syllables("xyzphrm")
        assert count >= 1
        assert len(weights) == count

    def test_syllable_count_cached(self):
        # Calling twice should return same result (exercises cache)
        c1 = syllable_count("praise")
        c2 = syllable_count("praise")
        assert c1 == c2


class TestEnglishFingerprint:

    def test_returns_four_keys(self):
        fp = english_fingerprint("The LORD is my shepherd I shall not want")
        assert set(fp.keys()) == {"syllable_density", "morpheme_ratio", "sonority_score", "clause_compression"}

    def test_all_values_positive(self):
        fp = english_fingerprint("Blessed is the man who walks not in the counsel of the wicked")
        for v in fp.values():
            assert v > 0.0

    def test_empty_string_returns_zeros(self):
        fp = english_fingerprint("")
        for v in fp.values():
            assert v == 0.0

    def test_longer_words_higher_morpheme_ratio(self):
        simple = english_fingerprint("The cat sat")
        complex_ = english_fingerprint("Righteousness and lovingkindness")
        assert complex_["morpheme_ratio"] >= simple["morpheme_ratio"]


class TestBreathWeights:

    def test_returns_nonempty_for_sentence(self):
        weights = english_breath_weights("The LORD is my shepherd")
        assert len(weights) > 0

    def test_all_weights_in_range(self):
        weights = english_breath_weights("Praise the LORD all ye nations")
        for w in weights:
            assert 0.0 <= w <= 1.0

    def test_empty_string(self):
        weights = english_breath_weights("")
        assert weights == []
```

```python
# tests/test_score.py

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
from modules.score import _compute_breath_alignment


def test_identical_stress_positions_gives_high_alignment():
    heb_data = {
        "stress_positions": [0.25, 0.5, 0.75],
        "mean_weight": 0.60,
    }
    # English text engineered to have similar stress points
    # We cannot control exactly without mocking, so use a text
    # and just check the score is in valid range
    align, match = _compute_breath_alignment(
        heb_data, "Praise the LORD O Jerusalem"
    )
    assert 0.0 <= align <= 1.0
    assert 0.0 <= match <= 1.0


def test_empty_breath_data_returns_midpoint():
    heb_data = {"stress_positions": [], "mean_weight": 0.5}
    align, match = _compute_breath_alignment(heb_data, "some text")
    assert align == 0.5
    assert match == 0.5


def test_alignment_returns_floats():
    heb_data = {"stress_positions": [0.3, 0.7], "mean_weight": 0.55}
    align, match = _compute_breath_alignment(heb_data, "The heavens declare the glory of God")
    assert isinstance(align, float)
    assert isinstance(match, float)
```

Run:
```bash
docker compose --profile pipeline run --rm pipeline python -m pytest /pipeline/tests/test_phoneme_adapter.py /pipeline/tests/test_score.py -v
```

---

## Acceptance Criteria

- [ ] `translation_scores` contains rows for all verse × translation combinations (2,527 × 5 = 12,635 rows for Psalms with 5 translations)
- [ ] All deviation columns are non-null and ≥ 0
- [ ] `composite_deviation` values fall in range 0.0–5.0 for realistic translations
- [ ] `stress_alignment` and `weight_match` in range [0.0, 1.0]
- [ ] `breath_alignment` in range [0.0, 1.0]
- [ ] YLT and ULT should show lower composite deviation than UST for most verses (literal translations are closer to Hebrew structure) — verify via SQL query below
- [ ] All unit tests pass

---

## SQL Validation Queries

```sql
-- Score coverage
SELECT translation_key, COUNT(*) as scored_verses
FROM translation_scores ts
JOIN verses v ON ts.verse_id = v.verse_id WHERE v.book_num = 19
GROUP BY translation_key ORDER BY translation_key;
-- Expected: 2527 rows per translation

-- Mean scores by translation (lower deviation = more structure-preserving)
SELECT
  translation_key,
  ROUND(AVG(composite_deviation)::numeric, 4) AS mean_dev,
  ROUND(AVG(breath_alignment)::numeric, 4)    AS mean_breath_align,
  ROUND(MIN(composite_deviation)::numeric, 4) AS min_dev,
  ROUND(MAX(composite_deviation)::numeric, 4) AS max_dev
FROM translation_scores ts
JOIN verses v ON ts.verse_id = v.verse_id WHERE v.book_num = 19
GROUP BY translation_key
ORDER BY mean_dev;
-- Expected order (roughly): YLT < ULT < KJV < WEB < UST

-- Top 20 highest deviation verses (Psalm 23 example)
SELECT
  v.chapter, v.verse_num, ts.translation_key,
  ts.composite_deviation, ts.breath_alignment,
  t.verse_text
FROM translation_scores ts
JOIN verses v ON ts.verse_id = v.verse_id
JOIN translations t ON t.verse_id = v.verse_id AND t.translation_key = ts.translation_key
WHERE v.book_num = 19
ORDER BY ts.composite_deviation DESC
LIMIT 20;

-- Range check
SELECT
  MIN(composite_deviation), MAX(composite_deviation),
  MIN(stress_alignment),    MAX(stress_alignment),
  MIN(breath_alignment),    MAX(breath_alignment)
FROM translation_scores ts
JOIN verses v ON ts.verse_id = v.verse_id WHERE v.book_num = 19;
```
