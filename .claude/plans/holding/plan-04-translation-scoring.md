# Plan: Stage 4 — Translation Scoring

> **Depends on:** Stage 3 complete and verified — `syllable_tokens`, `breath_profiles`, and
> `translations` tables fully populated (2,527 verse rows, ~120,000 syllable token rows,
> 2,527 breath profiles).
> **Status:** active

## Goal

For every verse × translation pair, compute a weighted style-deviation vector and a
breath-alignment score against the Hebrew fingerprint and breath profile, then persist all
scores in `translation_scores`.

## Acceptance Criteria

- `translation_scores` contains rows for all verse × translation combinations (2,527 × 5 = 12,635
  rows for Psalms with 5 translations)
- All deviation columns are non-null and >= 0
- `composite_deviation` values fall in range 0.0–5.0 for realistic translations
- `stress_alignment` and `weight_match` in range [0.0, 1.0]
- `breath_alignment` in range [0.0, 1.0]
- YLT shows lower mean `composite_deviation` than UST (literal translations are closer to
  Hebrew structure)
- All 12 unit tests pass

## Architecture

`pipeline/adapters/phoneme_adapter.py` provides the CMU Pronouncing Dictionary lookup layer,
with a heuristic vowel-group counter as fallback for out-of-vocabulary words; it exposes
`english_fingerprint()` (returns a 4-dim dict) and `english_breath_weights()` (flat list of
per-syllable weights).  `pipeline/modules/score.py` loads Hebrew fingerprints and breath profiles
from the database, calls the phoneme adapter per translation text, computes per-dimension absolute
deviations and a weighted composite, computes breath alignment, and batch-upserts all results into
`translation_scores`.

## Tech Stack

- Python 3.11, uv, 88-char line limit, ruff enforced
- `pronouncing` library (CMU Pronouncing Dictionary, no API calls)
- `numpy` for vectorised weighted deviation
- `psycopg2` + `psycopg2.extras` for batch upsert
- `unittest.mock.MagicMock` for DB mocking in tests

---

## Tasks

### Task 1: phoneme_adapter — CMU lookup and syllable heuristic

**Files:**
- `tests/test_phoneme_adapter.py` (write first)
- `pipeline/adapters/phoneme_adapter.py`

---

**Steps:**

1. Write test:

```python
# tests/test_phoneme_adapter.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

import pytest
from adapters.phoneme_adapter import (
    get_syllables_and_stress,
    syllable_count,
    english_fingerprint,
    english_breath_weights,
    VOWEL_OPENNESS,
)


def test_cmu_lookup_the_lord() -> None:
    """'lord' is in CMU dict and returns at least one phoneme."""
    count, weights = get_syllables_and_stress("lord")
    assert count >= 1
    assert len(weights) >= 1
    assert all(0.0 < w <= 1.0 for w in weights)


def test_cmu_lookup_missing_word_heuristic() -> None:
    """A word absent from CMU falls back to the heuristic counter."""
    # 'xyzphrm' will never be in CMU; heuristic must return >= 1
    count, weights = get_syllables_and_stress("xyzphrm")
    assert count >= 1
    assert len(weights) == count


def test_syllable_count_shepherd() -> None:
    """'shepherd' has exactly 2 syllables."""
    assert syllable_count("shepherd") == 2


def test_syllable_count_heuristic_simple() -> None:
    """Single-syllable word 'cat' counts as 1 via heuristic."""
    # Import internal helper directly
    from adapters.phoneme_adapter import _heuristic_syllables

    count, weights = _heuristic_syllables("cat")
    assert count == 1
    assert len(weights) == 1


def test_stress_positions_detected() -> None:
    """'shepherd' has primary stress on the first syllable (index 0)."""
    count, weights = get_syllables_and_stress("shepherd")
    assert count == 2
    # Primary-stressed syllable carries more weight than secondary
    assert weights[0] > weights[1]


def test_vowel_openness_aa() -> None:
    """The 'AA' phoneme (e.g. 'father') maps to openness 1.0."""
    assert VOWEL_OPENNESS["AA"] == 1.00


def test_english_fingerprint_density_range() -> None:
    """syllable_density for a normal English sentence falls in [0.5, 3.0]."""
    fp = english_fingerprint(
        "The LORD is my shepherd; I shall not want."
    )
    assert 0.5 <= fp["syllable_density"] <= 3.0


def test_english_fingerprint_all_dimensions() -> None:
    """All four fingerprint dimensions are present and non-null/non-negative."""
    fp = english_fingerprint(
        "The LORD is my shepherd; I shall not want."
    )
    for key in (
        "syllable_density",
        "morpheme_ratio",
        "sonority_score",
        "clause_compression",
    ):
        assert key in fp
        assert fp[key] is not None
        assert fp[key] >= 0.0


def test_phoneme_adapter_empty_string() -> None:
    """Empty input returns a zero-dict (not None, not an exception)."""
    fp = english_fingerprint("")
    assert fp is not None
    for v in fp.values():
        assert v == 0.0
    weights = english_breath_weights("")
    assert weights == []
```

2. Run and confirm FAILED:

```bash
uv run --frozen pytest tests/test_phoneme_adapter.py -v
# Expected: FAILED (ImportError or ModuleNotFoundError) —
# pipeline/adapters/phoneme_adapter.py does not exist yet
```

3. Implement `pipeline/adapters/phoneme_adapter.py`:

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
from functools import lru_cache
from typing import Dict, List, Tuple

import pronouncing

logger = logging.getLogger(__name__)

# CMU ARPABET vowel codes → openness score (approximate IPA mapping).
# Stress digit stripped before lookup.
VOWEL_OPENNESS: Dict[str, float] = {
    # Open vowels (low, back)
    "AA": 1.00,  # father
    "AE": 0.85,  # cat
    "AH": 0.80,  # strut / schwa
    # Mid vowels
    "AO": 0.75,  # thought
    "AW": 0.72,  # mouth
    "AY": 0.78,  # price
    "EH": 0.65,  # dress
    "EY": 0.60,  # face
    "OW": 0.70,  # goat
    "OY": 0.68,  # choice
    # Close vowels (high, front/back)
    "IH": 0.45,  # kit
    "IY": 0.40,  # fleece
    "UH": 0.42,  # foot
    "UW": 0.38,  # goose
    # Reduced vowel
    "ER": 0.50,  # nurse
}
DEFAULT_VOWEL_OPENNESS: float = 0.55

# Simplified English onset sonority by first character
_ENG_SONORITY: Dict[str, float] = {
    "l": 0.90,
    "r": 0.85,
    "m": 0.80,
    "n": 0.78,
    "w": 0.75,
    "y": 0.70,
    "h": 0.65,
    "v": 0.60,
    "z": 0.55,
    "f": 0.50,
    "s": 0.48,
    "b": 0.35,
    "d": 0.30,
    "g": 0.25,
    "p": 0.20,
    "t": 0.18,
    "k": 0.15,
}


def get_syllables_and_stress(word: str) -> Tuple[int, List[float]]:
    """
    Return (syllable_count, stress_weights) for an English word.

    stress_weights is a list of per-syllable breath weights derived from
    vowel openness and CMU stress marks (0=unstressed, 1=primary,
    2=secondary).  Falls back to heuristic counter for unknown words.
    """
    word_clean = re.sub(r"[^\w]", "", word.lower())
    phones_list = pronouncing.phones_for_word(word_clean)
    if phones_list:
        return _parse_cmu_phones(phones_list[0])
    return _heuristic_syllables(word_clean)


def _parse_cmu_phones(phones_str: str) -> Tuple[int, List[float]]:
    """Parse a CMU phones string into syllable count and per-syllable weights."""
    tokens = phones_str.split()
    syllable_weights: List[float] = []

    for token in tokens:
        if token and token[-1].isdigit():
            stress = int(token[-1])
            vowel_code = token[:-1]
            openness = VOWEL_OPENNESS.get(vowel_code, DEFAULT_VOWEL_OPENNESS)
            stress_multiplier = {0: 0.7, 1: 1.0, 2: 0.85}.get(stress, 0.75)
            syllable_weights.append(round(openness * stress_multiplier, 4))

    if not syllable_weights:
        return 1, [DEFAULT_VOWEL_OPENNESS]

    return len(syllable_weights), syllable_weights


def _heuristic_syllables(word: str) -> Tuple[int, List[float]]:
    """
    Heuristic syllable counter for words not in the CMU dictionary.

    Rules: count vowel groups (aeiouy), adjusting for silent-e endings.
    """
    word = word.lower()
    if len(word) > 2 and word.endswith("e") and word[-2] not in "aeiouy":
        word = word[:-1]
    if len(word) > 3 and word.endswith("ed") and word[-3] not in "aeiouy":
        word = word[:-2]
    vowel_groups = re.findall(r"[aeiouy]+", word)
    count = max(1, len(vowel_groups))
    return count, [DEFAULT_VOWEL_OPENNESS] * count


@lru_cache(maxsize=10_000)
def syllable_count(word: str) -> int:
    """Return cached syllable count for a single word."""
    return get_syllables_and_stress(word)[0]


def english_fingerprint(text: str) -> Dict[str, float]:
    """
    Compute the 4-dimensional style fingerprint for an English verse.

    Returns a dict with keys: syllable_density, morpheme_ratio,
    sonority_score, clause_compression.
    """
    words = _tokenize(text)
    zero: Dict[str, float] = {
        "syllable_density": 0.0,
        "morpheme_ratio": 0.0,
        "sonority_score": 0.0,
        "clause_compression": 0.0,
    }
    if not words:
        return zero

    n = len(words)

    syl_counts = [syllable_count(w) for w in words]
    syllable_density = sum(syl_counts) / n

    # Morpheme ratio proxy: mean word character length / 4.0
    morpheme_ratio = min(sum(len(w) for w in words) / (n * 4.0), 4.0)

    sonority_values = [_english_onset_sonority(w) for w in words]
    sonority_score = sum(sonority_values) / len(sonority_values)

    clause_separators = text.count(",") + text.count(";") + 1
    clause_compression = n / clause_separators

    return {
        "syllable_density": round(syllable_density, 4),
        "morpheme_ratio": round(morpheme_ratio, 4),
        "sonority_score": round(sonority_score, 4),
        "clause_compression": round(clause_compression, 4),
    }


def english_breath_weights(text: str) -> List[float]:
    """
    Return a flat list of per-syllable breath weights for an English verse.

    Used by score.py for breath alignment scoring.
    """
    words = _tokenize(text)
    weights: List[float] = []
    for w in words:
        _, syl_weights = get_syllables_and_stress(w)
        weights.extend(syl_weights)
    return weights


def _tokenize(text: str) -> List[str]:
    """Split English verse text into alphabetic word tokens."""
    return [
        w
        for w in re.split(r"\s+", re.sub(r"[^\w\s]", " ", text))
        if w and w.isalpha()
    ]


def _english_onset_sonority(word: str) -> float:
    """Return approximate onset sonority for a word based on its first letter."""
    first = word[0].lower() if word else ""
    return _ENG_SONORITY.get(first, 0.30)
```

4. Run and confirm PASSED:

```bash
uv run --frozen pytest tests/test_phoneme_adapter.py -v
# Expected: 9 passed
```

5. Lint and typecheck:

```bash
uv run --frozen ruff check . --fix && uv run --frozen pyright
```

6. Commit: `"feat(stage4): add phoneme_adapter with CMU lookup and heuristic fallback"`

---

### Task 2: score module — deviation computation and breath alignment

**Files:**
- `tests/test_score.py` (write first)
- `pipeline/modules/score.py`

---

**Steps:**

1. Write test:

```python
# tests/test_score.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

from unittest.mock import MagicMock, patch, call
import pytest


# ── _compute_breath_alignment unit tests ────────────────────────────────────

def test_breath_alignment_range() -> None:
    """stress_alignment and weight_match must always be in [0.0, 1.0]."""
    from modules.score import _compute_breath_alignment

    heb_data = {
        "stress_positions": [0.25, 0.5, 0.75],
        "mean_weight": 0.60,
    }
    for text in [
        "The LORD is my shepherd; I shall not want.",
        "Praise the LORD all ye nations.",
        "He restores my soul.",
        "Yea though I walk through the valley of the shadow of death.",
    ]:
        align, match = _compute_breath_alignment(heb_data, text)
        assert 0.0 <= align <= 1.0, f"stress_alignment out of range for: {text!r}"
        assert 0.0 <= match <= 1.0, f"weight_match out of range for: {text!r}"


def test_breath_alignment_empty_stress_returns_midpoint() -> None:
    """Empty Hebrew stress list should return 0.5 midpoint for stress_alignment."""
    from modules.score import _compute_breath_alignment

    heb_data = {"stress_positions": [], "mean_weight": 0.5}
    align, match = _compute_breath_alignment(heb_data, "some text")
    assert align == 0.5


def test_breath_alignment_returns_floats() -> None:
    """Both return values must be Python floats."""
    from modules.score import _compute_breath_alignment

    heb_data = {"stress_positions": [0.3, 0.7], "mean_weight": 0.55}
    align, match = _compute_breath_alignment(
        heb_data, "The heavens declare the glory of God"
    )
    assert isinstance(align, float)
    assert isinstance(match, float)


# ── run() integration tests using mocked DB ─────────────────────────────────

def _make_config(translation_keys: list[str] | None = None) -> dict:
    keys = translation_keys or ["KJV", "YLT"]
    return {
        "corpus": {"books": [{"book_num": 19}], "debug_chapters": [23]},
        "translations": {
            "sources": [{"id": k} for k in keys]
        },
        "scoring": {
            "batch_size": 50,
            "deviation_weights": {
                "density": 0.35,
                "morpheme": 0.25,
                "sonority": 0.20,
                "compression": 0.20,
            },
            "breath_alignment_weights": {"stress": 0.60, "weight": 0.40},
        },
    }


def _make_conn_mock(heb_fp_rows: list, heb_breath_rows: list, trans_rows: list):
    """
    Build a MagicMock psycopg2 connection that returns preset rows for
    the three loader queries, in order.
    """
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    cursor.fetchall.side_effect = [heb_fp_rows, heb_breath_rows, trans_rows]
    cursor.fetchone.return_value = (0,)
    return conn, cursor


def test_score_idempotent() -> None:
    """
    Calling run() twice with the same data must not raise a UNIQUE violation.
    The ON CONFLICT DO UPDATE clause handles idempotency; we verify run()
    returns a positive scored count both times without raising.
    """
    from modules.score import run

    verse_id = 1
    heb_fp_rows = [(verse_id, 2.1, 1.8, 0.60, 3.5)]
    heb_breath_rows = [
        (verse_id, [0.6, 0.5, 0.7], [0.3, 0.7], 0.60)
    ]
    trans_rows = [
        (verse_id, "KJV", "The LORD is my shepherd; I shall not want."),
        (verse_id, "YLT", "Jehovah is my shepherd, I do not lack."),
    ]

    config = _make_config(["KJV", "YLT"])

    for _ in range(2):
        conn, cursor = _make_conn_mock(heb_fp_rows, heb_breath_rows, trans_rows)
        # Patch batch_upsert so we don't need a real DB
        with patch("modules.score.batch_upsert") as mock_upsert:
            result = run(conn, config)
        assert result.get("scored", 0) == 2  # 1 verse × 2 translations
        mock_upsert.assert_called_once()


def test_deviation_ylt_lt_ust() -> None:
    """
    YLT (literal) should produce a lower composite_deviation than UST
    (simplification) for the same Hebrew verse.  We test the scoring math
    directly by computing fingerprints for representative texts.
    """
    from adapters.phoneme_adapter import english_fingerprint
    import numpy as np

    # Psalm 23:1 — YLT is more word-for-word; UST is dynamic-equivalent
    ylt_text = "Jehovah is my shepherd, I do not lack."
    ust_text = (
        "God is my caretaker. He provides for everything I need."
    )

    heb_fp = {
        "syllable_density": 2.1,
        "morpheme_ratio": 1.80,
        "sonority_score": 0.60,
        "clause_compression": 3.0,
    }
    weights = np.array([0.35, 0.25, 0.20, 0.20])

    def composite(text: str) -> float:
        eng = english_fingerprint(text)
        heb_vec = np.array(list(heb_fp.values()))
        eng_vec = np.array(
            [eng["syllable_density"], eng["morpheme_ratio"],
             eng["sonority_score"], eng["clause_compression"]]
        )
        return float(np.dot(weights, np.abs(heb_vec - eng_vec)))

    ylt_dev = composite(ylt_text)
    ust_dev = composite(ust_text)
    assert ylt_dev < ust_dev, (
        f"Expected YLT ({ylt_dev:.4f}) < UST ({ust_dev:.4f})"
    )
```

2. Run and confirm FAILED:

```bash
uv run --frozen pytest tests/test_score.py -v
# Expected: FAILED (ImportError) — pipeline/modules/score.py does not exist yet
```

3. Implement `pipeline/modules/score.py`:

```python
"""
Stage 4 — Translation scoring.

For each verse × translation pair:
  1. Compute English style fingerprint via phoneme_adapter
  2. Compute weighted Euclidean deviation from the Hebrew fingerprint
  3. Compute breath alignment against the Hebrew breath curve
  4. Batch-upsert results into translation_scores
"""

from __future__ import annotations

import logging
from typing import Dict, List, Tuple

import numpy as np
import psycopg2
import psycopg2.extras

from adapters.db_adapter import batch_upsert
from adapters.phoneme_adapter import english_breath_weights, english_fingerprint

logger = logging.getLogger(__name__)


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    """
    Score all verse × translation pairs and persist results.

    Returns {"scored": int, "elapsed_s": float}.
    """
    import time

    t0 = time.monotonic()

    corpus = config.get("corpus", {})
    book_nums: List[int] = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters: List[int] = corpus.get("debug_chapters", [])
    batch_size: int = config.get("scoring", {}).get("batch_size", 100)

    w = config.get("scoring", {}).get("deviation_weights", {})
    dev_weights = np.array(
        [
            w.get("density", 0.35),
            w.get("morpheme", 0.25),
            w.get("sonority", 0.20),
            w.get("compression", 0.20),
        ]
    )

    bw = config.get("scoring", {}).get("breath_alignment_weights", {})
    stress_weight: float = bw.get("stress", 0.60)
    wm_weight: float = bw.get("weight", 0.40)

    sources = config.get("translations", {}).get("sources", [])
    translation_keys: List[str] = [s["id"] for s in sources]

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

    score_rows: list = []
    for verse_id, heb_fp in heb_fps.items():
        heb_vec = np.array(
            [
                heb_fp["syllable_density"],
                heb_fp["morpheme_ratio"],
                heb_fp["sonority_score"],
                heb_fp["clause_compression"],
            ]
        )
        heb_curve = heb_breath.get(verse_id, {})

        for key in translation_keys:
            text = trans_texts.get((verse_id, key))
            if not text:
                continue

            eng_fp = english_fingerprint(text)
            eng_vec = np.array(
                [
                    eng_fp["syllable_density"],
                    eng_fp["morpheme_ratio"],
                    eng_fp["sonority_score"],
                    eng_fp["clause_compression"],
                ]
            )

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

            score_rows.append(
                (
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
                )
            )

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
    logger.info("Scored %d verse × translation pairs in %.2fs", len(score_rows), elapsed)
    return {"scored": len(score_rows), "elapsed_s": elapsed}


def _load_hebrew_fingerprints(
    conn: psycopg2.extensions.connection,
    book_nums: List[int],
    debug_chapters: List[int],
) -> Dict[int, Dict[str, float]]:
    """Load verse_fingerprints rows for the configured corpus."""
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
                "morpheme_ratio": float(row[2] or 0),
                "sonority_score": float(row[3] or 0),
                "clause_compression": float(row[4] or 0),
            }
            for row in cur.fetchall()
        }


def _load_hebrew_breath(
    conn: psycopg2.extensions.connection,
    book_nums: List[int],
    debug_chapters: List[int],
) -> Dict[int, Dict]:
    """Load breath_profiles rows for the configured corpus."""
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
                "breath_curve": row[1] or [],
                "stress_positions": row[2] or [],
                "mean_weight": float(row[3] or 0),
            }
            for row in cur.fetchall()
        }


def _load_translation_texts(
    conn: psycopg2.extensions.connection,
    translation_keys: List[str],
    book_nums: List[int],
    debug_chapters: List[int],
) -> Dict[Tuple[int, str], str]:
    """Load translations rows for the configured corpus and translation keys."""
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
) -> Tuple[float, float]:
    """
    Compute (stress_alignment, weight_match) for a verse × translation pair.

    stress_alignment: 1 minus mean minimum distance between Hebrew and English
    stressed syllable positions (both normalized 0–1).

    weight_match: similarity between mean Hebrew breath weight and mean English
    syllable weight, scaled so a difference of 0.5 maps to 0.0.
    """
    heb_stress: List[float] = heb_data.get("stress_positions", [])
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
```

4. Run and confirm PASSED:

```bash
uv run --frozen pytest tests/test_score.py -v
# Expected: 5 passed
```

5. Run full test suite for both new test files:

```bash
uv run --frozen pytest tests/test_phoneme_adapter.py tests/test_score.py -v
# Expected: 14 passed total (9 + 5)
```

6. Lint and typecheck:

```bash
uv run --frozen ruff check . --fix && uv run --frozen pyright
```

7. Commit: `"feat(stage4): add score module with deviation and breath alignment computation"`

---

### Task 3: verify stage acceptance criteria

**Files:** no new files — SQL queries only

**Steps:**

1. Start the pipeline container and run Stage 4:

```bash
docker compose --profile pipeline run --rm pipeline \
  python -m pipeline.run --stages 4
```

2. Verify row counts via SQL:

```sql
-- Score coverage: expect 2,527 rows per translation
SELECT translation_key, COUNT(*) AS scored_verses
FROM translation_scores ts
JOIN verses v ON ts.verse_id = v.verse_id
WHERE v.book_num = 19
GROUP BY translation_key
ORDER BY translation_key;

-- Range check: all values must be in [0.0, 1.0] for alignment columns
SELECT
  MIN(composite_deviation),  MAX(composite_deviation),
  MIN(stress_alignment),     MAX(stress_alignment),
  MIN(breath_alignment),     MAX(breath_alignment)
FROM translation_scores ts
JOIN verses v ON ts.verse_id = v.verse_id
WHERE v.book_num = 19;

-- Literal translations should rank lower (better) on composite_deviation
SELECT
  translation_key,
  ROUND(AVG(composite_deviation)::numeric, 4) AS mean_dev,
  ROUND(AVG(breath_alignment)::numeric, 4)    AS mean_breath_align
FROM translation_scores ts
JOIN verses v ON ts.verse_id = v.verse_id
WHERE v.book_num = 19
GROUP BY translation_key
ORDER BY mean_dev;
-- Expected order (roughly): YLT < ULT < KJV < WEB < UST
```

3. Confirm all unit tests still pass after full pipeline run:

```bash
uv run --frozen pytest tests/test_phoneme_adapter.py tests/test_score.py -v
```

4. Commit: `"chore(stage4): verified acceptance criteria — 12635 score rows written"`

---

## SQL Validation Reference

```sql
-- Top 20 highest-deviation verses (useful for Stage 5 seeding)
SELECT
  v.chapter, v.verse_num, ts.translation_key,
  ts.composite_deviation, ts.breath_alignment,
  t.verse_text
FROM translation_scores ts
JOIN verses v ON ts.verse_id = v.verse_id
JOIN translations t
  ON t.verse_id = v.verse_id AND t.translation_key = ts.translation_key
WHERE v.book_num = 19
ORDER BY ts.composite_deviation DESC
LIMIT 20;
```
