# Stage 3 — Breath & Phonetic Analysis
## Detailed Implementation Plan

> **Depends on:** Stage 2 (`verses` and `word_tokens` populated)  
> **Produces:** `syllable_tokens` (~120,000 rows), `breath_profiles` (2,527 rows), colon boundaries stored; triggers Stage 2 second pass for chiasm detection  
> **Estimated time:** 20–60 minutes (Psalms corpus)

---

## Objectives

1. Parse every Hebrew word into syllables with vowel and consonant classification
2. Compute composite breath weight per syllable
3. Identify colon boundaries from Masoretic disjunctive accent positions
4. Store verse-level breath profiles including full breath curves
5. Back-populate colon-level fingerprint arrays on `verse_fingerprints` (enables Stage 2 chiasm pass)

---

## Theoretical Background

### Hebrew Syllable Structure

A Hebrew syllable (with Masoretic vocalization) is a consonant-vowel or consonant-vowel-consonant unit. The Masoretic niqqud system provides explicit vowel notation, making syllable parsing deterministic for pointed text.

Syllable types:
- **Open** (CV): consonant + non-shewa vowel; e.g., דָּ in דָּוִד
- **Closed** (CVC): consonant + vowel + consonant; e.g., דָּוִד's final syllable דְ (with following consonant)
- **Shewa syllable**: consonant + shewa (half-syllable, minimal weight)

### Breath Weight Composite

Each syllable receives a composite weight on a 0–1 scale from four acoustic properties:

| Component | Weight | What it measures |
|---|---|---|
| Vowel openness | 40% | How open/low is the vowel (a > e/o > i/u > shewa) |
| Vowel length | 25% | Long vs short vs ultra-short vs shewa |
| Syllable openness | 20% | Open CV > closed CVC |
| Onset class | 15% | Resonants/gutturals are more "open" than stops |

### Colon Boundaries

Masoretic accents include a set of **disjunctive** accents that mark phrase and clause boundaries. The major disjunctives (atnah, silluq, zaqef qaton, zaqef gadol, tifha when final, shalshelet) indicate colon boundaries. The BHSA stores accent data, allowing automatic extraction without manual annotation.

---

## File Structure for This Stage

```
pipeline/
  modules/
    breath.py              ← syllable parser + breath profile computation
  tests/
    test_breath.py
```

---

## Step 1 — File: `modules/breath.py`

```python
"""
Stage 3 — Breath and phonetic analysis.

Parses Hebrew words into syllables, assigns breath weights,
detects colon boundaries from Masoretic accents, and stores
verse-level breath profiles.

After this module runs, back-populates colon_fingerprints
on verse_fingerprints (required for Stage 2 chiasm detection).
"""

from __future__ import annotations

import json
import logging
import sys
from typing import List, Dict, Tuple, Optional

import numpy as np
import psycopg2
import psycopg2.extras

sys.path.insert(0, "/pipeline")
from adapters.db_adapter import verse_ids_for_stage, batch_upsert

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# Unicode character sets
# ─────────────────────────────────────────────────────────────────

# Full vowel points (qamets, patah, tsere, segol, hiriq, holam, shuruq, qibbuts)
FULL_VOWELS = {
    "\u05B7": ("patah",    "short",       0.90),   # patah  ַ
    "\u05B8": ("qamets",   "long",        1.00),   # qamets ָ
    "\u05B5": ("tsere",    "long",        0.70),   # tsere  ֵ
    "\u05B6": ("segol",    "short",       0.65),   # segol  ֶ
    "\u05B4": ("hiriq",    "short",       0.50),   # hiriq  ִ
    "\u05BC": ("dagesh",   None,          None),   # dagesh — not a vowel
    "\u05B9": ("holam",    "long",        0.75),   # holam  ֹ
    "\u05BA": ("holam_waw","long",        0.75),   # holam with waw
    "\u05BB": ("shuruq",   "long",        0.55),   # shuruq ּ (waw + dagesh)
    "\u05C1": ("shin_dot", None,          None),   # shin dot
    "\u05C2": ("sin_dot",  None,          None),   # sin dot
}

# Half-vowels (shewa and hataf forms)
HALF_VOWELS = {
    "\u05B0": ("shewa",       "ultra-short", 0.10),
    "\u05B1": ("hataf_segol", "ultra-short", 0.30),
    "\u05B2": ("hataf_patah", "ultra-short", 0.35),
    "\u05B3": ("hataf_qamets","ultra-short", 0.40),
}

ALL_VOWELS = {**FULL_VOWELS, **HALF_VOWELS}

# Consonants
CONSONANTS = frozenset("אבגדהוזחטיכלמנסעפצקרשת" + "ךםןףץ")

# Consonant onset classes
ONSET_CLASS = {
    # Gutturals — breathy, affect following vowel
    "א": ("guttural", 0.65),
    "ה": ("guttural", 0.70),
    "ח": ("guttural", 0.55),
    "ע": ("guttural", 0.60),
    # Sibilants
    "ש": ("sibilant", 0.40),
    "שׁ": ("sibilant", 0.40),
    "שׂ": ("sibilant", 0.40),
    "ס": ("sibilant", 0.40),
    "ז": ("sibilant", 0.45),
    "צ": ("sibilant", 0.35),
    # Liquids — most sonorous consonants
    "ל": ("liquid",   0.90),
    "ר": ("liquid",   0.85),
    # Nasals
    "מ": ("nasal",    0.80),
    "נ": ("nasal",    0.80),
    # Approximants
    "י": ("liquid",   0.75),
    "ו": ("liquid",   0.70),
    # Voiced stops
    "ב": ("stop",     0.30),
    "ג": ("stop",     0.30),
    "ד": ("stop",     0.30),
    # Voiceless stops
    "כ": ("stop",     0.20),
    "ך": ("stop",     0.20),
    "פ": ("stop",     0.20),
    "ף": ("stop",     0.20),
    "ת": ("stop",     0.20),
    "ק": ("stop",     0.15),
    "ט": ("stop",     0.20),
}
DEFAULT_ONSET = ("stop", 0.25)

# Disjunctive accent Unicode points (Masoretic — colon boundary indicators)
# Reference: BHSA documentation and BHS accent system
DISJUNCTIVE_ACCENTS = frozenset({
    "\u0591",  # etnahta (atnah) — major pause, always colon boundary
    "\u05C3",  # silluq (verse end, always boundary)
    "\u0592",  # segol accent (zaqef gadol)
    "\u0593",  # shalshelet
    "\u0594",  # zaqef qaton
    "\u0595",  # zaqef gadol
    "\u0596",  # tifha (when on penultimate word)
    "\u059A",  # yetiv
    "\u059C",  # geresh
    "\u059D",  # geresh muqdam
    "\u059E",  # gershayim
    "\u05A1",  # pazer
    "\u05A8",  # qadma
    "\u05A9",  # telisha qetana
    "\u05AC",  # ole
    "\u05AE",  # zinor
})

# ─────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────

def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    """
    Process all pending verses: parse syllables, compute breath profiles.
    Then back-populate colon fingerprints for chiasm support.
    """
    corpus = config.get("corpus", {})
    book_nums = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters = corpus.get("debug_chapters", [])
    batch_size = config.get("breath", {}).get("batch_size", 100)

    pending = verse_ids_for_stage(conn, "breath_profiles", book_nums, debug_chapters)
    if not pending:
        logger.info("All breath profiles already computed.")
        _backpopulate_colon_fingerprints(conn, book_nums)
        return {"breath_profiles": 0}

    logger.info(f"Computing breath profiles for {len(pending)} verses")

    # Fetch token data for pending verses
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT wt.verse_id, wt.token_id, wt.position,
                   wt.surface_form, wt.part_of_speech
            FROM word_tokens wt
            WHERE wt.verse_id = ANY(%s)
            ORDER BY wt.verse_id, wt.position
            """,
            (pending,)
        )
        token_rows = cur.fetchall()

    from collections import defaultdict
    by_verse: Dict[int, list] = defaultdict(list)
    for row in token_rows:
        by_verse[row[0]].append(row)

    syllable_rows_all = []
    profile_rows = []

    for verse_id in pending:
        tokens = by_verse.get(verse_id, [])
        if not tokens:
            continue

        syllable_data, profile = _process_verse(tokens)
        syllable_rows_all.extend(
            (verse_id,) + s for s in syllable_data
        )
        profile_rows.append((verse_id,) + profile)

    # Store syllable tokens
    batch_upsert(
        conn,
        """
        INSERT INTO syllable_tokens
          (verse_id, token_id, syllable_index, syllable_text, nucleus_vowel,
           vowel_openness, vowel_length, is_open, onset_class, breath_weight,
           stress_position, colon_index)
        VALUES %s
        ON CONFLICT (token_id, syllable_index) DO NOTHING
        """,
        syllable_rows_all,
        batch_size=batch_size,
    )

    # Store breath profiles
    batch_upsert(
        conn,
        """
        INSERT INTO breath_profiles
          (verse_id, mean_weight, open_ratio, guttural_density,
           colon_count, colon_boundaries, stress_positions, breath_curve)
        VALUES %s
        ON CONFLICT (verse_id) DO UPDATE
          SET mean_weight       = EXCLUDED.mean_weight,
              open_ratio        = EXCLUDED.open_ratio,
              guttural_density  = EXCLUDED.guttural_density,
              colon_count       = EXCLUDED.colon_count,
              colon_boundaries  = EXCLUDED.colon_boundaries,
              stress_positions  = EXCLUDED.stress_positions,
              breath_curve      = EXCLUDED.breath_curve,
              computed_at       = NOW()
        """,
        profile_rows,
        batch_size=batch_size,
    )

    # Update verse colon counts
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE verses v
            SET colon_count = bp.colon_count
            FROM breath_profiles bp
            WHERE v.verse_id = bp.verse_id
              AND v.verse_id = ANY(%s)
            """,
            (pending,)
        )
    conn.commit()

    logger.info(f"Breath profiles stored: {len(profile_rows)}")
    logger.info(f"Syllable tokens stored: {len(syllable_rows_all)}")

    # Back-populate colon fingerprints for chiasm
    _backpopulate_colon_fingerprints(conn, book_nums)

    return {
        "breath_profiles": len(profile_rows),
        "syllable_tokens": len(syllable_rows_all),
    }


# ─────────────────────────────────────────────────────────────────
# Verse processing
# ─────────────────────────────────────────────────────────────────

def _process_verse(tokens: list) -> Tuple[list, tuple]:
    """
    Process all tokens in a verse.

    Returns:
        syllable_data: list of tuples for syllable_tokens insert
        profile: tuple for breath_profiles insert
    """
    all_syllables = []   # flat list of syllable dicts across all tokens
    colon_boundaries = [0]   # syllable index where each colon starts
    current_colon = 1

    for token_row in tokens:
        verse_id, token_id, position, surface_form, pos_tag = token_row

        # Check for disjunctive accent in surface form
        has_disjunctive = any(c in DISJUNCTIVE_ACCENTS for c in surface_form)

        syls = parse_syllables(surface_form)

        for syl_idx, syl in enumerate(syls):
            syl["token_id"] = token_id
            syl["verse_id"] = verse_id
            syl["colon_index"] = current_colon
            syl["global_idx"] = len(all_syllables)
            all_syllables.append(syl)

        if has_disjunctive and position < len(tokens):
            current_colon += 1
            colon_boundaries.append(len(all_syllables))

    if not all_syllables:
        # Empty token list — return minimal profile
        return [], (0.0, 0.0, 0.0, 1, [], [], [])

    # Compute normalized stress positions (0–1 scale across verse)
    n_syls = len(all_syllables)
    stress_positions = []
    for i, syl in enumerate(all_syllables):
        if syl.get("is_stressed"):
            stress_positions.append(round(i / max(n_syls - 1, 1), 4))

    # Breath curve
    breath_curve = [round(s["breath_weight"], 4) for s in all_syllables]

    # Summary statistics
    weights = [s["breath_weight"] for s in all_syllables]
    mean_weight = round(sum(weights) / len(weights), 4)
    open_ratio  = round(sum(1 for s in all_syllables if s.get("is_open")) / n_syls, 4)
    guttural_density = round(
        sum(1 for s in all_syllables if s.get("onset_class") == "guttural") / n_syls, 4
    )
    colon_count = current_colon

    # Build syllable_tokens rows
    syllable_rows = []
    for syl in all_syllables:
        syllable_rows.append((
            syl["token_id"],
            syl["syl_idx"],
            syl["text"],
            syl.get("nucleus_vowel"),
            syl.get("vowel_openness"),
            syl.get("vowel_length"),
            syl.get("is_open"),
            syl.get("onset_class"),
            syl["breath_weight"],
            round(syl["global_idx"] / max(n_syls - 1, 1), 4),  # stress_position
            syl["colon_index"],
        ))

    profile = (
        mean_weight,
        open_ratio,
        guttural_density,
        colon_count,
        colon_boundaries,
        stress_positions,
        breath_curve,
    )
    return syllable_rows, profile


# ─────────────────────────────────────────────────────────────────
# Syllable parser
# ─────────────────────────────────────────────────────────────────

def parse_syllables(word: str) -> List[dict]:
    """
    Parse a Hebrew word (with niqqud) into syllable structures.

    Each syllable dict contains:
        syl_idx, text, nucleus_vowel, vowel_openness, vowel_length,
        is_open, onset_class, breath_weight, is_stressed
    """
    syllables = []
    chars = list(word)
    i = 0
    syl_idx = 0

    while i < len(chars):
        ch = chars[i]

        # Skip non-consonant, non-vowel characters (accents, etc.)
        if ch not in CONSONANTS and ch not in ALL_VOWELS:
            i += 1
            continue

        if ch not in CONSONANTS:
            i += 1
            continue

        # Start of a syllable: onset consonant
        onset_char = ch
        onset_class, onset_weight = ONSET_CLASS.get(onset_char, DEFAULT_ONSET)
        syl_text = ch
        i += 1

        # Collect vowel (may be preceded by dagesh)
        nucleus_vowel = None
        vowel_name = None
        vowel_openness = 0.0
        vowel_length = "ultra-short"

        # Skip dagesh / shin dot immediately after onset
        while i < len(chars) and chars[i] in ("\u05BC", "\u05C1", "\u05C2", "\u05BD"):
            syl_text += chars[i]
            i += 1

        if i < len(chars) and chars[i] in ALL_VOWELS:
            vowel_ch = chars[i]
            syl_text += vowel_ch
            vdata = ALL_VOWELS.get(vowel_ch)
            if vdata and vdata[2] is not None:
                vowel_name, vowel_length, vowel_openness = vdata
                nucleus_vowel = vowel_ch
            i += 1

        # Skip any following marks (holam, shin points, etc.)
        while i < len(chars) and chars[i] not in CONSONANTS and chars[i] not in DISJUNCTIVE_ACCENTS:
            if chars[i] in ALL_VOWELS:
                # Second vowel indicator (e.g., holam waw)
                vdata = ALL_VOWELS.get(chars[i])
                if vdata and vdata[2] is not None and vowel_openness == 0.0:
                    vowel_name, vowel_length, vowel_openness = vdata
                    nucleus_vowel = chars[i]
            syl_text += chars[i]
            i += 1

        # Determine syllable openness
        is_open = (vowel_length in ("long", "short")) and (
            i >= len(chars) or chars[i] in CONSONANTS
            # closed when next character starts a new syllable with shewa
        )

        # Compute composite breath weight
        vowel_component   = vowel_openness * 0.40
        length_component  = _vowel_length_score(vowel_length) * 0.25
        open_component    = (1.0 if is_open else 0.4) * 0.20
        onset_component   = onset_weight * 0.15

        breath_weight = round(
            vowel_component + length_component + open_component + onset_component, 4
        )

        syllables.append({
            "syl_idx":       syl_idx,
            "text":          syl_text,
            "nucleus_vowel": nucleus_vowel,
            "vowel_openness": vowel_openness,
            "vowel_length":  vowel_length,
            "is_open":       is_open,
            "onset_class":   onset_class,
            "breath_weight": breath_weight,
            "is_stressed":   vowel_length == "long",  # long vowels carry stress
        })
        syl_idx += 1

    # Minimum: return at least one syllable stub for single-letter words
    if not syllables:
        syllables.append({
            "syl_idx": 0, "text": word, "nucleus_vowel": None,
            "vowel_openness": 0.3, "vowel_length": "short",
            "is_open": True, "onset_class": "stop",
            "breath_weight": 0.25, "is_stressed": False,
        })

    return syllables


def _vowel_length_score(length: Optional[str]) -> float:
    """Normalized score for vowel length (0–1)."""
    return {"long": 1.0, "short": 0.65, "ultra-short": 0.25, "shewa": 0.10}.get(length or "short", 0.5)


# ─────────────────────────────────────────────────────────────────
# Back-populate colon fingerprints for chiasm detection
# ─────────────────────────────────────────────────────────────────

def _backpopulate_colon_fingerprints(
    conn: psycopg2.extensions.connection,
    book_nums: List[int]
) -> None:
    """
    Compute colon-level style fingerprints from syllable_tokens data
    and store them in verse_fingerprints.colon_fingerprints (JSONB).

    Called at end of Stage 3. Required before chiasm detection can run.
    """
    logger.info("Back-populating colon fingerprints...")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT st.verse_id, st.colon_index,
                   AVG(st.vowel_openness) AS mean_openness,
                   AVG(st.breath_weight)  AS mean_weight,
                   COUNT(*)               AS syllable_count
            FROM syllable_tokens st
            JOIN verses v ON st.verse_id = v.verse_id
            WHERE v.book_num = ANY(%s)
            GROUP BY st.verse_id, st.colon_index
            ORDER BY st.verse_id, st.colon_index
            """,
            (book_nums,)
        )
        rows = cur.fetchall()

    from collections import defaultdict
    by_verse: Dict[int, list] = defaultdict(list)
    for verse_id, colon_idx, mean_openness, mean_weight, syl_count in rows:
        by_verse[verse_id].append({
            "colon":       colon_idx,
            "density":     round(float(syl_count), 2),
            "sonority":    round(float(mean_openness or 0), 4),
            "mean_weight": round(float(mean_weight or 0), 4),
        })

    update_rows = [
        (json.dumps(colons), verse_id)
        for verse_id, colons in by_verse.items()
    ]

    if not update_rows:
        logger.info("No colon data to back-populate.")
        return

    with conn.cursor() as cur:
        psycopg2.extras.executemany(
            """
            UPDATE verse_fingerprints
            SET colon_fingerprints = %s
            WHERE verse_id = %s
            """,
            update_rows,
        )
    conn.commit()
    logger.info(f"Colon fingerprints back-populated for {len(update_rows)} verses")
```

---

## Step 2 — Test Cases

Save as `tests/test_breath.py`:

```python
"""Tests for Stage 3 breath analysis module."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from modules.breath import (
    parse_syllables,
    _vowel_length_score,
    FULL_VOWELS,
    HALF_VOWELS,
    DISJUNCTIVE_ACCENTS,
)


class TestParseSyllables:

    def test_simple_two_syllable_word(self):
        # דָּוִד (David) — two syllables: דָּ + וִד
        word = "דָּוִד"
        syls = parse_syllables(word)
        assert len(syls) >= 1   # must have at least 1
        for s in syls:
            assert "breath_weight" in s
            assert 0.0 <= s["breath_weight"] <= 1.0

    def test_single_consonant_returns_one_syllable(self):
        word = "כ"
        syls = parse_syllables(word)
        assert len(syls) == 1

    def test_vowel_openness_assignment(self):
        # Word with qamets (openness 1.0)
        word = "בָּ"
        syls = parse_syllables(word)
        assert syls[0]["vowel_openness"] == 1.0

    def test_shewa_gets_low_weight(self):
        # Word beginning with shewa (half-vowel)
        word = "בְּ"
        syls = parse_syllables(word)
        # breath weight should be low due to ultra-short vowel
        assert syls[0]["breath_weight"] < 0.5

    def test_guttural_onset_detected(self):
        # Aleph (א) is guttural
        word = "אָב"
        syls = parse_syllables(word)
        assert syls[0]["onset_class"] == "guttural"

    def test_liquid_onset_detected(self):
        # Lamed (ל) is liquid
        word = "לֵב"
        syls = parse_syllables(word)
        assert syls[0]["onset_class"] == "liquid"

    def test_breath_weight_in_range(self):
        words = ["הַ", "בְּ", "רָ", "שָׁלוֹם", "אֱלֹהִים"]
        for w in words:
            for syl in parse_syllables(w):
                assert 0.0 <= syl["breath_weight"] <= 1.0, f"Out of range for word '{w}'"

    def test_returns_nonempty_for_any_consonantal_text(self):
        # Unvocalized consonants
        word = "שלום"
        syls = parse_syllables(word)
        assert len(syls) >= 1

    def test_all_required_keys_present(self):
        word = "אֱלֹהִים"
        syls = parse_syllables(word)
        required_keys = {
            "syl_idx", "text", "nucleus_vowel", "vowel_openness",
            "vowel_length", "is_open", "onset_class", "breath_weight", "is_stressed"
        }
        for syl in syls:
            assert required_keys.issubset(syl.keys()), f"Missing keys: {required_keys - syl.keys()}"


class TestVowelLengthScore:

    def test_long_is_highest(self):
        assert _vowel_length_score("long") == 1.0

    def test_short_is_medium(self):
        assert 0.5 < _vowel_length_score("short") < 1.0

    def test_ultra_short_is_low(self):
        assert _vowel_length_score("ultra-short") < 0.5

    def test_shewa_is_lowest(self):
        assert _vowel_length_score("shewa") <= _vowel_length_score("ultra-short")

    def test_unknown_returns_midrange(self):
        assert 0.0 < _vowel_length_score(None) < 1.0


class TestDisjunctiveAccents:

    def test_etnahta_in_set(self):
        # Etnahta (U+0591) is always a colon boundary
        assert "\u0591" in DISJUNCTIVE_ACCENTS

    def test_silluq_in_set(self):
        assert "\u05C3" in DISJUNCTIVE_ACCENTS

    def test_set_is_nonempty(self):
        assert len(DISJUNCTIVE_ACCENTS) >= 8
```

Run:
```bash
docker compose --profile pipeline run --rm pipeline python -m pytest /pipeline/tests/test_breath.py -v
```

Expected: all tests pass.

---

## Acceptance Criteria

- [ ] `syllable_tokens` contains ~120,000 rows for Psalms (range 100,000–140,000 is acceptable)
- [ ] `breath_profiles` contains exactly one row per verse (2,527 for Psalms)
- [ ] All `breath_weight` values in `syllable_tokens` are in range [0.0, 1.0]
- [ ] All `mean_weight`, `open_ratio`, `guttural_density` in range [0.0, 1.0]
- [ ] `colon_count` is ≥1 for every verse; median should be ~2–4 for Psalms poetry
- [ ] `colon_fingerprints` in `verse_fingerprints` is non-null for all verses after back-population
- [ ] All unit tests pass
- [ ] Stage is resumable: re-running inserts 0 new rows

---

## SQL Validation Queries

```sql
-- Syllable token count
SELECT COUNT(*) FROM syllable_tokens st
JOIN verses v ON st.verse_id = v.verse_id WHERE v.book_num = 19;
-- Expected: ~100,000–140,000

-- Breath profile coverage
SELECT COUNT(*) FROM breath_profiles bp
JOIN verses v ON bp.verse_id = v.verse_id WHERE v.book_num = 19;
-- Expected: 2527

-- Colon distribution for Psalms
SELECT colon_count, COUNT(*) as verse_count
FROM breath_profiles bp
JOIN verses v ON bp.verse_id = v.verse_id
WHERE v.book_num = 19
GROUP BY colon_count ORDER BY colon_count;
-- Most verses should have 2–4 colons

-- Breath weight range check
SELECT
  MIN(breath_weight) as min_w,
  MAX(breath_weight) as max_w,
  AVG(breath_weight) as avg_w
FROM syllable_tokens st
JOIN verses v ON st.verse_id = v.verse_id WHERE v.book_num = 19;
-- Expected: min ~0.10, max ~1.0, avg ~0.50–0.65

-- Onset class distribution
SELECT onset_class, COUNT(*) FROM syllable_tokens st
JOIN verses v ON st.verse_id = v.verse_id WHERE v.book_num = 19
GROUP BY onset_class ORDER BY COUNT(*) DESC;

-- Psalm 23:1 breath curve sample
SELECT s.syllable_index, s.syllable_text, s.breath_weight, s.onset_class, s.colon_index
FROM syllable_tokens s
JOIN word_tokens wt ON s.token_id = wt.token_id
JOIN verses v ON s.verse_id = v.verse_id
WHERE v.book_num = 19 AND v.chapter = 23 AND v.verse_num = 1
ORDER BY wt.position, s.syllable_index;
```

---

## Notes on Accent Coverage

The BHSA corpus contains full Masoretic accent data for all pointed books. Psalms, Job, and Proverbs use the **poetic accent system** (ta'amei ha-miqra poetical) which differs from the prose system. The key distinction is that the etnahta (atnah) functions the same in both systems as the primary verse-medial pause. The poetic books also have additional accents (shalshelet, sinnor) that are handled in the `DISJUNCTIVE_ACCENTS` set.

The colon boundary detection is deliberately broad — all major disjunctive accents are counted. This may over-segment some verses with multiple mid-verse pauses. The `chiasm.py` threshold configuration can compensate for this by requiring stronger similarity for pattern matching when colon counts are high.
