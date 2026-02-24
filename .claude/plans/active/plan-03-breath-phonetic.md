# Plan: Stage 3 — Breath & Phonetic Analysis

> **Depends on:** Stage 2 complete (`verses` table has 2,527 rows,
> `word_tokens` table has ~43,000 rows, `verse_fingerprints` has 2,527 rows
> with all four scalar dimensions populated)
> **Status:** active

## Goal

Parse every Hebrew word in the Psalms corpus into syllables with vowel and
consonant classification, assign composite breath weights to each syllable,
detect colon boundaries from Masoretic disjunctive accent positions, store
verse-level breath profiles, and back-populate `verse_fingerprints.colon_fingerprints`
to enable the Stage 2 chiasm detection second pass.

## Acceptance Criteria

- `syllable_tokens` contains approximately 120,000 rows for Psalms (range
  100,000–140,000 is acceptable)
- `breath_profiles` contains exactly one row per verse (2,527 rows for Psalms)
- All `breath_weight` values in `syllable_tokens` are in range [0.0, 1.0]
- All `mean_weight`, `open_ratio`, and `guttural_density` values in
  `breath_profiles` are in range [0.0, 1.0]
- `colon_count` is >= 1 for every verse; median across Psalms should be 2–4
- `verse_fingerprints.colon_fingerprints` is non-null and a non-empty JSON
  array for all 2,527 Psalms verses after back-population
- All unit tests in `tests/test_breath.py` pass
- Running Stage 3 a second time inserts 0 new `breath_profiles` rows
  (fully idempotent)
- After Stage 3 completes: triggering chiasm.run() (Plan 02 deferred task)
  produces at least some `chiasm_candidates` rows

## Architecture

A single module `modules/breath.py` drives this entire stage. It reads
`word_tokens.surface_form` directly from the database (no BHSA re-load needed),
parses each Hebrew word into syllables using Unicode niqqud character inspection,
assigns a 4-component composite breath weight to each syllable, detects colon
boundaries by checking each surface_form for Masoretic disjunctive accent
characters (Unicode combining chars), and writes two tables: `syllable_tokens`
(one row per syllable) and `breath_profiles` (one row per verse). After those
writes, a back-population step aggregates syllable data per colon and updates
`verse_fingerprints.colon_fingerprints` (JSONB), which is what enables the
chiasm detection module to run.

## Tech Stack

- Python 3.11, type hints everywhere, 88-character line limit (ruff enforced)
- `psycopg2` (NOT psycopg3) with `psycopg2.extras.execute_values` for batch inserts
- `psycopg2.extras.executemany` for the back-population UPDATE (no VALUES %s needed)
- No external NLP libraries — all syllable logic is pure Unicode character inspection
- `unittest.mock.MagicMock` for DB unit tests; no live DB required for unit tests
- `uv run --frozen pytest` as test runner; functions not classes

---

## Hebrew Unicode Reference

The following sets are the foundation for all syllable parsing logic. They must
be defined as module-level constants in `breath.py`.

### Vowel Points

```python
# Full vowel points — each contributes 1 syllable nucleus
FULL_VOWELS = {
    '\u05B7': ('patah',    'short',       0.85),
    '\u05B8': ('qamets',   'long',        1.00),
    '\u05B5': ('tsere',    'long',        0.65),
    '\u05B6': ('segol',    'short',       0.70),
    '\u05B4': ('hiriq',    'short',       0.45),
    '\u05B9': ('holam',    'long',        0.75),
    '\u05BA': ('holam_waw','long',        0.75),
    '\u05BB': ('qibbuts',  'long',        0.40),
    '\u05BC': ('dagesh',   None,          None),   # not a vowel — skip
    '\u05C1': ('shin_dot', None,          None),   # diacritical — skip
    '\u05C2': ('sin_dot',  None,          None),   # diacritical — skip
}

# Half-vowels — contribute 0.5 or are ultra-short
HALF_VOWELS = {
    '\u05B0': ('shewa',        'ultra-short', 0.10),
    '\u05B1': ('hataf_segol',  'ultra-short', 0.35),
    '\u05B2': ('hataf_patah',  'ultra-short', 0.40),
    '\u05B3': ('hataf_qamets', 'ultra-short', 0.45),
}
```

### Consonant Onset Classes

```python
GUTTURALS = frozenset('אהחע')
# Used for guttural_density computation in breath_profiles
```

Full ONSET_CLASS dict (stored in breath.py):
- Gutturals (א ה ח ע): class = "guttural", onset_weight = 0.55–0.70
- Liquids (ל ר): class = "liquid", onset_weight = 0.85–0.90
- Nasals (מ נ): class = "nasal", onset_weight = 0.80
- Sibilants (ש ס ז צ): class = "sibilant", onset_weight = 0.35–0.45
- Approximants (י ו): class = "liquid", onset_weight = 0.70–0.75
- Voiced stops (ב ג ד): class = "stop", onset_weight = 0.30
- Voiceless stops (כ ך פ ף ת ק ט): class = "stop", onset_weight = 0.15–0.20

### Disjunctive Accents (Colon Boundary Markers)

```python
DISJUNCTIVE_ACCENTS = frozenset({
    '\u0591',  # etnahta — always a colon boundary (primary verse-medial pause)
    '\u05C3',  # sof pasuq — end of verse (always boundary)
    '\u0592',  # segolta
    '\u0593',  # shalshelet
    '\u0594',  # zaqef qatan
    '\u0595',  # zaqef gadol
    '\u0596',  # tifha
    '\u059A',  # yetiv
    '\u059C',  # geresh
    '\u059D',  # geresh muqdam
    '\u059E',  # gershayim
    '\u05A1',  # pazer
    '\u05A8',  # qadma
    '\u05A9',  # telisha qetana
    '\u05AC',  # ole
    '\u05AE',  # zinor
})
```

### Breath Weight Composite Formula

For each syllable:

```
breath_weight = (0.40 * vowel_openness)
              + (0.25 * vowel_length_score)
              + (0.20 * syllable_openness_score)
              + (0.15 * onset_weight)
```

Where:
- `vowel_openness`: the third element of the FULL_VOWELS or HALF_VOWELS tuple
  (0.0 if dagesh/diacritical, 0.0 if no vowel found)
- `vowel_length_score`: full vowel = 1.0, short = 0.65, ultra-short = 0.25,
  shewa = 0.10 (map from vowel length string via `_vowel_length_score()`)
- `syllable_openness_score`: open syllable (no coda consonant) = 1.0,
  closed syllable = 0.4 (not 0.0, since even closed syllables have some breath)
- `onset_weight`: from ONSET_CLASS dict, default 0.25 for unmapped consonants

---

## Tasks

### Task 1: `modules/breath.py` — Syllable Parser

**Files:** `pipeline/modules/breath.py`

This is the only new source file for Stage 3.

**Steps:**

1. Write tests in `tests/test_breath.py`:

   ```python
   """Tests for Stage 3 breath analysis module.

   All tests are pure-logic unit tests. No DB connection required.
   Primary fixture: Psalm 23:1 — יְהוָה רֹעִי לֹא אֶחְסָר
   """

   from __future__ import annotations

   import sys
   from pathlib import Path
   sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

   import pytest
   from modules.breath import (
       parse_syllables,
       _vowel_length_score,
       _compute_breath_weight,
       FULL_VOWELS,
       HALF_VOWELS,
       DISJUNCTIVE_ACCENTS,
       GUTTURALS,
   )

   # Psalm 23:1 Hebrew text (niqqud included)
   PSALM_23_1_WORDS = [
       "יְהוָה",  # The LORD
       "רֹעִי",   # is my shepherd
       "לֹא",    # not
       "אֶחְסָר", # I shall want
   ]


   def test_breath_weight_range() -> None:
       """All breath weights for Psalm 23:1 must be in [0.0, 1.0]."""
       for word in PSALM_23_1_WORDS:
           for syl in parse_syllables(word):
               w = syl["breath_weight"]
               assert 0.0 <= w <= 1.0, (
                   f"breath_weight={w} out of range for word '{word}'"
               )


   def test_colon_boundary_detected() -> None:
       """Etnahta (U+0591) in a word surface_form signals a colon boundary."""
       # Construct a word string with etnahta embedded
       word_with_etnahta = "יְהוָ\u0591ה"
       assert any(c in DISJUNCTIVE_ACCENTS for c in word_with_etnahta)


   def test_guttural_density_alef() -> None:
       """Word starting with alef (×) has guttural onset class."""
       word = "אֶרֶץ"  # land/earth
       syls = parse_syllables(word)
       assert len(syls) >= 1
       assert syls[0]["onset_class"] == "guttural", (
           f"Expected 'guttural' for alef onset, got '{syls[0]['onset_class']}'"
       )


   def test_mean_weight_psalm_23_1() -> None:
       """Mean breath weight across all syllables of Psalm 23:1 must be in [0.3, 0.8]."""
       all_weights = []
       for word in PSALM_23_1_WORDS:
           for syl in parse_syllables(word):
               all_weights.append(syl["breath_weight"])
       assert len(all_weights) > 0
       mean = sum(all_weights) / len(all_weights)
       assert 0.3 <= mean <= 0.8, f"Mean breath weight {mean:.3f} outside [0.3, 0.8]"


   def test_colon_count_minimum() -> None:
       """Every verse processed should have colon_count >= 1.

       Verified by checking that _process_verse returns a profile with
       colon_count >= 1 even for a single-word verse-like token list.
       """
       from modules.breath import _process_verse
       # Minimal token list: (verse_id, token_id, position, surface_form, pos)
       tokens = [(1, 101, 1, "יְהוָה", "proper_noun")]
       _syllable_rows, profile = _process_verse(tokens)
       # profile tuple: (mean_weight, open_ratio, guttural_density,
       #                  colon_count, colon_boundaries, stress_positions, breath_curve)
       colon_count = profile[3]
       assert colon_count >= 1, f"colon_count={colon_count} must be >= 1"


   def test_colon_fingerprints_backpopulated() -> None:
       """After _backpopulate_colon_fingerprints runs, verse_fingerprints rows
       get non-empty colon_fingerprints.

       Uses mocked DB to confirm the UPDATE is issued with non-empty JSON.
       """
       from unittest.mock import MagicMock, patch
       import json

       conn = MagicMock()
       cur = MagicMock()
       conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
       conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

       # Simulate aggregated syllable data from DB query
       cur.fetchall.return_value = [
           (1, 1, 0.75, 0.60, 5),  # verse_id=1, colon_idx=1, openness, weight, count
           (1, 2, 0.65, 0.55, 4),  # verse_id=1, colon_idx=2
       ]

       from modules.breath import _backpopulate_colon_fingerprints
       _backpopulate_colon_fingerprints(conn, [19])

       # executemany must have been called with data
       cur.executemany.assert_called_once()
       call_args = cur.executemany.call_args
       update_rows = call_args[0][1]
       assert len(update_rows) == 1  # one verse_id
       json_str, verse_id = update_rows[0]
       parsed = json.loads(json_str)
       assert isinstance(parsed, list)
       assert len(parsed) == 2  # two colons
       assert parsed[0]["colon"] == 1


   def test_breath_idempotent() -> None:
       """Running parse_syllables twice on same word produces identical results."""
       word = "אֱלֹהִים"
       result1 = parse_syllables(word)
       result2 = parse_syllables(word)
       assert len(result1) == len(result2)
       for s1, s2 in zip(result1, result2):
           assert s1["breath_weight"] == s2["breath_weight"]
           assert s1["onset_class"] == s2["onset_class"]


   # --- Auxiliary tests for helper functions ---

   def test_vowel_length_score_long_is_max() -> None:
       """Long vowels must return the maximum length score (1.0)."""
       assert _vowel_length_score("long") == 1.0


   def test_vowel_length_score_ordering() -> None:
       """Vowel length scores must be ordered: long > short > ultra-short."""
       assert _vowel_length_score("long") > _vowel_length_score("short")
       assert _vowel_length_score("short") > _vowel_length_score("ultra-short")


   def test_vowel_length_score_unknown_is_midrange() -> None:
       """Unknown or None vowel length must return a valid midrange value."""
       score = _vowel_length_score(None)
       assert 0.0 < score < 1.0


   def test_full_vowels_has_qamets() -> None:
       """FULL_VOWELS dict must contain qamets (U+05B8)."""
       assert '\u05B8' in FULL_VOWELS


   def test_full_vowels_dagesh_has_no_openness() -> None:
       """Dagesh (U+05BC) entry must have None openness (it is not a vowel)."""
       entry = FULL_VOWELS.get('\u05BC')
       assert entry is not None
       assert entry[2] is None  # third element = openness = None for dagesh


   def test_half_vowels_has_shewa() -> None:
       """HALF_VOWELS dict must contain shewa (U+05B0)."""
       assert '\u05B0' in HALF_VOWELS


   def test_disjunctive_accents_has_etnahta() -> None:
       """DISJUNCTIVE_ACCENTS must include etnahta (U+0591)."""
       assert '\u0591' in DISJUNCTIVE_ACCENTS


   def test_disjunctive_accents_has_sof_pasuq() -> None:
       """DISJUNCTIVE_ACCENTS must include sof pasuq (U+05C3)."""
       assert '\u05C3' in DISJUNCTIVE_ACCENTS


   def test_disjunctive_accents_minimum_size() -> None:
       """DISJUNCTIVE_ACCENTS must have at least 8 members."""
       assert len(DISJUNCTIVE_ACCENTS) >= 8


   def test_gutturals_contains_all_four() -> None:
       """GUTTURALS must contain alef, he, het, ayin."""
       for ch in 'אהחע':
           assert ch in GUTTURALS, f"Guttural '{ch}' missing from GUTTURALS set"


   def test_parse_syllables_returns_nonempty() -> None:
       """parse_syllables must return at least one syllable for any input."""
       for word in ["א", "כ", "שלום", "אֱלֹהִים", "יְהוָה"]:
           result = parse_syllables(word)
           assert len(result) >= 1, f"No syllables returned for '{word}'"


   def test_parse_syllables_all_required_keys() -> None:
       """Every syllable dict must contain all required keys."""
       required = {
           "syl_idx", "text", "nucleus_vowel", "vowel_openness",
           "vowel_length", "is_open", "onset_class",
           "breath_weight", "is_stressed",
       }
       for word in PSALM_23_1_WORDS:
           for syl in parse_syllables(word):
               missing = required - syl.keys()
               assert not missing, (
                   f"Syllable for '{word}' missing keys: {missing}"
               )


   def test_parse_syllables_breath_weight_is_float() -> None:
       """breath_weight must be a float (not int or None)."""
       for word in PSALM_23_1_WORDS:
           for syl in parse_syllables(word):
               assert isinstance(syl["breath_weight"], float), (
                   f"breath_weight type={type(syl['breath_weight'])} for '{word}'"
               )


   def test_compute_breath_weight_uses_all_components() -> None:
       """_compute_breath_weight must respect the 4-component formula."""
       # Full open syllable with qamets (openness=1.0, long) and liquid onset
       w = _compute_breath_weight(
           vowel_openness=1.0,
           vowel_length="long",
           is_open=True,
           onset_weight=0.90,
       )
       # Minimum expected: 0.40*1.0 + 0.25*1.0 + 0.20*1.0 + 0.15*0.90 = 0.985
       assert w >= 0.98
       assert w <= 1.0


   def test_compute_breath_weight_shewa_is_low() -> None:
       """A shewa syllable with stop onset should produce low breath weight."""
       w = _compute_breath_weight(
           vowel_openness=0.10,
           vowel_length="ultra-short",
           is_open=False,
           onset_weight=0.20,
       )
       # 0.40*0.10 + 0.25*0.25 + 0.20*0.4 + 0.15*0.20 = 0.04+0.0625+0.08+0.03 = 0.2125
       assert w < 0.35
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_breath.py -v
   # Expected: FAILED — ModuleNotFoundError: No module named 'modules.breath'
   ```

3. Implement `pipeline/modules/breath.py`:

   ```python
   """
   Stage 3 — Breath and phonetic analysis.

   Parses Hebrew words into syllables, assigns breath weights based on
   vowel and consonant properties, detects colon boundaries from Masoretic
   disjunctive accent positions, and stores verse-level breath profiles.

   After this module runs, back-populates colon_fingerprints on
   verse_fingerprints (required before Stage 2 chiasm detection can run).
   """

   from __future__ import annotations

   import json
   import logging
   import time
   from collections import defaultdict
   from typing import Dict, List, Optional, Tuple

   import psycopg2
   import psycopg2.extras

   from adapters.db_adapter import batch_upsert, verse_ids_for_stage

   logger = logging.getLogger(__name__)

   # ── Unicode character sets ────────────────────────────────────────────────

   # Full vowel points: (name, length_class, openness_score)
   # dagesh and diacriticals have openness=None and are not syllable nuclei.
   FULL_VOWELS: Dict[str, Tuple[str, Optional[str], Optional[float]]] = {
       "\u05B7": ("patah",    "short",  0.85),
       "\u05B8": ("qamets",   "long",   1.00),
       "\u05B5": ("tsere",    "long",   0.65),
       "\u05B6": ("segol",    "short",  0.70),
       "\u05B4": ("hiriq",    "short",  0.45),
       "\u05B9": ("holam",    "long",   0.75),
       "\u05BA": ("holam_waw","long",   0.75),
       "\u05BB": ("qibbuts",  "long",   0.40),
       "\u05BC": ("dagesh",   None,     None),
       "\u05C1": ("shin_dot", None,     None),
       "\u05C2": ("sin_dot",  None,     None),
   }

   # Half-vowels: (name, length_class, openness_score)
   HALF_VOWELS: Dict[str, Tuple[str, str, float]] = {
       "\u05B0": ("shewa",        "ultra-short", 0.10),
       "\u05B1": ("hataf_segol",  "ultra-short", 0.35),
       "\u05B2": ("hataf_patah",  "ultra-short", 0.40),
       "\u05B3": ("hataf_qamets", "ultra-short", 0.45),
   }

   ALL_VOWELS: Dict[str, tuple] = {**FULL_VOWELS, **HALF_VOWELS}

   # Hebrew consonants including final (sofit) forms
   CONSONANTS: frozenset = frozenset(
       "אבגדהוזחטיכלמנסעפצקרשת" + "ךםןףץ"
   )

   # Guttural consonants (affect following vowel, used for guttural_density)
   GUTTURALS: frozenset = frozenset("אהחע")

   # Consonant onset classes: consonant → (class_name, onset_weight)
   ONSET_CLASS: Dict[str, Tuple[str, float]] = {
       # Gutturals
       "א": ("guttural", 0.65),
       "ה": ("guttural", 0.70),
       "ח": ("guttural", 0.55),
       "ע": ("guttural", 0.60),
       # Sibilants
       "ש": ("sibilant", 0.40),
       "ס": ("sibilant", 0.40),
       "ז": ("sibilant", 0.45),
       "צ": ("sibilant", 0.35),
       # Liquids
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
   DEFAULT_ONSET: Tuple[str, float] = ("stop", 0.25)

   # Masoretic disjunctive accents — each signals a colon boundary
   DISJUNCTIVE_ACCENTS: frozenset = frozenset({
       "\u0591",  # etnahta — primary verse-medial pause
       "\u05C3",  # sof pasuq — end of verse
       "\u0592",  # segolta
       "\u0593",  # shalshelet
       "\u0594",  # zaqef qatan
       "\u0595",  # zaqef gadol
       "\u0596",  # tifha
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

   # ── Main entry point ──────────────────────────────────────────────────────

   def run(
       conn: psycopg2.extensions.connection,
       config: dict,
   ) -> dict:
       """Process all pending verses: parse syllables and compute breath profiles.

       After writing syllable_tokens and breath_profiles, calls
       _backpopulate_colon_fingerprints to update verse_fingerprints.colon_fingerprints,
       which is required before Stage 2 chiasm detection can run.

       Args:
           conn: Live psycopg2 connection.
           config: Full parsed config.yml.

       Returns:
           Dict with "rows_written", "elapsed_s", "breath_profiles",
           "syllable_tokens".
       """
       t0 = time.perf_counter()
       corpus = config.get("corpus", {})
       book_nums: List[int] = [
           b["book_num"] for b in corpus.get("books", [])
       ]
       debug_chapters: List[int] = corpus.get("debug_chapters", [])
       batch_size: int = config.get("breath", {}).get("batch_size", 100)

       pending = verse_ids_for_stage(
           conn, "breath_profiles", book_nums, debug_chapters
       )
       if not pending:
           logger.info("All breath profiles already computed.")
           _backpopulate_colon_fingerprints(conn, book_nums)
           return {
               "rows_written": 0,
               "elapsed_s": round(time.perf_counter() - t0, 2),
               "breath_profiles": 0,
               "syllable_tokens": 0,
           }

       logger.info("Computing breath profiles for %d verses", len(pending))

       # Fetch word tokens for pending verses (surface_form is all we need)
       with conn.cursor() as cur:
           cur.execute(
               """
               SELECT wt.verse_id, wt.token_id, wt.position,
                      wt.surface_form, wt.part_of_speech
               FROM word_tokens wt
               WHERE wt.verse_id = ANY(%s)
               ORDER BY wt.verse_id, wt.position
               """,
               (pending,),
           )
           token_rows = cur.fetchall()

       by_verse: Dict[int, list] = defaultdict(list)
       for row in token_rows:
           by_verse[row[0]].append(row)

       syllable_rows_all: List[tuple] = []
       profile_rows: List[tuple] = []

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
             SET mean_weight      = EXCLUDED.mean_weight,
                 open_ratio       = EXCLUDED.open_ratio,
                 guttural_density = EXCLUDED.guttural_density,
                 colon_count      = EXCLUDED.colon_count,
                 colon_boundaries = EXCLUDED.colon_boundaries,
                 stress_positions = EXCLUDED.stress_positions,
                 breath_curve     = EXCLUDED.breath_curve,
                 computed_at      = NOW()
           """,
           profile_rows,
           batch_size=batch_size,
       )

       # Update colon_count on verses table
       with conn.cursor() as cur:
           cur.execute(
               """
               UPDATE verses v
               SET colon_count = bp.colon_count
               FROM breath_profiles bp
               WHERE v.verse_id = bp.verse_id
                 AND v.verse_id = ANY(%s)
               """,
               (pending,),
           )
       conn.commit()

       elapsed = round(time.perf_counter() - t0, 2)
       logger.info(
           "Breath profiles stored: %d, syllable tokens: %d in %.1fs",
           len(profile_rows),
           len(syllable_rows_all),
           elapsed,
       )

       _backpopulate_colon_fingerprints(conn, book_nums)

       return {
           "rows_written": len(profile_rows),
           "elapsed_s": elapsed,
           "breath_profiles": len(profile_rows),
           "syllable_tokens": len(syllable_rows_all),
       }

   # ── Verse processing ──────────────────────────────────────────────────────

   def _process_verse(
       tokens: list,
   ) -> Tuple[List[tuple], tuple]:
       """Process all tokens in a verse into syllables and a breath profile.

       Args:
           tokens: List of rows from word_tokens:
               (verse_id, token_id, position, surface_form, part_of_speech).

       Returns:
           Tuple of (syllable_rows, profile_tuple) where:
               syllable_rows: list of tuples for syllable_tokens insert
                   (without verse_id prefix — caller prepends it).
               profile_tuple: tuple for breath_profiles insert
                   (mean_weight, open_ratio, guttural_density, colon_count,
                    colon_boundaries_json, stress_positions_json,
                    breath_curve_json).
       """
       all_syllables: List[dict] = []
       colon_boundaries: List[int] = [0]
       current_colon = 1
       n_tokens = len(tokens)

       for idx, token_row in enumerate(tokens):
           _verse_id, token_id, position, surface_form, _pos_tag = token_row
           has_disjunctive = any(c in DISJUNCTIVE_ACCENTS for c in surface_form)
           syls = parse_syllables(surface_form)

           for syl in syls:
               syl["token_id"] = token_id
               syl["colon_index"] = current_colon
               syl["global_idx"] = len(all_syllables)
               all_syllables.append(syl)

           # Advance colon counter after the word that carries the accent,
           # but not after the very last token (that would create an empty colon)
           if has_disjunctive and idx < n_tokens - 1:
               current_colon += 1
               colon_boundaries.append(len(all_syllables))

       if not all_syllables:
           return [], (
               0.0, 0.0, 0.0, 1,
               json.dumps([0]),
               json.dumps([]),
               json.dumps([]),
           )

       n_syls = len(all_syllables)
       weights = [s["breath_weight"] for s in all_syllables]
       mean_weight = round(sum(weights) / n_syls, 4)
       open_ratio = round(
           sum(1 for s in all_syllables if s.get("is_open")) / n_syls, 4
       )
       guttural_density = round(
           sum(
               1 for s in all_syllables if s.get("onset_class") == "guttural"
           ) / n_syls,
           4,
       )
       colon_count = current_colon

       # Normalize stress positions to [0, 1] scale
       stress_positions: List[float] = [
           round(s["global_idx"] / max(n_syls - 1, 1), 4)
           for s in all_syllables
           if s.get("is_stressed")
       ]
       breath_curve: List[float] = [
           round(s["breath_weight"], 4) for s in all_syllables
       ]

       # Build syllable_tokens rows (verse_id prepended by caller)
       syllable_rows: List[tuple] = []
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
               round(syl["global_idx"] / max(n_syls - 1, 1), 4),
               syl["colon_index"],
           ))

       profile: tuple = (
           mean_weight,
           open_ratio,
           guttural_density,
           colon_count,
           json.dumps(colon_boundaries),
           json.dumps(stress_positions),
           json.dumps(breath_curve),
       )
       return syllable_rows, profile

   # ── Syllable parser ───────────────────────────────────────────────────────

   def parse_syllables(word: str) -> List[dict]:
       """Parse a Hebrew word (with niqqud) into syllable structures.

       Each syllable is identified by its onset consonant. The parser scans
       left-to-right: on each consonant, it collects dagesh and vowel combining
       characters that follow, then determines syllable openness and computes
       a composite breath weight.

       Args:
           word: Hebrew word string, potentially with niqqud combining chars.

       Returns:
           Non-empty list of syllable dicts. Each dict has keys:
               syl_idx (int), text (str), nucleus_vowel (str|None),
               vowel_openness (float), vowel_length (str|None),
               is_open (bool), onset_class (str), breath_weight (float),
               is_stressed (bool).
       """
       syllables: List[dict] = []
       chars = list(word)
       n = len(chars)
       i = 0
       syl_idx = 0

       while i < n:
           ch = chars[i]

           # Skip combining marks and accent characters that are not consonants
           if ch not in CONSONANTS:
               i += 1
               continue

           # Onset consonant
           onset_char = ch
           onset_class, onset_weight = ONSET_CLASS.get(onset_char, DEFAULT_ONSET)
           syl_text = ch
           i += 1

           # Consume dagesh, shin dot, sin dot immediately after onset
           while i < n and chars[i] in ("\u05BC", "\u05C1", "\u05C2", "\u05BD"):
               syl_text += chars[i]
               i += 1

           # Collect nucleus vowel
           nucleus_vowel: Optional[str] = None
           vowel_openness: float = 0.0
           vowel_length: Optional[str] = None

           if i < n and chars[i] in ALL_VOWELS:
               vowel_ch = chars[i]
               syl_text += vowel_ch
               vdata = ALL_VOWELS[vowel_ch]
               if vdata[2] is not None:  # skip dagesh/diacritical entries
                   vowel_name, vowel_length, vowel_openness = vdata
                   nucleus_vowel = vowel_ch
               i += 1

           # Consume any trailing non-consonant, non-accent characters
           while (
               i < n
               and chars[i] not in CONSONANTS
               and chars[i] not in DISJUNCTIVE_ACCENTS
           ):
               if chars[i] in ALL_VOWELS:
                   vdata = ALL_VOWELS[chars[i]]
                   # Accept a second vowel indicator only if no vowel found yet
                   if vdata[2] is not None and vowel_openness == 0.0:
                       _vname, vowel_length, vowel_openness = vdata
                       nucleus_vowel = chars[i]
               syl_text += chars[i]
               i += 1

           # Open syllable: ends in a vowel (next char is consonant or end of word)
           is_open: bool = vowel_length in ("long", "short")

           breath_weight = _compute_breath_weight(
               vowel_openness=vowel_openness,
               vowel_length=vowel_length,
               is_open=is_open,
               onset_weight=onset_weight,
           )

           syllables.append({
               "syl_idx":        syl_idx,
               "text":           syl_text,
               "nucleus_vowel":  nucleus_vowel,
               "vowel_openness": vowel_openness,
               "vowel_length":   vowel_length,
               "is_open":        is_open,
               "onset_class":    onset_class,
               "breath_weight":  breath_weight,
               "is_stressed":    vowel_length == "long",
           })
           syl_idx += 1

       # Guarantee at least one syllable for single-consonant / unvocalized words
       if not syllables:
           onset_class, onset_weight = ONSET_CLASS.get(
               next((c for c in word if c in CONSONANTS), ""), DEFAULT_ONSET
           )
           syllables.append({
               "syl_idx":        0,
               "text":           word,
               "nucleus_vowel":  None,
               "vowel_openness": 0.3,
               "vowel_length":   "short",
               "is_open":        True,
               "onset_class":    onset_class,
               "breath_weight":  _compute_breath_weight(0.3, "short", True, onset_weight),
               "is_stressed":    False,
           })

       return syllables


   def _compute_breath_weight(
       vowel_openness: float,
       vowel_length: Optional[str],
       is_open: bool,
       onset_weight: float,
   ) -> float:
       """Compute the composite breath weight for a syllable.

       Formula:
           breath_weight = 0.40 * vowel_openness
                         + 0.25 * vowel_length_score
                         + 0.20 * syllable_openness_score
                         + 0.15 * onset_weight

       Args:
           vowel_openness: Openness score of the nucleus vowel (0.0–1.0).
           vowel_length: Length class string ("long", "short", "ultra-short").
           is_open: True if the syllable is open (CV), False if closed (CVC).
           onset_weight: Sonority weight of the onset consonant.

       Returns:
           Float breath weight in [0.0, 1.0].
       """
       length_score = _vowel_length_score(vowel_length)
       openness_score = 1.0 if is_open else 0.4
       weight = (
           0.40 * vowel_openness
           + 0.25 * length_score
           + 0.20 * openness_score
           + 0.15 * onset_weight
       )
       return round(min(max(weight, 0.0), 1.0), 4)


   def _vowel_length_score(length: Optional[str]) -> float:
       """Return a normalized score for a vowel length class.

       Args:
           length: Length class string or None.

       Returns:
           Float in (0.0, 1.0].
       """
       return {
           "long":        1.00,
           "short":       0.65,
           "ultra-short": 0.25,
           "shewa":       0.10,
       }.get(length or "short", 0.50)

   # ── Back-population of colon fingerprints ─────────────────────────────────

   def _backpopulate_colon_fingerprints(
       conn: psycopg2.extensions.connection,
       book_nums: List[int],
   ) -> None:
       """Aggregate syllable data per colon and update verse_fingerprints.

       Computes colon-level summary statistics from syllable_tokens and writes
       them into verse_fingerprints.colon_fingerprints as a JSONB array. This
       data is required by modules/chiasm.py before it can run.

       Each element of the JSON array has the structure:
           {"colon": int, "density": float, "sonority": float, "mean_weight": float}

       Args:
           conn: Live psycopg2 connection.
           book_nums: Books to process (filter).
       """
       logger.info("Back-populating colon fingerprints for book_nums=%s", book_nums)

       with conn.cursor() as cur:
           cur.execute(
               """
               SELECT st.verse_id,
                      st.colon_index,
                      AVG(st.vowel_openness) AS mean_openness,
                      AVG(st.breath_weight)  AS mean_weight,
                      COUNT(*)               AS syllable_count
               FROM syllable_tokens st
               JOIN verses v ON st.verse_id = v.verse_id
               WHERE v.book_num = ANY(%s)
               GROUP BY st.verse_id, st.colon_index
               ORDER BY st.verse_id, st.colon_index
               """,
               (book_nums,),
           )
           rows = cur.fetchall()

       by_verse: Dict[int, List[dict]] = defaultdict(list)
       for verse_id, colon_idx, mean_openness, mean_weight, syl_count in rows:
           by_verse[verse_id].append({
               "colon":       colon_idx,
               "density":     round(float(syl_count), 2),
               "sonority":    round(float(mean_openness or 0.0), 4),
               "mean_weight": round(float(mean_weight or 0.0), 4),
           })

       if not by_verse:
           logger.info("No colon data found for back-population.")
           return

       update_rows = [
           (json.dumps(colons), verse_id)
           for verse_id, colons in by_verse.items()
       ]

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
       logger.info(
           "Colon fingerprints back-populated for %d verses", len(update_rows)
       )
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_breath.py -v
   # Expected: all tests PASSED (target: 20+ tests)
   ```

5. Lint and typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat(stage3): add breath module — syllable parser + phonetic analysis"`

---

### Task 2: Integration Run and Acceptance Verification

**Files:** No new source files. SQL queries and pipeline runner commands.

This task runs after `modules/breath.py` is implemented and Stage 2 is
confirmed complete (2,527 verse rows, ~43,000 word_token rows).

**Steps:**

1. Execute Stage 3 against the live corpus:

   ```bash
   docker compose --profile pipeline run --rm pipeline \
     python -m pipeline.run --stages 3
   ```

2. Verify syllable token count:

   ```sql
   SELECT COUNT(*) FROM syllable_tokens st
   JOIN verses v ON st.verse_id = v.verse_id WHERE v.book_num = 19;
   -- Expected: 100,000–140,000 (target ~120,000)
   ```

3. Verify breath profile coverage:

   ```sql
   SELECT COUNT(*) FROM breath_profiles bp
   JOIN verses v ON bp.verse_id = v.verse_id WHERE v.book_num = 19;
   -- Expected: 2527 (exactly one per verse)
   ```

4. Verify breath weight range is within bounds:

   ```sql
   SELECT
     MIN(breath_weight) AS min_w,
     MAX(breath_weight) AS max_w,
     AVG(breath_weight) AS avg_w
   FROM syllable_tokens st
   JOIN verses v ON st.verse_id = v.verse_id WHERE v.book_num = 19;
   -- Expected: min ~0.10, max ~1.0, avg ~0.50–0.65
   ```

5. Verify colon distribution is realistic for Hebrew poetry:

   ```sql
   SELECT colon_count, COUNT(*) AS verse_count
   FROM breath_profiles bp
   JOIN verses v ON bp.verse_id = v.verse_id
   WHERE v.book_num = 19
   GROUP BY colon_count ORDER BY colon_count;
   -- Most verses should have 2–4 colons
   -- No verses should have colon_count < 1
   ```

6. Verify colon_fingerprints were back-populated:

   ```sql
   SELECT COUNT(*) FROM verse_fingerprints
   WHERE colon_fingerprints IS NOT NULL
     AND colon_fingerprints != '[]'::jsonb
     AND verse_id IN (
       SELECT verse_id FROM verses WHERE book_num = 19
     );
   -- Expected: 2527
   ```

7. Verify Psalm 23:1 breath curve as a spot check:

   ```sql
   SELECT s.syllable_index, s.syllable_text, s.breath_weight,
          s.onset_class, s.colon_index
   FROM syllable_tokens s
   JOIN word_tokens wt ON s.token_id = wt.token_id
   JOIN verses v ON s.verse_id = v.verse_id
   WHERE v.book_num = 19 AND v.chapter = 23 AND v.verse_num = 1
   ORDER BY wt.position, s.syllable_index;
   -- Psalm 23:1: יְהוָה רֹעִי לֹא אֶחְסָר
   -- Expect etnahta after רֹעִי (colon 1), remainder in colon 2
   ```

8. Commit: `"test(stage3): verify breath profile acceptance criteria"`

---

### Task 3: Idempotency Verification

**Files:** No new files.

**Steps:**

1. Run Stage 3 a second time:

   ```bash
   docker compose --profile pipeline run --rm pipeline \
     python -m pipeline.run --stages 3
   ```

2. Confirm the log output shows 0 new breath profiles:

   ```
   Stage targeting breath_profiles: 2527 total, 2527 done, 0 pending
   All breath profiles already computed.
   ```

3. Confirm row counts are unchanged by re-running the SQL queries from Task 2.

4. Confirm the back-population step ran again (it always runs even when
   breath_profiles is fully populated) and did not corrupt the data:

   ```sql
   -- Spot-check one verse
   SELECT colon_fingerprints FROM verse_fingerprints
   WHERE verse_id = (
     SELECT verse_id FROM verses
     WHERE book_num = 19 AND chapter = 23 AND verse_num = 1
   );
   -- Should return a non-null JSON array with 2 elements (2 colons in Ps 23:1)
   ```

5. Run the full test suite for Stages 2 and 3 together to confirm no regressions:

   ```bash
   uv run --frozen pytest tests/test_db_adapter.py tests/test_ingest.py \
     tests/test_fingerprint.py tests/test_chiasm.py tests/test_breath.py -v
   # Expected: all PASSED
   ```

6. Commit: `"test(stage3): confirm idempotency — Stage 3 acceptance criteria met"`

---

### Task 4: Trigger Stage 2 Chiasm Second Pass

**Files:** No new source files. This task triggers the deferred chiasm.run()
from Plan 02 Task 4.

**Precondition:** Task 2 above must be complete (colon_fingerprints back-populated
for all 2,527 Psalms verses).

**Steps:**

1. Confirm precondition:

   ```sql
   SELECT COUNT(*) FROM verse_fingerprints
   WHERE colon_fingerprints IS NOT NULL
     AND colon_fingerprints != '[]'::jsonb;
   -- Must equal 2527 before proceeding
   ```

2. Run the chiasm stage:

   ```bash
   docker compose --profile pipeline run --rm pipeline \
     python -m pipeline.run --stages 2-chiasm
   ```

3. Verify candidates were stored:

   ```sql
   SELECT COUNT(*) FROM chiasm_candidates;
   -- Expected: at least 1 candidate (typically 20–200 for Psalms
   -- depending on config.chiasm.similarity_threshold and min_confidence)

   SELECT pattern_type, COUNT(*) FROM chiasm_candidates
   GROUP BY pattern_type;
   -- Should show both ABBA and ABCBA rows
   ```

4. Spot-check a candidate:

   ```sql
   SELECT cc.pattern_type, cc.confidence,
          v1.chapter AS start_chapter, v1.verse_num AS start_verse,
          v2.chapter AS end_chapter, v2.verse_num AS end_verse
   FROM chiasm_candidates cc
   JOIN verses v1 ON cc.verse_id_start = v1.verse_id
   JOIN verses v2 ON cc.verse_id_end   = v2.verse_id
   ORDER BY cc.confidence DESC
   LIMIT 5;
   ```

5. Run chiasm unit tests to confirm they still pass:

   ```bash
   uv run --frozen pytest tests/test_chiasm.py -v
   # Expected: all PASSED
   ```

6. Commit: `"feat(stage2-chiasm): execute chiasm second pass after Stage 3 complete"`

---

## SQL Validation Queries (Full Suite)

```sql
-- Syllable token count
SELECT COUNT(*) FROM syllable_tokens st
JOIN verses v ON st.verse_id = v.verse_id WHERE v.book_num = 19;
-- Expected: 100,000–140,000

-- Breath profile coverage
SELECT COUNT(*) FROM breath_profiles bp
JOIN verses v ON bp.verse_id = v.verse_id WHERE v.book_num = 19;
-- Expected: 2527

-- Colon distribution
SELECT colon_count, COUNT(*) AS verse_count
FROM breath_profiles bp
JOIN verses v ON bp.verse_id = v.verse_id
WHERE v.book_num = 19
GROUP BY colon_count ORDER BY colon_count;
-- Median expected: 2–4

-- Breath weight range check
SELECT
  MIN(breath_weight) AS min_w,
  MAX(breath_weight) AS max_w,
  AVG(breath_weight) AS avg_w
FROM syllable_tokens st
JOIN verses v ON st.verse_id = v.verse_id WHERE v.book_num = 19;
-- min ~0.10, max ~1.0, avg ~0.50–0.65

-- Onset class distribution
SELECT onset_class, COUNT(*) AS n
FROM syllable_tokens st
JOIN verses v ON st.verse_id = v.verse_id WHERE v.book_num = 19
GROUP BY onset_class ORDER BY n DESC;
-- Expect: stop > liquid > nasal > sibilant > guttural (typical Hebrew distribution)

-- Colon fingerprints back-populated
SELECT COUNT(*) FROM verse_fingerprints
WHERE colon_fingerprints IS NOT NULL
  AND colon_fingerprints != '[]'::jsonb
  AND verse_id IN (SELECT verse_id FROM verses WHERE book_num = 19);
-- Expected: 2527

-- Psalm 23:1 breath curve
SELECT s.syllable_index, s.syllable_text, s.breath_weight,
       s.onset_class, s.colon_index
FROM syllable_tokens s
JOIN word_tokens wt ON s.token_id = wt.token_id
JOIN verses v ON s.verse_id = v.verse_id
WHERE v.book_num = 19 AND v.chapter = 23 AND v.verse_num = 1
ORDER BY wt.position, s.syllable_index;
```

## Notes on Accent Coverage

The BHSA corpus stores full Masoretic accent data for all pointed books. Psalms,
Job, and Proverbs use the **poetic accent system** (ta'amei ha-miqra poetical),
which differs from the prose system. Etnahta (atnah) functions the same in both
systems as the primary verse-medial pause. The poetic books also have additional
accents (shalshelet, zinor) that are handled in the `DISJUNCTIVE_ACCENTS` set.

The colon boundary detection is deliberately broad — all major disjunctive accents
are counted. This may over-segment some verses with multiple mid-verse pauses.
The chiasm module's threshold configuration in `config.yml` compensates by
requiring stronger similarity scores for pattern matching when colon counts are
high.

## Conftest Reminder

Ensure `tests/conftest.py` at repo root contains the path injection:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "pipeline"))
```

This allows all test files to import `from modules.breath import ...` and
`from adapters.db_adapter import ...` without installing the package.
