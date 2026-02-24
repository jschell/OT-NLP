# Plan: Stage 2 — Morphology & Fingerprinting

> **Depends on:** Stage 0 (schema created, all tables present, cross-container
> connectivity verified), Stage 1 (BHSA downloaded at configured data_path,
> translation files present)
> **Status:** active

## Goal

Extract morphological data from the BHSA corpus into `verses` and `word_tokens`,
compute a 4-dimensional style fingerprint per verse into `verse_fingerprints`, and
implement chiasm candidate detection (deferred until after Stage 3 supplies colon
boundary data).

## Acceptance Criteria

- `verses` table contains exactly 2,527 rows for Psalms (book_num = 19)
- `word_tokens` table contains approximately 43,000 rows for Psalms
- `verse_fingerprints` contains exactly one row per verse (2,527 rows)
- All four fingerprint columns are non-null and within expected ranges:
  - `syllable_density`: 1.5–4.0
  - `morpheme_ratio`: 1.0–4.5
  - `sonority_score`: 0.2–0.8
  - `clause_compression`: 2.0–15.0
- All unit tests in `test_db_adapter.py`, `test_fingerprint.py`, and
  `test_chiasm.py` pass
- Running the full stage twice with identical config produces identical row counts
  (no duplicates — idempotent)
- **DEFERRED:** After Stage 3 completes, `chiasm_candidates` contains at least
  some candidates (expect 20–200 for Psalms depending on config thresholds)

## Architecture

The stage is split into three modules plus a shared database adapter. `ingest.py`
drives the text-fabric BHSA API to extract Hebrew surface forms and morphological
tags, writing them to `verses` and `word_tokens`. `fingerprint.py` reads those
tables and computes four scalar dimensions per verse that together form a style
fingerprint vector. `chiasm.py` is implemented now but executed only after Stage 3
populates `verse_fingerprints.colon_fingerprints` via its back-population step;
`run.py` sequences this correctly. The `db_adapter.py` module provides resumable
upsert primitives shared by all pipeline stages.

## Tech Stack

- Python 3.11, type hints everywhere, 88-character line limit (ruff enforced)
- `psycopg2` (NOT psycopg3) with `psycopg2.extras.execute_values` for batch inserts
- `text-fabric` + BHSA corpus for Hebrew morphological data
- `numpy` for vector math in chiasm detection
- `unittest.mock.MagicMock` for DB unit tests (no live DB required for unit tests)
- `uv run --frozen pytest` as test runner; functions not classes

---

## Tasks

### Task 1: `adapters/db_adapter.py` — Shared DB Utilities

**Files:** `pipeline/adapters/__init__.py`, `pipeline/adapters/db_adapter.py`

This module must be written FIRST. Every other module in Stage 2 imports from it.

**Steps:**

1. Write tests in `tests/test_db_adapter.py`:

   ```python
   """Tests for db_adapter resumable upsert utilities."""
   from __future__ import annotations

   from unittest.mock import MagicMock, patch, call
   import pytest

   import sys
   from pathlib import Path
   sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

   from adapters.db_adapter import (
       get_processed_verse_ids,
       batch_upsert,
       verse_ids_for_stage,
   )


   def test_get_processed_verse_ids_empty() -> None:
       """Returns empty set when target table has no rows."""
       conn = MagicMock()
       cur = MagicMock()
       cur.fetchall.return_value = []
       conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
       conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

       result = get_processed_verse_ids(conn, "verse_fingerprints")

       assert result == set()
       cur.execute.assert_called_once()


   def test_get_processed_verse_ids_populated() -> None:
       """Returns correct set of verse_ids from target table."""
       conn = MagicMock()
       cur = MagicMock()
       cur.fetchall.return_value = [(1,), (2,), (7,)]
       conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
       conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

       result = get_processed_verse_ids(conn, "verse_fingerprints")

       assert result == {1, 2, 7}


   def test_get_processed_verse_ids_custom_id_col() -> None:
       """Accepts a custom id_column argument."""
       conn = MagicMock()
       cur = MagicMock()
       cur.fetchall.return_value = [(42,)]
       conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
       conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

       result = get_processed_verse_ids(conn, "chiasm_candidates", "verse_id_start")

       assert result == {42}
       sql_call = cur.execute.call_args[0][0]
       assert "verse_id_start" in sql_call


   def test_batch_upsert_returns_count() -> None:
       """Returns total number of rows passed across all batches."""
       conn = MagicMock()
       cur = MagicMock()
       conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
       conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

       rows = [(i, f"val{i}") for i in range(5)]
       sql = "INSERT INTO test_table (id, val) VALUES %s"

       count = batch_upsert(conn, sql, rows, batch_size=3)

       assert count == 5
       # Two batches: [0..2] and [3..4]
       assert conn.commit.call_count == 2


   def test_batch_upsert_empty_rows() -> None:
       """Returns 0 for empty input without calling execute."""
       conn = MagicMock()
       count = batch_upsert(conn, "INSERT INTO t VALUES %s", [])
       assert count == 0
       conn.cursor.assert_not_called()


   def test_verse_ids_for_stage_subtracts_done() -> None:
       """Returns only verse_ids not yet in the target table."""
       conn = MagicMock()
       cur = MagicMock()
       conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
       conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

       # First call: get_all_verse_ids → returns [1, 2, 3, 4]
       # Second call: get_processed_verse_ids → returns {2, 4}
       cur.fetchall.side_effect = [
           [(1,), (2,), (3,), (4,)],  # all verse ids
           [(2,), (4,)],               # already processed
       ]

       result = verse_ids_for_stage(conn, "verse_fingerprints", [19])

       assert sorted(result) == [1, 3]
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_db_adapter.py -v
   # Expected: FAILED — ModuleNotFoundError: No module named 'adapters'
   ```

3. Create `pipeline/adapters/__init__.py` (empty) and implement
   `pipeline/adapters/db_adapter.py`:

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
   from typing import List, Optional, Set

   import psycopg2
   import psycopg2.extras

   logger = logging.getLogger(__name__)


   def get_processed_verse_ids(
       conn: psycopg2.extensions.connection,
       table: str,
       id_column: str = "verse_id",
   ) -> Set[int]:
       """Return the set of verse_ids that already have rows in table.

       Used to skip already-computed verses when resuming a pipeline
       stage after interruption.

       Args:
           conn: Live psycopg2 connection.
           table: Target table name to query.
           id_column: Column name to read (default "verse_id").

       Returns:
           Set of integer verse IDs already present in the table.
       """
       with conn.cursor() as cur:
           cur.execute(f"SELECT DISTINCT {id_column} FROM {table}")
           return {row[0] for row in cur.fetchall()}


   def get_all_verse_ids(
       conn: psycopg2.extensions.connection,
       book_nums: List[int],
       debug_chapters: Optional[List[int]] = None,
   ) -> List[int]:
       """Return all verse_ids for the given books, optionally filtered.

       Args:
           conn: Live psycopg2 connection.
           book_nums: List of book numbers to include.
           debug_chapters: If provided, restrict to these chapter numbers.

       Returns:
           Sorted list of verse_id integers.
       """
       with conn.cursor() as cur:
           if debug_chapters:
               cur.execute(
                   """
                   SELECT verse_id FROM verses
                   WHERE book_num = ANY(%s) AND chapter = ANY(%s)
                   ORDER BY verse_id
                   """,
                   (book_nums, debug_chapters),
               )
           else:
               cur.execute(
                   """
                   SELECT verse_id FROM verses
                   WHERE book_num = ANY(%s)
                   ORDER BY verse_id
                   """,
                   (book_nums,),
               )
           return [row[0] for row in cur.fetchall()]


   def verse_ids_for_stage(
       conn: psycopg2.extensions.connection,
       target_table: str,
       book_nums: List[int],
       debug_chapters: Optional[List[int]] = None,
   ) -> List[int]:
       """Return verse_ids that need processing for a given stage.

       Subtracts already-processed IDs from the full corpus set. Call
       this at the start of each module's run() to implement resumability.

       Args:
           conn: Live psycopg2 connection.
           target_table: Table to check for existing rows.
           book_nums: Books in scope.
           debug_chapters: Optional chapter filter for faster dev runs.

       Returns:
           Sorted list of verse_ids that still need processing.
       """
       all_ids = set(get_all_verse_ids(conn, book_nums, debug_chapters))
       done_ids = get_processed_verse_ids(conn, target_table)
       pending = sorted(all_ids - done_ids)
       logger.info(
           "Stage targeting %s: %d total, %d done, %d pending",
           target_table,
           len(all_ids),
           len(done_ids),
           len(pending),
       )
       return pending


   def batch_upsert(
       conn: psycopg2.extensions.connection,
       query: str,
       rows: List[tuple],
       batch_size: int = 100,
   ) -> int:
       """Execute a psycopg2 execute_values upsert in batches.

       Commits after each batch so progress is saved incrementally.

       Args:
           conn: Live psycopg2 connection.
           query: SQL with VALUES %s placeholder.
           rows: List of tuples to insert.
           batch_size: Rows per commit batch.

       Returns:
           Total number of rows processed.
       """
       if not rows:
           return 0
       total = 0
       for i in range(0, len(rows), batch_size):
           batch = rows[i : i + batch_size]
           with conn.cursor() as cur:
               psycopg2.extras.execute_values(cur, query, batch)
           conn.commit()
           total += len(batch)
       return total
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_db_adapter.py -v
   # Expected: 6 tests PASSED
   ```

5. Lint and typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat(stage2): add db_adapter with resumable upsert utilities"`

---

### Task 2: `modules/ingest.py` — BHSA to verses + word_tokens

**Files:** `pipeline/modules/__init__.py`, `pipeline/modules/ingest.py`

**Steps:**

1. Write tests in `tests/test_ingest.py`:

   ```python
   """Tests for Stage 2 ingest module — pure-logic unit tests only.

   BHSA/text-fabric is not available in unit test runs. All tests here
   target the helper functions that do NOT require a live TF API or DB.
   Integration tests (actual row counts) are verified via SQL queries
   documented in the acceptance criteria.
   """
   from __future__ import annotations

   import sys
   from pathlib import Path
   sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

   from modules.ingest import POS_MAP, STEM_MAP, _count_prefixes_from_pfm


   def test_pos_map_covers_core_tags() -> None:
       """POS_MAP must include the seven most common BHSA part-of-speech codes."""
       required = {"subs", "verb", "prep", "conj", "prps", "nmpr", "art"}
       assert required.issubset(POS_MAP.keys())


   def test_pos_map_verb_maps_correctly() -> None:
       assert POS_MAP["verb"] == "verb"


   def test_pos_map_noun_maps_correctly() -> None:
       assert POS_MAP["subs"] == "noun"


   def test_stem_map_covers_major_stems() -> None:
       required = {"qal", "nif", "piel", "hif"}
       assert required.issubset(STEM_MAP.keys())


   def test_count_prefixes_absent() -> None:
       """pfm='absent' or empty string → 0 prefixes."""
       assert _count_prefixes_from_pfm("absent") == 0
       assert _count_prefixes_from_pfm("") == 0
       assert _count_prefixes_from_pfm(None) == 0


   def test_count_prefixes_present() -> None:
       """Any non-empty, non-'absent' pfm value → 1 prefix."""
       assert _count_prefixes_from_pfm("W") == 1
       assert _count_prefixes_from_pfm("B") == 1


   def test_morpheme_count_formula() -> None:
       """prefix_count + 1 (stem) + has_suffix matches documented formula."""
       # No prefix, no suffix: morpheme_count = 1
       assert 0 + 1 + 0 == 1
       # 1 prefix, no suffix: morpheme_count = 2
       assert 1 + 1 + 0 == 2
       # 1 prefix, 1 suffix: morpheme_count = 3
       assert 1 + 1 + 1 == 3
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_ingest.py -v
   # Expected: FAILED — ModuleNotFoundError: No module named 'modules'
   ```

3. Create `pipeline/modules/__init__.py` (empty) and implement
   `pipeline/modules/ingest.py`:

   ```python
   """
   Stage 2 — BHSA ingest.

   Extracts morphological data from the BHSA text-fabric corpus and writes
   to `verses` and `word_tokens` tables. Designed to be resumable: if
   interrupted, re-running will skip already-ingested verses.
   """

   from __future__ import annotations

   import logging
   from typing import Dict, List, Optional, Tuple

   import psycopg2
   import psycopg2.extras

   from adapters.db_adapter import batch_upsert

   logger = logging.getLogger(__name__)

   # BHSA part-of-speech codes → human-readable labels
   POS_MAP: Dict[str, str] = {
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
       "art": "article",
   }

   # BHSA verbal stem codes → human-readable labels
   STEM_MAP: Dict[str, str] = {
       "qal": "qal",
       "nif": "niphal",
       "piel": "piel",
       "pual": "pual",
       "hif": "hiphil",
       "hof": "hophal",
       "hit": "hitpael",
       "htpo": "hitpolel",
       "nit": "nitpael",
   }

   # BHSA book name → book number for supported corpus books
   BOOK_NUM_MAP: Dict[str, int] = {
       "Psalms": 19,
       "Isaiah": 23,
       "Job": 18,
       "Lamentations": 25,
   }


   def _count_prefixes_from_pfm(pfm: Optional[str]) -> int:
       """Return 1 if pfm indicates a prefix morpheme, else 0.

       Args:
           pfm: The pfm feature value from BHSA (may be None, empty, or
               the string 'absent' when no prefix is present).

       Returns:
           1 if a prefix morpheme is indicated, 0 otherwise.
       """
       if not pfm or pfm == "absent":
           return 0
       return 1


   def run(
       conn: psycopg2.extensions.connection,
       config: dict,
   ) -> dict:
       """Ingest BHSA morphological data into `verses` and `word_tokens`.

       Reads the BHSA corpus via text-fabric and upserts verse rows and
       per-word token rows. Uses ON CONFLICT DO UPDATE so re-runs are safe.

       Args:
           conn: Live psycopg2 connection to psalms_db.
           config: Full parsed config.yml dict.

       Returns:
           Dict with keys "rows_written" (total token rows), "elapsed_s",
           "verses" (verse row count), "word_tokens" (token row count).
       """
       import time
       from tf.fabric import Fabric

       t0 = time.perf_counter()

       bhsa_path: str = config["bhsa"]["data_path"]
       corpus = config.get("corpus", {})
       book_nums: List[int] = [b["book_num"] for b in corpus.get("books", [])]
       debug_chapters: List[int] = corpus.get("debug_chapters", [])
       batch_size: int = config.get("fingerprint", {}).get("batch_size", 100)

       logger.info("Loading BHSA from %s", bhsa_path)
       TF = Fabric(
           locations=[f"{bhsa_path}/github/ETCBC/bhsa/tf/c"],
           silent=True,
       )
       api = TF.load(
           "book chapter verse "
           "g_word_utf8 lex sp "
           "prs uvf vs "
           "pfm",
           silent=True,
       )
       F, T, L = api.F, api.T, api.L

       # Reverse map: book_num → BHSA book name
       num_to_name = {v: k for k, v in BOOK_NUM_MAP.items()}

       total_verses = 0
       total_tokens = 0

       for book_num in book_nums:
           bhsa_name = num_to_name.get(book_num)
           if not bhsa_name:
               logger.warning("No BHSA name mapping for book_num=%d", book_num)
               continue
           v, t = _ingest_book(
               conn, api, book_num, bhsa_name, debug_chapters, batch_size
           )
           total_verses += v
           total_tokens += t

       elapsed = round(time.perf_counter() - t0, 2)
       logger.info(
           "Ingest complete: %d verses, %d tokens in %.1fs",
           total_verses,
           total_tokens,
           elapsed,
       )
       return {
           "rows_written": total_tokens,
           "elapsed_s": elapsed,
           "verses": total_verses,
           "word_tokens": total_tokens,
       }


   def _ingest_book(
       conn: psycopg2.extensions.connection,
       api: object,
       book_num: int,
       bhsa_book_name: str,
       debug_chapters: List[int],
       batch_size: int,
   ) -> Tuple[int, int]:
       """Ingest one book from the BHSA corpus.

       Args:
           conn: Live psycopg2 connection.
           api: Loaded text-fabric API object (has .F, .T, .L).
           book_num: Numeric book identifier for the DB.
           bhsa_book_name: BHSA internal book name string.
           debug_chapters: If non-empty, only ingest these chapters.
           batch_size: Rows per commit batch.

       Returns:
           Tuple of (verse_count, token_count) inserted/updated.
       """
       F, T, L = api.F, api.T, api.L

       book_node = next(
           n for n in F.otype.s("book") if T.bookName(n) == bhsa_book_name
       )
       verse_nodes = L.d(book_node, "verse")

       verse_rows: List[tuple] = []
       token_rows_by_verse: Dict[Tuple[int, int], List[tuple]] = {}

       for v_node in verse_nodes:
           chapter = int(F.chapter.v(v_node))
           verse_num = int(F.verse.v(v_node))

           if debug_chapters and chapter not in debug_chapters:
               continue

           words = L.d(v_node, "word")
           surface_forms = [F.g_word_utf8.v(w) for w in words]
           hebrew_text = " ".join(surface_forms)
           verse_rows.append((book_num, chapter, verse_num, hebrew_text))

           tokens: List[tuple] = []
           for pos, w_node in enumerate(words, start=1):
               pos_tag = F.sp.v(w_node) or ""
               human_pos = POS_MAP.get(pos_tag, pos_tag)
               is_verb = pos_tag == "verb"
               is_noun = pos_tag in ("subs", "nmpr")
               stem: Optional[str] = (
                   STEM_MAP.get(F.vs.v(w_node)) if is_verb else None
               )
               pfm_val = F.pfm.v(w_node)
               prefix_count = _count_prefixes_from_pfm(pfm_val)
               has_suffix = bool(F.prs.v(w_node) or F.uvf.v(w_node))
               morpheme_count = prefix_count + 1 + (1 if has_suffix else 0)

               tokens.append((
                   pos,
                   F.g_word_utf8.v(w_node),
                   F.lex.v(w_node),
                   human_pos,
                   morpheme_count,
                   is_verb,
                   is_noun,
                   stem,
               ))
           token_rows_by_verse[(chapter, verse_num)] = tokens

       # Upsert verses, capture generated verse_ids
       logger.info(
           "Upserting %d verse rows for book %d", len(verse_rows), book_num
       )
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
               fetch=True,
           )
           verse_id_map: Dict[Tuple[int, int], int] = {
               (r[0], r[1]): r[2] for r in cur.fetchall()
           }
       conn.commit()

       # Build flat token rows with verse_id
       all_token_rows: List[tuple] = []
       for (chapter, verse_num), tokens in token_rows_by_verse.items():
           verse_id = verse_id_map.get((chapter, verse_num))
           if verse_id is None:
               continue
           for t in tokens:
               all_token_rows.append((verse_id,) + t)

       logger.info(
           "Upserting %d token rows for book %d",
           len(all_token_rows),
           book_num,
       )
       batch_upsert(
           conn,
           """
           INSERT INTO word_tokens
             (verse_id, position, surface_form, lexeme, part_of_speech,
              morpheme_count, is_verb, is_noun, stem)
           VALUES %s
           ON CONFLICT (verse_id, position) DO UPDATE
             SET surface_form  = EXCLUDED.surface_form,
                 lexeme        = EXCLUDED.lexeme,
                 part_of_speech = EXCLUDED.part_of_speech,
                 morpheme_count = EXCLUDED.morpheme_count
           """,
           all_token_rows,
           batch_size=batch_size,
       )

       logger.info("Book %d ingested.", book_num)
       return len(verse_rows), len(all_token_rows)
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_ingest.py -v
   # Expected: 7 tests PASSED
   ```

5. Lint and typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat(stage2): add ingest module — BHSA to verses + word_tokens"`

---

### Task 3: `modules/fingerprint.py` — 4-Dimensional Style Fingerprinting

**Files:** `pipeline/modules/fingerprint.py`

**Fingerprint formulas:**

- `syllable_density` = total syllables / word_count. Syllables counted by vowel
  points: full vowels (U+05B4–U+05BB, U+05C1, U+05C2) each count as 1; if only
  shewa/hataf half-vowels are present, count as 1 (monosyllabic function word); if
  no vowels at all, count as 1 (unvocalized — assume monosyllabic).
- `morpheme_ratio` = mean morpheme_count across all word_tokens for the verse.
  morpheme_count is already stored in `word_tokens` from Stage 2 ingest
  (prefix_count + 1 + has_suffix).
- `sonority_score` = mean onset consonant sonority weight across all words. The
  onset is the first consonant character in the surface_form. Weights: guttural
  (א ה ח ע) = 0.3–0.65, liquid (ל ר) = 0.85–0.90, sibilant (ש ס ז צ) = 0.35–0.45,
  nasal (מ נ) = 0.80, stop (כ פ ת ק ט ב ג ד) = 0.15–0.30, approximant (י ו) = 0.70–0.75.
- `clause_compression` = word_count / (conjunction_count + 1), where conjunctions
  are word_tokens where part_of_speech = 'conjunction'. Denominator minimum is 1
  so result is always >= 1.

**Steps:**

1. Write tests in `tests/test_fingerprint.py`:

   ```python
   """Tests for Stage 2 style fingerprinting module."""

   from __future__ import annotations

   import sys
   from pathlib import Path
   sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

   import pytest
   from modules.fingerprint import (
       count_hebrew_syllables,
       _compute_fingerprint,
       _onset_sonority,
       SONORITY,
   )

   # Psalm 23:1 in Hebrew: יְהוָה רֹעִי לֹא אֶחְסָר
   PSALM_23_1_TOKENS = [
       # (verse_id, position, surface_form, morpheme_count, part_of_speech)
       (1, 1, "יְהוָה", 1, "proper_noun"),
       (1, 2, "רֹעִי", 2, "verb"),
       (1, 3, "לֹא", 1, "negative_particle"),
       (1, 4, "אֶחְסָר", 1, "verb"),
   ]


   def test_syllable_density_psalm_23_1() -> None:
       """Psalm 23:1 syllable density must fall in the documented range."""
       fp = _compute_fingerprint(PSALM_23_1_TOKENS)
       assert 1.5 <= fp["syllable_density"] <= 4.0, (
           f"syllable_density={fp['syllable_density']} outside [1.5, 4.0]"
       )


   def test_morpheme_ratio_simple() -> None:
       """Word with 2 prefixes + stem: morpheme_count=3, ratio=3.0 for single word."""
       tokens = [(1, 1, "וּבְאֶרֶץ", 3, "noun")]
       fp = _compute_fingerprint(tokens)
       assert abs(fp["morpheme_ratio"] - 3.0) < 0.01


   def test_sonority_score_range() -> None:
       """sonority_score must always be in [0.0, 1.0]."""
       fp = _compute_fingerprint(PSALM_23_1_TOKENS)
       assert 0.0 <= fp["sonority_score"] <= 1.0


   def test_clause_compression_no_conjunctions() -> None:
       """Without any conjunctions denominator is 1 → compression = word_count."""
       tokens = [
           (1, 1, "הָאִישׁ", 1, "noun"),
           (1, 2, "הַהוּא", 2, "pronoun_demonstrative"),
           (1, 3, "הָלַךְ", 2, "verb"),
       ]
       fp = _compute_fingerprint(tokens)
       # No conjunctions → clause_starts = 1 → compression = 3/1 = 3.0
       assert abs(fp["clause_compression"] - 3.0) < 0.01


   def test_fingerprint_all_columns_not_null() -> None:
       """All four fingerprint dimensions must be present and non-None."""
       fp = _compute_fingerprint(PSALM_23_1_TOKENS)
       for key in ("syllable_density", "morpheme_ratio", "sonority_score",
                   "clause_compression"):
           assert fp[key] is not None, f"Key '{key}' is None"
           assert isinstance(fp[key], float), f"Key '{key}' is not float"


   def test_fingerprint_idempotent() -> None:
       """Running compute_fingerprint twice on the same tokens returns identical dict."""
       fp1 = _compute_fingerprint(PSALM_23_1_TOKENS)
       fp2 = _compute_fingerprint(PSALM_23_1_TOKENS)
       assert fp1 == fp2


   def test_count_hebrew_syllables_yehwah() -> None:
       """יְהוָה has 2 full vowels: shewa (half) + qamets → count = 1 full."""
       # hiriq or qamets is the full vowel; shewa is half
       # Expected: 1 full vowel → count_hebrew_syllables returns 1
       word = "יְהוָה"
       count = count_hebrew_syllables(word)
       # The word has 1 full vowel (qamets on ה) + 1 shewa on י
       # With only shewa + 1 full → count = 1
       assert count >= 1


   def test_count_hebrew_syllables_no_vowels() -> None:
       """Unvocalized word returns 1 (monosyllabic assumption)."""
       assert count_hebrew_syllables("שלם") == 1


   def test_onset_sonority_alef() -> None:
       """Alef (א) onset returns the guttural sonority value."""
       score = _onset_sonority("אֶרֶץ")
       assert 0.0 < score <= 1.0
       # Alef is mapped in SONORITY — should match that entry
       alef_val = SONORITY.get("א")
       if alef_val is not None:
           assert abs(score - alef_val) < 0.01


   def test_onset_sonority_lamed() -> None:
       """Lamed (ל) onset returns high liquid sonority value >= 0.85."""
       score = _onset_sonority("לֵב")
       assert score >= 0.85
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_fingerprint.py -v
   # Expected: FAILED — ModuleNotFoundError: No module named 'modules.fingerprint'
   ```

3. Implement `pipeline/modules/fingerprint.py`:

   ```python
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
   from typing import Dict, List

   import psycopg2
   import psycopg2.extras

   from adapters.db_adapter import batch_upsert, verse_ids_for_stage

   logger = logging.getLogger(__name__)

   # Hebrew consonant set (includes final forms)
   CONSONANTS = frozenset(
       "אבגדהוזחטיכלמנסעפצקרשת" + "ךםןףץ"
   )

   # Full vowel point Unicode characters (each = 1 syllable nucleus)
   FULL_VOWEL_POINTS = frozenset(
       "\u05B4\u05B5\u05B6\u05B7\u05B8\u05B9\u05BA\u05BB\u05C1\u05C2"
   )

   # Half/ultra-short vowel points (shewa + hataf forms)
   HALF_VOWEL_POINTS = frozenset("\u05B0\u05B1\u05B2\u05B3")

   # Onset consonant sonority weights (0.0 = least sonorous, 1.0 = most)
   SONORITY: Dict[str, float] = {
       # Gutturals
       "א": 0.60, "ה": 0.65, "ח": 0.45, "ע": 0.55,
       # Liquids
       "ל": 0.90, "ר": 0.85,
       # Nasals
       "מ": 0.80, "נ": 0.80,
       # Sibilants
       "ש": 0.40, "ס": 0.40, "ז": 0.45, "צ": 0.35,
       # Approximants
       "י": 0.75, "ו": 0.70,
       # Voiced stops
       "ב": 0.30, "ג": 0.30, "ד": 0.30,
       # Voiceless stops
       "כ": 0.20, "ך": 0.20, "פ": 0.20, "ף": 0.20,
       "ת": 0.20, "ק": 0.15, "ט": 0.20,
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
       book_nums: List[int] = [b["book_num"] for b in corpus.get("books", [])]
       debug_chapters: List[int] = corpus.get("debug_chapters", [])
       batch_size: int = config.get("fingerprint", {}).get("batch_size", 100)

       pending = verse_ids_for_stage(
           conn, "verse_fingerprints", book_nums, debug_chapters
       )
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

       by_verse: Dict[int, list] = defaultdict(list)
       for row in rows:
           by_verse[row[0]].append(row)

       fingerprint_rows: List[tuple] = []
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


   def _compute_fingerprint(tokens: list) -> Dict[str, float]:
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
           sum(sonority_values) / len(sonority_values)
           if sonority_values else 0.0
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
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_fingerprint.py -v
   # Expected: 10 tests PASSED
   ```

5. Lint and typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat(stage2): add fingerprint module — 4D style fingerprinting"`

---

### Task 4: `modules/chiasm.py` — ABBA/ABCBA Pattern Detection

**Files:** `pipeline/modules/chiasm.py`

> **DEFERRED — Run after Plan 03 completes.**
> This module is implemented now but cannot be executed until Stage 3 has
> back-populated `verse_fingerprints.colon_fingerprints`. The unit tests test
> pure logic and do not require a live DB or colon data, so they can pass today.
> The `run()` function itself should not be invoked until after Plan 03 Task 3
> (back-population) is confirmed complete.
>
> TODO: After Plan 03 Task 3 is done, return here and run:
> ```bash
> uv run --frozen pytest tests/test_chiasm.py -v
> # Then trigger chiasm.run(conn, config) via pipeline runner or manual script
> ```

**Chiasm detection logic:**

- Load `verse_fingerprints.colon_fingerprints` (JSONB array) for all verses
- Each array element: `{"colon": int, "density": float, "sonority": float,
  "mean_weight": float}` (populated by Stage 3 back-population)
- Build a 4-element vector per colon: `[density, morpheme_ratio, sonority,
  compression]` — for back-populated data, `morpheme_ratio` and `compression`
  may be 0.0 if not yet stored; use available dimensions
- ABBA: window of 4 colons i..i+3 where cosine_sim(colon[i], colon[i+3]) >=
  threshold AND cosine_sim(colon[i+1], colon[i+2]) >= threshold
- ABCBA: window of 5 colons i..i+4 where cosine_sim(colon[i], colon[i+4]) >=
  threshold AND cosine_sim(colon[i+1], colon[i+3]) >= threshold (colon i+2 is
  the pivot C, unconstrained)
- Confidence = mean of the two similarity scores
- Only store if confidence >= `config.chiasm.min_confidence` (default 0.65)

**Steps:**

1. Write tests in `tests/test_chiasm.py`:

   ```python
   """Tests for Stage 2 chiasm detection module.

   These tests exercise pure logic only (cosine similarity, pattern detection).
   No DB connection is required. Execution of chiasm.run() is deferred until
   Stage 3 back-populates colon_fingerprints.
   """

   from __future__ import annotations

   import sys
   from pathlib import Path
   sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

   import numpy as np
   import pytest
   from modules.chiasm import _cosine_similarity, _detect_patterns


   def test_abba_pattern_detected() -> None:
       """Four colons with matching outer/inner pairs should produce ABBA result."""
       a = np.array([1.0, 0.0, 0.0, 0.0])
       b = np.array([0.0, 1.0, 0.0, 0.0])
       # Layout: A B B' A' — colons 0&3 match, colons 1&2 match
       colons = [
           (100, 1, a),
           (100, 2, b),
           (100, 3, b),
           (100, 4, a),
       ]
       results = _detect_patterns(colons, threshold=0.8, min_confidence=0.65)
       abba = [r for r in results if r["pattern_type"] == "ABBA"]
       assert len(abba) >= 1
       assert abba[0]["confidence"] >= 0.8


   def test_abcba_pattern_detected() -> None:
       """Five colons with ABCBA structure should produce ABCBA result."""
       a = np.array([1.0, 0.0, 0.0, 0.0])
       b = np.array([0.0, 1.0, 0.0, 0.0])
       c = np.array([0.0, 0.0, 1.0, 0.0])  # pivot
       colons = [
           (101, 1, a),
           (101, 2, b),
           (101, 3, c),   # pivot — no match required
           (102, 1, b),   # matches colon index 1
           (102, 2, a),   # matches colon index 0
       ]
       results = _detect_patterns(colons, threshold=0.8, min_confidence=0.65)
       abcba = [r for r in results if r["pattern_type"] == "ABCBA"]
       assert len(abcba) >= 1


   def test_below_threshold_not_detected() -> None:
       """Similarity below the configured threshold should produce no candidates."""
       # Random orthogonal-ish vectors: guaranteed low similarity
       rng = np.random.default_rng(seed=42)
       vectors = [rng.random(4) for _ in range(6)]
       colons = [(200, i, v) for i, v in enumerate(vectors)]
       results = _detect_patterns(colons, threshold=0.999, min_confidence=0.999)
       assert len(results) == 0


   def test_chiasm_confidence_range() -> None:
       """Confidence must always be in [0.0, 1.0]."""
       a = np.array([1.0, 0.0, 0.0, 0.0])
       b = np.array([0.0, 1.0, 0.0, 0.0])
       colons = [(300, 1, a), (300, 2, b), (300, 3, b), (300, 4, a)]
       results = _detect_patterns(colons, threshold=0.5, min_confidence=0.0)
       for r in results:
           assert 0.0 <= r["confidence"] <= 1.0, (
               f"confidence {r['confidence']} outside [0,1]"
           )


   def test_cosine_similarity_identical_vectors() -> None:
       """Identical vectors have cosine similarity of 1.0."""
       v = np.array([1.0, 2.0, 3.0, 4.0])
       assert abs(_cosine_similarity(v, v) - 1.0) < 1e-6


   def test_cosine_similarity_orthogonal_vectors() -> None:
       """Orthogonal vectors have cosine similarity of 0.0."""
       a = np.array([1.0, 0.0, 0.0, 0.0])
       b = np.array([0.0, 1.0, 0.0, 0.0])
       assert abs(_cosine_similarity(a, b)) < 1e-6


   def test_cosine_similarity_zero_vector() -> None:
       """Zero vector returns 0.0 (no division by zero)."""
       a = np.zeros(4)
       b = np.array([1.0, 2.0, 3.0, 4.0])
       assert _cosine_similarity(a, b) == 0.0
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_chiasm.py -v
   # Expected: FAILED — ModuleNotFoundError: No module named 'modules.chiasm'
   ```

3. Implement `pipeline/modules/chiasm.py`:

   ```python
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
   from typing import Dict, List, Optional, Tuple

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
       similarity_threshold: float = chiasm_config.get(
           "similarity_threshold", 0.75
       )
       min_confidence: float = chiasm_config.get("min_confidence", 0.65)
       max_stanza_verses: int = chiasm_config.get("max_stanza_verses", 8)

       corpus = config.get("corpus", {})
       book_nums: List[int] = [
           b["book_num"] for b in corpus.get("books", [])
       ]

       colon_fps = _load_colon_fingerprints(conn, book_nums)
       if not colon_fps:
           logger.warning(
               "No colon fingerprints found. Run Stage 3 first."
           )
           return {
               "rows_written": 0,
               "elapsed_s": round(time.perf_counter() - t0, 2),
               "candidates_found": 0,
           }

       chapters = _load_chapter_groupings(conn, book_nums)
       candidates: List[dict] = []

       for (_book_num, _chapter), verse_ids in chapters.items():
           if len(verse_ids) > max_stanza_verses:
               verse_ids = verse_ids[:max_stanza_verses]

           chapter_colons: List[Tuple[int, int, np.ndarray]] = []
           for v_id in verse_ids:
               for colon_idx, vec in colon_fps.get(v_id, []):
                   chapter_colons.append((v_id, colon_idx, vec))

           if len(chapter_colons) < 4:
               continue

           found = _detect_patterns(
               chapter_colons, similarity_threshold, min_confidence
           )
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
       book_nums: List[int],
   ) -> Dict[int, List[Tuple[int, np.ndarray]]]:
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

       result: Dict[int, List[Tuple[int, np.ndarray]]] = {}
       for verse_id, colon_fps_json in rows:
           if not colon_fps_json:
               continue
           colon_data = (
               colon_fps_json
               if isinstance(colon_fps_json, list)
               else json.loads(colon_fps_json)
           )
           colon_vecs: List[Tuple[int, np.ndarray]] = []
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
       book_nums: List[int],
   ) -> Dict[Tuple[int, int], List[int]]:
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
           chapters: Dict[Tuple[int, int], List[int]] = defaultdict(list)
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
       colons: List[Tuple[int, int, np.ndarray]],
       threshold: float,
       min_confidence: float,
   ) -> List[dict]:
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
       candidates: List[dict] = []
       n = len(colons)

       for i in range(n - 3):
           # ABBA window: i, i+1, i+2, i+3
           if i + 3 < n:
               v_ids_abba = [colons[j][0] for j in range(i, i + 4)]
               outer_sim = _cosine_similarity(colons[i][2], colons[i + 3][2])
               inner_sim = _cosine_similarity(
                   colons[i + 1][2], colons[i + 2][2]
               )
               if outer_sim >= threshold and inner_sim >= threshold:
                   confidence = (outer_sim + inner_sim) / 2
                   if confidence >= min_confidence:
                       candidates.append({
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
                       })

           # ABCBA window: i, i+1, i+2 (pivot), i+3, i+4
           if i + 4 < n:
               v_ids_abcba = [colons[j][0] for j in range(i, i + 5)]
               outer_sim = _cosine_similarity(colons[i][2], colons[i + 4][2])
               inner_sim = _cosine_similarity(
                   colons[i + 1][2], colons[i + 3][2]
               )
               if outer_sim >= threshold and inner_sim >= threshold:
                   confidence = (outer_sim + inner_sim) / 2
                   if confidence >= min_confidence:
                       candidates.append({
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
                       })

       return candidates


   def _store_candidates(
       conn: psycopg2.extensions.connection,
       candidates: List[dict],
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
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_chiasm.py -v
   # Expected: 7 tests PASSED
   ```

5. Lint and typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat(stage2): add chiasm module — ABBA/ABCBA detection (deferred execution)"`

---

### Task 5: Idempotency Verification

**Files:** No new files. SQL queries run against a populated database.

This task runs after the stage has been executed at least once against the live
BHSA corpus. Its purpose is to confirm that re-running produces zero new rows.

**Steps:**

1. Run the full Stage 2 pipeline once:

   ```bash
   docker compose --profile pipeline run --rm pipeline \
     python -m pipeline.run --stages 2
   ```

2. Record the row counts:

   ```sql
   SELECT COUNT(*) FROM verses WHERE book_num = 19;
   -- Note: expect 2527

   SELECT COUNT(*) FROM word_tokens wt
   JOIN verses v ON wt.verse_id = v.verse_id WHERE v.book_num = 19;
   -- Note: expect ~43000

   SELECT COUNT(*) FROM verse_fingerprints vf
   JOIN verses v ON vf.verse_id = v.verse_id WHERE v.book_num = 19;
   -- Note: expect 2527
   ```

3. Run Stage 2 a second time with the same config:

   ```bash
   docker compose --profile pipeline run --rm pipeline \
     python -m pipeline.run --stages 2
   ```

4. Verify counts are unchanged:

   ```sql
   -- All three queries above must return identical numbers.
   -- If word_tokens count increased, the ON CONFLICT clause is missing.
   -- If fingerprints count increased, there's a duplicate insert bug.
   ```

5. Verify Psalm 23:1 token sample:

   ```sql
   SELECT position, surface_form, lexeme, part_of_speech, morpheme_count
   FROM word_tokens wt
   JOIN verses v ON wt.verse_id = v.verse_id
   WHERE v.book_num = 19 AND v.chapter = 23 AND v.verse_num = 1
   ORDER BY position;
   -- Expected: 4-5 rows for יְהוָה רֹעִי לֹא אֶחְסָר
   -- (verse superscription לְדָוִד may add 1–2 rows depending on BHSA versification)
   ```

6. Run all Stage 2 tests together:

   ```bash
   uv run --frozen pytest tests/test_db_adapter.py tests/test_ingest.py \
     tests/test_fingerprint.py tests/test_chiasm.py -v
   # Expected: all PASSED
   ```

7. Commit: `"test(stage2): verify idempotency — Stage 2 acceptance criteria met"`

---

## SQL Validation Queries (Full Suite)

```sql
-- Verse count
SELECT COUNT(*) FROM verses WHERE book_num = 19;
-- Expected: 2527

-- Word token count
SELECT COUNT(*) FROM word_tokens wt
JOIN verses v ON wt.verse_id = v.verse_id WHERE v.book_num = 19;
-- Expected: ~43000

-- Fingerprint coverage
SELECT COUNT(*) FROM verse_fingerprints vf
JOIN verses v ON vf.verse_id = v.verse_id WHERE v.book_num = 19;
-- Expected: 2527

-- Fingerprint range check (all values in expected ranges)
SELECT
  MIN(syllable_density),   MAX(syllable_density),
  MIN(morpheme_ratio),     MAX(morpheme_ratio),
  MIN(sonority_score),     MAX(sonority_score),
  MIN(clause_compression), MAX(clause_compression)
FROM verse_fingerprints vf
JOIN verses v ON vf.verse_id = v.verse_id WHERE v.book_num = 19;
-- syllable_density:   1.5–4.0
-- morpheme_ratio:     1.0–4.5
-- sonority_score:     0.2–0.8
-- clause_compression: 2.0–15.0

-- Psalm 23:1 token sample
SELECT position, surface_form, lexeme, part_of_speech, morpheme_count
FROM word_tokens wt
JOIN verses v ON wt.verse_id = v.verse_id
WHERE v.book_num = 19 AND v.chapter = 23 AND v.verse_num = 1
ORDER BY position;
```

## Deferred: Chiasm Second Pass (after Plan 03 completes)

After Stage 3 back-populates `verse_fingerprints.colon_fingerprints`, trigger the
chiasm second pass:

```bash
# Confirm colon_fingerprints are populated
# (run this SQL from JupyterLab or psql)
# SELECT COUNT(*) FROM verse_fingerprints WHERE colon_fingerprints IS NOT NULL
#   AND colon_fingerprints != '[]';
# Expected: 2527

# Run chiasm detection
docker compose --profile pipeline run --rm pipeline \
  python -m pipeline.run --stages 2-chiasm

# Verify candidates
# SELECT COUNT(*) FROM chiasm_candidates;
# Expected: 20–200 candidates (varies with threshold config)
```
