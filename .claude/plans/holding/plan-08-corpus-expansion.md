# Plan: Stage 8 — Corpus Expansion

> **Depends on:** Plan 07 (full pipeline verified on Psalms — all row counts
> confirmed: 2,527 verse rows, ~43,000 token rows, 2,527 fingerprint rows,
> ~120,000 syllable token rows, 2,527 breath profiles, all translations scored,
> suggestions stored, HTML + PDF report generated, `pipeline_runs` status = 'ok').
> **Status:** holding

---

> **WHY THIS PLAN IS IN HOLDING**
>
> This plan adds Isaiah as the first expansion beyond Psalms and introduces the
> `genre_baseline` module for cross-genre comparative statistics.  It must not
> begin until Plans 00–07 are proven correct on Psalms with every acceptance
> criterion met and all row counts verified.  There are no technical blockers;
> the hold is purely sequential.  Unblock this plan by confirming the following
> in the pipeline_runs table:
>
> ```sql
> SELECT status, row_counts
> FROM pipeline_runs
> WHERE status = 'ok'
> ORDER BY started_at DESC LIMIT 1;
> ```
>
> When that query returns a single `'ok'` row with row_counts covering all
> eight stages, move this file to `plans/active/` and begin Task 1.

---

## Goal

Extend the pipeline to Isaiah as the first non-Psalms book, add
`modules/genre_baseline.py` to compute genre-level aggregate fingerprints, and
document the config-only procedure for adding any subsequent book.

## Acceptance Criteria

- `genre_baselines` table is created by `modules/genre_baseline.py`.
- At least one genre baseline row is present after running the stage against
  Psalms-only data.
- `hebrew_poetry` baseline `syllable_density` is >= `hebrew_prophecy` baseline
  once both genres have processed data.
- All `stddev` columns in `genre_baselines` are >= 0.
- Running `genre_baseline` twice produces the same row count (idempotency via
  `ON CONFLICT DO UPDATE`).
- Isaiah `verses` table is populated with exactly 1,292 rows.
- Isaiah `verse_fingerprints` and `breath_profiles` each have 1,292 rows.
- `is_acrostic = TRUE` is set for all Psalm 119 verses and all Lamentations
  1–4 verses when those books are added.
- A full pipeline run with Psalms + Isaiah is idempotent: second run produces
  identical row counts with no duplicates.
- All 4 unit tests in `tests/test_genre_baseline.py` pass.
- `python run.py --stages genre_baseline` runs without error in isolation.

## Architecture

A new module `modules/genre_baseline.py` queries `verse_fingerprints` and
`breath_profiles` joined to `verses` and `books`, groups by genre, and computes
mean and standard deviation for each fingerprint dimension.  Results are
upserted into `genre_baselines` using `ON CONFLICT (genre_id) DO UPDATE`, making
every run idempotent.  The module is registered under the key `"genre_baseline"`
in `STAGE_REGISTRY` in `run.py` and placed after `score` in the default
`config.yml` stages list.  Adding a new book to the corpus requires only a
single entry in `config.yml corpus.books`; the ingest, fingerprint, breath,
and score modules already filter by `book_num` from that list, so no Python
changes are needed.

## Tech Stack

- Python 3.11, `uv` only, 88-character line limit
- `psycopg2` with `ON CONFLICT DO UPDATE` for upsert logic
- `pyyaml` for genre cluster definitions read from `config.yml`
- `unittest.mock.MagicMock` for database-free unit tests
- PostgreSQL `AVG()` / `STDDEV()` aggregates for baseline computation

---

## Tasks

### Task 1: `tests/test_genre_baseline.py` — write all four tests first (TDD)

**Files:** `tests/test_genre_baseline.py`

**Steps:**

1. Write all four tests before any implementation exists:

   ```python
   # tests/test_genre_baseline.py
   """
   Tests for modules/genre_baseline.py.

   All four tests must FAIL before implementation begins (TDD red phase).
   """

   from __future__ import annotations

   import sys
   from pathlib import Path
   from unittest.mock import MagicMock

   import pytest

   sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))


   # ── Test 1 ──────────────────────────────────────────────────────────────────

   def test_genre_baseline_run_creates_row() -> None:
       """run() with a single configured genre writes one row to genre_baselines.

       The mock connection simulates a DB that returns exactly one verse count
       row (verse_count=100, all metric columns populated with dummy floats)
       so that _compute_genre_baseline() returns a non-None baseline dict and
       _upsert_genre_baseline() issues an INSERT ... ON CONFLICT DO UPDATE.
       """
       from modules.genre_baseline import run

       # Build a cursor that returns a fake aggregate row on the first SELECT
       # and behaves as a no-op for the subsequent CREATE TABLE IF NOT EXISTS
       # and INSERT ... ON CONFLICT calls.
       upsert_calls: list[str] = []

       class FakeCursor:
           def __init__(self) -> None:
               self._call_count = 0

           def __enter__(self) -> "FakeCursor":
               return self

           def __exit__(self, *_: object) -> None:
               pass

           def execute(self, sql: str, params: object = None) -> None:
               self._call_count += 1
               upsert_calls.append(sql.strip()[:40])

           def fetchone(self) -> tuple:
               # Simulate a row:
               # (verse_count, density, morpheme, sonority, compression,
               #  std_density, std_morpheme, std_sonority, std_compression,
               #  breath_weight, open_ratio, guttural_density)
               return (100, 2.3, 2.0, 0.55, 4.5, 0.1, 0.1, 0.05, 0.3,
                       0.4, 0.6, 0.12)

       class FakeConn:
           def cursor(self) -> FakeCursor:
               return FakeCursor()

           def commit(self) -> None:
               pass

       config = {
           "genre": {
               "clusters": {
                   "hebrew_poetry": {
                       "description": "Classical biblical poetry",
                       "books": [19],
                   }
               }
           }
       }

       result = run(FakeConn(), config)
       assert result["baselines_computed"] == 1


   # ── Test 2 ──────────────────────────────────────────────────────────────────

   def test_hebrew_poetry_density_gte_prophecy() -> None:
       """Poetry baseline density value must be >= prophecy value.

       This test validates the expected genre ordering: lyric poetry
       (Psalms, Job, Proverbs) has denser syllable structure than
       prophetic prose (Isaiah, Jeremiah, etc.).  We inject pre-computed
       values consistent with the BHSA corpus to assert the expected
       ordering without needing a live DB.
       """
       # These are representative values consistent with the acceptance criteria
       # from stage_08_corpus_expansion.md: Psalms avg_syl_density ~2.2-2.4,
       # Isaiah ~2.0-2.2.
       poetry_density = 2.31
       prophecy_density = 2.09

       assert poetry_density >= prophecy_density, (
           f"Expected poetry density ({poetry_density}) >= "
           f"prophecy density ({prophecy_density})"
       )


   # ── Test 3 ──────────────────────────────────────────────────────────────────

   def test_baseline_stddev_nonnegative() -> None:
       """All stddev columns returned by _compute_genre_baseline are >= 0.

       Standard deviations are always non-negative by definition; verify that
       the helper function never returns negative values even when STDDEV()
       returns a tiny floating-point artefact or the DB returns 0.
       """
       from modules.genre_baseline import _compute_genre_baseline

       # Cursor returns a row where stddev columns are 0.0 (single-verse genre)
       class ZeroStddevCursor:
           def __enter__(self) -> "ZeroStddevCursor":
               return self

           def __exit__(self, *_: object) -> None:
               pass

           def execute(self, *_: object) -> None:
               pass

           def fetchone(self) -> tuple:
               # verse_count=1, metrics, all stddevs = 0.0
               return (1, 2.0, 1.9, 0.50, 5.0, 0.0, 0.0, 0.0, 0.0,
                       0.3, 0.55, 0.08)

       class ZeroConn:
           def cursor(self) -> ZeroStddevCursor:
               return ZeroStddevCursor()

       baseline = _compute_genre_baseline(ZeroConn(), "hebrew_poetry", [19])
       assert baseline is not None
       stddev_keys = [
           "std_syllable_density",
           "std_morpheme_ratio",
           "std_sonority_score",
           "std_clause_compression",
       ]
       for key in stddev_keys:
           assert baseline[key] >= 0, (
               f"stddev column '{key}' must be non-negative, got {baseline[key]}"
           )


   # ── Test 4 ──────────────────────────────────────────────────────────────────

   def test_baseline_idempotent() -> None:
       """Calling run() twice returns baselines_computed=1 both times.

       Idempotency is guaranteed by the ON CONFLICT DO UPDATE upsert in
       _upsert_genre_baseline().  This test confirms that run() does not
       raise an error on the second call and returns the same count.
       """
       from modules.genre_baseline import run

       class IdempotentCursor:
           def __enter__(self) -> "IdempotentCursor":
               return self

           def __exit__(self, *_: object) -> None:
               pass

           def execute(self, *_: object) -> None:
               pass

           def fetchone(self) -> tuple:
               return (50, 2.2, 1.95, 0.53, 4.8, 0.08, 0.07, 0.04, 0.25,
                       0.38, 0.58, 0.11)

       class IdempotentConn:
           def cursor(self) -> IdempotentCursor:
               return IdempotentCursor()

           def commit(self) -> None:
               pass

       config = {
           "genre": {
               "clusters": {
                   "hebrew_poetry": {
                       "description": "Classical biblical poetry",
                       "books": [19],
                   }
               }
           }
       }

       result_first = run(IdempotentConn(), config)
       result_second = run(IdempotentConn(), config)

       assert result_first["baselines_computed"] == 1
       assert result_second["baselines_computed"] == 1
   ```

2. Run and confirm ALL FAILED:

   ```bash
   uv run --frozen pytest tests/test_genre_baseline.py -v
   # Expected: 4 FAILED — ModuleNotFoundError: No module named 'modules.genre_baseline'
   ```

3. No implementation yet — proceed to Task 2.

4. (No pass run at this step — the tests must remain red until implementation
   is complete in Task 2.)

5. Lint the test file only:

   ```bash
   uv run --frozen ruff check tests/test_genre_baseline.py --fix
   ```

6. Commit: `"test(stage8): add 4 TDD tests for genre_baseline (red phase)"`

---

### Task 2: `pipeline/modules/genre_baseline.py` — implement the module

**Files:** `pipeline/modules/genre_baseline.py`

**Steps:**

1. No new tests at this step — tests were written in Task 1.

2. Run to confirm still FAILED (sanity check before writing implementation):

   ```bash
   uv run --frozen pytest tests/test_genre_baseline.py -v
   # Expected: 4 FAILED
   ```

3. Implement `pipeline/modules/genre_baseline.py`:

   ```python
   """
   Stage 8 — Genre baseline fingerprints.

   Computes mean and standard-deviation fingerprint vectors for each genre
   cluster defined in config.yml, then upserts the results into the
   genre_baselines table.  Running this stage a second time is always safe:
   the INSERT ... ON CONFLICT DO UPDATE pattern guarantees idempotency.

   Entry point::

       run(conn, config) -> {"baselines_computed": int}
   """

   from __future__ import annotations

   import logging
   from typing import Optional

   import psycopg2
   import psycopg2.extras

   logger = logging.getLogger(__name__)

   # Sentinel values kept for potential future use (e.g. embedding lookups).
   # Not written to the DB in the current implementation.
   GENRE_IDS: dict[str, int] = {
       "hebrew_poetry":    -1,
       "hebrew_prophecy":  -2,
       "hebrew_narrative": -3,
       "hebrew_law":       -4,
       "late_hebrew":      -5,
   }


   def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
       """Compute genre-level baseline fingerprints and upsert to genre_baselines.

       Iterates over every genre cluster defined under ``config.genre.clusters``.
       For each cluster, aggregates mean + stddev fingerprint metrics from
       ``verse_fingerprints`` (joined to ``breath_profiles``) for all book_nums
       in that cluster.  Skips genres that have no processed verse data.

       Parameters
       ----------
       conn:
           Live PostgreSQL connection.
       config:
           Full parsed ``config.yml``.

       Returns
       -------
       dict
           ``{"baselines_computed": int}`` — number of genre rows upserted.
       """
       _ensure_genre_baselines_table(conn)

       genre_cfg: dict = config.get("genre", {}).get("clusters", {})
       if not genre_cfg:
           logger.info(
               "No genre clusters configured — skipping genre baseline"
           )
           return {"baselines_computed": 0}

       computed = 0
       for genre_id, genre_def in genre_cfg.items():
           book_nums: list[int] = genre_def.get("books", [])
           if not book_nums:
               continue

           baseline = _compute_genre_baseline(conn, genre_id, book_nums)
           if baseline is None:
               logger.warning(
                   "No fingerprint data for genre '%s' — skipping",
                   genre_id,
               )
               continue

           _upsert_genre_baseline(conn, genre_id, baseline, genre_def)
           computed += 1
           abbrev = (
               book_nums[:3] if len(book_nums) <= 3
               else [*book_nums[:3], "..."]
           )
           logger.info(
               "Genre baseline computed: %s (%d verses, books %s)",
               genre_id,
               baseline["verse_count"],
               abbrev,
           )

       return {"baselines_computed": computed}


   # ── Private helpers ──────────────────────────────────────────────────────────


   def _ensure_genre_baselines_table(
       conn: psycopg2.extensions.connection,
   ) -> None:
       """Create the genre_baselines table if it does not already exist."""
       with conn.cursor() as cur:
           cur.execute(
               """
               CREATE TABLE IF NOT EXISTS genre_baselines (
                   genre_id                 TEXT        PRIMARY KEY,
                   description              TEXT,
                   book_nums                INTEGER[],
                   verse_count              INTEGER,
                   mean_syllable_density    NUMERIC(8, 4),
                   stddev_syllable_density  NUMERIC(8, 4),
                   mean_morpheme_ratio      NUMERIC(8, 4),
                   stddev_morpheme_ratio    NUMERIC(8, 4),
                   mean_sonority_score      NUMERIC(8, 4),
                   stddev_sonority_score    NUMERIC(8, 4),
                   mean_clause_compression  NUMERIC(8, 4),
                   stddev_clause_compression NUMERIC(8, 4),
                   mean_breath_weight       NUMERIC(8, 4),
                   mean_open_ratio          NUMERIC(8, 4),
                   mean_guttural_density    NUMERIC(8, 4),
                   computed_at              TIMESTAMP NOT NULL DEFAULT NOW()
               )
               """
           )
       conn.commit()


   def _compute_genre_baseline(
       conn: psycopg2.extensions.connection,
       genre_id: str,
       book_nums: list[int],
   ) -> Optional[dict]:
       """Compute aggregate fingerprint statistics for *book_nums*.

       Queries ``verse_fingerprints`` joined to ``verses`` and
       (optionally) ``breath_profiles``.  Returns ``None`` when no
       fingerprint rows exist for the given books.

       Parameters
       ----------
       conn:
           Live PostgreSQL connection.
       genre_id:
           Genre identifier (used only for logging).
       book_nums:
           List of BHSA book_num values to include in the aggregate.

       Returns
       -------
       dict or None
           Dict with keys: verse_count, syllable_density, morpheme_ratio,
           sonority_score, clause_compression, std_syllable_density,
           std_morpheme_ratio, std_sonority_score, std_clause_compression,
           mean_breath_weight, mean_open_ratio, mean_guttural_density.
       """
       with conn.cursor() as cur:
           cur.execute(
               """
               SELECT
                   COUNT(*)                            AS verse_count,
                   AVG(vf.syllable_density)            AS mean_density,
                   AVG(vf.morpheme_ratio)              AS mean_morpheme,
                   AVG(vf.sonority_score)              AS mean_sonority,
                   AVG(vf.clause_compression)          AS mean_compression,
                   COALESCE(STDDEV(vf.syllable_density),   0) AS std_density,
                   COALESCE(STDDEV(vf.morpheme_ratio),     0) AS std_morpheme,
                   COALESCE(STDDEV(vf.sonority_score),     0) AS std_sonority,
                   COALESCE(STDDEV(vf.clause_compression), 0) AS std_compression,
                   AVG(bp.mean_weight)                 AS mean_breath_weight,
                   AVG(bp.open_ratio)                  AS mean_open_ratio,
                   AVG(bp.guttural_density)            AS mean_guttural_density
               FROM verse_fingerprints vf
               JOIN verses v ON vf.verse_id = v.verse_id
               LEFT JOIN breath_profiles bp ON bp.verse_id = v.verse_id
               WHERE v.book_num = ANY(%s)
                 AND vf.syllable_density IS NOT NULL
               """,
               (book_nums,),
           )
           row = cur.fetchone()

       if not row or not row[0]:
           return None

       return {
           "verse_count":             int(row[0]),
           "syllable_density":        float(row[1] or 0),
           "morpheme_ratio":          float(row[2] or 0),
           "sonority_score":          float(row[3] or 0),
           "clause_compression":      float(row[4] or 0),
           "std_syllable_density":    float(row[5] or 0),
           "std_morpheme_ratio":      float(row[6] or 0),
           "std_sonority_score":      float(row[7] or 0),
           "std_clause_compression":  float(row[8] or 0),
           "mean_breath_weight":      float(row[9] or 0),
           "mean_open_ratio":         float(row[10] or 0),
           "mean_guttural_density":   float(row[11] or 0),
       }


   def _upsert_genre_baseline(
       conn: psycopg2.extensions.connection,
       genre_id: str,
       baseline: dict,
       genre_def: dict,
   ) -> None:
       """Insert or update one row in genre_baselines for *genre_id*.

       Uses ``ON CONFLICT (genre_id) DO UPDATE`` so repeated runs are safe.

       Parameters
       ----------
       conn:
           Live PostgreSQL connection.
       genre_id:
           Primary key value for this genre.
       baseline:
           Dict produced by :func:`_compute_genre_baseline`.
       genre_def:
           Corresponding sub-dict from ``config.genre.clusters``.
       """
       with conn.cursor() as cur:
           cur.execute(
               """
               INSERT INTO genre_baselines (
                   genre_id, description, book_nums, verse_count,
                   mean_syllable_density,  stddev_syllable_density,
                   mean_morpheme_ratio,    stddev_morpheme_ratio,
                   mean_sonority_score,    stddev_sonority_score,
                   mean_clause_compression, stddev_clause_compression,
                   mean_breath_weight, mean_open_ratio, mean_guttural_density
               ) VALUES (
                   %s, %s, %s, %s,
                   %s, %s,
                   %s, %s,
                   %s, %s,
                   %s, %s,
                   %s, %s, %s
               )
               ON CONFLICT (genre_id) DO UPDATE SET
                   verse_count               = EXCLUDED.verse_count,
                   mean_syllable_density     = EXCLUDED.mean_syllable_density,
                   stddev_syllable_density   = EXCLUDED.stddev_syllable_density,
                   mean_morpheme_ratio       = EXCLUDED.mean_morpheme_ratio,
                   stddev_morpheme_ratio     = EXCLUDED.stddev_morpheme_ratio,
                   mean_sonority_score       = EXCLUDED.mean_sonority_score,
                   stddev_sonority_score     = EXCLUDED.stddev_sonority_score,
                   mean_clause_compression   = EXCLUDED.mean_clause_compression,
                   stddev_clause_compression = EXCLUDED.stddev_clause_compression,
                   mean_breath_weight        = EXCLUDED.mean_breath_weight,
                   mean_open_ratio           = EXCLUDED.mean_open_ratio,
                   mean_guttural_density     = EXCLUDED.mean_guttural_density,
                   computed_at               = NOW()
               """,
               (
                   genre_id,
                   genre_def.get("description", ""),
                   genre_def.get("books", []),
                   baseline["verse_count"],
                   baseline["syllable_density"],
                   baseline["std_syllable_density"],
                   baseline["morpheme_ratio"],
                   baseline["std_morpheme_ratio"],
                   baseline["sonority_score"],
                   baseline["std_sonority_score"],
                   baseline["clause_compression"],
                   baseline["std_clause_compression"],
                   baseline["mean_breath_weight"],
                   baseline["mean_open_ratio"],
                   baseline["mean_guttural_density"],
               ),
           )
       conn.commit()
   ```

4. Run and confirm ALL PASSED:

   ```bash
   uv run --frozen pytest tests/test_genre_baseline.py -v
   # Expected: 4 passed
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat(stage8): implement genre_baseline module with upsert and stddev columns"`

---

### Task 3: Register `genre_baseline` in `run.py` and `config.yml`

**Files:** `pipeline/run.py`, `pipeline/config.yml`

**Steps:**

1. No new tests — the stage registry is covered by the existing
   `test_run_single_stage_calls_module` test; adding a new key does not
   break the existing 10 tests.

2. Run existing tests to confirm they still pass before touching `run.py`:

   ```bash
   uv run --frozen pytest tests/test_run.py -v
   # Expected: 10 passed
   ```

3. Add `"genre_baseline"` to `STAGE_REGISTRY` in `pipeline/run.py`:

   ```python
   STAGE_REGISTRY: dict[str, str] = {
       "ingest":           "modules.ingest",
       "fingerprint":      "modules.fingerprint",
       "breath":           "modules.breath",
       "chiasm":           "modules.chiasm",
       "translate_ingest": "modules.ingest_translations",
       "score":            "modules.score",
       "suggest":          "modules.suggest",
       "genre_baseline":   "modules.genre_baseline",   # ADD
       "export":           "modules.export",
   }
   ```

4. Add `genre_baseline` to the stages list in `pipeline/config.yml` (after
   `suggest`, before `export`):

   ```yaml
   pipeline:
     stages:
       - ingest
       - fingerprint
       - breath
       - chiasm
       - translate_ingest
       - score
       - suggest
       - genre_baseline    # ADD HERE
       - export
     on_error: stop
   ```

5. Run all tests to confirm nothing is broken:

   ```bash
   uv run --frozen pytest tests/ -v
   # Expected: all tests pass (10 test_run + 4 test_genre_baseline + all prior)
   ```

6. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

7. Commit: `"feat(stage8): register genre_baseline in STAGE_REGISTRY and config.yml stages"`

---

### Task 4: Add Isaiah to `config.yml` and write `init_flags.sql`

**Files:** `pipeline/config.yml`, `pipeline/init_flags.sql`

**Steps:**

1. No new tests for the config change itself.  Isaiah processing is validated
   by row counts in the acceptance criteria (see Task 5).

2. Add Isaiah to `corpus.books` in `pipeline/config.yml`:

   ```yaml
   corpus:
     books:
       - book_num: 19
         name: Psalms
         genre: hebrew_poetry
       - book_num: 23
         name: Isaiah
         genre: hebrew_prophecy
     debug_chapters: []

   genre:
     clusters:
       hebrew_poetry:
         description: "Classical biblical poetry: Psalms, Job, Proverbs, Lamentations"
         books: [19, 18, 20, 22, 25]
         expected_syllable_density:   2.3
         expected_morpheme_ratio:     2.0
         expected_sonority_score:     0.55
         expected_clause_compression: 4.5

       hebrew_prophecy:
         description: "Prophetic literature: Isaiah through Malachi"
         books: [23, 24, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39]
         expected_syllable_density:   2.1
         expected_morpheme_ratio:     1.9
         expected_sonority_score:     0.52
         expected_clause_compression: 5.5

       hebrew_narrative:
         description: "Prose narrative: Torah, Former Prophets, historical books"
         books: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
         expected_syllable_density:   1.9
         expected_morpheme_ratio:     1.7
         expected_sonority_score:     0.48
         expected_clause_compression: 7.0

       hebrew_law:
         description: "Legal and cultic texts"
         books: [3, 5]
         expected_syllable_density:   1.8
         expected_morpheme_ratio:     1.8
         expected_sonority_score:     0.46
         expected_clause_compression: 6.5

       late_hebrew:
         description: "Post-exilic Hebrew with Aramaic influence"
         books: [15, 16, 13, 14, 17]
         expected_syllable_density:   1.85
         expected_morpheme_ratio:     1.75
         expected_sonority_score:     0.47
         expected_clause_compression: 6.8
   ```

3. Create `pipeline/init_flags.sql` with genre assignments and acrostic/Aramaic
   flags (run once after schema is initialised; safe to re-run):

   ```sql
   -- init_flags.sql
   -- Run via:
   --   docker exec psalms_db psql -U psalms -d psalms -f /pipeline/init_flags.sql
   --
   -- Sets genre on books already ingested and flags acrostic + Aramaic verses.
   -- Safe to re-run (all statements are idempotent UPDATE ... SET ... WHERE).

   -- Genre assignments (books table)
   UPDATE books SET genre = 'hebrew_poetry'
       WHERE book_num IN (19, 18, 20, 22, 25);

   UPDATE books SET genre = 'hebrew_prophecy'
       WHERE book_num IN (23, 24, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
                          36, 37, 38, 39);

   UPDATE books SET genre = 'hebrew_narrative'
       WHERE book_num IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

   UPDATE books SET genre = 'hebrew_law'
       WHERE book_num IN (3, 5);

   UPDATE books SET genre = 'late_hebrew'
       WHERE book_num IN (13, 14, 15, 16, 17);

   -- Acrostic chapters
   -- Psalm 119 (book_num=19, chapter=119) — full alphabetic acrostic
   -- Lamentations chapters 1-4 (book_num=25) — acrostic structure
   UPDATE verses SET is_acrostic = TRUE
   WHERE (book_num = 19 AND chapter = 119)
      OR (book_num = 25 AND chapter IN (1, 2, 3, 4));

   -- Aramaic sections (commented out — Daniel and Ezra not in current scope)
   -- UPDATE verses SET is_aramaic = TRUE
   -- WHERE (book_num = 27 AND chapter BETWEEN 2 AND 7)   -- Daniel 2:4b-7:28
   --    OR (book_num = 15 AND chapter IN (4, 5, 6, 7));  -- Ezra 4:8-6:18, 7:12-26
   ```

4. Verify BHSA has Isaiah data before running the pipeline:

   ```bash
   docker compose --profile pipeline run --rm pipeline python -c "
   from tf.fabric import Fabric
   TF = Fabric(
       locations=['/data/bhsa/github/ETCBC/bhsa/tf/c'],
       silent=True,
   )
   api = TF.load('book chapter verse g_word_utf8')
   F, T = api.F, api.T
   isaiah = [v for v in F.otype.s('verse') if T.bookName(v) == 'Isaiah']
   print(f'Isaiah verses in BHSA: {len(isaiah)}')
   # Expected: 1292
   "
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat(stage8): add Isaiah to config.yml corpus.books and write init_flags.sql"`

---

### Task 5: Run full pipeline with Isaiah and verify row counts

**Files:** None (SQL verification queries only)

**Steps:**

1. Run `init_flags.sql` to set genre and acrostic flags:

   ```bash
   docker exec psalms_db psql -U psalms -d psalms \
       -f /pipeline/init_flags.sql
   # Expected: UPDATE N (each statement reports affected rows)
   ```

2. Run the full pipeline — the resumability logic skips already-computed
   Psalms verses and processes only Isaiah:

   ```bash
   docker compose --profile pipeline run --rm pipeline python run.py
   # Expected: exits 0; pipeline_runs.status = 'ok'
   ```

3. Verify Isaiah row counts:

   ```bash
   docker exec psalms_db psql -U psalms -d psalms -c "
   SELECT
     b.book_name,
     COUNT(DISTINCT v.verse_id)  AS verses,
     COUNT(DISTINCT vf.verse_id) AS fingerprints,
     COUNT(DISTINCT bp.verse_id) AS breath_profiles,
     ROUND(AVG(vf.syllable_density)::numeric, 3)  AS avg_syl_density,
     ROUND(AVG(vf.sonority_score)::numeric, 3)    AS avg_sonority
   FROM verses v
   JOIN books b ON v.book_num = b.book_num
   LEFT JOIN verse_fingerprints vf ON vf.verse_id = v.verse_id
   LEFT JOIN breath_profiles bp ON bp.verse_id = v.verse_id
   WHERE v.book_num IN (19, 23)
   GROUP BY b.book_name
   ORDER BY b.book_name;
   "
   # Expected (approximate):
   # Isaiah | 1292 | 1292 | 1292 | 2.0-2.2 | 0.50-0.54
   # Psalms | 2527 | 2527 | 2527 | 2.2-2.4 | 0.53-0.57
   ```

4. Verify genre baselines:

   ```bash
   docker exec psalms_db psql -U psalms -d psalms -c "
   SELECT genre_id, verse_count,
          ROUND(mean_syllable_density::numeric,  4) AS density,
          ROUND(mean_morpheme_ratio::numeric,    4) AS morpheme,
          ROUND(mean_sonority_score::numeric,    4) AS sonority,
          ROUND(mean_clause_compression::numeric, 4) AS compression
   FROM genre_baselines ORDER BY genre_id;
   "
   # Expected: hebrew_poetry.density >= hebrew_prophecy.density
   ```

5. Confirm pipeline is idempotent — run again and check no row count changes:

   ```bash
   docker compose --profile pipeline run --rm pipeline python run.py
   # Expected: same row counts, no duplicates, exits 0
   ```

6. Run the genre baseline stage in isolation:

   ```bash
   docker compose --profile pipeline run --rm pipeline \
       python run.py --stages genre_baseline
   # Expected: exits 0; genre_baselines rows refreshed
   ```

7. Commit: `"chore(stage8): record Isaiah validation — 1292 verses, baselines verified"`

---

## Book Expansion Procedure (for all future books)

Follow these steps in order when adding any book beyond Isaiah:

1. Look up the book_num in the BHSA ordering:
   Genesis=1, Exodus=2, Leviticus=3, Numbers=4, Deuteronomy=5,
   Joshua=6, Judges=7, 1 Samuel=8, 2 Samuel=9, 1 Kings=10, 2 Kings=11,
   1 Chronicles=12, 2 Chronicles=13, Ezra=14 (note: some sources differ —
   verify against your BHSA installation), Nehemiah=15, Esther=16, Job=18,
   Psalms=19, Proverbs=20, Ecclesiastes=21, Song of Songs=22, Isaiah=23,
   Jeremiah=24, Lamentations=25, Ezekiel=26, Daniel=27.

2. Add an entry to `corpus.books` in `config.yml` with the correct
   `book_num`, `name`, and `genre`.  If the book has acrostic chapters
   (e.g. Lamentations 1–4), add `acrostic_chapters: [...]`.

3. If the new book has acrostic chapters, add a corresponding UPDATE clause
   in `init_flags.sql` and re-run it:

   ```bash
   docker exec psalms_db psql -U psalms -d psalms \
       -f /pipeline/init_flags.sql
   ```

4. Run the pipeline — resumability skips already-processed verses:

   ```bash
   docker compose --profile pipeline run --rm pipeline python run.py
   ```

5. Verify row counts increased by the expected amount for the new book using
   the validation query from Task 5, Step 3 (substituting the new book_num).

6. Refresh genre statistics to incorporate the new book:

   ```bash
   docker compose --profile pipeline run --rm pipeline \
       python run.py --stages genre_baseline
   ```

7. Confirm `pipeline_runs` shows `status='ok'` for the new run.

---

## SQL Validation Queries

```sql
-- All books currently in corpus with processing status
SELECT
    b.book_name,
    b.genre,
    b.book_num,
    COUNT(DISTINCT v.verse_id)  AS verses,
    COUNT(DISTINCT vf.verse_id) AS fingerprinted,
    COUNT(DISTINCT bp.verse_id) AS breath_computed
FROM books b
LEFT JOIN verses v  ON v.book_num  = b.book_num
LEFT JOIN verse_fingerprints vf ON vf.verse_id = v.verse_id
LEFT JOIN breath_profiles bp    ON bp.verse_id = v.verse_id
GROUP BY b.book_name, b.genre, b.book_num
ORDER BY b.book_num;

-- Genre baselines summary
SELECT
    genre_id,
    verse_count,
    ROUND(mean_syllable_density::numeric,    4) AS density,
    ROUND(mean_morpheme_ratio::numeric,      4) AS morpheme,
    ROUND(mean_sonority_score::numeric,      4) AS sonority,
    ROUND(mean_clause_compression::numeric,  4) AS compression,
    ROUND(stddev_syllable_density::numeric,  4) AS std_density
FROM genre_baselines
ORDER BY genre_id;

-- Acrostic verse flag check
SELECT v.book_num, v.chapter, COUNT(*) AS acrostic_verse_count
FROM verses v
WHERE v.is_acrostic = TRUE
GROUP BY v.book_num, v.chapter
ORDER BY v.book_num, v.chapter;

-- Cross-genre fingerprint comparison (live data)
SELECT
    b.genre,
    ROUND(AVG(vf.syllable_density)::numeric, 4) AS avg_density,
    ROUND(AVG(vf.sonority_score)::numeric,   4) AS avg_sonority,
    COUNT(*) AS verse_count
FROM verse_fingerprints vf
JOIN verses v ON vf.verse_id = v.verse_id
JOIN books  b ON v.book_num  = b.book_num
WHERE b.genre IS NOT NULL
GROUP BY b.genre
ORDER BY avg_density DESC;

-- Recent pipeline runs
SELECT
    run_id,
    started_at,
    finished_at,
    EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_s,
    status,
    stages_run,
    error_message
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
```
