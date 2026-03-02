# Plan 08 — Corpus Expansion (Isaiah + Genre Baselines)

> Promoted from holding. Trigger: confirm one `status='ok'` row in `pipeline_runs`.

## Goal

Add Isaiah as the first expansion book beyond Psalms and implement the `genre_baseline`
module that computes aggregate fingerprints per genre cluster. This validates that the
pipeline is truly book-agnostic and that genre-level comparison is possible.

## Prerequisites

Confirm Stage 7 success before starting:
```sql
SELECT status, book_nums, finished_at
FROM pipeline_runs
ORDER BY finished_at DESC
LIMIT 1;
-- Must return status='ok'
```

## Tasks

### Task 1 — Write TDD tests (red phase)

Write 4 failing tests in `tests/test_genre_baseline.py`:
- `test_run_returns_correct_keys` — returns `{"rows_written": int, "elapsed_s": float}`
- `test_genre_baselines_table_populated` — `genre_baselines` has ≥1 row after run
- `test_poetry_density_higher_than_prophecy` — `hebrew_poetry`.syllable_density_mean >
  `hebrew_prophecy`.syllable_density_mean
- `test_run_is_idempotent` — second run doesn't raise, row count stays constant

Run: `uv run --frozen pytest tests/test_genre_baseline.py -v` → all 4 must FAIL.

### Task 2 — Implement `modules/genre_baseline.py`

Module interface:
```python
def run(conn: psycopg2.Connection, config: dict) -> dict:
    """Compute per-genre aggregate fingerprints from verse_fingerprints.
    Returns {"rows_written": int, "elapsed_s": float}.
    """
```

Logic:
1. Read genre assignments from `books` table (already seeded in schema)
2. JOIN `verse_fingerprints` + `verses` + `books` grouped by genre
3. Compute AVG + STDDEV for: `lexical_density`, `morpheme_ratio`, `sonority_score`,
   `syllable_density` (and 3 breath metrics from `breath_profiles`)
4. UPSERT into `genre_baselines` with conflict on `genre`

### Task 3 — Register in orchestrator

Edit `pipeline/run.py`:
```python
STAGE_REGISTRY = {
    ...
    "genre_baseline": "modules.genre_baseline",  # add after "export"
}
```

Edit `pipeline/config.yml`:
```yaml
pipeline:
  stages:
    - ...
    - export
    - genre_baseline   # add at end
```

### Task 4 — Add Isaiah to corpus + write `init_flags.sql`

Edit `pipeline/config.yml`:
```yaml
corpus:
  books:
    - book_num: 19   # Psalms (existing)
    - book_num: 23   # Isaiah (new)
```

Create `pipeline/init_flags.sql`:
```sql
-- Genre assignments
UPDATE books SET genre = 'hebrew_poetry'
WHERE book_num IN (18, 19, 20, 22, 25);  -- Job, Psalms, Proverbs, Song, Lamentations

UPDATE books SET genre = 'hebrew_prophecy'
WHERE book_num BETWEEN 23 AND 39;  -- Isaiah through Malachi

-- Acrostic flags
UPDATE verses SET is_acrostic = TRUE
WHERE book_num = 19 AND chapter = 119;  -- Psalm 119

UPDATE verses SET is_acrostic = TRUE
WHERE book_num = 25 AND chapter IN (1, 2, 3, 4);  -- Lamentations 1–4
```

Run `init_flags.sql` in DB container after schema init:
```bash
docker exec psalms_db psql -U psalms -d psalms -f /pipeline/init_flags.sql
```

### Task 5 — Run pipeline + validate acceptance criteria

```bash
docker compose --profile pipeline run --rm pipeline python run.py
```

Verify:
```sql
-- Isaiah verses ingested
SELECT COUNT(*) FROM verses WHERE book_num = 23;   -- expect 1,292

-- Genre baselines computed
SELECT genre, syllable_density_mean, sonority_mean
FROM genre_baselines
ORDER BY genre;   -- expect hebrew_poetry density > hebrew_prophecy density

-- Pipeline run recorded
SELECT status, book_nums FROM pipeline_runs ORDER BY finished_at DESC LIMIT 1;
```

## Acceptance Criteria

- 1,292 Isaiah verse rows in `verses`
- `genre_baselines` table has ≥2 rows (hebrew_poetry + hebrew_prophecy)
- `hebrew_poetry`.syllable_density_mean > `hebrew_prophecy`.syllable_density_mean
- All 4 tests pass: `uv run --frozen pytest tests/test_genre_baseline.py -v`
- Pipeline exits 0 with `status='ok'` in `pipeline_runs`

## Files to Create/Modify

- `pipeline/modules/genre_baseline.py` (new)
- `pipeline/init_flags.sql` (new)
- `pipeline/run.py` — add genre_baseline to STAGE_REGISTRY
- `pipeline/config.yml` — add genre_baseline to stages; add Isaiah to corpus.books
- `tests/test_genre_baseline.py` (new)
