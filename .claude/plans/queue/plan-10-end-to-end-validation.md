# Plan 10 — End-to-End Pipeline Validation

## Goal

Systematically verify that every completed stage meets its acceptance criteria from
`CLAUDE.md`. Produce a `validate_pipeline.py` script that runs all SQL checks in a
single pass and exits non-zero if any criterion fails. This creates a repeatable
regression gate before any new corpus expansion.

## Prerequisites

- All containers healthy (`docker compose ps`)
- At least one `status='ok'` row in `pipeline_runs`

## Tasks

### Task 1 — Write TDD tests (red phase)

Write 6 failing tests in `tests/test_validate_pipeline.py`:

- `test_validate_returns_dict_with_all_stages` — returned dict has keys for stages
  0–8 plus `"overall"`
- `test_stage2_verse_count` — `verses` table has exactly 2,527 rows for book 19
- `test_stage2_token_count` — `morpheme_tokens` has ≥ 40,000 rows for book 19
- `test_stage2_fingerprint_count` — `verse_fingerprints` has 2,527 rows for book 19
- `test_stage3_syllable_count` — `syllable_tokens` has ≥ 100,000 rows for book 19
- `test_stage3_breath_profile_count` — `breath_profiles` has 2,527 rows for book 19
- `test_stage4_scores_populated` — `translation_scores` has ≥1 row per translation
  per verse (no NULLs in composite_deviation)
- `test_stage8_isaiah_verses` — `verses` has exactly 1,292 rows for book 23
- `test_genre_baselines_density_ordering` — `hebrew_poetry`.syllable_density_mean >
  `hebrew_prophecy`.syllable_density_mean

Run: `uv run --frozen pytest tests/test_validate_pipeline.py -v` → all must FAIL.

### Task 2 — Implement `pipeline/validate_pipeline.py`

Module interface (extends existing module convention):

```python
def run(conn: psycopg2.Connection, config: dict) -> dict:
    """Run all stage acceptance-criteria checks.

    Returns {
        "stage_0": {"passed": bool, "detail": str},
        "stage_1": {"passed": bool, "detail": str},
        ...
        "stage_8": {"passed": bool, "detail": str},
        "overall": {"passed": bool, "failures": list[str]},
    }
    Raises AssertionError if overall.passed is False.
    """
```

Checks to implement (one per stage):

| Stage | SQL Check | Expected |
|-------|-----------|----------|
| 0 | `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'` | ≥ 10 tables |
| 1 | `SELECT COUNT(DISTINCT translation_key) FROM translations` | = 5 translations |
| 2a | `SELECT COUNT(*) FROM verses WHERE book_num=19` | = 2,527 |
| 2b | `SELECT COUNT(*) FROM morpheme_tokens t JOIN verses v ON t.verse_id=v.verse_id WHERE v.book_num=19` | ≥ 40,000 |
| 2c | `SELECT COUNT(*) FROM verse_fingerprints f JOIN verses v ON f.verse_id=v.verse_id WHERE v.book_num=19` | = 2,527 |
| 3a | `SELECT COUNT(*) FROM syllable_tokens t JOIN verses v ON t.verse_id=v.verse_id WHERE v.book_num=19` | ≥ 100,000 |
| 3b | `SELECT COUNT(*) FROM breath_profiles b JOIN verses v ON b.verse_id=v.verse_id WHERE v.book_num=19` | = 2,527 |
| 4 | `SELECT COUNT(*) FROM translation_scores WHERE composite_deviation IS NULL` | = 0 |
| 5 | `SELECT COUNT(*) FROM translation_suggestions` | ≥ 1 |
| 6 | `SELECT COUNT(*) FROM pipeline_runs WHERE status='ok'` | ≥ 1 |
| 7 | `SELECT COUNT(*) FROM pipeline_runs ORDER BY finished_at DESC LIMIT 1` | status = 'ok' |
| 8 | `SELECT COUNT(*) FROM verses WHERE book_num=23` | = 1,292 |
| 8b | `SELECT COUNT(*) FROM genre_baselines` | ≥ 2 |

Each check logs pass/fail at INFO level. Collect all failures and raise `AssertionError`
at the end if any checks failed.

### Task 3 — Register in orchestrator (optional stage)

Edit `pipeline/run.py` to accept `--validate-only` flag:

```python
if args.validate_only:
    import validate_pipeline
    result = validate_pipeline.run(conn, config)
    sys.exit(0 if result["overall"]["passed"] else 1)
```

This lets CI/CD run `python run.py --validate-only` without re-running the pipeline.

### Task 4 — Add convenience make target / script

Create `scripts/validate.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
docker compose --profile pipeline run --rm pipeline \
    python run.py --validate-only
```

### Task 5 — Run validation and capture baseline report

```bash
bash scripts/validate.sh 2>&1 | tee data/outputs/validation_baseline.txt
```

Commit the baseline output so future runs can diff against it.

### Task 6 — Fix any failures found

For each failed check:
1. Identify root cause (missing rows, wrong counts, NULL columns)
2. Re-run the relevant stage in isolation
3. Re-run `scripts/validate.sh` until all checks pass

## Acceptance Criteria

- `uv run --frozen pytest tests/test_validate_pipeline.py -v` — all tests pass
- `bash scripts/validate.sh` exits 0
- `data/outputs/validation_baseline.txt` committed showing all checks green
- No NULL values in `translation_scores.composite_deviation`
- `pipeline_runs` contains at least one `status='ok'` row

## Files to Create/Modify

- `pipeline/validate_pipeline.py` (new)
- `tests/test_validate_pipeline.py` (new)
- `pipeline/run.py` — add `--validate-only` flag
- `scripts/validate.sh` (new)
