# Plan 11 — Poetic Books Corpus Expansion (Job, Proverbs, Song of Songs, Lamentations)

## Goal

Add the four remaining Hebrew poetic books to the corpus. Together with Psalms they form
the complete `hebrew_poetry` genre cluster, enabling genre baselines to be computed from
a full population rather than a single book. This is a config-driven change — no new
pipeline code is required.

Expected verse counts (BHSA):

| Book | book_num | Verses |
|------|----------|--------|
| Job | 18 | ~1,070 |
| Psalms | 19 | 2,527 (existing) |
| Proverbs | 20 | ~915 |
| Song of Songs | 22 | ~117 |
| Lamentations | 25 | ~154 |

> Note: Exact BHSA verse counts may differ slightly from print Bible counts due to
> versification differences. Use SQL row counts from the run as the authoritative totals.

## Prerequisites

- Plan 10 (validation) complete — all Stage 0–8 checks passing
- At least one `status='ok'` row in `pipeline_runs`

## Tasks

### Task 1 — Write TDD tests (red phase)

Write 8 failing tests in `tests/test_poetic_expansion.py`:

- `test_job_verses_loaded` — `verses` has ≥ 1,000 rows for book_num = 18
- `test_proverbs_verses_loaded` — `verses` has ≥ 900 rows for book_num = 20
- `test_song_verses_loaded` — `verses` has ≥ 100 rows for book_num = 22
- `test_lamentations_verses_loaded` — `verses` has ≥ 140 rows for book_num = 25
- `test_all_poetic_books_have_fingerprints` — `verse_fingerprints` count matches
  `verses` count for each of books 18, 20, 22, 25
- `test_all_poetic_books_have_breath_profiles` — `breath_profiles` count matches
  `verses` count for each new book
- `test_all_poetic_books_have_translation_scores` — `translation_scores` has ≥1 row
  per (verse_id, translation_key) pair for new books
- `test_genre_baseline_poetry_uses_all_five_books` — genre_baselines row for
  `hebrew_poetry` is derived from all five books (verify via updated row counts
  rising after re-run)

Run: `uv run --frozen pytest tests/test_poetic_expansion.py -v` → all must FAIL.

### Task 2 — Update `config.yml` corpus

Edit `pipeline/config.yml`, `corpus.books` section:

```yaml
corpus:
  books:
    - book_num: 18
      name: Job
    - book_num: 19
      name: Psalms
    - book_num: 20
      name: Proverbs
    - book_num: 22
      name: Song of Songs
    - book_num: 23
      name: Isaiah
    - book_num: 25
      name: Lamentations
```

No other code changes are required — all stages use `corpus.books` from config.

### Task 3 — Extend `validate_data.py` with new book fixtures

Add spot-check fixtures for the new books to the `CHECKS` list in
`pipeline/validate_data.py`:

```python
# Job 3:1 — Job opens his mouth to curse the day
(18, 3, 1, "KJV", "After this opened Job his mouth"),
# Job 1:1 — first verse
(18, 1, 1, "KJV", "There was a man"),
# Proverbs 1:1 — superscription
(20, 1, 1, "KJV", "The proverbs of Solomon"),
# Proverbs 3:5 — well-known verse
(20, 3, 5, "KJV", "Trust in the Lord"),
# Song of Songs 1:1 — superscription
(22, 1, 1, "KJV", "The song of songs"),
# Lamentations 1:1 — opening lament
(25, 1, 1, "KJV", "How doth the city sit"),
```

> Verify exact KJV text against actual DB content before committing. Adjust prefixes
> to match the scrollmapper v2 KJV rendering.

### Task 4 — Verify `init_flags.sql` genre assignments cover new books

Confirm (do not re-run; it was written in plan-08) that `init_flags.sql` already
assigns the correct genre:

```sql
SELECT book_num, genre FROM books
WHERE book_num IN (18, 20, 22, 25)
ORDER BY book_num;
-- All four should return genre = 'hebrew_poetry'
```

If any row returns NULL genre, apply the missing UPDATE:

```sql
UPDATE books SET genre = 'hebrew_poetry'
WHERE book_num IN (18, 19, 20, 22, 25);
```

Also confirm the acrostic flags for Lamentations 1–4 are set:

```sql
SELECT COUNT(*) FROM verses
WHERE book_num = 25 AND is_acrostic = TRUE;
-- Expect ≥ 88 (22 letters × 4 chapters)
```

### Task 5 — Run pipeline stages for new books only

Because existing Psalms/Isaiah rows are already populated, use the pipeline's
resumable design — stages skip rows that already exist. Run the full pipeline to
process only the new verses:

```bash
docker compose --profile pipeline run --rm pipeline python run.py
```

Monitor progress:

```bash
# Expected after run completes:
SELECT book_num, COUNT(*) as verses
FROM verses
GROUP BY book_num
ORDER BY book_num;
```

### Task 6 — Re-run genre_baseline to incorporate all five poetic books

After new verses are processed, re-run the genre_baseline stage to update the
aggregate fingerprint from all five poetic books:

```bash
docker compose --profile pipeline run --rm pipeline \
    python -c "
import psycopg2, yaml, modules.genre_baseline as gb
config = yaml.safe_load(open('/pipeline/config.yml'))
conn = psycopg2.connect(dsn='...')
print(gb.run(conn, config))
"
```

Verify update:

```sql
SELECT genre, syllable_density_mean, verse_count
FROM genre_baselines
WHERE genre = 'hebrew_poetry';
-- verse_count should now be total of all 5 poetic books
```

### Task 7 — Run all tests and validate

```bash
uv run --frozen pytest tests/test_poetic_expansion.py -v
bash scripts/validate.sh
```

All checks must pass before marking this plan complete.

### Task 8 — Update documentation

Update `README.md` verification section to reflect expanded corpus row counts:

```
verses:              ≥ 4,783 (Psalms + Job + Proverbs + Song + Isaiah + Lamentations)
verse_fingerprints:  = verse count (one-to-one)
breath_profiles:     = verse count (one-to-one)
genre_baselines:     ≥ 2 rows (hebrew_poetry + hebrew_prophecy)
```

## Acceptance Criteria

- `uv run --frozen pytest tests/test_poetic_expansion.py -v` — all 8 tests pass
- `bash scripts/validate.sh` — exits 0
- `verses` table has rows for all 6 configured books (18, 19, 20, 22, 23, 25)
- `verse_fingerprints` and `breath_profiles` counts match `verses` counts per book
- `translation_scores` has no NULLs in `composite_deviation` for new books
- `genre_baselines` for `hebrew_poetry` reflects all five poetic books
- Pipeline exits with `status='ok'` in `pipeline_runs`

## Files to Create/Modify

- `pipeline/config.yml` — add books 18, 20, 22, 25 to `corpus.books`
- `pipeline/validate_data.py` — add KJV spot-check fixtures for new books
- `tests/test_poetic_expansion.py` (new)
- `README.md` — update verification row counts
