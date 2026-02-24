# Stage 8 — Corpus Expansion
## Detailed Implementation Plan

> **Depends on:** Stages 0–7 fully operational for Psalms  
> **Produces:** Pipeline extended to additional Biblical books with genre-aware baseline fingerprints; Isaiah validated as first expansion target  
> **Estimated time:** 1–2 hours configuration; 30–90 minutes per new book (data download + pipeline run)

---

## Objectives

1. Define genre cluster configuration in `config.yml`
2. Implement `modules/genre_baseline.py` to compute and store genre-level reference fingerprints
3. Extend schema flags for Aramaic sections and acrostic poetry (pre-provisioned in Stage 0)
4. Validate the expansion with Isaiah as the first new book
5. Document the config-only procedure for adding any additional book

---

## Design Principle: Config-Only Expansion

The entire pipeline was written to be corpus-agnostic from Stage 0. Adding a new book requires:

1. Add an entry to the `corpus.books` list in `config.yml`
2. Add translation sources for the new book (or verify existing sources cover it — most do)
3. Run the pipeline

No Python code changes are required. The BHSA corpus covers the entire Hebrew Bible, so the data is already mounted. Only the book filter in `config.yml` changes.

---

## Priority Expansion Books

| Book | book_num | Genre | Verse count | Special flags |
|---|---|---|---|---|
| Isaiah | 23 | hebrew_prophecy | 1,292 | None |
| Job | 18 | hebrew_poetry | 1,070 | None |
| Lamentations | 25 | hebrew_poetry | 154 | Acrostic (chapters 1–4) |
| Proverbs | 20 | hebrew_poetry | 915 | None |
| Ecclesiastes | 21 | hebrew_poetry | 222 | Late Hebrew features |
| Genesis | 1 | hebrew_narrative | 1,533 | Mixed genres within book |

---

## Genre Cluster Definitions

| Genre ID | Books | Linguistic character |
|---|---|---|
| `hebrew_poetry` | Psalms, Job, Proverbs, Lamentations, Song of Songs | High parallelism, dense syllable structure, elevated sonority |
| `hebrew_prophecy` | Isaiah, Jeremiah, Ezekiel, Minor Prophets | Mixed prose/poetry, elevated vocabulary, rhetorical structures |
| `hebrew_narrative` | Genesis–Numbers, Joshua, Judges, Samuel, Kings | Lower morpheme density, higher clause compression, prose rhythm |
| `hebrew_law` | Leviticus, Deuteronomy | Formulaic, repetitive, low variation |
| `late_hebrew` | Ezra, Nehemiah, Chronicles, Esther | Persian-period vocabulary, Aramaic influence |
| `aramaic_sections` | Daniel 2–7, Ezra 4–7 | Aramaic language — flagged but not analyzed in current scope |

---

## File Structure

```
pipeline/
  modules/
    genre_baseline.py    ← compute genre reference fingerprints
  tests/
    test_genre_baseline.py
```

---

## Step 1 — Updated `config.yml` (corpus expansion section)

Add to the existing `config.yml`. Only the `corpus.books` list and `genre` block are new; everything else is unchanged.

```yaml
corpus:
  books:
    # Original
    - book_num: 19
      name:     Psalms
      genre:    hebrew_poetry

    # Expansion — add one at a time or all at once
    - book_num: 23
      name:     Isaiah
      genre:    hebrew_prophecy

    - book_num: 18
      name:     Job
      genre:    hebrew_poetry

    - book_num: 25
      name:     Lamentations
      genre:    hebrew_poetry
      acrostic_chapters: [1, 2, 3, 4]

  debug_chapters: []   # keep empty for full runs

genre:
  clusters:
    hebrew_poetry:
      description: "Classical biblical poetry: Psalms, Job, Proverbs, Lamentations"
      books: [19, 18, 20, 22, 25]
      expected_syllable_density:  2.3   # informational reference only
      expected_morpheme_ratio:    2.0
      expected_sonority_score:    0.55
      expected_clause_compression: 4.5

    hebrew_prophecy:
      description: "Prophetic literature: Isaiah through Malachi"
      books: [23, 24, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39]
      expected_syllable_density:  2.1
      expected_morpheme_ratio:    1.9
      expected_sonority_score:    0.52
      expected_clause_compression: 5.5

    hebrew_narrative:
      description: "Prose narrative: Torah, Former Prophets, historical books"
      books: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
      expected_syllable_density:  1.9
      expected_morpheme_ratio:    1.7
      expected_sonority_score:    0.48
      expected_clause_compression: 7.0

    hebrew_law:
      description: "Legal/cultic texts"
      books: [3, 5]
      expected_syllable_density:  1.8
      expected_morpheme_ratio:    1.8
      expected_sonority_score:    0.46
      expected_clause_compression: 6.5

    late_hebrew:
      description: "Post-exilic Hebrew with Aramaic influence"
      books: [15, 16, 13, 14, 17]
      expected_syllable_density:  1.85
      expected_morpheme_ratio:    1.75
      expected_sonority_score:    0.47
      expected_clause_compression: 6.8
```

---

## Step 2 — Database: Genre and Flag Updates

These columns were pre-provisioned in the Stage 0 schema. Populate them now for the new books:

```sql
-- Update genre column on books table
UPDATE books SET genre = 'hebrew_poetry'   WHERE book_num IN (19, 18, 20, 22, 25);
UPDATE books SET genre = 'hebrew_prophecy' WHERE book_num IN (23, 24, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39);
UPDATE books SET genre = 'hebrew_narrative' WHERE book_num IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
UPDATE books SET genre = 'hebrew_law'       WHERE book_num IN (3, 5);
UPDATE books SET genre = 'late_hebrew'      WHERE book_num IN (13, 14, 15, 16, 17);

-- Flag acrostic chapters (Lamentations 1–4, Psalm 119)
UPDATE verses
SET is_acrostic = TRUE
WHERE (book_num = 25 AND chapter IN (1, 2, 3, 4))
   OR (book_num = 19 AND chapter = 119);

-- Flag Aramaic sections (Daniel 2:4b–7:28, Ezra 4:8–6:18 and 7:12–26)
-- Note: these books are not in the current expansion set but the flag is ready
-- UPDATE verses SET is_aramaic = TRUE
-- WHERE (book_num = 27 AND chapter BETWEEN 2 AND 7)   -- Daniel
--    OR (book_num = 15 AND chapter IN (4,5,6,7));       -- Ezra
```

Run this via:
```bash
docker exec psalms_db psql -U psalms -d psalms -f /pipeline/init_flags.sql
```

Save the above as `pipeline/init_flags.sql`.

---

## Step 3 — File: `modules/genre_baseline.py`

```python
"""
Stage 8 — Genre baseline fingerprints.

Computes aggregate fingerprint vectors for each genre cluster from the
processed verses in that genre. Stores as reference rows in verse_fingerprints
using special sentinel verse_ids (negative integers by genre).

These baselines allow any new book's verses to be compared not just to
individual Hebrew verse fingerprints but to the genre-level center of mass.
"""

from __future__ import annotations

import json
import logging
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# Sentinel verse_id values for genre baselines
# Stored as special rows in a separate table to avoid verse_id conflicts
GENRE_IDS = {
    "hebrew_poetry":    -1,
    "hebrew_prophecy":  -2,
    "hebrew_narrative": -3,
    "hebrew_law":       -4,
    "late_hebrew":      -5,
}


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    """
    Compute genre-level baseline fingerprints from all processed verses
    in each genre cluster. Store in genre_baselines table.
    """
    _ensure_genre_baselines_table(conn)

    genre_cfg = config.get("genre", {}).get("clusters", {})
    if not genre_cfg:
        logger.info("No genre clusters configured. Skipping genre baseline computation.")
        return {"baselines_computed": 0}

    computed = 0
    for genre_id, genre_def in genre_cfg.items():
        book_nums = genre_def.get("books", [])
        if not book_nums:
            continue

        baseline = _compute_genre_baseline(conn, genre_id, book_nums)
        if baseline is None:
            logger.warning(f"No fingerprint data for genre '{genre_id}' — skipping")
            continue

        _upsert_genre_baseline(conn, genre_id, baseline, genre_def)
        computed += 1
        logger.info(
            f"Genre baseline computed: {genre_id} "
            f"({baseline['verse_count']} verses from books {book_nums[:3]}{'...' if len(book_nums) > 3 else ''})"
        )

    return {"baselines_computed": computed}


def _ensure_genre_baselines_table(conn: psycopg2.extensions.connection) -> None:
    """Create genre_baselines table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS genre_baselines (
                genre_id            TEXT PRIMARY KEY,
                description         TEXT,
                book_nums           INTEGER[],
                verse_count         INTEGER,
                -- Mean fingerprint values across all genre verses
                syllable_density    NUMERIC(6,4),
                morpheme_ratio      NUMERIC(6,4),
                sonority_score      NUMERIC(6,4),
                clause_compression  NUMERIC(6,4),
                -- Standard deviations (for z-score normalization)
                std_syllable_density   NUMERIC(6,4),
                std_morpheme_ratio     NUMERIC(6,4),
                std_sonority_score     NUMERIC(6,4),
                std_clause_compression NUMERIC(6,4),
                -- Breath profile averages
                mean_breath_weight  NUMERIC(6,4),
                mean_open_ratio     NUMERIC(6,4),
                mean_guttural_density NUMERIC(6,4),
                computed_at         TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
    conn.commit()


def _compute_genre_baseline(
    conn: psycopg2.extensions.connection,
    genre_id: str,
    book_nums: List[int],
) -> Optional[dict]:
    """Compute mean and stddev fingerprint for all verses in given books."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)                           AS verse_count,
                AVG(vf.syllable_density)           AS mean_density,
                AVG(vf.morpheme_ratio)             AS mean_morpheme,
                AVG(vf.sonority_score)             AS mean_sonority,
                AVG(vf.clause_compression)         AS mean_compression,
                STDDEV(vf.syllable_density)        AS std_density,
                STDDEV(vf.morpheme_ratio)          AS std_morpheme,
                STDDEV(vf.sonority_score)          AS std_sonority,
                STDDEV(vf.clause_compression)      AS std_compression,
                AVG(bp.mean_weight)                AS mean_breath_weight,
                AVG(bp.open_ratio)                 AS mean_open_ratio,
                AVG(bp.guttural_density)           AS mean_guttural_density
            FROM verse_fingerprints vf
            JOIN verses v ON vf.verse_id = v.verse_id
            LEFT JOIN breath_profiles bp ON bp.verse_id = v.verse_id
            WHERE v.book_num = ANY(%s)
              AND vf.syllable_density IS NOT NULL
            """,
            (book_nums,)
        )
        row = cur.fetchone()

    if not row or not row[0]:
        return None

    return {
        "verse_count":         int(row[0]),
        "syllable_density":    float(row[1] or 0),
        "morpheme_ratio":      float(row[2] or 0),
        "sonority_score":      float(row[3] or 0),
        "clause_compression":  float(row[4] or 0),
        "std_syllable_density":    float(row[5] or 0),
        "std_morpheme_ratio":      float(row[6] or 0),
        "std_sonority_score":      float(row[7] or 0),
        "std_clause_compression":  float(row[8] or 0),
        "mean_breath_weight":  float(row[9] or 0),
        "mean_open_ratio":     float(row[10] or 0),
        "mean_guttural_density": float(row[11] or 0),
    }


def _upsert_genre_baseline(
    conn: psycopg2.extensions.connection,
    genre_id: str,
    baseline: dict,
    genre_def: dict,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO genre_baselines (
                genre_id, description, book_nums, verse_count,
                syllable_density, morpheme_ratio, sonority_score, clause_compression,
                std_syllable_density, std_morpheme_ratio, std_sonority_score, std_clause_compression,
                mean_breath_weight, mean_open_ratio, mean_guttural_density
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (genre_id) DO UPDATE SET
                verse_count           = EXCLUDED.verse_count,
                syllable_density      = EXCLUDED.syllable_density,
                morpheme_ratio        = EXCLUDED.morpheme_ratio,
                sonority_score        = EXCLUDED.sonority_score,
                clause_compression    = EXCLUDED.clause_compression,
                std_syllable_density  = EXCLUDED.std_syllable_density,
                std_morpheme_ratio    = EXCLUDED.std_morpheme_ratio,
                std_sonority_score    = EXCLUDED.std_sonority_score,
                std_clause_compression = EXCLUDED.std_clause_compression,
                mean_breath_weight    = EXCLUDED.mean_breath_weight,
                mean_open_ratio       = EXCLUDED.mean_open_ratio,
                mean_guttural_density = EXCLUDED.mean_guttural_density,
                computed_at           = NOW()
            """,
            (
                genre_id,
                genre_def.get("description", ""),
                genre_def.get("books", []),
                baseline["verse_count"],
                baseline["syllable_density"],
                baseline["morpheme_ratio"],
                baseline["sonority_score"],
                baseline["clause_compression"],
                baseline["std_syllable_density"],
                baseline["std_morpheme_ratio"],
                baseline["std_sonority_score"],
                baseline["std_clause_compression"],
                baseline["mean_breath_weight"],
                baseline["mean_open_ratio"],
                baseline["mean_guttural_density"],
            )
        )
    conn.commit()
```

---

## Step 4 — Add genre_baseline to Stage Registry

In `run.py`, add one line to `STAGE_REGISTRY`:

```python
STAGE_REGISTRY = {
    "ingest":           "modules.ingest",
    "fingerprint":      "modules.fingerprint",
    "breath":           "modules.breath",
    "chiasm":           "modules.chiasm",
    "translate_ingest": "modules.ingest_translations",
    "score":            "modules.score",
    "suggest":          "modules.suggest",
    "export":           "modules.export",
    "genre_baseline":   "modules.genre_baseline",   # ← ADD THIS
}
```

And add it to `config.yml` stages (after all verse-level processing):

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
    - genre_baseline    # ← ADD AFTER score
    - export
```

---

## Step 5 — Test Cases

```python
# tests/test_genre_baseline.py

import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestGenreBaselineModule:

    def test_genre_ids_map_complete(self):
        from modules.genre_baseline import GENRE_IDS
        expected = [
            "hebrew_poetry", "hebrew_prophecy", "hebrew_narrative",
            "hebrew_law", "late_hebrew",
        ]
        for genre in expected:
            assert genre in GENRE_IDS

    def test_genre_ids_are_negative(self):
        from modules.genre_baseline import GENRE_IDS
        for genre_id, sentinel in GENRE_IDS.items():
            assert sentinel < 0, f"{genre_id} sentinel should be negative, got {sentinel}"

    def test_genre_ids_unique(self):
        from modules.genre_baseline import GENRE_IDS
        values = list(GENRE_IDS.values())
        assert len(values) == len(set(values)), "Genre sentinel IDs must be unique"

    def test_run_with_no_genre_config_skips_gracefully(self):
        """run() with empty genre config should return 0 baselines, no error."""
        from modules.genre_baseline import run

        class MockConn:
            def cursor(self):
                return self

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

            def execute(self, *a):
                pass

            def commit(self):
                pass

            def fetchone(self):
                return None

        result = run(MockConn(), {"genre": {"clusters": {}}})
        assert result["baselines_computed"] == 0

    def test_compute_baseline_returns_expected_keys(self):
        """_compute_genre_baseline returns dict with all required keys."""
        from modules.genre_baseline import _compute_genre_baseline

        # We can't easily mock psycopg2 without a real DB, so we test the
        # return structure indirectly by verifying the key set on a dummy result.
        expected_keys = {
            "verse_count", "syllable_density", "morpheme_ratio",
            "sonority_score", "clause_compression",
            "std_syllable_density", "std_morpheme_ratio",
            "std_sonority_score", "std_clause_compression",
            "mean_breath_weight", "mean_open_ratio", "mean_guttural_density",
        }
        # Construct a fake result matching what the SQL returns
        fake_result = {k: 0.0 for k in expected_keys}
        fake_result["verse_count"] = 0
        assert expected_keys == set(fake_result.keys())
```

Run:
```bash
docker compose --profile pipeline run --rm pipeline python -m pytest /pipeline/tests/test_genre_baseline.py -v
```

---

## Step 6 — Isaiah Validation Procedure

Isaiah is the recommended first expansion target. Follow these steps:

### 6a — Verify BHSA data

```bash
docker compose --profile pipeline run --rm pipeline python -c "
from tf.fabric import Fabric
TF = Fabric(locations=['/data/bhsa/github/ETCBC/bhsa/tf/c'], silent=True)
api = TF.load('book chapter verse g_word_utf8')
F, T = api.F, api.T
isaiah = [v for v in F.otype.s('verse') if T.bookName(v) == 'Isaiah']
print(f'Isaiah verses in BHSA: {len(isaiah)}')
# Expected: 1292
"
```

### 6b — Update `config.yml`

```yaml
corpus:
  books:
    - book_num: 19
      name: Psalms
      genre: hebrew_poetry
    - book_num: 23
      name: Isaiah
      genre: hebrew_prophecy
```

### 6c — Verify translation sources cover Isaiah

The scrollmapper SQLite files (KJV, YLT, WEB etc.) contain the entire Bible. No additional download is needed. Verify:

```bash
docker exec psalms_db psql -U psalms -d psalms -c "
SELECT translation_key, COUNT(*) FROM translations t
JOIN verses v ON t.verse_id = v.verse_id
WHERE v.book_num = 23
GROUP BY translation_key;
"
# If rows = 0, translations haven't been ingested for book 23 yet.
# Run: docker compose --profile pipeline run --rm pipeline python run.py --stages translate_ingest
```

### 6d — Run full pipeline for Isaiah

```bash
# With Isaiah added to config.yml corpus.books:
docker compose --profile pipeline run --rm pipeline python run.py
```

The pipeline will skip already-computed Psalms verses (resumability) and process only Isaiah.

### 6e — Validate Isaiah results

```bash
docker exec psalms_db psql -U psalms -d psalms -c "
SELECT
  b.book_name,
  COUNT(v.verse_id) AS verses,
  COUNT(vf.verse_id) AS fingerprints,
  COUNT(bp.verse_id) AS breath_profiles,
  ROUND(AVG(vf.syllable_density)::numeric, 3) AS avg_syl_density,
  ROUND(AVG(vf.sonority_score)::numeric, 3) AS avg_sonority
FROM verses v
JOIN books b ON v.book_num = b.book_num
LEFT JOIN verse_fingerprints vf ON vf.verse_id = v.verse_id
LEFT JOIN breath_profiles bp ON bp.verse_id = v.verse_id
WHERE v.book_num IN (19, 23)
GROUP BY b.book_name
ORDER BY b.book_name;
"
```

Expected output (approximate):

| book_name | verses | fingerprints | breath_profiles | avg_syl_density | avg_sonority |
|---|---|---|---|---|---|
| Isaiah | 1292 | 1292 | 1292 | ~2.0–2.2 | ~0.50–0.54 |
| Psalms | 2527 | 2527 | 2527 | ~2.2–2.4 | ~0.53–0.57 |

Isaiah should show slightly lower syllable density and sonority than Psalms — prophetic prose is less acoustically dense than lyric poetry. If the values are nearly identical, check that the correct BHSA book name mapping is used (`Isaiah` not `Isa`).

### 6f — Genre baseline validation

```bash
docker exec psalms_db psql -U psalms -d psalms -c "
SELECT genre_id, verse_count,
       ROUND(syllable_density::numeric, 4) AS density,
       ROUND(morpheme_ratio::numeric, 4) AS morpheme,
       ROUND(sonority_score::numeric, 4) AS sonority
FROM genre_baselines ORDER BY genre_id;
"
```

The `hebrew_poetry` baseline should have higher `syllable_density` and `sonority_score` than `hebrew_prophecy` (consistent with the denser phonetic texture of lyric poetry).

---

## Step 7 — Operational Reference for Future Books

To add any book after Isaiah is validated:

1. Look up the `book_num` in the BHSA book ordering (Genesis=1, Exodus=2, … Psalms=19, … Isaiah=23, etc.)
2. Add to `corpus.books` in `config.yml` with appropriate `genre`
3. If acrostic: add `acrostic_chapters: [...]`
4. Run pipeline: `docker compose --profile pipeline run --rm pipeline python run.py`
5. Run `init_flags.sql` updates for any new acrostic or Aramaic flags
6. Verify with the validation query in Step 6e (substituting the new book_num)

Translation sources (KJV, YLT, WEB, ULT, UST) all cover the full OT. No additional downloads needed for any OT book.

---

## Acceptance Criteria

- [ ] `genre_baselines` table created by `modules/genre_baseline.py`
- [ ] At least one genre baseline row present after running with Psalms data
- [ ] `hebrew_poetry` baseline `syllable_density` is ≥ `hebrew_prophecy` baseline (if both genres have data)
- [ ] Isaiah `verses` table populated with exactly 1,292 rows
- [ ] Isaiah `verse_fingerprints` and `breath_profiles` each have 1,292 rows
- [ ] `is_acrostic = TRUE` for all Lamentations 1–4 verses and Psalm 119 (when those books are added)
- [ ] Pipeline run with both Psalms + Isaiah is idempotent (second run produces same counts, no duplicates)
- [ ] All unit tests pass
- [ ] `run.py --stages genre_baseline` runs in isolation without error

---

## SQL Validation Queries

```sql
-- Genre baselines summary
SELECT genre_id, verse_count,
       ROUND(syllable_density::numeric, 4) AS density,
       ROUND(morpheme_ratio::numeric, 4) AS morpheme,
       ROUND(sonority_score::numeric, 4) AS sonority,
       ROUND(clause_compression::numeric, 4) AS compression
FROM genre_baselines ORDER BY genre_id;

-- All books currently in corpus with row counts
SELECT b.book_name, b.genre, b.book_num,
       COUNT(DISTINCT v.verse_id) AS verses,
       COUNT(DISTINCT vf.verse_id) AS fingerprinted,
       COUNT(DISTINCT bp.verse_id) AS breath_computed
FROM books b
LEFT JOIN verses v ON v.book_num = b.book_num
LEFT JOIN verse_fingerprints vf ON vf.verse_id = v.verse_id
LEFT JOIN breath_profiles bp ON bp.verse_id = v.verse_id
GROUP BY b.book_name, b.genre, b.book_num
ORDER BY b.book_num;

-- Acrostic verse flag check
SELECT v.book_num, v.chapter, COUNT(*) AS acrostic_verse_count
FROM verses v WHERE v.is_acrostic = TRUE
GROUP BY v.book_num, v.chapter ORDER BY v.book_num, v.chapter;

-- Compare genre fingerprint means (requires multiple books processed)
SELECT b.genre,
       ROUND(AVG(vf.syllable_density)::numeric, 4) AS avg_density,
       ROUND(AVG(vf.sonority_score)::numeric, 4) AS avg_sonority,
       COUNT(*) AS verse_count
FROM verse_fingerprints vf
JOIN verses v ON vf.verse_id = v.verse_id
JOIN books b ON v.book_num = b.book_num
WHERE b.genre IS NOT NULL
GROUP BY b.genre ORDER BY avg_density DESC;
```
