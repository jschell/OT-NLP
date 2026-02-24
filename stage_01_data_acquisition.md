# Stage 1 — Data Acquisition & Configuration
## Detailed Implementation Plan

> **Depends on:** Stage 0 (running Docker stack, initialized schema)  
> **Produces:** All Tier 1 translation data ingested into PostgreSQL; BHSA dataset mounted; translation adapter fully tested; `config.yml` complete  
> **Estimated time:** 1–3 hours (dominated by download time)

---

## Objectives

1. Download BHSA Hebrew Bible dataset via text-fabric
2. Download all Tier 1 translation source files (SQLite + USFM)
3. Implement `adapters/translation_adapter.py` with SQLite, USFM, and API adapter classes
4. Run one-time ingest of all translation text into PostgreSQL `translations` table
5. Validate each configured translation returns correct text for known verses

---

## Important: One Database, One Truth

SQLite files and USFM directories are **source formats** used exactly once during this stage. After ingest completes, every subsequent stage reads translation text exclusively from PostgreSQL. The source files become inert downloads and may be archived or deleted without affecting the pipeline.

```
Source files on disk          Stage 1 ingest          PostgreSQL (all stages)
──────────────────────    ──────────────────────►   ──────────────────────────
t_kjv.db   (SQLite)           read once              translations table
t_ylt.db   (SQLite)           read once                verse_id | key | text
ult/       (USFM dir)         read once                  ...
ust/       (USFM dir)         read once
```

---

## Step 1 — Download BHSA Dataset

The BHSA (Bible Hebrew Syntactic Analysis) dataset is fetched via the `text-fabric` Python library. Run this inside the pipeline container:

```bash
docker compose --profile pipeline run --rm pipeline python -c "
import text_fabric
from tf.app import use
A = use('ETCBC/bhsa', hoist=globals(), checkout='clone')
"
```

This downloads ~200 MB to the text-fabric cache, which maps to `/data/bhsa/` via the volume mount. The `checkout='clone'` flag pins to the current GitHub HEAD — note the exact commit hash in `config.yml` after download completes.

Expected directory after download:
```
/data/bhsa/
  github/
    ETCBC/
      bhsa/
        tf/
          c/           ← complete Hebrew Bible corpus in TF format
```

Verify the Psalms corpus is present:
```bash
docker compose --profile pipeline run --rm pipeline python -c "
from tf.fabric import Fabric
TF = Fabric(locations=['/data/bhsa/github/ETCBC/bhsa/tf/c'], silent=True)
api = TF.load('book chapter verse g_word_utf8 lex sp')
psalms = [v for v in api.F.verse.s() if api.T.bookName(v) == 'Psalms']
print(f'Psalms verses: {len(psalms)}')
"
# Expected: Psalms verses: 2527
```

---

## Step 2 — Download Translation Source Files

Run from PowerShell on the Windows host (these are one-time downloads):

```powershell
$translations = "C:\psalms-nlp\data\translations"

# scrollmapper SQLite databases
$bases = @("t_kjv","t_ylt","t_web","t_asv","t_dby")
foreach ($b in $bases) {
    $url = "https://github.com/scrollmapper/bible_databases/raw/master/sqlite/$b.db"
    Invoke-WebRequest -Uri $url -OutFile "$translations\$b.db"
    Write-Host "Downloaded $b.db"
}

# unfoldingWord USFM repositories (clone — preserves version history)
Set-Location $translations
git clone --depth 1 https://github.com/unfoldingWord/en_ult ult
git clone --depth 1 https://github.com/unfoldingWord/en_ust ust
```

Expected result:
```
C:\psalms-nlp\data\translations\
  t_kjv.db  t_ylt.db  t_web.db  t_asv.db  t_dby.db
  ult\       (USFM files: 19PSA.usfm and others)
  ust\       (USFM files: 19PSA.usfm and others)
```

---

## Step 3 — File: `adapters/translation_adapter.py`

```python
"""
Translation adapter layer.

All adapters implement a single interface: get_verses(book_num) -> dict[tuple, str]
where the key is (chapter, verse_num) and the value is the verse text.

This module is used exactly once per translation, during Stage 1 ingest.
After ingest, all translation text is read from PostgreSQL.
"""

from __future__ import annotations

import re
import sqlite3
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Tuple

import yaml


VerseMap = Dict[Tuple[int, int], str]   # {(chapter, verse): text}


# ─────────────────────────────────────────────────────────────────
# Base class
# ─────────────────────────────────────────────────────────────────

class TranslationAdapter(ABC):
    """Base class for all translation source adapters."""

    def __init__(self, source_config: dict):
        self.id = source_config["id"]
        self.config = source_config

    @abstractmethod
    def get_verses(self, book_num: int) -> VerseMap:
        """Return all verses for book_num as {(chapter, verse): text}."""
        ...


# ─────────────────────────────────────────────────────────────────
# SQLite adapter (scrollmapper format)
# Schema: CREATE TABLE t (b INTEGER, c INTEGER, v INTEGER, t TEXT)
# Book 19 = Psalms
# ─────────────────────────────────────────────────────────────────

class SQLiteScrollmapperAdapter(TranslationAdapter):

    def get_verses(self, book_num: int) -> VerseMap:
        path = self.config["path"]
        if not Path(path).exists():
            raise FileNotFoundError(f"SQLite file not found: {path}")

        conn = sqlite3.connect(path)
        try:
            cur = conn.execute(
                "SELECT c, v, t FROM t WHERE b = ? ORDER BY c, v",
                (book_num,)
            )
            return {(int(row[0]), int(row[1])): row[2].strip() for row in cur}
        finally:
            conn.close()


# ─────────────────────────────────────────────────────────────────
# USFM adapter (unfoldingWord / eBible format)
# Reads directory of .usfm files; locates the file for the given
# book using the standard two-digit book number prefix.
# ─────────────────────────────────────────────────────────────────

class USFMAdapter(TranslationAdapter):

    # Map book_num → USFM 2-digit book code (OT books 1-39)
    BOOK_CODES = {
        1: "01", 2: "02", 3: "03", 4: "04", 5: "05",
        6: "06", 7: "07", 8: "08", 9: "09", 10: "10",
        11: "11", 12: "12", 13: "13", 14: "14", 15: "15",
        16: "16", 17: "17", 18: "18", 19: "19", 20: "20",
        21: "21", 22: "22", 23: "23", 24: "24", 25: "25",
        26: "26", 27: "27", 28: "28", 29: "29", 30: "30",
        31: "31", 32: "32", 33: "33", 34: "34", 35: "35",
        36: "36", 37: "37", 38: "38", 39: "39",
    }

    def get_verses(self, book_num: int) -> VerseMap:
        usfm_dir = Path(self.config["path"])
        code = self.BOOK_CODES.get(book_num)
        if code is None:
            raise ValueError(f"No USFM book code mapping for book_num={book_num}")

        # Locate file — unfoldingWord uses pattern like 19PSA.usfm
        candidates = list(usfm_dir.glob(f"{code}*.usfm"))
        if not candidates:
            raise FileNotFoundError(
                f"No USFM file matching '{code}*.usfm' in {usfm_dir}"
            )
        usfm_path = candidates[0]

        return self._parse_usfm(usfm_path.read_text(encoding="utf-8"))

    def _parse_usfm(self, text: str) -> VerseMap:
        """
        Minimal USFM parser. Handles:
          \\c <chapter>
          \\v <verse> <text...>
          \\p  \\q  \\q2  \\m  (paragraph markers — ignored)
          Inline markers: \\w..\\w* \\f..\\f* \\x..\\x* stripped
        """
        verses: VerseMap = {}
        chapter = 0
        verse_num = 0
        verse_parts: List[str] = []

        def flush():
            if chapter and verse_num and verse_parts:
                text = " ".join(verse_parts).strip()
                text = re.sub(r"\s+", " ", text)
                verses[(chapter, verse_num)] = text

        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue

            if line.startswith("\\c "):
                flush()
                verse_parts = []
                verse_num = 0
                chapter = int(line.split()[1])
                continue

            if line.startswith("\\v "):
                flush()
                verse_parts = []
                parts = line.split(None, 2)
                verse_num = int(parts[1])
                if len(parts) > 2:
                    verse_parts.append(self._strip_markers(parts[2]))
                continue

            # Continuation of current verse (paragraph markers etc.)
            if chapter and verse_num and not line.startswith("\\"):
                verse_parts.append(self._strip_markers(line))
            elif chapter and verse_num and line.startswith("\\") and not any(
                line.startswith(m) for m in ("\\c ", "\\v ", "\\s", "\\ms", "\\mt")
            ):
                # Inline paragraph markers — append cleaned tail if any
                tail = re.sub(r"^\\[a-z0-9]+\*?\s*", "", line)
                if tail:
                    verse_parts.append(self._strip_markers(tail))

        flush()
        return verses

    @staticmethod
    def _strip_markers(text: str) -> str:
        """Remove USFM inline character markers and footnote/cross-ref spans."""
        # Remove footnote/cross-ref spans: \f ... \f*  and \x ... \x*
        text = re.sub(r"\\[fx].*?\\[fx]\*", "", text, flags=re.DOTALL)
        # Remove word-level markup: \w word|lemma="..." \w*
        text = re.sub(r"\\w\s*(.*?)\|[^\\]*?\\w\*", r"\1", text)
        # Remove remaining inline markers: \add \nd \bk etc.
        text = re.sub(r"\\[a-z0-9]+\*?", "", text)
        return text.strip()


# ─────────────────────────────────────────────────────────────────
# API adapter (ESV and similar — query at ingest time, not score time)
# For translations with no downloadable corpus but a free API.
# ─────────────────────────────────────────────────────────────────

class APIAdapter(TranslationAdapter):

    # Book names as used by the ESV API
    ESV_BOOK_NAMES = {
        19: "Psalms",
        23: "Isaiah",
        18: "Job",
        25: "Lamentations",
    }

    def get_verses(self, book_num: int) -> VerseMap:
        import requests, time

        provider = self.config.get("provider", "esv")
        if provider != "esv":
            raise NotImplementedError(f"API provider '{provider}' not implemented")

        api_key_env = self.config.get("api_key_env", "ESV_API_KEY")
        api_key = os.environ.get(api_key_env)
        if not api_key:
            raise EnvironmentError(
                f"API key not set. Set environment variable: {api_key_env}"
            )

        book_name = self.ESV_BOOK_NAMES.get(book_num)
        if not book_name:
            raise ValueError(f"No ESV book mapping for book_num={book_num}")

        rate_limit = self.config.get("rate_limit", 500)  # requests per day
        pause = 86400 / rate_limit  # seconds between requests

        headers = {"Authorization": f"Token {api_key}"}
        base_url = "https://api.esv.org/v3/passage/text/"
        verses: VerseMap = {}

        # ESV API: request chapter by chapter
        # Psalm chapter count is 150; adjust for other books
        chapter_counts = {19: 150, 23: 66, 18: 42, 25: 5}
        num_chapters = chapter_counts.get(book_num, 50)

        for ch in range(1, num_chapters + 1):
            params = {
                "q": f"{book_name} {ch}",
                "include-headings": "false",
                "include-footnotes": "false",
                "include-verse-numbers": "true",
                "include-short-copyright": "false",
            }
            resp = requests.get(base_url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            for passage in data.get("passages", []):
                for match in re.finditer(r"\[(\d+)\]\s*(.*?)(?=\[|\Z)", passage, re.DOTALL):
                    v_num = int(match.group(1))
                    v_text = re.sub(r"\s+", " ", match.group(2)).strip()
                    if v_text:
                        verses[(ch, v_num)] = v_text

            time.sleep(pause)

        return verses


# ─────────────────────────────────────────────────────────────────
# Adapter registry and factory
# ─────────────────────────────────────────────────────────────────

_ADAPTER_MAP = {
    "sqlite_scrollmapper": SQLiteScrollmapperAdapter,
    "usfm":                USFMAdapter,
    "api":                 APIAdapter,
}


def get_adapter(source_config: dict) -> TranslationAdapter:
    """Return the correct adapter instance for a source config block."""
    fmt = source_config.get("format")
    cls = _ADAPTER_MAP.get(fmt)
    if cls is None:
        raise ValueError(
            f"Unknown translation format '{fmt}'. "
            f"Valid options: {list(_ADAPTER_MAP.keys())}"
        )
    return cls(source_config)
```

---

## Step 4 — File: `modules/ingest_translations.py`

This module performs the one-time ingest of all configured translation sources into PostgreSQL.

```python
"""
Stage 1 — Translation ingest.

Reads all translation sources configured in config.yml via their adapters,
then upserts verse text into the `translations` table in PostgreSQL.

After this module runs, no other stage ever reads SQLite or USFM files.
All translation text lives in PostgreSQL.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg2
import psycopg2.extras

from adapters.translation_adapter import get_adapter

logger = logging.getLogger(__name__)


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    """
    Ingest all configured translations.

    Returns a summary dict:
        {translation_id: row_count, ...}
    """
    sources = config.get("translations", {}).get("sources", [])
    corpus_books = [b["book_num"] for b in config.get("corpus", {}).get("books", [])]

    if not sources:
        logger.warning("No translation sources configured. Skipping.")
        return {}

    summary: dict[str, int] = {}

    for source in sources:
        t_id = source["id"]
        logger.info(f"Ingesting translation: {t_id}")

        try:
            adapter = get_adapter(source)
        except Exception as e:
            logger.error(f"Failed to create adapter for {t_id}: {e}")
            raise

        total_rows = 0
        for book_num in corpus_books:
            try:
                verse_map = adapter.get_verses(book_num)
            except Exception as e:
                logger.error(f"Failed to read {t_id} book {book_num}: {e}")
                raise

            rows = _upsert_verses(conn, t_id, book_num, verse_map)
            total_rows += rows
            logger.info(f"  Book {book_num}: {rows} verses ingested")

        summary[t_id] = total_rows
        logger.info(f"  Total for {t_id}: {total_rows} verses")

    return summary


def _upsert_verses(
    conn: psycopg2.extensions.connection,
    translation_key: str,
    book_num: int,
    verse_map: dict,
) -> int:
    """
    Upsert translation verse rows.

    Requires that `verses` table is already populated for this book
    (which happens in Stage 2 ingest). For Stage 1 we only check that
    verse records exist; if not, we skip and log a warning.
    """
    with conn.cursor() as cur:
        # Fetch verse_ids for this book
        cur.execute(
            "SELECT chapter, verse_num, verse_id FROM verses WHERE book_num = %s",
            (book_num,)
        )
        verse_lookup = {(r[0], r[1]): r[2] for r in cur.fetchall()}

    if not verse_lookup:
        logger.warning(
            f"No verses found in 'verses' table for book_num={book_num}. "
            "Run Stage 2 ingest first, then re-run translation ingest."
        )
        return 0

    rows_to_insert = []
    for (chapter, verse_num), text in verse_map.items():
        verse_id = verse_lookup.get((chapter, verse_num))
        if verse_id is None:
            logger.debug(f"  Skipping {translation_key} {book_num}:{chapter}:{verse_num} — no verse record")
            continue
        rows_to_insert.append((verse_id, translation_key, text))

    if not rows_to_insert:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO translations (verse_id, translation_key, verse_text)
            VALUES %s
            ON CONFLICT (verse_id, translation_key) DO UPDATE
              SET verse_text = EXCLUDED.verse_text
            """,
            rows_to_insert,
        )
    conn.commit()
    return len(rows_to_insert)
```

---

## Step 5 — File: `modules/validate_data.py`

```python
"""
Stage 1 — Data validation.

Checks that known verses return expected text.
Raises AssertionError with a clear message if any check fails.
"""

import logging
import psycopg2

logger = logging.getLogger(__name__)

# Known-good verse checks: (book_num, chapter, verse, translation_key, expected_prefix)
CHECKS = [
    # Psalm 23:1
    (19, 23, 1, "KJV", "The LORD is my shepherd"),
    (19, 23, 1, "YLT", "Jehovah"),
    (19, 23, 1, "WEB", "Yahweh"),
    (19, 23, 1, "ULT", "Yahweh"),
    (19, 23, 1, "UST", "God"),
    # Psalm 1:1
    (19, 1,  1, "KJV", "Blessed"),
    # Psalm 150:6 (last verse)
    (19, 150, 6, "KJV", "Let every thing that hath breath"),
]


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    failed = []
    passed = 0

    for book_num, chapter, verse_num, key, expected_prefix in CHECKS:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.verse_text
                FROM translations t
                JOIN verses v ON t.verse_id = v.verse_id
                WHERE v.book_num = %s AND v.chapter = %s AND v.verse_num = %s
                  AND t.translation_key = %s
                """,
                (book_num, chapter, verse_num, key)
            )
            row = cur.fetchone()

        if row is None:
            failed.append(f"MISSING: {key} {book_num}:{chapter}:{verse_num}")
        elif not row[0].startswith(expected_prefix):
            failed.append(
                f"WRONG TEXT: {key} {book_num}:{chapter}:{verse_num}\n"
                f"  Expected prefix: '{expected_prefix}'\n"
                f"  Got: '{row[0][:60]}'"
            )
        else:
            passed += 1

    if failed:
        msg = f"Data validation failed ({len(failed)} checks):\n" + "\n".join(failed)
        logger.error(msg)
        raise AssertionError(msg)

    logger.info(f"Data validation passed: {passed}/{len(CHECKS)} checks")
    return {"passed": passed, "failed": 0}
```

---

## Step 6 — File: `modules/count_verses.py`

Quick utility to verify expected row counts:

```python
"""Verify expected verse counts per book per translation."""

import logging
import psycopg2

logger = logging.getLogger(__name__)

EXPECTED_VERSE_COUNTS = {
    19: 2527,   # Psalms
}


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    sources = config.get("translations", {}).get("sources", [])
    corpus_books = [b["book_num"] for b in config.get("corpus", {}).get("books", [])]
    results = {}

    for book_num in corpus_books:
        expected = EXPECTED_VERSE_COUNTS.get(book_num)
        for source in sources:
            key = source["id"]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM translations t
                    JOIN verses v ON t.verse_id = v.verse_id
                    WHERE v.book_num = %s AND t.translation_key = %s
                    """,
                    (book_num, key)
                )
                count = cur.fetchone()[0]

            status = "OK" if (expected is None or count >= expected * 0.98) else "LOW"
            results[f"{key}:{book_num}"] = {"count": count, "expected": expected, "status": status}

            if status == "LOW":
                logger.warning(
                    f"Low verse count for {key} book {book_num}: "
                    f"got {count}, expected ~{expected}"
                )
            else:
                logger.info(f"{key} book {book_num}: {count} verses ({status})")

    return results
```

---

## Step 7 — Run Stage 1

```bash
# From Windows host:
docker compose --profile pipeline run --rm pipeline python -c "
import yaml, psycopg2, logging, sys
sys.path.insert(0, '/pipeline')
logging.basicConfig(level=logging.INFO)

with open('/pipeline/config.yml') as f:
    config = yaml.safe_load(f)

conn = psycopg2.connect(
    host='db', dbname='psalms', user='psalms', password='psalms_dev'
)

from modules.ingest_translations import run as ingest
from modules.validate_data import run as validate

result = ingest(conn, config)
print('Ingest summary:', result)

# Note: full validate only works after Stage 2 populates 'verses' table
# Run this script again after Stage 2 to confirm text correctness
"
```

> **Sequencing note:** Translation ingest requires verse records to exist in the `verses` table, which is populated in Stage 2. The adapter reads source files and holds the data; if the `verses` table is empty, it logs a warning and skips. After Stage 2 runs, re-run this script to complete the ingest.

The full recommended execution order is:
1. Stage 1: Download files, test adapters
2. Stage 2: Run `modules/ingest.py` to populate `verses` and `word_tokens`
3. Stage 1 (re-run `ingest_translations.py`): Now that `verses` exists, fully ingest translations
4. Stage 1: Run `validate_data.py` to confirm correctness

---

## Acceptance Criteria

- [ ] `/data/bhsa/github/ETCBC/bhsa/tf/c/` directory exists and contains TF files
- [ ] `python -c "from tf.fabric import Fabric; ..."` reports exactly 2,527 Psalms verses
- [ ] All 5 SQLite files present in `/data/translations/` and readable
- [ ] `ult/19PSA.usfm` and `ust/19PSA.usfm` exist
- [ ] `adapters/translation_adapter.py` passes adapter unit tests (see below)
- [ ] After Stage 2: all 5 translations present in `translations` table for Psalms
- [ ] After Stage 2: `validate_data.py` passes all checks with 0 failures
- [ ] After Stage 2: `count_verses.py` reports ≥2480 verses per translation (≥98% of 2527)

---

## Test Cases

Save as `tests/test_translation_adapters.py`:

```python
"""Unit tests for translation adapters. Run with: pytest tests/"""

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from adapters.translation_adapter import (
    SQLiteScrollmapperAdapter,
    USFMAdapter,
    get_adapter,
)


# ── SQLite Adapter ────────────────────────────────────────────

def make_sqlite_db(tmp_path: Path, rows: list) -> str:
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (b INTEGER, c INTEGER, v INTEGER, t TEXT)")
    conn.executemany("INSERT INTO t VALUES (?,?,?,?)", rows)
    conn.commit()
    conn.close()
    return db_path


def test_sqlite_adapter_returns_correct_verses(tmp_path):
    db_path = make_sqlite_db(tmp_path, [
        (19, 23, 1, "The LORD is my shepherd"),
        (19, 23, 2, "He maketh me to lie down"),
        (19,  1, 1, "Blessed is the man"),
    ])
    adapter = SQLiteScrollmapperAdapter({"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path})
    verses = adapter.get_verses(book_num=19)
    assert (23, 1) in verses
    assert verses[(23, 1)] == "The LORD is my shepherd"
    assert (23, 2) in verses
    assert len(verses) == 3


def test_sqlite_adapter_filters_by_book(tmp_path):
    db_path = make_sqlite_db(tmp_path, [
        (19, 1, 1, "Psalms verse"),
        (1,  1, 1, "Genesis verse"),
    ])
    adapter = SQLiteScrollmapperAdapter({"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path})
    verses = adapter.get_verses(book_num=19)
    assert len(verses) == 1
    assert (1, 1) in verses


def test_sqlite_adapter_raises_for_missing_file():
    adapter = SQLiteScrollmapperAdapter({"id": "KJV", "format": "sqlite_scrollmapper", "path": "/nonexistent/path.db"})
    with pytest.raises(FileNotFoundError):
        adapter.get_verses(19)


# ── USFM Adapter ──────────────────────────────────────────────

SAMPLE_USFM = r"""
\id PSA
\c 1
\p
\v 1 Blessed is the man that walketh not in the counsel of the ungodly,
\v 2 But his delight is in the law of the LORD;
\c 2
\q
\v 1 Why do the heathen rage,
\v 2 and the people imagine a vain thing?
"""


def make_usfm_dir(tmp_path: Path, content: str) -> Path:
    usfm_dir = tmp_path / "ult"
    usfm_dir.mkdir()
    (usfm_dir / "19PSA.usfm").write_text(content, encoding="utf-8")
    return usfm_dir


def test_usfm_adapter_parses_chapters_and_verses(tmp_path):
    usfm_dir = make_usfm_dir(tmp_path, SAMPLE_USFM)
    adapter = USFMAdapter({"id": "ULT", "format": "usfm", "path": str(usfm_dir)})
    verses = adapter.get_verses(19)
    assert (1, 1) in verses
    assert "Blessed" in verses[(1, 1)]
    assert (2, 1) in verses
    assert "heathen" in verses[(2, 1)]


def test_usfm_adapter_strips_footnotes(tmp_path):
    usfm_with_footnotes = r"""
\c 1
\v 1 The word\f + \fr 1:1 \ft footnote text\f* of the LORD.
"""
    usfm_dir = make_usfm_dir(tmp_path, usfm_with_footnotes)
    adapter = USFMAdapter({"id": "ULT", "format": "usfm", "path": str(usfm_dir)})
    verses = adapter.get_verses(19)
    assert "footnote" not in verses[(1, 1)]
    assert "word" in verses[(1, 1)]


def test_usfm_adapter_strips_word_markers(tmp_path):
    usfm_with_w = r"""
\c 1
\v 1 \w Blessed|lemma="blessed" x-morph="Adj"\w* is the man.
"""
    usfm_dir = make_usfm_dir(tmp_path, usfm_with_w)
    adapter = USFMAdapter({"id": "ULT", "format": "usfm", "path": str(usfm_dir)})
    verses = adapter.get_verses(19)
    assert verses[(1, 1)] == "Blessed is the man."


def test_usfm_adapter_raises_for_missing_file(tmp_path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    adapter = USFMAdapter({"id": "ULT", "format": "usfm", "path": str(empty_dir)})
    with pytest.raises(FileNotFoundError):
        adapter.get_verses(19)


# ── Registry ──────────────────────────────────────────────────

def test_get_adapter_returns_correct_type(tmp_path):
    db_path = make_sqlite_db(tmp_path, [])
    adapter = get_adapter({"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path})
    assert isinstance(adapter, SQLiteScrollmapperAdapter)


def test_get_adapter_raises_for_unknown_format():
    with pytest.raises(ValueError, match="Unknown translation format"):
        get_adapter({"id": "X", "format": "magic_format"})
```

Run tests:
```bash
docker compose --profile pipeline run --rm pipeline python -m pytest /pipeline/tests/test_translation_adapters.py -v
```

Expected output: 10 passed.
