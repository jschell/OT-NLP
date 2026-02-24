# Plan: Stage 1 — Data Acquisition & Translation Adapters

> **Depends on:** Plan 00 (Stage 0) — running Docker stack, initialized schema,
> healthy `psalms_db` container with all 11 tables created.
> **Folder:** active

## Goal

Download all source translation files, implement and test the translation adapter layer,
and set up the ingest module so that all configured translations can be loaded into
PostgreSQL once Stage 2 has populated the `verses` table.

## Architecture

Translation source files (SQLite and USFM) are treated as read-once inputs. An adapter
class per format normalizes each source into a `{(chapter, verse_num): text}` mapping.
A factory function selects the correct adapter from the `config.yml` source block. The
`ingest_translations` module calls each adapter, looks up `verse_id` values from the
`verses` table, and bulk-upserts rows into `translations`. Because `translations` references
`verses` via a foreign key, the full ingest run cannot complete until Stage 2 populates
`verses`; the module detects an empty `verses` table, logs a warning, and exits cleanly with
zero rows written. Stage 1 is therefore fully testable without a live database.

**Execution order:**
1. Stage 1 — download files, test adapters, wire up `ingest_translations` module
2. Stage 2 — populate `verses` and `word_tokens` from BHSA
3. Stage 1 re-run — `ingest_translations` now resolves `verse_id` lookups and writes rows
4. Stage 1 validation — `validate_data.py` confirms correct text for known verses

## Tech Stack

- `psycopg2` (not psycopg3) for PostgreSQL access
- `sqlite3` (stdlib) for scrollmapper SQLite files
- `re` (stdlib) for USFM marker stripping
- `pathlib.Path` for file resolution
- `pytest` + `tmp_path` fixture for adapter unit tests
- `unittest.mock` for DB-level tests without a live container

## Acceptance Criteria

After Stage 2 has run and Stage 1 ingest is re-executed:

- `/data/bhsa/github/ETCBC/bhsa/tf/c/` directory exists and contains TF files
- `python -c "from tf.fabric import Fabric; ..."` reports exactly 2,527 Psalms verses
- All 5 SQLite files present in `/data/translations/` and readable
- `ult/19PSA.usfm` and `ust/19PSA.usfm` exist
- All 10 adapter unit tests pass
- All 5 translations present in `translations` table for Psalms
- `validate_data.py` passes all checks with 0 failures
- `count_verses.py` reports >= 2,480 verses per translation (>= 98% of 2,527)

Verification commands (run after Stage 2):

```bash
# BHSA corpus present
docker compose --profile pipeline run --rm pipeline python -c "
from tf.fabric import Fabric
TF = Fabric(locations=['/data/bhsa/github/ETCBC/bhsa/tf/c'], silent=True)
api = TF.load('book chapter verse g_word_utf8 lex sp')
psalms = [v for v in api.F.verse.s() if api.T.bookName(v) == 'Psalms']
print(f'Psalms verses: {len(psalms)}')
"
# Expected: Psalms verses: 2527

# Translation row counts (after Stage 2 re-run)
docker exec psalms_db psql -U psalms -d psalms -c "
SELECT translation_key, COUNT(*) AS verse_count
FROM translations
GROUP BY translation_key
ORDER BY translation_key;
"
# Expected: 5 rows, each with ~2527 verses

# Spot-check Psalm 23:1 KJV
docker exec psalms_db psql -U psalms -d psalms -c "
SELECT t.verse_text
FROM translations t
JOIN verses v ON t.verse_id = v.verse_id
WHERE v.book_num = 19 AND v.chapter = 23 AND v.verse_num = 1
  AND t.translation_key = 'KJV';
"
# Expected: The LORD is my shepherd; I shall not want.
```

---

## Tasks

### Task 1: conftest.py DB Fixture

**Files:**
- `/home/user/OT-NLP/tests/conftest.py`

**Steps:**

1. Write the DB fixture test — verify the fixture creates a mock connection:

   ```python
   # In tests/test_conftest_fixtures.py
   """Tests that shared pytest fixtures are correctly defined."""

   from unittest.mock import MagicMock

   import psycopg2


   def test_mock_conn_fixture_is_available(mock_conn: MagicMock) -> None:
       """mock_conn fixture must be available in tests."""
       assert mock_conn is not None


   def test_mock_conn_has_cursor(mock_conn: MagicMock) -> None:
       """mock_conn must support .cursor() context manager."""
       with mock_conn.cursor() as cur:
           assert cur is not None
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_conftest_fixtures.py -v
   # Expected: FAILED — fixture 'mock_conn' not found
   ```

3. Create `tests/conftest.py` at `/home/user/OT-NLP/tests/conftest.py`:

   ```python
   """
   Shared pytest fixtures for all pipeline stage tests.

   The root conftest.py (repo root) handles sys.path setup.
   This file provides database and config fixtures.
   """

   from __future__ import annotations

   from contextlib import contextmanager
   from typing import Generator
   from unittest.mock import MagicMock, patch

   import pytest


   @pytest.fixture()
   def mock_conn() -> MagicMock:
       """
       Return a MagicMock that behaves like a psycopg2 connection.

       The cursor is set up as a context manager so ``with conn.cursor() as cur``
       works without a live database.
       """
       conn = MagicMock()
       cursor = MagicMock()

       # Support: with conn.cursor() as cur:
       conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
       conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

       return conn


   @pytest.fixture()
   def minimal_config() -> dict:
       """
       Return the smallest valid config dict needed by pipeline modules.

       Mirrors the structure of pipeline/config.yml without requiring
       the file to be present on disk during unit tests.
       """
       return {
           "pipeline": {"name": "psalms_nlp", "version": "0.1.0"},
           "corpus": {
               "books": [{"book_num": 19, "name": "Psalms"}],
               "debug_chapters": [],
           },
           "bhsa": {"data_path": "/data/bhsa"},
           "translations": {
               "sources": [
                   {
                       "id": "KJV",
                       "format": "sqlite_scrollmapper",
                       "path": "/data/translations/t_kjv.db",
                   }
               ]
           },
           "fingerprint": {"batch_size": 100, "conflict_mode": "skip"},
           "breath": {"batch_size": 100, "conflict_mode": "skip"},
           "scoring": {
               "batch_size": 100,
               "conflict_mode": "skip",
               "deviation_weights": {
                   "density": 0.35,
                   "morpheme": 0.25,
                   "sonority": 0.20,
                   "compression": 0.20,
               },
           },
           "llm": {"provider": "none", "model": "", "max_tokens": 256},
           "export": {"output_dir": "/data/outputs"},
       }
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_conftest_fixtures.py -v
   # Expected: 2 passed
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"test: add tests/conftest.py with mock_conn and minimal_config fixtures"`

---

### Task 2: TDD — SQLiteScrollmapperAdapter

**Files:**
- `/home/user/OT-NLP/tests/test_translation_adapters.py` (partial — SQLite section)
- `/home/user/OT-NLP/pipeline/adapters/__init__.py`
- `/home/user/OT-NLP/pipeline/adapters/translation_adapter.py` (partial)

**Steps:**

1. Write the SQLite adapter tests in `tests/test_translation_adapters.py`:

   ```python
   """Unit tests for translation adapters."""

   from __future__ import annotations

   import sqlite3
   from pathlib import Path

   import pytest

   from adapters.translation_adapter import (
       SQLiteScrollmapperAdapter,
       USFMAdapter,
       APIAdapter,
       get_adapter,
   )


   # ── Helpers ────────────────────────────────────────────────────

   def make_sqlite_db(tmp_path: Path, rows: list[tuple]) -> str:
       """Create a temporary scrollmapper-format SQLite database."""
       db_path = str(tmp_path / "test.db")
       conn = sqlite3.connect(db_path)
       conn.execute(
           "CREATE TABLE t (b INTEGER, c INTEGER, v INTEGER, t TEXT)"
       )
       conn.executemany("INSERT INTO t VALUES (?,?,?,?)", rows)
       conn.commit()
       conn.close()
       return db_path


   def make_usfm_dir(tmp_path: Path, content: str) -> Path:
       """Create a temporary directory with a 19PSA.usfm file."""
       usfm_dir = tmp_path / "ult"
       usfm_dir.mkdir()
       (usfm_dir / "19PSA.usfm").write_text(content, encoding="utf-8")
       return usfm_dir


   # ── SQLiteScrollmapperAdapter ──────────────────────────────────

   def test_sqlite_adapter_returns_psalm_23_1(tmp_path: Path) -> None:
       """SQLite adapter must return correct text for Psalm 23:1."""
       db_path = make_sqlite_db(
           tmp_path,
           [
               (19, 23, 1, "The LORD is my shepherd; I shall not want."),
               (19, 23, 2, "He maketh me to lie down in green pastures."),
           ],
       )
       adapter = SQLiteScrollmapperAdapter(
           {"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path}
       )
       verses = adapter.get_verses(book_num=19)

       assert (23, 1) in verses
       assert verses[(23, 1)] == "The LORD is my shepherd; I shall not want."


   def test_sqlite_adapter_returns_none_for_nonexistent_verse(
       tmp_path: Path,
   ) -> None:
       """SQLite adapter must return an empty map when no verses match."""
       db_path = make_sqlite_db(tmp_path, [(19, 23, 1, "Some text")])
       adapter = SQLiteScrollmapperAdapter(
           {"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path}
       )
       verses = adapter.get_verses(book_num=19)

       # Verse (99, 99) does not exist in the DB
       assert (99, 99) not in verses


   def test_sqlite_adapter_raises_for_missing_file() -> None:
       """SQLite adapter must raise FileNotFoundError for a missing .db file."""
       adapter = SQLiteScrollmapperAdapter(
           {
               "id": "KJV",
               "format": "sqlite_scrollmapper",
               "path": "/nonexistent/path.db",
           }
       )
       with pytest.raises(FileNotFoundError, match="/nonexistent/path.db"):
           adapter.get_verses(19)


   def test_sqlite_adapter_filters_by_book_num(tmp_path: Path) -> None:
       """SQLite adapter must only return verses for the requested book_num."""
       db_path = make_sqlite_db(
           tmp_path,
           [
               (19, 1, 1, "Psalms verse"),
               (1, 1, 1, "Genesis verse"),
           ],
       )
       adapter = SQLiteScrollmapperAdapter(
           {"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path}
       )
       verses = adapter.get_verses(book_num=19)

       assert len(verses) == 1
       assert (1, 1) in verses
       assert verses[(1, 1)] == "Psalms verse"
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_translation_adapters.py \
     -k "sqlite" -v
   # Expected: FAILED — ImportError: cannot import name 'SQLiteScrollmapperAdapter'
   ```

3. Create `pipeline/adapters/__init__.py` (empty):

   ```python
   # adapters package
   ```

4. Create the base class and `SQLiteScrollmapperAdapter` in
   `pipeline/adapters/translation_adapter.py` at
   `/home/user/OT-NLP/pipeline/adapters/translation_adapter.py`:

   ```python
   """
   Translation adapter layer.

   Each adapter implements a single interface:

       get_verses(book_num: int) -> VerseMap

   where VerseMap = dict[(chapter, verse_num), text].

   This module is used exactly once per translation during Stage 1 ingest.
   After ingest completes, all translation text is read from PostgreSQL;
   the source files on disk become inert.
   """

   from __future__ import annotations

   import re
   import sqlite3
   from abc import ABC, abstractmethod
   from pathlib import Path
   from typing import Dict, List, Tuple

   VerseMap = Dict[Tuple[int, int], str]   # {(chapter, verse_num): text}


   # ──────────────────────────────────────────────────────────────────
   # Base class
   # ──────────────────────────────────────────────────────────────────

   class TranslationAdapter(ABC):
       """Abstract base for all translation source adapters."""

       def __init__(self, source_config: dict) -> None:
           """Initialise from a config.yml source block."""
           self.id: str = source_config["id"]
           self.config: dict = source_config

       @abstractmethod
       def get_verses(self, book_num: int) -> VerseMap:
           """Return all verses for book_num as {(chapter, verse_num): text}."""
           ...


   # ──────────────────────────────────────────────────────────────────
   # SQLite adapter — scrollmapper format
   # Schema: CREATE TABLE t (b INT, c INT, v INT, t TEXT)
   # ──────────────────────────────────────────────────────────────────

   class SQLiteScrollmapperAdapter(TranslationAdapter):
       """Read translation text from a scrollmapper-format SQLite database."""

       def get_verses(self, book_num: int) -> VerseMap:
           """
           Return all verses for book_num from the SQLite file.

           Raises FileNotFoundError if the configured path does not exist.
           """
           path = self.config["path"]
           if not Path(path).exists():
               raise FileNotFoundError(
                   f"SQLite translation file not found: {path}"
               )

           conn = sqlite3.connect(path)
           try:
               cur = conn.execute(
                   "SELECT c, v, t FROM t WHERE b = ? ORDER BY c, v",
                   (book_num,),
               )
               return {
                   (int(row[0]), int(row[1])): row[2].strip()
                   for row in cur
               }
           finally:
               conn.close()
   ```

   Note: USFMAdapter and APIAdapter will be added in Tasks 3 and 4. The import in the
   test file references them already; keep those imports in place and add stubs in the
   module so the file parses without error:

   ```python
   # Temporary stubs — replaced in Tasks 3 and 4

   class USFMAdapter(TranslationAdapter):
       """USFM adapter — implemented in Task 3."""

       def get_verses(self, book_num: int) -> VerseMap:  # noqa: ARG002
           raise NotImplementedError("USFMAdapter not yet implemented")


   class APIAdapter(TranslationAdapter):
       """API adapter — implemented in Task 4."""

       def get_verses(self, book_num: int) -> VerseMap:  # noqa: ARG002
           raise NotImplementedError("APIAdapter not yet implemented")


   _ADAPTER_MAP: dict[str, type[TranslationAdapter]] = {
       "sqlite_scrollmapper": SQLiteScrollmapperAdapter,
       "usfm": USFMAdapter,
       "api": APIAdapter,
   }


   def get_adapter(source_config: dict) -> TranslationAdapter:
       """
       Return the correct adapter instance for a source config block.

       Raises ValueError if the 'format' key is not recognized.
       """
       fmt = source_config.get("format")
       cls = _ADAPTER_MAP.get(fmt)  # type: ignore[arg-type]
       if cls is None:
           raise ValueError(
               f"Unknown translation format '{fmt}'. "
               f"Valid options: {list(_ADAPTER_MAP.keys())}"
           )
       return cls(source_config)
   ```

5. Run and confirm SQLite tests PASSED:

   ```bash
   uv run --frozen pytest tests/test_translation_adapters.py \
     -k "sqlite" -v
   # Expected: 4 passed
   ```

6. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

7. Commit: `"feat: SQLiteScrollmapperAdapter — red-green TDD complete"`

---

### Task 3: TDD — USFMAdapter

**Files:**
- `/home/user/OT-NLP/tests/test_translation_adapters.py` (USFM section added)
- `/home/user/OT-NLP/pipeline/adapters/translation_adapter.py` (USFMAdapter implemented)

**Steps:**

1. Append the USFM tests to `tests/test_translation_adapters.py`:

   ```python
   # ── USFMAdapter ────────────────────────────────────────────────

   SAMPLE_USFM = r"""
   \id PSA
   \c 1
   \p
   \v 1 Blessed is the man that walketh not in the counsel of the ungodly,
   \v 2 But his delight is in the law of the LORD;
   \c 23
   \q
   \v 1 The LORD is my shepherd; I shall not want.
   \v 2 He maketh me to lie down in green pastures:
   """


   def test_usfm_adapter_parses_psalm_23_1(tmp_path: Path) -> None:
       """USFM adapter must return correct text for Psalm 23:1."""
       usfm_dir = make_usfm_dir(tmp_path, SAMPLE_USFM)
       adapter = USFMAdapter(
           {"id": "ULT", "format": "usfm", "path": str(usfm_dir)}
       )
       verses = adapter.get_verses(19)

       assert (23, 1) in verses
       assert "shepherd" in verses[(23, 1)]


   def test_usfm_adapter_strips_footnotes(tmp_path: Path) -> None:
       """USFM adapter must remove footnote spans from verse text."""
       usfm_content = r"""
   \c 1
   \v 1 The word\f + \fr 1:1 \ft footnote text here\f* of the LORD.
   """
       usfm_dir = make_usfm_dir(tmp_path, usfm_content)
       adapter = USFMAdapter(
           {"id": "ULT", "format": "usfm", "path": str(usfm_dir)}
       )
       verses = adapter.get_verses(19)

       assert (1, 1) in verses
       assert "footnote" not in verses[(1, 1)]
       assert "word" in verses[(1, 1)]


   def test_usfm_adapter_strips_word_markers(tmp_path: Path) -> None:
       r"""USFM adapter must strip \w...\w* inline markers, keeping the word."""
       usfm_content = r"""
   \c 1
   \v 1 \w Blessed|lemma="blessed" x-morph="Adj"\w* is the man.
   """
       usfm_dir = make_usfm_dir(tmp_path, usfm_content)
       adapter = USFMAdapter(
           {"id": "ULT", "format": "usfm", "path": str(usfm_dir)}
       )
       verses = adapter.get_verses(19)

       assert (1, 1) in verses
       assert verses[(1, 1)] == "Blessed is the man."


   def test_usfm_adapter_raises_for_missing_file(tmp_path: Path) -> None:
       """USFM adapter must raise FileNotFoundError if no matching .usfm file."""
       empty_dir = tmp_path / "empty"
       empty_dir.mkdir()
       adapter = USFMAdapter(
           {"id": "ULT", "format": "usfm", "path": str(empty_dir)}
       )
       with pytest.raises(FileNotFoundError):
           adapter.get_verses(19)
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_translation_adapters.py \
     -k "usfm" -v
   # Expected: FAILED — NotImplementedError from USFMAdapter stub
   ```

3. Replace the `USFMAdapter` stub in `pipeline/adapters/translation_adapter.py` with the
   full implementation:

   ```python
   class USFMAdapter(TranslationAdapter):
       """
       Read translation text from a directory of USFM files.

       File naming convention: <two-digit-book-num><code>.usfm
       Example: 19PSA.usfm for Psalms (book 19).
       """

       # Map book_num -> two-digit USFM book number prefix (OT books 1-39)
       BOOK_CODES: dict[int, str] = {
           n: str(n).zfill(2) for n in range(1, 40)
       }

       def get_verses(self, book_num: int) -> VerseMap:
           """
           Locate the USFM file for book_num and parse all verses.

           Raises ValueError if book_num has no USFM code mapping.
           Raises FileNotFoundError if no matching .usfm file is found.
           """
           usfm_dir = Path(self.config["path"])
           code = self.BOOK_CODES.get(book_num)
           if code is None:
               raise ValueError(
                   f"No USFM book code mapping for book_num={book_num}"
               )

           candidates = list(usfm_dir.glob(f"{code}*.usfm"))
           if not candidates:
               raise FileNotFoundError(
                   f"No USFM file matching '{code}*.usfm' in {usfm_dir}"
               )

           raw = candidates[0].read_text(encoding="utf-8")
           return self._parse_usfm(raw)

       def _parse_usfm(self, text: str) -> VerseMap:
           """
           Minimal USFM parser.

           Handles: \\c <chapter>, \\v <verse> <text>
           Paragraph markers (\\p \\q \\q2 \\m) are ignored.
           Inline markers and footnotes are stripped via _strip_markers.
           """
           verses: VerseMap = {}
           chapter = 0
           verse_num = 0
           verse_parts: List[str] = []

           def flush() -> None:
               if chapter and verse_num and verse_parts:
                   joined = " ".join(verse_parts).strip()
                   cleaned = re.sub(r"\s+", " ", joined)
                   verses[(chapter, verse_num)] = cleaned

           for line in text.splitlines():
               line = line.strip()
               if not line:
                   continue

               if line.startswith(r"\c "):
                   flush()
                   verse_parts = []
                   verse_num = 0
                   chapter = int(line.split()[1])
                   continue

               if line.startswith(r"\v "):
                   flush()
                   verse_parts = []
                   parts = line.split(None, 2)
                   verse_num = int(parts[1])
                   if len(parts) > 2:
                       verse_parts.append(self._strip_markers(parts[2]))
                   continue

               # Continuation text not starting with a marker
               if chapter and verse_num and not line.startswith("\\"):
                   verse_parts.append(self._strip_markers(line))
               elif chapter and verse_num and line.startswith("\\"):
                   # Skip section/stanza markers; include tail of others
                   skip_prefixes = (
                       r"\c ", r"\v ", r"\s", r"\ms", r"\mt", r"\id",
                   )
                   if not any(line.startswith(p) for p in skip_prefixes):
                       tail = re.sub(r"^\\[a-z0-9]+\*?\s*", "", line)
                       if tail:
                           verse_parts.append(self._strip_markers(tail))

           flush()
           return verses

       @staticmethod
       def _strip_markers(text: str) -> str:
           """
           Remove USFM inline character markers and footnote/cross-ref spans.

           Strips: \\f...\\f*  \\x...\\x*  \\w word|attrs\\w*
           and remaining \\marker or \\marker* tags.
           """
           # Remove footnote and cross-reference spans
           text = re.sub(r"\\[fx].*?\\[fx]\*", "", text, flags=re.DOTALL)
           # Unwrap word-level markup: keep the word, drop attributes
           text = re.sub(r"\\w\s*(.*?)\|[^\\]*?\\w\*", r"\1", text)
           # Remove remaining inline markers
           text = re.sub(r"\\[a-z0-9]+\*?", "", text)
           return text.strip()
   ```

4. Run and confirm USFM tests PASSED:

   ```bash
   uv run --frozen pytest tests/test_translation_adapters.py \
     -k "usfm" -v
   # Expected: 4 passed
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat: USFMAdapter — red-green TDD complete"`

---

### Task 4: TDD — APIAdapter Stub + Factory Function

**Files:**
- `/home/user/OT-NLP/tests/test_translation_adapters.py` (API + factory section added)
- `/home/user/OT-NLP/pipeline/adapters/translation_adapter.py` (APIAdapter + factory)

**Steps:**

1. Append the API adapter and factory tests to `tests/test_translation_adapters.py`:

   ```python
   # ── APIAdapter ────────────────────────────────────────────────

   def test_api_adapter_raises_without_env_key() -> None:
       """API adapter must raise EnvironmentError when API key env var is unset."""
       import os
       # Ensure the env var is absent for this test
       env_key = "ESV_API_KEY_TEST_ONLY"
       os.environ.pop(env_key, None)

       adapter = APIAdapter(
           {
               "id": "ESV",
               "format": "api",
               "provider": "esv",
               "api_key_env": env_key,
           }
       )
       with pytest.raises(EnvironmentError, match=env_key):
           adapter.get_verses(19)


   # ── Factory function ──────────────────────────────────────────

   def test_factory_creates_sqlite_adapter(tmp_path: Path) -> None:
       """get_adapter must return SQLiteScrollmapperAdapter for sqlite format."""
       db_path = make_sqlite_db(tmp_path, [])
       adapter = get_adapter(
           {"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path}
       )
       assert isinstance(adapter, SQLiteScrollmapperAdapter)


   def test_factory_creates_usfm_adapter(tmp_path: Path) -> None:
       """get_adapter must return USFMAdapter for usfm format."""
       usfm_dir = make_usfm_dir(tmp_path, "")
       adapter = get_adapter(
           {"id": "ULT", "format": "usfm", "path": str(usfm_dir)}
       )
       assert isinstance(adapter, USFMAdapter)


   def test_factory_raises_for_unknown_format() -> None:
       """get_adapter must raise ValueError for an unrecognized format key."""
       with pytest.raises(ValueError, match="Unknown translation format"):
           get_adapter({"id": "X", "format": "magic_format_xyz"})


   def test_factory_adapter_id_is_set(tmp_path: Path) -> None:
       """Adapter created by factory must have the correct .id attribute."""
       db_path = make_sqlite_db(tmp_path, [])
       adapter = get_adapter(
           {"id": "YLT", "format": "sqlite_scrollmapper", "path": db_path}
       )
       assert adapter.id == "YLT"
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_translation_adapters.py \
     -k "api or factory" -v
   # Expected: FAILED — APIAdapter stub raises NotImplementedError, not EnvironmentError
   ```

3. Replace the `APIAdapter` stub in `pipeline/adapters/translation_adapter.py` with the
   real implementation:

   ```python
   class APIAdapter(TranslationAdapter):
       """
       Read translation text from a web API (ESV and similar).

       This adapter is used only during Stage 1 ingest. API keys are read
       from environment variables named in source_config['api_key_env'].
       No HTTP requests are made during unit tests (key check fails first).
       """

       # Map book_num -> human-readable name for ESV API queries
       ESV_BOOK_NAMES: dict[int, str] = {
           18: "Job",
           19: "Psalms",
           23: "Isaiah",
           25: "Lamentations",
       }

       # Chapter counts per book for ESV pagination
       _CHAPTER_COUNTS: dict[int, int] = {
           18: 42,
           19: 150,
           23: 66,
           25: 5,
       }

       def get_verses(self, book_num: int) -> VerseMap:
           """
           Fetch all verses for book_num from the configured API.

           Raises EnvironmentError if the API key environment variable is unset.
           Raises NotImplementedError for non-ESV providers.
           Raises ValueError if book_num has no API mapping.
           """
           import os
           import time

           import requests as _requests

           provider = self.config.get("provider", "esv")
           if provider != "esv":
               raise NotImplementedError(
                   f"API provider '{provider}' not implemented"
               )

           api_key_env = self.config.get("api_key_env", "ESV_API_KEY")
           api_key = os.environ.get(api_key_env)
           if not api_key:
               raise EnvironmentError(
                   f"ESV API key not set. "
                   f"Set environment variable: {api_key_env}"
               )

           book_name = self.ESV_BOOK_NAMES.get(book_num)
           if not book_name:
               raise ValueError(
                   f"No ESV book name mapping for book_num={book_num}"
               )

           num_chapters = self._CHAPTER_COUNTS.get(book_num, 50)
           rate_limit = int(self.config.get("rate_limit", 500))
           pause = 86400.0 / rate_limit

           headers = {"Authorization": f"Token {api_key}"}
           base_url = "https://api.esv.org/v3/passage/text/"
           verses: VerseMap = {}

           for ch in range(1, num_chapters + 1):
               params = {
                   "q": f"{book_name} {ch}",
                   "include-headings": "false",
                   "include-footnotes": "false",
                   "include-verse-numbers": "true",
                   "include-short-copyright": "false",
               }
               resp = _requests.get(
                   base_url, headers=headers, params=params, timeout=30
               )
               resp.raise_for_status()
               data = resp.json()

               for passage in data.get("passages", []):
                   for match in re.finditer(
                       r"\[(\d+)\]\s*(.*?)(?=\[|\Z)", passage, re.DOTALL
                   ):
                       v_num = int(match.group(1))
                       v_text = re.sub(r"\s+", " ", match.group(2)).strip()
                       if v_text:
                           verses[(ch, v_num)] = v_text

               time.sleep(pause)

           return verses
   ```

4. Run all adapter tests and confirm all 13 pass:

   ```bash
   uv run --frozen pytest tests/test_translation_adapters.py -v
   # Expected: 13 passed
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat: APIAdapter + get_adapter factory — adapter layer complete, 13 tests passing"`

---

### Task 5: TDD — ingest_translations.py Module

**Files:**
- `/home/user/OT-NLP/tests/test_ingest_translations.py`
- `/home/user/OT-NLP/pipeline/modules/__init__.py`
- `/home/user/OT-NLP/pipeline/modules/ingest_translations.py`

**Steps:**

1. Write tests in `tests/test_ingest_translations.py`:

   ```python
   """Unit tests for the ingest_translations pipeline module."""

   from __future__ import annotations

   import sqlite3
   from pathlib import Path
   from typing import Any
   from unittest.mock import MagicMock, call, patch

   import pytest

   from modules.ingest_translations import run


   # ── Helpers ────────────────────────────────────────────────────

   def make_sqlite_db(tmp_path: Path, rows: list[tuple]) -> str:
       """Create a minimal scrollmapper SQLite DB for integration-style tests."""
       db_path = str(tmp_path / "test.db")
       conn = sqlite3.connect(db_path)
       conn.execute(
           "CREATE TABLE t (b INTEGER, c INTEGER, v INTEGER, t TEXT)"
       )
       conn.executemany("INSERT INTO t VALUES (?,?,?,?)", rows)
       conn.commit()
       conn.close()
       return db_path


   def make_config(db_path: str) -> dict:
       """Return a minimal config pointing at a test SQLite file."""
       return {
           "corpus": {"books": [{"book_num": 19, "name": "Psalms"}]},
           "translations": {
               "sources": [
                   {
                       "id": "KJV",
                       "format": "sqlite_scrollmapper",
                       "path": db_path,
                   }
               ]
           },
       }


   # ── Tests ──────────────────────────────────────────────────────

   def test_run_returns_zero_when_verses_table_empty(
       tmp_path: Path, mock_conn: MagicMock
   ) -> None:
       """
       run() must return rows_written=0 when the verses table is empty.

       The translations table requires a verse_id FK; if no verses exist
       (Stage 2 has not run yet), ingest must skip gracefully.
       """
       db_path = make_sqlite_db(
           tmp_path,
           [(19, 23, 1, "The LORD is my shepherd; I shall not want.")],
       )
       config = make_config(db_path)

       # Cursor returns no rows for the verse lookup query
       cursor = mock_conn.cursor.return_value.__enter__.return_value
       cursor.fetchall.return_value = []

       result = run(mock_conn, config)

       assert result["rows_written"] == 0
       assert "elapsed_s" in result


   def test_run_inserts_correct_row_count(
       tmp_path: Path, mock_conn: MagicMock
   ) -> None:
       """
       run() must insert one row per matched (chapter, verse_num) pair.

       When verses table returns 3 matching rows, 3 translation rows
       must be written.
       """
       db_path = make_sqlite_db(
           tmp_path,
           [
               (19, 23, 1, "The LORD is my shepherd; I shall not want."),
               (19, 23, 2, "He maketh me to lie down in green pastures:"),
               (19, 23, 3, "He restoreth my soul:"),
           ],
       )
       config = make_config(db_path)

       cursor = mock_conn.cursor.return_value.__enter__.return_value
       # Simulate verses table: (chapter, verse_num, verse_id)
       cursor.fetchall.return_value = [(23, 1, 101), (23, 2, 102), (23, 3, 103)]

       with patch(
           "modules.ingest_translations.psycopg2.extras.execute_values"
       ) as mock_exec:
           result = run(mock_conn, config)

       assert result["rows_written"] == 3
       assert mock_exec.call_count == 1


   def test_run_is_idempotent(
       tmp_path: Path, mock_conn: MagicMock
   ) -> None:
       """
       run() called twice must produce the same rows_written count.

       The module uses INSERT ... ON CONFLICT DO UPDATE, so running twice
       must not raise an error and must write the same number of rows.
       """
       db_path = make_sqlite_db(
           tmp_path,
           [(19, 23, 1, "The LORD is my shepherd; I shall not want.")],
       )
       config = make_config(db_path)

       cursor = mock_conn.cursor.return_value.__enter__.return_value
       cursor.fetchall.return_value = [(23, 1, 101)]

       with patch(
           "modules.ingest_translations.psycopg2.extras.execute_values"
       ):
           result_first = run(mock_conn, config)

       # Reset mock call counts for second run
       cursor.fetchall.return_value = [(23, 1, 101)]

       with patch(
           "modules.ingest_translations.psycopg2.extras.execute_values"
       ):
           result_second = run(mock_conn, config)

       assert result_first["rows_written"] == result_second["rows_written"]
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_ingest_translations.py -v
   # Expected: FAILED — ImportError: No module named 'modules.ingest_translations'
   ```

3. Create `pipeline/modules/__init__.py`:

   ```python
   # modules package
   ```

4. Create `pipeline/modules/ingest_translations.py` at
   `/home/user/OT-NLP/pipeline/modules/ingest_translations.py`:

   ```python
   """
   Stage 1 — Translation ingest module.

   Reads all translation sources configured in config.yml, resolves
   verse_id values from the 'verses' table, and bulk-upserts verse text
   into the 'translations' table.

   Dependency note:
       The 'translations' table references 'verses' via a foreign key.
       If 'verses' is empty (Stage 2 has not run yet), this module logs
       a warning per book and returns rows_written=0.
       Re-run after Stage 2 to complete the ingest.

   Module interface:
       run(conn, config) -> {"rows_written": int, "elapsed_s": float}
   """

   from __future__ import annotations

   import logging
   import time
   from typing import Any

   import psycopg2
   import psycopg2.extras

   from adapters.translation_adapter import get_adapter

   logger = logging.getLogger(__name__)


   def run(
       conn: psycopg2.extensions.connection, config: dict
   ) -> dict[str, Any]:
       """
       Ingest all configured translations into the translations table.

       Args:
           conn:   Live psycopg2 connection to the psalms database.
           config: Full parsed config.yml dictionary.

       Returns:
           A dict with keys 'rows_written' (int) and 'elapsed_s' (float).
       """
       t0 = time.monotonic()
       sources = config.get("translations", {}).get("sources", [])
       corpus_books = [
           b["book_num"]
           for b in config.get("corpus", {}).get("books", [])
       ]

       if not sources:
           logger.warning("No translation sources configured. Skipping.")
           return {"rows_written": 0, "elapsed_s": time.monotonic() - t0}

       total_rows = 0

       for source in sources:
           t_id = source["id"]
           logger.info("Ingesting translation: %s", t_id)

           try:
               adapter = get_adapter(source)
           except ValueError:
               logger.exception(
                   "Failed to create adapter for translation '%s'", t_id
               )
               raise

           for book_num in corpus_books:
               try:
                   verse_map = adapter.get_verses(book_num)
               except (FileNotFoundError, OSError):
                   logger.exception(
                       "Failed to read translation '%s' book %d",
                       t_id,
                       book_num,
                   )
                   raise

               rows = _upsert_translation_verses(
                   conn, t_id, book_num, verse_map
               )
               total_rows += rows
               logger.info(
                   "  %s book %d: %d verses ingested", t_id, book_num, rows
               )

       elapsed = time.monotonic() - t0
       logger.info(
           "Translation ingest complete: %d rows in %.2fs",
           total_rows,
           elapsed,
       )
       return {"rows_written": total_rows, "elapsed_s": elapsed}


   def _upsert_translation_verses(
       conn: psycopg2.extensions.connection,
       translation_key: str,
       book_num: int,
       verse_map: dict[tuple[int, int], str],
   ) -> int:
       """
       Resolve verse_id lookups and upsert translation rows.

       If the verses table contains no rows for book_num, logs a warning
       and returns 0 without inserting anything. This is expected when
       Stage 2 has not yet run.

       Returns the number of rows written.
       """
       with conn.cursor() as cur:
           cur.execute(
               "SELECT chapter, verse_num, verse_id "
               "FROM verses WHERE book_num = %s",
               (book_num,),
           )
           verse_lookup: dict[tuple[int, int], int] = {
               (int(row[0]), int(row[1])): int(row[2])
               for row in cur.fetchall()
           }

       if not verse_lookup:
           logger.warning(
               "No verses found for book_num=%d — "
               "run Stage 2 ingest first, then re-run translation ingest.",
               book_num,
           )
           return 0

       rows_to_insert: list[tuple[int, str, str]] = []
       for (chapter, verse_num), text in verse_map.items():
           verse_id = verse_lookup.get((chapter, verse_num))
           if verse_id is None:
               logger.debug(
                   "  Skipping %s %d:%d:%d — no verse record",
                   translation_key,
                   book_num,
                   chapter,
                   verse_num,
               )
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

5. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_ingest_translations.py -v
   # Expected: 3 passed
   ```

6. Run the full test suite to confirm nothing is broken:

   ```bash
   uv run --frozen pytest -v
   # Expected: all tests pass
   ```

7. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

8. Commit: `"feat: ingest_translations module — red-green TDD complete, 3 tests passing"`

---

### Task 6: Data Download (Shell Commands — Not TDD)

**Note:** These commands are run manually on the host machine. They require internet access.
Data files are git-ignored; this task produces no committed code.

**Steps:**

1. Create the translations data directory on the host:

   ```bash
   mkdir -p /home/user/OT-NLP/data/translations
   ```

2. Download scrollmapper SQLite translation files:

   ```bash
   cd /home/user/OT-NLP/data/translations

   for NAME in t_kjv t_ylt t_web t_asv t_dby; do
     curl -L \
       "https://github.com/scrollmapper/bible_databases/raw/master/sqlite/${NAME}.db" \
       -o "${NAME}.db"
     echo "Downloaded ${NAME}.db"
   done
   ```

3. Clone unfoldingWord USFM translation repositories:

   ```bash
   cd /home/user/OT-NLP/data/translations

   git clone --depth 1 https://github.com/unfoldingWord/en_ult ult
   git clone --depth 1 https://github.com/unfoldingWord/en_ust ust
   ```

4. Verify the expected files are present:

   ```bash
   ls /home/user/OT-NLP/data/translations/
   # Expected: t_kjv.db  t_ylt.db  t_web.db  t_asv.db  t_dby.db  ult/  ust/

   ls /home/user/OT-NLP/data/translations/ult/19PSA.usfm
   # Expected: file exists

   ls /home/user/OT-NLP/data/translations/ust/19PSA.usfm
   # Expected: file exists
   ```

5. Download BHSA Hebrew Bible dataset via text-fabric inside the pipeline container:

   ```bash
   docker compose --profile pipeline run --rm pipeline python -c "
   from tf.app import use
   A = use('ETCBC/bhsa', hoist=globals(), checkout='clone')
   "
   # Downloads ~200 MB to /data/bhsa/ (mapped from host data/bhsa/)
   ```

6. Verify Psalms corpus is present:

   ```bash
   docker compose --profile pipeline run --rm pipeline python -c "
   from tf.fabric import Fabric
   TF = Fabric(
     locations=['/data/bhsa/github/ETCBC/bhsa/tf/c'], silent=True
   )
   api = TF.load('book chapter verse g_word_utf8 lex sp')
   psalms = [v for v in api.F.verse.s() if api.T.bookName(v) == 'Psalms']
   print(f'Psalms verses: {len(psalms)}')
   "
   # Expected: Psalms verses: 2527
   ```

---

### Task 7: Integration Verification

**Note:** This task requires Stage 2 to have run first. The commands below verify the
complete Stage 1 state after `ingest_translations` is run against a populated `verses`
table.

**Steps:**

1. After Stage 2 completes, run translation ingest inside the pipeline container:

   ```bash
   docker compose --profile pipeline run --rm pipeline python -c "
   import sys, yaml, psycopg2, logging
   sys.path.insert(0, '/pipeline')
   logging.basicConfig(level=logging.INFO)

   with open('/pipeline/config.yml') as f:
       config = yaml.safe_load(f)

   conn = psycopg2.connect(
       host='db',
       dbname='psalms',
       user='psalms',
       password='psalms_dev',
   )

   from modules.ingest_translations import run
   result = run(conn, config)
   print('Result:', result)
   conn.close()
   "
   # Expected: rows_written > 0 for each translation
   ```

2. Verify translation row counts:

   ```bash
   docker exec psalms_db psql -U psalms -d psalms -c "
   SELECT translation_key, COUNT(*) AS verse_count
   FROM translations
   GROUP BY translation_key
   ORDER BY translation_key;
   "
   # Expected: 5 rows (KJV, YLT, WEB, ULT, UST), each ~2527 verses
   ```

3. Spot-check the primary fixture verse — Psalm 23:1:

   ```bash
   docker exec psalms_db psql -U psalms -d psalms -c "
   SELECT t.translation_key, t.verse_text
   FROM translations t
   JOIN verses v ON t.verse_id = v.verse_id
   WHERE v.book_num = 19 AND v.chapter = 23 AND v.verse_num = 1
   ORDER BY t.translation_key;
   "
   # Expected KJV: The LORD is my shepherd; I shall not want.
   # Hebrew reference: יְהוָ֥ה רֹ֝עִ֗י לֹ֣א אֶחְסָֽר
   # (Book=19, Chapter=23, Verse=1)
   ```

4. Run the full pytest suite one final time to confirm no regressions:

   ```bash
   uv run --frozen pytest -v
   # Expected: all tests pass
   ```

5. Commit: `"feat: Stage 1 complete — adapters implemented, ingest module verified"`

---

## Resumability Verification

Running `ingest_translations` a second time must produce the same `rows_written` count and
must not raise an error. The `ON CONFLICT (verse_id, translation_key) DO UPDATE` clause in
`_upsert_translation_verses` guarantees this:

```bash
# First run (after Stage 2)
docker compose --profile pipeline run --rm pipeline python -c "
import sys, yaml, psycopg2
sys.path.insert(0, '/pipeline')
with open('/pipeline/config.yml') as f:
    config = yaml.safe_load(f)
conn = psycopg2.connect(host='db', dbname='psalms',
                        user='psalms', password='psalms_dev')
from modules.ingest_translations import run
r1 = run(conn, config)
print('First run:', r1['rows_written'])
conn.close()
"

# Second run
docker compose --profile pipeline run --rm pipeline python -c "
import sys, yaml, psycopg2
sys.path.insert(0, '/pipeline')
with open('/pipeline/config.yml') as f:
    config = yaml.safe_load(f)
conn = psycopg2.connect(host='db', dbname='psalms',
                        user='psalms', password='psalms_dev')
from modules.ingest_translations import run
r2 = run(conn, config)
print('Second run:', r2['rows_written'])
conn.close()
"
# Expected: both runs print the same rows_written value

# Confirm total row count is stable
docker exec psalms_db psql -U psalms -d psalms \
  -c "SELECT COUNT(*) FROM translations;"
```

The idempotency test (`test_run_is_idempotent`) in `tests/test_ingest_translations.py`
also covers this at the unit-test level.
