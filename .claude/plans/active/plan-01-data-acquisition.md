# Plan: Stage 1 — Data Acquisition & Translation Adapters

> **Depends on:** Plan 00 (Stage 0) — running Docker stack with healthy `psalms_db`
> container and all 11 tables initialized via `init_schema.sql`.
> **Status:** active

## Goal

Download all translation source files, implement a fully-tested adapter layer that
normalizes each format into a uniform verse map, wire up the ingest module that writes
translation text into PostgreSQL, and produce a post-ingest validation script that
confirms each configured translation returns correct text for known verses.

## Acceptance Criteria

- Each configured translation (`KJV`, `YLT`, `WEB`, `ULT`, `UST`) returns correct text
  for Psalm 23:1 when queried through its adapter
- All 10 adapter unit tests pass
- `pipeline/adapters/translation_adapter.py` is importable with full type annotations
- `pipeline/modules/ingest_translations.py` exports `run(conn, config) -> dict`
- After Stage 2 populates `verses`: all 5 translations have rows in `translations` table
  for all 2,527 Psalms verses (verified by `validate_data.py` with 0 failures)

## Architecture

Translation source files (SQLite `.db` and USFM directories) are treated as read-once
inputs during Stage 1 ingest. An adapter class per format normalizes each source into a
`dict[tuple[int, int], str]` mapping of `{(chapter, verse_num): verse_text}`. A factory
function selects the correct adapter class from the source's `format` field in
`config.yml`. The `ingest_translations` module calls each adapter, resolves `verse_id`
foreign keys by querying the `verses` table, and bulk-upserts rows into `translations`
via `psycopg2.extras.execute_values`. Because `translations` has a foreign key to
`verses`, and `verses` is populated in Stage 2, a full ingest cannot complete until after
Stage 2 runs. The module detects an empty `verses` table, logs a warning, and returns
gracefully with zero rows written. All adapter logic is therefore fully testable without
any database connection.

## Tech Stack

- `psycopg2` (not psycopg3) for PostgreSQL interaction
- `psycopg2.extras.execute_values` for batch inserts
- `sqlite3` (stdlib) for scrollmapper SQLite files
- `pathlib.Path` for USFM file discovery
- `re` for USFM inline marker stripping
- `pyyaml` for config parsing
- `pytest` with in-memory SQLite and inline USFM fixture strings (no file I/O in tests)

---

## Tasks

### Task 1: Write all 10 adapter tests (test-first, all must fail initially)

**Files:**
- `tests/test_translation_adapters.py`

**Steps:**

1. Write all 10 tests now. Every test must fail because the adapter module does not
   exist yet. This is the TDD red phase.

   ```python
   # tests/test_translation_adapters.py
   """
   Unit tests for translation adapters.

   All tests run without a database or filesystem — SQLite tests use an
   in-memory database created inline; USFM tests use a small inline fixture
   string written to a tmp_path directory.

   Run with:
       uv run --frozen pytest tests/test_translation_adapters.py -v
   """
   from __future__ import annotations

   import sqlite3
   from pathlib import Path

   import pytest

   from adapters.translation_adapter import (
       APIAdapter,
       SQLiteScrollmapperAdapter,
       TranslationAdapter,
       USFMAdapter,
       adapter_factory,
   )

   # ── Helpers ───────────────────────────────────────────────────


   def _make_sqlite_db(tmp_path: Path, rows: list[tuple]) -> str:
       """Create a scrollmapper-schema SQLite file at tmp_path/test.db."""
       db_path = str(tmp_path / "test.db")
       conn = sqlite3.connect(db_path)
       conn.execute("CREATE TABLE t (b INTEGER, c INTEGER, v INTEGER, t TEXT)")
       conn.executemany("INSERT INTO t VALUES (?,?,?,?)", rows)
       conn.commit()
       conn.close()
       return db_path


   def _make_usfm_dir(tmp_path: Path, content: str) -> Path:
       """Write content into tmp_path/ult/19PSA.usfm and return the dir path."""
       usfm_dir = tmp_path / "ult"
       usfm_dir.mkdir()
       (usfm_dir / "19PSA.usfm").write_text(content, encoding="utf-8")
       return usfm_dir


   # ── USFM fixture ──────────────────────────────────────────────

   PSALM_23_USFM = r"""\id PSA
   \c 23
   \q
   \v 1 The \w LORD|lemma="LORD" x-morph="Nh"\w* is my shepherd; I shall not want.
   \v 2 He maketh me to lie down in green pastures.
   \c 24
   \q
   \v 1 The earth is the LORD's, and the fulness thereof.
   """

   # ── Test 1: SQLiteScrollmapperAdapter returns KJV Ps 23:1 ─────


   def test_sqlite_adapter_psalm_23_1(tmp_path: Path) -> None:
       """SQLiteScrollmapperAdapter must return correct KJV text for Psalm 23:1."""
       db_path = _make_sqlite_db(tmp_path, [
           (19, 23, 1, "The LORD is my shepherd; I shall not want."),
           (19, 23, 2, "He maketh me to lie down in green pastures."),
       ])
       adapter = SQLiteScrollmapperAdapter(
           {"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path}
       )
       verses = adapter.get_verse(19, 23, 1)
       assert verses == "The LORD is my shepherd; I shall not want."


   # ── Test 2: SQLiteScrollmapperAdapter returns None for missing verse ──


   def test_sqlite_adapter_missing_verse(tmp_path: Path) -> None:
       """SQLiteScrollmapperAdapter must return None for a verse that doesn't exist."""
       db_path = _make_sqlite_db(tmp_path, [
           (19, 23, 1, "The LORD is my shepherd; I shall not want."),
       ])
       adapter = SQLiteScrollmapperAdapter(
           {"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path}
       )
       result = adapter.get_verse(19, 23, 99)
       assert result is None


   # ── Test 3: SQLiteScrollmapperAdapter filters by book ────────


   def test_sqlite_adapter_book_filter(tmp_path: Path) -> None:
       """SQLiteScrollmapperAdapter.get_verse(19, ...) must not return Genesis rows."""
       db_path = _make_sqlite_db(tmp_path, [
           (19, 23, 1, "The LORD is my shepherd"),
           (1,   1, 1, "In the beginning God created"),
       ])
       adapter = SQLiteScrollmapperAdapter(
           {"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path}
       )
       # Requesting Psalms book=19, chapter=1, verse=1 must not return Genesis
       result = adapter.get_verse(1, 1, 1)
       # This is book 1 (Genesis) — should still work; confirm isolation
       assert result == "In the beginning God created"
       psalms_result = adapter.get_verse(19, 23, 1)
       assert psalms_result == "The LORD is my shepherd"


   # ── Test 4: USFMAdapter returns Psalm 23:1 ───────────────────


   def test_usfm_adapter_psalm_23_1(tmp_path: Path) -> None:
       """USFMAdapter must return the cleaned text for Psalm 23:1."""
       usfm_dir = _make_usfm_dir(tmp_path, PSALM_23_USFM)
       adapter = USFMAdapter(
           {"id": "ULT", "format": "usfm", "path": str(usfm_dir)}
       )
       result = adapter.get_verse(19, 23, 1)
       assert result is not None
       # After stripping \w markup, "LORD" must remain
       assert "LORD" in result
       assert "shepherd" in result


   # ── Test 5: USFMAdapter strips inline markup ──────────────────


   def test_usfm_adapter_strips_markup(tmp_path: Path) -> None:
       """USFMAdapter must strip \\w, \\f, \\x markers, leaving plain text."""
       usfm_with_markup = r"""\c 1
   \v 1 \w Blessed|lemma="blessed" x-morph="Adj"\w* is the man\f + \fr 1:1 \ft note\f*.
   """
       usfm_dir = _make_usfm_dir(tmp_path, usfm_with_markup)
       adapter = USFMAdapter(
           {"id": "ULT", "format": "usfm", "path": str(usfm_dir)}
       )
       result = adapter.get_verse(19, 1, 1)
       assert result is not None
       assert "Blessed" in result
       assert "lemma" not in result
       assert "x-morph" not in result
       assert "note" not in result
       assert r"\w" not in result
       assert r"\f" not in result


   # ── Test 6: USFMAdapter returns None for missing verse ────────


   def test_usfm_adapter_missing_verse(tmp_path: Path) -> None:
       """USFMAdapter must return None when the verse does not exist in the file."""
       usfm_dir = _make_usfm_dir(tmp_path, PSALM_23_USFM)
       adapter = USFMAdapter(
           {"id": "ULT", "format": "usfm", "path": str(usfm_dir)}
       )
       result = adapter.get_verse(19, 23, 999)
       assert result is None


   # ── Test 7: APIAdapter stub returns None ─────────────────────


   def test_api_adapter_stub_returns_none() -> None:
       """APIAdapter.get_verse must return None (not implemented / stub)."""
       adapter = APIAdapter(
           {"id": "ESV", "format": "api", "provider": "esv"}
       )
       result = adapter.get_verse(19, 23, 1)
       assert result is None


   # ── Test 8: adapter_factory returns SQLiteScrollmapperAdapter ─


   def test_adapter_factory_sqlite(tmp_path: Path) -> None:
       """adapter_factory must return SQLiteScrollmapperAdapter for sqlite_scrollmapper."""
       db_path = _make_sqlite_db(tmp_path, [])
       adapter = adapter_factory(
           {"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path}
       )
       assert isinstance(adapter, SQLiteScrollmapperAdapter)


   # ── Test 9: adapter_factory returns USFMAdapter ───────────────


   def test_adapter_factory_usfm(tmp_path: Path) -> None:
       """adapter_factory must return USFMAdapter for usfm format."""
       adapter = adapter_factory(
           {"id": "ULT", "format": "usfm", "path": "/some/path"}
       )
       assert isinstance(adapter, USFMAdapter)


   # ── Test 10: adapter_factory raises ValueError for unknown format ──


   def test_adapter_factory_unknown_format_raises() -> None:
       """adapter_factory must raise ValueError for an unrecognized format string."""
       with pytest.raises(ValueError, match="Unknown translation format"):
           adapter_factory({"id": "X", "format": "magic_format"})
   ```

2. Run and confirm ALL 10 FAILED (ImportError because module does not exist):

   ```bash
   uv run --frozen pytest tests/test_translation_adapters.py -v
   # Expected: ERROR — cannot import 'adapters.translation_adapter'
   # All 10 tests collected, all fail with ImportError
   ```

3. No implementation yet. Confirm the failure, then proceed to Task 2.

4. Lint + typecheck (the test file itself must be clean):

   ```bash
   uv run --frozen ruff check tests/test_translation_adapters.py --fix
   uv run --frozen pyright tests/test_translation_adapters.py
   ```

5. Commit: `"test: add all 10 adapter tests (red — no implementation yet)"`

---

### Task 2: Package init files

**Files:**
- `pipeline/adapters/__init__.py`
- `pipeline/modules/__init__.py`

**Steps:**

1. Write test in `tests/test_package_structure.py`:

   ```python
   # tests/test_package_structure.py
   """Verify that pipeline package directories exist and are importable."""
   from pathlib import Path


   def test_adapters_init_exists() -> None:
       """pipeline/adapters/__init__.py must exist."""
       assert (
           Path(__file__).parent.parent / "pipeline" / "adapters" / "__init__.py"
       ).exists()


   def test_modules_init_exists() -> None:
       """pipeline/modules/__init__.py must exist."""
       assert (
           Path(__file__).parent.parent / "pipeline" / "modules" / "__init__.py"
       ).exists()


   def test_adapters_importable() -> None:
       """pipeline/adapters must be importable as a package."""
       import adapters  # noqa: F401


   def test_modules_importable() -> None:
       """pipeline/modules must be importable as a package."""
       import modules  # noqa: F401
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_package_structure.py -v
   # Expected: FAILED — __init__.py files do not exist yet
   ```

3. Implement both `__init__.py` files:

   `pipeline/adapters/__init__.py`:

   ```python
   # pipeline/adapters/__init__.py
   """Translation adapter package."""
   ```

   `pipeline/modules/__init__.py`:

   ```python
   # pipeline/modules/__init__.py
   """Pipeline stage modules package."""
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_package_structure.py -v
   # Expected: PASSED (4 tests)
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"scaffold: add adapters/ and modules/ package init files"`

---

### Task 3: Implement translation_adapter.py

**Files:**
- `pipeline/adapters/translation_adapter.py`

**Steps:**

1. Confirm the 10 adapter tests still fail (ImportError should now become
   AttributeError since `__init__.py` exists but the module does not):

   ```bash
   uv run --frozen pytest tests/test_translation_adapters.py -v
   # Expected: FAILED — 'adapters' has no attribute 'translation_adapter'
   ```

2. Implement `pipeline/adapters/translation_adapter.py`:

   ```python
   # pipeline/adapters/translation_adapter.py
   """
   Translation adapter layer.

   Each adapter implements a single interface method:
       get_verse(book_num, chapter, verse) -> str | None

   This module is used during Stage 1 ingest only. After ingest completes,
   all translation text is read exclusively from PostgreSQL.

   Adapter classes:
       SQLiteScrollmapperAdapter  — scrollmapper SQLite format (.db files)
       USFMAdapter                — USFM directory format (unfoldingWord / eBible)
       APIAdapter                 — placeholder; returns None (not yet implemented)

   Factory function:
       adapter_factory(source_config)  — returns the correct adapter instance
   """

   from __future__ import annotations

   import re
   import sqlite3
   from abc import ABC, abstractmethod
   from pathlib import Path
   from typing import Optional


   # ─────────────────────────────────────────────────────────────────
   # Type alias
   # ─────────────────────────────────────────────────────────────────

   # Internal cache: {(chapter, verse_num): verse_text}
   _VerseCache = dict[tuple[int, int], str]


   # ─────────────────────────────────────────────────────────────────
   # Abstract base
   # ─────────────────────────────────────────────────────────────────


   class TranslationAdapter(ABC):
       """Base class for all translation source adapters."""

       def __init__(self, source_config: dict) -> None:
           """
           Initialize the adapter with a source config block from config.yml.

           Args:
               source_config: Dict with at minimum 'id', 'format', 'path' keys.
           """
           self.id: str = source_config["id"]
           self.config: dict = source_config

       @abstractmethod
       def get_verse(self, book_num: int, chapter: int, verse: int) -> Optional[str]:
           """
           Return the verse text for the given coordinates, or None if not found.

           Args:
               book_num: BHSA book number (e.g. 19 for Psalms).
               chapter:  Chapter number (1-based).
               verse:    Verse number (1-based).

           Returns:
               Verse text string, or None if the verse does not exist in this source.
           """
           ...


   # ─────────────────────────────────────────────────────────────────
   # SQLite adapter — scrollmapper format
   # Schema: CREATE TABLE t (b INTEGER, c INTEGER, v INTEGER, t TEXT)
   # where b = book_num (19 = Psalms), c = chapter, v = verse, t = text
   # ─────────────────────────────────────────────────────────────────


   class SQLiteScrollmapperAdapter(TranslationAdapter):
       """
       Adapter for scrollmapper-format SQLite Bible databases.

       Downloads available at:
       https://github.com/scrollmapper/bible_databases/tree/master/sqlite
       """

       def get_verse(self, book_num: int, chapter: int, verse: int) -> Optional[str]:
           """Return the verse text from the SQLite file, or None if not found."""
           path = Path(self.config["path"])
           if not path.exists():
               raise FileNotFoundError(
                   f"SQLite translation file not found: {path}"
               )
           conn = sqlite3.connect(str(path))
           try:
               cur = conn.execute(
                   "SELECT t FROM t WHERE b = ? AND c = ? AND v = ?",
                   (book_num, chapter, verse),
               )
               row = cur.fetchone()
               if row is None:
                   return None
               return str(row[0]).strip()
           finally:
               conn.close()


   # ─────────────────────────────────────────────────────────────────
   # USFM adapter — unfoldingWord / eBible format
   # Reads a directory of .usfm files; locates the book file by the
   # two-digit book number prefix (e.g. 19PSA.usfm for Psalms).
   # ─────────────────────────────────────────────────────────────────

   # USFM book codes: book_num -> two-digit string prefix
   _USFM_BOOK_CODES: dict[int, str] = {
       num: f"{num:02d}" for num in range(1, 40)
   }


   class USFMAdapter(TranslationAdapter):
       """
       Adapter for USFM-format Bible directories (unfoldingWord ULT/UST).

       Parses USFM files line by line. Handles:
         \\c <chapter>     — chapter marker
         \\v <verse> <text> — verse marker with inline text
         \\p \\q \\q2 \\m  — paragraph markers (ignored)
         \\w...\\w*        — word-level markup (stripped, word retained)
         \\f...\\f*        — footnote spans (stripped entirely)
         \\x...\\x*        — cross-reference spans (stripped entirely)
       """

       # Per-instance cache: populated on first access per book file
       _cache: dict[str, _VerseCache]

       def __init__(self, source_config: dict) -> None:
           super().__init__(source_config)
           self._cache = {}

       def get_verse(self, book_num: int, chapter: int, verse: int) -> Optional[str]:
           """Return the verse text from the USFM directory, or None if not found."""
           cache_key = str(book_num)
           if cache_key not in self._cache:
               self._cache[cache_key] = self._load_book(book_num)
           return self._cache[cache_key].get((chapter, verse))

       def _load_book(self, book_num: int) -> _VerseCache:
           """Locate and parse the USFM file for book_num. Returns full verse map."""
           usfm_dir = Path(self.config["path"])
           code = _USFM_BOOK_CODES.get(book_num)
           if code is None:
               raise ValueError(
                   f"No USFM book code for book_num={book_num}"
               )
           candidates = list(usfm_dir.glob(f"{code}*.usfm"))
           if not candidates:
               raise FileNotFoundError(
                   f"No USFM file matching '{code}*.usfm' in {usfm_dir}"
               )
           text = candidates[0].read_text(encoding="utf-8")
           return _parse_usfm(text)


   def _parse_usfm(text: str) -> _VerseCache:
       """
       Parse a USFM string into a verse map.

       Args:
           text: Full USFM file content as a string.

       Returns:
           Dict mapping (chapter, verse_num) -> cleaned verse text.
       """
       verses: _VerseCache = {}
       chapter = 0
       verse_num = 0
       parts: list[str] = []

       def _flush() -> None:
           if chapter and verse_num and parts:
               raw = " ".join(parts).strip()
               verses[(chapter, verse_num)] = re.sub(r"\s+", " ", raw)

       for line in text.splitlines():
           line = line.strip()
           if not line:
               continue

           if line.startswith(r"\c "):
               _flush()
               parts = []
               verse_num = 0
               try:
                   chapter = int(line.split()[1])
               except (IndexError, ValueError):
                   pass
               continue

           if line.startswith(r"\v "):
               _flush()
               parts = []
               tokens = line.split(None, 2)
               try:
                   verse_num = int(tokens[1])
               except (IndexError, ValueError):
                   verse_num = 0
               if len(tokens) > 2:
                   parts.append(_strip_usfm_inline(tokens[2]))
               continue

           # Continuation text within the current verse
           if chapter and verse_num:
               # Skip structural markers that start new blocks
               if line.startswith(r"\c ") or line.startswith(r"\v "):
                   continue
               # Skip section headings (\s, \ms, \mt)
               if re.match(r"\\(s|ms|mt)\d?\s", line):
                   continue
               # Paragraph markers (\p, \q, \q2, \m, etc.) — take any trailing text
               if re.match(r"\\[pqmb]\d?\s*", line):
                   tail = re.sub(r"^\\[pqmb]\d?\s*", "", line)
                   if tail:
                       parts.append(_strip_usfm_inline(tail))
               elif not line.startswith("\\"):
                   parts.append(_strip_usfm_inline(line))

       _flush()
       return verses


   def _strip_usfm_inline(text: str) -> str:
       """
       Remove USFM inline character markers from a text fragment.

       Removes:
         - Footnote spans:        \\f ... \\f*
         - Cross-ref spans:       \\x ... \\x*
         - Word markup:           \\w word|attrs\\w*  -> word
         - Other inline markers:  \\nd\\* \\add\\* etc.
       """
       # Remove footnote and cross-reference spans (may be multi-token)
       text = re.sub(r"\\[fx]\s.*?\\[fx]\*", "", text)
       # Strip word markup: keep the word, discard the attribute block
       text = re.sub(r"\\w\s+(.*?)\|[^\\]*?\\w\*", r"\1", text)
       # Remove remaining inline markers like \nd \add \bk etc.
       text = re.sub(r"\\[a-zA-Z0-9]+\*?", "", text)
       return text.strip()


   # ─────────────────────────────────────────────────────────────────
   # API adapter — stub (not yet implemented)
   # ─────────────────────────────────────────────────────────────────


   class APIAdapter(TranslationAdapter):
       """
       Stub adapter for API-based translations (e.g. ESV API).

       Not yet implemented. Returns None for all verse lookups.
       Full implementation planned for a future stage if API access is configured.
       """

       def get_verse(self, book_num: int, chapter: int, verse: int) -> Optional[str]:
           """Return None — API adapter is not yet implemented."""
           return None


   # ─────────────────────────────────────────────────────────────────
   # Factory
   # ─────────────────────────────────────────────────────────────────

   _ADAPTER_MAP: dict[str, type[TranslationAdapter]] = {
       "sqlite_scrollmapper": SQLiteScrollmapperAdapter,
       "usfm": USFMAdapter,
       "api": APIAdapter,
   }


   def adapter_factory(source_config: dict) -> TranslationAdapter:
       """
       Return the correct adapter instance for a source config block.

       Args:
           source_config: Dict with at minimum 'id' and 'format' keys.
                          'format' must be one of: sqlite_scrollmapper, usfm, api.

       Returns:
           Configured TranslationAdapter instance.

       Raises:
           ValueError: If 'format' is not a recognized adapter type.
       """
       fmt: str = source_config.get("format", "")
       cls = _ADAPTER_MAP.get(fmt)
       if cls is None:
           raise ValueError(
               f"Unknown translation format '{fmt}'. "
               f"Valid options: {sorted(_ADAPTER_MAP.keys())}"
           )
       return cls(source_config)
   ```

3. Run and confirm all 10 tests PASS:

   ```bash
   uv run --frozen pytest tests/test_translation_adapters.py -v
   # Expected: PASSED (10 tests)
   # test_sqlite_adapter_psalm_23_1               PASSED
   # test_sqlite_adapter_missing_verse            PASSED
   # test_sqlite_adapter_book_filter              PASSED
   # test_usfm_adapter_psalm_23_1                 PASSED
   # test_usfm_adapter_strips_markup              PASSED
   # test_usfm_adapter_missing_verse              PASSED
   # test_api_adapter_stub_returns_none           PASSED
   # test_adapter_factory_sqlite                  PASSED
   # test_adapter_factory_usfm                    PASSED
   # test_adapter_factory_unknown_format_raises   PASSED
   ```

4. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

5. Commit: `"feat: implement translation_adapter.py — SQLite, USFM, API, factory"`

---

### Task 4: Implement ingest_translations.py module

**Files:**
- `pipeline/modules/ingest_translations.py`

**Steps:**

1. Write test in `tests/test_ingest_translations.py`:

   ```python
   # tests/test_ingest_translations.py
   """
   Unit tests for the ingest_translations module.

   Uses an in-memory PostgreSQL mock via a simple dict-based stub so no live
   database is required. The key behaviors tested are:
     - run() returns a dict with 'rows_written' and 'elapsed_s'
     - run() writes 0 rows when verses table is empty (pre-Stage-2 case)
     - run() skips sources with unknown formats gracefully (raises ValueError)
   """
   from __future__ import annotations

   import sqlite3
   from pathlib import Path
   from typing import Any
   from unittest.mock import MagicMock, patch

   import pytest

   from modules.ingest_translations import run


   # ── Helpers ───────────────────────────────────────────────────


   def _make_sqlite_db(tmp_path: Path, rows: list[tuple]) -> str:
       db_path = str(tmp_path / "kjv.db")
       conn = sqlite3.connect(db_path)
       conn.execute("CREATE TABLE t (b INTEGER, c INTEGER, v INTEGER, t TEXT)")
       conn.executemany("INSERT INTO t VALUES (?,?,?,?)", rows)
       conn.commit()
       conn.close()
       return db_path


   def _make_config(db_path: str) -> dict:
       return {
           "corpus": {"books": [{"book_num": 19}]},
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


   # ── Tests ─────────────────────────────────────────────────────


   def test_run_returns_required_keys(tmp_path: Path) -> None:
       """run() must return a dict with at least 'rows_written' and 'elapsed_s'."""
       db_path = _make_sqlite_db(tmp_path, [
           (19, 23, 1, "The LORD is my shepherd"),
       ])
       config = _make_config(db_path)

       # Mock a connection that returns an empty verses table
       mock_conn = MagicMock()
       mock_cursor = MagicMock()
       mock_cursor.fetchall.return_value = []  # verses table empty
       mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
       mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

       result = run(mock_conn, config)
       assert "rows_written" in result
       assert "elapsed_s" in result


   def test_run_writes_zero_rows_when_verses_empty(tmp_path: Path) -> None:
       """run() must write 0 rows and not crash when verses table is empty."""
       db_path = _make_sqlite_db(tmp_path, [
           (19, 23, 1, "The LORD is my shepherd"),
       ])
       config = _make_config(db_path)

       mock_conn = MagicMock()
       mock_cursor = MagicMock()
       mock_cursor.fetchall.return_value = []
       mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
       mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

       result = run(mock_conn, config)
       assert result["rows_written"] == 0


   def test_run_no_sources_returns_zero(tmp_path: Path) -> None:
       """run() with no configured sources must return rows_written=0."""
       config: dict[str, Any] = {
           "corpus": {"books": [{"book_num": 19}]},
           "translations": {"sources": []},
       }
       mock_conn = MagicMock()
       result = run(mock_conn, config)
       assert result["rows_written"] == 0


   def test_run_raises_on_unknown_format(tmp_path: Path) -> None:
       """run() must raise ValueError if a source uses an unknown format."""
       config: dict[str, Any] = {
           "corpus": {"books": [{"book_num": 19}]},
           "translations": {
               "sources": [{"id": "X", "format": "magic_format", "path": "/x"}]
           },
       }
       mock_conn = MagicMock()
       with pytest.raises(ValueError, match="Unknown translation format"):
           run(mock_conn, config)
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_ingest_translations.py -v
   # Expected: FAILED — modules.ingest_translations does not exist
   ```

3. Implement `pipeline/modules/ingest_translations.py`:

   ```python
   # pipeline/modules/ingest_translations.py
   """
   Stage 1 — Translation ingest.

   Reads all translation sources configured in config.yml via their adapters,
   then upserts verse text into the `translations` PostgreSQL table.

   Entry point:
       run(conn, config) -> {"rows_written": int, "elapsed_s": float}

   Sequencing note:
       This module writes to `translations`, which has a foreign key on `verses`.
       If `verses` is empty (pre-Stage-2), the module logs a warning and returns
       rows_written=0 without error. Re-run after Stage 2 populates `verses`.
   """

   from __future__ import annotations

   import logging
   import time

   import psycopg2.extensions
   import psycopg2.extras

   from adapters.translation_adapter import adapter_factory

   logger = logging.getLogger(__name__)


   def run(
       conn: psycopg2.extensions.connection,
       config: dict,
   ) -> dict:
       """
       Ingest all configured translations into the translations table.

       Args:
           conn:   Live psycopg2 connection to the psalms database.
           config: Full parsed config.yml dict.

       Returns:
           {"rows_written": int, "elapsed_s": float}
       """
       t0 = time.monotonic()
       sources = config.get("translations", {}).get("sources", [])
       corpus_books = [
           b["book_num"]
           for b in config.get("corpus", {}).get("books", [])
       ]

       if not sources:
           logger.warning("No translation sources configured. Skipping.")
           return {"rows_written": 0, "elapsed_s": 0.0}

       total_written = 0

       for source in sources:
           t_id: str = source["id"]
           logger.info("Ingesting translation: %s", t_id)

           # adapter_factory raises ValueError for unknown formats —
           # let it propagate so the caller knows about misconfiguration
           adapter = adapter_factory(source)

           for book_num in corpus_books:
               rows = _ingest_book(conn, adapter, t_id, book_num)
               total_written += rows
               logger.info("  %s book %s: %d rows written", t_id, book_num, rows)

       elapsed = time.monotonic() - t0
       logger.info(
           "Translation ingest complete: %d rows in %.2fs", total_written, elapsed
       )
       return {"rows_written": total_written, "elapsed_s": round(elapsed, 3)}


   def _ingest_book(
       conn: psycopg2.extensions.connection,
       adapter: object,
       translation_key: str,
       book_num: int,
   ) -> int:
       """
       Fetch verses for one book from the adapter and upsert into translations.

       Args:
           conn:            Live database connection.
           adapter:         TranslationAdapter instance.
           translation_key: Short ID string e.g. 'KJV'.
           book_num:        BHSA book number.

       Returns:
           Number of rows upserted.
       """
       # Look up (chapter, verse_num) -> verse_id for this book
       with conn.cursor() as cur:
           cur.execute(
               "SELECT chapter, verse_num, verse_id "
               "FROM verses WHERE book_num = %s",
               (book_num,),
           )
           verse_lookup: dict[tuple[int, int], int] = {
               (int(r[0]), int(r[1])): int(r[2]) for r in cur.fetchall()
           }

       if not verse_lookup:
           logger.warning(
               "verses table is empty for book_num=%s. "
               "Re-run after Stage 2 populates verses.",
               book_num,
           )
           return 0

       rows_to_insert: list[tuple[int, str, str]] = []

       for (chapter, verse_num), verse_id in verse_lookup.items():
           text = adapter.get_verse(book_num, chapter, verse_num)  # type: ignore[attr-defined]
           if text is None:
               logger.debug(
                   "  %s %s:%s:%s — not found in source, skipping",
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
               ON CONFLICT (verse_id, translation_key)
               DO UPDATE SET verse_text = EXCLUDED.verse_text
               """,
               rows_to_insert,
           )
       conn.commit()
       return len(rows_to_insert)
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_ingest_translations.py -v
   # Expected: PASSED (4 tests)
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat: implement ingest_translations.py — run(conn, config) -> dict"`

---

### Task 5: Post-ingest validation script

**Files:**
- `pipeline/validate_data.py`

**Steps:**

1. Write test in `tests/test_validate_data.py`:

   ```python
   # tests/test_validate_data.py
   """
   Unit tests for the validate_data module.

   Tests run against a mock connection — no live database required.
   """
   from __future__ import annotations

   from unittest.mock import MagicMock

   import pytest

   from validate_data import run


   def _mock_conn(verse_text: str | None) -> MagicMock:
       """Build a mock psycopg2 connection that returns verse_text for any query."""
       mock_conn = MagicMock()
       mock_cursor = MagicMock()
       if verse_text is not None:
           mock_cursor.fetchone.return_value = (verse_text,)
       else:
           mock_cursor.fetchone.return_value = None
       mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
       mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
       return mock_conn


   def test_run_passes_when_all_checks_pass() -> None:
       """run() must return {'passed': N, 'failed': 0} when all checks find data."""
       # Return KJV text for every check
       conn = _mock_conn("The LORD is my shepherd; I shall not want.")
       result = run(conn, {})
       assert result["failed"] == 0
       assert result["passed"] > 0


   def test_run_raises_assertion_error_on_missing_verse() -> None:
       """run() must raise AssertionError when a verse is missing from the DB."""
       conn = _mock_conn(None)
       with pytest.raises(AssertionError, match="MISSING"):
           run(conn, {})
   ```

2. Run and confirm FAILED:

   ```bash
   uv run --frozen pytest tests/test_validate_data.py -v
   # Expected: FAILED — validate_data module does not exist
   ```

3. Implement `pipeline/validate_data.py`:

   ```python
   # pipeline/validate_data.py
   """
   Stage 1 — Post-ingest data validation.

   Checks that known verses return expected text from the translations table.
   Raises AssertionError with a clear diagnostic message if any check fails.

   Entry point:
       run(conn, config) -> {"passed": int, "failed": int}
   """

   from __future__ import annotations

   import logging

   import psycopg2.extensions

   logger = logging.getLogger(__name__)

   # Known-good checks: (book_num, chapter, verse_num, translation_key, expected_prefix)
   # These are immutable facts — if they fail, the data is wrong.
   CHECKS: list[tuple[int, int, int, str, str]] = [
       # Psalm 23:1 — primary fixture across all configured translations
       (19, 23, 1, "KJV", "The LORD is my shepherd"),
       (19, 23, 1, "YLT", "Jehovah"),
       (19, 23, 1, "WEB", "Yahweh"),
       (19, 23, 1, "ULT", "Yahweh"),
       (19, 23, 1, "UST", "God"),
       # Psalm 1:1 — first verse of the book
       (19,  1, 1, "KJV", "Blessed"),
       # Psalm 150:6 — last verse of the book
       (19, 150, 6, "KJV", "Let every thing that hath breath"),
   ]


   def run(
       conn: psycopg2.extensions.connection,
       config: dict,
   ) -> dict:
       """
       Validate that known verses return expected text from the translations table.

       Args:
           conn:   Live psycopg2 connection.
           config: Full parsed config.yml dict (unused; present for module interface).

       Returns:
           {"passed": int, "failed": int}

       Raises:
           AssertionError: If any check fails. Message includes all failures.
       """
       failures: list[str] = []
       passed = 0

       for book_num, chapter, verse_num, key, expected_prefix in CHECKS:
           with conn.cursor() as cur:
               cur.execute(
                   """
                   SELECT t.verse_text
                   FROM translations t
                   JOIN verses v ON t.verse_id = v.verse_id
                   WHERE v.book_num = %s
                     AND v.chapter = %s
                     AND v.verse_num = %s
                     AND t.translation_key = %s
                   """,
                   (book_num, chapter, verse_num, key),
               )
               row = cur.fetchone()

           ref = f"{key} {book_num}:{chapter}:{verse_num}"

           if row is None:
               failures.append(f"MISSING: {ref}")
           elif not row[0].startswith(expected_prefix):
               failures.append(
                   f"WRONG TEXT: {ref}\n"
                   f"  Expected prefix: '{expected_prefix}'\n"
                   f"  Got:             '{row[0][:80]}'"
               )
           else:
               passed += 1

       if failures:
           msg = (
               f"Data validation FAILED ({len(failures)} check(s)):\n"
               + "\n".join(failures)
           )
           logger.error(msg)
           raise AssertionError(msg)

       logger.info(
           "Data validation passed: %d/%d checks", passed, len(CHECKS)
       )
       return {"passed": passed, "failed": 0}
   ```

4. Run and confirm PASSED:

   ```bash
   uv run --frozen pytest tests/test_validate_data.py -v
   # Expected: PASSED (2 tests)
   ```

5. Lint + typecheck:

   ```bash
   uv run --frozen ruff check . --fix && uv run --frozen pyright
   ```

6. Commit: `"feat: add validate_data.py — post-ingest correctness checks"`

---

### Task 6: Full test suite green

**Steps:**

1. Run the complete test suite including all new Stage 1 tests:

   ```bash
   uv run --frozen pytest tests/ -v
   # Expected: all tests PASSED
   # tests/test_scaffold.py                  — 5 tests
   # tests/test_config.py                    — 6 tests
   # tests/test_schema_sql.py                — 8 tests
   # tests/test_dockerfile.py                — 9 tests
   # tests/test_streamlit_placeholder.py     — 8 tests
   # tests/test_docker_compose.py            — 12 tests
   # tests/test_package_structure.py         — 4 tests
   # tests/test_translation_adapters.py      — 10 tests
   # tests/test_ingest_translations.py       — 4 tests
   # tests/test_validate_data.py             — 2 tests
   ```

2. Run lint and format:

   ```bash
   uv run --frozen ruff check . --fix
   uv run --frozen ruff format .
   uv run --frozen pyright
   ```

3. Commit: `"stage-1: all adapter and ingest tests passing"`

---

### Task 7: Download source files and run live ingest (integration step)

This task runs outside pytest — it requires Docker and internet access, and completes
only after Stage 2 has populated the `verses` table. It is documented here so the
integration sequence is unambiguous.

**Steps:**

1. Download BHSA dataset (inside pipeline container):

   ```bash
   docker compose --profile pipeline run --rm pipeline python -c "
   from tf.app import use
   A = use('ETCBC/bhsa', hoist=globals(), checkout='clone')
   "
   # Downloads ~200 MB to /data/bhsa/ (mapped from ./data/bhsa on the host)
   ```

2. Download scrollmapper SQLite files (from the host):

   ```bash
   cd /home/user/OT-NLP/data/translations
   for db in t_kjv t_ylt t_web t_asv t_dby; do
     curl -L \
       "https://github.com/scrollmapper/bible_databases/raw/master/sqlite/${db}.db" \
       -o "${db}.db"
   done
   ```

3. Clone unfoldingWord USFM repositories:

   ```bash
   cd /home/user/OT-NLP/data/translations
   git clone --depth 1 https://github.com/unfoldingWord/en_ult ult
   git clone --depth 1 https://github.com/unfoldingWord/en_ust ust
   ```

   Verify the Psalms USFM files exist:

   ```bash
   ls /home/user/OT-NLP/data/translations/ult/19PSA.usfm
   ls /home/user/OT-NLP/data/translations/ust/19PSA.usfm
   # Expected: both files present
   ```

4. Run Stage 2 first to populate `verses` (per the sequencing note in the module):

   ```bash
   docker compose --profile pipeline run --rm pipeline \
     python -m modules.ingest
   # Stage 2 populates verses (2,527 rows) and word_tokens (~43,000 rows)
   ```

5. Run translation ingest (Stage 1 second pass):

   ```bash
   docker compose --profile pipeline run --rm pipeline python -c "
   import yaml, psycopg2, logging, sys
   sys.path.insert(0, '/pipeline')
   logging.basicConfig(level=logging.INFO)

   with open('/pipeline/config.yml') as f:
       config = yaml.safe_load(f)

   conn = psycopg2.connect(
       host='db', dbname='psalms', user='psalms', password='psalms_dev'
   )

   from modules.ingest_translations import run
   result = run(conn, config)
   print('Result:', result)
   conn.close()
   "
   # Expected: rows_written = 5 * 2527 = 12,635 (5 translations x 2,527 verses)
   ```

6. Run data validation:

   ```bash
   docker compose --profile pipeline run --rm pipeline python -c "
   import yaml, psycopg2, sys
   sys.path.insert(0, '/pipeline')

   with open('/pipeline/config.yml') as f:
       config = yaml.safe_load(f)

   conn = psycopg2.connect(
       host='db', dbname='psalms', user='psalms', password='psalms_dev'
   )

   from validate_data import run
   result = run(conn, config)
   print('Validation result:', result)
   conn.close()
   "
   # Expected: {'passed': 7, 'failed': 0}
   ```

7. Query Psalm 23:1 directly in JupyterLab to confirm Stage 1 acceptance:

   ```python
   # In a JupyterLab notebook cell:
   import psycopg2
   conn = psycopg2.connect(
       host='db', dbname='psalms', user='psalms', password='psalms_dev'
   )
   with conn.cursor() as cur:
       cur.execute("""
           SELECT t.translation_key, t.verse_text
           FROM translations t
           JOIN verses v ON t.verse_id = v.verse_id
           WHERE v.book_num = 19 AND v.chapter = 23 AND v.verse_num = 1
           ORDER BY t.translation_key
       """)
       for row in cur.fetchall():
           print(row[0], ':', row[1])
   # Expected: 5 rows, one per translation, each with correct Psalm 23:1 text
   ```

8. Final commit: `"stage-1: complete — all translations ingested, validation passing"`
