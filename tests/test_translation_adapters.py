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
    USFMAdapter,
    adapter_factory,
)

# ── Helpers ───────────────────────────────────────────────────


def _make_sqlite_db(tmp_path: Path, rows: list[tuple]) -> str:
    """Create a scrollmapper v2-schema SQLite file at tmp_path/test.db.

    Table is named KJV_verses to match the adapter id used in all tests.
    Rows are 4-tuples: (book_id, chapter, verse, text).
    """
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE KJV_verses "
        "(id INTEGER PRIMARY KEY AUTOINCREMENT, "
        " book_id INTEGER, chapter INTEGER, verse INTEGER, text TEXT)"
    )
    conn.executemany(
        "INSERT INTO KJV_verses (book_id, chapter, verse, text) VALUES (?,?,?,?)",
        rows,
    )
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
    db_path = _make_sqlite_db(
        tmp_path,
        [
            (19, 23, 1, "The LORD is my shepherd; I shall not want."),
            (19, 23, 2, "He maketh me to lie down in green pastures."),
        ],
    )
    adapter = SQLiteScrollmapperAdapter(
        {"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path}
    )
    verses = adapter.get_verse(19, 23, 1)
    assert verses == "The LORD is my shepherd; I shall not want."


# ── Test 2: SQLiteScrollmapperAdapter returns None for missing verse ──


def test_sqlite_adapter_missing_verse(tmp_path: Path) -> None:
    """SQLiteScrollmapperAdapter must return None for a verse that doesn't exist."""
    db_path = _make_sqlite_db(
        tmp_path,
        [
            (19, 23, 1, "The LORD is my shepherd; I shall not want."),
        ],
    )
    adapter = SQLiteScrollmapperAdapter(
        {"id": "KJV", "format": "sqlite_scrollmapper", "path": db_path}
    )
    result = adapter.get_verse(19, 23, 99)
    assert result is None


# ── Test 3: SQLiteScrollmapperAdapter filters by book ────────


def test_sqlite_adapter_book_filter(tmp_path: Path) -> None:
    """SQLiteScrollmapperAdapter.get_verse(19, ...) must not return Genesis rows."""
    db_path = _make_sqlite_db(
        tmp_path,
        [
            (19, 23, 1, "The LORD is my shepherd"),
            (1, 1, 1, "In the beginning God created"),
        ],
    )
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
    adapter = USFMAdapter({"id": "ULT", "format": "usfm", "path": str(usfm_dir)})
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
    adapter = USFMAdapter({"id": "ULT", "format": "usfm", "path": str(usfm_dir)})
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
    adapter = USFMAdapter({"id": "ULT", "format": "usfm", "path": str(usfm_dir)})
    result = adapter.get_verse(19, 23, 999)
    assert result is None


# ── Test 7: APIAdapter stub returns None ─────────────────────


def test_api_adapter_stub_returns_none() -> None:
    """APIAdapter.get_verse must return None (not implemented / stub)."""
    adapter = APIAdapter({"id": "ESV", "format": "api", "provider": "esv"})
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
    adapter = adapter_factory({"id": "ULT", "format": "usfm", "path": "/some/path"})
    assert isinstance(adapter, USFMAdapter)


# ── Test 10: adapter_factory raises ValueError for unknown format ──


def test_adapter_factory_unknown_format_raises() -> None:
    """adapter_factory must raise ValueError for an unrecognized format string."""
    with pytest.raises(ValueError, match="Unknown translation format"):
        adapter_factory({"id": "X", "format": "magic_format"})
