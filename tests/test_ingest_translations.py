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
from unittest.mock import MagicMock

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
