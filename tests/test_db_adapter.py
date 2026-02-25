# tests/test_db_adapter.py
"""Tests for db_adapter resumable upsert utilities."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from adapters.db_adapter import (
    batch_upsert,
    get_processed_verse_ids,
    verse_ids_for_stage,
)


def test_get_processed_verse_ids_empty() -> None:
    """Returns empty set when target table has no rows."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = []
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    result = get_processed_verse_ids(conn, "verse_fingerprints")

    assert result == set()
    cur.execute.assert_called_once()


def test_get_processed_verse_ids_populated() -> None:
    """Returns correct set of verse_ids from target table."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = [(1,), (2,), (7,)]
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    result = get_processed_verse_ids(conn, "verse_fingerprints")

    assert result == {1, 2, 7}


def test_get_processed_verse_ids_custom_id_col() -> None:
    """Accepts a custom id_column argument."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = [(42,)]
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    result = get_processed_verse_ids(conn, "chiasm_candidates", "verse_id_start")

    assert result == {42}
    sql_call = cur.execute.call_args[0][0]
    assert "verse_id_start" in sql_call


def test_batch_upsert_returns_count() -> None:
    """Returns total number of rows passed across all batches."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    rows = [(i, f"val{i}") for i in range(5)]
    sql = "INSERT INTO test_table (id, val) VALUES %s"

    with patch("psycopg2.extras.execute_values"):
        count = batch_upsert(conn, sql, rows, batch_size=3)

    assert count == 5
    # Two batches: [0..2] and [3..4]
    assert conn.commit.call_count == 2


def test_batch_upsert_empty_rows() -> None:
    """Returns 0 for empty input without calling execute."""
    conn = MagicMock()
    count = batch_upsert(conn, "INSERT INTO t VALUES %s", [])
    assert count == 0
    conn.cursor.assert_not_called()


def test_verse_ids_for_stage_subtracts_done() -> None:
    """Returns only verse_ids not yet in the target table."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # First call: get_all_verse_ids → returns [1, 2, 3, 4]
    # Second call: get_processed_verse_ids → returns {2, 4}
    cur.fetchall.side_effect = [
        [(1,), (2,), (3,), (4,)],  # all verse ids
        [(2,), (4,)],              # already processed
    ]

    result = verse_ids_for_stage(conn, "verse_fingerprints", [19])

    assert sorted(result) == [1, 3]
