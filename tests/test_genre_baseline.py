# tests/test_genre_baseline.py
"""Tests for Stage 8 genre baseline module.

All tests use mocked DB connections — no live database required.

Run with:
    uv run --frozen pytest tests/test_genre_baseline.py -v
"""

from __future__ import annotations

from unittest.mock import MagicMock

# ── Helpers ─────────────────────────────────────────────────────────────────


def _mock_conn(aggregate_rows: list) -> MagicMock:
    """Return a MagicMock connection whose cursor returns *aggregate_rows* on
    the first fetchall() call (the aggregation query).  Subsequent cursor
    uses (the upsert loop) call execute() but not fetchall()."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cursor.fetchall.return_value = aggregate_rows
    return conn


_TWO_GENRE_ROWS = [
    # (genre, count, density_mean, density_stddev, morph_mean, morph_stddev,
    #  sonority_mean, sonority_stddev, compression_mean, compression_stddev)
    ("hebrew_poetry", 2527, 2.20, 0.30, 1.80, 0.20, 0.55, 0.05, 3.20, 0.40),
    ("hebrew_prophecy", 1292, 2.00, 0.28, 1.70, 0.18, 0.51, 0.05, 3.00, 0.38),
]


# ── Tests ────────────────────────────────────────────────────────────────────


def test_run_returns_correct_keys() -> None:
    """run() must return a dict with 'rows_written' (int) and 'elapsed_s' (float)."""
    from modules.genre_baseline import run

    result = run(_mock_conn(_TWO_GENRE_ROWS), {})

    assert isinstance(result, dict), "run() must return a dict"
    assert "rows_written" in result, "result must contain 'rows_written'"
    assert "elapsed_s" in result, "result must contain 'elapsed_s'"
    assert isinstance(result["rows_written"], int)
    assert isinstance(result["elapsed_s"], float)


def test_run_writes_one_row_per_genre() -> None:
    """rows_written must equal the number of distinct genres in verse_fingerprints."""
    from modules.genre_baseline import run

    result = run(_mock_conn(_TWO_GENRE_ROWS), {})

    assert result["rows_written"] == 2, (
        f"Expected 2 rows (one per genre), got {result['rows_written']}"
    )


def test_run_is_idempotent() -> None:
    """Calling run() twice with the same data must not raise and must return
    the same rows_written count both times."""
    from modules.genre_baseline import run

    result1 = run(_mock_conn(_TWO_GENRE_ROWS), {})
    result2 = run(_mock_conn(_TWO_GENRE_ROWS), {})

    assert result1["rows_written"] == result2["rows_written"]


def test_run_no_fingerprints_returns_zero() -> None:
    """When no verse_fingerprints with a genre exist, rows_written must be 0."""
    from modules.genre_baseline import run

    result = run(_mock_conn([]), {})

    assert result["rows_written"] == 0, (
        "Expected rows_written=0 when no genre data is present"
    )
