# tests/test_validate_data.py
"""
Unit tests for the validate_data module.

Tests run against a mock connection — no live database required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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
    conn = _mock_conn("The LORD is my shepherd; I shall not want.")
    # Patch CHECKS to a single entry that the mock text satisfies
    with patch("validate_data.CHECKS", [(19, 23, 1, "KJV", "The LORD is my shepherd")]):
        result = run(conn, {})
    assert result["failed"] == 0
    assert result["passed"] > 0


def test_run_raises_assertion_error_on_missing_verse() -> None:
    """run() must raise AssertionError when a verse is missing from the DB."""
    conn = _mock_conn(None)
    with pytest.raises(AssertionError, match="MISSING"):
        run(conn, {})
