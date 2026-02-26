# tests/test_streamlit_queries.py
"""Unit tests for Streamlit query and data-transform helpers.

Tests call the helper functions extracted from streamlit/app.py.
All DB calls are mocked via unittest.mock.MagicMock — no live DB needed.
Streamlit decorators (@st.cache_data, @st.cache_resource) are transparent
outside of a running Streamlit server, so functions import and run normally.

Run with:
    uv run --frozen pytest tests/test_streamlit_queries.py -v
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Ensure streamlit/ is importable
sys.path.insert(0, str(Path(__file__).parent.parent / "streamlit"))

# Stub heavy Streamlit symbols before importing app so that module-level
# st.set_page_config(), st.title(), st.cache_resource() etc. do not raise
# outside a running Streamlit server.
# We use MagicMock as the base so that *any* st.* attribute resolves without
# error; only the decorator helpers need special handling so that
# @st.cache_resource / @st.cache_data pass the decorated function through.
_st_stub = MagicMock()
_st_stub.cache_resource = lambda f=None, **kw: f if f else lambda fn: fn  # type: ignore[attr-defined]
_st_stub.cache_data = lambda f=None, **kw: f if f else lambda fn: fn  # type: ignore[attr-defined]
_st_stub.set_page_config = MagicMock()  # type: ignore[attr-defined]
_st_stub.sidebar = MagicMock()  # type: ignore[attr-defined]
_st_stub.secrets = {}  # type: ignore[attr-defined]
# st.runtime.exists() → False so the module-level page-rendering block is skipped
_st_stub.runtime = MagicMock()  # type: ignore[attr-defined]
_st_stub.runtime.exists.return_value = False  # type: ignore[attr-defined]
sys.modules.setdefault("streamlit", _st_stub)

# Also stub plotly import used at module level
import plotly.graph_objects as _go  # noqa: F401, E402

# Import the helpers we want to test directly from the module's namespace after load.
_app = importlib.import_module("app")


# ── fetch_breath_profile ──────────────────────────────────────────────────────


def test_fetch_breath_profile_returns_row() -> None:
    """fetch_breath_profile returns a dict row for valid inputs."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = {
        "verse_id": 555,
        "breath_curve": [0.5, 0.7, 0.6],
        "mean_weight": 0.6,
        "colon_count": 2,
        "hebrew_text": "יְהוָה רֹעִי",
    }
    result = _app.fetch_breath_profile(mock_conn, chapter=23, verse_num=1)
    assert result is not None
    assert result["verse_id"] == 555
    assert isinstance(result["breath_curve"], list)


def test_fetch_breath_profile_returns_none_when_missing() -> None:
    """fetch_breath_profile returns None when the verse is absent from DB."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None
    result = _app.fetch_breath_profile(mock_conn, chapter=999, verse_num=99)
    assert result is None


# ── fetch_deviation_scores ────────────────────────────────────────────────────


def test_fetch_deviation_scores_returns_list() -> None:
    """fetch_deviation_scores returns a list of score dicts."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [
        {"chapter": 23, "translation_key": "KJV", "composite_deviation": 0.15},
        {"chapter": 23, "translation_key": "ESV", "composite_deviation": 0.10},
    ]
    result = _app.fetch_deviation_scores(mock_conn, translation_keys=["KJV", "ESV"])
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["translation_key"] == "KJV"


def test_fetch_deviation_scores_empty_returns_empty_list() -> None:
    """fetch_deviation_scores returns [] when no rows found."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    result = _app.fetch_deviation_scores(mock_conn, translation_keys=["KJV"])
    assert result == []


# ── build_heatmap_matrix ──────────────────────────────────────────────────────


def test_build_heatmap_matrix_shape() -> None:
    """build_heatmap_matrix produces a matrix with correct row/col counts."""
    score_rows = [
        {"chapter": 1, "translation_key": "KJV", "composite_deviation": 0.20},
        {"chapter": 1, "translation_key": "ESV", "composite_deviation": 0.15},
        {"chapter": 23, "translation_key": "KJV", "composite_deviation": 0.10},
        {"chapter": 23, "translation_key": "ESV", "composite_deviation": 0.08},
    ]
    chapters, keys, matrix = _app.build_heatmap_matrix(score_rows)
    assert chapters == [1, 23]
    assert sorted(keys) == ["ESV", "KJV"]
    assert len(matrix) == 2  # 2 chapters
    assert len(matrix[0]) == 2  # 2 translations


def test_build_heatmap_matrix_empty_returns_empty() -> None:
    """build_heatmap_matrix returns three empty lists for empty input."""
    chapters, keys, matrix = _app.build_heatmap_matrix([])
    assert chapters == []
    assert keys == []
    assert matrix == []


# ── fetch_chiasm_candidates ───────────────────────────────────────────────────


def test_fetch_chiasm_candidates_returns_list() -> None:
    """fetch_chiasm_candidates returns a list of candidate dicts."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [
        {
            "verse_id_start": 100,
            "verse_id_end": 105,
            "pattern_type": "ABBA",
            "confidence": 0.85,
            "colon_matches": [{"a": 0, "b": 3}],
        }
    ]
    result = _app.fetch_chiasm_candidates(
        mock_conn, verse_ids=[100, 101, 102, 103, 104, 105]
    )
    assert isinstance(result, list)
    assert result[0]["pattern_type"] == "ABBA"


# ── fetch_translation_scores_for_verse ───────────────────────────────────────


def test_fetch_translation_scores_for_verse() -> None:
    """fetch_translation_scores_for_verse returns per-key score dicts."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [
        {
            "translation_key": "KJV",
            "composite_deviation": 0.12,
            "breath_alignment": 0.78,
            "density_deviation": 0.05,
            "morpheme_deviation": 0.03,
            "sonority_deviation": 0.02,
            "compression_deviation": 0.02,
        }
    ]
    result = _app.fetch_translation_scores_for_verse(
        mock_conn, verse_id=555, translation_keys=["KJV"]
    )
    assert "KJV" in result
    assert result["KJV"]["composite_deviation"] == pytest.approx(0.12)
