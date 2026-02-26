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
from decimal import Decimal
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


# ── fetch_breath_profiles_batch ──────────────────────────────────────────────


def test_fetch_breath_profiles_batch_returns_dict() -> None:
    """fetch_breath_profiles_batch returns a dict keyed by verse_num."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [
        {
            "verse_num": 1,
            "verse_id": 310,
            "hebrew_text": "יְהוָה רֹעִי",
            "breath_curve": [0.5, 0.8, 0.6],
            "mean_weight": 0.63,
            "colon_count": 2,
        },
        {
            "verse_num": 2,
            "verse_id": 311,
            "hebrew_text": "בִּנְאוֹת דֶּשֶׁא",
            "breath_curve": [0.4, 0.7, 0.9],
            "mean_weight": 0.67,
            "colon_count": 2,
        },
    ]
    result = _app.fetch_breath_profiles_batch(mock_conn, chapter=23, verse_nums=[1, 2])

    assert isinstance(result, dict)
    assert set(result.keys()) == {1, 2}
    assert result[1]["verse_id"] == 310
    assert result[2]["hebrew_text"] == "בִּנְאוֹת דֶּשֶׁא"
    assert isinstance(result[1]["breath_curve"], list)


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


# ── build_translation_fingerprints ───────────────────────────────────────────


def test_build_translation_fingerprints_uses_deviations() -> None:
    """build_translation_fingerprints derives each dimension from heb_fp - deviation.

    Guards against regression to hardcoded dummy values: with two translations
    having *different* deviation scores, the resulting fingerprints must differ.
    """
    heb_fp = {
        "syllable_density": 1.57,
        "morpheme_ratio": 2.86,
        "sonority_score": 0.73,
        "clause_compression": 7.0,
    }
    scores = {
        "KJV": {
            "density_deviation": 0.46,
            "morpheme_deviation": 1.97,
            "sonority_deviation": 0.18,
            "compression_deviation": 2.5,
        },
        "UST": {
            "density_deviation": 0.10,
            "morpheme_deviation": 1.00,
            "sonority_deviation": 0.05,
            "compression_deviation": 6.0,
        },
    }
    fps = _app.build_translation_fingerprints(heb_fp, scores, ["KJV", "UST"])

    assert len(fps) == 2

    kjv_fp, ust_fp = fps

    # Values must NOT be the old hardcoded dummies (1.5, 1.0, 0.4, 5.0)
    assert kjv_fp["syllable_density"] != pytest.approx(1.5), "Dummy value detected"
    assert kjv_fp["morpheme_ratio"] != pytest.approx(1.0), "Dummy value detected"
    assert kjv_fp["sonority_score"] != pytest.approx(0.4), "Dummy value detected"
    assert kjv_fp["clause_compression"] != pytest.approx(5.0), "Dummy value detected"

    # KJV and UST must differ (different deviations → different fingerprints)
    assert kjv_fp["syllable_density"] != pytest.approx(ust_fp["syllable_density"])
    assert kjv_fp["clause_compression"] != pytest.approx(ust_fp["clause_compression"])

    # Values are non-negative (max(0, …) guard)
    for fp in fps:
        for dim, val in fp.items():
            assert val >= 0.0, f"{dim} went negative: {val}"

    # Spot-check arithmetic: KJV syllable_density = max(0, 1.57 - 0.46) = 1.11
    assert kjv_fp["syllable_density"] == pytest.approx(1.57 - 0.46, abs=1e-6)

    # UST clause_compression: max(0, 7.0 - 6.0) = 1.0
    assert ust_fp["clause_compression"] == pytest.approx(1.0, abs=1e-6)


def test_build_translation_fingerprints_clamps_to_zero() -> None:
    """build_translation_fingerprints never returns a negative dimension value."""
    heb_fp = {"syllable_density": 0.5, "morpheme_ratio": 0.3,
               "sonority_score": 0.1, "clause_compression": 1.0}
    # Deviation larger than heb_fp value — would go negative without the clamp
    scores = {
        "KJV": {
            "density_deviation": 2.0,
            "morpheme_deviation": 2.0,
            "sonority_deviation": 2.0,
            "compression_deviation": 5.0,
        }
    }
    fps = _app.build_translation_fingerprints(heb_fp, scores, ["KJV"])
    for dim, val in fps[0].items():
        assert val == pytest.approx(0.0), f"{dim} not clamped: {val}"


def test_build_translation_fingerprints_missing_scores_defaults() -> None:
    """When a translation has no scores entry, fingerprint mirrors heb_fp."""
    heb_fp = {"syllable_density": 1.5, "morpheme_ratio": 2.0,
               "sonority_score": 0.6, "clause_compression": 5.0}
    scores: dict = {}  # no scores at all
    fps = _app.build_translation_fingerprints(heb_fp, scores, ["NEW"])
    # With zero deviation, eng ≈ heb
    assert fps[0]["syllable_density"] == pytest.approx(1.5)
    assert fps[0]["morpheme_ratio"] == pytest.approx(2.0)
    assert fps[0]["sonority_score"] == pytest.approx(0.6)
    assert fps[0]["clause_compression"] == pytest.approx(5.0)


def test_build_translation_fingerprints_accepts_decimal_scores() -> None:
    """build_translation_fingerprints handles Decimal deviation values from psycopg2.

    psycopg2 returns NUMERIC columns as decimal.Decimal, not float.  Without an
    explicit float() cast, subtracting Decimal from float raises TypeError and
    the radar chart silently fails to render.
    """
    heb_fp = {
        "syllable_density": 1.5714,
        "morpheme_ratio": 2.8571,
        "sonority_score": 0.7286,
        "clause_compression": 7.0,
    }
    # Simulate the Decimal values psycopg2 returns for NUMERIC(7,4) columns
    scores = {
        "KJV": {
            "density_deviation": Decimal("0.4603"),
            "morpheme_deviation": Decimal("1.9682"),
            "sonority_deviation": Decimal("0.1764"),
            "compression_deviation": Decimal("2.5000"),
        },
    }
    # Must not raise TypeError
    fps = _app.build_translation_fingerprints(heb_fp, scores, ["KJV"])

    assert len(fps) == 1
    fp = fps[0]
    # All values must be plain Python floats (not Decimal)
    for dim, val in fp.items():
        assert isinstance(val, float), f"{dim} is {type(val).__name__}, expected float"
        assert val >= 0.0
    # Spot-check: syllable_density = max(0, 1.5714 - 0.4603) ≈ 1.1111
    assert fp["syllable_density"] == pytest.approx(1.5714 - 0.4603, abs=1e-4)
