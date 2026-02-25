# tests/test_score.py
"""Tests for Stage 4 translation scoring module.

All tests are pure-logic unit tests using mocked DB connections.
No live database connection required.

Run with:
    uv run --frozen pytest tests/test_score.py -v
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np

# ── _compute_breath_alignment unit tests ────────────────────────────────────


def test_breath_alignment_range() -> None:
    """stress_alignment and weight_match must always be in [0.0, 1.0]."""
    from modules.score import _compute_breath_alignment

    heb_data = {
        "stress_positions": [0.25, 0.5, 0.75],
        "mean_weight": 0.60,
    }
    for text in [
        "The LORD is my shepherd; I shall not want.",
        "Praise the LORD all ye nations.",
        "He restores my soul.",
        "Yea though I walk through the valley of the shadow of death.",
    ]:
        align, match = _compute_breath_alignment(heb_data, text)
        assert 0.0 <= align <= 1.0, f"stress_alignment out of range for: {text!r}"
        assert 0.0 <= match <= 1.0, f"weight_match out of range for: {text!r}"


def test_breath_alignment_empty_stress_returns_midpoint() -> None:
    """Empty Hebrew stress list should return 0.5 midpoint for stress_alignment."""
    from modules.score import _compute_breath_alignment

    heb_data = {"stress_positions": [], "mean_weight": 0.5}
    align, match = _compute_breath_alignment(heb_data, "some text")
    assert align == 0.5


def test_breath_alignment_returns_floats() -> None:
    """Both return values must be Python floats."""
    from modules.score import _compute_breath_alignment

    heb_data = {"stress_positions": [0.3, 0.7], "mean_weight": 0.55}
    align, match = _compute_breath_alignment(
        heb_data, "The heavens declare the glory of God"
    )
    assert isinstance(align, float)
    assert isinstance(match, float)


# ── run() integration tests using mocked DB ─────────────────────────────────


def _make_config(translation_keys: list[str] | None = None) -> dict:
    keys = translation_keys or ["KJV", "YLT"]
    return {
        "corpus": {"books": [{"book_num": 19}], "debug_chapters": [23]},
        "translations": {
            "sources": [{"id": k} for k in keys]
        },
        "scoring": {
            "batch_size": 50,
            "deviation_weights": {
                "density": 0.35,
                "morpheme": 0.25,
                "sonority": 0.20,
                "compression": 0.20,
            },
            "breath_alignment_weights": {"stress": 0.60, "weight": 0.40},
        },
    }


def _make_conn_mock(
    heb_fp_rows: list,
    heb_breath_rows: list,
    trans_rows: list,
) -> tuple:
    """Build a MagicMock psycopg2 connection that returns preset rows for
    the three loader queries, in order.
    """
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    cursor.fetchall.side_effect = [heb_fp_rows, heb_breath_rows, trans_rows]
    cursor.fetchone.return_value = (0,)
    return conn, cursor


def test_score_idempotent() -> None:
    """Calling run() twice with the same data must not raise a UNIQUE violation.

    The ON CONFLICT DO UPDATE clause handles idempotency; we verify run()
    returns a positive scored count both times without raising.
    """
    from modules.score import run

    verse_id = 1
    heb_fp_rows = [(verse_id, 2.1, 1.8, 0.60, 3.5)]
    heb_breath_rows = [
        (verse_id, [0.6, 0.5, 0.7], [0.3, 0.7], 0.60)
    ]
    trans_rows = [
        (verse_id, "KJV", "The LORD is my shepherd; I shall not want."),
        (verse_id, "YLT", "Jehovah is my shepherd, I do not lack."),
    ]

    config = _make_config(["KJV", "YLT"])

    for _ in range(2):
        conn, cursor = _make_conn_mock(heb_fp_rows, heb_breath_rows, trans_rows)
        with patch("modules.score.batch_upsert") as mock_upsert:
            result = run(conn, config)
        assert result.get("scored", 0) == 2  # 1 verse × 2 translations
        mock_upsert.assert_called_once()


def test_deviation_ylt_lt_ust() -> None:
    """YLT (literal) should produce a lower composite_deviation than UST
    (simplification) for the same Hebrew verse. We test the scoring math
    directly by computing fingerprints for representative texts.
    """
    from adapters.phoneme_adapter import english_fingerprint

    # Psalm 23:1 — YLT is more word-for-word; UST is dynamic-equivalent
    ylt_text = "Jehovah is my shepherd, I do not lack."
    ust_text = "God is my caretaker. He provides for everything I need."

    heb_fp = {
        "syllable_density": 2.1,
        "morpheme_ratio": 1.80,
        "sonority_score": 0.60,
        "clause_compression": 3.0,
    }
    weights = np.array([0.35, 0.25, 0.20, 0.20])

    def composite(text: str) -> float:
        eng = english_fingerprint(text)
        heb_vec = np.array(list(heb_fp.values()))
        eng_vec = np.array([
            eng["syllable_density"],
            eng["morpheme_ratio"],
            eng["sonority_score"],
            eng["clause_compression"],
        ])
        return float(np.dot(weights, np.abs(heb_vec - eng_vec)))

    ylt_dev = composite(ylt_text)
    ust_dev = composite(ust_text)
    assert ylt_dev < ust_dev, (
        f"Expected YLT ({ylt_dev:.4f}) < UST ({ust_dev:.4f})"
    )
