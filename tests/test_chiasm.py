# tests/test_chiasm.py
"""Tests for Stage 2 chiasm detection module.

These tests exercise pure logic only (cosine similarity, pattern detection).
No DB connection is required. Execution of chiasm.run() is deferred until
Stage 3 back-populates colon_fingerprints.
"""

from __future__ import annotations

import numpy as np
from modules.chiasm import _cosine_similarity, _detect_patterns


def test_abba_pattern_detected() -> None:
    """Four colons with matching outer/inner pairs should produce ABBA result."""
    a = np.array([1.0, 0.0, 0.0, 0.0])
    b = np.array([0.0, 1.0, 0.0, 0.0])
    # Layout: A B B' A' — colons 0&3 match, colons 1&2 match
    colons = [
        (100, 1, a),
        (100, 2, b),
        (100, 3, b),
        (100, 4, a),
    ]
    results = _detect_patterns(colons, threshold=0.8, min_confidence=0.65)
    abba = [r for r in results if r["pattern_type"] == "ABBA"]
    assert len(abba) >= 1
    assert abba[0]["confidence"] >= 0.8


def test_abcba_pattern_detected() -> None:
    """Five colons with ABCBA structure should produce ABCBA result."""
    a = np.array([1.0, 0.0, 0.0, 0.0])
    b = np.array([0.0, 1.0, 0.0, 0.0])
    c = np.array([0.0, 0.0, 1.0, 0.0])  # pivot
    colons = [
        (101, 1, a),
        (101, 2, b),
        (101, 3, c),  # pivot — no match required
        (102, 1, b),  # matches colon index 1
        (102, 2, a),  # matches colon index 0
    ]
    results = _detect_patterns(colons, threshold=0.8, min_confidence=0.65)
    abcba = [r for r in results if r["pattern_type"] == "ABCBA"]
    assert len(abcba) >= 1


def test_below_threshold_not_detected() -> None:
    """Similarity below the configured threshold should produce no candidates."""
    # Random orthogonal-ish vectors: guaranteed low similarity
    rng = np.random.default_rng(seed=42)
    vectors = [rng.random(4) for _ in range(6)]
    colons = [(200, i, v) for i, v in enumerate(vectors)]
    results = _detect_patterns(colons, threshold=0.999, min_confidence=0.999)
    assert len(results) == 0


def test_chiasm_confidence_range() -> None:
    """Confidence must always be in [0.0, 1.0]."""
    a = np.array([1.0, 0.0, 0.0, 0.0])
    b = np.array([0.0, 1.0, 0.0, 0.0])
    colons = [(300, 1, a), (300, 2, b), (300, 3, b), (300, 4, a)]
    results = _detect_patterns(colons, threshold=0.5, min_confidence=0.0)
    for r in results:
        assert 0.0 <= r["confidence"] <= 1.0, (
            f"confidence {r['confidence']} outside [0,1]"
        )


def test_cosine_similarity_identical_vectors() -> None:
    """Identical vectors have cosine similarity of 1.0."""
    v = np.array([1.0, 2.0, 3.0, 4.0])
    assert abs(_cosine_similarity(v, v) - 1.0) < 1e-6


def test_cosine_similarity_orthogonal_vectors() -> None:
    """Orthogonal vectors have cosine similarity of 0.0."""
    a = np.array([1.0, 0.0, 0.0, 0.0])
    b = np.array([0.0, 1.0, 0.0, 0.0])
    assert abs(_cosine_similarity(a, b)) < 1e-6


def test_cosine_similarity_zero_vector() -> None:
    """Zero vector returns 0.0 (no division by zero)."""
    a = np.zeros(4)
    b = np.array([1.0, 2.0, 3.0, 4.0])
    assert _cosine_similarity(a, b) == 0.0
