# tests/test_phoneme_adapter.py
"""Tests for English phoneme adapter (Stage 4).

Uses CMU Pronouncing Dictionary via the `pronouncing` library with
a heuristic fallback for unknown words. No API calls.

Run with:
    uv run --frozen pytest tests/test_phoneme_adapter.py -v
"""

from __future__ import annotations

from adapters.phoneme_adapter import (
    VOWEL_OPENNESS,
    _heuristic_syllables,
    english_breath_weights,
    english_fingerprint,
    get_syllables_and_stress,
    syllable_count,
)


def test_cmu_lookup_the_lord() -> None:
    """'lord' is in CMU dict and returns at least one phoneme."""
    count, weights = get_syllables_and_stress("lord")
    assert count >= 1
    assert len(weights) >= 1
    assert all(0.0 < w <= 1.0 for w in weights)


def test_cmu_lookup_missing_word_heuristic() -> None:
    """A word absent from CMU falls back to the heuristic counter."""
    # 'xyzphrm' will never be in CMU; heuristic must return >= 1
    count, weights = get_syllables_and_stress("xyzphrm")
    assert count >= 1
    assert len(weights) == count


def test_syllable_count_shepherd() -> None:
    """'shepherd' has exactly 2 syllables."""
    assert syllable_count("shepherd") == 2


def test_syllable_count_heuristic_simple() -> None:
    """Single-syllable word 'cat' counts as 1 via heuristic."""
    count, weights = _heuristic_syllables("cat")
    assert count == 1
    assert len(weights) == 1


def test_stress_positions_detected() -> None:
    """'shepherd' has primary stress on the first syllable (index 0)."""
    count, weights = get_syllables_and_stress("shepherd")
    assert count == 2
    # Primary-stressed syllable carries more weight than secondary
    assert weights[0] > weights[1]


def test_vowel_openness_aa() -> None:
    """The 'AA' phoneme (e.g. 'father') maps to openness 1.0."""
    assert VOWEL_OPENNESS["AA"] == 1.00


def test_english_fingerprint_density_range() -> None:
    """syllable_density for a normal English sentence falls in [0.5, 3.0]."""
    fp = english_fingerprint("The LORD is my shepherd; I shall not want.")
    assert 0.5 <= fp["syllable_density"] <= 3.0


def test_english_fingerprint_all_dimensions() -> None:
    """All four fingerprint dimensions are present and non-null/non-negative."""
    fp = english_fingerprint("The LORD is my shepherd; I shall not want.")
    for key in (
        "syllable_density",
        "morpheme_ratio",
        "sonority_score",
        "clause_compression",
    ):
        assert key in fp
        assert fp[key] is not None
        assert fp[key] >= 0.0


def test_phoneme_adapter_empty_string() -> None:
    """Empty input returns a zero-dict (not None, not an exception)."""
    fp = english_fingerprint("")
    assert fp is not None
    for v in fp.values():
        assert v == 0.0
    weights = english_breath_weights("")
    assert weights == []
