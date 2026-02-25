# tests/test_fingerprint.py
"""Tests for Stage 2 style fingerprinting module."""

from __future__ import annotations

from modules.fingerprint import (
    SONORITY,
    _compute_fingerprint,
    _onset_sonority,
    count_hebrew_syllables,
)

# Psalm 23:1 in Hebrew: יְהוָה רֹעִי לֹא אֶחְסָר
PSALM_23_1_TOKENS = [
    # (verse_id, position, surface_form, morpheme_count, part_of_speech)
    (1, 1, "יְהוָה", 1, "proper_noun"),
    (1, 2, "רֹעִי", 2, "verb"),
    (1, 3, "לֹא", 1, "negative_particle"),
    (1, 4, "אֶחְסָר", 1, "verb"),
]


def test_syllable_density_psalm_23_1() -> None:
    """Psalm 23:1 syllable density must fall in the documented range."""
    fp = _compute_fingerprint(PSALM_23_1_TOKENS)
    assert 1.5 <= fp["syllable_density"] <= 4.0, (
        f"syllable_density={fp['syllable_density']} outside [1.5, 4.0]"
    )


def test_morpheme_ratio_simple() -> None:
    """Word with 2 prefixes + stem: morpheme_count=3, ratio=3.0 for single word."""
    tokens = [(1, 1, "וּבְאֶרֶץ", 3, "noun")]
    fp = _compute_fingerprint(tokens)
    assert abs(fp["morpheme_ratio"] - 3.0) < 0.01


def test_sonority_score_range() -> None:
    """sonority_score must always be in [0.0, 1.0]."""
    fp = _compute_fingerprint(PSALM_23_1_TOKENS)
    assert 0.0 <= fp["sonority_score"] <= 1.0


def test_clause_compression_no_conjunctions() -> None:
    """Without any conjunctions denominator is 1 → compression = word_count."""
    tokens = [
        (1, 1, "הָאִישׁ", 1, "noun"),
        (1, 2, "הַהוּא", 2, "pronoun_demonstrative"),
        (1, 3, "הָלַךְ", 2, "verb"),
    ]
    fp = _compute_fingerprint(tokens)
    # No conjunctions → clause_starts = 1 → compression = 3/1 = 3.0
    assert abs(fp["clause_compression"] - 3.0) < 0.01


def test_fingerprint_all_columns_not_null() -> None:
    """All four fingerprint dimensions must be present and non-None."""
    fp = _compute_fingerprint(PSALM_23_1_TOKENS)
    for key in (
        "syllable_density",
        "morpheme_ratio",
        "sonority_score",
        "clause_compression",
    ):
        assert fp[key] is not None, f"Key '{key}' is None"
        assert isinstance(fp[key], float), f"Key '{key}' is not float"


def test_fingerprint_idempotent() -> None:
    """Running compute_fingerprint twice on the same tokens returns identical dict."""
    fp1 = _compute_fingerprint(PSALM_23_1_TOKENS)
    fp2 = _compute_fingerprint(PSALM_23_1_TOKENS)
    assert fp1 == fp2


def test_count_hebrew_syllables_yehwah() -> None:
    """יְהוָה has 2 full vowels: shewa (half) + qamets → count = 1 full."""
    # hiriq or qamets is the full vowel; shewa is half
    # Expected: 1 full vowel (qamets on ה) + 1 shewa on י
    word = "יְהוָה"
    count = count_hebrew_syllables(word)
    assert count >= 1


def test_count_hebrew_syllables_no_vowels() -> None:
    """Unvocalized word returns 1 (monosyllabic assumption)."""
    assert count_hebrew_syllables("שלם") == 1


def test_onset_sonority_alef() -> None:
    """Alef (א) onset returns the guttural sonority value."""
    score = _onset_sonority("אֶרֶץ")
    assert 0.0 < score <= 1.0
    # Alef is mapped in SONORITY — should match that entry
    alef_val = SONORITY.get("א")
    if alef_val is not None:
        assert abs(score - alef_val) < 0.01


def test_onset_sonority_lamed() -> None:
    """Lamed (ל) onset returns high liquid sonority value >= 0.85."""
    score = _onset_sonority("לֵב")
    assert score >= 0.85
