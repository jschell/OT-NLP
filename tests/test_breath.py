# tests/test_breath.py
"""Tests for Stage 3 breath analysis module.

All tests are pure-logic unit tests. No DB connection required.
Primary fixture: Psalm 23:1 — יְהוָה רֹעִי לֹא אֶחְסָר

Run with:
    uv run --frozen pytest tests/test_breath.py -v
"""

from __future__ import annotations

from modules.breath import (
    DISJUNCTIVE_ACCENTS,
    FULL_VOWELS,
    GUTTURALS,
    HALF_VOWELS,
    _compute_breath_weight,
    _vowel_length_score,
    parse_syllables,
)

# Psalm 23:1 Hebrew text (niqqud included)
PSALM_23_1_WORDS = [
    "יְהוָה",  # The LORD
    "רֹעִי",   # is my shepherd
    "לֹא",     # not
    "אֶחְסָר",  # I shall want
]


def test_breath_weight_range() -> None:
    """All breath weights for Psalm 23:1 must be in [0.0, 1.0]."""
    for word in PSALM_23_1_WORDS:
        for syl in parse_syllables(word):
            w = syl["breath_weight"]
            assert 0.0 <= w <= 1.0, (
                f"breath_weight={w} out of range for word '{word}'"
            )


def test_colon_boundary_detected() -> None:
    """Etnahta (U+0591) in a word surface_form signals a colon boundary."""
    # Construct a word string with etnahta embedded
    word_with_etnahta = "יְהוָ\u0591ה"
    assert any(c in DISJUNCTIVE_ACCENTS for c in word_with_etnahta)


def test_guttural_density_alef() -> None:
    """Word starting with alef (א) has guttural onset class."""
    word = "אֶרֶץ"  # land/earth
    syls = parse_syllables(word)
    assert len(syls) >= 1
    assert syls[0]["onset_class"] == "guttural", (
        f"Expected 'guttural' for alef onset, got '{syls[0]['onset_class']}'"
    )


def test_mean_weight_psalm_23_1() -> None:
    """Mean breath weight across all syllables of Psalm 23:1 must be in [0.3, 0.8]."""
    all_weights = []
    for word in PSALM_23_1_WORDS:
        for syl in parse_syllables(word):
            all_weights.append(syl["breath_weight"])
    assert len(all_weights) > 0
    mean = sum(all_weights) / len(all_weights)
    assert 0.3 <= mean <= 0.8, f"Mean breath weight {mean:.3f} outside [0.3, 0.8]"


def test_colon_count_minimum() -> None:
    """Every verse processed should have colon_count >= 1.

    Verified by checking that _process_verse returns a profile with
    colon_count >= 1 even for a single-word verse-like token list.
    """
    from modules.breath import _process_verse

    # Minimal token list: (verse_id, token_id, position, surface_form, pos)
    tokens = [(1, 101, 1, "יְהוָה", "proper_noun")]
    _syllable_rows, profile = _process_verse(tokens)
    # profile tuple: (mean_weight, open_ratio, guttural_density,
    #                  colon_count, colon_boundaries, stress_positions, breath_curve)
    colon_count = profile[3]
    assert colon_count >= 1, f"colon_count={colon_count} must be >= 1"


def test_colon_fingerprints_backpopulated() -> None:
    """After _backpopulate_colon_fingerprints runs, verse_fingerprints rows
    get non-empty colon_fingerprints.

    Uses mocked DB to confirm the UPDATE is issued with non-empty JSON.
    """
    import json
    from unittest.mock import MagicMock

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Simulate aggregated syllable data from DB query
    cur.fetchall.return_value = [
        (1, 1, 0.75, 0.60, 5),  # verse_id=1, colon_idx=1, openness, weight, count
        (1, 2, 0.65, 0.55, 4),  # verse_id=1, colon_idx=2
    ]

    from modules.breath import _backpopulate_colon_fingerprints

    _backpopulate_colon_fingerprints(conn, [19])

    # executemany must have been called with data
    cur.executemany.assert_called_once()
    call_args = cur.executemany.call_args
    update_rows = call_args[0][1]
    assert len(update_rows) == 1  # one verse_id
    json_str, verse_id = update_rows[0]
    parsed = json.loads(json_str)
    assert isinstance(parsed, list)
    assert len(parsed) == 2  # two colons
    assert parsed[0]["colon"] == 1


def test_breath_idempotent() -> None:
    """Running parse_syllables twice on same word produces identical results."""
    word = "אֱלֹהִים"
    result1 = parse_syllables(word)
    result2 = parse_syllables(word)
    assert len(result1) == len(result2)
    for s1, s2 in zip(result1, result2, strict=True):
        assert s1["breath_weight"] == s2["breath_weight"]
        assert s1["onset_class"] == s2["onset_class"]


# --- Auxiliary tests for helper functions ---


def test_vowel_length_score_long_is_max() -> None:
    """Long vowels must return the maximum length score (1.0)."""
    assert _vowel_length_score("long") == 1.0


def test_vowel_length_score_ordering() -> None:
    """Vowel length scores must be ordered: long > short > ultra-short."""
    assert _vowel_length_score("long") > _vowel_length_score("short")
    assert _vowel_length_score("short") > _vowel_length_score("ultra-short")


def test_vowel_length_score_unknown_is_midrange() -> None:
    """Unknown or None vowel length must return a valid midrange value."""
    score = _vowel_length_score(None)
    assert 0.0 < score < 1.0


def test_full_vowels_has_qamets() -> None:
    """FULL_VOWELS dict must contain qamets (U+05B8)."""
    assert "\u05B8" in FULL_VOWELS


def test_full_vowels_dagesh_has_no_openness() -> None:
    """Dagesh (U+05BC) entry must have None openness (it is not a vowel)."""
    entry = FULL_VOWELS.get("\u05BC")
    assert entry is not None
    assert entry[2] is None  # third element = openness = None for dagesh


def test_half_vowels_has_shewa() -> None:
    """HALF_VOWELS dict must contain shewa (U+05B0)."""
    assert "\u05B0" in HALF_VOWELS


def test_disjunctive_accents_has_etnahta() -> None:
    """DISJUNCTIVE_ACCENTS must include etnahta (U+0591)."""
    assert "\u0591" in DISJUNCTIVE_ACCENTS


def test_disjunctive_accents_has_sof_pasuq() -> None:
    """DISJUNCTIVE_ACCENTS must include sof pasuq (U+05C3)."""
    assert "\u05C3" in DISJUNCTIVE_ACCENTS


def test_disjunctive_accents_minimum_size() -> None:
    """DISJUNCTIVE_ACCENTS must have at least 8 members."""
    assert len(DISJUNCTIVE_ACCENTS) >= 8


def test_gutturals_contains_all_four() -> None:
    """GUTTURALS must contain alef, he, het, ayin."""
    for ch in "אהחע":
        assert ch in GUTTURALS, f"Guttural '{ch}' missing from GUTTURALS set"


def test_parse_syllables_returns_nonempty() -> None:
    """parse_syllables must return at least one syllable for any input."""
    for word in ["א", "כ", "שלום", "אֱלֹהִים", "יְהוָה"]:
        result = parse_syllables(word)
        assert len(result) >= 1, f"No syllables returned for '{word}'"


def test_parse_syllables_all_required_keys() -> None:
    """Every syllable dict must contain all required keys."""
    required = {
        "syl_idx", "text", "nucleus_vowel", "vowel_openness",
        "vowel_length", "is_open", "onset_class",
        "breath_weight", "is_stressed",
    }
    for word in PSALM_23_1_WORDS:
        for syl in parse_syllables(word):
            missing = required - syl.keys()
            assert not missing, (
                f"Syllable for '{word}' missing keys: {missing}"
            )


def test_parse_syllables_breath_weight_is_float() -> None:
    """breath_weight must be a float (not int or None)."""
    for word in PSALM_23_1_WORDS:
        for syl in parse_syllables(word):
            assert isinstance(syl["breath_weight"], float), (
                f"breath_weight type={type(syl['breath_weight'])} for '{word}'"
            )


def test_compute_breath_weight_uses_all_components() -> None:
    """_compute_breath_weight must respect the 4-component formula."""
    # Full open syllable with qamets (openness=1.0, long) and liquid onset
    w = _compute_breath_weight(
        vowel_openness=1.0,
        vowel_length="long",
        is_open=True,
        onset_weight=0.90,
    )
    # Minimum expected: 0.40*1.0 + 0.25*1.0 + 0.20*1.0 + 0.15*0.90 = 0.985
    assert w >= 0.98
    assert w <= 1.0


def test_compute_breath_weight_shewa_is_low() -> None:
    """A shewa syllable with stop onset should produce low breath weight."""
    w = _compute_breath_weight(
        vowel_openness=0.10,
        vowel_length="ultra-short",
        is_open=False,
        onset_weight=0.20,
    )
    # 0.40*0.10 + 0.25*0.25 + 0.20*0.4 + 0.15*0.20 = 0.04+0.0625+0.08+0.03 = 0.2125
    assert w < 0.35
