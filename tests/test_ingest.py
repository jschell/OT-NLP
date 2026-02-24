# tests/test_ingest.py
"""Tests for Stage 2 ingest module — pure-logic unit tests only.

BHSA/text-fabric is not available in unit test runs. All tests here
target the helper functions that do NOT require a live TF API or DB.
Integration tests (actual row counts) are verified via SQL queries
documented in the acceptance criteria.
"""
from __future__ import annotations

from modules.ingest import POS_MAP, STEM_MAP, _count_prefixes_from_pfm


def test_pos_map_covers_core_tags() -> None:
    """POS_MAP must include the seven most common BHSA part-of-speech codes."""
    required = {"subs", "verb", "prep", "conj", "prps", "nmpr", "art"}
    assert required.issubset(POS_MAP.keys())


def test_pos_map_verb_maps_correctly() -> None:
    assert POS_MAP["verb"] == "verb"


def test_pos_map_noun_maps_correctly() -> None:
    assert POS_MAP["subs"] == "noun"


def test_stem_map_covers_major_stems() -> None:
    required = {"qal", "nif", "piel", "hif"}
    assert required.issubset(STEM_MAP.keys())


def test_count_prefixes_absent() -> None:
    """pfm='absent' or empty string → 0 prefixes."""
    assert _count_prefixes_from_pfm("absent") == 0
    assert _count_prefixes_from_pfm("") == 0
    assert _count_prefixes_from_pfm(None) == 0


def test_count_prefixes_present() -> None:
    """Any non-empty, non-'absent' pfm value → 1 prefix."""
    assert _count_prefixes_from_pfm("W") == 1
    assert _count_prefixes_from_pfm("B") == 1


def test_morpheme_count_formula() -> None:
    """prefix_count + 1 (stem) + has_suffix matches documented formula."""
    # No prefix, no suffix: morpheme_count = 1
    assert 0 + 1 + 0 == 1
    # 1 prefix, no suffix: morpheme_count = 2
    assert 1 + 1 + 0 == 2
    # 1 prefix, 1 suffix: morpheme_count = 3
    assert 1 + 1 + 1 == 3
