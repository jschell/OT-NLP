# tests/test_config.py
"""Verify config.yml is valid YAML and contains all required top-level sections."""

from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent.parent / "pipeline" / "config.yml"

REQUIRED_SECTIONS = [
    "pipeline",
    "corpus",
    "bhsa",
    "translations",
    "fingerprint",
    "breath",
    "chiasm",
    "scoring",
    "llm",
    "export",
]


def test_config_file_exists() -> None:
    """pipeline/config.yml must exist."""
    assert CONFIG_PATH.exists(), f"config.yml not found at {CONFIG_PATH}"


def test_config_is_valid_yaml() -> None:
    """config.yml must parse without error."""
    data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "config.yml root must be a mapping"


def test_config_has_required_sections() -> None:
    """config.yml must contain all required top-level sections."""
    data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    for section in REQUIRED_SECTIONS:
        assert section in data, f"Missing required section: '{section}'"


def test_config_corpus_has_psalms() -> None:
    """corpus.books must include Psalms (book_num=19)."""
    data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    book_nums = [b["book_num"] for b in data["corpus"]["books"]]
    assert 19 in book_nums, "Psalms (book_num=19) missing from corpus.books"


def test_config_translations_have_kjv() -> None:
    """translations.sources must include a KJV entry."""
    data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    ids = [s["id"] for s in data["translations"]["sources"]]
    assert "KJV" in ids, f"KJV not in translation sources: {ids}"


def test_config_llm_provider_default() -> None:
    """llm.provider must default to 'none' for fully offline operation."""
    data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    assert data["llm"]["provider"] == "none"
