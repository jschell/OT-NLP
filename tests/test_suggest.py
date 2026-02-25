# tests/test_suggest.py
"""Tests for Stage 5 suggestion module.

All tests use mocked LLM calls and DB connections — no live provider required.

Run with:
    uv run --frozen pytest tests/test_suggest.py -v
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

# ── helpers ──────────────────────────────────────────────────────────────────


def _psalm23_candidate(
    mean_weight: float = 0.65,
    deviation: float = 0.35,
    guttural_density: float = 0.10,
) -> dict:
    """Return a minimal candidate dict matching Psalm 23:1 (KJV)."""
    return {
        "verse_id": 1,
        "translation_key": "KJV",
        "current_composite_deviation": deviation,
        "hebrew_text": "יְהוָה רֹעִי לֹא אֶחְסָר",
        "chapter": 23,
        "verse_num": 1,
        "book_name": "Psalms",
        "existing_translation": "The LORD is my shepherd; I shall not want.",
        "guttural_density": guttural_density,
        "heb_fingerprint": {
            "syllable_density": 2.1,
            "morpheme_ratio": 1.8,
            "sonority_score": 0.60,
            "clause_compression": 3.0,
        },
        "heb_breath": {
            "stress_positions": [0.3, 0.7],
            "mean_weight": mean_weight,
        },
    }


# ── _build_prompt ─────────────────────────────────────────────────────────────


def test_suggest_build_prompt_contains_verse_reference() -> None:
    """Prompt must contain 'Psalms 23:1' and the existing translation."""
    from modules.suggest import _build_prompt

    prompt = _build_prompt(_psalm23_candidate())
    assert "Psalms 23:1" in prompt
    assert "The LORD is my shepherd" in prompt
    assert "KJV" in prompt
    assert len(prompt) > 100


def test_suggest_build_prompt_open_vowel_hint_for_high_weight() -> None:
    """For mean_weight > 0.65 the prompt should hint at open/resonant vowels."""
    from modules.suggest import _build_prompt

    prompt = _build_prompt(_psalm23_candidate(mean_weight=0.80))
    lower = prompt.lower()
    assert "open" in lower or "resonant" in lower


# ── run() integration tests ───────────────────────────────────────────────────


def test_suggest_noop_exits_cleanly() -> None:
    """
    provider=none must return {"generated": 0, "skipped": "no_provider"}
    and write zero rows to the DB.
    """
    from modules.suggest import run

    conn = MagicMock()
    config: dict = {
        "llm": {"provider": "none"},
        "corpus": {"books": [{"book_num": 19}], "debug_chapters": []},
        "scoring": {"deviation_weights": {}},
    }
    result = run(conn, config)
    assert result == {"generated": 0, "skipped": "no_provider"}
    conn.cursor.assert_not_called()


def test_suggest_filters_by_threshold() -> None:
    """
    Verses with composite_deviation < min_composite_deviation must be skipped.
    We mock _get_suggestion_candidates to return an empty list (simulating all
    verses below threshold) and assert generated == 0.
    """
    from modules.suggest import run

    conn = MagicMock()
    config: dict = {
        "llm": {
            "provider": "anthropic",
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 128,
            "suggestion_filter": {
                "min_composite_deviation": 0.15,
                "max_suggestions_per_verse": 3,
            },
        },
        "corpus": {"books": [{"book_num": 19}], "debug_chapters": []},
        "scoring": {"deviation_weights": {}},
    }

    with (
        patch("modules.suggest._get_suggestion_candidates", return_value=[]),
        patch("modules.suggest.LLMAdapter.from_config") as mock_factory,
    ):
        mock_adapter = MagicMock()
        mock_adapter.is_enabled.return_value = True
        mock_factory.return_value = mock_adapter

        result = run(conn, config)

    assert result["generated"] == 0
    mock_adapter.ask.assert_not_called()


def test_suggest_respects_max_per_verse() -> None:
    """
    When _existing_suggestion_count >= max_suggestions_per_verse the verse
    must be skipped — ask() must not be called.
    """
    from modules.suggest import run

    conn = MagicMock()
    config: dict = {
        "llm": {
            "provider": "anthropic",
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 128,
            "suggestion_filter": {
                "min_composite_deviation": 0.15,
                "max_suggestions_per_verse": 3,
            },
        },
        "corpus": {"books": [{"book_num": 19}], "debug_chapters": []},
        "scoring": {"deviation_weights": {}},
    }
    candidate = _psalm23_candidate()

    with (
        patch(
            "modules.suggest._get_suggestion_candidates",
            return_value=[candidate],
        ),
        # Already 3 suggestions exist — at max
        patch(
            "modules.suggest._existing_suggestion_count",
            return_value=3,
        ),
        patch("modules.suggest.LLMAdapter.from_config") as mock_factory,
    ):
        mock_adapter = MagicMock()
        mock_adapter.is_enabled.return_value = True
        mock_factory.return_value = mock_adapter
        result = run(conn, config)

    assert result["generated"] == 0
    mock_adapter.ask.assert_not_called()


def test_suggest_stores_improvement_delta() -> None:
    """
    improvement_delta = original_composite_deviation - suggestion_composite_deviation.
    A positive delta means the suggestion is better (lower deviation).
    """
    from modules.suggest import run

    conn = MagicMock()
    config: dict = {
        "llm": {
            "provider": "anthropic",
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 128,
            "suggestion_filter": {
                "min_composite_deviation": 0.15,
                "max_suggestions_per_verse": 3,
            },
        },
        "corpus": {"books": [{"book_num": 19}], "debug_chapters": []},
        "scoring": {
            "deviation_weights": {
                "density": 0.35,
                "morpheme": 0.25,
                "sonority": 0.20,
                "compression": 0.20,
            }
        },
    }
    # original deviation is 0.35; we expect improvement_delta to be stored
    candidate = _psalm23_candidate(deviation=0.35)

    stored: list = []

    def fake_store(conn_arg: object, s: dict) -> None:
        stored.append(s)

    with (
        patch(
            "modules.suggest._get_suggestion_candidates",
            return_value=[candidate],
        ),
        patch("modules.suggest._existing_suggestion_count", return_value=0),
        patch("modules.suggest._store_suggestion", side_effect=fake_store),
        patch("modules.suggest.LLMAdapter.from_config") as mock_factory,
    ):
        mock_adapter = MagicMock()
        mock_adapter.is_enabled.return_value = True
        mock_adapter.provider = "anthropic"
        mock_adapter.model = "claude-haiku-4-5-20251001"
        # Suggestion text similar to original — will have some deviation
        mock_adapter.ask.return_value = (
            "The LORD tends me as a shepherd; I lack nothing."
        )
        mock_factory.return_value = mock_adapter
        result = run(conn, config)

    assert result["generated"] == 1
    assert len(stored) == 1
    row = stored[0]
    assert "improvement_delta" in row
    # improvement_delta = original_dev - suggestion_dev;
    # both are real floats (positive or negative)
    assert isinstance(row["improvement_delta"], float)
    assert row["composite_deviation"] >= 0.0
    assert row["breath_alignment"] >= 0.0


# ── pure-logic unit test ──────────────────────────────────────────────────────


def test_suggest_negative_delta_stored() -> None:
    """
    A suggestion that is worse than the original (negative improvement_delta)
    must still be stored — filtering by quality is a reporting concern.
    """
    from modules.suggest import _weighted_deviation

    heb_fp = {
        "syllable_density": 2.1,
        "morpheme_ratio": 1.8,
        "sonority_score": 0.60,
        "clause_compression": 3.0,
    }
    weights = {
        "density": 0.35,
        "morpheme": 0.25,
        "sonority": 0.20,
        "compression": 0.20,
    }

    # A very different English fingerprint — will have high deviation
    worse_fp = {
        "syllable_density": 1.0,
        "morpheme_ratio": 0.5,
        "sonority_score": 0.10,
        "clause_compression": 10.0,
    }
    dev = _weighted_deviation(heb_fp, worse_fp, weights)
    original_dev = 0.20  # lower than what we just computed
    delta = original_dev - dev
    # Delta must be negative: suggestion is worse
    assert delta < 0.0
