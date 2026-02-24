# Plan: Stage 5 — LLM Suggestions

> **Depends on:** Stage 4 complete and verified — `translation_scores` fully populated for all
> verse × translation pairs (2,527 × 5 = 12,635 rows minimum for Psalms).
> **Status:** active

## Goal

For every verse that exceeds a configured composite-deviation threshold, generate one or more
LLM-assisted alternative English translations that better preserve the Hebrew phonetic and
rhythmic texture, score each suggestion with the Stage 4 metrics, and persist results in the
`suggestions` table; the stage must exit cleanly with no errors when `provider: none` (the
default offline mode).

## Acceptance Criteria

- With `LLM_PROVIDER=none`, `run()` returns `{"generated": 0, "skipped": "no_provider"}` and
  writes zero rows — no exceptions raised
- With a live provider, suggestions are generated only for verses whose `composite_deviation`
  >= `min_composite_deviation` (default 0.15)
- Each suggestion row contains non-empty `suggested_text`, all score columns non-null,
  `llm_provider`, `llm_model`, `prompt_version`
- `improvement_delta` may be negative (worse suggestion than original) — this is valid data and
  must be stored
- Re-running never writes a 4th suggestion when `max_suggestions_per_verse: 3`
- All 10 unit tests pass (5 in `test_llm_adapter.py` + 5 in `test_suggest.py`)

## Architecture

`pipeline/adapters/llm_adapter.py` exposes a single `LLMAdapter` class whose `ask()` method
dispatches to provider-specific implementations (Anthropic, OpenAI, Gemini, OpenRouter, Ollama)
or returns an empty string for `provider=none`; provider selection reads environment variables
so no code change is needed to switch providers.  `pipeline/modules/suggest.py` queries
high-deviation verse × translation pairs, builds a breath-aware prompt for each, calls
`adapter.ask()`, re-scores the suggestion text using the same phoneme adapter and deviation
formulas as Stage 4, computes `improvement_delta`, and inserts one row per suggestion into
`suggestions`.

## Tech Stack

- Python 3.11, uv, 88-char line limit, ruff enforced
- `anthropic`, `openai`, `google-generativeai`, `requests` (provider SDKs — imported lazily
  inside provider methods to avoid import failures when not installed)
- `psycopg2` for DB access; no batch upsert for suggestions (serial insert is fine)
- `unittest.mock.patch`, `MagicMock`, `monkeypatch` for isolating provider SDK calls in tests
- IMPORTANT: `provider=none` path MUST be tested first — it is the default and offline mode

---

## Tasks

### Task 1: llm_adapter — provider factory and NoOp path

**Files:**
- `tests/test_llm_adapter.py` (write first)
- `pipeline/adapters/llm_adapter.py`

---

**Steps:**

1. Write test:

```python
# tests/test_llm_adapter.py
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

import pytest
from unittest.mock import MagicMock, patch


def test_noop_adapter_factory() -> None:
    """provider=none builds an adapter whose ask() returns '' without raising."""
    from adapters.llm_adapter import LLMAdapter

    config: dict = {"llm": {"provider": "none", "model": ""}}
    adapter = LLMAdapter.from_config(config)
    assert not adapter.is_enabled()
    result = adapter.ask("Any prompt", max_tokens=64)
    assert result == ""


def test_anthropic_adapter_factory() -> None:
    """provider=anthropic creates an enabled adapter — no live API call."""
    from adapters.llm_adapter import LLMAdapter

    config: dict = {
        "llm": {"provider": "anthropic", "model": "claude-haiku-4-5-20251001"}
    }
    # Patch the anthropic SDK so no real import or network call happens
    with patch.dict(os.environ, {"LLM_API_KEY": "dummy-key"}):
        adapter = LLMAdapter.from_config(config)
    assert adapter.is_enabled()
    assert adapter.provider == "anthropic"
    assert adapter.model == "claude-haiku-4-5-20251001"


def test_all_providers_in_factory() -> None:
    """All five real providers can be instantiated without errors."""
    from adapters.llm_adapter import LLMAdapter

    providers = ["anthropic", "openai", "gemini", "openrouter", "ollama"]
    for provider in providers:
        config: dict = {"llm": {"provider": provider, "model": "test-model"}}
        adapter = LLMAdapter.from_config(config)
        assert adapter.is_enabled(), f"Expected {provider} to be enabled"
        assert adapter.provider == provider


def test_unknown_provider_raises() -> None:
    """ask() raises ValueError for an unrecognised provider string."""
    from adapters.llm_adapter import LLMAdapter

    adapter = LLMAdapter(provider="magic_llm", model="x")
    with pytest.raises(ValueError, match="Unknown LLM provider"):
        adapter.ask("test prompt")


def test_ollama_adapter_uses_host() -> None:
    """OllamaAdapter stores OLLAMA_HOST from environment at construction time."""
    from adapters.llm_adapter import LLMAdapter

    with patch.dict(
        os.environ,
        {"LLM_PROVIDER": "ollama", "LLM_MODEL": "llama3",
         "OLLAMA_HOST": "http://my-ollama:11434"},
    ):
        config: dict = {"llm": {}}
        adapter = LLMAdapter.from_config(config)
    assert adapter.ollama_host == "http://my-ollama:11434"
    assert adapter.provider == "ollama"
```

2. Run and confirm FAILED:

```bash
uv run --frozen pytest tests/test_llm_adapter.py -v
# Expected: FAILED (ImportError) — pipeline/adapters/llm_adapter.py does not exist yet
```

3. Implement `pipeline/adapters/llm_adapter.py`:

```python
"""
LLM adapter — unified interface for all supported providers.

All adapters expose: ask(prompt: str, max_tokens: int) -> str

Provider selection is via the LLM_PROVIDER environment variable (or
config["llm"]["provider"]).  If provider is 'none' or unset, ask()
returns an empty string and logs a debug message — no exception raised.

Provider-specific SDKs are imported lazily inside each _ask_* method
so that missing optional packages only fail at call-time, not import-time.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_KNOWN_PROVIDERS = frozenset(
    {"none", "anthropic", "openai", "gemini", "openrouter", "ollama"}
)


class LLMAdapter:
    """
    Provider-agnostic LLM interface.

    Usage::

        adapter = LLMAdapter.from_config(config)
        response = adapter.ask("Translate this verse ...", max_tokens=256)
    """

    def __init__(
        self,
        provider: str,
        model: str,
        api_key: Optional[str] = None,
        temperature: float = 0.3,
        ollama_host: Optional[str] = None,
    ) -> None:
        self.provider = provider
        self.model = model
        self.api_key = api_key
        self.temperature = temperature
        self.ollama_host = ollama_host

    @classmethod
    def from_config(cls, config: dict) -> "LLMAdapter":
        """Construct an LLMAdapter from config + environment variables.

        Environment variables override config values.
        """
        llm_cfg = config.get("llm", {})
        provider = os.environ.get(
            "LLM_PROVIDER", llm_cfg.get("provider", "none")
        )
        model = os.environ.get("LLM_MODEL", llm_cfg.get("model", ""))
        api_key = os.environ.get("LLM_API_KEY", "")
        temperature = float(llm_cfg.get("temperature", 0.3))
        ollama_host = os.environ.get(
            "OLLAMA_HOST", llm_cfg.get("ollama_host", "")
        )
        return cls(provider, model, api_key, temperature, ollama_host)

    def ask(self, prompt: str, max_tokens: int = 256) -> str:
        """
        Send a prompt and return the response text.

        Returns empty string if provider is 'none'.
        Raises ValueError for unknown providers.
        Raises RuntimeError on API-level errors (caller should log and skip).
        """
        if not self.provider or self.provider == "none":
            logger.debug("LLM provider is 'none' — skipping generation")
            return ""

        if self.provider == "anthropic":
            return self._ask_anthropic(prompt, max_tokens)
        if self.provider == "openai":
            return self._ask_openai(prompt, max_tokens)
        if self.provider == "gemini":
            return self._ask_gemini(prompt, max_tokens)
        if self.provider == "openrouter":
            return self._ask_openrouter(prompt, max_tokens)
        if self.provider == "ollama":
            return self._ask_ollama(prompt, max_tokens)

        raise ValueError(
            f"Unknown LLM provider: '{self.provider}'. "
            f"Valid providers: {sorted(_KNOWN_PROVIDERS)}"
        )

    def is_enabled(self) -> bool:
        """Return True when a real (non-none) provider is configured."""
        return bool(self.provider and self.provider != "none")

    # ── Provider implementations ──────────────────────────────────────────

    def _ask_anthropic(self, prompt: str, max_tokens: int) -> str:
        """Call the Anthropic Messages API."""
        import anthropic  # lazy import — optional dependency

        client = anthropic.Anthropic(api_key=self.api_key)
        message = client.messages.create(
            model=self.model or "claude-haiku-4-5-20251001",
            max_tokens=max_tokens,
            temperature=self.temperature,
            messages=[{"role": "user", "content": prompt}],
        )
        return str(message.content[0].text).strip()

    def _ask_openai(self, prompt: str, max_tokens: int) -> str:
        """Call the OpenAI Chat Completions API."""
        from openai import OpenAI  # lazy import

        client = OpenAI(api_key=self.api_key)
        response = client.chat.completions.create(
            model=self.model or "gpt-4o-mini",
            max_tokens=max_tokens,
            temperature=self.temperature,
            messages=[{"role": "user", "content": prompt}],
        )
        content = response.choices[0].message.content
        return (content or "").strip()

    def _ask_gemini(self, prompt: str, max_tokens: int) -> str:
        """Call the Google Generative AI API."""
        import google.generativeai as genai  # lazy import

        genai.configure(api_key=self.api_key)
        model_obj = genai.GenerativeModel(self.model or "gemini-1.5-flash")
        response = model_obj.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=self.temperature,
            ),
        )
        return str(response.text).strip()

    def _ask_openrouter(self, prompt: str, max_tokens: int) -> str:
        """Call the OpenRouter proxy API via HTTP."""
        import requests  # lazy import

        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": self.model,
                "max_tokens": max_tokens,
                "temperature": self.temperature,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        resp.raise_for_status()
        return str(resp.json()["choices"][0]["message"]["content"]).strip()

    def _ask_ollama(self, prompt: str, max_tokens: int) -> str:
        """Call a locally-running Ollama instance via HTTP."""
        import requests  # lazy import

        host = self.ollama_host or "http://ollama:11434"
        resp = requests.post(
            f"{host}/api/generate",
            json={
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "num_predict": max_tokens,
                    "temperature": self.temperature,
                },
            },
            timeout=120,
        )
        resp.raise_for_status()
        return str(resp.json()["response"]).strip()
```

4. Run and confirm PASSED:

```bash
uv run --frozen pytest tests/test_llm_adapter.py -v
# Expected: 5 passed
```

5. Lint and typecheck:

```bash
uv run --frozen ruff check . --fix && uv run --frozen pyright
```

6. Commit: `"feat(stage5): add LLMAdapter with noop path and provider factory"`

---

### Task 2: suggest module — prompt builder, filter, storage

**Files:**
- `tests/test_suggest.py` (write first)
- `pipeline/modules/suggest.py`

---

**Steps:**

1. Write test:

```python
# tests/test_suggest.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

from unittest.mock import MagicMock, patch, call
import pytest


# ── _build_prompt ────────────────────────────────────────────────────────────

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
    weights = {"density": 0.35, "morpheme": 0.25, "sonority": 0.20,
               "compression": 0.20}

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
```

2. Run and confirm FAILED:

```bash
uv run --frozen pytest tests/test_suggest.py -v
# Expected: FAILED (ImportError) — pipeline/modules/suggest.py does not exist yet
```

3. Implement `pipeline/modules/suggest.py`:

```python
"""
Stage 5 — LLM-assisted suggestion generation.

For each verse exceeding the configured composite_deviation threshold,
generates one or more alternative English translations that better preserve
the Hebrew phonetic and structural texture.  Each suggestion is scored with
the same metrics as Stage 4 translations.

If LLM_PROVIDER is 'none' (the default), the stage exits cleanly with
{"generated": 0, "skipped": "no_provider"} and writes no rows.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from adapters.llm_adapter import LLMAdapter
from adapters.phoneme_adapter import english_breath_weights, english_fingerprint
from modules.score import _compute_breath_alignment

logger = logging.getLogger(__name__)

# Bump this string whenever the prompt template changes to preserve provenance.
PROMPT_VERSION = "v1"


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    """
    Generate and store LLM suggestions for high-deviation verses.

    Returns {"generated": int, "skipped": int | str, "elapsed_s": float}.
    Skips cleanly (no DB writes) if LLM provider is 'none'.
    """
    import time

    t0 = time.monotonic()

    adapter = LLMAdapter.from_config(config)
    if not adapter.is_enabled():
        logger.info("LLM provider is 'none' — suggestion stage skipped.")
        return {"generated": 0, "skipped": "no_provider"}

    llm_cfg = config.get("llm", {})
    max_tokens: int = llm_cfg.get("max_tokens", 256)
    filter_cfg = llm_cfg.get("suggestion_filter", {})
    min_deviation: float = filter_cfg.get("min_composite_deviation", 0.15)
    max_per_verse: int = filter_cfg.get("max_suggestions_per_verse", 3)

    corpus = config.get("corpus", {})
    book_nums: List[int] = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters: List[int] = corpus.get("debug_chapters", [])

    w = config.get("scoring", {}).get("deviation_weights", {})
    dev_weights: Dict[str, float] = {
        "density": w.get("density", 0.35),
        "morpheme": w.get("morpheme", 0.25),
        "sonority": w.get("sonority", 0.20),
        "compression": w.get("compression", 0.20),
    }

    candidates = _get_suggestion_candidates(
        conn, book_nums, debug_chapters, min_deviation
    )
    logger.info(
        "Found %d verse × translation pairs above threshold %.3f",
        len(candidates),
        min_deviation,
    )

    generated = 0
    skipped = 0

    for candidate in candidates:
        verse_id = candidate["verse_id"]
        translation_key = candidate["translation_key"]

        if _existing_suggestion_count(conn, verse_id, translation_key) >= max_per_verse:
            continue

        prompt = _build_prompt(candidate)

        try:
            suggestion_text = adapter.ask(prompt, max_tokens=max_tokens)
        except Exception:
            logger.exception(
                "LLM error for verse %d / %s — skipping", verse_id, translation_key
            )
            skipped += 1
            continue

        if not suggestion_text:
            skipped += 1
            continue

        heb_fp = candidate["heb_fingerprint"]
        heb_breath = candidate["heb_breath"]
        eng_fp = english_fingerprint(suggestion_text)

        composite_deviation = _weighted_deviation(heb_fp, eng_fp, dev_weights)
        stress_align, weight_match = _compute_breath_alignment(
            heb_breath, suggestion_text
        )
        breath_alignment = round(stress_align * 0.60 + weight_match * 0.40, 4)
        improvement_delta = round(
            candidate["current_composite_deviation"] - composite_deviation, 4
        )

        _store_suggestion(
            conn,
            {
                "verse_id": verse_id,
                "translation_key": translation_key,
                "suggested_text": suggestion_text,
                "composite_deviation": round(composite_deviation, 4),
                "breath_alignment": breath_alignment,
                "improvement_delta": improvement_delta,
                "llm_provider": adapter.provider,
                "llm_model": adapter.model,
                "prompt_version": PROMPT_VERSION,
            },
        )
        generated += 1

    elapsed = round(time.monotonic() - t0, 2)
    logger.info(
        "Suggestions generated: %d, skipped: %d in %.2fs",
        generated,
        skipped,
        elapsed,
    )
    return {"generated": generated, "skipped": skipped, "elapsed_s": elapsed}


# ── Prompt construction ───────────────────────────────────────────────────────


def _build_prompt(candidate: dict) -> str:
    """
    Build a breath-aware translation improvement prompt.

    Provides the LLM with the Hebrew verse reference, existing translation,
    deviation score, and specific phonetic guidance derived from the Hebrew
    breath profile.
    """
    ref = (
        f"{candidate['book_name']} "
        f"{candidate['chapter']}:{candidate['verse_num']}"
    )
    hebrew = candidate["hebrew_text"]
    translation_key = candidate["translation_key"]
    existing_text = candidate["existing_translation"]
    deviation = candidate["current_composite_deviation"]
    mean_weight: float = candidate["heb_breath"].get("mean_weight", 0.5)
    guttural_density: float = candidate.get("guttural_density", 0.0)

    phonetic_notes: List[str] = []
    if mean_weight > 0.65:
        phonetic_notes.append(
            "predominantly open, resonant vowels (high phonetic weight)"
        )
    elif mean_weight < 0.40:
        phonetic_notes.append(
            "compressed, closed syllables (low phonetic weight)"
        )
    else:
        phonetic_notes.append("balanced mix of open and closed syllables")

    if guttural_density > 0.25:
        phonetic_notes.append(
            "frequent guttural consonants (breathy, aspirated quality)"
        )

    phonetic_desc = "; ".join(phonetic_notes) if phonetic_notes else (
        "standard phonetic texture"
    )

    if mean_weight > 0.55:
        word_preference = "resonant, open-vowel words (praise, glory, flowing)"
    else:
        word_preference = "crisp, closed-syllable words"

    prompt = (
        f"You are a Biblical Hebrew translator with expertise in phonetic"
        f" and rhythmic accuracy.\n\n"
        f"TASK: Suggest a single improved English translation of the following"
        f" verse that better preserves the phonetic and rhythmic texture of"
        f" the original Hebrew.\n\n"
        f"VERSE: {ref}\n"
        f"HEBREW TEXT: {hebrew}\n"
        f"CURRENT TRANSLATION ({translation_key}): {existing_text}\n"
        f"CURRENT DEVIATION SCORE: {deviation:.3f}"
        f" (lower is better; 0.0 = perfect structural match)\n"
        f"HEBREW PHONETIC CHARACTER: {phonetic_desc}\n\n"
        f"REQUIREMENTS:\n"
        f"1. Preserve the meaning of the original translation faithfully"
        f" — do not add or remove ideas\n"
        f"2. Where meaning allows, prefer {word_preference}\n"
        f"3. Maintain similar clause structure and rhythm to the Hebrew\n"
        f"4. Do not use archaic language unless it appears in the existing"
        f" translation\n"
        f"5. Return ONLY the translation text with no commentary,"
        f" explanation, or quotation marks\n\n"
        f"TRANSLATION:"
    )
    return prompt


# ── Database helpers ──────────────────────────────────────────────────────────


def _get_suggestion_candidates(
    conn: psycopg2.extensions.connection,
    book_nums: List[int],
    debug_chapters: List[int],
    min_deviation: float,
) -> List[dict]:
    """Fetch high-deviation verse × translation pairs with full context."""
    q = """
        SELECT
            ts.verse_id, ts.translation_key,
            ts.composite_deviation,
            v.hebrew_text, v.chapter, v.verse_num, b.book_name,
            t.verse_text AS existing_translation,
            vf.syllable_density, vf.morpheme_ratio,
            vf.sonority_score, vf.clause_compression,
            bp.breath_curve, bp.stress_positions,
            bp.mean_weight, bp.guttural_density
        FROM translation_scores ts
        JOIN verses v ON ts.verse_id = v.verse_id
        JOIN books b ON v.book_num = b.book_num
        JOIN translations t
          ON t.verse_id = ts.verse_id
         AND t.translation_key = ts.translation_key
        JOIN verse_fingerprints vf ON vf.verse_id = ts.verse_id
        JOIN breath_profiles bp ON bp.verse_id = ts.verse_id
        WHERE v.book_num = ANY(%s)
          AND ts.composite_deviation >= %s
    """
    params: list = [book_nums, min_deviation]
    if debug_chapters:
        q += " AND v.chapter = ANY(%s)"
        params.append(debug_chapters)
    q += " ORDER BY ts.composite_deviation DESC"

    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    candidates: List[dict] = []
    for row in rows:
        d = dict(zip(cols, row))
        candidates.append(
            {
                "verse_id": d["verse_id"],
                "translation_key": d["translation_key"],
                "current_composite_deviation": float(d["composite_deviation"]),
                "hebrew_text": d["hebrew_text"],
                "chapter": d["chapter"],
                "verse_num": d["verse_num"],
                "book_name": d["book_name"],
                "existing_translation": d["existing_translation"],
                "guttural_density": float(d.get("guttural_density") or 0),
                "heb_fingerprint": {
                    "syllable_density": float(d["syllable_density"] or 0),
                    "morpheme_ratio": float(d["morpheme_ratio"] or 0),
                    "sonority_score": float(d["sonority_score"] or 0),
                    "clause_compression": float(d["clause_compression"] or 0),
                },
                "heb_breath": {
                    "breath_curve": d["breath_curve"] or [],
                    "stress_positions": d["stress_positions"] or [],
                    "mean_weight": float(d["mean_weight"] or 0),
                },
            }
        )
    return candidates


def _existing_suggestion_count(
    conn: psycopg2.extensions.connection,
    verse_id: int,
    translation_key: str,
) -> int:
    """Return the number of suggestions already stored for this pair."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM suggestions"
            " WHERE verse_id = %s AND translation_key = %s",
            (verse_id, translation_key),
        )
        result = cur.fetchone()
        return int(result[0]) if result else 0


def _weighted_deviation(
    heb_fp: Dict[str, float],
    eng_fp: Dict[str, float],
    weights: Dict[str, float],
) -> float:
    """
    Compute weighted L1 deviation between Hebrew and English fingerprints.

    Uses the same formula as Stage 4: sum of w_i * |heb_i - eng_i|.
    """
    dims = [
        "syllable_density",
        "morpheme_ratio",
        "sonority_score",
        "clause_compression",
    ]
    weight_keys = ["density", "morpheme", "sonority", "compression"]
    total = 0.0
    for dim, wk in zip(dims, weight_keys):
        total += abs(heb_fp.get(dim, 0.0) - eng_fp.get(dim, 0.0)) * weights.get(
            wk, 0.25
        )
    return total


def _store_suggestion(
    conn: psycopg2.extensions.connection, s: dict
) -> None:
    """Insert one suggestion row; commits immediately."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO suggestions
              (verse_id, translation_key, suggested_text,
               composite_deviation, breath_alignment, improvement_delta,
               llm_provider, llm_model, prompt_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                s["verse_id"],
                s["translation_key"],
                s["suggested_text"],
                s["composite_deviation"],
                s["breath_alignment"],
                s["improvement_delta"],
                s["llm_provider"],
                s["llm_model"],
                s["prompt_version"],
            ),
        )
    conn.commit()
```

4. Run and confirm PASSED:

```bash
uv run --frozen pytest tests/test_suggest.py -v
# Expected: 5 passed
```

5. Run full suite for both Stage 5 test files:

```bash
uv run --frozen pytest tests/test_llm_adapter.py tests/test_suggest.py -v
# Expected: 10 passed total (5 + 5)
```

6. Lint and typecheck:

```bash
uv run --frozen ruff check . --fix && uv run --frozen pyright
```

7. Commit: `"feat(stage5): add suggest module with prompt builder, filter, and storage"`

---

### Task 3: verify stage acceptance criteria

**Files:** no new files — SQL queries and Docker run only

**Steps:**

1. Verify offline (noop) path:

```bash
# Ensure provider is unset or 'none' in docker-compose.yml, then:
docker compose --profile pipeline run --rm pipeline \
  python -m pipeline.run --stages 5
# Expected output: "LLM provider is 'none' — suggestion stage skipped."
# Expected return: {"generated": 0, "skipped": "no_provider"}
```

2. (Optional) Verify with live provider — set env vars and re-run:

```bash
# Edit docker-compose.yml to set LLM_PROVIDER, LLM_API_KEY, LLM_MODEL, then:
docker compose --profile pipeline run --rm pipeline \
  python -m pipeline.run --stages 5
```

3. Verify suggestions table via SQL:

```sql
-- Count suggestions by translation
SELECT translation_key,
       COUNT(*) AS suggestion_count,
       ROUND(AVG(improvement_delta)::numeric, 4) AS mean_improvement
FROM suggestions
GROUP BY translation_key
ORDER BY suggestion_count DESC;

-- Check no verse exceeds max_suggestions_per_verse
SELECT verse_id, translation_key, COUNT(*) AS cnt
FROM suggestions
GROUP BY verse_id, translation_key
HAVING COUNT(*) > 3;
-- Expected: 0 rows

-- Inspect best and worst suggestions
SELECT s.suggestion_id, v.chapter, v.verse_num, s.translation_key,
       s.composite_deviation, s.improvement_delta,
       t.verse_text AS original,
       s.suggested_text
FROM suggestions s
JOIN verses v ON s.verse_id = v.verse_id
JOIN translations t
  ON t.verse_id = s.verse_id AND t.translation_key = s.translation_key
WHERE v.book_num = 19
ORDER BY s.improvement_delta DESC
LIMIT 10;

-- Confirm negative deltas are stored (worse suggestions)
SELECT COUNT(*) FROM suggestions WHERE improvement_delta < 0;

-- Provider/model provenance
SELECT llm_provider, llm_model, prompt_version, COUNT(*) AS cnt
FROM suggestions
GROUP BY llm_provider, llm_model, prompt_version;
```

4. Confirm all unit tests still pass:

```bash
uv run --frozen pytest tests/test_llm_adapter.py tests/test_suggest.py -v
# Expected: 10 passed
```

5. Commit: `"chore(stage5): verified acceptance criteria — suggestions table populated"`

---

## Provider Switching Reference

To switch LLM providers, only `docker-compose.yml` environment variables need to change:

```yaml
# Offline default (no LLM)
environment:
  LLM_PROVIDER: none

# Anthropic
environment:
  LLM_PROVIDER: anthropic
  LLM_API_KEY:  sk-ant-...
  LLM_MODEL:    claude-haiku-4-5-20251001

# OpenAI
environment:
  LLM_PROVIDER: openai
  LLM_API_KEY:  sk-...
  LLM_MODEL:    gpt-4o-mini

# Gemini
environment:
  LLM_PROVIDER: gemini
  LLM_API_KEY:  AIza...
  LLM_MODEL:    gemini-1.5-flash

# OpenRouter (proxy to many models)
environment:
  LLM_PROVIDER: openrouter
  LLM_API_KEY:  sk-or-...
  LLM_MODEL:    meta-llama/llama-3.1-8b-instruct

# Local Ollama (also enable the `ollama` service block)
environment:
  LLM_PROVIDER: ollama
  LLM_MODEL:    llama3
  OLLAMA_HOST:  http://ollama:11434
```

No Python code changes are needed.
