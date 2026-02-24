# Stage 5 — LLM Integration & Suggestions
## Detailed Implementation Plan

> **Depends on:** Stage 4 (`translation_scores` populated)  
> **Produces:** `suggestions` table populated for verses that exceed the configured deviation threshold; pipeline continues gracefully if no LLM is configured  
> **Estimated time:** Variable — depends on LLM provider and number of qualifying verses

---

## Objectives

1. Implement `adapters/llm_adapter.py` with a unified `ask()` interface covering all supported providers
2. Implement `modules/suggest.py` with breath-aware prompt construction and suggestion storage
3. Score each suggestion with the same metrics as Stage 4 translations
4. Ensure full graceful degradation: if `provider: none`, the stage logs a skip and exits cleanly

---

## Supported Providers

| Provider | Type | Config requirement |
|---|---|---|
| `none` | Disabled | Default — stage skips cleanly |
| `anthropic` | Cloud API | `LLM_API_KEY` env var, `LLM_MODEL` (e.g. `claude-haiku-4-5-20251001`) |
| `openai` | Cloud API | `LLM_API_KEY` env var, `LLM_MODEL` (e.g. `gpt-4o-mini`) |
| `gemini` | Cloud API | `LLM_API_KEY` env var, `LLM_MODEL` (e.g. `gemini-1.5-flash`) |
| `openrouter` | Proxy API | `LLM_API_KEY` env var, `LLM_MODEL` (provider/model string) |
| `ollama` | Local | `OLLAMA_HOST` env var (e.g. `http://ollama:11434`), `LLM_MODEL` |

Provider is set via `LLM_PROVIDER` environment variable in `docker-compose.yml`. No code changes are needed to switch providers.

---

## File Structure

```
pipeline/
  adapters/
    llm_adapter.py         ← unified ask() interface
  modules/
    suggest.py             ← prompt builder + suggestion storage + scoring
  tests/
    test_llm_adapter.py
    test_suggest.py
```

---

## Step 1 — File: `adapters/llm_adapter.py`

```python
"""
LLM adapter — unified interface for all supported providers.

All adapters expose: ask(prompt: str, max_tokens: int) -> str

Provider selection is via the LLM_PROVIDER environment variable.
If provider is 'none' or not set, ask() returns empty string and
logs a debug message — no exceptions raised.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


class LLMAdapter:
    """
    Provider-agnostic LLM interface.

    Usage:
        adapter = LLMAdapter.from_config(config)
        response = adapter.ask("Translate this verse ...", max_tokens=256)
    """

    def __init__(self, provider: str, model: str, api_key: Optional[str] = None,
                 temperature: float = 0.3, ollama_host: Optional[str] = None):
        self.provider = provider
        self.model = model
        self.api_key = api_key
        self.temperature = temperature
        self.ollama_host = ollama_host

    @classmethod
    def from_config(cls, config: dict) -> "LLMAdapter":
        llm_cfg = config.get("llm", {})
        provider = os.environ.get("LLM_PROVIDER", llm_cfg.get("provider", "none"))
        model    = os.environ.get("LLM_MODEL",    llm_cfg.get("model", ""))
        api_key  = os.environ.get("LLM_API_KEY",  "")
        temp     = float(llm_cfg.get("temperature", 0.3))
        ollama   = os.environ.get("OLLAMA_HOST",  llm_cfg.get("ollama_host", ""))
        return cls(provider, model, api_key, temp, ollama)

    def ask(self, prompt: str, max_tokens: int = 256) -> str:
        """
        Send a prompt and return the response text.
        Returns empty string if provider is 'none'.
        Raises RuntimeError on API errors (caller decides to skip or abort).
        """
        if self.provider == "none" or not self.provider:
            logger.debug("LLM provider is 'none' — skipping generation")
            return ""

        if self.provider == "anthropic":
            return self._ask_anthropic(prompt, max_tokens)
        elif self.provider == "openai":
            return self._ask_openai(prompt, max_tokens)
        elif self.provider == "gemini":
            return self._ask_gemini(prompt, max_tokens)
        elif self.provider == "openrouter":
            return self._ask_openrouter(prompt, max_tokens)
        elif self.provider == "ollama":
            return self._ask_ollama(prompt, max_tokens)
        else:
            raise ValueError(f"Unknown LLM provider: '{self.provider}'")

    def is_enabled(self) -> bool:
        return bool(self.provider and self.provider != "none")

    # ── Provider implementations ──────────────────────────────────

    def _ask_anthropic(self, prompt: str, max_tokens: int) -> str:
        import anthropic
        client = anthropic.Anthropic(api_key=self.api_key)
        message = client.messages.create(
            model=self.model or "claude-haiku-4-5-20251001",
            max_tokens=max_tokens,
            temperature=self.temperature,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()

    def _ask_openai(self, prompt: str, max_tokens: int) -> str:
        from openai import OpenAI
        client = OpenAI(api_key=self.api_key)
        response = client.chat.completions.create(
            model=self.model or "gpt-4o-mini",
            max_tokens=max_tokens,
            temperature=self.temperature,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content.strip()

    def _ask_gemini(self, prompt: str, max_tokens: int) -> str:
        import google.generativeai as genai
        genai.configure(api_key=self.api_key)
        model = genai.GenerativeModel(self.model or "gemini-1.5-flash")
        response = model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=self.temperature,
            ),
        )
        return response.text.strip()

    def _ask_openrouter(self, prompt: str, max_tokens: int) -> str:
        import requests
        response = requests.post(
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
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"].strip()

    def _ask_ollama(self, prompt: str, max_tokens: int) -> str:
        import requests
        host = self.ollama_host or "http://ollama:11434"
        response = requests.post(
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
        response.raise_for_status()
        return response.json()["response"].strip()
```

---

## Step 2 — File: `modules/suggest.py`

```python
"""
Stage 5 — LLM-assisted suggestion generation.

For each verse that exceeds the configured composite_deviation threshold,
generates one or more alternative English translation suggestions that
attempt to better preserve the Hebrew phonetic and structural texture.

Each suggestion is scored with the same metrics as Stage 4 translations.
"""

from __future__ import annotations

import logging
from typing import List, Dict, Optional, Tuple

import psycopg2
import psycopg2.extras

from adapters.llm_adapter import LLMAdapter
from adapters.phoneme_adapter import english_fingerprint, english_breath_weights
from modules.score import _compute_breath_alignment

logger = logging.getLogger(__name__)

# Prompt version — bump when prompt template changes to track provenance
PROMPT_VERSION = "v1.0"


def run(conn: psycopg2.extensions.connection, config: dict) -> dict:
    """
    Generate and store LLM suggestions for high-deviation verses.
    Skips cleanly if LLM provider is 'none'.
    """
    adapter = LLMAdapter.from_config(config)
    if not adapter.is_enabled():
        logger.info("LLM provider is 'none' — suggestion stage skipped.")
        return {"generated": 0, "skipped": "no_provider"}

    llm_cfg = config.get("llm", {})
    max_tokens = llm_cfg.get("max_tokens", 256)
    filter_cfg = llm_cfg.get("suggestion_filter", {})
    min_deviation = filter_cfg.get("min_composite_deviation", 0.15)
    max_per_verse = filter_cfg.get("max_suggestions_per_verse", 3)

    corpus = config.get("corpus", {})
    book_nums = [b["book_num"] for b in corpus.get("books", [])]
    debug_chapters = corpus.get("debug_chapters", [])

    # Deviation weights (same as Stage 4)
    w = config.get("scoring", {}).get("deviation_weights", {})
    dev_weights = {
        "density": w.get("density", 0.35),
        "morpheme": w.get("morpheme", 0.25),
        "sonority": w.get("sonority", 0.20),
        "compression": w.get("compression", 0.20),
    }

    # Find qualifying verses
    candidates = _get_suggestion_candidates(
        conn, book_nums, debug_chapters, min_deviation
    )
    logger.info(f"Found {len(candidates)} verse × translation pairs above threshold")

    generated = 0
    skipped = 0

    for candidate in candidates:
        verse_id        = candidate["verse_id"]
        translation_key = candidate["translation_key"]
        existing_count  = _existing_suggestion_count(conn, verse_id, translation_key)

        if existing_count >= max_per_verse:
            continue

        # Build context-rich prompt
        prompt = _build_prompt(candidate)

        try:
            suggestion_text = adapter.ask(prompt, max_tokens=max_tokens)
        except Exception as e:
            logger.warning(
                f"LLM error for verse {verse_id} / {translation_key}: {e}. Skipping."
            )
            skipped += 1
            continue

        if not suggestion_text:
            skipped += 1
            continue

        # Score the suggestion
        heb_fp = candidate["heb_fingerprint"]
        heb_breath = candidate["heb_breath"]
        eng_fp = english_fingerprint(suggestion_text)

        composite_deviation = _weighted_deviation(heb_fp, eng_fp, dev_weights)
        stress_align, weight_match = _compute_breath_alignment(heb_breath, suggestion_text)
        breath_alignment = round(
            stress_align * 0.60 + weight_match * 0.40, 4
        )
        improvement_delta = round(
            candidate["current_composite_deviation"] - composite_deviation, 4
        )

        # Store
        _store_suggestion(conn, {
            "verse_id":                 verse_id,
            "translation_key":          translation_key,
            "suggested_text":           suggestion_text,
            "composite_deviation":      round(composite_deviation, 4),
            "breath_alignment":         breath_alignment,
            "improvement_delta":        improvement_delta,
            "llm_provider":             adapter.provider,
            "llm_model":                adapter.model,
            "prompt_version":           PROMPT_VERSION,
        })
        generated += 1

    logger.info(f"Suggestions generated: {generated}, skipped: {skipped}")
    return {"generated": generated, "skipped": skipped}


# ─────────────────────────────────────────────────────────────────
# Prompt construction
# ─────────────────────────────────────────────────────────────────

def _build_prompt(candidate: dict) -> str:
    """
    Build a breath-aware translation prompt.

    Provides the LLM with:
    - The Hebrew verse text with rough phonetic description
    - The existing translation and its deviation
    - Specific instruction to preserve phonetic texture
    - Strict output format requirement
    """
    ref = f"{candidate['book_name']} {candidate['chapter']}:{candidate['verse_num']}"
    hebrew = candidate["hebrew_text"]
    translation_key = candidate["translation_key"]
    existing_text = candidate["existing_translation"]
    deviation = candidate["current_composite_deviation"]
    mean_weight = candidate["heb_breath"].get("mean_weight", 0.5)
    guttural_density = candidate.get("guttural_density", 0.0)

    # Describe the Hebrew phonetic character
    phonetic_notes = []
    if mean_weight > 0.65:
        phonetic_notes.append("predominantly open, resonant vowels (high phonetic weight)")
    elif mean_weight < 0.40:
        phonetic_notes.append("compressed, closed syllables (low phonetic weight)")
    else:
        phonetic_notes.append("balanced mix of open and closed syllables")

    if guttural_density > 0.25:
        phonetic_notes.append("frequent guttural consonants (breathy, aspirated quality)")

    phonetic_desc = "; ".join(phonetic_notes) if phonetic_notes else "standard phonetic texture"

    prompt = f"""You are a Biblical Hebrew translator with expertise in phonetic and rhythmic accuracy.

TASK: Suggest a single improved English translation of the following verse that better preserves
the phonetic and rhythmic texture of the original Hebrew.

VERSE: {ref}
HEBREW TEXT: {hebrew}
CURRENT TRANSLATION ({translation_key}): {existing_text}
CURRENT DEVIATION SCORE: {deviation:.3f} (lower is better; 0.0 = perfect structural match)
HEBREW PHONETIC CHARACTER: {phonetic_desc}

REQUIREMENTS:
1. Preserve the meaning of the original translation faithfully — do not add or remove ideas
2. Where meaning allows, prefer English words with similar phonetic weight to the Hebrew:
   - Hebrew has {phonetic_desc}
   - Prefer {'resonant, open-vowel words (praise, glory, flowing)' if mean_weight > 0.55 else 'crisp, closed-syllable words'}
3. Maintain similar clause structure and rhythm to the Hebrew
4. Do not use archaic language unless it appears in the existing translation
5. Return ONLY the translation text with no commentary, explanation, or quotation marks

TRANSLATION:"""

    return prompt


# ─────────────────────────────────────────────────────────────────
# Database helpers
# ─────────────────────────────────────────────────────────────────

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
            vf.syllable_density, vf.morpheme_ratio, vf.sonority_score, vf.clause_compression,
            bp.breath_curve, bp.stress_positions, bp.mean_weight, bp.guttural_density
        FROM translation_scores ts
        JOIN verses v ON ts.verse_id = v.verse_id
        JOIN books b ON v.book_num = b.book_num
        JOIN translations t ON t.verse_id = ts.verse_id AND t.translation_key = ts.translation_key
        JOIN verse_fingerprints vf ON vf.verse_id = ts.verse_id
        JOIN breath_profiles bp ON bp.verse_id = ts.verse_id
        WHERE v.book_num = ANY(%s)
          AND ts.composite_deviation >= %s
    """
    params = [book_nums, min_deviation]
    if debug_chapters:
        q += " AND v.chapter = ANY(%s)"
        params.append(debug_chapters)
    q += " ORDER BY ts.composite_deviation DESC"

    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    candidates = []
    for row in rows:
        d = dict(zip(cols, row))
        candidates.append({
            "verse_id":                     d["verse_id"],
            "translation_key":              d["translation_key"],
            "current_composite_deviation":  float(d["composite_deviation"]),
            "hebrew_text":                  d["hebrew_text"],
            "chapter":                      d["chapter"],
            "verse_num":                    d["verse_num"],
            "book_name":                    d["book_name"],
            "existing_translation":         d["existing_translation"],
            "guttural_density":             float(d.get("guttural_density") or 0),
            "heb_fingerprint": {
                "syllable_density":   float(d["syllable_density"] or 0),
                "morpheme_ratio":     float(d["morpheme_ratio"] or 0),
                "sonority_score":     float(d["sonority_score"] or 0),
                "clause_compression": float(d["clause_compression"] or 0),
            },
            "heb_breath": {
                "breath_curve":      d["breath_curve"] or [],
                "stress_positions":  d["stress_positions"] or [],
                "mean_weight":       float(d["mean_weight"] or 0),
            },
        })
    return candidates


def _existing_suggestion_count(
    conn: psycopg2.extensions.connection,
    verse_id: int,
    translation_key: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM suggestions WHERE verse_id = %s AND translation_key = %s",
            (verse_id, translation_key)
        )
        return cur.fetchone()[0]


def _weighted_deviation(heb_fp: dict, eng_fp: dict, weights: dict) -> float:
    dims = ["syllable_density", "morpheme_ratio", "sonority_score", "clause_compression"]
    weight_keys = ["density", "morpheme", "sonority", "compression"]
    total = 0.0
    for dim, wk in zip(dims, weight_keys):
        total += abs(heb_fp.get(dim, 0) - eng_fp.get(dim, 0)) * weights.get(wk, 0.25)
    return total


def _store_suggestion(conn: psycopg2.extensions.connection, s: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO suggestions
              (verse_id, translation_key, suggested_text,
               composite_deviation, breath_alignment, improvement_delta,
               llm_provider, llm_model, prompt_version)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                s["verse_id"], s["translation_key"], s["suggested_text"],
                s["composite_deviation"], s["breath_alignment"], s["improvement_delta"],
                s["llm_provider"], s["llm_model"], s["prompt_version"],
            )
        )
    conn.commit()
```

---

## Step 3 — Provider Switching Reference

To switch providers, only environment variables in `docker-compose.yml` change:

```yaml
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

# Local Ollama (enable the ollama service block too)
environment:
  LLM_PROVIDER: ollama
  LLM_MODEL:    llama3
  OLLAMA_HOST:  http://ollama:11434

# Disabled (default)
environment:
  LLM_PROVIDER: none
```

---

## Step 4 — Test Cases

```python
# tests/test_llm_adapter.py

import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from adapters.llm_adapter import LLMAdapter


def test_none_provider_returns_empty():
    adapter = LLMAdapter(provider="none", model="")
    result = adapter.ask("Any prompt here", max_tokens=100)
    assert result == ""


def test_none_provider_is_not_enabled():
    adapter = LLMAdapter(provider="none", model="")
    assert not adapter.is_enabled()


def test_configured_provider_is_enabled():
    adapter = LLMAdapter(provider="anthropic", model="claude-haiku-4-5-20251001", api_key="dummy")
    assert adapter.is_enabled()


def test_unknown_provider_raises():
    adapter = LLMAdapter(provider="magic_llm", model="x")
    with pytest.raises(ValueError, match="Unknown LLM provider"):
        adapter.ask("test")


def test_from_config_respects_env_override(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "none")
    config = {"llm": {"provider": "anthropic", "model": "opus"}}
    adapter = LLMAdapter.from_config(config)
    assert adapter.provider == "none"
```

```python
# tests/test_suggest.py

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.suggest import _build_prompt, _weighted_deviation, PROMPT_VERSION


def test_build_prompt_contains_verse_reference():
    candidate = {
        "verse_id": 1,
        "translation_key": "KJV",
        "current_composite_deviation": 0.35,
        "hebrew_text": "יְהוָה רֹעִי",
        "chapter": 23,
        "verse_num": 1,
        "book_name": "Psalms",
        "existing_translation": "The LORD is my shepherd",
        "guttural_density": 0.1,
        "heb_fingerprint": {"syllable_density": 2.1, "morpheme_ratio": 1.8, "sonority_score": 0.6, "clause_compression": 3.0},
        "heb_breath": {"stress_positions": [0.3, 0.7], "mean_weight": 0.65},
    }
    prompt = _build_prompt(candidate)
    assert "Psalms 23:1" in prompt
    assert "The LORD is my shepherd" in prompt
    assert "KJV" in prompt
    assert len(prompt) > 100


def test_build_prompt_describes_high_weight():
    candidate = {
        "verse_id": 1, "translation_key": "KJV",
        "current_composite_deviation": 0.2,
        "hebrew_text": "test", "chapter": 1, "verse_num": 1, "book_name": "Psalms",
        "existing_translation": "test",
        "guttural_density": 0.0,
        "heb_fingerprint": {"syllable_density": 2.0, "morpheme_ratio": 1.5, "sonority_score": 0.5, "clause_compression": 3.0},
        "heb_breath": {"stress_positions": [], "mean_weight": 0.80},
    }
    prompt = _build_prompt(candidate)
    assert "open" in prompt.lower() or "resonant" in prompt.lower()


def test_weighted_deviation_zero_for_identical():
    fp = {"syllable_density": 2.0, "morpheme_ratio": 1.5, "sonority_score": 0.5, "clause_compression": 4.0}
    weights = {"density": 0.35, "morpheme": 0.25, "sonority": 0.20, "compression": 0.20}
    assert _weighted_deviation(fp, fp, weights) == 0.0


def test_weighted_deviation_positive_for_different():
    heb = {"syllable_density": 2.0, "morpheme_ratio": 1.5, "sonority_score": 0.5, "clause_compression": 4.0}
    eng = {"syllable_density": 1.2, "morpheme_ratio": 1.1, "sonority_score": 0.3, "clause_compression": 6.0}
    weights = {"density": 0.35, "morpheme": 0.25, "sonority": 0.20, "compression": 0.20}
    dev = _weighted_deviation(heb, eng, weights)
    assert dev > 0.0


def test_prompt_version_is_string():
    assert isinstance(PROMPT_VERSION, str)
    assert len(PROMPT_VERSION) > 0
```

---

## Acceptance Criteria

- [ ] With `provider: none`, stage exits cleanly with `{"generated": 0, "skipped": "no_provider"}` — no errors
- [ ] With a live provider configured and `min_composite_deviation: 0.15`, suggestions are generated for qualifying verses
- [ ] Each suggestion row has: `verse_id`, `translation_key`, `suggested_text` (non-empty), all score columns non-null, `llm_provider`, `llm_model`, `prompt_version`
- [ ] `improvement_delta` may be negative (suggestion worse than original — this is valid data)
- [ ] Re-running with `max_suggestions_per_verse: 3` does not add a 4th suggestion for any verse
- [ ] All unit tests pass including the `none` provider tests

---

## SQL Validation Queries

```sql
-- Count suggestions by translation
SELECT translation_key, COUNT(*) as suggestion_count,
       ROUND(AVG(improvement_delta)::numeric, 4) as mean_improvement
FROM suggestions
GROUP BY translation_key ORDER BY suggestion_count DESC;

-- Best suggestions (highest improvement)
SELECT s.suggestion_id, v.chapter, v.verse_num, s.translation_key,
       s.composite_deviation, s.improvement_delta,
       t.verse_text AS original, s.suggested_text
FROM suggestions s
JOIN verses v ON s.verse_id = v.verse_id
JOIN translations t ON t.verse_id = s.verse_id AND t.translation_key = s.translation_key
WHERE v.book_num = 19
ORDER BY s.improvement_delta DESC
LIMIT 10;

-- Provider/model used
SELECT llm_provider, llm_model, prompt_version, COUNT(*) as count
FROM suggestions GROUP BY llm_provider, llm_model, prompt_version;
```
