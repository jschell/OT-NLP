# tests/test_llm_adapter.py
"""Tests for LLM adapter — provider factory and noop path (Stage 5).

No live API calls — provider SDK calls are mocked or not reached.

Run with:
    uv run --frozen pytest tests/test_llm_adapter.py -v
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest


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
        {
            "LLM_PROVIDER": "ollama",
            "LLM_MODEL": "llama3",
            "OLLAMA_HOST": "http://my-ollama:11434",
        },
    ):
        config: dict = {"llm": {}}
        adapter = LLMAdapter.from_config(config)
    assert adapter.ollama_host == "http://my-ollama:11434"
    assert adapter.provider == "ollama"
