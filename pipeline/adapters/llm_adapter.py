# pipeline/adapters/llm_adapter.py
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
        api_key: str | None = None,
        temperature: float = 0.3,
        ollama_host: str | None = None,
    ) -> None:
        self.provider = provider
        self.model = model
        self.api_key = api_key
        self.temperature = temperature
        self.ollama_host = ollama_host

    @classmethod
    def from_config(cls, config: dict) -> LLMAdapter:
        """Construct an LLMAdapter from config + environment variables.

        Environment variables override config values.
        """
        llm_cfg = config.get("llm", {})
        provider: str = (
            os.environ.get("LLM_PROVIDER")
            or llm_cfg.get("provider")
            or "none"
        )
        model: str = os.environ.get("LLM_MODEL") or llm_cfg.get("model") or ""
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
        return str(getattr(message.content[0], "text", "")).strip()

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
