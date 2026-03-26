"""
llm_config.py
-------------
Centralised LLM provider configuration.

Returns a pre-built OpenAI-compatible client and model name based on the
LLM_PROVIDER environment variable (or .env file). Both LM Studio and Ollama
expose OpenAI-compatible APIs, so only the base URL, api_key, and model name
differ between them.

Usage:
    from utils.llm_config import get_llm_client
    client, model = get_llm_client()

Env vars:
    LLM_PROVIDER   lmstudio (default) | ollama
    LLM_MODEL      Override the model name
    LLM_BASE_URL   Override the base URL (with or without /v1 suffix)
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(Path(__file__).parent.parent / ".env")

_PROVIDER_DEFAULTS = {
    "lmstudio": {
        "base_url": "http://127.0.0.1:10101/v1",
        "api_key": "lm-studio",
        "model": "google/gemma-3-4b",
    },
    "ollama": {
        "base_url": "http://localhost:11434/v1",
        "api_key": "ollama",
        "model": "gemma3:4b",
    },
}


def get_llm_client(timeout: float = 60.0) -> tuple[OpenAI, str]:
    """Return (client, model_name) configured for the active LLM_PROVIDER."""
    provider = os.getenv("LLM_PROVIDER", "lmstudio").lower()
    if provider not in _PROVIDER_DEFAULTS:
        raise ValueError(
            f"Unknown LLM_PROVIDER '{provider}'. Choose: {', '.join(_PROVIDER_DEFAULTS)}"
        )

    defaults = _PROVIDER_DEFAULTS[provider]
    base_url = os.getenv("LLM_BASE_URL", defaults["base_url"])
    if not base_url.endswith("/v1"):
        base_url = base_url.rstrip("/") + "/v1"
    model = os.getenv("LLM_MODEL", defaults["model"])

    client = OpenAI(base_url=base_url, api_key=defaults["api_key"], timeout=timeout)
    return client, model
