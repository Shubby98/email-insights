"""
lifecycle.py
------------
Model load/unload for local LLM providers.

LM Studio: explicit HTTP calls to load/unload the model.
Ollama:    no-op — loads on first request, manages its own lifecycle.
"""

import requests

from .provider import get_base_url, get_model, get_provider


def load_model(model: str | None = None) -> None:
    """
    Ensure the model is loaded and ready.

    LM Studio:
      1. GET /v1/models — list currently loaded models
      2. If missing, POST /api/v0/models/load (synchronous, waits until ready)
    Ollama: no-op.
    """
    if get_provider() != "lmstudio":
        return

    model = model or get_model()
    base = get_base_url()

    try:
        resp = requests.get(f"{base}/v1/models", timeout=10)
        resp.raise_for_status()
        loaded_ids = [m["id"] for m in resp.json().get("data", [])]

        if any(model in mid or mid in model for mid in loaded_ids):
            print(f"[lmstudio] Model '{model}' is already loaded.")
            return

        print(f"[lmstudio] Model not loaded — loading '{model}' (this may take a moment)...")
        load_resp = requests.post(
            f"{base}/api/v0/models/load",
            json={"identifier": model},
            timeout=180,
        )
        load_resp.raise_for_status()
        print(f"[lmstudio] Model loaded successfully.")

    except requests.ConnectionError:
        print(f"[lmstudio] ERROR: Cannot connect to LM Studio at {base}.")
        print("  Make sure LM Studio is open and the local server is running.")
        raise SystemExit(1)

    except Exception as e:
        print(f"[lmstudio] WARNING: Could not auto-load model: {e}")
        raise


def unload_model(model: str | None = None) -> None:
    """
    Release the model from memory after processing is done.

    LM Studio: POST /api/v0/models/unload.
    Ollama: no-op.
    """
    if get_provider() != "lmstudio":
        return

    model = model or get_model()
    base = get_base_url()

    try:
        resp = requests.post(
            f"{base}/api/v0/models/unload",
            json={"identifier": model},
            timeout=30,
        )
        resp.raise_for_status()
        print(f"[lmstudio] Model '{model}' unloaded.")
    except Exception as e:
        print(f"[lmstudio] WARNING: Could not unload model: {e}")
