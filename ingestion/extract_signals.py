"""
extract_signals.py
------------------
Step 2 of the ingestion pipeline.

Sends each email to a local LLM via LM Studio and extracts structured signals.

LM Studio runs an OpenAI-compatible API at http://localhost:1234/v1
We use the `openai` Python client pointed at that local endpoint.

Why a local LLM?
  - Privacy: email data never leaves your machine
  - Cost: no API fees
  - Speed: low latency for bulk processing
"""

import json
import requests
from openai import OpenAI

LM_STUDIO_BASE = "http://127.0.0.1:10101"

# Point the OpenAI client at LM Studio's local server.
# api_key can be anything — LM Studio doesn't validate it.
client = OpenAI(
    base_url=f"{LM_STUDIO_BASE}/v1",
    api_key="lm-studio",
)

# The exact model string must match what's loaded in LM Studio.
# LM Studio shows the model identifier in the UI — copy it here.
LOCAL_MODEL = "google/gemma-3-4b"  # Replace with your actual model name, e.g. "lmstudio-community/Meta-Llama-3-8B-Instruct-GGUF"

def ensure_model_loaded(model: str = LOCAL_MODEL) -> None:
    """
    Auto-load the model in LM Studio if it isn't already active.

    1. GET /v1/models  — OpenAI-compatible endpoint lists only loaded models.
    2. If our model is missing, POST /api/v0/models/load to load it.
       LM Studio's load call is synchronous — it returns once the model is ready.
    """
    try:
        resp = requests.get(f"{LM_STUDIO_BASE}/v1/models", timeout=10)
        resp.raise_for_status()
        loaded_ids = [m["id"] for m in resp.json().get("data", [])]

        if any(model in mid or mid in model for mid in loaded_ids):
            print(f"[lmstudio] Model '{model}' is already loaded.")
            return

        print(f"[lmstudio] Model not loaded — loading '{model}' (this may take a moment)...")
        load_resp = requests.post(
            f"{LM_STUDIO_BASE}/api/v0/models/load",
            json={"identifier": model},
            timeout=180,  # large models can take a while
        )
        load_resp.raise_for_status()
        print(f"[lmstudio] Model loaded successfully.")

    except requests.ConnectionError:
        print(f"[lmstudio] ERROR: Cannot connect to LM Studio at {LM_STUDIO_BASE}.")
        print("  Make sure LM Studio is open and the local server is running.")
        raise SystemExit(1)

    except Exception as e:
        print(f"[lmstudio] WARNING: Could not auto-load model: {e}")
        raise


def unload_model(model: str = LOCAL_MODEL) -> None:
    """Unload the model from LM Studio to free up VRAM/RAM after the pipeline finishes."""
    try:
        resp = requests.post(
            f"{LM_STUDIO_BASE}/api/v0/models/unload",
            json={"identifier": model},
            timeout=30,
        )
        resp.raise_for_status()
        print(f"[lmstudio] Model '{model}' unloaded.")
    except Exception as e:
        print(f"[lmstudio] WARNING: Could not unload model: {e}")


# This is the structured schema we want the LLM to fill in.
# We embed it directly in the prompt so the model knows exactly what to return.
EXTRACTION_SCHEMA = """
{
  "topic": "job application | recruiter outreach | rejection | interview | networking | other",
  "tone": "positive | neutral | negative",
  "sender_type": "recruiter | company HR | networking contact | university | other",
  "requires_action": true or false,
  "urgency": "high | medium | low"
}
"""


def build_prompt(email: dict) -> str:
    """
    Build the prompt we send to the LLM.

    We give it:
    - Clear instructions
    - The schema to fill in
    - The actual email content
    - A strict reminder to return only JSON
    """
    return f"""You are an email analyst. Extract structured signals from the email below.

Return ONLY a valid JSON object matching this schema — no explanation, no markdown:
{EXTRACTION_SCHEMA}

Email:
Subject: {email.get('subject', '')}
From: {email.get('sender_name', '')} <{email.get('sender_email', '')}>
Body: {email.get('body', '')}

JSON output:"""


def extract_signals(email: dict) -> dict:
    """
    Call the local LLM to extract signals from one email.

    Returns a dict with keys: topic, tone, sender_type, requires_action, urgency
    Falls back to safe defaults if parsing fails.
    """
    prompt = build_prompt(email)

    try:
        response = client.chat.completions.create(
            model=LOCAL_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,   # Low temperature → more deterministic, consistent JSON
            max_tokens=256,    # Signals are small; no need for more
        )

        raw_content = response.choices[0].message.content.strip()

        # The LLM might wrap its response in ```json ... ``` code fences.
        # Strip them if present before parsing.
        if raw_content.startswith("```"):
            raw_content = raw_content.strip("`").lstrip("json").strip()

        signals = json.loads(raw_content)
        print(f"  [extract] Email {email.get('sender_email', email.get('id', '?'))}: topic={signals.get('topic')}, tone={signals.get('tone')}")
        return signals

    except json.JSONDecodeError as e:
        print(f"  [extract] WARNING: Could not parse JSON for email {email.get('sender_email', email.get('id', '?'))}: {e}")
        return _fallback_signals()

    except Exception as e:
        print(f"  [extract] ERROR processing email {email.get('sender_email', email.get('id', '?'))}: {e}")
        return _fallback_signals()


def _fallback_signals() -> dict:
    """Return safe default signals when extraction fails."""
    return {
        "topic": "other",
        "tone": "neutral",
        "sender_type": "other",
        "requires_action": False,
        "urgency": "low",
    }


# Run standalone to test a single extraction against LM Studio
if __name__ == "__main__":
    test_email = {
        "id": "test-1",
        "from_email": "recruiter@company.com",
        "subject": "Exciting Python Engineer Opportunity!",
        "body": "Hi, I found your profile and think you'd be a great fit for our Senior Python Engineer role. Can we chat this week?",
        "date": "2024-01-10",
    }
    result = extract_signals(test_email)
    print("\nExtracted signals:")
    print(json.dumps(result, indent=2))
