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
from openai import OpenAI

# Point the OpenAI client at LM Studio's local server.
# api_key can be anything — LM Studio doesn't validate it.
client = OpenAI(
    base_url="http://localhost:1234/v1",
    api_key="lm-studio",
)

# The exact model string must match what's loaded in LM Studio.
# LM Studio shows the model identifier in the UI — copy it here.
LOCAL_MODEL = "local-model"  # Replace with your actual model name, e.g. "lmstudio-community/Meta-Llama-3-8B-Instruct-GGUF"

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
From: {email.get('from_email', '')}
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
        print(f"  [extract] Email {email.get('id')}: topic={signals.get('topic')}, tone={signals.get('tone')}")
        return signals

    except json.JSONDecodeError as e:
        print(f"  [extract] WARNING: Could not parse JSON for email {email.get('id')}: {e}")
        return _fallback_signals()

    except Exception as e:
        print(f"  [extract] ERROR processing email {email.get('id')}: {e}")
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
