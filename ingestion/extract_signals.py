"""
extract_signals.py
------------------
Step 2 of the ingestion pipeline.

Sends each email to a local LLM and extracts structured signals.

Supports LM Studio and Ollama via the LLM_PROVIDER env var (default: lmstudio).
Both expose an OpenAI-compatible API — set LLM_PROVIDER=ollama to switch.

Why a local LLM?
  - Privacy: email data never leaves your machine
  - Cost: no API fees
  - Speed: low latency for bulk processing
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import locallm

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
        raw_content = locallm.complete(prompt)
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
