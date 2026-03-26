"""
completion.py
-------------
Single entry point for chat completion against the active local LLM.
"""

from .provider import get_client, get_model


def complete(
    prompt: str,
    *,
    model: str | None = None,
    temperature: float = 0.1,
    max_tokens: int = 256,
) -> str:
    """
    Send a prompt to the local LLM and return the raw text response.

    Strips markdown code fences if the model wraps its output in them.
    Raises on any API error — callers handle retries and fallbacks.
    """
    response = get_client().chat.completions.create(
        model=model or get_model(),
        messages=[{"role": "user", "content": prompt}],
        temperature=temperature,
        max_tokens=max_tokens,
    )

    raw = response.choices[0].message.content.strip()

    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()

    return raw
