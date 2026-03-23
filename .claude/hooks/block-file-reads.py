#!/usr/bin/env python3
import json
import sys

# Files to block (exact suffix/name match)
BLOCKED_FILES = [
    ".env",
    ".env.local",
    "secrets.json",
    "private_key.pem",
]

# Patterns to block (substring match)
BLOCKED_PATTERNS = [
    ".env",
    "certs/",
    "private/",
]

def main():
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        sys.exit(0)  # Non-blocking if input is malformed

    file_path = data.get("tool_input", {}).get("file_path", "")

    if not file_path:
        sys.exit(0)

    # Exact name/suffix match
    for blocked in BLOCKED_FILES:
        if file_path.endswith(blocked) or file_path == blocked:
            print(f"Blocked: Reading '{file_path}' is not allowed.", file=sys.stderr)
            sys.exit(2)

    # Substring/pattern match
    for pattern in BLOCKED_PATTERNS:
        if pattern in file_path:
            print(f"Blocked: Reading '{file_path}' matches restricted pattern '{pattern}'.", file=sys.stderr)
            sys.exit(2)

    sys.exit(0)

if __name__ == "__main__":
    main()