"""
parse_csv.py
------------
Step 1 of the ingestion pipeline.

Reads emails.csv and returns a list of email dicts.
Each dict has keys: id, from_email, subject, body, date
"""

import csv
from pathlib import Path


def parse_emails(csv_path: str) -> list[dict]:
    """
    Read the CSV file and return a list of email records.

    Each record is a plain dict — easy to pass around and inspect.
    No transformation happens here; we just load raw data.
    """
    csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    emails = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)  # DictReader uses the header row as keys
        for row in reader:
            emails.append(dict(row))  # Convert OrderedDict → plain dict

    print(f"[parse_csv] Loaded {len(emails)} emails from {csv_path}")
    return emails


# Run standalone to verify the CSV loads correctly
if __name__ == "__main__":
    emails = parse_emails("data/recent_emails.csv")
    for e in emails:
        print(f"  [{e['sender_name']}] {e['subject'][:50]}...")
