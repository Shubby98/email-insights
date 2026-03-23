"""
store_signals.py
----------------
Step 3 (and orchestrator) of the ingestion pipeline.

1. Initialises the SQLite database (signals table)
2. Calls parse_csv → get raw emails
3. Calls extract_signals → get structured signals per email
4. Stores each result via db.signals.store()

Run this file to kick off the full ingestion pipeline:
    python ingestion/store_signals.py
"""

import sys
from pathlib import Path

# Allow importing sibling modules and the project-level db/ package
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).parent))

import db.signals as db_signals
from db.signals import make_email_id
from parse_csv import parse_emails
from extract_signals import extract_signals, ensure_model_loaded, unload_model

CSV_PATH = _PROJECT_ROOT / "data" / "recent_emails.csv"


def run_ingestion(csv_path: Path = CSV_PATH) -> None:
    """
    Orchestrate the full pipeline:
      parse CSV → extract signals → store in SQLite
    """
    print(f"[ingestion] Starting pipeline...")
    print(f"  CSV: {csv_path}")

    ensure_model_loaded()

    emails = parse_emails(str(csv_path))

    # Ensure the signals table exists before the first insert
    db_signals.init_table()

    processed = 0
    failed = 0

    for i, email in enumerate(emails, start=1):
        email_id = make_email_id(
            email.get("date", ""),
            email.get("sender_name", ""),
            email.get("sender_email", ""),
        )
        print(f"\n[ingestion] Processing {i}/{len(emails)} — ID: {email_id}")

        signals = extract_signals(email)
        db_signals.store(email_id, signals, email.get("date", ""))
        processed += 1

    print(f"\n[ingestion] Done! {processed} emails stored, {failed} failed.")
    unload_model()


if __name__ == "__main__":
    run_ingestion()
