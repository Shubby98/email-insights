"""
store_signals.py
----------------
Step 3 (and orchestrator) of the ingestion pipeline.

1. Initializes the SQLite database and creates the signals table
2. Calls parse_csv → get raw emails
3. Calls extract_signals → get structured signals per email
4. Inserts each result into the database

Run this file to kick off the full ingestion pipeline:
    python ingestion/store_signals.py
"""

import sqlite3
import sys
from pathlib import Path

# Allow importing sibling modules (parse_csv, extract_signals)
# when running this file directly from the project root
sys.path.insert(0, str(Path(__file__).parent))

from parse_csv import parse_emails
from extract_signals import extract_signals

# Paths relative to the project root
CSV_PATH = Path(__file__).parent.parent / "data" / "recent_emails.csv"
DB_PATH = Path(__file__).parent.parent / "database" / "signals.db"


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

def init_db(conn: sqlite3.Connection) -> None:
    """
    Create the signals table if it doesn't already exist.

    Schema explanation:
      - id            : auto-incrementing primary key
      - email_id      : the original email ID from the CSV (UNIQUE prevents duplicates)
      - topic         : what the email is about (from LLM extraction)
      - tone          : sentiment of the email
      - sender_type   : who sent it
      - urgency       : how time-sensitive it is
      - requires_action: whether you need to do something
      - date          : the email date (stored as text, ISO format preferred)
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id       TEXT    UNIQUE,
            topic          TEXT,
            tone           TEXT,
            sender_type    TEXT,
            urgency        TEXT,
            requires_action INTEGER,  -- SQLite has no BOOLEAN; store 0/1
            date           TEXT
        )
    """)
    conn.commit()
    print("[store] Database initialized.")


def store_signal(
    conn: sqlite3.Connection,
    email_id: str,
    signals: dict,
    date: str,
) -> None:
    """
    Insert or replace one signal record.

    INSERT OR REPLACE: if an email_id already exists, overwrite it.
    This makes re-runs safe — you won't get duplicate rows.
    """
    conn.execute(
        """
        INSERT OR REPLACE INTO signals
            (email_id, topic, tone, sender_type, urgency, requires_action, date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            email_id,
            signals.get("topic"),
            signals.get("tone"),
            signals.get("sender_type"),
            signals.get("urgency"),
            int(signals.get("requires_action", False)),  # Convert bool → 0/1
            date,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Main ingestion pipeline
# ---------------------------------------------------------------------------

def run_ingestion(csv_path: Path = CSV_PATH, db_path: Path = DB_PATH) -> None:
    """
    Orchestrate the full pipeline:
      parse CSV → extract signals → store in SQLite
    """
    print(f"[ingestion] Starting pipeline...")
    print(f"  CSV:      {csv_path}")
    print(f"  Database: {db_path}")

    # Load all emails from CSV
    emails = parse_emails(str(csv_path))

    # Connect to (or create) the SQLite database
    conn = sqlite3.connect(str(db_path))
    init_db(conn)

    processed = 0
    failed = 0

    for i, email in enumerate(emails, start=1):
        email_id = email.get("id") or f"{email.get('sender_email', '')}_{email.get('date', str(i))}"
        print(f"\n[ingestion] Processing {i}/{len(emails)} — ID: {email_id}")

        signals = extract_signals(email)

        store_signal(
            conn=conn,
            email_id=email_id,
            signals=signals,
            date=email.get("date", ""),
        )
        processed += 1

    conn.close()
    print(f"\n[ingestion] Done! {processed} emails stored, {failed} failed.")
    print(f"[ingestion] Database saved to: {db_path}")


if __name__ == "__main__":
    run_ingestion()
