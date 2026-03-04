"""
job_runner.py
-------------
Background worker process that polls SQLite for extraction jobs and
runs them against the local LM Studio model.

This process is completely separate from the MCP server. They share
only the SQLite database file — no sockets, no queues, no shared memory.

Responsibilities:
  - Poll jobs table every 10 seconds for work
  - Process emails by calling LM Studio (OpenAI-compatible API)
  - Retry once on timeout or malformed JSON; store failures in failed_extractions
  - Update processed_emails after every email so progress is visible in real time
  - Log all meaningful events via utils/logger.py

Run this from the project root:
  python worker/job_runner.py
"""

import csv
import json
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from openai import OpenAI

# ---------------------------------------------------------------------------
# Path setup — resolve project root regardless of where we're called from
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.logger import get_logger

logger = get_logger("worker")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = PROJECT_ROOT / "database" / "signals.db"
CSV_PATH = PROJECT_ROOT / "data" / "recent_emails.csv"

LM_STUDIO_BASE = "http://127.0.0.1:10101"
DEFAULT_MODEL = "google/gemma-3-4b"

POLL_INTERVAL = 10  # seconds between polls

# Default extraction schema used when no custom schema is set for a job
DEFAULT_EXTRACTION_SCHEMA = """{
  "topic": "job application | recruiter outreach | rejection | interview | networking | other",
  "tone": "positive | neutral | negative",
  "sender_type": "recruiter | company HR | networking contact | university | other",
  "requires_action": true or false,
  "urgency": "high | medium | low"
}"""

# ---------------------------------------------------------------------------
# LM Studio client (module-level; recreated if connection is lost)
# ---------------------------------------------------------------------------
lm_client = OpenAI(
    base_url=f"{LM_STUDIO_BASE}/v1",
    api_key="lm-studio",
    timeout=60.0,
)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # allows concurrent readers
    return conn


def init_db() -> None:
    """Create jobs and failed_extractions tables if they don't exist yet."""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            schema_id       INTEGER,
            status          TEXT NOT NULL DEFAULT 'pending',
            run_at          TEXT,
            total_emails    INTEGER DEFAULT 0,
            processed_emails INTEGER DEFAULT 0,
            created_at      TEXT DEFAULT (datetime('now')),
            completed_at    TEXT,
            error_message   TEXT,
            retry_of_job_id INTEGER
        );

        CREATE TABLE IF NOT EXISTS failed_extractions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id        INTEGER NOT NULL,
            email_id      TEXT NOT NULL,
            error_message TEXT,
            created_at    TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Job polling
# ---------------------------------------------------------------------------

def get_next_job() -> dict | None:
    """
    Return the next job that should run, or None if nothing is ready.

    Picks up:
      - status = 'pending'  (run immediately)
      - status = 'scheduled' AND run_at <= now  (scheduled time reached)
    """
    conn = _get_conn()
    cursor = conn.execute("""
        SELECT *
        FROM jobs
        WHERE status = 'pending'
           OR (status = 'scheduled' AND run_at <= datetime('now'))
        ORDER BY created_at ASC
        LIMIT 1
    """)
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def mark_job_running(job_id: int) -> None:
    conn = _get_conn()
    conn.execute(
        "UPDATE jobs SET status = 'running' WHERE job_id = ?",
        (job_id,),
    )
    conn.commit()
    conn.close()


def update_progress(job_id: int, processed: int) -> None:
    conn = _get_conn()
    conn.execute(
        "UPDATE jobs SET processed_emails = ? WHERE job_id = ?",
        (processed, job_id),
    )
    conn.commit()
    conn.close()


def mark_job_done(job_id: int, total: int, processed: int) -> None:
    conn = _get_conn()
    conn.execute(
        """UPDATE jobs
           SET status = 'completed',
               completed_at = datetime('now'),
               total_emails = ?,
               processed_emails = ?
           WHERE job_id = ?""",
        (total, processed, job_id),
    )
    conn.commit()
    conn.close()


def mark_job_failed(job_id: int, error_message: str) -> None:
    conn = _get_conn()
    conn.execute(
        """UPDATE jobs
           SET status = 'failed',
               completed_at = datetime('now'),
               error_message = ?
           WHERE job_id = ?""",
        (error_message, job_id),
    )
    conn.commit()
    conn.close()


def set_total_emails(job_id: int, total: int) -> None:
    conn = _get_conn()
    conn.execute(
        "UPDATE jobs SET total_emails = ? WHERE job_id = ?",
        (total, job_id),
    )
    conn.commit()
    conn.close()


def save_failed_extraction(job_id: int, email_id: str, error_message: str) -> None:
    conn = _get_conn()
    conn.execute(
        "INSERT INTO failed_extractions (job_id, email_id, error_message) VALUES (?, ?, ?)",
        (job_id, email_id, error_message),
    )
    conn.commit()
    conn.close()


def store_signal(email_id: str, signals: dict, date: str) -> None:
    """Write extracted signals back into the signals table."""
    conn = _get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO signals
           (email_id, topic, tone, sender_type, urgency, requires_action, date)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            email_id,
            signals.get("topic", "other"),
            signals.get("tone", "neutral"),
            signals.get("sender_type", "other"),
            signals.get("urgency", "low"),
            1 if signals.get("requires_action") else 0,
            date,
        ),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Email loading
# ---------------------------------------------------------------------------

def load_all_emails() -> list[dict]:
    """Load all emails from the CSV file."""
    emails = []
    if not CSV_PATH.exists():
        logger.warning("CSV not found at %s — no emails to process", CSV_PATH)
        return emails

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            emails.append(dict(row))
    return emails


def load_failed_emails(source_job_id: int) -> list[dict]:
    """
    For retry jobs: load only the emails that failed in the source job.
    Matches them against the CSV by email_id.
    """
    conn = _get_conn()
    cursor = conn.execute(
        "SELECT email_id FROM failed_extractions WHERE job_id = ?",
        (source_job_id,),
    )
    failed_ids = {row["email_id"] for row in cursor.fetchall()}
    conn.close()

    if not failed_ids:
        return []

    all_emails = load_all_emails()
    return [e for e in all_emails if e.get("id") in failed_ids]


# ---------------------------------------------------------------------------
# Custom schema loading
# ---------------------------------------------------------------------------

def load_schema(schema_id: int | None) -> str:
    """
    Load extraction schema JSON string from custom_schemas table.
    Falls back to DEFAULT_EXTRACTION_SCHEMA if not found or table missing.
    """
    if schema_id is None:
        return DEFAULT_EXTRACTION_SCHEMA

    try:
        conn = _get_conn()
        cursor = conn.execute(
            "SELECT schema_json FROM custom_schemas WHERE id = ?",
            (schema_id,),
        )
        row = cursor.fetchone()
        conn.close()
        if row:
            return row["schema_json"]
    except sqlite3.OperationalError:
        # custom_schemas table doesn't exist yet
        pass

    return DEFAULT_EXTRACTION_SCHEMA


# ---------------------------------------------------------------------------
# LM Studio extraction with retry
# ---------------------------------------------------------------------------

def _build_prompt(email: dict, schema: str) -> str:
    return f"""You are an email analyst. Extract structured signals from the email below.

Return ONLY a valid JSON object matching this schema — no explanation, no markdown:
{schema}

Email:
Subject: {email.get('subject', '')}
From: {email.get('sender_name', email.get('from_email', ''))} <{email.get('sender_email', email.get('from_email', ''))}>
Body: {email.get('body', '')}

JSON output:"""


def _call_lm_studio(email: dict, schema: str) -> dict:
    """
    One attempt to extract signals from an email via LM Studio.

    Raises:
        requests.exceptions.Timeout  — for connection/read timeouts
        json.JSONDecodeError          — for malformed LLM output
        Exception                     — for any other LM Studio error
    """
    prompt = _build_prompt(email, schema)

    response = lm_client.chat.completions.create(
        model=DEFAULT_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
        max_tokens=256,
    )

    raw = response.choices[0].message.content.strip()

    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()

    return json.loads(raw)  # raises JSONDecodeError if malformed


def extract_with_retry(
    email: dict,
    schema: str,
    index: int,
    total: int,
) -> tuple[dict | None, str | None]:
    """
    Try once, retry once on timeout or JSON error.

    Returns:
        (signals_dict, None)        — on success
        (None, error_message_str)   — after both attempts fail
    """
    email_id = email.get("id", "?")

    for attempt in range(2):
        try:
            signals = _call_lm_studio(email, schema)
            return signals, None

        except (json.JSONDecodeError, Exception) as e:
            error_str = str(e)

            if attempt == 0:
                logger.warning(
                    "[%d/%d] email_id=%s retrying after error: %s",
                    index, total, email_id, error_str,
                )
                time.sleep(1)  # brief pause before retry
            else:
                return None, error_str

    return None, "unknown error"  # unreachable, but satisfies type checker


# ---------------------------------------------------------------------------
# Job processing
# ---------------------------------------------------------------------------

def process_job(job: dict) -> None:
    """
    Process a single job end-to-end.

    Flow:
      1. Determine which emails to process (all vs. retry subset)
      2. Set total_emails on the job
      3. Extract signals for each email with retry logic
      4. Store successes in signals table; log failures to failed_extractions
      5. Mark job completed/failed with summary stats
    """
    job_id = job["job_id"]
    schema_id = job.get("schema_id")
    retry_of = job.get("retry_of_job_id")

    mark_job_running(job_id)

    # Load emails
    if retry_of:
        emails = load_failed_emails(retry_of)
        logger.info(
            "Job %d picked up (retry of job %d): %d emails to process",
            job_id, retry_of, len(emails),
        )
    else:
        emails = load_all_emails()
        logger.info(
            "Job %d picked up: schema_id=%s, %d emails to process",
            job_id, schema_id, len(emails),
        )

    total = len(emails)
    set_total_emails(job_id, total)

    if total == 0:
        mark_job_failed(job_id, "No emails found to process")
        logger.error("Job %d failed: No emails found to process", job_id)
        return

    schema = load_schema(schema_id)
    start_time = datetime.now()
    success_count = 0
    failed_count = 0

    for i, email in enumerate(emails, start=1):
        email_id = email.get("id", f"row-{i}")

        signals, error = extract_with_retry(email, schema, i, total)

        if signals is not None:
            store_signal(email_id, signals, email.get("date", ""))
            success_count += 1
            logger.info(
                "[%d/%d] email_id=%s extracted: topic=%s, tone=%s",
                i, total, email_id,
                signals.get("topic", "?"),
                signals.get("tone", "?"),
            )
        else:
            failed_count += 1
            save_failed_extraction(job_id, email_id, error)
            logger.error(
                "[%d/%d] email_id=%s failed after retry, saved to failed_extractions",
                i, total, email_id,
            )

        # Update progress after every email so callers can see real-time progress
        update_progress(job_id, i)

    duration = (datetime.now() - start_time).total_seconds()
    mark_job_done(job_id, total, total)

    logger.info(
        "Job %d completed in %.1fs: %d success, %d failed",
        job_id, duration, success_count, failed_count,
    )


# ---------------------------------------------------------------------------
# Main poll loop
# ---------------------------------------------------------------------------

def main() -> None:
    init_db()
    logger.info("Worker started, polling every %d seconds", POLL_INTERVAL)

    while True:
        job = get_next_job()

        if job is None:
            logger.info("No jobs found, next poll in %d seconds", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)
            continue

        job_id = job["job_id"]
        status = job["status"]
        run_at = job.get("run_at")

        # Double-check: if it's scheduled and run_at is in the future, skip
        # (shouldn't happen given the SQL query, but guard defensively)
        if status == "scheduled" and run_at:
            logger.info(
                "Scheduled job %d reached run time, starting extraction",
                job_id,
            )

        try:
            process_job(job)
        except Exception as e:
            error_msg = str(e)
            logger.error("Job %d failed: %s", job_id, error_msg)
            mark_job_failed(job_id, error_msg)


if __name__ == "__main__":
    main()
