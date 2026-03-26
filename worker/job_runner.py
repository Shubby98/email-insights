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
import sys
import time
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup — resolve project root regardless of where we're called from
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import db.jobs as db_jobs
import db.signals as db_signals
import locallm
from db.signals import make_email_id
from utils.logger import get_logger

logger = get_logger("worker")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CSV_PATH = PROJECT_ROOT / "data" / "recent_emails.csv"

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
# Email loading (CSV — no DB)
# ---------------------------------------------------------------------------

def load_all_emails() -> list[dict]:
    """Load all emails from the CSV file."""
    if not CSV_PATH.exists():
        logger.warning("CSV not found at %s — no emails to process", CSV_PATH)
        return []

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return [dict(row) for row in csv.DictReader(f)]


def load_failed_emails(source_job_id: int) -> list[dict]:
    """
    For retry jobs: load only the emails that failed in the source job.
    Matches them against the CSV by email_id.
    """
    failed_ids = db_jobs.get_failed_email_ids(source_job_id)
    if not failed_ids:
        return []
    return [e for e in load_all_emails() if e.get("id") in failed_ids]


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------

def load_schema(schema_id: int | None) -> str:
    """
    Load extraction schema JSON string.
    Falls back to DEFAULT_EXTRACTION_SCHEMA if schema_id is None or not found.
    """
    if schema_id is None:
        return DEFAULT_EXTRACTION_SCHEMA
    custom = db_jobs.load_custom_schema(schema_id)
    return custom if custom is not None else DEFAULT_EXTRACTION_SCHEMA


# ---------------------------------------------------------------------------
# Extraction with retry
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


def _call_llm(email: dict, schema: str) -> dict:
    """
    One attempt to extract signals from an email via the local LLM.

    Raises:
        json.JSONDecodeError  — for malformed LLM output
        Exception             — for any other provider error
    """
    prompt = _build_prompt(email, schema)
    raw = locallm.complete(prompt)
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
            signals = _call_llm(email, schema)
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

    db_jobs.mark_running(job_id)

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
    db_jobs.set_total(job_id, total)

    if total == 0:
        db_jobs.mark_failed(job_id, "No emails found to process")
        logger.error("Job %d failed: No emails found to process", job_id)
        return

    schema = load_schema(schema_id)
    start_time = datetime.now()
    success_count = 0
    failed_count = 0

    for i, email in enumerate(emails, start=1):
        email_id = make_email_id(
            email.get("date", ""),
            email.get("sender_name", ""),
            email.get("sender_email", ""),
        )

        signals, error = extract_with_retry(email, schema, i, total)

        if signals is not None:
            db_signals.store(email_id, signals, email.get("date", ""))
            success_count += 1
            logger.info(
                "[%d/%d] email_id=%s extracted: topic=%s, tone=%s",
                i, total, email_id,
                signals.get("topic", "?"),
                signals.get("tone", "?"),
            )
        else:
            failed_count += 1
            db_jobs.save_failed_extraction(job_id, email_id, error)
            logger.error(
                "[%d/%d] email_id=%s failed after retry, saved to failed_extractions",
                i, total, email_id,
            )

        # Update progress after every email so callers can see real-time progress
        db_jobs.update_progress(job_id, i)

    duration = (datetime.now() - start_time).total_seconds()
    db_jobs.mark_done(job_id, total, total)

    logger.info(
        "Job %d completed in %.1fs: %d success, %d failed",
        job_id, duration, success_count, failed_count,
    )


# ---------------------------------------------------------------------------
# Main poll loop
# ---------------------------------------------------------------------------

def main() -> None:
    db_jobs.init_tables()
    logger.info("Worker started, polling every %d seconds", POLL_INTERVAL)

    while True:
        job = db_jobs.get_next_pending()

        if job is None:
            logger.info("No jobs found, next poll in %d seconds", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)
            continue

        job_id = job["job_id"]
        status = job["status"]
        run_at = job.get("run_at")

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
            db_jobs.mark_failed(job_id, error_msg)


if __name__ == "__main__":
    main()
