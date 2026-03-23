"""
tools.py
--------
Pure business-logic functions — no MCP plumbing here.

Separated from server.py so the split between "what Claude can ask" (server.py)
and "how we answer" (this file) is always clear.

All database work is delegated to the db/ package. Each function here handles:
  - Input validation
  - Logging
  - Scheduling logic (time parsing for schedule_extraction)
  - JSON serialisation for the MCP response
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Resolve project root so db/ and utils/ can be imported regardless of cwd
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import db.jobs as db_jobs
import db.signals as db_signals
from utils.logger import get_logger

logger = get_logger("mcp_tools")


# ---------------------------------------------------------------------------
# Tool 1: get_email_signals
# ---------------------------------------------------------------------------

def get_email_signals(
    date: str = None,
    topic: str = None,
    tone: str = None,
    limit: int = 50,
) -> str:
    """
    Query signals with optional filters.

    Builds a dynamic WHERE clause based on whichever filters are provided.
    Uses parameterized queries (? placeholders) to prevent SQL injection.
    """
    logger.info(
        "Tool get_email_signals called with params: date=%s, topic=%s, tone=%s, limit=%s",
        date, topic, tone, limit,
    )
    rows = db_signals.query(date=date, topic=topic, tone=tone, limit=limit)
    logger.info("Tool get_email_signals returned %d rows", len(rows))
    return json.dumps({"count": len(rows), "signals": rows}, indent=2)


# ---------------------------------------------------------------------------
# Tool 2: get_topic_distribution
# ---------------------------------------------------------------------------

def get_topic_distribution() -> str:
    """
    Count emails grouped by topic.

    Returns sorted by count descending so the most common topic is first.
    """
    logger.info("Tool get_topic_distribution called with params: (none)")
    rows = db_signals.topic_distribution()
    logger.info("Tool get_topic_distribution returned %d rows", len(rows))
    return json.dumps({"topic_distribution": rows}, indent=2)


# ---------------------------------------------------------------------------
# Tool 3: get_sender_patterns
# ---------------------------------------------------------------------------

def get_sender_patterns() -> str:
    """
    Break down emails by sender type with additional stats.

    Also shows how many require action and the urgency mix per sender type.
    """
    logger.info("Tool get_sender_patterns called with params: (none)")
    sender_rows, urgency_rows = db_signals.sender_patterns()
    logger.info(
        "Tool get_sender_patterns returned %d rows",
        len(sender_rows) + len(urgency_rows),
    )
    return json.dumps(
        {"by_sender_type": sender_rows, "urgency_breakdown": urgency_rows},
        indent=2,
    )


# ---------------------------------------------------------------------------
# Tool 4: search_signals
# ---------------------------------------------------------------------------

def search_signals(keyword: str) -> str:
    """
    Full-text search across topic and sender_type columns.

    LIKE with % wildcards on both sides = "contains" search.
    """
    logger.info("Tool search_signals called with params: keyword=%s", keyword)

    if not keyword or not keyword.strip():
        return json.dumps({"error": "keyword cannot be empty"})

    rows = db_signals.search(keyword)
    logger.info("Tool search_signals returned %d rows", len(rows))
    return json.dumps(
        {"keyword": keyword, "count": len(rows), "results": rows},
        indent=2,
    )


# ---------------------------------------------------------------------------
# Tool 5: schedule_extraction
# ---------------------------------------------------------------------------

def schedule_extraction(
    schema_id: int = None,
    run_mode: str = "now",
    scheduled_time: str = None,
) -> str:
    """
    Create a new extraction job and hand it off to the background worker.

    run_mode options:
      "now"       — start immediately (status=pending, run_at=null)
      "scheduled" — start at a specific time (status=scheduled, run_at=parsed scheduled_time)
      "midnight"  — start tonight at 00:00:00 (status=scheduled, run_at=tomorrow 00:00:00)

    scheduled_time formats (only used when run_mode="scheduled"):
      "HH:MM"              — today at that time
      "YYYY-MM-DD HH:MM"   — specific date and time

    This tool returns immediately — extraction runs asynchronously in the worker.
    """
    logger.info(
        "Tool schedule_extraction called with params: schema_id=%s, run_mode=%s, scheduled_time=%s",
        schema_id, run_mode, scheduled_time,
    )

    run_at = None
    status = "pending"

    if run_mode == "now":
        pass  # defaults already set above

    elif run_mode == "midnight":
        status = "scheduled"
        run_at = (
            datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            + timedelta(days=1)
        ).strftime("%Y-%m-%d %H:%M:%S")

    elif run_mode == "scheduled":
        if not scheduled_time:
            return json.dumps(
                {"error": "scheduled_time is required when run_mode='scheduled'"}
            )
        status = "scheduled"
        try:
            if len(scheduled_time) <= 5:  # "HH:MM"
                t = datetime.strptime(scheduled_time, "%H:%M")
                run_at_dt = datetime.now().replace(
                    hour=t.hour, minute=t.minute, second=0, microsecond=0
                )
            else:  # "YYYY-MM-DD HH:MM"
                run_at_dt = datetime.strptime(scheduled_time, "%Y-%m-%d %H:%M")
            run_at = run_at_dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            return json.dumps({"error": f"Invalid scheduled_time format: {e}"})

    else:
        return json.dumps(
            {"error": f"Unknown run_mode '{run_mode}'. Use 'now', 'scheduled', or 'midnight'."}
        )

    job_id = db_jobs.create_job(schema_id, status, run_at)
    logger.info("Job created: job_id=%d, run_mode=%s, run_at=%s", job_id, run_mode, run_at)

    return json.dumps(
        {
            "job_id": job_id,
            "status": status,
            "schema_id": schema_id,
            "run_mode": run_mode,
            "run_at": run_at,
            "message": "Job created. The worker will pick it up automatically.",
        },
        indent=2,
    )


# ---------------------------------------------------------------------------
# Tool 6: check_job_status
# ---------------------------------------------------------------------------

def check_job_status(job_id: int) -> str:
    """
    Return real-time status and progress for a specific job.

    progress_pct is 0–100 and updates after every email processed.
    """
    logger.info("Tool check_job_status called with params: job_id=%d", job_id)

    job = db_jobs.get_job(job_id)
    if job is None:
        return json.dumps({"error": f"Job {job_id} not found"})

    total = job["total_emails"] or 0
    processed = job["processed_emails"] or 0
    progress_pct = round(processed / total * 100, 1) if total > 0 else 0.0

    logger.info("Tool check_job_status returned 1 rows")
    return json.dumps(
        {
            "job_id": job["job_id"],
            "status": job["status"],
            "total_emails": total,
            "processed_emails": processed,
            "progress_pct": progress_pct,
            "run_at": job["run_at"],
            "created_at": job["created_at"],
            "completed_at": job["completed_at"],
            "error_message": job["error_message"],
        },
        indent=2,
    )


# ---------------------------------------------------------------------------
# Tool 7: retry_failed_emails
# ---------------------------------------------------------------------------

def retry_failed_emails(job_id: int) -> str:
    """
    Create a new job that reprocesses only the emails that failed in a previous job.

    The new job inherits the same schema_id and is queued immediately (status=pending).
    The worker identifies which emails to retry via the retry_of_job_id link.
    """
    logger.info("Tool retry_failed_emails called with params: job_id=%d", job_id)

    job = db_jobs.get_job(job_id)
    if job is None:
        return json.dumps({"error": f"Job {job_id} not found"})

    failed_count = db_jobs.count_failed(job_id)
    if failed_count == 0:
        return json.dumps(
            {"message": f"No failed extractions found for job {job_id}. Nothing to retry."}
        )

    new_job_id = db_jobs.create_retry_job(job["schema_id"], job_id)
    logger.info(
        "Job created: job_id=%d, run_mode=now (retry of job %d), run_at=None",
        new_job_id, job_id,
    )

    return json.dumps(
        {
            "new_job_id": new_job_id,
            "retrying_job_id": job_id,
            "failed_emails_queued": failed_count,
            "status": "pending",
            "message": "Retry job created. The worker will process failed emails shortly.",
        },
        indent=2,
    )
