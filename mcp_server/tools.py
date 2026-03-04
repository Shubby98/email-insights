"""
tools.py
--------
Pure database query functions — no MCP logic here.

This file is intentionally separated from server.py so you can see the clear
split between:
  - Business logic (this file): SQL queries against SQLite
  - MCP plumbing (server.py): registering functions as tools Claude can call

Each function returns a JSON string because that's what we send back to Claude.
Claude parses the JSON and uses it to answer the user's question.
"""

import json
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Resolve project root so utils/ can be imported regardless of working directory
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from utils.logger import get_logger

logger = get_logger("mcp_tools")

DB_PATH = _PROJECT_ROOT / "database" / "signals.db"


def _get_connection() -> sqlite3.Connection:
    """
    Open a connection to the signals database.

    check_same_thread=False is needed because MCP may call tools from
    different threads. We open a fresh connection per call to keep things simple.
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. "
            "Run `python ingestion/store_signals.py` first to populate it."
        )
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row  # Rows act like dicts: row["topic"] works
    return conn


def _ensure_jobs_tables(conn: sqlite3.Connection) -> None:
    """Create jobs and failed_extractions tables if they don't exist yet."""
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

    conn = _get_connection()

    query = "SELECT * FROM signals WHERE 1=1"
    params = []

    if date:
        query += " AND date = ?"
        params.append(date)
    if topic:
        query += " AND topic LIKE ?"
        params.append(f"%{topic}%")
    if tone:
        query += " AND tone = ?"
        params.append(tone)

    query += " ORDER BY date DESC LIMIT ?"
    params.append(limit)

    cursor = conn.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    logger.info("Tool get_email_signals returned %d rows", len(rows))
    return json.dumps({"count": len(rows), "signals": rows}, indent=2)


# ---------------------------------------------------------------------------
# Tool 2: get_topic_distribution
# ---------------------------------------------------------------------------

def get_topic_distribution() -> str:
    """
    Count emails grouped by topic.

    Uses GROUP BY to aggregate — no Python-level loops needed.
    Returns sorted by count descending so the most common topic is first.
    """
    logger.info("Tool get_topic_distribution called with params: (none)")

    conn = _get_connection()

    cursor = conn.execute("""
        SELECT
            topic,
            COUNT(*) AS count
        FROM signals
        GROUP BY topic
        ORDER BY count DESC
    """)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    logger.info("Tool get_topic_distribution returned %d rows", len(rows))
    return json.dumps({"topic_distribution": rows}, indent=2)


# ---------------------------------------------------------------------------
# Tool 3: get_sender_patterns
# ---------------------------------------------------------------------------

def get_sender_patterns() -> str:
    """
    Break down emails by sender type with additional stats.

    Goes beyond a simple count — also shows:
    - How many require action (useful for prioritization)
    - The urgency mix within each sender type
    """
    logger.info("Tool get_sender_patterns called with params: (none)")

    conn = _get_connection()

    sender_cursor = conn.execute("""
        SELECT
            sender_type,
            COUNT(*) AS total,
            SUM(requires_action) AS requires_action_count
        FROM signals
        GROUP BY sender_type
        ORDER BY total DESC
    """)
    sender_rows = [dict(row) for row in sender_cursor.fetchall()]

    urgency_cursor = conn.execute("""
        SELECT
            urgency,
            COUNT(*) AS count
        FROM signals
        GROUP BY urgency
        ORDER BY count DESC
    """)
    urgency_rows = [dict(row) for row in urgency_cursor.fetchall()]

    conn.close()

    row_count = len(sender_rows) + len(urgency_rows)
    logger.info("Tool get_sender_patterns returned %d rows", row_count)

    return json.dumps(
        {
            "by_sender_type": sender_rows,
            "urgency_breakdown": urgency_rows,
        },
        indent=2,
    )


# ---------------------------------------------------------------------------
# Tool 4: search_signals
# ---------------------------------------------------------------------------

def search_signals(keyword: str) -> str:
    """
    Full-text search across topic and sender_type columns.

    LIKE with % wildcards on both sides = "contains" search.
    Searches both topic and sender_type so one keyword can match either.
    """
    logger.info("Tool search_signals called with params: keyword=%s", keyword)

    if not keyword or not keyword.strip():
        return json.dumps({"error": "keyword cannot be empty"})

    conn = _get_connection()
    pattern = f"%{keyword.strip()}%"

    cursor = conn.execute(
        """
        SELECT *
        FROM signals
        WHERE topic LIKE ?
           OR sender_type LIKE ?
        ORDER BY date DESC
        """,
        (pattern, pattern),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

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

    conn = _get_connection()
    _ensure_jobs_tables(conn)

    run_at = None
    status = "pending"

    if run_mode == "now":
        status = "pending"
        run_at = None

    elif run_mode == "midnight":
        status = "scheduled"
        tomorrow = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(days=1)
        run_at = tomorrow.strftime("%Y-%m-%d %H:%M:%S")

    elif run_mode == "scheduled":
        if not scheduled_time:
            conn.close()
            return json.dumps({"error": "scheduled_time is required when run_mode='scheduled'"})

        status = "scheduled"
        # Parse "HH:MM" or "YYYY-MM-DD HH:MM"
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
            conn.close()
            return json.dumps({"error": f"Invalid scheduled_time format: {e}"})

    else:
        conn.close()
        return json.dumps({"error": f"Unknown run_mode '{run_mode}'. Use 'now', 'scheduled', or 'midnight'."})

    cursor = conn.execute(
        """INSERT INTO jobs (schema_id, status, run_at)
           VALUES (?, ?, ?)""",
        (schema_id, status, run_at),
    )
    job_id = cursor.lastrowid
    conn.commit()
    conn.close()

    logger.info(
        "Job created: job_id=%d, run_mode=%s, run_at=%s",
        job_id, run_mode, run_at,
    )

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

    conn = _get_connection()
    _ensure_jobs_tables(conn)

    cursor = conn.execute(
        "SELECT * FROM jobs WHERE job_id = ?",
        (job_id,),
    )
    row = cursor.fetchone()
    conn.close()

    if row is None:
        return json.dumps({"error": f"Job {job_id} not found"})

    job = dict(row)
    total = job["total_emails"] or 0
    processed = job["processed_emails"] or 0
    progress_pct = round((processed / total * 100), 1) if total > 0 else 0.0

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

    conn = _get_connection()
    _ensure_jobs_tables(conn)

    # Verify the source job exists
    job_cursor = conn.execute(
        "SELECT schema_id FROM jobs WHERE job_id = ?",
        (job_id,),
    )
    job_row = job_cursor.fetchone()

    if job_row is None:
        conn.close()
        return json.dumps({"error": f"Job {job_id} not found"})

    # Count how many emails actually failed
    count_cursor = conn.execute(
        "SELECT COUNT(*) AS cnt FROM failed_extractions WHERE job_id = ?",
        (job_id,),
    )
    failed_count = count_cursor.fetchone()["cnt"]

    if failed_count == 0:
        conn.close()
        return json.dumps(
            {"message": f"No failed extractions found for job {job_id}. Nothing to retry."}
        )

    schema_id = job_row["schema_id"]
    cursor = conn.execute(
        """INSERT INTO jobs (schema_id, status, retry_of_job_id)
           VALUES (?, 'pending', ?)""",
        (schema_id, job_id),
    )
    new_job_id = cursor.lastrowid
    conn.commit()
    conn.close()

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
