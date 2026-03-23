"""
db/jobs.py
----------
All read/write operations for the jobs and failed_extractions tables.

Every function calls init_jobs_tables() before touching the DB so callers
never need to worry about table existence. The CREATE TABLE IF NOT EXISTS
DDL is cheap and idempotent.
"""

import sqlite3

from .connection import get_connection
from .schema import init_jobs_tables


def _conn() -> sqlite3.Connection:
    """Open a connection and ensure jobs tables exist."""
    conn = get_connection()
    init_jobs_tables(conn)
    return conn


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

def init_tables() -> None:
    """Explicitly create jobs tables. Call once on worker startup."""
    conn = get_connection()
    init_jobs_tables(conn)
    conn.close()


# ---------------------------------------------------------------------------
# Job creation
# ---------------------------------------------------------------------------

def create_job(schema_id: int | None, status: str, run_at: str | None) -> int:
    """Insert a new job row and return the new job_id."""
    conn = _conn()
    cursor = conn.execute(
        "INSERT INTO jobs (schema_id, status, run_at) VALUES (?, ?, ?)",
        (schema_id, status, run_at),
    )
    job_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return job_id


def create_retry_job(schema_id: int | None, source_job_id: int) -> int:
    """Insert a pending job that retries failed emails from source_job_id."""
    conn = _conn()
    cursor = conn.execute(
        "INSERT INTO jobs (schema_id, status, retry_of_job_id) VALUES (?, 'pending', ?)",
        (schema_id, source_job_id),
    )
    job_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return job_id


# ---------------------------------------------------------------------------
# Job reads
# ---------------------------------------------------------------------------

def get_job(job_id: int) -> dict | None:
    """Return a job row as a dict, or None if not found."""
    conn = _conn()
    row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_next_pending() -> dict | None:
    """
    Return the next job that should run now, or None if nothing is ready.

    Picks up:
      - status = 'pending'
      - status = 'scheduled' AND run_at <= now
    Ordered FIFO by created_at.
    """
    conn = _conn()
    row = conn.execute("""
        SELECT * FROM jobs
        WHERE status = 'pending'
           OR (status = 'scheduled' AND run_at <= datetime('now'))
        ORDER BY created_at ASC
        LIMIT 1
    """).fetchone()
    conn.close()
    return dict(row) if row else None


def count_failed(job_id: int) -> int:
    """Return the number of failed extraction records for a job."""
    conn = _conn()
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM failed_extractions WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    conn.close()
    return row["cnt"]


def get_failed_email_ids(job_id: int) -> set[str]:
    """Return the set of email_ids that failed in a given job."""
    conn = _conn()
    rows = conn.execute(
        "SELECT email_id FROM failed_extractions WHERE job_id = ?",
        (job_id,),
    ).fetchall()
    conn.close()
    return {row["email_id"] for row in rows}


def load_custom_schema(schema_id: int) -> str | None:
    """
    Load schema JSON from custom_schemas table.
    Returns None if not found or the table doesn't exist yet.
    """
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT schema_json FROM custom_schemas WHERE id = ?",
            (schema_id,),
        ).fetchone()
        conn.close()
        return row["schema_json"] if row else None
    except sqlite3.OperationalError:
        # custom_schemas table doesn't exist yet
        return None


# ---------------------------------------------------------------------------
# Job updates
# ---------------------------------------------------------------------------

def mark_running(job_id: int) -> None:
    conn = _conn()
    conn.execute("UPDATE jobs SET status = 'running' WHERE job_id = ?", (job_id,))
    conn.commit()
    conn.close()


def set_total(job_id: int, total: int) -> None:
    conn = _conn()
    conn.execute("UPDATE jobs SET total_emails = ? WHERE job_id = ?", (total, job_id))
    conn.commit()
    conn.close()


def update_progress(job_id: int, processed: int) -> None:
    conn = _conn()
    conn.execute(
        "UPDATE jobs SET processed_emails = ? WHERE job_id = ?",
        (processed, job_id),
    )
    conn.commit()
    conn.close()


def mark_done(job_id: int, total: int, processed: int) -> None:
    conn = _conn()
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


def mark_failed(job_id: int, error_message: str) -> None:
    conn = _conn()
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


# ---------------------------------------------------------------------------
# Failed extractions
# ---------------------------------------------------------------------------

def save_failed_extraction(job_id: int, email_id: str, error_message: str) -> None:
    conn = _conn()
    conn.execute(
        "INSERT INTO failed_extractions (job_id, email_id, error_message) VALUES (?, ?, ?)",
        (job_id, email_id, error_message),
    )
    conn.commit()
    conn.close()
