"""
db/schema.py
------------
DDL for every table in signals.db.

Each init_* function is idempotent (CREATE TABLE IF NOT EXISTS) so callers
can call them on every startup without checking first.
"""

import sqlite3


def init_signals_table(conn: sqlite3.Connection) -> None:
    """Create the signals table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id        TEXT    UNIQUE,
            topic           TEXT,
            tone            TEXT,
            sender_type     TEXT,
            urgency         TEXT,
            requires_action INTEGER,  -- SQLite has no BOOLEAN; store 0/1
            date            TEXT
        )
    """)
    conn.commit()


def init_jobs_tables(conn: sqlite3.Connection) -> None:
    """Create jobs and failed_extractions tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id           INTEGER PRIMARY KEY AUTOINCREMENT,
            schema_id        INTEGER,
            status           TEXT NOT NULL DEFAULT 'pending',
            run_at           TEXT,
            total_emails     INTEGER DEFAULT 0,
            processed_emails INTEGER DEFAULT 0,
            created_at       TEXT DEFAULT (datetime('now')),
            completed_at     TEXT,
            error_message    TEXT,
            retry_of_job_id  INTEGER
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
