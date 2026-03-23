"""
db/signals.py
-------------
All read/write operations for the signals table.

Every function manages its own connection so callers don't have to.
For bulk ingestion (many inserts in a loop), this is still fine — SQLite
connection overhead is negligible at the scale we operate at.
"""

import hashlib
import sqlite3

from .connection import get_connection
from .schema import init_signals_table


def make_email_id(date: str, sender_name: str, sender_email: str) -> str:
    """
    Build a stable, unique ID for an email from its date + sender_name + sender_email.

    Normalises to lowercase and strips whitespace before hashing so minor
    formatting differences (trailing space, mixed case) don't produce duplicate rows.
    Returns the first 16 hex chars of SHA-256 — collision probability is negligible
    at the scale we operate at and the short ID is easy to read in logs.
    """
    raw = "|".join([
        (date or "").strip(),
        (sender_name or "").strip().lower(),
        (sender_email or "").strip().lower(),
    ])
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def init_table() -> None:
    """Create the signals table. Safe to call repeatedly."""
    conn = get_connection()
    init_signals_table(conn)
    conn.close()


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

def store(email_id: str, signals: dict, date: str) -> None:
    """
    Insert or replace one signal record.

    INSERT OR REPLACE: if email_id already exists, overwrite it.
    This makes re-runs idempotent — no duplicate rows.
    """
    conn = get_connection()
    conn.execute(
        """INSERT OR REPLACE INTO signals
               (email_id, topic, tone, sender_type, urgency, requires_action, date)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            email_id,
            signals.get("topic"),
            signals.get("tone"),
            signals.get("sender_type"),
            signals.get("urgency"),
            1 if signals.get("requires_action") else 0,
            date,
        ),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------

def query(
    date: str = None,
    topic: str = None,
    tone: str = None,
    limit: int = 50,
) -> list[dict]:
    """Return signals rows matching the given filters."""
    conn = get_connection(require_exists=True)

    sql = "SELECT * FROM signals WHERE 1=1"
    params: list = []

    if date:
        sql += " AND date = ?"
        params.append(date)
    if topic:
        sql += " AND topic LIKE ?"
        params.append(f"%{topic}%")
    if tone:
        sql += " AND tone = ?"
        params.append(tone)

    sql += " ORDER BY date DESC LIMIT ?"
    params.append(limit)

    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return rows


def topic_distribution() -> list[dict]:
    """Return [{"topic": ..., "count": ...}, ...] sorted by count desc."""
    conn = get_connection(require_exists=True)
    rows = [dict(r) for r in conn.execute("""
        SELECT topic, COUNT(*) AS count
        FROM signals
        GROUP BY topic
        ORDER BY count DESC
    """).fetchall()]
    conn.close()
    return rows


def sender_patterns() -> tuple[list[dict], list[dict]]:
    """
    Return (by_sender_type, urgency_breakdown).

    by_sender_type  : [{"sender_type": ..., "total": ..., "requires_action_count": ...}]
    urgency_breakdown: [{"urgency": ..., "count": ...}]
    """
    conn = get_connection(require_exists=True)

    sender_rows = [dict(r) for r in conn.execute("""
        SELECT
            sender_type,
            COUNT(*) AS total,
            SUM(requires_action) AS requires_action_count
        FROM signals
        GROUP BY sender_type
        ORDER BY total DESC
    """).fetchall()]

    urgency_rows = [dict(r) for r in conn.execute("""
        SELECT urgency, COUNT(*) AS count
        FROM signals
        GROUP BY urgency
        ORDER BY count DESC
    """).fetchall()]

    conn.close()
    return sender_rows, urgency_rows


def search(keyword: str) -> list[dict]:
    """LIKE search across topic and sender_type (case-insensitive)."""
    conn = get_connection(require_exists=True)
    pattern = f"%{keyword.strip()}%"
    rows = [dict(r) for r in conn.execute("""
        SELECT * FROM signals
        WHERE topic LIKE ? OR sender_type LIKE ?
        ORDER BY date DESC
    """, (pattern, pattern)).fetchall()]
    conn.close()
    return rows
