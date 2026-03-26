"""
db/raw_emails.py
----------------
All read/write operations for the raw_emails table.
"""

from .connection import get_connection
from .schema import init_raw_emails_table
from .signals import make_email_id


def init_table() -> None:
    """Create the raw_emails table. Safe to call repeatedly."""
    conn = get_connection()
    init_raw_emails_table(conn)
    conn.close()


def store(email: dict) -> str:
    """
    Insert or replace one raw email record.

    Args:
        email: dict with keys date, sender_name, sender_email, subject, body

    Returns:
        The email_id that was stored.
    """
    email_id = make_email_id(
        email.get("date", ""),
        email.get("sender_name", ""),
        email.get("sender_email", ""),
    )
    conn = get_connection()
    conn.execute(
        """INSERT OR REPLACE INTO raw_emails
               (email_id, date, sender_name, sender_email, subject, body)
               VALUES (?, ?, ?, ?, ?, ?)""",
        (
            email_id,
            email.get("date"),
            email.get("sender_name"),
            email.get("sender_email"),
            email.get("subject"),
            email.get("body"),
        ),
    )
    conn.commit()
    conn.close()
    return email_id


def query(limit: int = 50) -> list[dict]:
    """Return raw email rows, most recent first."""
    conn = get_connection(require_exists=True)
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM raw_emails ORDER BY date DESC LIMIT ?", (limit,)
    ).fetchall()]
    conn.close()
    return rows
