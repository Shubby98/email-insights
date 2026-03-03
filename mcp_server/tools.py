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
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "database" / "signals.db"


def _get_connection() -> sqlite3.Connection:
    """
    Open a read connection to the signals database.

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
    conn = _get_connection()

    query = "SELECT * FROM signals WHERE 1=1"
    params = []

    # Each filter is optional — only add it to the query if provided
    if date:
        query += " AND date = ?"
        params.append(date)
    if topic:
        # LIKE allows partial matches: "recruit" matches "recruiter outreach"
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
    conn = _get_connection()

    # First: count by sender_type
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

    # Second: urgency breakdown across all senders
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

    return json.dumps(
        {"keyword": keyword, "count": len(rows), "results": rows},
        indent=2,
    )
