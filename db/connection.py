"""
db/connection.py
----------------
Single source of truth for opening SQLite connections.

All other db modules import get_connection() from here. No other file
should call sqlite3.connect() directly.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "database" / "signals.db"


def get_connection(require_exists: bool = False) -> sqlite3.Connection:
    """
    Open a connection to the signals database.

    Args:
        require_exists: If True, raise FileNotFoundError when the DB file is
                        missing. Set this for read-only query tools that
                        shouldn't create an empty database.

    Notes:
        - WAL mode lets the background worker write while MCP tools read.
        - check_same_thread=False is needed because MCP may call tools from
          different threads; we open a fresh connection per call anyway.
        - row_factory=sqlite3.Row lets callers do row["column"] instead of row[0].
    """
    if require_exists and not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. "
            "Run `python ingestion/store_signals.py` first to populate it."
        )
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn
