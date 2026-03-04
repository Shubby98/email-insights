"""
logger.py
---------
Shared structured logger for the email-insights project.

Used by both worker/job_runner.py and mcp_server/tools.py.

Log format: [YYYY-MM-DD HH:MM:SS] [LEVEL] message
Output:
  - StreamHandler → sys.stderr  (safe for MCP stdio, visible in terminal)
  - RotatingFileHandler → logs/worker.log  (5 MB max, 3 backups)

The logs/ directory is created automatically on first import.
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

# logs/ sits at the project root, two levels above this file (utils/logger.py)
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

_LOG_FILE = LOGS_DIR / "worker.log"
_LOG_FORMAT = "[%(asctime)s] [%(levelname)s] %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger that writes to both stderr and logs/worker.log.

    Calling get_logger() multiple times with the same name is safe —
    Python's logging module returns the same instance and we guard
    against adding duplicate handlers.
    """
    logger = logging.getLogger(name)

    # Guard: only attach handlers once per logger instance
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # stderr is safe for MCP servers (MCP uses stdout for JSON-RPC)
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Rotate at 5 MB, keep last 3 files
    file_handler = RotatingFileHandler(
        _LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
