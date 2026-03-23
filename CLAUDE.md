# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Fetch emails from IMAP (test: 10 most recent)
python ingestion/fetch_emails_imap.py --limit 10

# Fetch emails and immediately run ingestion pipeline
python ingestion/fetch_emails_imap.py --limit 10 --ingest

# Run ingestion pipeline on an existing CSV
python ingestion/store_signals.py

# Start background worker for async job processing
python worker/job_runner.py

# Test extraction standalone
python ingestion/extract_signals.py
```

No test suite exists yet.

## Architecture

Two independent processes share `database/signals.db` via SQLite (WAL mode):

```
Claude Desktop (stdio)
  └── mcp_server/server.py     FastMCP server, @mcp.tool() decorators
        └── mcp_server/tools.py  Query functions + job scheduling logic

worker/job_runner.py            Background worker, polls SQLite every 10s
  └── ingestion/extract_signals.py  LM Studio call (OpenAI-compatible client)
```

**Ingestion pipeline** (one-shot, run manually):
`fetch_emails_imap.py` (IMAP) → `data/fetched_emails.csv` → `parse_csv.py` → `extract_signals.py` (LM Studio) → `store_signals.py` (SQLite)

Or skip IMAP and start from an existing CSV (`data/recent_emails.csv`).

**Async job flow**: Claude calls `schedule_extraction_tool()` → job written to `jobs` table → worker picks it up → progress tracked in `processed_emails` column → `check_job_status_tool()` polls until complete.

## Key Configuration

Two hardcoded constants that must match your LM Studio setup:

| File | Constant | Value |
|------|----------|-------|
| `ingestion/extract_signals.py` | `LM_STUDIO_BASE` | `http://127.0.0.1:10101` |
| `ingestion/extract_signals.py` | `LOCAL_MODEL` | `google/gemma-3-4b` |
| `worker/job_runner.py` | `LM_STUDIO_BASE` | `http://127.0.0.1:10101` |
| `worker/job_runner.py` | `DEFAULT_MODEL` | `google/gemma-3-4b` |

LM Studio must be running with the model loaded before ingestion or job processing.

## MCP Tools (server.py → tools.py)

**Query tools:** `get_email_signals_tool`, `get_topic_distribution_tool`, `get_sender_patterns_tool`, `search_signals_tool`

**Job tools:** `schedule_extraction_tool`, `check_job_status_tool`, `retry_failed_emails_tool`

All tools return JSON strings. Docstrings on the functions become tool descriptions Claude Desktop reads.

## SQLite Schema

**signals**: `id, email_id (UNIQUE), topic, tone, sender_type, urgency, requires_action (0/1), date`

**jobs**: `job_id, schema_id, status (pending|scheduled|running|completed|failed), run_at, total_emails, processed_emails, created_at, completed_at, error_message, retry_of_job_id`

**failed_extractions**: `id, job_id, email_id, error_message, created_at`

`INSERT OR REPLACE` keeps ingestion idempotent. `jobs` and `failed_extractions` tables are created on first call to `schedule_extraction_tool`.

## Logging

`utils/logger.py` — shared logger, import with `get_logger(name)`. Writes to stderr (stdout is reserved for MCP JSON-RPC) and `logs/worker.log` (rotating, 5 MB, 3 backups). The `logs/` directory is created automatically.

## Claude Desktop Setup

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` with an absolute path to `mcp_server/server.py` in the `args` field.
