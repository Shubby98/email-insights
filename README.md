# email-insights

An MCP server that exposes email signal analytics to Claude Desktop, with a background worker for scheduled async extraction jobs and structured logging.

## Project Structure

```
email-insights/
├── data/
│   └── emails.csv              # Raw email data (id, from, subject, body, date)
├── database/
│   └── signals.db              # SQLite database (created after running ingestion)
├── db/
│   ├── connection.py           # Single source of truth for SQLite connections
│   ├── schema.py               # DDL for all tables (idempotent CREATE IF NOT EXISTS)
│   ├── signals.py              # Read/write for signals table
│   ├── raw_emails.py           # Read/write for raw_emails table
│   └── jobs.py                 # Read/write for jobs and failed_extractions tables
├── ingestion/
│   ├── fetch_emails_imap.py    # Fetch emails via IMAP → store raw in SQLite
│   ├── parse_csv.py            # Step 1: Load emails from CSV
│   ├── extract_signals.py      # Step 2: Call local LLM to extract signals
│   └── store_signals.py        # Step 3: Write signals to SQLite (run this)
├── logs/
│   └── worker.log              # Rotating log file (auto-created, 5 MB max, 3 backups)
├── mcp_server/
│   ├── server.py               # MCP server: registers tools and starts listening
│   └── tools.py                # SQLite query functions + job scheduling tools
├── utils/
│   └── logger.py               # Shared structured logger (stderr + rotating file)
├── worker/
│   └── job_runner.py           # Background worker: polls SQLite and runs extraction jobs
├── requirements.txt
└── README.md
```

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure IMAP credentials

Copy `.env.example` to `.env` and fill in your credentials:

```
IMAP_HOST=imap.gmail.com
IMAP_USER=you@gmail.com
IMAP_PASSWORD=your-app-specific-password
IMAP_PORT=993          # optional, default 993
IMAP_MAILBOX=INBOX     # optional, default INBOX
```

For Gmail, generate an app-specific password at [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords).

### 3. Fetch emails into SQLite

Fetch all emails from your inbox and store them in the `raw_emails` table:

```bash
python ingestion/fetch_emails_imap.py
```

A progress bar shows live fetch and store status. Options:

```bash
# Fetch only the 50 most recent emails
python ingestion/fetch_emails_imap.py --limit 50

# Also export a CSV backup
python ingestion/fetch_emails_imap.py --output data/backup.csv

# Count emails in a date range (no fetch)
python ingestion/fetch_emails_imap.py --count --start-date 2025-01-01 --end-date 2025-03-01
```

### 4. Start LM Studio

- Open LM Studio and load any instruction-following model (Llama 3, Mistral, etc.)
- Start the local server: **Local Server → Start Server**
- Default URL: `http://127.0.0.1:10101`
- Copy the model identifier string and paste it into `ingestion/extract_signals.py` as `LOCAL_MODEL`

### 5. Run signal extraction

```bash
python ingestion/store_signals.py
```

This reads `data/emails.csv`, sends each email to your local LLM for signal extraction,
and stores the results in `database/signals.db`.

### 6. Start the background worker

The worker is a separate process that polls for scheduled extraction jobs. Run it in a dedicated terminal:

```bash
python worker/job_runner.py
```

The worker logs all activity to `logs/worker.log` and to stderr. It polls SQLite every 10 seconds and picks up any pending or due-scheduled jobs automatically.

### 7. Connect Claude Desktop

Add this server to your Claude Desktop config:

**Mac:** `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "email-insights": {
      "command": "python",
      "args": ["/absolute/path/to/email-insights/mcp_server/server.py"]
    }
  }
}
```

Restart Claude Desktop. You should see `email-insights` in the tools list.

## MCP Tools

### Query tools

| Tool | Description |
|------|-------------|
| `get_email_signals_tool` | Query signals with optional date/topic/tone filters |
| `get_topic_distribution_tool` | Count of emails per topic category |
| `get_sender_patterns_tool` | Breakdown by sender type with urgency stats |
| `search_signals_tool` | Search signals by keyword |

### Job scheduling tools

| Tool | Description |
|------|-------------|
| `schedule_extraction_tool` | Create an extraction job — runs now, at a scheduled time, or at midnight |
| `check_job_status_tool` | Get real-time progress for a job (updates after every email) |
| `retry_failed_emails_tool` | Requeue only the emails that failed in a previous job |

All scheduling tools return immediately. Extraction runs asynchronously in the worker process.

#### `schedule_extraction_tool` run modes

| `run_mode` | Behavior | `scheduled_time` |
|---|---|---|
| `"now"` | Worker picks it up on the next poll (default) | not used |
| `"scheduled"` | Runs at a specific time | `"HH:MM"` or `"YYYY-MM-DD HH:MM"` |
| `"midnight"` | Runs tonight at 00:00:00 | not used |

## Architecture

```
Claude Desktop ──stdio──▶ mcp_server/server.py
                                  │
                          mcp_server/tools.py
                                  │
                           SQLite signals.db
                                  │
                        worker/job_runner.py  ◀── runs separately
                                  │
                          LM Studio (local LLM)
```

The MCP server and worker are **two completely separate processes** that share only the SQLite database. The MCP server never waits for extraction to finish — it creates a job record and returns immediately. The worker owns all writes to the `jobs` and `failed_extractions` tables (status updates, progress, failures); the MCP server only reads job status.

## SQLite Schema

```sql
CREATE TABLE raw_emails (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id     TEXT    UNIQUE,    -- SHA-256(date|sender_name|sender_email)[:16]
    date         TEXT,              -- ISO format from email Date header
    sender_name  TEXT,
    sender_email TEXT,
    subject      TEXT,
    body         TEXT,
    fetched_at   TEXT DEFAULT (datetime('now'))
);

CREATE TABLE signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id        TEXT    UNIQUE,
    topic           TEXT,       -- job application | recruiter outreach | rejection | interview | networking | other
    tone            TEXT,       -- positive | neutral | negative
    sender_type     TEXT,       -- recruiter | company HR | networking contact | university | other
    urgency         TEXT,       -- high | medium | low
    requires_action INTEGER,    -- 0 or 1
    date            TEXT        -- ISO format: YYYY-MM-DD
);

CREATE TABLE jobs (
    job_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_id        INTEGER,
    status           TEXT NOT NULL DEFAULT 'pending',  -- pending | scheduled | running | completed | failed
    run_at           TEXT,       -- ISO datetime; NULL means run immediately
    total_emails     INTEGER DEFAULT 0,
    processed_emails INTEGER DEFAULT 0,
    created_at       TEXT DEFAULT (datetime('now')),
    completed_at     TEXT,
    error_message    TEXT,
    retry_of_job_id  INTEGER     -- set for retry jobs; links back to source job
);

CREATE TABLE failed_extractions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id        INTEGER NOT NULL,
    email_id      TEXT NOT NULL,
    error_message TEXT,
    created_at    TEXT DEFAULT (datetime('now'))
);
```

Both `jobs` and `failed_extractions` are created automatically on first use — no manual migration needed.

## Structured Logging

All worker activity is written to `logs/worker.log` (created automatically) and to stderr.

Log format:
```
[2026-03-05 14:22:01] [INFO] Worker started, polling every 10 seconds
[2026-03-05 14:22:11] [INFO] Job 1 picked up: schema_id=None, 10 emails to process
[2026-03-05 14:22:13] [INFO] [1/10] email_id=e001 extracted: topic=recruiter outreach, tone=positive
[2026-03-05 14:22:14] [WARNING] [2/10] email_id=e002 retrying after error: JSONDecodeError
[2026-03-05 14:22:16] [ERROR] [2/10] email_id=e002 failed after retry, saved to failed_extractions
[2026-03-05 14:22:45] [INFO] Job 1 completed in 34.2s: 9 success, 1 failed
```

The log file rotates at 5 MB and keeps the last 3 files (`worker.log`, `worker.log.1`, `worker.log.2`).

## What to Learn from the Code

### `mcp_server/server.py`
- **`FastMCP("email-insights")`** — creates the server instance with a display name
- **`@mcp.tool()`** — registers the decorated function as a callable MCP tool
- **Docstrings matter** — Claude reads them to decide when and how to call each tool
- **Type hints** — FastMCP uses them to build the JSON input schema Claude receives
- **`mcp.run()`** — starts the stdio loop; Claude Desktop communicates via stdin/stdout

### `mcp_server/tools.py`
- Completely separate from MCP — plain Python functions returning JSON strings
- Parameterized SQL queries prevent injection: `WHERE topic LIKE ?` with `params`
- `sqlite3.Row` factory lets you access columns by name: `row["topic"]`
- `_ensure_jobs_tables()` uses `CREATE TABLE IF NOT EXISTS` — safe to call on every tool invocation

### `worker/job_runner.py`
- Polls SQLite every 10 seconds — no message broker needed, just a shared DB
- `PRAGMA journal_mode=WAL` allows the MCP server to read while the worker writes
- Retry logic: one re-attempt on timeout or bad JSON, then `failed_extractions`
- `processed_emails` updated after every email so `check_job_status_tool` always reflects live progress

### `utils/logger.py`
- `get_logger(name)` is idempotent — safe to call from any module, no duplicate handlers
- `RotatingFileHandler` prevents unbounded disk growth
- Uses `sys.stderr` for the stream handler — `sys.stdout` is reserved for MCP's JSON-RPC protocol

### `ingestion/fetch_emails_imap.py`
- `imaplib.IMAP4_SSL` — connects to any IMAP server; credentials loaded from `.env`
- `mail.search(None, "ALL")` returns all message IDs; reversed for most-recent-first order
- `tqdm` progress bars show live fetch and SQLite store status with current subject as suffix
- Stores to `raw_emails` table via `db.raw_emails` — idempotent (`INSERT OR REPLACE`)
- `--output` is optional: CSV is only written when explicitly passed

### `ingestion/extract_signals.py`
- `OpenAI(base_url="http://127.0.0.1:10101/v1")` — points the client at LM Studio
- Low `temperature=0.1` — more deterministic output, better for structured JSON
- Strips markdown code fences the LLM might wrap around its JSON response
- Falls back to safe defaults if parsing fails — pipeline never crashes on one bad email
