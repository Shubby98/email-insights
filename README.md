# email-insights

An MCP server that exposes email signal analytics to Claude Desktop.

## Project Structure

```
email-insights/
├── data/
│   └── emails.csv              # Raw email data (id, from, subject, body, date)
├── database/
│   └── signals.db              # SQLite database (created after running ingestion)
├── ingestion/
│   ├── parse_csv.py            # Step 1: Load emails from CSV
│   ├── extract_signals.py      # Step 2: Call local LLM to extract signals
│   └── store_signals.py        # Step 3: Write signals to SQLite (run this)
├── mcp_server/
│   ├── server.py               # MCP server: registers tools and starts listening
│   └── tools.py                # SQLite query functions (no MCP logic here)
├── requirements.txt
└── README.md
```

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start LM Studio

- Open LM Studio and load any instruction-following model (Llama 3, Mistral, etc.)
- Start the local server: **Local Server → Start Server**
- Default URL: `http://localhost:1234/v1`
- Copy the model identifier string and paste it into `ingestion/extract_signals.py` as `LOCAL_MODEL`

### 3. Run the ingestion pipeline

```bash
python ingestion/store_signals.py
```

This reads `data/emails.csv`, sends each email to your local LLM for signal extraction,
and stores the results in `database/signals.db`.

### 4. Connect Claude Desktop

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

| Tool | Description |
|------|-------------|
| `get_email_signals_tool` | Query signals with optional date/topic/tone filters |
| `get_topic_distribution_tool` | Count of emails per topic category |
| `get_sender_patterns_tool` | Breakdown by sender type with urgency stats |
| `search_signals_tool` | Search signals by keyword |

## SQLite Schema

```sql
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
```

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
- Returns JSON strings so Claude can parse and reason about the data

### `ingestion/extract_signals.py`
- `OpenAI(base_url="http://localhost:1234/v1")` — points the client at LM Studio
- Low `temperature=0.1` — more deterministic output, better for structured JSON
- Strips markdown code fences the LLM might wrap around its JSON response
- Falls back to safe defaults if parsing fails — pipeline never crashes on one bad email
