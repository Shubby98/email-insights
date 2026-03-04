"""
server.py
---------
The MCP server entry point.

This is the file Claude Desktop launches as a subprocess. It:
  1. Creates an MCP server instance
  2. Registers tools (functions Claude can call)
  3. Starts listening on stdio — Claude Desktop talks to it via stdin/stdout

How Claude Desktop connects:
  You add this server to ~/Library/Application Support/Claude/claude_desktop_config.json:

  {
    "mcpServers": {
      "email-insights": {
        "command": "python",
        "args": ["/absolute/path/to/email-insights/mcp_server/server.py"]
      }
    }
  }

  Claude Desktop launches this script as a child process and communicates
  with it using the MCP protocol over stdio (JSON-RPC messages).

Learning notes:
  - FastMCP is the high-level SDK wrapper — it handles all protocol boilerplate
  - @mcp.tool() is how you register a function as a callable tool
  - The docstring becomes the tool's description — Claude reads it to decide
    when and how to call the tool. Write good docstrings!
  - Type hints on parameters become the tool's input schema — Claude uses them
    to know what arguments to pass
"""

import sys
from pathlib import Path

# Make sure Python can find tools.py in the same directory as this file.
# This is needed when running: python mcp_server/server.py from the project root.
sys.path.insert(0, str(Path(__file__).parent))

from mcp.server.fastmcp import FastMCP
from tools import (
    get_email_signals,
    get_topic_distribution,
    get_sender_patterns,
    search_signals,
    schedule_extraction,
    check_job_status,
    retry_failed_emails,
)

# ---------------------------------------------------------------------------
# Step 1: Initialize the MCP server
# ---------------------------------------------------------------------------
# FastMCP("name") creates the server. The name shows up in Claude Desktop's
# connected tools list and in logs. Pick something descriptive.

mcp = FastMCP("email-insights")


# ---------------------------------------------------------------------------
# Step 2: Register tools with @mcp.tool()
# ---------------------------------------------------------------------------
# Each decorated function becomes a tool Claude can call.
#
# FastMCP automatically:
#   - Uses the function name as the tool name
#   - Uses the docstring as the tool description (CRITICAL — Claude reads this)
#   - Uses type hints to build the JSON input schema
#   - Handles serialization of return values
#
# Rule of thumb: if you can't tell from the docstring alone what the tool does
# and when to use it, Claude won't call it correctly.

@mcp.tool()
def get_email_signals_tool(
    date: str = None,
    topic: str = None,
    tone: str = None,
    limit: int = 50,
) -> str:
    """
    Query email signals from the database with optional filters.

    Use this to retrieve processed email signals. Combine filters to narrow results.

    Args:
        date:  Filter by exact date in ISO format, e.g. '2024-01-15'
        topic: Filter by topic keyword. Partial match supported.
               Valid topics: job application, recruiter outreach, rejection,
               interview, networking, other
        tone:  Filter by sentiment. One of: positive, neutral, negative
        limit: Maximum number of results to return (default: 50)

    Returns:
        JSON with 'count' and 'signals' array. Each signal has:
        id, email_id, topic, tone, sender_type, urgency, requires_action, date
    """
    return get_email_signals(date=date, topic=topic, tone=tone, limit=limit)


@mcp.tool()
def get_topic_distribution_tool() -> str:
    """
    Get a count of emails grouped by topic category.

    Use this to understand the overall composition of the email inbox:
    how many are job applications vs. rejections vs. recruiter outreach, etc.

    Returns:
        JSON with 'topic_distribution' array, each item having 'topic' and 'count'.
        Sorted by count descending (most common topic first).
    """
    return get_topic_distribution()


@mcp.tool()
def get_sender_patterns_tool() -> str:
    """
    Get a breakdown of emails grouped by sender type, with urgency stats.

    Use this to understand who is sending emails and how many require action.
    Good for answering: "Which type of sender requires the most follow-up?"

    Returns:
        JSON with two sections:
        - by_sender_type: count and action-required count per sender category
        - urgency_breakdown: count of high/medium/low urgency emails overall
    """
    return get_sender_patterns()


@mcp.tool()
def search_signals_tool(keyword: str) -> str:
    """
    Search email signals by topic or sender type keyword.

    Use this when the user asks about a specific category or wants to find
    emails matching a particular theme.

    Args:
        keyword: Word or phrase to search for. Searches topic and sender_type fields.
                 Examples: 'recruiter', 'rejection', 'interview', 'networking'

    Returns:
        JSON with 'keyword', 'count', and 'results' array of matching signals.
    """
    return search_signals(keyword=keyword)


# ---------------------------------------------------------------------------
# Step 3: Start the server
# ---------------------------------------------------------------------------
# mcp.run() starts the stdio transport loop.
#
# The server:
#   - Reads JSON-RPC messages from stdin (sent by Claude Desktop)
#   - Dispatches tool calls to the registered functions above
#   - Writes JSON-RPC responses back to stdout
#
# This loop runs forever until the parent process (Claude Desktop) kills it.

@mcp.tool()
def schedule_extraction_tool(
    schema_id: int = None,
    run_mode: str = "now",
    scheduled_time: str = None,
) -> str:
    """
    Schedule an email extraction job to run in the background worker.

    This tool returns immediately — extraction is asynchronous.
    Use check_job_status_tool to track progress.

    Args:
        schema_id:      ID of the custom extraction schema to use (optional).
                        Omit to use the default signal schema.
        run_mode:       When to run the job. One of:
                          "now"       — start as soon as the worker polls (default)
                          "scheduled" — start at the time specified by scheduled_time
                          "midnight"  — start tonight at midnight (00:00:00)
        scheduled_time: Required only when run_mode="scheduled".
                        Format: "HH:MM" (today at that time) or "YYYY-MM-DD HH:MM".

    Returns:
        JSON with job_id, status, run_mode, run_at, and a confirmation message.
    """
    return schedule_extraction(
        schema_id=schema_id,
        run_mode=run_mode,
        scheduled_time=scheduled_time,
    )


@mcp.tool()
def check_job_status_tool(job_id: int) -> str:
    """
    Get the current status and progress of an extraction job.

    Use this after calling schedule_extraction_tool to monitor progress.
    The processed_emails count updates in real time as the worker runs.

    Args:
        job_id: The job ID returned by schedule_extraction_tool.

    Returns:
        JSON with: job_id, status, total_emails, processed_emails,
        progress_pct (0–100), run_at, created_at, completed_at, error_message.
        Status values: pending | scheduled | running | completed | failed
    """
    return check_job_status(job_id=job_id)


@mcp.tool()
def retry_failed_emails_tool(job_id: int) -> str:
    """
    Requeue all emails that failed during a previous extraction job.

    Creates a new job (status=pending) that processes only the failed emails
    from the specified job, using the same schema. Useful for recovering from
    LM Studio timeouts or transient errors without reprocessing everything.

    Args:
        job_id: The ID of the job whose failed emails should be retried.

    Returns:
        JSON with new_job_id, retrying_job_id, failed_emails_queued, and status.
    """
    return retry_failed_emails(job_id=job_id)


if __name__ == "__main__":
    print("[server] email-insights MCP server starting...", file=sys.stderr)
    mcp.run()  # Blocks here, listening for tool calls via stdio
