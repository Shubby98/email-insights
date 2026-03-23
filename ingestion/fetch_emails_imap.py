"""
fetch_emails_imap.py
--------------------
Fetches emails from an IMAP server and saves them to a CSV file
in the same format expected by the ingestion pipeline (parse_csv.py).

Reads credentials from .env at the project root:
    IMAP_HOST     — e.g., imap.gmail.com
    IMAP_USER     — your email address
    IMAP_PASSWORD — app-specific password (Gmail: myaccount.google.com/apppasswords)
    IMAP_PORT     — (optional) defaults to 993 (SSL)
    IMAP_MAILBOX  — (optional) defaults to INBOX

Usage:
    # Test: fetch only the 10 most recent emails
    python ingestion/fetch_emails_imap.py --limit 10

    # Fetch to a custom output file
    python ingestion/fetch_emails_imap.py --limit 20 --output data/my_emails.csv

    # Fetch and immediately run the full ingestion pipeline (signals → SQLite)
    python ingestion/fetch_emails_imap.py --limit 10 --ingest
"""

import argparse
import csv
import email as email_lib
import imaplib
import sys
from datetime import datetime
from email.header import decode_header as _decode_header
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path

from dotenv import load_dotenv
import os

# Load .env from project root (two levels up from ingestion/)
load_dotenv(Path(__file__).parent.parent / ".env")

DEFAULT_OUTPUT = Path(__file__).parent.parent / "data" / "fetched_emails.csv"


# ---------------------------------------------------------------------------
# Header helpers
# ---------------------------------------------------------------------------

def decode_mime_words(raw: str) -> str:
    """Decode MIME-encoded header value (e.g. =?utf-8?b?...?=) to plain string."""
    if not raw:
        return ""
    parts = []
    for fragment, charset in _decode_header(raw):
        if isinstance(fragment, bytes):
            parts.append(fragment.decode(charset or "utf-8", errors="replace"))
        else:
            parts.append(fragment)
    return "".join(parts)


def extract_body(msg: email_lib.message.Message) -> str:
    """
    Return the plain-text body of an email message.
    For multipart messages, prefer text/plain over text/html.
    Falls back to decoding text/html if no plain part exists.
    """
    if msg.is_multipart():
        plain = None
        html = None
        for part in msg.walk():
            ct = part.get_content_type()
            cd = part.get("Content-Disposition", "")
            if "attachment" in cd:
                continue
            charset = part.get_content_charset() or "utf-8"
            if ct == "text/plain" and plain is None:
                plain = part.get_payload(decode=True).decode(charset, errors="replace")
            elif ct == "text/html" and html is None:
                html = part.get_payload(decode=True).decode(charset, errors="replace")
        return plain or html or ""
    else:
        charset = msg.get_content_charset() or "utf-8"
        return msg.get_payload(decode=True).decode(charset, errors="replace")


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _to_imap_date(iso_date: str) -> str:
    """Convert 'YYYY-MM-DD' to IMAP search date format 'DD-Mon-YYYY'."""
    return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%d-%b-%Y")


# ---------------------------------------------------------------------------
# Email count by date range
# ---------------------------------------------------------------------------

def count_emails_by_date(start_date: str, end_date: str) -> int:
    """
    Count emails in the mailbox whose date falls within [start_date, end_date].

    Args:
        start_date: inclusive start date as 'YYYY-MM-DD'
        end_date:   inclusive end date as 'YYYY-MM-DD'

    Returns:
        Number of matching emails.
    """
    host = os.environ.get("IMAP_HOST", "imap.gmail.com")
    user = os.environ.get("IMAP_USER")
    password = os.environ.get("IMAP_PASSWORD")
    port = int(os.environ.get("IMAP_PORT", 993))
    mailbox = os.environ.get("IMAP_MAILBOX", "INBOX")

    if not user or not password:
        raise ValueError(
            "IMAP_USER and IMAP_PASSWORD must be set in .env or environment. "
            "Copy .env.example to .env and fill in your credentials."
        )

    # IMAP BEFORE is exclusive, so add one day to include end_date
    from datetime import timedelta
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    imap_since = _to_imap_date(start_date)
    imap_before = end_dt.strftime("%d-%b-%Y")

    print(f"[imap] Connecting to {host}:{port} as {user}...")
    mail = imaplib.IMAP4_SSL(host, port)
    mail.login(user, password)
    mail.select(mailbox, readonly=True)

    # SINCE is inclusive, BEFORE is exclusive in IMAP
    _, data = mail.search(None, f'(SINCE "{imap_since}" BEFORE "{imap_before}")')
    count = len(data[0].split()) if data[0] else 0

    mail.logout()
    print(f"[imap] Emails between {start_date} and {end_date}: {count}")
    return count


# ---------------------------------------------------------------------------
# IMAP fetch
# ---------------------------------------------------------------------------

def fetch_emails(limit: int | None = None) -> list[dict]:
    """
    Connect to IMAP server, fetch emails, and return a list of dicts with keys:
        date, sender_name, sender_email, subject, body

    Args:
        limit: max number of emails to fetch (most recent first). None = all.
    """
    host = os.environ.get("IMAP_HOST", "imap.gmail.com")
    user = os.environ.get("IMAP_USER")
    password = os.environ.get("IMAP_PASSWORD")
    port = int(os.environ.get("IMAP_PORT", 993))
    mailbox = os.environ.get("IMAP_MAILBOX", "INBOX")

    if not user or not password:
        raise ValueError(
            "IMAP_USER and IMAP_PASSWORD must be set in .env or environment. "
            "Copy .env.example to .env and fill in your credentials."
        )

    print(f"[imap] Connecting to {host}:{port} as {user}...")
    mail = imaplib.IMAP4_SSL(host, port)
    mail.login(user, password)

    mail.select(mailbox, readonly=True)
    _, data = mail.search(None, "ALL")
    all_ids = data[0].split()

    # Most recent first — reverse the list, then take the limit
    ids_to_fetch = list(reversed(all_ids))
    if limit:
        ids_to_fetch = ids_to_fetch[:limit]

    print(f"[imap] Found {len(all_ids)} emails in {mailbox}. Fetching {len(ids_to_fetch)}...")

    emails = []
    for i, uid in enumerate(ids_to_fetch, start=1):
        _, msg_data = mail.fetch(uid, "(RFC822)")
        raw = msg_data[0][1]
        msg = email_lib.message_from_bytes(raw)

        # Parse From header → name + address
        raw_from = decode_mime_words(msg.get("From", ""))
        sender_name, sender_email = parseaddr(raw_from)
        sender_name = sender_name or sender_email  # fallback if no display name

        # Parse date
        raw_date = msg.get("Date", "")
        try:
            date_iso = parsedate_to_datetime(raw_date).isoformat()
        except Exception:
            date_iso = raw_date  # keep raw if parse fails

        subject = decode_mime_words(msg.get("Subject", "(no subject)"))
        body = extract_body(msg)

        emails.append({
            "date": date_iso,
            "sender_name": sender_name,
            "sender_email": sender_email,
            "subject": subject,
            "body": body,
        })
        print(f"  [{i}/{len(ids_to_fetch)}] {sender_name} — {subject[:60]}")

    mail.logout()
    print(f"[imap] Done. Fetched {len(emails)} emails.")
    return emails


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def save_to_csv(emails: list[dict], output_path: Path) -> None:
    """Write the list of email dicts to a CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["date", "sender_name", "sender_email", "subject", "body"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(emails)
    print(f"[imap] Saved to {output_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch emails via IMAP and save to CSV."
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max number of emails to fetch (most recent first). Omit for all."
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT,
        help=f"Output CSV path. Default: {DEFAULT_OUTPUT}"
    )
    parser.add_argument(
        "--ingest", action="store_true",
        help="After fetching, run the full ingestion pipeline (LM Studio → SQLite)."
    )
    parser.add_argument(
        "--count", action="store_true",
        help="Count emails in the given date range (requires --start-date and --end-date)."
    )
    parser.add_argument(
        "--start-date", type=str, default=None,
        help="Start date for --count mode, format YYYY-MM-DD (inclusive)."
    )
    parser.add_argument(
        "--end-date", type=str, default=None,
        help="End date for --count mode, format YYYY-MM-DD (inclusive)."
    )
    args = parser.parse_args()

    if args.count:
        if not args.start_date or not args.end_date:
            parser.error("--count requires both --start-date and --end-date")
        total = count_emails_by_date(args.start_date, args.end_date)
        print(f"Total: {total}")
        return

    emails = fetch_emails(limit=args.limit)
    save_to_csv(emails, args.output)

    if args.ingest:
        print("\n[imap] --ingest flag set. Starting ingestion pipeline...")
        # Allow importing sibling modules
        sys.path.insert(0, str(Path(__file__).parent))
        from store_signals import run_ingestion
        run_ingestion(csv_path=args.output)


if __name__ == "__main__":
    main()
