import os
import base64
import socket
import time
from datetime import datetime, timezone
from typing import Optional, List

from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


# Creates and returns an authenticated Gmail API service instance.
# This function handles token loading, token refresh, first-time OAuth login,
# and writes the updated token back to disk for future runs.
def get_gmail_service():
    creds: Optional[Credentials] = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.json", "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


# Fetches message IDs from Gmail with pagination support and optional hard limit.
# This is used to collect all candidate LinkedIn emails before reading each
# message body in detail.
def list_all_message_ids(service, query: str, page_size: int = 500, limit: Optional[int] = None) -> List[str]:
    ids: List[str] = []
    page_token = None
    while True:
        max_results = page_size
        if limit is not None:
            remaining = limit - len(ids)
            if remaining <= 0:
                break
            max_results = min(page_size, remaining)

        resp = (
            service.users()
            .messages()
            .list(userId="me", q=query, maxResults=max_results, pageToken=page_token)
            .execute()
        )
        ids.extend([m["id"] for m in resp.get("messages", [])])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return ids


# Retrieves a single Gmail message and retries transient failures.
# Network/API issues like timeout or temporary HttpError are retried with
# backoff so one unstable request does not fail the whole sync.
def get_message(service, msg_id: str, retries: int = 3, retry_delay_sec: float = 1.2):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return (
                service.users()
                .messages()
                .get(userId="me", id=msg_id, format="full")
                .execute(num_retries=2)
            )
        except (TimeoutError, socket.timeout, HttpError) as err:
            last_err = err
            if attempt < retries:
                time.sleep(retry_delay_sec * attempt)
                continue
            raise last_err


# Reads a specific header value from Gmail payload headers.
# Returns empty string if the requested header is missing.
def get_header(payload, name: str) -> str:
    for h in payload.get("headers", []):
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""


# Converts Gmail internalDate (milliseconds since epoch) into ISO timestamp.
# Returns empty string if the date is missing or parsing fails.
def get_message_time_iso(msg: dict) -> str:
    ms = msg.get("internalDate")
    if not ms:
        return ""
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return ""


# Extracts best-effort readable body text from Gmail payload structure.
# It traverses MIME parts, decodes base64url content, and prefers the body
# variant (plain/html) that includes job-event signals.
def extract_body_text(payload) -> str:
    # Decodes Gmail base64url body chunk into utf-8 string with replacement
    # for invalid bytes, so parsing never fails due to encoding issues.
    def decode(data: str) -> str:
        return base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8", errors="replace")

    # Checks whether text contains keywords that indicate job event content.
    # Used to prefer the HTML body when plain text is incomplete/noisy.
    def has_job_signal(text: str) -> bool:
        from linkedin_parser import normalize_text

        t = normalize_text(text)
        signals = (
            "basvurunuz",
            "sirketindeki",
            "tarafindan goruntulendi",
            "tarihinde basvuruldu",
            "ise alim takimi",
        )
        return any(s in t for s in signals)

    if "body" in payload and payload["body"].get("data"):
        return decode(payload["body"]["data"])

    stack = payload.get("parts", [])[:]
    best_text = ""
    best_html = ""
    while stack:
        part = stack.pop(0)
        body = part.get("body", {})
        data = body.get("data")
        mime = part.get("mimeType", "")
        if part.get("parts"):
            stack.extend(part["parts"])
        if not data:
            continue
        content = decode(data)
        if mime == "text/plain" and not best_text:
            best_text = content
        if mime == "text/html" and not best_html:
            best_html = content

    if best_text and not has_job_signal(best_text) and best_html and has_job_signal(best_html):
        return best_html
    return best_text or best_html or ""
