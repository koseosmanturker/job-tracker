from datetime import datetime, timezone
from pathlib import Path

from database import load_sync_state_row, save_sync_state_row
from gmail_client import (
    get_gmail_service,
    list_all_message_ids,
    get_message,
    get_header,
    get_message_time_iso,
    extract_body_text,
)
from linkedin_parser import (
    classify_email,
    extract_job_title_and_location,
    extract_job_url,
    extract_company_display_name,
    extract_rejected_event,
)
from repository import read_jobs_csv, upsert_job_csv, write_jobs_csv, show_viewed_jobs, mark_rejected_by_company_title
from review_repository import (
    NEEDS_REVIEW_FILE,
    MANUAL_CORRECTIONS_FILE,
    build_needs_review_item,
    find_manual_correction,
    queue_needs_review,
)

DEFAULT_QUERY = "from:(jobs-noreply@linkedin.com) newer_than:365d"
SYNC_STATE_FILE = ".sync_state.json"


# Loads sync state metadata from disk and returns dict.
# If state file does not exist or is invalid, returns empty state.
def load_sync_state(state_path: str) -> dict:
    return load_sync_state_row(state_path=state_path)


# Persists latest sync metadata to disk so next run can be incremental.
def save_sync_state(state_path: str, state: dict):
    save_sync_state_row(state, state_path=state_path)


# Builds Gmail query according to sync history.
# First sync uses 365-day window; subsequent syncs use "after:<unix_ts>".
def build_incremental_query(state: dict) -> str:
    if not (state or {}).get("initialized"):
        return DEFAULT_QUERY

    last_synced_at = (state or {}).get("last_synced_at")
    if not last_synced_at:
        return DEFAULT_QUERY

    try:
        dt = datetime.fromisoformat(last_synced_at)
    except Exception:
        return DEFAULT_QUERY

    ts = int(dt.timestamp())
    return f"from:(jobs-noreply@linkedin.com) after:{ts}"


def build_full_window_query(days: int) -> str:
    safe_days = max(1, min(int(days), 3650))
    return f"from:(jobs-noreply@linkedin.com) newer_than:{safe_days}d"


# Runs end-to-end synchronization from Gmail into local jobs CSV.
# The function fetches candidate LinkedIn mails, parses events, merges each
# record into repository, writes CSV, and returns a summary dict.
def run_sync(csv_path: str = "jobs.csv",
            #mail_limit: int = 200,
            query: str | None = None,
            force_full: bool = False):
    
    service = get_gmail_service()
    jobs = read_jobs_csv(csv_path)
    state_path = str(Path(csv_path).with_name(SYNC_STATE_FILE))
    review_path = str(Path(csv_path).with_name(NEEDS_REVIEW_FILE))
    corrections_path = str(Path(csv_path).with_name(MANUAL_CORRECTIONS_FILE))
    sync_state = load_sync_state(state_path)
    if force_full:
        query_to_use = query or DEFAULT_QUERY
    else:
        query_to_use = query or build_incremental_query(sync_state)
    pending_rejections: list[tuple[str, str]] = []

    ids = list_all_message_ids(service, query_to_use,
                                #limit=mail_limit
                                )
    
    print(f"Found: {len(ids)} query={query_to_use}")

    processed = 0
    skipped = 0
    needs_review_added = 0
    manual_corrections_used = 0

    for mid in ids:
        try:
            msg = get_message(service, mid)
        except Exception as err:
            skipped += 1
            print(f"SKIP -> id={mid} reason={type(err).__name__}: {err}")
            continue

        subject = get_header(msg["payload"], "Subject")
        body = extract_body_text(msg["payload"])
        event_time = get_message_time_iso(msg)

        manual_correction = find_manual_correction(corrections_path, subject=subject, body_text=body)
        if manual_correction:
            incoming = {
                "company": manual_correction.get("company", ""),
                "job_title": manual_correction.get("job_title", ""),
                "location": manual_correction.get("location", ""),
                "job_url": manual_correction.get("job_url", ""),
                "applied": bool(manual_correction.get("applied", False)),
                "applied_time": event_time if manual_correction.get("applied", False) else "",
                "viewed": bool(manual_correction.get("viewed", False)),
                "viewed_time": event_time if manual_correction.get("viewed", False) else "",
                "downloaded": False,
                "rejected": bool(manual_correction.get("rejected", False)),
                "favorite": False,
            }
            upsert_job_csv(jobs, incoming)
            processed += 1
            manual_corrections_used += 1
            continue

        rejected_company, rejected_title = extract_rejected_event(subject)
        if rejected_company and rejected_title:
            # Process rejections after all normal upserts to avoid ordering issues.
            pending_rejections.append((rejected_company, rejected_title))
            continue

        company, applied_evt, viewed_evt = classify_email(subject, body)
        if not company:
            continue
        company_display = extract_company_display_name(subject, body, company)

        job_title, location = extract_job_title_and_location(subject, body, company)
        job_url = extract_job_url(body) or ""

        if not company or not job_title:
            added = queue_needs_review(
                review_path,
                build_needs_review_item(
                    message_id=mid,
                    subject=subject,
                    body_text=body,
                    event_time=event_time,
                    reason="missing_company" if not company else "missing_job_title",
                ),
            )
            if added:
                needs_review_added += 1
            continue

        incoming = {
            "company": company_display or company,
            "job_title": job_title,
            "location": location,
            "job_url": job_url,
            "applied": bool(applied_evt or viewed_evt),
            "applied_time": event_time if (applied_evt or viewed_evt) else "",
            "viewed": bool(viewed_evt),
            "viewed_time": event_time if viewed_evt else "",
            "downloaded": False,
            "rejected": False,
        }

        upsert_job_csv(jobs, incoming)
        processed += 1
        print(f"processed={processed-1} -> company={company} | job_title={job_title} | location={location} | applied={applied_evt} | viewed={viewed_evt}")

    rejected_marked = 0
    rejected_not_found = 0
    for rejected_company, rejected_title in pending_rejections:
        marked = mark_rejected_by_company_title(jobs, rejected_company, rejected_title)
        if marked:
            rejected_marked += 1
            print(f"REJECTED -> company={rejected_company} | job_title={rejected_title}")
        else:
            rejected_not_found += 1
            print(f"REJECTED_NOT_FOUND -> company={rejected_company} | job_title={rejected_title}")

    write_jobs_csv(csv_path, jobs)

    # Mark state initialized so first-ever sync starts with DEFAULT_QUERY.
    sync_state["initialized"] = True
    if ids:
        sync_state["last_synced_at"] = datetime.now(timezone.utc).isoformat()
    sync_state["last_query"] = query_to_use
    save_sync_state(state_path, sync_state)
    # show_viewed_jobs(jobs)

    summary = {
        "processed": processed,
        "skipped": skipped,
        "rejected_marked": rejected_marked,
        "rejected_not_found": rejected_not_found,
        "needs_review_added": needs_review_added,
        "manual_corrections_used": manual_corrections_used,
        "query": query_to_use,
        "last_synced_at": sync_state.get("last_synced_at", ""),
        "csv_path": csv_path,
    }
    print(
        "DONE "
        f"processed={processed} skipped={skipped} "
        f"needs_review_added={needs_review_added} manual_corrections_used={manual_corrections_used} "
        f"rejected_marked={rejected_marked} rejected_not_found={rejected_not_found} "
        f"csv={csv_path}"
    )
    return summary


# Provides command-line entrypoint for quick manual sync runs.
def main():
    run_sync()


if __name__ == "__main__":
    main()
