from datetime import datetime, timezone

from database import get_current_user_id, get_gmail_token, load_sync_state_row, save_gmail_token, save_sync_state_row
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
from repository import read_jobs, upsert_job, write_jobs, mark_rejected_by_company_title
from review_repository import (
    build_needs_review_item,
    find_manual_correction,
    queue_needs_review,
)

DEFAULT_QUERY = "from:(jobs-noreply@linkedin.com) newer_than:365d"


def load_sync_state() -> dict:
    return load_sync_state_row()


def save_sync_state(state: dict):
    save_sync_state_row(state)


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


def run_sync(query: str | None = None, force_full: bool = False):
    user_id = get_current_user_id()
    token_json = get_gmail_token(user_id)
    service = get_gmail_service(
        token_json=token_json or None,
        on_token_saved=lambda tj: save_gmail_token(tj, user_id),
    )
    jobs = read_jobs()
    sync_state = load_sync_state()
    if force_full:
        query_to_use = query or DEFAULT_QUERY
    else:
        query_to_use = query or build_incremental_query(sync_state)
    pending_rejections: list[tuple[str, str]] = []

    ids = list_all_message_ids(service, query_to_use)

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

        manual_correction = find_manual_correction(subject=subject, body_text=body)
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
            upsert_job(jobs, incoming)
            processed += 1
            manual_corrections_used += 1
            continue

        rejected_company, rejected_title = extract_rejected_event(subject)
        if rejected_company and rejected_title:
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

        upsert_job(jobs, incoming)
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

    write_jobs(jobs)

    sync_state["initialized"] = True
    if ids:
        sync_state["last_synced_at"] = datetime.now(timezone.utc).isoformat()
    sync_state["last_query"] = query_to_use
    save_sync_state(sync_state)

    summary = {
        "processed": processed,
        "skipped": skipped,
        "rejected_marked": rejected_marked,
        "rejected_not_found": rejected_not_found,
        "needs_review_added": needs_review_added,
        "manual_corrections_used": manual_corrections_used,
        "query": query_to_use,
        "last_synced_at": sync_state.get("last_synced_at", ""),
    }
    print(
        "DONE "
        f"processed={processed} skipped={skipped} "
        f"needs_review_added={needs_review_added} manual_corrections_used={manual_corrections_used} "
        f"rejected_marked={rejected_marked} rejected_not_found={rejected_not_found}"
    )
    return summary


def main():
    run_sync()


if __name__ == "__main__":
    main()
