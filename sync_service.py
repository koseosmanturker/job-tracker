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


# Runs end-to-end synchronization from Gmail into local jobs CSV.
# The function fetches candidate LinkedIn mails, parses events, merges each
# record into repository, writes CSV, and returns a summary dict.
def run_sync(csv_path: str = "jobs.csv", mail_limit: int = 200, query: str = "from:(jobs-noreply@linkedin.com) newer_than:365d"):
    service = get_gmail_service()
    jobs = read_jobs_csv(csv_path)
    pending_rejections: list[tuple[str, str]] = []

    ids = list_all_message_ids(service, query, limit=mail_limit)
    print(f"Found: {len(ids)} (limit={mail_limit})")

    processed = 0
    skipped = 0

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
    # show_viewed_jobs(jobs)

    summary = {
        "processed": processed,
        "skipped": skipped,
        "rejected_marked": rejected_marked,
        "rejected_not_found": rejected_not_found,
        "csv_path": csv_path,
    }
    print(
        "DONE "
        f"processed={processed} skipped={skipped} "
        f"rejected_marked={rejected_marked} rejected_not_found={rejected_not_found} "
        f"csv={csv_path}"
    )
    return summary


# Provides command-line entrypoint for quick manual sync runs.
def main():
    run_sync()


if __name__ == "__main__":
    main()
