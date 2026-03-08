from gmail_client import (
    get_gmail_service,
    list_all_message_ids,
    get_message,
    get_header,
    get_message_time_iso,
    extract_body_text,
)
from linkedin_parser import classify_email, extract_job_title_and_location, extract_job_url
from repository import read_jobs_csv, upsert_job_csv, write_jobs_csv, show_viewed_jobs


# Runs end-to-end synchronization from Gmail into local jobs CSV.
# The function fetches candidate LinkedIn mails, parses events, merges each
# record into repository, writes CSV, and returns a summary dict.
def run_sync(csv_path: str = "jobs.csv", mail_limit: int = 50, query: str = "from:(jobs-noreply@linkedin.com) newer_than:365d"):
    service = get_gmail_service()
    jobs = read_jobs_csv(csv_path)

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

        company, applied_evt, viewed_evt = classify_email(subject, body)
        if not company:
            continue

        job_title, location = extract_job_title_and_location(subject, body, company)
        job_url = extract_job_url(body) or ""

        incoming = {
            "company": company,
            "job_title": job_title,
            "location": location,
            "job_url": job_url,
            "applied": bool(applied_evt or viewed_evt),
            "applied_time": event_time if (applied_evt or viewed_evt) else "",
            "viewed": bool(viewed_evt),
            "viewed_time": event_time if viewed_evt else "",
            "downloaded": False,
        }

        upsert_job_csv(jobs, incoming)
        processed += 1
        print(f"OK -> company={company} | applied={applied_evt} | viewed={viewed_evt}")

    write_jobs_csv(csv_path, jobs)
    show_viewed_jobs(jobs)

    summary = {"processed": processed, "skipped": skipped, "csv_path": csv_path}
    print(f"DONE processed={processed} skipped={skipped} csv={csv_path}")
    return summary


# Provides command-line entrypoint for quick manual sync runs.
def main():
    run_sync()


if __name__ == "__main__":
    main()
