import csv
import os
from typing import Dict

from linkedin_parser import (
    normalize_text,
    str_to_bool,
    normalize_job_url,
    extract_job_id,
    is_bad_title,
    is_probable_location_line,
    looks_like_applied_date_line,
)

CSV_HEADERS = [
    "company",
    "job_title",
    "location",
    "job_url",
    "applied",
    "applied_time",
    "viewed",
    "viewed_time",
    "downloaded",
    "rejected",
    "favorite",
]


# Picks the better display form for company name while keeping matching logic normalized.
# Preference is given to the variant that contains uppercase styling if old value
# is plain-lowercase, so UI gradually improves as new mails are parsed.
def pick_better_company(old_company: str, new_company: str) -> str:
    if not old_company:
        return new_company
    if not new_company:
        return old_company

    old_has_upper = any(ch.isupper() for ch in old_company)
    new_has_upper = any(ch.isupper() for ch in new_company)

    if not old_has_upper and new_has_upper:
        return new_company
    return old_company


# Builds stable deduplication key per row.
# Preferred key uses LinkedIn job ID; fallback key uses normalized company,
# title, and location tuple when ID is unavailable.
def row_key(row: dict) -> str:
    job_id = extract_job_id(row.get("job_url", ""))
    if job_id:
        return f"id:{job_id}"
    return "|".join(
        [
            normalize_text(row.get("company", "")),
            normalize_text(row.get("job_title", "")),
            normalize_text(row.get("location", "")),
        ]
    )


# Chooses earliest non-empty timestamp between existing and incoming values.
# This preserves the first-known event time when duplicate mails arrive later.
def choose_earliest_time(old_val: str, new_val: str) -> str:
    if not old_val:
        return new_val
    if not new_val:
        return old_val
    return min(old_val, new_val)


# Reads CSV file and returns in-memory dict keyed by row_key.
# The loader normalizes booleans/URLs and enforces "viewed implies applied"
# to keep state internally consistent.
def read_jobs_csv(csv_path: str) -> Dict[str, dict]:
    jobs: Dict[str, dict] = {}
    if not os.path.exists(csv_path):
        return jobs

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = (row.get("company") or "").strip()
            job_title = (row.get("job_title") or "").strip()
            if not company or not job_title:
                continue

            normalized_row = {
                "company": company,
                "job_title": job_title,
                "location": (row.get("location") or "").strip(),
                "job_url": normalize_job_url((row.get("job_url") or "").strip()) or "",
                "applied": True if row.get("applied", "") == "" else str_to_bool(row.get("applied", "")),
                "applied_time": (row.get("applied_time") or "").strip(),
                "viewed": str_to_bool(row.get("viewed", "")),
                "viewed_time": (row.get("viewed_time") or "").strip(),
                "downloaded": str_to_bool(row.get("downloaded", "")),
                "rejected": str_to_bool(row.get("rejected", "")),
                "favorite": str_to_bool(row.get("favorite", "")),
            }
            if normalized_row["viewed"]:
                normalized_row["applied"] = True

            jobs[row_key(normalized_row)] = normalized_row

    return jobs


# Chooses more reliable title when old/new candidates conflict.
# Preference is given to non-noise, non-location, and non-date-like titles.
def pick_better_title(old_title: str, new_title: str, company: str) -> str:
    if not old_title:
        return new_title
    if not new_title:
        return old_title
    if is_bad_title(old_title, company) and not is_bad_title(new_title, company):
        return new_title
    if looks_like_applied_date_line(old_title) and not looks_like_applied_date_line(new_title):
        return new_title
    if is_probable_location_line(old_title, company) and not is_probable_location_line(new_title, company):
        return new_title
    return old_title


# Inserts or merges incoming job event into in-memory store.
# This function performs deduplication, title/location/job_url enrichment,
# and state merges for applied/viewed/downloaded flags and timestamps.
def upsert_job_csv(jobs: Dict[str, dict], incoming: dict):
    key = row_key(incoming)
    existing = jobs.get(key)

    if not existing and extract_job_id(incoming.get("job_url", "")):
        incoming_company_n = normalize_text(incoming.get("company", ""))
        for old_key, old_row in list(jobs.items()):
            old_company_n = normalize_text(old_row.get("company", ""))
            old_job_id = extract_job_id(old_row.get("job_url", ""))
            if incoming_company_n != old_company_n:
                continue
            if old_job_id:
                continue
            if is_bad_title(old_row.get("job_title", ""), old_row.get("company", "")) or not is_probable_location_line(
                old_row.get("location", ""), old_row.get("company", "")
            ):
                existing = old_row
                del jobs[old_key]
                break

    if not existing:
        incoming["applied"] = bool(incoming.get("applied") or incoming.get("viewed"))
        incoming["downloaded"] = bool(incoming.get("downloaded", False))
        incoming["rejected"] = bool(incoming.get("rejected", False))
        incoming["favorite"] = bool(incoming.get("favorite", False))
        if not incoming.get("job_title"):
            incoming["job_title"] = "Unknown Title"
        jobs[key] = incoming
        return

    existing["job_url"] = incoming.get("job_url") or existing.get("job_url", "")
    existing["company"] = pick_better_company(existing.get("company", ""), incoming.get("company", ""))
    existing["location"] = incoming.get("location") or existing.get("location", "")
    existing["job_title"] = pick_better_title(
        existing.get("job_title", ""),
        incoming.get("job_title", ""),
        existing.get("company", incoming.get("company", "")),
    )
    if not existing["job_title"]:
        existing["job_title"] = "Unknown Title"

    incoming_applied = bool(incoming.get("applied") or incoming.get("viewed"))
    existing["applied"] = bool(existing.get("applied") or incoming_applied)
    existing["viewed"] = bool(existing.get("viewed") or incoming.get("viewed"))
    existing["downloaded"] = bool(existing.get("downloaded", False))
    existing["rejected"] = bool(existing.get("rejected", False) or incoming.get("rejected", False))
    existing["favorite"] = bool(existing.get("favorite", False))

    existing["applied_time"] = choose_earliest_time(existing.get("applied_time", ""), incoming.get("applied_time", ""))
    existing["viewed_time"] = choose_earliest_time(existing.get("viewed_time", ""), incoming.get("viewed_time", ""))
    if existing["applied"] and not existing["applied_time"] and existing["viewed_time"]:
        existing["applied_time"] = existing["viewed_time"]
    if existing["viewed"] and not existing["viewed_time"] and existing["applied_time"]:
        existing["viewed_time"] = existing["applied_time"]

    jobs[key] = existing


# Marks an existing job row as rejected by matching normalized company and title.
# First tries exact normalized equality, then falls back to contains match.
def mark_rejected_by_company_title(jobs: Dict[str, dict], company: str, job_title: str) -> bool:
    company_n = normalize_text(company)
    title_n = normalize_text(job_title)

    if not company_n or not title_n:
        return False

    for row in jobs.values():
        if normalize_text(row.get("company", "")) == company_n and normalize_text(row.get("job_title", "")) == title_n:
            row["rejected"] = True
            return True

    for row in jobs.values():
        if company_n in normalize_text(row.get("company", "")) and title_n in normalize_text(row.get("job_title", "")):
            row["rejected"] = True
            return True

    return False


# Writes normalized job rows back to CSV in deterministic order.
# Sorting by company/title makes diffs and manual review easier.
def write_jobs_csv(csv_path: str, jobs: Dict[str, dict]):
    rows = sorted(jobs.values(), key=lambda x: (x["company"].lower(), x["job_title"].lower()))
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


# Prints viewed job summary to terminal for quick post-sync inspection.
def show_viewed_jobs(jobs: Dict[str, dict]):
    viewed_rows = [row for row in jobs.values() if row["viewed"]]
    print(f"\n--- Goruntulenen Toplam Is Sayisi: {len(viewed_rows)} ---")
    for row in viewed_rows:
        print(f"Sirket: {row['company']} | Pozisyon: {row['job_title']}")


# Toggles downloaded state for the given row key.
# Returns (success, new_downloaded_value).
def toggle_downloaded_by_row_id(jobs: Dict[str, dict], row_id: str) -> tuple[bool, bool]:
    row = jobs.get(row_id)
    if not row:
        return False, False
    new_value = not bool(row.get("downloaded", False))
    row["downloaded"] = new_value
    jobs[row_id] = row
    return True, new_value


# Toggles favorite state for the given row key.
# Returns (success, new_favorite_value).
def toggle_favorite_by_row_id(jobs: Dict[str, dict], row_id: str) -> tuple[bool, bool]:
    row = jobs.get(row_id)
    if not row:
        return False, False
    new_value = not bool(row.get("favorite", False))
    row["favorite"] = new_value
    jobs[row_id] = row
    return True, new_value
