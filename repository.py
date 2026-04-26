from typing import Dict

from database import list_job_rows, replace_job_rows
from linkedin_parser import (
    normalize_text,
    str_to_bool,
    normalize_job_url,
    extract_job_id,
    is_bad_title,
    is_probable_location_line,
    looks_like_applied_date_line,
)

JOB_FIELDS = [
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
    "follow_up_done",
]


def _normalize_row(row: dict | None) -> dict:
    normalized = {field: "" for field in JOB_FIELDS}
    for field in JOB_FIELDS:
        normalized[field] = str((row or {}).get(field, "") or "").strip()
    return normalized


def _is_missing_review_value(value: str) -> bool:
    normalized = normalize_text(value)
    return normalized in {"", "-", "unknown title"}


def is_incomplete_job_row(row: dict) -> bool:
    return any(
        _is_missing_review_value(row.get(field, ""))
        for field in ("company", "job_title", "location")
    )


def is_unloaded_job_row(row: dict) -> bool:
    return _is_missing_review_value(row.get("company", "")) or _is_missing_review_value(row.get("job_title", ""))


def read_job_rows() -> list[dict]:
    return [_normalize_row(row) for row in list_job_rows()]


def write_job_rows(rows: list[dict]):
    normalized_rows = [_normalize_row(row) for row in rows]
    replace_job_rows(normalized_rows)


def list_incomplete_job_rows() -> list[dict]:
    items: list[dict] = []
    for idx, row in enumerate(read_job_rows()):
        if not is_incomplete_job_row(row):
            continue
        item = dict(row)
        item["csv_row_index"] = idx
        item["review_id"] = f"csv-{idx}"
        items.append(item)
    return items


def get_job_row_by_index(row_index: int) -> dict | None:
    rows = read_job_rows()
    if row_index < 0 or row_index >= len(rows):
        return None
    item = dict(rows[row_index])
    item["csv_row_index"] = row_index
    item["review_id"] = f"csv-{row_index}"
    return item


def update_job_row_by_index(row_index: int, updates: dict) -> dict | None:
    rows = read_job_rows()
    if row_index < 0 or row_index >= len(rows):
        return None

    row = _normalize_row(rows[row_index])
    row["company"] = (updates.get("company") or "").strip()
    row["job_title"] = (updates.get("job_title") or "").strip()
    row["location"] = (updates.get("location") or "").strip()
    row["job_url"] = normalize_job_url((updates.get("job_url") or "").strip()) or ""
    write_job_rows(rows[:row_index] + [row] + rows[row_index + 1 :])
    row["csv_row_index"] = row_index
    row["review_id"] = f"csv-{row_index}"
    return row


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


def choose_earliest_time(old_val: str, new_val: str) -> str:
    if not old_val:
        return new_val
    if not new_val:
        return old_val
    return min(old_val, new_val)


def read_jobs() -> Dict[str, dict]:
    jobs: Dict[str, dict] = {}
    for row in read_job_rows():
        company = row.get("company", "")
        job_title = row.get("job_title", "")
        if is_unloaded_job_row(row):
            continue

        normalized_row = {
            "company": company,
            "job_title": job_title,
            "location": row.get("location", ""),
            "job_url": normalize_job_url(row.get("job_url", "")) or "",
            "applied": True if row.get("applied", "") == "" else str_to_bool(row.get("applied", "")),
            "applied_time": row.get("applied_time", ""),
            "viewed": str_to_bool(row.get("viewed", "")),
            "viewed_time": row.get("viewed_time", ""),
            "downloaded": str_to_bool(row.get("downloaded", "")),
            "rejected": str_to_bool(row.get("rejected", "")),
            "favorite": str_to_bool(row.get("favorite", "")),
            "follow_up_done": str_to_bool(row.get("follow_up_done", "")),
        }
        if normalized_row["viewed"]:
            normalized_row["applied"] = True

        jobs[row_key(normalized_row)] = normalized_row

    return jobs


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


def upsert_job(jobs: Dict[str, dict], incoming: dict):
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
        incoming["follow_up_done"] = bool(incoming.get("follow_up_done", False))
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
    existing["follow_up_done"] = bool(existing.get("follow_up_done", False))

    existing["applied_time"] = choose_earliest_time(existing.get("applied_time", ""), incoming.get("applied_time", ""))
    existing["viewed_time"] = choose_earliest_time(existing.get("viewed_time", ""), incoming.get("viewed_time", ""))
    if existing["applied"] and not existing["applied_time"] and existing["viewed_time"]:
        existing["applied_time"] = existing["viewed_time"]
    if existing["viewed"] and not existing["viewed_time"] and existing["applied_time"]:
        existing["viewed_time"] = existing["applied_time"]

    jobs[key] = existing


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


def write_jobs(jobs: Dict[str, dict]):
    valid_rows = sorted(jobs.values(), key=lambda x: (x["company"].lower(), x["job_title"].lower()))
    preserved_rows = [
        {field: row.get(field, "") for field in JOB_FIELDS}
        for row in read_job_rows()
        if is_unloaded_job_row(row)
    ]
    write_job_rows(valid_rows + preserved_rows)


