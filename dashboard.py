from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, request

from repository import read_jobs_csv


BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "jobs.csv"

web = Flask(__name__)


# Parses multiple datetime text formats into datetime object.
# Returns None when input is empty, placeholder, or invalid.
def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None

    s = str(value).strip()
    if not s or s == "-":
        return None

    # Try a few common formats
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    # Last resort: python's ISO parser (covers many cases)
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


# Formats raw timestamp string into compact dashboard-friendly display.
# Returns "-" if value cannot be parsed.
def format_time(value: str | None) -> str:
    dt = _parse_dt(value)
    if not dt:
        return "-"
    return dt.strftime("%b %d, %H:%M")


# Converts repository dict structure into template row list.
# Also appends pre-formatted datetime strings to each row.
def to_rows(jobs_dict: dict) -> list[dict]:
    rows = list(jobs_dict.values())

    # add formatted time fields once, so template uses *_fmt
    for r in rows:
        r["applied_time_fmt"] = format_time(r.get("applied_time"))
        r["viewed_time_fmt"] = format_time(r.get("viewed_time"))

    return rows


# Renders dashboard with filters, sorting, stats, and derived display fields.
@web.get("/")
def home():
    jobs = to_rows(read_jobs_csv(str(CSV_PATH)))

    # Filters
    search = request.args.get("search", "").strip().lower()
    viewed_only = request.args.get("viewed") == "1"
    applied_only = request.args.get("applied") == "1"
    downloaded_only = request.args.get("downloaded") == "1"

    # Sorting
    sort = (request.args.get("sort") or "").strip()          # "applied_time" | "viewed_time" | ""
    order = (request.args.get("order") or "desc").strip()    # "asc" | "desc"
    reverse = (order == "desc")

    filtered: list[dict] = []
    for row in jobs:
        company = str(row.get("company", ""))
        title = str(row.get("job_title", ""))
        location = str(row.get("location", ""))
        text = f"{company} {title} {location}".lower()

        if search and search not in text:
            continue
        if viewed_only and not row.get("viewed", False):
            continue
        if applied_only and not row.get("applied", False):
            continue
        if downloaded_only and not row.get("downloaded", False):
            continue

        filtered.append(row)

    # Apply sorting AFTER filtering
    if sort == "viewed_time":
        # put None values to the end (both asc/desc)
        filtered.sort(
            key=lambda r: (_parse_dt(r.get("viewed_time")) is None, _parse_dt(r.get("viewed_time")) or datetime.min),
            reverse=reverse,
        )
    elif sort == "applied_time":
        filtered.sort(
            key=lambda r: (_parse_dt(r.get("applied_time")) is None, _parse_dt(r.get("applied_time")) or datetime.min),
            reverse=reverse,
        )
    else:
        # default sorting (your original behavior)
        filtered.sort(
            key=lambda r: (
                str(r.get("company", "")).lower(),
                str(r.get("job_title", "")).lower(),
            )
        )

    stats = {
        "total": len(jobs),
        "applied": sum(1 for r in jobs if r.get("applied", False)),
        "viewed": sum(1 for r in jobs if r.get("viewed", False)),
        "downloaded": sum(1 for r in jobs if r.get("downloaded", False)),
    }

    return render_template(
        "dashboard.html",
        rows=filtered,
        stats=stats,
        search=search,
        viewed_only=viewed_only,
        applied_only=applied_only,
        downloaded_only=downloaded_only,
        csv_path=str(CSV_PATH),

        # pass current sort state so template can preserve it if you want
        sort=sort,
        order=order,
    )


if __name__ == "__main__":
    web.run(host="127.0.0.1", port=5000, debug=True)
