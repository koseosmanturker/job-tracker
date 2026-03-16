from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, jsonify

from repository import (
    read_jobs_csv,
    write_jobs_csv,
    toggle_downloaded_by_row_id,
    toggle_favorite_by_row_id,
)
from sync_service import run_sync, load_sync_state, SYNC_STATE_FILE


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
    rows = []
    for row_id, row in jobs_dict.items():
        item = dict(row)
        item["row_id"] = row_id
        rows.append(item)

    # add formatted time fields once, so template uses *_fmt
    for r in rows:
        r["applied_time_fmt"] = format_time(r.get("applied_time"))
        r["viewed_time_fmt"] = format_time(r.get("viewed_time"))

    return rows


# Triggers mail synchronization and redirects back to dashboard.
# The sync query is selected by sync_service (first full window, then incremental).
@web.post("/sync")
def sync_mails():
    sync_mode = (request.form.get("sync_mode") or "incremental").strip().lower()
    force_full = sync_mode == "full"
    summary = run_sync(csv_path=str(CSV_PATH), force_full=force_full)
    return redirect(
        url_for(
            "home",
            synced="1",
            sync_mode=sync_mode,
            processed=str(summary.get("processed", 0)),
            skipped=str(summary.get("skipped", 0)),
            rejected_marked=str(summary.get("rejected_marked", 0)),
        )
    )


# Toggles downloaded status for a row and returns JSON for AJAX UI update.
@web.post("/toggle-downloaded/<path:row_id>")
def toggle_downloaded(row_id: str):
    jobs = read_jobs_csv(str(CSV_PATH))
    success, downloaded = toggle_downloaded_by_row_id(jobs, row_id)
    if not success:
        return jsonify({"success": False, "error": "record_not_found"}), 404
    write_jobs_csv(str(CSV_PATH), jobs)
    return jsonify({"success": True, "downloaded": downloaded})


@web.post("/toggle-favorite/<path:row_id>")
def toggle_favorite(row_id: str):
    jobs = read_jobs_csv(str(CSV_PATH))
    success, favorite = toggle_favorite_by_row_id(jobs, row_id)
    if not success:
        return jsonify({"success": False, "error": "record_not_found"}), 404
    write_jobs_csv(str(CSV_PATH), jobs)
    return jsonify({"success": True, "favorite": favorite})


# Renders dashboard with filters, sorting, stats, and derived display fields.
def render_jobs_page(*, favorites_only: bool = False):
    jobs = to_rows(read_jobs_csv(str(CSV_PATH)))
    state_path = str(CSV_PATH.with_name(SYNC_STATE_FILE))
    sync_state = load_sync_state(state_path)
    last_sync_time_fmt = format_time(sync_state.get("last_synced_at"))

    # Filters
    search = request.args.get("search", "").strip().lower()
    viewed_only = request.args.get("viewed") == "1"
    applied_only = request.args.get("applied") == "1"
    downloaded_only = request.args.get("downloaded") == "1"
    rejected_only = request.args.get("rejected") == "1"
    synced = request.args.get("synced") == "1"
    sync_mode = (request.args.get("sync_mode") or "incremental").strip().lower()
    processed = int(request.args.get("processed", "0") or 0)
    skipped = int(request.args.get("skipped", "0") or 0)
    rejected_marked = int(request.args.get("rejected_marked", "0") or 0)

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
        if rejected_only and not row.get("rejected", False):
            continue
        if favorites_only and not row.get("favorite", False):
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

    applied_count = sum(1 for r in jobs if r.get("applied", False))
    viewed_count = sum(1 for r in jobs if r.get("viewed", False))
    downloaded_count = sum(1 for r in jobs if r.get("downloaded", False))
    rejected_count = sum(1 for r in jobs if r.get("rejected", False))
    favorites_count = sum(1 for r in jobs if r.get("favorite", False))

    # Rates are based on total applied jobs.
    def pct(value: int, total: int) -> str:
        if total <= 0:
            return "0.0%"
        return f"{(value / total) * 100:.1f}%"

    stats = {
        "applied": applied_count,
        "viewed": viewed_count,
        "downloaded": downloaded_count,
        "rejected": rejected_count,
        "favorites": favorites_count,
        "viewed_rate": pct(viewed_count, applied_count),
        "downloaded_rate": pct(downloaded_count, applied_count),
        "rejected_rate": pct(rejected_count, applied_count),
    }

    return render_template(
        "dashboard.html",
        rows=filtered,
        stats=stats,
        search=search,
        viewed_only=viewed_only,
        applied_only=applied_only,
        downloaded_only=downloaded_only,
        rejected_only=rejected_only,
        synced=synced,
        sync_mode=sync_mode,
        processed=processed,
        skipped=skipped,
        rejected_marked=rejected_marked,
        last_sync_time_fmt=last_sync_time_fmt,
        csv_path=str(CSV_PATH),
        favorites_only=favorites_only,
        page_title="Favorites" if favorites_only else "Your Career Agent",
        # page_subtitle=(
        #     "Starred job posts in one place."
        #     if favorites_only
        #     else "Track applied, viewed, downloaded, rejected, and favorite statuses in one place."
        # ),
        current_path="/favorites" if favorites_only else "/",
        sort=sort,
        order=order,
    )


@web.get("/")
def home():
    return render_jobs_page(favorites_only=False)


@web.get("/favorites")
def favorites():
    return render_jobs_page(favorites_only=True)


if __name__ == "__main__":
    web.run(host="127.0.0.1", port=5000, debug=True)
