from datetime import datetime
from pathlib import Path

from flask import Flask, abort, jsonify, redirect, render_template, request, url_for

from linkedin_parser import normalize_text
from repository import (
    read_jobs_csv,
    toggle_downloaded_by_row_id,
    toggle_favorite_by_row_id,
    toggle_follow_up_done_by_row_id,
    upsert_job_csv,
    write_jobs_csv,
)
from review_repository import (
    MANUAL_CORRECTIONS_FILE,
    NEEDS_REVIEW_FILE,
    get_review_item,
    list_needs_review,
    resolve_review_item,
    save_manual_correction,
)
from sync_service import SYNC_STATE_FILE, build_full_window_query, load_sync_state, run_sync


BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "jobs.csv"
NEEDS_REVIEW_PATH = BASE_DIR / NEEDS_REVIEW_FILE
MANUAL_CORRECTIONS_PATH = BASE_DIR / MANUAL_CORRECTIONS_FILE

web = Flask(__name__)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None

    s = str(value).strip()
    if not s or s == "-":
        return None

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

    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def format_time(value: str | None) -> str:
    dt = _parse_dt(value)
    if not dt:
        return "-"
    return dt.strftime("%b %d, %H:%M")


def format_time_with_date(value: str | None) -> str:
    dt = _parse_dt(value)
    if not dt:
        return "-"
    return dt.strftime("%b %d, %Y %H:%M")


def to_rows(jobs_dict: dict) -> list[dict]:
    rows = []
    for row_id, row in jobs_dict.items():
        item = dict(row)
        item["row_id"] = row_id
        item["applied_time_fmt"] = format_time(item.get("applied_time"))
        item["viewed_time_fmt"] = format_time(item.get("viewed_time"))
        rows.append(item)
    return rows


def build_base_context(*, current_path: str, page_title: str, page_subtitle: str) -> dict:
    state_path = str(CSV_PATH.with_name(SYNC_STATE_FILE))
    sync_state = load_sync_state(state_path)
    pending_reviews = list_needs_review(str(NEEDS_REVIEW_PATH), status="pending")
    return {
        "current_path": current_path,
        "page_title": page_title,
        "page_subtitle": page_subtitle,
        "last_sync_time_fmt": format_time(sync_state.get("last_synced_at")),
        "needs_review_count": len(pending_reviews),
    }


def build_followup_items(rows: list[dict]) -> list[dict]:
    now = datetime.now()
    items = []
    for row in rows:
        if row.get("rejected") or not row.get("viewed"):
            continue

        viewed_dt = _parse_dt(row.get("viewed_time")) or _parse_dt(row.get("applied_time"))
        if not viewed_dt:
            continue

        naive_viewed = viewed_dt.replace(tzinfo=None) if viewed_dt.tzinfo else viewed_dt
        days_waiting = (now - naive_viewed).days
        is_downloaded = bool(row.get("downloaded"))
        threshold_days = 7 if is_downloaded else 14
        if days_waiting < threshold_days:
            continue

        reason = (
            f"Viewed and downloaded {days_waiting} days ago with no rejection yet. This one is ready for a follow-up."
            if is_downloaded
            else f"Viewed {days_waiting} days ago with no rejection yet. Consider a follow-up now."
        )
        items.append(
            {
                **row,
                "days_waiting": days_waiting,
                "followup_hint": reason,
                "followup_threshold": threshold_days,
            }
        )

    return sorted(items, key=lambda row: (row["days_waiting"], row.get("company", "").lower(), row.get("job_title", "").lower()))


@web.post("/sync")
def sync_mails():
    sync_mode = (request.form.get("sync_mode") or "incremental").strip().lower()
    force_full = sync_mode == "full"
    query = None
    if force_full:
        try:
            full_sync_days = int(request.form.get("full_sync_days", "365") or 365)
        except ValueError:
            full_sync_days = 365
        query = build_full_window_query(full_sync_days)
    summary = run_sync(csv_path=str(CSV_PATH), force_full=force_full, query=query)
    redirect_endpoint = (request.form.get("next_endpoint") or "home").strip() or "home"
    return redirect(
        url_for(
            redirect_endpoint,
            synced="1",
            sync_mode=sync_mode,
            processed=str(summary.get("processed", 0)),
            skipped=str(summary.get("skipped", 0)),
            rejected_marked=str(summary.get("rejected_marked", 0)),
            needs_review_added=str(summary.get("needs_review_added", 0)),
            manual_corrections_used=str(summary.get("manual_corrections_used", 0)),
        )
    )


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


@web.post("/toggle-follow-up/<path:row_id>")
def toggle_follow_up(row_id: str):
    jobs = read_jobs_csv(str(CSV_PATH))
    success, follow_up_done = toggle_follow_up_done_by_row_id(jobs, row_id)
    if not success:
        return jsonify({"success": False, "error": "record_not_found"}), 404
    write_jobs_csv(str(CSV_PATH), jobs)
    return jsonify({"success": True, "follow_up_done": follow_up_done})


def render_jobs_page(*, favorites_only: bool = False):
    jobs = to_rows(read_jobs_csv(str(CSV_PATH)))
    context = build_base_context(
        current_path="/favorites" if favorites_only else "/",
        page_title="Favorites" if favorites_only else "Your Career Agent",
        page_subtitle=(
            "Starred roles that are still worth attention."
            if favorites_only
            else "Track your pipeline, surface follow-ups, and clean up parser misses."
        ),
    )

    search = request.args.get("search", "").strip().lower()
    viewed_only = request.args.get("viewed") == "1"
    downloaded_only = request.args.get("downloaded") == "1"
    rejected_only = request.args.get("rejected") == "1"
    sort = (request.args.get("sort") or "").strip()
    order = (request.args.get("order") or "desc").strip()
    reverse = order == "desc"
    synced = request.args.get("synced") == "1"
    processed = int(request.args.get("processed", "0") or 0)
    skipped = int(request.args.get("skipped", "0") or 0)
    needs_review_added = int(request.args.get("needs_review_added", "0") or 0)
    manual_corrections_used = int(request.args.get("manual_corrections_used", "0") or 0)

    filtered = []
    for row in jobs:
        text = f"{row.get('company', '')} {row.get('job_title', '')} {row.get('location', '')}".lower()
        if search and search not in text:
            continue
        if viewed_only and not row.get("viewed"):
            continue
        if downloaded_only and not row.get("downloaded"):
            continue
        if rejected_only and not row.get("rejected"):
            continue
        if favorites_only and not row.get("favorite"):
            continue
        filtered.append(row)

    if sort == "applied_time":
        filtered.sort(
            key=lambda row: (_parse_dt(row.get("applied_time")) is None, _parse_dt(row.get("applied_time")) or datetime.min),
            reverse=reverse,
        )
    elif sort == "viewed_time":
        filtered.sort(
            key=lambda row: (_parse_dt(row.get("viewed_time")) is None, _parse_dt(row.get("viewed_time")) or datetime.min),
            reverse=reverse,
        )
    else:
        filtered.sort(key=lambda row: (str(row.get("company", "")).lower(), str(row.get("job_title", "")).lower()))

    applied_count = sum(1 for r in jobs if r.get("applied", False))
    viewed_count = sum(1 for r in jobs if r.get("viewed", False))
    downloaded_count = sum(1 for r in jobs if r.get("downloaded", False))
    rejected_count = sum(1 for r in jobs if r.get("rejected", False))
    favorites_count = sum(1 for r in jobs if r.get("favorite", False))

    def pct(value: int, total: int) -> str:
        if total <= 0:
            return "0.0%"
        return f"{(value / total) * 100:.1f}%"

    return render_template(
        "dashboard.html",
        rows=filtered,
        stats={
            "applied": applied_count,
            "viewed": viewed_count,
            "downloaded": downloaded_count,
            "rejected": rejected_count,
            "favorites": favorites_count,
            "viewed_rate": pct(viewed_count, applied_count),
            "downloaded_rate": pct(downloaded_count, applied_count),
            "rejected_rate": pct(rejected_count, applied_count),
        },
        search=search,
        viewed_only=viewed_only,
        downloaded_only=downloaded_only,
        rejected_only=rejected_only,
        sort=sort,
        order=order,
        favorites_only=favorites_only,
        synced=synced,
        processed=processed,
        skipped=skipped,
        needs_review_added=needs_review_added,
        manual_corrections_used=manual_corrections_used,
        **context,
    )


@web.get("/")
def home():
    return render_jobs_page(favorites_only=False)


@web.get("/favorites")
def favorites():
    return render_jobs_page(favorites_only=True)


@web.get("/insights")
def insights():
    jobs = to_rows(read_jobs_csv(str(CSV_PATH)))
    followups = build_followup_items(jobs)
    context = build_base_context(
        current_path="/insights",
        page_title="Insights",
        page_subtitle="Work through follow-ups in viewed-date order and mark them done as you go.",
    )
    return render_template(
        "insights.html",
        followups=followups,
        synced=request.args.get("synced") == "1",
        processed=int(request.args.get("processed", "0") or 0),
        skipped=int(request.args.get("skipped", "0") or 0),
        needs_review_added=int(request.args.get("needs_review_added", "0") or 0),
        manual_corrections_used=int(request.args.get("manual_corrections_used", "0") or 0),
        format_time_with_date=format_time_with_date,
        **context,
    )


@web.get("/needs-review")
def needs_review():
    context = build_base_context(
        current_path="/needs-review",
        page_title="Needs Review",
        page_subtitle="Parser misses land here so you can repair them and teach the system.",
    )
    return render_template(
        "needs_review.html",
        review_rows=list_needs_review(str(NEEDS_REVIEW_PATH), status="pending"),
        format_time_with_date=format_time_with_date,
        **context,
    )


@web.route("/needs-review/<review_id>", methods=["GET", "POST"])
def review_detail(review_id: str):
    item = get_review_item(str(NEEDS_REVIEW_PATH), review_id)
    if not item:
        abort(404)

    if request.method == "POST":
        action = (request.form.get("action") or "apply").strip().lower()
        if action == "dismiss":
            resolve_review_item(str(NEEDS_REVIEW_PATH), review_id, "Dismissed manually.")
            return redirect(url_for("needs_review"))

        company = (request.form.get("company") or "").strip()
        job_title = (request.form.get("job_title") or "").strip()
        location = (request.form.get("location") or "").strip()
        job_url = (request.form.get("job_url") or "").strip()
        event_type = (request.form.get("event_type") or "applied").strip().lower()

        if company and job_title:
            jobs = read_jobs_csv(str(CSV_PATH))
            incoming = {
                "company": company,
                "job_title": job_title,
                "location": location,
                "job_url": job_url,
                "applied": event_type in {"applied", "viewed", "rejected"},
                "applied_time": item.get("event_time", ""),
                "viewed": event_type == "viewed",
                "viewed_time": item.get("event_time", "") if event_type == "viewed" else "",
                "downloaded": False,
                "rejected": event_type == "rejected",
                "favorite": False,
                "follow_up_done": False,
            }
            upsert_job_csv(jobs, incoming)
            write_jobs_csv(str(CSV_PATH), jobs)
            save_manual_correction(
                str(MANUAL_CORRECTIONS_PATH),
                subject=item.get("subject", ""),
                body_text=item.get("body_text", ""),
                corrected_fields={
                    "company": company,
                    "job_title": job_title,
                    "location": location,
                    "job_url": job_url,
                    "applied": incoming["applied"],
                    "viewed": incoming["viewed"],
                    "rejected": incoming["rejected"],
                },
            )
            resolve_review_item(str(NEEDS_REVIEW_PATH), review_id, f"Corrected as {event_type}.")
            return redirect(url_for("needs_review"))

    context = build_base_context(
        current_path="/needs-review",
        page_title="Manual Correction",
        page_subtitle="Repair one parser miss and store the correction for future syncs.",
    )
    return render_template(
        "review_detail.html",
        item=item,
        format_time_with_date=format_time_with_date,
        **context,
    )


if __name__ == "__main__":
    web.run(host="127.0.0.1", port=5000, debug=True)
