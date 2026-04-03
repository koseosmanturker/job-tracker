import json
import os
import io
import hashlib
import re
from datetime import datetime
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request

from flask import Flask, abort, jsonify, redirect, render_template, request, send_from_directory, url_for

from linkedin_parser import normalize_text
from repository import (
    get_job_row_by_index,
    is_incomplete_job_row,
    list_incomplete_job_rows,
    read_jobs_csv,
    toggle_downloaded_by_row_id,
    toggle_favorite_by_row_id,
    toggle_follow_up_done_by_row_id,
    update_job_row_by_index,
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
GENERATED_DIR = BASE_DIR / "generated"
CV_CACHE_DIR = BASE_DIR / ".cv_optimizer_cache"

web = Flask(__name__)
GENERATED_DIR.mkdir(exist_ok=True)
CV_CACHE_DIR.mkdir(exist_ok=True)


def load_dotenv_file(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    try:
        lines = dotenv_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_key = key.strip()
        if not env_key or env_key in os.environ:
            continue
        env_value = value.strip().strip('"').strip("'")
        os.environ[env_key] = env_value


load_dotenv_file(BASE_DIR / ".env")


@web.get("/pngs/<path:filename>")
def png_asset(filename: str):
    return send_from_directory(BASE_DIR / "pngs", filename)


@web.get("/favicon.png")
def favicon_png():
    return send_from_directory(BASE_DIR / "pngs", "favicon.png", mimetype="image/png")


@web.get("/favicon.ico")
def favicon_ico():
    return redirect(url_for("favicon_png"))


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


def _clean_text(value: str) -> str:
    text = (value or "").replace("\x00", " ")
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _normalize_phrase(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9+#./ -]+", "", (value or "").strip().lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _dedupe_list(values: list[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        normalized = _normalize_phrase(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(value.strip())
    return result


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    try:
        import pdfplumber
    except ImportError as exc:
        raise RuntimeError("pdfplumber is not installed.") from exc

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
    except Exception as exc:
        raise ValueError("Invalid PDF or unreadable PDF content.") from exc

    text = _clean_text("\n\n".join(pages))
    if not text:
        raise ValueError("The uploaded PDF does not contain extractable text.")
    return text


def _compact_cv_text(cv_text: str, limit: int = 6000) -> str:
    lines = [line.strip() for line in cv_text.splitlines() if line.strip()]
    if not lines:
        return ""

    preferred = []
    fallback = []
    heading_pattern = re.compile(r"\b(experience|employment|work history|skills|education|summary|profile|projects|certifications)\b", re.I)
    bullet_pattern = re.compile(r"^[-*•]")

    for index, line in enumerate(lines):
        target = preferred if heading_pattern.search(line) or bullet_pattern.search(line) else fallback
        target.append(line)
        if heading_pattern.search(line):
            for next_line in lines[index + 1 : index + 4]:
                if next_line not in target:
                    target.append(next_line)

    merged = _dedupe_list(preferred + fallback)
    chunks = []
    total = 0
    for line in merged:
        piece = line if not chunks else f"\n{line}"
        if total + len(piece) > limit:
            break
        chunks.append(line)
        total += len(piece)
    return "\n".join(chunks)


def _compact_job_text(job_text: str, limit: int = 4500) -> str:
    cleaned = _clean_text(job_text)
    if len(cleaned) <= limit:
        return cleaned

    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    selected = []
    total = 0
    for line in lines:
        piece = line if not selected else f"\n{line}"
        if total + len(piece) > limit:
            break
        selected.append(line)
        total += len(piece)
    return "\n".join(selected)


def _extract_text_from_response(body: dict) -> str:
    output = body.get("output") or []
    for item in output:
        for content in item.get("content") or []:
            text = content.get("text")
            if text:
                return text.strip()
    raise RuntimeError("OpenAI returned an empty response.")


def _call_openai_responses(*, prompt: str, max_output_tokens: int = 700) -> str:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    payload = {
        "model": "gpt-5-mini",
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": prompt,
                    }
                ],
            }
        ],
        "max_output_tokens": max_output_tokens,
        "reasoning": {"effort": "minimal"},
    }

    req = urllib_request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=35) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        try:
            raw_error = exc.read().decode("utf-8")
            parsed_error = json.loads(raw_error)
            error_body = parsed_error.get("error", {}).get("message") or raw_error
        except Exception:
            error_body = exc.reason
        raise RuntimeError(f"OpenAI API error: {error_body}") from exc
    except TimeoutError as exc:
        raise RuntimeError("OpenAI request timed out.") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"OpenAI connection error: {exc.reason}") from exc

    return _extract_text_from_response(body)


def _call_openai_json(*, prompt: str, max_output_tokens: int = 700) -> dict:
    text = _call_openai_responses(prompt=prompt, max_output_tokens=max_output_tokens)
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("OpenAI returned invalid JSON.") from exc


def _extract_structured_cv_data(cv_excerpt: str) -> dict:
    prompt = (
        "Extract CV data into JSON only. No markdown. No hallucinations. "
        'Return exactly {"experience":[{"title":"","company":"","dates":"","bullets":[]}],"skills":[],"education":[{"degree":"","institution":"","dates":""}]}. '
        "Use only facts present in the CV text. Keep bullets short.\n\n"
        f"CV TEXT:\n{cv_excerpt}"
    )
    data = _call_openai_json(prompt=prompt, max_output_tokens=900)
    return {
        "experience": data.get("experience") or [],
        "skills": _dedupe_list([str(item).strip() for item in (data.get("skills") or []) if str(item).strip()]),
        "education": data.get("education") or [],
    }


def _extract_job_requirements(job_excerpt: str) -> dict:
    prompt = (
        "Extract job requirements into JSON only. No markdown. "
        'Return exactly {"required_skills":[],"keywords":[],"responsibilities":[]}. '
        "Use only the job description. Keep lists concise and deduplicated.\n\n"
        f"JOB DESCRIPTION:\n{job_excerpt}"
    )
    data = _call_openai_json(prompt=prompt, max_output_tokens=700)
    return {
        "required_skills": _dedupe_list([str(item).strip() for item in (data.get("required_skills") or []) if str(item).strip()]),
        "keywords": _dedupe_list([str(item).strip() for item in (data.get("keywords") or []) if str(item).strip()]),
        "responsibilities": _dedupe_list([str(item).strip() for item in (data.get("responsibilities") or []) if str(item).strip()]),
    }


def _match_cv_to_job(structured_cv: dict, job_requirements: dict) -> tuple[int, list[str], list[str]]:
    cv_skills_raw = structured_cv.get("skills") or []
    job_skills_raw = (job_requirements.get("required_skills") or []) + (job_requirements.get("keywords") or [])

    cv_skill_map = {_normalize_phrase(skill): skill for skill in cv_skills_raw if _normalize_phrase(skill)}
    job_skill_map = {_normalize_phrase(skill): skill for skill in job_skills_raw if _normalize_phrase(skill)}

    if not job_skill_map:
        return 0, [], []

    overlap = sorted(set(cv_skill_map) & set(job_skill_map))
    missing = sorted(set(job_skill_map) - set(cv_skill_map))
    score = round((len(overlap) / max(1, len(job_skill_map))) * 100)
    return score, [job_skill_map[item] for item in missing], [job_skill_map[item] for item in overlap]


def _rewrite_cv_for_job(*, structured_cv: dict, job_requirements: dict) -> dict:
    prompt = (
        "Rewrite the CV for ATS fit using JSON only. "
        "Do not invent experience, metrics, tools, employers, dates, degrees, or skills. "
        "Only improve wording using provided job keywords when they truthfully fit existing experience. "
        'Return exactly {"summary":"","skills":[],"experience":[{"title":"","company":"","dates":"","bullets":[]}],"education":[{"degree":"","institution":"","dates":""}]}. '
        "Keep bullets concise and ATS-friendly.\n\n"
        f"STRUCTURED CV:\n{json.dumps(structured_cv, ensure_ascii=True)}\n\n"
        f"JOB REQUIREMENTS:\n{json.dumps(job_requirements, ensure_ascii=True)}"
    )
    data = _call_openai_json(prompt=prompt, max_output_tokens=1400)
    return {
        "summary": str(data.get("summary") or "").strip(),
        "skills": _dedupe_list([str(item).strip() for item in (data.get("skills") or structured_cv.get("skills") or []) if str(item).strip()]),
        "experience": data.get("experience") or structured_cv.get("experience") or [],
        "education": data.get("education") or structured_cv.get("education") or [],
    }


def _render_optimized_cv_html(*, optimized_cv: dict, match_score: int, missing_skills: list[str], matched_skills: list[str]) -> str:
    return render_template(
        "optimized_cv.html",
        cv=optimized_cv,
        match_score=match_score,
        missing_skills=missing_skills,
        matched_skills=matched_skills,
    )


def _write_pdf_from_html(*, html_content: str, output_path: Path) -> None:
    try:
        from weasyprint import HTML
    except ImportError as exc:
        raise RuntimeError("WeasyPrint is not installed.") from exc

    try:
        HTML(string=html_content, base_url=str(BASE_DIR)).write_pdf(str(output_path))
    except Exception as exc:
        raise RuntimeError("Failed to generate PDF.") from exc


def _load_cache(cache_key: str) -> dict | None:
    cache_path = CV_CACHE_DIR / f"{cache_key}.json"
    if not cache_path.exists():
        return None
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_cache(cache_key: str, payload: dict) -> None:
    cache_path = CV_CACHE_DIR / f"{cache_key}.json"
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
    incomplete_csv_reviews = list_incomplete_job_rows(str(CSV_PATH))
    return {
        "current_path": current_path,
        "page_title": page_title,
        "page_title_html": page_title,
        "page_subtitle": page_subtitle,
        "last_sync_time_fmt": format_time(sync_state.get("last_synced_at")),
        "needs_review_count": len(pending_reviews) + len(incomplete_csv_reviews),
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
        followup_status = "viewed and downloaded, no response" if is_downloaded else "viewed, no response"
        items.append(
            {
                **row,
                "days_waiting": days_waiting,
                "followup_hint": reason,
                "followup_threshold": threshold_days,
                "followup_status": followup_status,
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


def generate_followup_email(*, job_title: str, company: str, days: int, status: str = "viewed, no response") -> str:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    safe_job_title = job_title.strip()
    safe_company = company.strip()
    safe_status = status.strip() or "viewed, no response"
    safe_days = max(0, int(days))

    payload = {
        "model": "gpt-5-mini",
        "messages": [
            {
                "role": "system",
                "content": (
                    "Write a concise professional follow-up email. "
                    "Use only the provided job title, company, days since application, and status. "
                    "Do not invent personal details, recruiter names, or job facts. "
                    "Keep it natural, polished, and under 5 sentences."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Job title: {safe_job_title}\n"
                    f"Company: {safe_company}\n"
                    f"Days since application: {safe_days}\n"
                    f"Status: {safe_status}\n\n"
                    "Write a short follow-up email that explicitly mentions the job title and company."
                ),
            },
        ],
        "max_completion_tokens": 180,
        "temperature": 0.6,
        "n": 1,
        "reasoning_effort": "minimal",
    }

    req = urllib_request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=20) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        try:
            raw_error = exc.read().decode("utf-8")
            parsed_error = json.loads(raw_error)
            error_body = parsed_error.get("error", {}).get("message") or raw_error
        except Exception:
            error_body = exc.reason
        raise RuntimeError(f"OpenAI API error: {error_body}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"OpenAI connection error: {exc.reason}") from exc

    choices = body.get("choices") or []
    message = choices[0].get("message", {}) if choices else {}
    content = (message.get("content") or "").strip()
    if not content:
        raise RuntimeError("OpenAI returned an empty response.")
    return content


@web.get("/generated/<path:filename>")
def generated_file(filename: str):
    return send_from_directory(GENERATED_DIR, filename)


@web.post("/optimize-cv")
def optimize_cv():
    job_description = _clean_text(request.form.get("job_description") or "")
    cv_file = request.files.get("cv_file")

    if not job_description:
        return jsonify({"error": "missing_job_description"}), 400
    if cv_file is None or not cv_file.filename:
        return jsonify({"error": "missing_cv_file"}), 400

    pdf_bytes = cv_file.read()
    if not pdf_bytes:
        return jsonify({"error": "empty_cv_file"}), 400

    cv_hash = hashlib.sha256(pdf_bytes).hexdigest()
    job_hash = hashlib.sha256(job_description.encode("utf-8")).hexdigest()
    cache_key = hashlib.sha256(f"{cv_hash}:{job_hash}".encode("utf-8")).hexdigest()

    cached = _load_cache(cache_key)
    cached_pdf_url = (cached or {}).get("pdf_url", "")
    cached_pdf_name = cached_pdf_url.rsplit("/", 1)[-1] if cached_pdf_url else ""
    if cached and cached_pdf_name and (GENERATED_DIR / cached_pdf_name).exists():
        return jsonify(cached)

    try:
        cv_text = _extract_pdf_text(pdf_bytes)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500

    cv_excerpt = _compact_cv_text(cv_text)
    job_excerpt = _compact_job_text(job_description)

    if not cv_excerpt:
        return jsonify({"error": "empty_cv_text"}), 400

    try:
        structured_cv = _extract_structured_cv_data(cv_excerpt)
        job_requirements = _extract_job_requirements(job_excerpt)
        match_score, missing_skills, matched_skills = _match_cv_to_job(structured_cv, job_requirements)
        optimized_cv = _rewrite_cv_for_job(structured_cv=structured_cv, job_requirements=job_requirements)
        html_content = _render_optimized_cv_html(
            optimized_cv=optimized_cv,
            match_score=match_score,
            missing_skills=missing_skills,
            matched_skills=matched_skills,
        )
        pdf_filename = f"cv_{cache_key[:12]}.pdf"
        _write_pdf_from_html(html_content=html_content, output_path=GENERATED_DIR / pdf_filename)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 502

    response_payload = {
        "match_score": match_score,
        "missing_skills": missing_skills,
        "improved_cv_html": html_content,
        "pdf_url": url_for("generated_file", filename=pdf_filename),
    }
    _save_cache(cache_key, response_payload)
    return jsonify(response_payload)


@web.post("/generate-followup")
def generate_followup():
    payload = request.get_json(silent=True) or {}
    job_title = (payload.get("job_title") or "").strip()
    company = (payload.get("company") or "").strip()
    status = (payload.get("status") or "viewed, no response").strip()
    raw_days = payload.get("days", 0)

    try:
        days = int(raw_days)
    except (TypeError, ValueError):
        return jsonify({"error": "invalid_days"}), 400

    if not job_title or not company:
        return jsonify({"error": "missing_fields"}), 400

    try:
        email = generate_followup_email(job_title=job_title, company=company, days=days, status=status)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500

    return jsonify({"email": email})


def render_jobs_page(*, favorites_only: bool = False):
    jobs = to_rows(read_jobs_csv(str(CSV_PATH)))
    context = build_base_context(
        current_path="/favorites" if favorites_only else "/",
        page_title="Favorites" if favorites_only else "Career Intelligence Tool",
        page_subtitle=(
            "Starred roles that are still worth attention."
            if favorites_only
            else "Track your pipeline, surface follow-ups, and clean up parser misses."
        ),
    )
    if not favorites_only:
        context["page_title_html"] = 'Career <span class="titleGradient">Intelligence</span> Tool'

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


@web.get("/cv-optimizer")
def cv_optimizer():
    context = build_base_context(
        current_path="/cv-optimizer",
        page_title="CV Optimizer",
        page_subtitle="Upload a CV, paste a target job description, and generate an ATS-friendly tailored version.",
    )
    return render_template(
        "cv_optimizer.html",
        **context,
    )


@web.get("/needs-review")
def needs_review():
    pending_parser_reviews = list_needs_review(str(NEEDS_REVIEW_PATH), status="pending")
    incomplete_csv_reviews = list_incomplete_job_rows(str(CSV_PATH))
    context = build_base_context(
        current_path="/needs-review",
        page_title="Needs Review",
        page_subtitle="Parser misses and incomplete CSV rows land here so you can repair them quickly.",
    )
    return render_template(
        "needs_review.html",
        review_rows=pending_parser_reviews,
        csv_review_rows=incomplete_csv_reviews,
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
        initial_values={},
        format_time_with_date=format_time_with_date,
        **context,
    )


@web.route("/needs-review/csv/<int:row_index>", methods=["GET", "POST"])
def csv_review_detail(row_index: int):
    item = get_job_row_by_index(str(CSV_PATH), row_index)
    if not item or not is_incomplete_job_row(item):
        return redirect(url_for("needs_review"))

    if request.method == "POST":
        action = (request.form.get("action") or "apply").strip().lower()
        if action == "dismiss":
            return redirect(url_for("needs_review"))

        updated_row = update_job_row_by_index(
            str(CSV_PATH),
            row_index,
            {
                "company": request.form.get("company"),
                "job_title": request.form.get("job_title"),
                "location": request.form.get("location"),
                "job_url": request.form.get("job_url"),
            },
        )
        if not updated_row:
            abort(404)
        if is_incomplete_job_row(updated_row):
            return redirect(url_for("csv_review_detail", row_index=row_index))
        return redirect(url_for("needs_review"))

    context = build_base_context(
        current_path="/needs-review",
        page_title="Edit CSV Row",
        page_subtitle="Fill in the missing job details and the row will leave the review queue automatically.",
    )
    return render_template(
        "review_detail.html",
        item={
            "reason": "missing_csv_fields",
            "event_time": item.get("applied_time") or item.get("viewed_time") or "",
            "subject": "Incomplete CSV Row",
            "body_text": "",
            "body_preview": "",
            "source_kind": "csv",
            "csv_row_index": row_index,
        },
        initial_values={
            "company": item.get("company", ""),
            "job_title": item.get("job_title", ""),
            "location": item.get("location", ""),
            "job_url": item.get("job_url", ""),
        },
        format_time_with_date=format_time_with_date,
        **context,
    )


if __name__ == "__main__":
    web.run(host="127.0.0.1", port=5000, debug=True)
