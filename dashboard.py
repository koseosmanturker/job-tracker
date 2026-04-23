import json
import os
import io
import hashlib
import re
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlencode

from flask import Flask, abort, jsonify, redirect, render_template, request, send_from_directory, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash
from google_auth_oauthlib.flow import Flow

from linkedin_parser import normalize_text
from gmail_client import SCOPES
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
USER_REGISTRATIONS_PATH = BASE_DIR / "user_registrations.json"

web = Flask(__name__)
web.secret_key = os.environ.get("FLASK_SECRET_KEY", "job-tracker-dev-secret")
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
os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")


@web.get("/pngs/<path:filename>")
def png_asset(filename: str):
    return send_from_directory(BASE_DIR / "pngs", filename)


@web.get("/tracksy-logo.svg")
def tracksy_logo():
    return send_from_directory(BASE_DIR, "tracksy-logo.svg", mimetype="image/svg+xml")


@web.get("/favicon.png")
def favicon_png():
    return send_from_directory(BASE_DIR / "pngs", "favicon.png", mimetype="image/png")


@web.get("/favicon.ico")
def favicon_ico():
    return redirect(url_for("tracksy_logo"))


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


def load_user_registrations() -> list[dict]:
    if not USER_REGISTRATIONS_PATH.exists():
        return []
    try:
        data = json.loads(USER_REGISTRATIONS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    return data if isinstance(data, list) else []


def save_user_registrations(rows: list[dict]) -> None:
    USER_REGISTRATIONS_PATH.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_gmail_oauth_flow(redirect_uri: str, state: str | None = None) -> Flow:
    flow = Flow.from_client_secrets_file(
        str(BASE_DIR / "credentials.json"),
        scopes=SCOPES,
        state=state,
    )
    flow.redirect_uri = redirect_uri
    return flow


def build_local_oauth_redirect_uri() -> str:
    return url_for("oauth2callback", _external=True)


PLAN_LABELS = {
    "starter": "Starter",
    "serious": "Serious",
    "advanced": "Advanced",
}


def normalize_plan(value: str | None) -> str:
    plan = (value or "").strip().lower()
    return plan if plan in PLAN_LABELS else "starter"


def build_demo_jobs_store() -> dict[str, dict]:
    companies = [
        ("Notion Labs", "Senior Product Designer", "Remote, US"),
        ("Northgrid", "Operations Analyst", "Berlin, Germany"),
        ("RemoteWave", "Growth Marketing Lead", "London, UK"),
        ("Atlas AI", "Product Marketing Manager", "New York, US"),
        ("Bloomstack", "Customer Success Strategist", "Amsterdam, NL"),
        ("Nova Ledger", "Business Analyst", "Remote, EU"),
        ("Brightpath", "UX Researcher", "Austin, US"),
        ("Crest Labs", "Revenue Operations Manager", "Dublin, IE"),
        ("Orbit Scale", "Lifecycle Marketing Manager", "Remote, US"),
        ("Luma Health", "Strategy Associate", "Toronto, CA"),
        ("Peakline", "Program Manager", "Remote, UK"),
        ("Verve Tech", "Content Marketing Lead", "Barcelona, ES"),
        ("Harbor AI", "Founders Associate", "Istanbul, TR"),
        ("Metric Hive", "Partnerships Manager", "Paris, FR"),
        ("Sparklane", "Business Development Analyst", "Remote, US"),
        ("Northstar Bio", "Operations Specialist", "Boston, US"),
        ("Astra Cloud", "Customer Marketing Manager", "Remote, CA"),
        ("Fieldstone", "Employer Branding Lead", "Munich, DE"),
        ("Craftflow", "Product Operations Analyst", "Remote, EU"),
        ("Echo Mobility", "Growth Operations Manager", "Stockholm, SE"),
        ("Signal Forge", "User Acquisition Lead", "Remote, UK"),
        ("Kindred Pay", "CRM Manager", "Lisbon, PT"),
        ("PulseGrid", "Community Manager", "Remote, US"),
        ("Northbeam", "Commercial Analyst", "Zurich, CH"),
        ("Clearmint", "Brand Strategist", "Milan, IT"),
        ("Open Harbor", "Program Operations Associate", "Remote, TR"),
        ("Helio Works", "Marketplace Manager", "Warsaw, PL"),
        ("Sunline AI", "Growth Designer", "Prague, CZ"),
        ("Skyfoundry", "Sales Enablement Specialist", "Remote, US"),
        ("Brighter Day", "Marketing Analyst", "Copenhagen, DK"),
    ]
    favorite_indexes = {1, 6, 11, 18, 24}
    rejected_indexes = {8, 16, 27}
    follow_up_done_indexes = {0, 2, 5, 9, 13, 17, 21}
    now = datetime.utcnow().replace(microsecond=0)
    jobs: dict[str, dict] = {}
    for index, (company, title, location) in enumerate(companies):
        viewed_at = now - timedelta(days=index + 2, hours=(index % 5) * 3)
        applied_at = viewed_at - timedelta(days=(index % 4) + 1, hours=2)
        jobs[f"demo-{index+1:02d}"] = {
            "company": company,
            "job_title": title,
            "location": location,
            "job_url": f"https://www.linkedin.com/jobs/view/{770000000 + index}",
            "applied": True,
            "applied_time": applied_at.isoformat(),
            "viewed": True,
            "viewed_time": viewed_at.isoformat(),
            "downloaded": index < 20,
            "rejected": index in rejected_indexes,
            "favorite": index in favorite_indexes,
            "follow_up_done": index in follow_up_done_indexes,
        }
    return jobs


def build_demo_context(*, current_path: str, page_title: str, page_subtitle: str, jobs_rows: list[dict]) -> dict:
    pending_followups = [
        row for row in build_followup_items(jobs_rows) if not row.get("follow_up_done", False)
    ]
    return {
        "current_path": current_path,
        "page_title": page_title,
        "page_title_html": page_title,
        "page_subtitle": page_subtitle,
        "last_sync_time_fmt": "Demo snapshot",
        "follow_up_count": len(pending_followups),
        "needs_review_count": 0,
        "current_user_name": session.get("user_name", "Demo User"),
        "current_user_email": session.get("user_email", "demo@jobtracker.app"),
        "current_user_package": session.get("user_package", "advanced"),
        "current_user_package_label": PLAN_LABELS.get(session.get("user_package", "advanced"), "Advanced"),
        "jobs_href": "/demo/jobs",
        "favorites_href": "/demo/favorites",
        "follow_up_href": "/demo/follow-up",
        "ai_cv_studio_href": "/demo/ai-cv-studio",
        "needs_review_href": "/demo/jobs",
        "show_sync_controls": False,
        "demo_mode": True,
        "demo_exit_href": "/",
    }


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


SNAPSHOT_STOPWORDS = {
    "a", "about", "after", "all", "also", "an", "and", "any", "as", "at", "be", "because", "been", "but", "by",
    "can", "could", "do", "for", "from", "have", "if", "in", "into", "is", "it", "its", "may", "more", "must",
    "nice", "not", "of", "on", "or", "our", "role", "should", "some", "the", "their", "this", "to", "using",
    "we", "will", "with", "you", "your", "years", "year", "plus", "preferred", "required", "requirements",
    "responsibilities", "responsibility", "experience", "knowledge", "skills", "skill", "work", "working",
    "team", "ability", "strong", "good", "excellent", "including", "across", "within", "through", "over",
    "need", "needs", "looking", "seeking", "candidate", "candidates", "position", "job",
    "ve", "veya", "ile", "için", "icin", "olan", "olarak", "bir", "bu", "çok", "cok", "gibi", "gore", "göre",
    "tercihen", "zorunlu", "gereken", "gereklidir", "deneyim", "tecrube", "tecrübe", "bilgi", "beceri",
}


def _tokenize_for_snapshot(text: str) -> list[str]:
    cleaned = _normalize_phrase(text)
    if not cleaned:
        return []
    return re.findall(r"[a-z0-9+#./-]{2,}", cleaned)


def _clean_snapshot_term(value: str) -> str:
    text = _normalize_phrase(value)
    text = re.sub(r"\b(?:and|or|with|using|in|of|the|a|an|ve|veya|ile)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -,:;/")
    return text


def _split_requirement_fragments(text: str) -> list[str]:
    parts = re.split(r",|/|;|\band\b|\bor\b|\bve\b|\bveya\b", text, flags=re.I)
    return [_clean_snapshot_term(part) for part in parts if _clean_snapshot_term(part)]


def _extract_snapshot_job_terms(job_text: str) -> tuple[list[str], list[str]]:
    normalized_job = _clean_text(job_text)
    requirement_candidates = []

    cue_patterns = [
        r"(?:must have|required|requirements|experience with|proficient in|expertise in|knowledge of|familiar with|skills?|tech stack|tools?)[:\s]+([^\n]+)",
        r"(?:we need|we are looking for|looking for|seeking|must know|must be familiar with)[:\s]+([^\n]+)",
        r"(?:aranan nitelikler|gereksinimler|gereken yetkinlikler|teknolojiler|araçlar|beceriler)[:\s]+([^\n]+)",
    ]
    for pattern in cue_patterns:
        for match in re.findall(pattern, normalized_job, flags=re.I):
            requirement_candidates.extend(_split_requirement_fragments(match))

    tech_candidates = re.findall(r"\b[a-zA-Z][a-zA-Z0-9+#./-]{1,}\b", normalized_job)
    tech_terms = []
    for item in tech_candidates:
        normalized = _clean_snapshot_term(item)
        if len(normalized) < 2 or normalized in SNAPSHOT_STOPWORDS:
            continue
        if any(ch in normalized for ch in "+#./") or normalized.isupper():
            tech_terms.append(normalized)

    token_counts = Counter(
        token for token in _tokenize_for_snapshot(normalized_job)
        if token not in SNAPSHOT_STOPWORDS and len(token) > 2
    )
    keyword_candidates = [token for token, _ in token_counts.most_common(30)]

    required_terms = _dedupe_list(requirement_candidates + tech_terms)[:12]
    keyword_terms = _dedupe_list(keyword_candidates + tech_terms + requirement_candidates)
    keyword_terms = [term for term in keyword_terms if term not in {_normalize_phrase(item) for item in required_terms}]
    return required_terms, keyword_terms[:20]


def _cv_contains_term(cv_text: str, cv_tokens: set[str], term: str) -> bool:
    normalized_term = _normalize_phrase(term)
    if not normalized_term:
        return False
    if " " in normalized_term:
        return normalized_term in cv_text
    return normalized_term in cv_tokens


def _calculate_cv_snapshot(cv_text: str, job_text: str) -> dict:
    normalized_cv = _normalize_phrase(cv_text)
    cv_tokens = set(_tokenize_for_snapshot(normalized_cv))
    required_terms, keyword_terms = _extract_snapshot_job_terms(job_text)

    matched_required = [term for term in required_terms if _cv_contains_term(normalized_cv, cv_tokens, term)]
    missing_required = [term for term in required_terms if term not in matched_required]
    matched_keywords = [term for term in keyword_terms if _cv_contains_term(normalized_cv, cv_tokens, term)]
    missing_keywords = [term for term in keyword_terms if term not in matched_keywords]

    section_markers = (
        ("experience", 0.35),
        ("skills", 0.30),
        ("education", 0.20),
        ("projects", 0.15),
    )
    structure_score = 0.0
    for marker, weight in section_markers:
        if marker in normalized_cv:
            structure_score += weight
    structure_score = min(1.0, structure_score)

    required_coverage = len(matched_required) / max(1, len(required_terms)) if required_terms else 0.0
    keyword_coverage = len(matched_keywords) / max(1, len(keyword_terms)) if keyword_terms else 0.0

    raw_score = (required_coverage * 0.65) + (keyword_coverage * 0.25) + (structure_score * 0.10)
    snapshot_score = round(raw_score * 100)

    missing_skills = _dedupe_list(missing_required + missing_keywords)[:8]
    return {
        "score": snapshot_score,
        "missing_skills": missing_skills,
        "required_terms_total": len(required_terms),
        "required_terms_matched": len(matched_required),
        "keyword_terms_total": len(keyword_terms),
        "keyword_terms_matched": len(matched_keywords),
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
    jobs = to_rows(read_jobs_csv(str(CSV_PATH)))
    pending_followups = [
        row for row in build_followup_items(jobs) if not row.get("follow_up_done", False)
    ]
    return {
        "current_path": current_path,
        "page_title": page_title,
        "page_title_html": page_title,
        "page_subtitle": page_subtitle,
        "last_sync_time_fmt": format_time(sync_state.get("last_synced_at")),
        "follow_up_count": len(pending_followups),
        "needs_review_count": len(pending_reviews) + len(incomplete_csv_reviews),
        "current_user_name": session.get("user_name", ""),
        "current_user_email": session.get("user_email", ""),
        "current_user_package": session.get("user_package", ""),
        "current_user_package_label": PLAN_LABELS.get(session.get("user_package", ""), ""),
        "jobs_href": "/jobs",
        "favorites_href": "/favorites",
        "follow_up_href": "/follow-up",
        "ai_cv_studio_href": "/ai-cv-studio",
        "needs_review_href": "/needs-review",
        "show_sync_controls": True,
        "demo_mode": False,
        "demo_exit_href": "/",
    }


def build_sort_url(*, current_path: str, query_args, active_sort: str, active_order: str, target_sort: str, target_order: str) -> str:
    params = query_args.to_dict(flat=True)
    if active_sort == target_sort and active_order == target_order:
        params.pop("sort", None)
        params.pop("order", None)
    else:
        params["sort"] = target_sort
        params["order"] = target_order

    query = urlencode(params)
    return f"{current_path}?{query}" if query else current_path


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
    snapshot = _calculate_cv_snapshot(cv_text, job_description)

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
        return jsonify({"error": str(exc), "snapshot": snapshot}), 502

    response_payload = {
        "match_score": match_score,
        "missing_skills": missing_skills,
        "snapshot": snapshot,
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


def render_jobs_page(*, favorites_only: bool = False, rows_override: list[dict] | None = None, context_override: dict | None = None):
    jobs = [dict(row) for row in rows_override] if rows_override is not None else to_rows(read_jobs_csv(str(CSV_PATH)))
    context = context_override or build_base_context(
        current_path="/favorites" if favorites_only else "/jobs",
        page_title="Favorites" if favorites_only else "Tracksy",
        page_subtitle=(
            "Starred roles that are still worth attention."
            if favorites_only
            else "Track your pipeline, surface follow-ups, and clean up parser misses."
        ),
    )
    if not favorites_only:
        context["page_title_html"] = '<span class="titleGradient">Tracksy</span>'

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
        applied_time_asc_url=build_sort_url(
            current_path=context["current_path"],
            query_args=request.args,
            active_sort=sort,
            active_order=order,
            target_sort="applied_time",
            target_order="asc",
        ),
        applied_time_desc_url=build_sort_url(
            current_path=context["current_path"],
            query_args=request.args,
            active_sort=sort,
            active_order=order,
            target_sort="applied_time",
            target_order="desc",
        ),
        viewed_time_asc_url=build_sort_url(
            current_path=context["current_path"],
            query_args=request.args,
            active_sort=sort,
            active_order=order,
            target_sort="viewed_time",
            target_order="asc",
        ),
        viewed_time_desc_url=build_sort_url(
            current_path=context["current_path"],
            query_args=request.args,
            active_sort=sort,
            active_order=order,
            target_sort="viewed_time",
            target_order="desc",
        ),
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
    return send_from_directory(BASE_DIR, "landing.html")


@web.get("/demo")
def demo_home():
    return redirect("/demo/jobs")


@web.get("/demo/jobs")
def demo_jobs():
    rows = to_rows(build_demo_jobs_store())
    context = build_demo_context(
        current_path="/jobs",
        page_title="Tracksy Demo",
        page_subtitle="A guided demo dataset with 30 viewed roles, 20 downloaded CVs, and 5 saved favorites.",
        jobs_rows=rows,
    )
    context["page_title_html"] = '<span class="titleGradient">Tracksy</span> <span class="titleSolid">Demo</span>'
    return render_jobs_page(favorites_only=False, rows_override=rows, context_override=context)


@web.get("/demo/favorites")
def demo_favorites():
    rows = to_rows(build_demo_jobs_store())
    context = build_demo_context(
        current_path="/favorites",
        page_title="Favorites Demo",
        page_subtitle="Five highlighted roles are pre-saved so users can see how a focused shortlist feels in the product.",
        jobs_rows=rows,
    )
    return render_jobs_page(favorites_only=True, rows_override=rows, context_override=context)


@web.get("/demo/follow-up")
def demo_follow_up():
    rows = to_rows(build_demo_jobs_store())
    followups = build_followup_items(rows)
    context = build_demo_context(
        current_path="/follow-up",
        page_title="Follow-up Demo",
        page_subtitle="See how the app prioritizes viewed jobs and suggests the next outreach window automatically.",
        jobs_rows=rows,
    )
    return render_template(
        "insights.html",
        followups=followups,
        synced=False,
        processed=0,
        skipped=0,
        needs_review_added=0,
        manual_corrections_used=0,
        format_time_with_date=format_time_with_date,
        **context,
    )


@web.get("/demo/ai-cv-studio")
def demo_cv_optimizer():
    rows = to_rows(build_demo_jobs_store())
    context = build_demo_context(
        current_path="/ai-cv-studio",
        page_title="AI CV Studio Demo",
        page_subtitle="A preview of the CV tailoring workflow users unlock inside the product.",
        jobs_rows=rows,
    )
    return render_template("cv_optimizer.html", **context)


@web.route("/login", methods=["GET", "POST"])
def login():
    form_data = {
        "gmail": "",
    }
    errors: list[str] = []

    if request.method == "POST":
        form_data["gmail"] = (request.form.get("gmail") or "").strip().lower()
        password = request.form.get("password") or ""

        if not form_data["gmail"]:
            errors.append("Gmail address is required.")
        if not password:
            errors.append("Password is required.")

        if not errors:
            registrations = load_user_registrations()
            user = next(
                (row for row in registrations if (row.get("gmail") or "").lower() == form_data["gmail"]),
                None,
            )
            if not user or not check_password_hash(user.get("password_hash", ""), password):
                errors.append("Invalid Gmail or password.")
            else:
                session["user_email"] = user.get("gmail", "")
                session["user_name"] = user.get("name", "")
                session["user_package"] = normalize_plan(user.get("package"))
                return redirect(url_for("jobs"))

    return render_template(
        "login.html",
        form_data=form_data,
        errors=errors,
    )


@web.get("/logout")
def logout():
    session.pop("user_email", None)
    session.pop("user_name", None)
    session.pop("user_package", None)
    return redirect(url_for("home"))


@web.route("/profile", methods=["GET", "POST"])
def profile():
    user_email = session.get("user_email", "")
    if not user_email:
        return redirect(url_for("login"))

    registrations = load_user_registrations()
    user_index = -1
    user: dict | None = None
    for index, row in enumerate(registrations):
        if (row.get("gmail") or "").strip().lower() == user_email.strip().lower():
            user_index = index
            user = row
            break

    if user is None:
        session.pop("user_email", None)
        session.pop("user_name", None)
        session.pop("user_package", None)
        return redirect(url_for("login"))

    form_data = {
        "name": user.get("name", ""),
        "surname": user.get("surname", ""),
        "age": str(user.get("age", "")) if user.get("age", "") != "" else "",
        "gmail": user.get("gmail", ""),
        "linkedin_language": user.get("linkedin_language", ""),
        "package": normalize_plan(user.get("package")),
        "created_at": user.get("created_at", ""),
        "api_permission_granted": bool(user.get("api_permission_granted")),
    }
    errors: list[str] = []
    success = False

    if request.method == "POST":
        form_data.update(
            {
                "name": (request.form.get("name") or "").strip(),
                "surname": (request.form.get("surname") or "").strip(),
                "age": (request.form.get("age") or "").strip(),
                "linkedin_language": (request.form.get("linkedin_language") or "").strip(),
                "package": normalize_plan(request.form.get("package")),
            }
        )
        password = request.form.get("password") or ""
        password_repeat = request.form.get("password_repeat") or ""

        if not form_data["name"]:
            errors.append("Name is required.")
        if not form_data["surname"]:
            errors.append("Surname is required.")

        try:
            age_value = int(form_data["age"])
            if age_value < 16 or age_value > 100:
                errors.append("Age must be between 16 and 100.")
        except ValueError:
            errors.append("Age must be a valid number.")
            age_value = user.get("age", "")

        if not form_data["linkedin_language"]:
            errors.append("LinkedIn language is required.")

        if password or password_repeat:
            if len(password) < 8:
                errors.append("Password must be at least 8 characters.")
            if password != password_repeat:
                errors.append("Password and re-type password must match.")

        if not errors:
            updated_user = {
                **user,
                "name": form_data["name"],
                "surname": form_data["surname"],
                "age": age_value,
                "linkedin_language": form_data["linkedin_language"],
                "package": form_data["package"],
                "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            }
            if password:
                updated_user["password_hash"] = generate_password_hash(password)
            registrations[user_index] = updated_user
            save_user_registrations(registrations)
            session["user_name"] = updated_user.get("name", "")
            session["user_package"] = normalize_plan(updated_user.get("package"))
            user = updated_user
            form_data["age"] = str(updated_user.get("age", ""))
            success = True

    context = build_base_context(
        current_path="/profile",
        page_title="Profile",
        page_subtitle="View and update the account details connected to your job tracking workspace.",
    )
    return render_template(
        "profile.html",
        **context,
        form_data=form_data,
        errors=errors,
        success=success,
        plan_labels=PLAN_LABELS,
    )


@web.route("/register", methods=["GET", "POST"])
def register():
    pending_registration = session.get("pending_registration") or {}
    selected_plan = normalize_plan(request.args.get("plan") or pending_registration.get("package"))
    form_data = {
        "name": pending_registration.get("name", ""),
        "surname": pending_registration.get("surname", ""),
        "age": str(pending_registration.get("age", "")) if pending_registration.get("age", "") != "" else "",
        "gmail": pending_registration.get("gmail", ""),
        "linkedin_language": pending_registration.get("linkedin_language", "Turkish") or "Turkish",
        "api_permission": "yes" if pending_registration.get("api_permission_granted") else "",
        "package": selected_plan,
    }
    errors: list[str] = []
    success = False
    oauth_error = (request.args.get("oauth_error") or "").strip()
    oauth_error_detail = session.pop("oauth_last_error", "")

    if request.method == "POST":
        form_data = {
            "name": (request.form.get("name") or "").strip(),
            "surname": (request.form.get("surname") or "").strip(),
            "age": (request.form.get("age") or "").strip(),
            "gmail": (request.form.get("gmail") or "").strip().lower(),
            "linkedin_language": (request.form.get("linkedin_language") or "").strip(),
            "api_permission": (request.form.get("api_permission") or "").strip(),
            "package": normalize_plan(request.form.get("package")),
        }
        password = request.form.get("password") or ""
        password_repeat = request.form.get("password_repeat") or ""

        if not form_data["name"]:
            errors.append("Name is required.")
        if not form_data["surname"]:
            errors.append("Surname is required.")

        try:
            age_value = int(form_data["age"])
            if age_value < 16 or age_value > 100:
                errors.append("Age must be between 16 and 100.")
        except ValueError:
            errors.append("Age must be a valid number.")

        gmail_value = form_data["gmail"]
        if not gmail_value:
            errors.append("Gmail address is required.")
        elif not re.fullmatch(r"[A-Za-z0-9._%+-]+@gmail\.com", gmail_value):
            errors.append("Please enter a valid Gmail address.")

        if not password:
            errors.append("Password is required.")
        elif len(password) < 8:
            errors.append("Password must be at least 8 characters.")

        if password != password_repeat:
            errors.append("Password and re-type password must match.")

        if form_data["linkedin_language"] != "Turkish":
            errors.append("Only Turkish LinkedIn emails are supported right now.")

        if form_data["api_permission"] != "yes":
            errors.append("You need to allow Gmail API access to continue.")

        existing = load_user_registrations()
        if any((row.get("gmail") or "").lower() == gmail_value for row in existing):
            errors.append("This Gmail address is already registered.")

        if not errors:
            session["pending_registration"] = {
                "name": form_data["name"],
                "surname": form_data["surname"],
                "age": age_value,
                "gmail": gmail_value,
                "password_hash": generate_password_hash(password),
                "linkedin_language": form_data["linkedin_language"],
                "api_permission_granted": True,
                "package": form_data["package"],
            }
            return redirect(url_for("connect_gmail"))

    return render_template(
        "register.html",
        form_data=form_data,
        errors=errors,
        success=success,
        oauth_error=oauth_error,
        oauth_error_detail=oauth_error_detail,
        package_label=PLAN_LABELS.get(form_data["package"], "Starter"),
    )


@web.get("/connect-gmail")
def connect_gmail():
    pending_registration = session.get("pending_registration")
    if not pending_registration:
        return redirect(url_for("register"))

    credentials_path = BASE_DIR / "credentials.json"
    if not credentials_path.exists():
        return redirect(url_for("register", oauth_error="missing_credentials"))

    flow = build_gmail_oauth_flow(build_local_oauth_redirect_uri())
    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    session["oauth_state"] = state
    session["oauth_code_verifier"] = getattr(flow, "code_verifier", None)
    return redirect(authorization_url)


@web.get("/oauth2callback")
def oauth2callback():
    pending_registration = session.get("pending_registration")
    oauth_state = session.get("oauth_state")
    oauth_code_verifier = session.get("oauth_code_verifier")
    if not pending_registration or not oauth_state:
        return redirect(url_for("register", oauth_error="missing_state"))

    if request.args.get("error"):
        return redirect(url_for("register", oauth_error="access_denied"))

    try:
        flow = build_gmail_oauth_flow(build_local_oauth_redirect_uri(), state=oauth_state)
        if oauth_code_verifier:
            flow.code_verifier = oauth_code_verifier
        flow.fetch_token(authorization_response=request.url)
    except Exception as exc:
        session["oauth_last_error"] = str(exc)
        return redirect(url_for("register", oauth_error="oauth_failed"))

    token_path = BASE_DIR / "token.json"
    token_path.write_text(flow.credentials.to_json(), encoding="utf-8")
    existing = load_user_registrations()
    gmail_value = (pending_registration.get("gmail") or "").lower()
    if not any((row.get("gmail") or "").lower() == gmail_value for row in existing):
        existing.append(
            {
                **pending_registration,
                "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            }
        )
        save_user_registrations(existing)
    session["user_email"] = pending_registration.get("gmail", "")
    session["user_name"] = pending_registration.get("name", "")
    session["user_package"] = normalize_plan(pending_registration.get("package"))
    session.pop("oauth_state", None)
    session.pop("oauth_code_verifier", None)
    session.pop("pending_registration", None)
    session.pop("oauth_last_error", None)
    return redirect(url_for("jobs"))


@web.get("/jobs")
def jobs():
    return render_jobs_page(favorites_only=False)


@web.get("/favorites")
def favorites():
    return render_jobs_page(favorites_only=True)


@web.get("/insights")
def insights_legacy():
    return redirect(url_for("follow_up"))


@web.get("/follow-up")
def follow_up():
    jobs = to_rows(read_jobs_csv(str(CSV_PATH)))
    followups = build_followup_items(jobs)
    context = build_base_context(
        current_path="/follow-up",
        page_title="Follow-up",
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


@web.get("/ai-cv-studio")
def cv_optimizer():
    context = build_base_context(
        current_path="/ai-cv-studio",
        page_title="AI CV Studio",
        page_subtitle="Upload a CV, paste a target job description, and generate a tailored version for that role.",
    )
    return render_template(
        "cv_optimizer.html",
        **context,
    )


@web.get("/cv-optimizer")
def cv_optimizer_legacy():
    return redirect(url_for("cv_optimizer"))


@web.get("/needs-review")
def needs_review():
    pending_parser_reviews = list_needs_review(str(NEEDS_REVIEW_PATH), status="pending")
    incomplete_csv_reviews = list_incomplete_job_rows(str(CSV_PATH))
    context = build_base_context(
        current_path="/needs-review",
        page_title="Needs Review",
        page_subtitle="Parser misses and incomplete job records land here so you can repair them quickly.",
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


@web.route("/needs-review/job/<int:row_index>", methods=["GET", "POST"])
def job_record_review_detail(row_index: int):
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
            return redirect(url_for("job_record_review_detail", row_index=row_index))
        return redirect(url_for("needs_review"))

    context = build_base_context(
        current_path="/needs-review",
        page_title="Edit Job Record",
        page_subtitle="Fill in the missing job details and the record will leave the review queue automatically.",
    )
    return render_template(
        "review_detail.html",
        item={
            "reason": "missing_csv_fields",
            "event_time": item.get("applied_time") or item.get("viewed_time") or "",
            "subject": "Incomplete Job Record",
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
