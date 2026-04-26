import html
import re
import unicodedata
from typing import Optional, List, Tuple

URL_RE = re.compile(r"https?://\S+")

# Turkish patterns (run on normalize_text output)
APPLIED_RE = re.compile(r"basvurunuz\s+(?P<company>.+?)\s+sirketine\s+gonderildi", re.IGNORECASE)
VIEWED_RE = re.compile(r"basvurunuz\s+(?P<company>.+?)\s+tarafindan\s+goruntulendi", re.IGNORECASE)
REJECTED_SUBJECT_RE = re.compile(r"(?P<company>.+?)\s+sirketindeki\s+(?P<title>.+?)\s+basvurunuz\b", re.IGNORECASE)

# English patterns (run on normalize_text of subject only)
APPLIED_RE_EN = re.compile(r"your\s+application\s+was\s+sent\s+to\s+(?P<company>.+)", re.IGNORECASE)
VIEWED_RE_EN = re.compile(r"your\s+application\s+was\s+viewed\s+by\s+(?P<company>.+)", re.IGNORECASE)
REJECTED_SUBJECT_RE_EN = re.compile(
    r"your\s+application\s+to\s+(?P<title>.+?)\s+at\s+(?P<company>.+)",
    re.IGNORECASE,
)

NOISE_PHRASES = (
    "basvuru tarihi",
    "sizin icin onerilen benzer is ilanlarini kesfedin",
    "tum benzer isleri goster",
    "is ilanini yayinlayan kisi",
    "is ilanini goruntuleyin",
    "ilgilenebileceginiz benzer is ilanlarini inceleyin",
    "ozgecmis ve profil ile basvurun",
    "bu e-posta",
    "buna neden yer verdigimizi ogrenin",
    "daha basarili olmak icin bu adimlari atabilirsiniz",
    "ardindan profilinizi guclendirin",
    "profilinizi guncelleyin",
    "linkedin bildirim e-postalari aliyorsunuz",
    "aboneligi iptal",
    "gelen kutusu",
)

NOISE_PHRASES_EN = (
    "explore similar jobs",
    "show all similar jobs",
    "view job posting",
    "job poster",
    "you're receiving linkedin",
    "youre receiving linkedin",
    "find out why we included",
    "similar jobs you may be interested",
    "apply with resume",
    "apply with your resume",
    "strengthen your profile",
    "update your profile",
    "take these steps",
    "this email was sent",
    "unsubscribe",
)


# Normalizes free text to make matching resilient against Turkish diacritics,
# mixed encodings, punctuation noise, and inconsistent whitespace.
def normalize_text(value: str) -> str:
    text = (value or "").strip().casefold()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    repl = {
        "Ã…Å¸": "s",
        "Ã„Â±": "i",
        "Ã„Å¸": "g",
        "ÃƒÂ¼": "u",
        "ÃƒÂ¶": "o",
        "ÃƒÂ§": "c",
    }
    for src, dst in repl.items():
        text = text.replace(src, dst)
    text = text.replace("?", " ")
    return " ".join(text.split())


# Converts human-entered truthy values into bool.
def str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "evet"}


# Cleans trailing punctuation from extracted company names.
def normalize_company(name: str) -> str:
    return re.sub(r"[.,;:!]+$", "", (name or "").strip())


# Classifies whether an email represents applied and/or viewed event.
# Tries Turkish patterns on merged text, English patterns on subject only.
def classify_email(subject: str, body_text: str) -> Tuple[Optional[str], bool, bool]:
    merged = normalize_text(f"{subject}\n{body_text}")
    subject_n = normalize_text(subject)
    applied = False
    viewed = False
    company = None

    # Turkish patterns on merged normalized text
    m_applied = APPLIED_RE.search(merged)
    if m_applied:
        applied = True
        company = m_applied.group("company")

    m_viewed = VIEWED_RE.search(merged)
    if m_viewed:
        viewed = True
        company = company or m_viewed.group("company")

    # English patterns on normalized subject only (company ends the subject line)
    if not company:
        m_applied_en = APPLIED_RE_EN.search(subject_n)
        if m_applied_en:
            applied = True
            company = m_applied_en.group("company").strip()

        m_viewed_en = VIEWED_RE_EN.search(subject_n)
        if m_viewed_en:
            viewed = True
            company = company or m_viewed_en.group("company").strip()

    if company:
        company = normalize_company(company)

    return company, applied, viewed


# Converts raw HTML/plain body into normalized line list.
def body_to_lines(body_text: str) -> List[str]:
    if not body_text:
        return []
    text = body_text
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|li|tr|td|th|h[1-6])>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text).replace("\r", "\n")
    lines: List[str] = []
    for raw in text.split("\n"):
        line = re.sub(r"\s+", " ", raw).strip()
        if line:
            if re.fullmatch(r"[-_=*~\s]{4,}", line):
                continue
            lines.append(line)
    return lines


# Detects "applied date" lines so they are not mistaken as job title.
def looks_like_applied_date_line(text: str) -> bool:
    t = normalize_text(text)
    if re.search(r"\b\d{1,2}\s+\w+\s+tarihinde\s+basvuruldu\b", t):
        return True
    # English: "Applied on Jan 15" or "Applied on 15 January"
    if re.search(r"\bapplied\s+on\b", t):
        return True
    return False


# Filters known footer/marketing/system lines that should not affect parsing.
def is_noise_line(text: str) -> bool:
    t = normalize_text(text)
    if not t:
        return True
    if any(p in t for p in NOISE_PHRASES):
        return True
    if any(p in t for p in NOISE_PHRASES_EN):
        return True
    if "benzer" in t and "ilan" in t:
        return True
    if ("basvuru" in t and "tarih" in t) or re.search(r"b\w*\s*vuru\s+tarih", t):
        return True
    if "ilanini" in t and "yayinlayan" in t:
        return True
    if "daha" in t and "adim" in t and ("basar" in t or "basari" in t):
        return True
    if "daha" in t and "olmak" in t and "bu" in t and "atabilirsiniz" in t:
        return True
    if looks_like_applied_date_line(text):
        return True
    if re.fullmatch(r"[-_=*~\s]{4,}", text):
        return True
    return False


# Tests whether a line can be accepted as location in current parsing context.
def is_probable_location_line(text: str, company: str) -> bool:
    if not text:
        return False
    if is_noise_line(text):
        return False
    t = normalize_text(text)
    if "http://" in text or "https://" in text or "@" in text:
        return False
    # Turkish negative signals
    if "sirketindeki" in t or "basvurunuz" in t:
        return False
    if "is ilanini goruntuleyin" in t:
        return False
    # English negative signals
    if "your application" in t or "was viewed by" in t or "was sent to" in t:
        return False
    if "view job posting" in t or "hiring team" in t:
        return False
    if normalize_text(company) == t:
        return False
    return True


# Rejects title candidates that are too noisy, too short/long, or semantically wrong.
def is_bad_title(text: str, company: str) -> bool:
    t = normalize_text(text)
    company_n = normalize_text(company)
    blacklist = (
        # Turkish
        "linkedin", "basvurunuz", "goruntulendi", "gonderildi",
        "ise alim takimi", "tarihinde basvuruldu", "gelen kutusu", "aboneligi iptal",
        # English
        "your application", "was sent to", "was viewed by",
        "view job posting", "hiring team", "applied on",
        "explore similar jobs", "similar jobs",
        "youre receiving", "unsubscribe",
        "open",
    )
    if any(k in t for k in blacklist):
        return True
    if is_noise_line(text):
        return True
    if "http://" in text or "https://" in text or "@" in text:
        return True
    if company_n and t == company_n:
        return True
    if len(text) < 3 or len(text) > 140:
        return True
    if looks_like_applied_date_line(text):
        return True
    return False


# Extracts the best job title and location pair from subject/body.
def extract_job_title_and_location(subject: str, body_text: str, company: str) -> Tuple[str, str]:
    lines = body_to_lines(body_text)

    # Trim at section-break markers (both languages)
    for stop_idx, line in enumerate(lines):
        ln_n = normalize_text(line)
        if "sizin icin onerilen benzer is ilanlarini kesfedin" in ln_n:
            lines = lines[:stop_idx]
            break
        if "explore similar jobs" in ln_n or "similar jobs you may" in ln_n:
            lines = lines[:stop_idx]
            break

    company_n = normalize_text(company)
    title = ""
    location = ""

    def clean_location(value: str) -> str:
        loc = (value or "").strip()
        loc = re.sub(rf"^\s*{re.escape(company)}\s*[^A-Za-z0-9]+\s*", "", loc, flags=re.IGNORECASE)
        return loc.strip(" -|:;,.")

    def first_location_in_range(start_idx: int, end_idx: int) -> str:
        company_cmp = normalize_company(normalize_text(company))
        title_cmp = normalize_company(normalize_text(title))
        candidates: List[str] = []
        for j in range(max(0, start_idx), min(end_idx, len(lines))):
            cand = lines[j].strip()
            if is_probable_location_line(cand, company):
                cand_cmp = normalize_company(normalize_text(cand))
                if cand_cmp == company_cmp or (title_cmp and cand_cmp == title_cmp):
                    continue
                cleaned = clean_location(cand)
                if cleaned:
                    candidates.append(cleaned)
        if not candidates:
            return ""
        for cand in candidates:
            if "," in cand:
                return cand
        return candidates[0]

    subject_n = normalize_text(subject)

    # ── Turkish applied path ──────────────────────────────────────────────────
    if "sirketine gonderildi" in subject_n:
        event_idx = 0
        for i, ln in enumerate(lines):
            ln_n = normalize_text(ln)
            if "basvurunuz" in ln_n and "gonderildi" in ln_n:
                event_idx = i
                break

        applied_end_idx = min(event_idx + 20, len(lines))
        for i, ln in enumerate(lines[event_idx + 1:applied_end_idx], start=event_idx + 1):
            if "basvuru" in normalize_text(ln) and "tarih" in normalize_text(ln):
                applied_end_idx = i
                break

        for j in range(event_idx + 1, applied_end_idx):
            cand = lines[j].strip()
            if is_bad_title(cand, company):
                continue
            if normalize_text(cand) == company_n:
                continue
            title = cand
            break

        if title:
            title_n = normalize_text(title)
            t_idx = -1
            for i, ln in enumerate(lines[event_idx + 1:applied_end_idx], start=event_idx + 1):
                if normalize_text(ln) == title_n:
                    t_idx = i
                    break
            if t_idx >= 0:
                location = first_location_in_range(t_idx + 1, applied_end_idx)
            if not location:
                location = first_location_in_range(event_idx + 1, applied_end_idx)

        if title:
            return title, location

    # ── English applied path ──────────────────────────────────────────────────
    if "your application was sent to" in subject_n and not title:
        event_idx = 0
        for i, ln in enumerate(lines):
            ln_n = normalize_text(ln)
            if "application was sent to" in ln_n:
                event_idx = i
                break

        applied_end_idx = min(event_idx + 20, len(lines))
        for i, ln in enumerate(lines[event_idx + 1:applied_end_idx], start=event_idx + 1):
            if looks_like_applied_date_line(ln):
                applied_end_idx = i
                break

        for j in range(event_idx + 1, applied_end_idx):
            cand = lines[j].strip()
            if is_bad_title(cand, company):
                continue
            if normalize_text(cand) == company_n:
                continue
            title = cand
            break

        if title:
            title_n = normalize_text(title)
            t_idx = -1
            for i, ln in enumerate(lines[event_idx + 1:applied_end_idx], start=event_idx + 1):
                if normalize_text(ln) == title_n:
                    t_idx = i
                    break
            if t_idx >= 0:
                location = first_location_in_range(t_idx + 1, applied_end_idx)
            if not location:
                location = first_location_in_range(event_idx + 1, applied_end_idx)

        if title:
            return title, location

    # ── English viewed path ───────────────────────────────────────────────────
    if "your application was viewed by" in subject_n and not title:
        # Body typically: [event line] → [title] → [company] → [location]
        event_idx = 0
        for i, ln in enumerate(lines):
            ln_n = normalize_text(ln)
            if "application was viewed by" in ln_n or "viewed your application" in ln_n:
                event_idx = i
                break

        search_end = min(event_idx + 15, len(lines))
        for j in range(event_idx + 1, search_end):
            cand = lines[j].strip()
            if is_bad_title(cand, company):
                continue
            if normalize_text(cand) == company_n:
                continue
            title = cand
            break

        if title:
            title_n = normalize_text(title)
            t_idx = -1
            for i, ln in enumerate(lines[:search_end]):
                if normalize_text(ln) == title_n:
                    t_idx = i
                    break
            if t_idx >= 0:
                location = first_location_in_range(t_idx + 1, t_idx + 5)

        if title:
            return title, location

    # ── Hiring-team pattern (Turkish & English) ───────────────────────────────
    for i, line in enumerate(lines):
        ln_n = normalize_text(line)
        if "ise alim takimi" in ln_n or "hiring team" in ln_n:
            title_idx = -1
            for j in range(i + 1, min(i + 4, len(lines))):
                cand = lines[j].strip()
                if is_bad_title(cand, company):
                    continue
                title = cand
                title_idx = j
                break
            if title:
                location = first_location_in_range(title_idx + 1, i + 7)
                break

    # ── Bullet-separator pattern: "Company · Location" ────────────────────────
    for idx, line in enumerate(lines):
        if title and location:
            break
        left = ""
        right = ""

        parts = re.split(r"\s*[·•]\s*", line, maxsplit=1)
        if len(parts) == 2:
            left, right = parts
        else:
            m_sep = re.match(rf"^\s*({re.escape(company)})\s*[^A-Za-z0-9]+\s*(.+)$", line, flags=re.IGNORECASE)
            if m_sep:
                left, right = m_sep.group(1), m_sep.group(2)

        if not left or not right:
            continue

        left_n = normalize_text(left)
        if company_n and company_n not in left_n and left_n not in company_n:
            continue
        location = clean_location(right)
        for j in range(idx - 1, max(-1, idx - 5), -1):
            cand = lines[j].strip()
            if not is_bad_title(cand, company):
                title = cand
                break
        if title:
            break

    # ── Turkish fallback: "şirketindeki" in subject ───────────────────────────
    if not title:
        m = re.search(r"sirketindeki\s+(?P<title>.+?)\s+basvurunuz", subject_n)
        if m:
            title = m.group("title").strip()

    # ── Company-mention scan ──────────────────────────────────────────────────
    if not title:
        for i, line in enumerate(lines):
            if normalize_text(company) not in normalize_text(line):
                continue
            for j in range(i + 1, min(i + 6, len(lines))):
                cand = lines[j].strip()
                if is_bad_title(cand, company):
                    continue
                title = cand
                break
            if title:
                break

    # ── Turkish date-proximity fallback ──────────────────────────────────────
    if not title:
        for i, line in enumerate(lines):
            if "basvuru" in normalize_text(line) and "tarih" in normalize_text(line):
                for j in range(i - 1, max(-1, i - 6), -1):
                    cand = lines[j].strip()
                    if is_bad_title(cand, company):
                        continue
                    title = cand
                    break
                if title:
                    break

    # ── Location from title position ──────────────────────────────────────────
    if title and not location:
        title_n = normalize_text(title)
        title_idx = -1
        for i, line in enumerate(lines):
            if normalize_text(line) == title_n:
                title_idx = i
                break
        if title_idx >= 0:
            location = first_location_in_range(title_idx + 1, title_idx + 5)

    if title and looks_like_applied_date_line(title):
        m = re.search(r"sirketindeki\s+(?P<title>.+?)\s+basvurunuz", subject_n)
        if m:
            title = m.group("title").strip()

    return title, location


# Converts noisy LinkedIn URLs into canonical stable form.
def normalize_job_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    cleaned = url.replace("&amp;", "&").strip()
    m = re.search(r"linkedin\.com/(?:comm/)?jobs/view/(\d+)", cleaned)
    if m:
        return f"https://www.linkedin.com/jobs/view/{m.group(1)}"
    return cleaned.split("?", 1)[0]


# Extracts best candidate job URL from body text.
def extract_job_url(body_text: str) -> Optional[str]:
    if not body_text:
        return None
    normalized = body_text.replace("&amp;", "&")
    urls = [u.rstrip(').,;>"\'') for u in URL_RE.findall(normalized)]
    if not urls:
        return None
    for url in urls:
        if re.search(r"linkedin\.com/(?:comm/)?jobs/view/\d+", url):
            return normalize_job_url(url)
    for url in urls:
        if "linkedin.com" in url and "/jobs/view/" in url:
            return normalize_job_url(url)
    return normalize_job_url(urls[0])


# Parses LinkedIn job ID from URL when available.
def extract_job_id(job_url: str) -> str:
    m = re.search(r"linkedin\.com/(?:comm/)?jobs/view/(\d+)", job_url or "")
    return m.group(1) if m else ""


# Chooses a human-friendly company display name from raw subject/body.
def extract_company_display_name(subject: str, body_text: str, company_normalized: str) -> str:
    target = normalize_company(normalize_text(company_normalized))
    if not target:
        return company_normalized or ""

    candidates: List[str] = []

    # Turkish subject patterns
    m_subject_applied = re.search(
        r"başvurunuz\s+(?P<company>.+?)\s+şirketine\s+gönderildi",
        subject,
        flags=re.IGNORECASE,
    )
    if m_subject_applied:
        candidates.append(m_subject_applied.group("company").strip())

    m_subject_viewed = re.search(
        r"başvurunuz\s+(?P<company>.+?)\s+tarafından\s+görüntülendi",
        subject,
        flags=re.IGNORECASE,
    )
    if m_subject_viewed:
        candidates.append(m_subject_viewed.group("company").strip())

    # English subject patterns
    m_subject_applied_en = re.search(
        r"your\s+application\s+was\s+sent\s+to\s+(?P<company>.+)",
        subject,
        flags=re.IGNORECASE,
    )
    if m_subject_applied_en:
        raw = normalize_company(m_subject_applied_en.group("company").strip())
        if normalize_company(normalize_text(raw)) == target:
            candidates.append(raw)

    m_subject_viewed_en = re.search(
        r"your\s+application\s+was\s+viewed\s+by\s+(?P<company>.+)",
        subject,
        flags=re.IGNORECASE,
    )
    if m_subject_viewed_en:
        raw = normalize_company(m_subject_viewed_en.group("company").strip())
        if normalize_company(normalize_text(raw)) == target:
            candidates.append(raw)

    for ln in body_to_lines(body_text):
        cand = ln.strip()
        if normalize_company(normalize_text(cand)) == target:
            candidates.append(cand)

    def score(name: str) -> tuple[int, int, int]:
        has_upper = 1 if any(ch.isupper() for ch in name) else 0
        has_punct = 1 if any(ch in ".-&" for ch in name) else 0
        return (has_upper, has_punct, len(name))

    if candidates:
        return max(candidates, key=score)

    return company_normalized or ""


# Extracts rejection event payload from subject.
# Handles both Turkish and English LinkedIn rejection subjects.
def extract_rejected_event(subject: str) -> Tuple[Optional[str], Optional[str]]:
    subject_n = normalize_text(subject)

    # Turkish: "<company> şirketindeki <title> başvurunuz"
    m = REJECTED_SUBJECT_RE.search(subject_n)
    if m:
        company = normalize_company(m.group("company"))
        title = (m.group("title") or "").strip()
        return company or None, title or None

    # English: "Your application to <title> at <company>"
    m_en = REJECTED_SUBJECT_RE_EN.search(subject)
    if m_en:
        company = normalize_company(m_en.group("company").strip())
        title = (m_en.group("title") or "").strip()
        return company or None, title or None

    return None, None
