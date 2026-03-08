import html
import re
import unicodedata
from typing import Optional, List, Tuple

URL_RE = re.compile(r"https?://\S+")
APPLIED_RE = re.compile(r"basvurunuz\s+(?P<company>.+?)\s+sirketine\s+gonderildi", re.IGNORECASE)
VIEWED_RE = re.compile(r"basvurunuz\s+(?P<company>.+?)\s+tarafindan\s+goruntulendi", re.IGNORECASE)
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


# Normalizes free text to make matching resilient against Turkish diacritics,
# mixed encodings, punctuation noise, and inconsistent whitespace.
def normalize_text(value: str) -> str:
    text = (value or "").strip().casefold()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    repl = {
        "ÅŸ": "s",
        "Ä±": "i",
        "ÄŸ": "g",
        "Ã¼": "u",
        "Ã¶": "o",
        "Ã§": "c",
    }
    for src, dst in repl.items():
        text = text.replace(src, dst)
    text = text.replace("?", " ")
    return " ".join(text.split())


# Converts human-entered truthy values into bool.
# Supports common CLI/CSV values from both English and Turkish usage.
def str_to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "evet"}


# Cleans trailing punctuation from extracted company names.
# This stabilizes key generation and deduplication across emails.
def normalize_company(name: str) -> str:
    return re.sub(r"[.,;:!]+$", "", (name or "").strip())


# Classifies whether an email represents applied and/or viewed event.
# Also extracts the company candidate from known LinkedIn subject/body patterns.
def classify_email(subject: str, body_text: str) -> Tuple[Optional[str], bool, bool]:
    text = normalize_text(f"{subject}\n{body_text}")
    applied = False
    viewed = False
    company = None

    m_applied = APPLIED_RE.search(text)
    if m_applied:
        applied = True
        company = m_applied.group("company")

    m_viewed = VIEWED_RE.search(text)
    if m_viewed:
        viewed = True
        company = company or m_viewed.group("company")

    if company:
        company = normalize_company(company)

    return company, applied, viewed


# Converts raw HTML/plain body into normalized line list.
# The parser strips tags, removes noisy separator rows, and returns meaningful
# single-line text blocks for downstream title/location heuristics.
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


# Heuristic checker for lines that look like location information.
# It is intentionally broad because location formatting varies heavily.
def looks_like_location(text: str) -> bool:
    t = normalize_text(text)
    if "turkiye" in t or "istanbul" in t or "ankara" in t:
        return True
    if "polonya" in t or "almanya" in t or "ingiltere" in t or "usa" in t:
        return True
    if "metropol bolgesi" in t or "bolgesi" in t:
        return True
    if "uzaktan" in t or "remote" in t or "hybrid" in t:
        return True
    if "," in text:
        return True
    return False


# Detects "applied date" lines so they are not mistaken as job title.
def looks_like_applied_date_line(text: str) -> bool:
    t = normalize_text(text)
    return bool(re.search(r"\b\d{1,2}\s+\w+\s+tarihinde\s+basvuruldu\b", t))


# Tests whether a line can be accepted as location in current parsing context.
# The function excludes URLs, event messages, and pure company duplicates.
def is_probable_location_line(text: str, company: str) -> bool:
    if not text:
        return False
    t = normalize_text(text)
    if "http://" in text or "https://" in text or "@" in text:
        return False
    if "sirketindeki" in t or "basvurunuz" in t:
        return False
    if "is ilanini goruntuleyin" in t:
        return False
    if normalize_text(company) == t:
        return False
    if "Â·" in text or "â€¢" in text:
        return True
    if looks_like_location(text):
        return True
    return False


# Filters known footer/marketing/system lines that should not affect parsing.
def is_noise_line(text: str) -> bool:
    t = normalize_text(text)
    if not t:
        return True
    if any(p in t for p in NOISE_PHRASES):
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


# Rejects title candidates that are too noisy, too short/long, or semantically wrong.
# This guards against false positives from event lines and email chrome text.
def is_bad_title(text: str, company: str) -> bool:
    t = normalize_text(text)
    company_n = normalize_text(company)
    blacklist = (
        "linkedin",
        "basvurunuz",
        "goruntulendi",
        "gonderildi",
        "ise alim takimi",
        "tarihinde basvuruldu",
        "gelen kutusu",
        "aboneligi iptal",
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
    if " Â· " in text:
        return True
    if len(text) < 3 or len(text) > 140:
        return True
    if looks_like_applied_date_line(text):
        return True
    return False


# Extracts the best job title and location pair from subject/body.
# Multiple rule blocks are tried in order: applied-mail layout, viewed-mail layout,
# company-separator patterns, and finally conservative fallbacks.
def extract_job_title_and_location(subject: str, body_text: str, company: str) -> Tuple[str, str]:
    lines = body_to_lines(body_text)
    for stop_idx, line in enumerate(lines):
        if "sizin icin onerilen benzer is ilanlarini kesfedin" in normalize_text(line):
            lines = lines[:stop_idx]
            break

    company_n = normalize_text(company)
    title = ""
    location = ""

    # Removes repeated company/separator noise from extracted location candidate
    # and trims trailing punctuation for cleaner CSV output.
    def clean_location(value: str) -> str:
        loc = (value or "").strip()
        loc = re.sub(rf"^\s*{re.escape(company)}\s*[^A-Za-z0-9]+\s*", "", loc, flags=re.IGNORECASE)
        return loc.strip(" -|:;,.")

    subject_n = normalize_text(subject)
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
            if looks_like_location(cand):
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
                for j in range(t_idx + 1, applied_end_idx):
                    cand = lines[j].strip()
                    if is_noise_line(cand):
                        continue
                    if normalize_text(cand) == company_n:
                        continue
                    if is_probable_location_line(cand, company):
                        location = clean_location(cand)
                        break

        if title:
            return title, location

    for i, line in enumerate(lines):
        if "ise alim takimi" in normalize_text(line):
            for j in range(i + 1, min(i + 4, len(lines))):
                cand = lines[j].strip()
                if is_bad_title(cand, company):
                    continue
                if looks_like_location(cand):
                    continue
                title = cand
                break
            if title:
                for j in range(i + 1, min(i + 7, len(lines))):
                    cand_loc = lines[j].strip()
                    if is_probable_location_line(cand_loc, company):
                        if "Â·" in cand_loc or "â€¢" in cand_loc:
                            parts = re.split(r"\s*[Â·â€¢]\s*", cand_loc, maxsplit=1)
                            if len(parts) == 2:
                                left, right = parts
                                if normalize_text(company) in normalize_text(left):
                                    location = clean_location(right)
                                else:
                                    location = clean_location(cand_loc)
                        else:
                            location = clean_location(cand_loc)
                        break
                break

    for idx, line in enumerate(lines):
        if title and location:
            break
        left = ""
        right = ""

        parts = re.split(r"\s*[Â·â€¢]\s*", line, maxsplit=1)
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

    if not title:
        m = re.search(r"sirketindeki\s+(?P<title>.+?)\s+basvurunuz", subject_n)
        if m:
            title = m.group("title").strip()

    if not title:
        for i, line in enumerate(lines):
            if normalize_text(company) not in normalize_text(line):
                continue
            for j in range(i + 1, min(i + 6, len(lines))):
                cand = lines[j].strip()
                if is_bad_title(cand, company):
                    continue
                if looks_like_location(cand):
                    continue
                title = cand
                break
            if title:
                break

    if not title:
        for i, line in enumerate(lines):
            if "basvuru" in normalize_text(line) and "tarih" in normalize_text(line):
                for j in range(i - 1, max(-1, i - 6), -1):
                    cand = lines[j].strip()
                    if is_bad_title(cand, company):
                        continue
                    if looks_like_location(cand):
                        continue
                    title = cand
                    break
                if title:
                    break

    if looks_like_location(title):
        for line in lines:
            if not is_bad_title(line, company) and not looks_like_location(line):
                title = line
                break

    if not location and title:
        title_n = normalize_text(title)
        title_idx = -1
        for i, line in enumerate(lines):
            if normalize_text(line) == title_n:
                title_idx = i
                break
        if title_idx >= 0:
            for j in range(title_idx + 1, min(title_idx + 5, len(lines))):
                cand = lines[j].strip()
                if is_probable_location_line(cand, company):
                    if "Â·" in cand or "â€¢" in cand:
                        parts = re.split(r"\s*[Â·â€¢]\s*", cand, maxsplit=1)
                        if len(parts) == 2:
                            left, right = parts
                            if normalize_text(company) in normalize_text(left):
                                location = right.strip()
                            else:
                                location = clean_location(cand)
                    else:
                        location = clean_location(cand)
                    break

    if title and looks_like_applied_date_line(title):
        m = re.search(r"sirketindeki\s+(?P<title>.+?)\s+basvurunuz", subject_n)
        if m:
            title = m.group("title").strip()

    return title, location


# Converts noisy LinkedIn URLs into canonical stable form.
# It removes tracking query params and fixes special HTML encoding artifacts.
def normalize_job_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    cleaned = url.replace("&amp;", "&").strip()
    m = re.search(r"linkedin\.com/(?:comm/)?jobs/view/(\d+)", cleaned)
    if m:
        return f"https://www.linkedin.com/jobs/view/{m.group(1)}"
    return cleaned.split("?", 1)[0]


# Extracts best candidate job URL from body text.
# Preference order is strict LinkedIn job page link, then any /jobs/view/ link,
# then fallback to first URL.
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
# Returns empty string if URL is missing or pattern not found.
def extract_job_id(job_url: str) -> str:
    m = re.search(r"linkedin\.com/(?:comm/)?jobs/view/(\d+)", job_url or "")
    return m.group(1) if m else ""
