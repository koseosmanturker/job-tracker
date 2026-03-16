import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from linkedin_parser import body_to_lines, normalize_text

NEEDS_REVIEW_FILE = ".needs_review.json"
MANUAL_CORRECTIONS_FILE = ".manual_corrections.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json_list(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    return data if isinstance(data, list) else []


def _save_json_list(path: str, rows: list[dict]):
    Path(path).write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def build_message_signature(subject: str, body_text: str) -> str:
    subject_part = normalize_text(subject)
    lines = [normalize_text(line) for line in body_to_lines(body_text)[:12]]
    body_part = " | ".join(line for line in lines if line)
    return f"{subject_part} || {body_part}".strip()


def _make_review_id(signature: str) -> str:
    return hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]


def build_needs_review_item(*,
                            message_id: str,
                            subject: str,
                            body_text: str,
                            event_time: str,
                            reason: str) -> dict:
    signature = build_message_signature(subject, body_text)
    preview = "\n".join(body_to_lines(body_text)[:10]).strip()
    return {
        "review_id": _make_review_id(signature),
        "signature": signature,
        "message_id": message_id,
        "subject": subject.strip(),
        "body_preview": preview[:1200],
        "body_text": (body_text or "").strip()[:12000],
        "event_time": event_time,
        "reason": reason,
        "status": "pending",
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "resolution_note": "",
    }


def queue_needs_review(review_path: str, item: dict) -> bool:
    rows = _load_json_list(review_path)
    signature = item.get("signature", "")
    for row in rows:
        if row.get("signature") != signature:
            continue
        if row.get("status") == "resolved":
            return False
        row["updated_at"] = _utc_now_iso()
        row["event_time"] = item.get("event_time", row.get("event_time", ""))
        row["reason"] = item.get("reason", row.get("reason", ""))
        row["body_preview"] = item.get("body_preview", row.get("body_preview", ""))
        row["body_text"] = item.get("body_text", row.get("body_text", ""))
        _save_json_list(review_path, rows)
        return False

    rows.append(item)
    rows.sort(key=lambda row: row.get("updated_at", ""), reverse=True)
    _save_json_list(review_path, rows)
    return True


def list_needs_review(review_path: str, *, status: str | None = None) -> list[dict]:
    rows = _load_json_list(review_path)
    if status:
        rows = [row for row in rows if row.get("status") == status]
    return sorted(rows, key=lambda row: row.get("updated_at", ""), reverse=True)


def get_review_item(review_path: str, review_id: str) -> dict | None:
    for row in _load_json_list(review_path):
        if row.get("review_id") == review_id:
            return row
    return None


def resolve_review_item(review_path: str, review_id: str, resolution_note: str = "") -> bool:
    rows = _load_json_list(review_path)
    for row in rows:
        if row.get("review_id") != review_id:
            continue
        row["status"] = "resolved"
        row["resolution_note"] = (resolution_note or "").strip()
        row["updated_at"] = _utc_now_iso()
        _save_json_list(review_path, rows)
        return True
    return False


def save_manual_correction(corrections_path: str,
                           *,
                           subject: str,
                           body_text: str,
                           corrected_fields: dict):
    rows = _load_json_list(corrections_path)
    signature = build_message_signature(subject, body_text)
    record = {
        "signature": signature,
        "subject": subject.strip(),
        "corrected_fields": corrected_fields,
        "updated_at": _utc_now_iso(),
    }
    for idx, row in enumerate(rows):
        if row.get("signature") == signature:
            rows[idx] = record
            _save_json_list(corrections_path, rows)
            return
    rows.append(record)
    rows.sort(key=lambda row: row.get("updated_at", ""), reverse=True)
    _save_json_list(corrections_path, rows)


def find_manual_correction(corrections_path: str, *, subject: str, body_text: str) -> dict | None:
    signature = build_message_signature(subject, body_text)
    for row in _load_json_list(corrections_path):
        if row.get("signature") == signature:
            return row.get("corrected_fields") or None
    return None
