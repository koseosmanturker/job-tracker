import hashlib
from datetime import datetime, timezone

from database import (
    find_manual_correction_row,
    get_review_row,
    list_review_rows,
    resolve_review_row,
    save_manual_correction_row,
    upsert_review_row,
)
from linkedin_parser import body_to_lines, normalize_text


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def queue_needs_review(item: dict) -> bool:
    return upsert_review_row(item)


def list_needs_review(*, status: str | None = None) -> list[dict]:
    return list_review_rows(status=status)


def get_review_item(review_id: str) -> dict | None:
    return get_review_row(review_id)


def resolve_review_item(review_id: str, resolution_note: str = "") -> bool:
    return resolve_review_row(review_id, resolution_note)


def save_manual_correction(*,
                           subject: str,
                           body_text: str,
                           corrected_fields: dict):
    signature = build_message_signature(subject, body_text)
    save_manual_correction_row(
        subject=subject,
        signature=signature,
        corrected_fields=corrected_fields,
    )


def find_manual_correction(*, subject: str, body_text: str) -> dict | None:
    signature = build_message_signature(subject, body_text)
    return find_manual_correction_row(signature)
