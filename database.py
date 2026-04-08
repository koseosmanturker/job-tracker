import csv
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from linkedin_parser import extract_job_id, normalize_job_url, normalize_text, str_to_bool


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "job_tracker.db"
DEFAULT_USER_ID = "local-default-user"
DEFAULT_USER_EMAIL = "local@example.com"
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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_db_path() -> Path:
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if database_url.startswith("sqlite:///"):
        raw_path = database_url.removeprefix("sqlite:///")
        return Path(raw_path).expanduser()
    custom_path = os.environ.get("DB_PATH", "").strip()
    if custom_path:
        return Path(custom_path).expanduser()
    return DEFAULT_DB_PATH


def _connect() -> sqlite3.Connection:
    db_path = _resolve_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _normalize_job_row(row: dict | None) -> dict:
    normalized = {field: "" for field in JOB_FIELDS}
    for field in JOB_FIELDS:
        normalized[field] = str((row or {}).get(field, "") or "").strip()
    return normalized


def _bool_to_csv(value: object) -> str:
    return "1" if str_to_bool(str(value)) else ""


def _bool_to_db(value: object) -> int:
    return 1 if str_to_bool(str(value)) else 0


def _job_storage_key(row: dict, fallback_seed: str = "") -> str:
    job_url = normalize_job_url(row.get("job_url", "")) or ""
    job_id = extract_job_id(job_url)
    if job_id:
        return f"id:{job_id}"
    company = normalize_text(row.get("company", ""))
    job_title = normalize_text(row.get("job_title", ""))
    location = normalize_text(row.get("location", ""))
    if company or job_title or location:
        return "|".join([company, job_title, location])
    payload = json.dumps(_normalize_job_row(row), sort_keys=True, ensure_ascii=False)
    digest = hashlib.sha1(f"{fallback_seed}|{payload}".encode("utf-8")).hexdigest()[:16]
    return f"raw:{digest}"


def _dedupe_storage_key(base_key: str, seen: set[str]) -> str:
    if base_key not in seen:
        seen.add(base_key)
        return base_key
    suffix = 2
    while f"{base_key}#{suffix}" in seen:
        suffix += 1
    unique_key = f"{base_key}#{suffix}"
    seen.add(unique_key)
    return unique_key


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL DEFAULT '',
            display_name TEXT NOT NULL DEFAULT '',
            locale TEXT NOT NULL DEFAULT 'tr',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            row_key TEXT NOT NULL,
            company TEXT NOT NULL DEFAULT '',
            job_title TEXT NOT NULL DEFAULT '',
            location TEXT NOT NULL DEFAULT '',
            job_url TEXT NOT NULL DEFAULT '',
            applied INTEGER NOT NULL DEFAULT 0,
            applied_time TEXT NOT NULL DEFAULT '',
            viewed INTEGER NOT NULL DEFAULT 0,
            viewed_time TEXT NOT NULL DEFAULT '',
            downloaded INTEGER NOT NULL DEFAULT 0,
            rejected INTEGER NOT NULL DEFAULT 0,
            favorite INTEGER NOT NULL DEFAULT 0,
            follow_up_done INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, row_key),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS needs_review (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            review_id TEXT NOT NULL,
            signature TEXT NOT NULL,
            message_id TEXT NOT NULL DEFAULT '',
            subject TEXT NOT NULL DEFAULT '',
            body_preview TEXT NOT NULL DEFAULT '',
            body_text TEXT NOT NULL DEFAULT '',
            event_time TEXT NOT NULL DEFAULT '',
            reason TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            resolution_note TEXT NOT NULL DEFAULT '',
            UNIQUE(user_id, signature),
            UNIQUE(user_id, review_id),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS manual_corrections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            signature TEXT NOT NULL,
            subject TEXT NOT NULL DEFAULT '',
            corrected_fields_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, signature),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS sync_state (
            user_id TEXT PRIMARY KEY,
            initialized INTEGER NOT NULL DEFAULT 0,
            last_synced_at TEXT NOT NULL DEFAULT '',
            last_query TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """
    )
    now = _utc_now_iso()
    conn.execute(
        """
        INSERT INTO users (id, email, display_name, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at
        """,
        (DEFAULT_USER_ID, DEFAULT_USER_EMAIL, "Local User", now, now),
    )


def _cleanup_duplicate_incomplete_jobs(conn: sqlite3.Connection) -> None:
    duplicate_groups = conn.execute(
        """
        SELECT company, job_title, location, job_url, applied, applied_time, viewed, viewed_time,
               downloaded, rejected, favorite, follow_up_done, COUNT(*) AS row_count
        FROM jobs
        WHERE user_id = ? AND (company = '' OR job_title = '' OR location = '')
        GROUP BY company, job_title, location, job_url, applied, applied_time, viewed, viewed_time,
                 downloaded, rejected, favorite, follow_up_done
        HAVING COUNT(*) > 1
        """,
        (DEFAULT_USER_ID,),
    ).fetchall()

    for group in duplicate_groups:
        ids = conn.execute(
            """
            SELECT id
            FROM jobs
            WHERE user_id = ?
              AND company = ?
              AND job_title = ?
              AND location = ?
              AND job_url = ?
              AND applied = ?
              AND applied_time = ?
              AND viewed = ?
              AND viewed_time = ?
              AND downloaded = ?
              AND rejected = ?
              AND favorite = ?
              AND follow_up_done = ?
            ORDER BY id
            """,
            (
                DEFAULT_USER_ID,
                group["company"],
                group["job_title"],
                group["location"],
                group["job_url"],
                group["applied"],
                group["applied_time"],
                group["viewed"],
                group["viewed_time"],
                group["downloaded"],
                group["rejected"],
                group["favorite"],
                group["follow_up_done"],
            ),
        ).fetchall()
        duplicate_ids = [row["id"] for row in ids[1:]]
        if duplicate_ids:
            conn.executemany("DELETE FROM jobs WHERE id = ?", [(job_id,) for job_id in duplicate_ids])


def _read_csv_rows(csv_path: str) -> list[dict]:
    path = Path(csv_path)
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(_normalize_job_row(row))
    return rows


def _read_json_list(path_str: str) -> list[dict]:
    path = Path(path_str)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return data if isinstance(data, list) else []


def _read_json_dict(path_str: str) -> dict:
    path = Path(path_str)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _maybe_migrate_jobs(conn: sqlite3.Connection, csv_path: str | None) -> None:
    if not csv_path:
        return
    existing = conn.execute("SELECT COUNT(*) FROM jobs WHERE user_id = ?", (DEFAULT_USER_ID,)).fetchone()[0]
    if existing:
        return
    rows = _read_csv_rows(csv_path)
    if not rows:
        return
    now = _utc_now_iso()
    seen_keys: set[str] = set()
    for idx, raw in enumerate(rows):
        row = _normalize_job_row(raw)
        row_key = _dedupe_storage_key(_job_storage_key(row, fallback_seed=f"{idx}:{csv_path}"), seen_keys)
        conn.execute(
            """
            INSERT OR REPLACE INTO jobs (
                user_id, row_key, company, job_title, location, job_url, applied, applied_time,
                viewed, viewed_time, downloaded, rejected, favorite, follow_up_done, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                DEFAULT_USER_ID,
                row_key,
                row["company"],
                row["job_title"],
                row["location"],
                normalize_job_url(row["job_url"]) or "",
                _bool_to_db(row["applied"]),
                row["applied_time"],
                _bool_to_db(row["viewed"]),
                row["viewed_time"],
                _bool_to_db(row["downloaded"]),
                _bool_to_db(row["rejected"]),
                _bool_to_db(row["favorite"]),
                _bool_to_db(row["follow_up_done"]),
                now,
                now,
            ),
        )


def _maybe_migrate_reviews(conn: sqlite3.Connection, review_path: str | None) -> None:
    if not review_path:
        return
    existing = conn.execute("SELECT COUNT(*) FROM needs_review WHERE user_id = ?", (DEFAULT_USER_ID,)).fetchone()[0]
    if existing:
        return
    for row in _read_json_list(review_path):
        conn.execute(
            """
            INSERT OR IGNORE INTO needs_review (
                user_id, review_id, signature, message_id, subject, body_preview, body_text,
                event_time, reason, status, created_at, updated_at, resolution_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                DEFAULT_USER_ID,
                str(row.get("review_id", "") or ""),
                str(row.get("signature", "") or ""),
                str(row.get("message_id", "") or ""),
                str(row.get("subject", "") or ""),
                str(row.get("body_preview", "") or ""),
                str(row.get("body_text", "") or ""),
                str(row.get("event_time", "") or ""),
                str(row.get("reason", "") or ""),
                str(row.get("status", "pending") or "pending"),
                str(row.get("created_at", "") or _utc_now_iso()),
                str(row.get("updated_at", "") or _utc_now_iso()),
                str(row.get("resolution_note", "") or ""),
            ),
        )


def _maybe_migrate_corrections(conn: sqlite3.Connection, corrections_path: str | None) -> None:
    if not corrections_path:
        return
    existing = conn.execute("SELECT COUNT(*) FROM manual_corrections WHERE user_id = ?", (DEFAULT_USER_ID,)).fetchone()[0]
    if existing:
        return
    for row in _read_json_list(corrections_path):
        conn.execute(
            """
            INSERT OR IGNORE INTO manual_corrections (user_id, signature, subject, corrected_fields_json, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                DEFAULT_USER_ID,
                str(row.get("signature", "") or ""),
                str(row.get("subject", "") or ""),
                json.dumps(row.get("corrected_fields") or {}, ensure_ascii=False),
                str(row.get("updated_at", "") or _utc_now_iso()),
            ),
        )


def _maybe_migrate_sync_state(conn: sqlite3.Connection, state_path: str | None) -> None:
    if not state_path:
        return
    existing = conn.execute("SELECT COUNT(*) FROM sync_state WHERE user_id = ?", (DEFAULT_USER_ID,)).fetchone()[0]
    if existing:
        return
    state = _read_json_dict(state_path)
    if not state:
        return
    conn.execute(
        """
        INSERT OR REPLACE INTO sync_state (user_id, initialized, last_synced_at, last_query, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            DEFAULT_USER_ID,
            1 if state.get("initialized") else 0,
            str(state.get("last_synced_at", "") or ""),
            str(state.get("last_query", "") or ""),
            _utc_now_iso(),
        ),
    )


def ensure_database(*, csv_path: str | None = None, review_path: str | None = None, corrections_path: str | None = None, state_path: str | None = None) -> None:
    with _connect() as conn:
        _ensure_schema(conn)
        _maybe_migrate_jobs(conn, csv_path)
        _maybe_migrate_reviews(conn, review_path)
        _maybe_migrate_corrections(conn, corrections_path)
        _maybe_migrate_sync_state(conn, state_path)
        _cleanup_duplicate_incomplete_jobs(conn)


def list_job_rows(*, csv_path: str | None = None) -> list[dict]:
    ensure_database(csv_path=csv_path)
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT company, job_title, location, job_url, applied, applied_time, viewed, viewed_time,
                   downloaded, rejected, favorite, follow_up_done
            FROM jobs
            WHERE user_id = ?
            ORDER BY lower(company), lower(job_title), id
            """,
            (DEFAULT_USER_ID,),
        ).fetchall()
    result = []
    for row in rows:
        result.append(
            {
                "company": row["company"] or "",
                "job_title": row["job_title"] or "",
                "location": row["location"] or "",
                "job_url": row["job_url"] or "",
                "applied": _bool_to_csv(row["applied"]),
                "applied_time": row["applied_time"] or "",
                "viewed": _bool_to_csv(row["viewed"]),
                "viewed_time": row["viewed_time"] or "",
                "downloaded": _bool_to_csv(row["downloaded"]),
                "rejected": _bool_to_csv(row["rejected"]),
                "favorite": _bool_to_csv(row["favorite"]),
                "follow_up_done": _bool_to_csv(row["follow_up_done"]),
            }
        )
    return result


def replace_job_rows(rows: list[dict], *, csv_path: str | None = None) -> None:
    ensure_database(csv_path=csv_path)
    combined = sorted([_normalize_job_row(row) for row in rows], key=lambda row: (row.get("company", "").lower(), row.get("job_title", "").lower()))
    now = _utc_now_iso()
    seen_keys: set[str] = set()
    with _connect() as conn:
        conn.execute("DELETE FROM jobs WHERE user_id = ?", (DEFAULT_USER_ID,))
        for idx, row in enumerate(combined):
            row_key = _dedupe_storage_key(_job_storage_key(row, fallback_seed=f"replace:{idx}"), seen_keys)
            conn.execute(
                """
                INSERT INTO jobs (
                    user_id, row_key, company, job_title, location, job_url, applied, applied_time,
                    viewed, viewed_time, downloaded, rejected, favorite, follow_up_done, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    DEFAULT_USER_ID,
                    row_key,
                    row["company"],
                    row["job_title"],
                    row["location"],
                    normalize_job_url(row["job_url"]) or "",
                    _bool_to_db(row["applied"]),
                    row["applied_time"],
                    _bool_to_db(row["viewed"]),
                    row["viewed_time"],
                    _bool_to_db(row["downloaded"]),
                    _bool_to_db(row["rejected"]),
                    _bool_to_db(row["favorite"]),
                    _bool_to_db(row["follow_up_done"]),
                    now,
                    now,
                ),
            )


def list_review_rows(*, review_path: str | None = None, status: str | None = None) -> list[dict]:
    ensure_database(review_path=review_path)
    query = """
        SELECT review_id, signature, message_id, subject, body_preview, body_text, event_time, reason,
               status, created_at, updated_at, resolution_note
        FROM needs_review
        WHERE user_id = ?
    """
    params: list[object] = [DEFAULT_USER_ID]
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY updated_at DESC, id DESC"
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def upsert_review_row(item: dict, *, review_path: str | None = None) -> bool:
    ensure_database(review_path=review_path)
    with _connect() as conn:
        existing = conn.execute(
            "SELECT status FROM needs_review WHERE user_id = ? AND signature = ?",
            (DEFAULT_USER_ID, item.get("signature", "")),
        ).fetchone()
        if existing and existing["status"] == "resolved":
            return False
        now = _utc_now_iso()
        conn.execute(
            """
            INSERT INTO needs_review (
                user_id, review_id, signature, message_id, subject, body_preview, body_text,
                event_time, reason, status, created_at, updated_at, resolution_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, signature) DO UPDATE SET
                event_time=excluded.event_time,
                reason=excluded.reason,
                body_preview=excluded.body_preview,
                body_text=excluded.body_text,
                updated_at=excluded.updated_at
            """,
            (
                DEFAULT_USER_ID,
                str(item.get("review_id", "") or ""),
                str(item.get("signature", "") or ""),
                str(item.get("message_id", "") or ""),
                str(item.get("subject", "") or ""),
                str(item.get("body_preview", "") or ""),
                str(item.get("body_text", "") or ""),
                str(item.get("event_time", "") or ""),
                str(item.get("reason", "") or ""),
                str(item.get("status", "pending") or "pending"),
                str(item.get("created_at", "") or now),
                now,
                str(item.get("resolution_note", "") or ""),
            ),
        )
        return existing is None


def get_review_row(review_id: str, *, review_path: str | None = None) -> dict | None:
    ensure_database(review_path=review_path)
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT review_id, signature, message_id, subject, body_preview, body_text, event_time, reason,
                   status, created_at, updated_at, resolution_note
            FROM needs_review
            WHERE user_id = ? AND review_id = ?
            """,
            (DEFAULT_USER_ID, review_id),
        ).fetchone()
    return dict(row) if row else None


def resolve_review_row(review_id: str, resolution_note: str = "", *, review_path: str | None = None) -> bool:
    ensure_database(review_path=review_path)
    with _connect() as conn:
        cur = conn.execute(
            """
            UPDATE needs_review
            SET status = 'resolved', resolution_note = ?, updated_at = ?
            WHERE user_id = ? AND review_id = ?
            """,
            ((resolution_note or "").strip(), _utc_now_iso(), DEFAULT_USER_ID, review_id),
        )
        return cur.rowcount > 0


def save_manual_correction_row(*, subject: str, signature: str, corrected_fields: dict, corrections_path: str | None = None) -> None:
    ensure_database(corrections_path=corrections_path)
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO manual_corrections (user_id, signature, subject, corrected_fields_json, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id, signature) DO UPDATE SET
                subject=excluded.subject,
                corrected_fields_json=excluded.corrected_fields_json,
                updated_at=excluded.updated_at
            """,
            (DEFAULT_USER_ID, signature, subject.strip(), json.dumps(corrected_fields or {}, ensure_ascii=False), _utc_now_iso()),
        )


def find_manual_correction_row(signature: str, *, corrections_path: str | None = None) -> dict | None:
    ensure_database(corrections_path=corrections_path)
    with _connect() as conn:
        row = conn.execute(
            "SELECT corrected_fields_json FROM manual_corrections WHERE user_id = ? AND signature = ?",
            (DEFAULT_USER_ID, signature),
        ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row["corrected_fields_json"])
    except Exception:
        return None


def load_sync_state_row(*, state_path: str | None = None) -> dict:
    ensure_database(state_path=state_path)
    with _connect() as conn:
        row = conn.execute(
            "SELECT initialized, last_synced_at, last_query FROM sync_state WHERE user_id = ?",
            (DEFAULT_USER_ID,),
        ).fetchone()
    if not row:
        return {}
    return {
        "initialized": bool(row["initialized"]),
        "last_synced_at": row["last_synced_at"] or "",
        "last_query": row["last_query"] or "",
    }


def save_sync_state_row(state: dict, *, state_path: str | None = None) -> None:
    ensure_database(state_path=state_path)
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO sync_state (user_id, initialized, last_synced_at, last_query, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                initialized=excluded.initialized,
                last_synced_at=excluded.last_synced_at,
                last_query=excluded.last_query,
                updated_at=excluded.updated_at
            """,
            (
                DEFAULT_USER_ID,
                1 if state.get("initialized") else 0,
                str(state.get("last_synced_at", "") or ""),
                str(state.get("last_query", "") or ""),
                _utc_now_iso(),
            ),
        )
