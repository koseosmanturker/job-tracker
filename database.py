import hashlib
import json
import os
import sqlite3
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from env_utils import get_database_url, load_env
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

_CURRENT_USER_ID: ContextVar[str] = ContextVar("current_user_id", default=DEFAULT_USER_ID)
_POOL = None
_schema_initialized = False

load_env()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _database_url() -> str:
    return get_database_url()


def _is_postgres() -> bool:
    url = _database_url()
    return url.startswith("postgresql://") or url.startswith("postgres://")


def _resolve_db_path() -> Path:
    database_url = _database_url()
    if database_url.startswith("sqlite:///"):
        raw_path = database_url.removeprefix("sqlite:///")
        return Path(raw_path).expanduser()
    custom_path = os.environ.get("DB_PATH", "").strip()
    if custom_path:
        return Path(custom_path).expanduser()
    return DEFAULT_DB_PATH


def _get_pool():
    global _POOL
    if _POOL is not None:
        return _POOL
    try:
        from psycopg.rows import dict_row
        from psycopg_pool import ConnectionPool
    except ImportError as exc:
        raise RuntimeError(
            'PostgreSQL DATABASE_URL requires psycopg and psycopg-pool. Install with: pip install "psycopg[binary]" psycopg-pool'
        ) from exc
    _POOL = ConnectionPool(
        conninfo=_database_url(),
        min_size=1,
        max_size=5,
        kwargs={"row_factory": dict_row},
    )
    return _POOL


def _connect():
    if _is_postgres():
        return _get_pool().connection()

    db_path = _resolve_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _ph() -> str:
    return "%s" if _is_postgres() else "?"


def _phs(count: int) -> str:
    return ", ".join([_ph()] * count)


def _bool_to_str(value: object) -> str:
    return "1" if str_to_bool(str(value)) else ""


def _bool_to_db(value: object) -> object:
    flag = str_to_bool(str(value))
    return flag if _is_postgres() else (1 if flag else 0)


def _db_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str_to_bool(str(value))


def _db_timestamp(value: object) -> object:
    text = str(value or "").strip()
    if not _is_postgres():
        return text
    return text or None


def _row_timestamp(value: object) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def make_user_id(email: str) -> str:
    normalized = (email or DEFAULT_USER_EMAIL).strip().lower()
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:16]
    return f"user:{digest}"


def set_current_user_id(user_id: str | None) -> None:
    _CURRENT_USER_ID.set((user_id or "").strip() or DEFAULT_USER_ID)


def get_current_user_id() -> str:
    return _CURRENT_USER_ID.get() or DEFAULT_USER_ID


def _normalize_job_row(row: dict | None) -> dict:
    normalized = {field: "" for field in JOB_FIELDS}
    for field in JOB_FIELDS:
        normalized[field] = str((row or {}).get(field, "") or "").strip()
    return normalized


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


def _execute(conn, sql: str, params: tuple[Any, ...] = ()):
    return conn.execute(sql.replace("?", _ph()), params)


def _ensure_sqlite_columns(conn: sqlite3.Connection) -> None:
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    columns = {
        "name": "TEXT NOT NULL DEFAULT ''",
        "surname": "TEXT NOT NULL DEFAULT ''",
        "age": "INTEGER",
        "gmail": "TEXT NOT NULL DEFAULT ''",
        "linkedin_language": "TEXT NOT NULL DEFAULT ''",
        "api_permission_granted": "INTEGER NOT NULL DEFAULT 0",
        "package": "TEXT NOT NULL DEFAULT 'starter'",
        "gmail_token_json": "TEXT NOT NULL DEFAULT ''",
    }
    for column, definition in columns.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE users ADD COLUMN {column} {definition}")


def _ensure_schema(conn) -> None:
    if _is_postgres():
        statements = [
            """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                gmail TEXT NOT NULL DEFAULT '',
                password_hash TEXT NOT NULL DEFAULT '',
                display_name TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',
                surname TEXT NOT NULL DEFAULT '',
                age INTEGER,
                linkedin_language TEXT NOT NULL DEFAULT '',
                api_permission_granted BOOLEAN NOT NULL DEFAULT FALSE,
                package TEXT NOT NULL DEFAULT 'starter',
                gmail_token_json TEXT NOT NULL DEFAULT '',
                locale TEXT NOT NULL DEFAULT 'tr',
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS users_gmail_unique ON users (lower(gmail)) WHERE gmail <> ''",
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                row_key TEXT NOT NULL,
                company TEXT NOT NULL DEFAULT '',
                job_title TEXT NOT NULL DEFAULT '',
                location TEXT NOT NULL DEFAULT '',
                job_url TEXT NOT NULL DEFAULT '',
                applied BOOLEAN NOT NULL DEFAULT FALSE,
                applied_time TIMESTAMPTZ,
                viewed BOOLEAN NOT NULL DEFAULT FALSE,
                viewed_time TIMESTAMPTZ,
                downloaded BOOLEAN NOT NULL DEFAULT FALSE,
                rejected BOOLEAN NOT NULL DEFAULT FALSE,
                favorite BOOLEAN NOT NULL DEFAULT FALSE,
                follow_up_done BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                UNIQUE(user_id, row_key)
            )
            """,
            "CREATE INDEX IF NOT EXISTS jobs_user_company_title_idx ON jobs (user_id, lower(company), lower(job_title), id)",
            """
            CREATE TABLE IF NOT EXISTS needs_review (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                review_id TEXT NOT NULL,
                signature TEXT NOT NULL,
                message_id TEXT NOT NULL DEFAULT '',
                subject TEXT NOT NULL DEFAULT '',
                body_preview TEXT NOT NULL DEFAULT '',
                body_text TEXT NOT NULL DEFAULT '',
                event_time TIMESTAMPTZ,
                reason TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                resolution_note TEXT NOT NULL DEFAULT '',
                UNIQUE(user_id, signature),
                UNIQUE(user_id, review_id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS needs_review_user_status_updated_idx ON needs_review (user_id, status, updated_at DESC, id DESC)",
            """
            CREATE TABLE IF NOT EXISTS manual_corrections (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                signature TEXT NOT NULL,
                subject TEXT NOT NULL DEFAULT '',
                corrected_fields_json TEXT NOT NULL DEFAULT '{}',
                updated_at TIMESTAMPTZ NOT NULL,
                UNIQUE(user_id, signature)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sync_state (
                user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                initialized BOOLEAN NOT NULL DEFAULT FALSE,
                last_synced_at TIMESTAMPTZ,
                last_query TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMPTZ NOT NULL
            )
            """,
        ]
        for statement in statements:
            conn.execute(statement)
        try:
            conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS gmail_token_json TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass
    else:
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
        _ensure_sqlite_columns(conn)

    now = _utc_now_iso()
    _execute(
        conn,
        """
        INSERT INTO users (
            id, email, gmail, display_name, name, surname, linkedin_language,
            api_permission_granted, package, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at
        """,
        (
            DEFAULT_USER_ID,
            DEFAULT_USER_EMAIL,
            DEFAULT_USER_EMAIL,
            "Local User",
            "Local",
            "User",
            "Turkish",
            _bool_to_db(True),
            "advanced",
            _db_timestamp(now),
            _db_timestamp(now),
        ),
    )


def ensure_database() -> None:
    global _schema_initialized
    if _schema_initialized:
        return
    with _connect() as conn:
        _ensure_schema(conn)
    _schema_initialized = True


def _registration_from_row(row: dict) -> dict:
    return {
        "id": row["id"],
        "name": row.get("name", "") or "",
        "surname": row.get("surname", "") or "",
        "age": row.get("age", "") if row.get("age") is not None else "",
        "gmail": row.get("gmail") or row.get("email", "") or "",
        "email": row.get("email") or row.get("gmail", "") or "",
        "password_hash": row.get("password_hash", "") or "",
        "linkedin_language": row.get("linkedin_language", "") or "",
        "api_permission_granted": _db_bool(row.get("api_permission_granted", False)),
        "package": row.get("package", "starter") or "starter",
        "created_at": _row_timestamp(row.get("created_at", "")),
        "updated_at": _row_timestamp(row.get("updated_at", "")),
    }


def list_user_registrations() -> list[dict]:
    ensure_database()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, email, gmail, password_hash, display_name, name, surname, age,
                   linkedin_language, api_permission_granted, package, created_at, updated_at
            FROM users
            WHERE id <> ?
            ORDER BY lower(email)
            """.replace("?", _ph()),
            (DEFAULT_USER_ID,),
        ).fetchall()
    return [_registration_from_row(dict(row)) for row in rows]


def save_user_registrations(rows: list[dict]) -> None:
    ensure_database()
    now = _utc_now_iso()
    with _connect() as conn:
        for raw in rows:
            gmail = (raw.get("gmail") or raw.get("email") or "").strip().lower()
            if not gmail:
                continue
            user_id = raw.get("id") or make_user_id(gmail)
            name = str(raw.get("name", "") or "").strip()
            surname = str(raw.get("surname", "") or "").strip()
            display_name = " ".join(part for part in [name, surname] if part).strip()
            age_raw = raw.get("age", None)
            try:
                age_value = int(age_raw) if age_raw not in (None, "") else None
            except (TypeError, ValueError):
                age_value = None
            _execute(
                conn,
                """
                INSERT INTO users (
                    id, email, gmail, password_hash, display_name, name, surname, age,
                    linkedin_language, api_permission_granted, package, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    email=excluded.email,
                    gmail=excluded.gmail,
                    password_hash=excluded.password_hash,
                    display_name=excluded.display_name,
                    name=excluded.name,
                    surname=excluded.surname,
                    age=excluded.age,
                    linkedin_language=excluded.linkedin_language,
                    api_permission_granted=excluded.api_permission_granted,
                    package=excluded.package,
                    updated_at=excluded.updated_at
                """,
                (
                    user_id,
                    gmail,
                    gmail,
                    str(raw.get("password_hash", "") or ""),
                    display_name,
                    name,
                    surname,
                    age_value,
                    str(raw.get("linkedin_language", "") or ""),
                    _bool_to_db(raw.get("api_permission_granted", False)),
                    str(raw.get("package", "starter") or "starter"),
                    _db_timestamp(raw.get("created_at", "") or now),
                    _db_timestamp(now),
                ),
            )


def list_job_rows() -> list[dict]:
    ensure_database()
    user_id = get_current_user_id()
    with _connect() as conn:
        rows = _execute(
            conn,
            """
            SELECT company, job_title, location, job_url, applied, applied_time, viewed, viewed_time,
                   downloaded, rejected, favorite, follow_up_done
            FROM jobs
            WHERE user_id = ?
            ORDER BY lower(company), lower(job_title), id
            """,
            (user_id,),
        ).fetchall()
    result = []
    for row in rows:
        result.append(
            {
                "company": row["company"] or "",
                "job_title": row["job_title"] or "",
                "location": row["location"] or "",
                "job_url": row["job_url"] or "",
                "applied": _bool_to_str(row["applied"]),
                "applied_time": _row_timestamp(row["applied_time"]),
                "viewed": _bool_to_str(row["viewed"]),
                "viewed_time": _row_timestamp(row["viewed_time"]),
                "downloaded": _bool_to_str(row["downloaded"]),
                "rejected": _bool_to_str(row["rejected"]),
                "favorite": _bool_to_str(row["favorite"]),
                "follow_up_done": _bool_to_str(row["follow_up_done"]),
            }
        )
    return result


def replace_job_rows(rows: list[dict]) -> None:
    ensure_database()
    user_id = get_current_user_id()
    combined = sorted([_normalize_job_row(row) for row in rows], key=lambda row: (row.get("company", "").lower(), row.get("job_title", "").lower()))
    now = _utc_now_iso()
    seen_keys: set[str] = set()
    with _connect() as conn:
        _execute(conn, "DELETE FROM jobs WHERE user_id = ?", (user_id,))
        for idx, row in enumerate(combined):
            row_key = _dedupe_storage_key(_job_storage_key(row, fallback_seed=f"replace:{idx}"), seen_keys)
            _execute(
                conn,
                """
                INSERT INTO jobs (
                    user_id, row_key, company, job_title, location, job_url, applied, applied_time,
                    viewed, viewed_time, downloaded, rejected, favorite, follow_up_done, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    row_key,
                    row["company"],
                    row["job_title"],
                    row["location"],
                    normalize_job_url(row["job_url"]) or "",
                    _bool_to_db(row["applied"]),
                    _db_timestamp(row["applied_time"]),
                    _bool_to_db(row["viewed"]),
                    _db_timestamp(row["viewed_time"]),
                    _bool_to_db(row["downloaded"]),
                    _bool_to_db(row["rejected"]),
                    _bool_to_db(row["favorite"]),
                    _bool_to_db(row["follow_up_done"]),
                    _db_timestamp(now),
                    _db_timestamp(now),
                ),
            )


def list_review_rows(*, status: str | None = None) -> list[dict]:
    ensure_database()
    user_id = get_current_user_id()
    query = """
        SELECT review_id, signature, message_id, subject, body_preview, body_text, event_time, reason,
               status, created_at, updated_at, resolution_note
        FROM needs_review
        WHERE user_id = ?
    """
    params: list[object] = [user_id]
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY updated_at DESC, id DESC"
    with _connect() as conn:
        rows = _execute(conn, query, tuple(params)).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["event_time"] = _row_timestamp(item.get("event_time"))
        item["created_at"] = _row_timestamp(item.get("created_at"))
        item["updated_at"] = _row_timestamp(item.get("updated_at"))
        result.append(item)
    return result


def upsert_review_row(item: dict) -> bool:
    ensure_database()
    user_id = get_current_user_id()
    with _connect() as conn:
        existing = _execute(
            conn,
            "SELECT status FROM needs_review WHERE user_id = ? AND signature = ?",
            (user_id, item.get("signature", "")),
        ).fetchone()
        if existing and existing["status"] == "resolved":
            return False
        now = _utc_now_iso()
        _execute(
            conn,
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
                user_id,
                str(item.get("review_id", "") or ""),
                str(item.get("signature", "") or ""),
                str(item.get("message_id", "") or ""),
                str(item.get("subject", "") or ""),
                str(item.get("body_preview", "") or ""),
                str(item.get("body_text", "") or ""),
                _db_timestamp(item.get("event_time", "")),
                str(item.get("reason", "") or ""),
                str(item.get("status", "pending") or "pending"),
                _db_timestamp(item.get("created_at", "") or now),
                _db_timestamp(now),
                str(item.get("resolution_note", "") or ""),
            ),
        )
        return existing is None


def get_review_row(review_id: str) -> dict | None:
    ensure_database()
    user_id = get_current_user_id()
    with _connect() as conn:
        row = _execute(
            conn,
            """
            SELECT review_id, signature, message_id, subject, body_preview, body_text, event_time, reason,
                   status, created_at, updated_at, resolution_note
            FROM needs_review
            WHERE user_id = ? AND review_id = ?
            """,
            (user_id, review_id),
        ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["event_time"] = _row_timestamp(item.get("event_time"))
    item["created_at"] = _row_timestamp(item.get("created_at"))
    item["updated_at"] = _row_timestamp(item.get("updated_at"))
    return item


def resolve_review_row(review_id: str, resolution_note: str = "") -> bool:
    ensure_database()
    user_id = get_current_user_id()
    with _connect() as conn:
        cur = _execute(
            conn,
            """
            UPDATE needs_review
            SET status = 'resolved', resolution_note = ?, updated_at = ?
            WHERE user_id = ? AND review_id = ?
            """,
            ((resolution_note or "").strip(), _db_timestamp(_utc_now_iso()), user_id, review_id),
        )
        return cur.rowcount > 0


def save_manual_correction_row(*, subject: str, signature: str, corrected_fields: dict) -> None:
    ensure_database()
    user_id = get_current_user_id()
    with _connect() as conn:
        _execute(
            conn,
            """
            INSERT INTO manual_corrections (user_id, signature, subject, corrected_fields_json, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id, signature) DO UPDATE SET
                subject=excluded.subject,
                corrected_fields_json=excluded.corrected_fields_json,
                updated_at=excluded.updated_at
            """,
            (user_id, signature, subject.strip(), json.dumps(corrected_fields or {}, ensure_ascii=False), _db_timestamp(_utc_now_iso())),
        )


def find_manual_correction_row(signature: str) -> dict | None:
    ensure_database()
    user_id = get_current_user_id()
    with _connect() as conn:
        row = _execute(
            conn,
            "SELECT corrected_fields_json FROM manual_corrections WHERE user_id = ? AND signature = ?",
            (user_id, signature),
        ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row["corrected_fields_json"])
    except Exception:
        return None


def load_sync_state_row() -> dict:
    ensure_database()
    user_id = get_current_user_id()
    with _connect() as conn:
        row = _execute(
            conn,
            "SELECT initialized, last_synced_at, last_query FROM sync_state WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    if not row:
        return {}
    return {
        "initialized": _db_bool(row["initialized"]),
        "last_synced_at": _row_timestamp(row["last_synced_at"]),
        "last_query": row["last_query"] or "",
    }


def save_sync_state_row(state: dict) -> None:
    ensure_database()
    user_id = get_current_user_id()
    with _connect() as conn:
        _execute(
            conn,
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
                user_id,
                _bool_to_db(state.get("initialized")),
                _db_timestamp(state.get("last_synced_at", "")),
                str(state.get("last_query", "") or ""),
                _db_timestamp(_utc_now_iso()),
            ),
        )


def _toggle_job_field(row_key: str, field: str) -> bool | None:
    ensure_database()
    user_id = get_current_user_id()
    now = _db_timestamp(_utc_now_iso())
    with _connect() as conn:
        if _is_postgres():
            cur = _execute(
                conn,
                f"UPDATE jobs SET {field} = NOT {field}, updated_at = ? WHERE user_id = ? AND row_key = ? RETURNING {field}",
                (now, user_id, row_key),
            )
            row = cur.fetchone()
            return _db_bool(row[field]) if row else None
        else:
            cur = _execute(
                conn,
                f"UPDATE jobs SET {field} = NOT {field}, updated_at = ? WHERE user_id = ? AND row_key = ?",
                (now, user_id, row_key),
            )
            if cur.rowcount == 0:
                return None
            row = _execute(conn, f"SELECT {field} FROM jobs WHERE user_id = ? AND row_key = ?", (user_id, row_key)).fetchone()
            return _db_bool(row[field]) if row else None


def toggle_job_downloaded(row_key: str) -> bool | None:
    return _toggle_job_field(row_key, "downloaded")


def toggle_job_favorite(row_key: str) -> bool | None:
    return _toggle_job_field(row_key, "favorite")


def toggle_job_follow_up_done(row_key: str) -> bool | None:
    return _toggle_job_field(row_key, "follow_up_done")


def get_gmail_token(user_id: str | None = None) -> str:
    ensure_database()
    uid = (user_id or "").strip() or get_current_user_id()
    with _connect() as conn:
        row = _execute(conn, "SELECT gmail_token_json FROM users WHERE id = ?", (uid,)).fetchone()
    if not row:
        return ""
    return row["gmail_token_json"] or ""


def save_gmail_token(token_json: str, user_id: str | None = None) -> None:
    ensure_database()
    uid = (user_id or "").strip() or get_current_user_id()
    with _connect() as conn:
        _execute(
            conn,
            "UPDATE users SET gmail_token_json = ?, updated_at = ? WHERE id = ?",
            (token_json or "", _db_timestamp(_utc_now_iso()), uid),
        )
