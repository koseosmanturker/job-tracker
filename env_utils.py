import os
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent


def load_env(dotenv_path: Path | None = None) -> None:
    load_dotenv(dotenv_path=dotenv_path or (BASE_DIR / ".env"), override=True)


def get_database_url() -> str:
    return (os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL") or "").strip()
