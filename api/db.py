from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import os
import sqlite3
from typing import Generator

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "mart" / "ops_copilot.db"


def resolve_db_path() -> Path:
    return Path(os.getenv("OPS_COPILOT_DB_PATH", str(DEFAULT_DB_PATH)))


@contextmanager
def get_connection() -> Generator[sqlite3.Connection, None, None]:
    db_path = resolve_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def record_api_usage(endpoint: str, method: str, role: str, status_code: int, request_id: str) -> None:
    db_path = resolve_db_path()
    if not db_path.exists():
        return

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO api_usage_log (endpoint, method, role, status_code, requested_at, request_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (endpoint, method, role, status_code, datetime.now(timezone.utc).isoformat(), request_id),
        )
        conn.commit()
    except sqlite3.Error:
        pass
    finally:
        conn.close()
