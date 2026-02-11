from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from etl.generate_sample_data import main as generate_raw
from etl.load_mart import main as load_mart
from etl.transform import transform

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "mart" / "ops_copilot.db"


@pytest.fixture(scope="session")
def prepared_db_path() -> Path:
    os.environ["OPS_COPILOT_END_DATE"] = "2026-02-10"
    generate_raw()
    transform()
    load_mart()
    assert DB_PATH.exists()
    return DB_PATH


@pytest.fixture()
def conn(prepared_db_path: Path):
    connection = sqlite3.connect(prepared_db_path)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
    finally:
        connection.close()
