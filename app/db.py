from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

DB_PATH = os.getenv(
    "JML_DB_PATH",
    os.path.join(os.path.dirname(__file__), "jml.db"),
)

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

@contextmanager
def db_conn():
    conn = _connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def init_db() -> None:
    with db_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            employee_id TEXT PRIMARY KEY,
            given_name   TEXT NOT NULL DEFAULT '',
            family_name  TEXT NOT NULL DEFAULT '',
            role         TEXT NOT NULL DEFAULT '',
            manager      TEXT NOT NULL DEFAULT '',
            status       TEXT NOT NULL DEFAULT 'active',
            created_at   TEXT NOT NULL,
            updated_at   TEXT NOT NULL
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS hr_events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ts           TEXT NOT NULL,
            event_type   TEXT NOT NULL,
            employee_id  TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            FOREIGN KEY(employee_id) REFERENCES employees(employee_id) ON DELETE CASCADE
        );
        """)

def upsert_employee(
    employee_id: str,
    given_name: str = "",
    family_name: str = "",
    role: str = "",
    manager: str = "",
    status: str = "active",
) -> None:
    ts = datetime.utcnow().isoformat()
    with db_conn() as conn:
        conn.execute(
            """
            INSERT INTO employees (employee_id, given_name, family_name, role, manager, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(employee_id) DO UPDATE SET
                given_name=excluded.given_name,
                family_name=excluded.family_name,
                role=excluded.role,
                manager=excluded.manager,
                status=excluded.status,
                updated_at=excluded.updated_at;
            """,
            (employee_id, given_name, family_name, role, manager, status, ts, ts),
        )

def set_employee_status(employee_id: str, status: str) -> None:
    ts = datetime.utcnow().isoformat()
    with db_conn() as conn:
        conn.execute(
            "UPDATE employees SET status=?, updated_at=? WHERE employee_id=?;",
            (status, ts, employee_id),
        )

def log_hr_event(event_type: str, employee_id: str, payload_json: str) -> None:
    ts = datetime.utcnow().isoformat()
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO hr_events (ts, event_type, employee_id, payload_json) VALUES (?, ?, ?, ?);",
            (ts, event_type, employee_id, payload_json),
        )

def list_employees() -> List[Dict[str, Any]]:
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT employee_id, given_name, family_name, role, manager, status, created_at, updated_at FROM employees ORDER BY employee_id;"
        ).fetchall()
    return [dict(r) for r in rows]

def list_hr_events(limit: int = 200) -> List[Dict[str, Any]]:
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT id, ts, event_type, employee_id, payload_json FROM hr_events ORDER BY id DESC LIMIT ?;",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]

def get_employee(employee_id: str) -> Optional[Dict[str, Any]]:
    with db_conn() as conn:
        row = conn.execute(
            "SELECT employee_id, given_name, family_name, role, manager, status, created_at, updated_at FROM employees WHERE employee_id=?;",
            (employee_id,),
        ).fetchone()
    return dict(row) if row else None
