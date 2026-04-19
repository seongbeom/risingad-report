"""SQLite DB - 계정, 스케줄, 실행 로그 관리"""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "cafe24.db"


def get_db():
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@contextmanager
def db_conn():
    conn = get_db()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with db_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                cafe24_id TEXT NOT NULL,
                sub_id TEXT NOT NULL,
                password TEXT NOT NULL,
                label TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now', 'localtime'))
            );

            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                cron_hour INTEGER NOT NULL DEFAULT 8,
                cron_minute INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                started_at TEXT DEFAULT (datetime('now', 'localtime')),
                finished_at TEXT,
                status TEXT DEFAULT 'running',
                result_file TEXT,
                error TEXT,
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            );
        """)


# --- 계정 CRUD ---

def list_accounts():
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM accounts ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]


def get_account(account_id):
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
        return dict(row) if row else None


def add_account(cafe24_id, sub_id, password, label=""):
    with db_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO accounts (id, cafe24_id, sub_id, password, label) VALUES (?, ?, ?, ?, ?)",
            (cafe24_id, cafe24_id, sub_id, password, label),
        )


def delete_account(account_id):
    with db_conn() as conn:
        conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))


# --- 스케줄 CRUD ---

def list_schedules():
    with db_conn() as conn:
        rows = conn.execute("""
            SELECT s.*, a.cafe24_id, a.label
            FROM schedules s JOIN accounts a ON s.account_id = a.id
            ORDER BY s.cron_hour, s.cron_minute
        """).fetchall()
        return [dict(r) for r in rows]


def get_schedule(account_id):
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM schedules WHERE account_id = ?", (account_id,)).fetchone()
        return dict(row) if row else None


def upsert_schedule(account_id, hour, minute, enabled=True):
    with db_conn() as conn:
        existing = conn.execute("SELECT id FROM schedules WHERE account_id = ?", (account_id,)).fetchone()
        if existing:
            conn.execute(
                "UPDATE schedules SET cron_hour=?, cron_minute=?, enabled=? WHERE account_id=?",
                (hour, minute, int(enabled), account_id),
            )
        else:
            conn.execute(
                "INSERT INTO schedules (account_id, cron_hour, cron_minute, enabled) VALUES (?, ?, ?, ?)",
                (account_id, hour, minute, int(enabled)),
            )


def delete_schedule(account_id):
    with db_conn() as conn:
        conn.execute("DELETE FROM schedules WHERE account_id = ?", (account_id,))


# --- 실행 로그 ---

def add_run(account_id):
    with db_conn() as conn:
        cur = conn.execute("INSERT INTO runs (account_id) VALUES (?)", (account_id,))
        return cur.lastrowid


def finish_run(run_id, status, result_file=None, error=None):
    with db_conn() as conn:
        conn.execute(
            "UPDATE runs SET finished_at=datetime('now','localtime'), status=?, result_file=?, error=? WHERE id=?",
            (status, result_file, error, run_id),
        )


def list_runs(account_id=None, limit=20):
    with db_conn() as conn:
        if account_id:
            rows = conn.execute(
                "SELECT r.*, a.cafe24_id FROM runs r JOIN accounts a ON r.account_id = a.id WHERE r.account_id=? ORDER BY r.id DESC LIMIT ?",
                (account_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT r.*, a.cafe24_id FROM runs r JOIN accounts a ON r.account_id = a.id ORDER BY r.id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]


def get_result(account_id, date_str):
    result_file = Path(__file__).parent / "data" / account_id / f"{date_str}.json"
    if result_file.exists():
        with open(result_file, encoding="utf-8") as f:
            return json.load(f)
    return None


# 초기화
init_db()
