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
                spreadsheet_id TEXT DEFAULT '',
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

            -- 일별 지표 (대시보드/쿼리용)
            CREATE TABLE IF NOT EXISTS metrics (
                account_id TEXT NOT NULL,
                date TEXT NOT NULL,
                매출 INTEGER DEFAULT 0,
                구매건수 INTEGER DEFAULT 0,
                방문자수 INTEGER DEFAULT 0,
                방문당매출 INTEGER DEFAULT 0,
                신규방문 INTEGER DEFAULT 0,
                재방문 INTEGER DEFAULT 0,
                순방문자수 INTEGER DEFAULT 0,
                순방문비중 REAL DEFAULT 0,
                신규비중 REAL DEFAULT 0,
                재방문비중 REAL DEFAULT 0,
                전환율 REAL DEFAULT 0,
                구매개수 INTEGER DEFAULT 0,
                합구매 REAL DEFAULT 0,
                처음구매 INTEGER DEFAULT 0,
                처음구매비중 REAL DEFAULT 0,
                재구매 INTEGER DEFAULT 0,
                객단가 INTEGER DEFAULT 0,
                회원가입 INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT (datetime('now', 'localtime')),
                PRIMARY KEY (account_id, date),
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_metrics_date ON metrics(date);

            -- 시간별 지표 (1일 = 24행)
            CREATE TABLE IF NOT EXISTS metrics_hourly (
                account_id TEXT NOT NULL,
                date TEXT NOT NULL,
                hour INTEGER NOT NULL,
                매출 INTEGER DEFAULT 0,
                구매건수 INTEGER DEFAULT 0,
                객단가 INTEGER DEFAULT 0,
                매출액비교 INTEGER DEFAULT 0,
                매출액증감 INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT (datetime('now', 'localtime')),
                PRIMARY KEY (account_id, date, hour),
                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_metrics_hourly_date ON metrics_hourly(date);
        """)

        # 기존 DB에 spreadsheet_id 컬럼 없으면 추가 (마이그레이션)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()]
        if "spreadsheet_id" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN spreadsheet_id TEXT DEFAULT ''")


# --- 일별 지표 CRUD ---

METRIC_COLS = [
    "매출", "구매건수", "방문자수", "방문당매출", "신규방문", "재방문",
    "순방문자수", "순방문비중", "신규비중", "재방문비중", "전환율",
    "구매개수", "합구매", "처음구매", "처음구매비중", "재구매",
    "객단가", "회원가입",
]


def upsert_metrics(account_id, date, metrics):
    """metrics dict (sheets.extract_metrics 결과)를 (account_id, date) 키로 upsert.
    비정상 날짜(미래 / 90일 이전)는 무시 - 스크래퍼 캘린더 클릭 오류 방어."""
    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        delta_days = (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - dt).days
        if delta_days < 0 or delta_days > 90:
            print(f"[upsert_metrics] 비정상 날짜 무시: {account_id} {date} ({delta_days}일)")
            return
    except (ValueError, TypeError):
        return
    cols = ["account_id", "date"] + METRIC_COLS + ["updated_at"]
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join(f'"{c}"' for c in cols)
    update_clause = ",".join(f'"{c}"=excluded."{c}"' for c in METRIC_COLS + ["updated_at"])
    values = [account_id, date] + [metrics.get(c, 0) or 0 for c in METRIC_COLS] + [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    with db_conn() as conn:
        conn.execute(
            f"INSERT INTO metrics ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(account_id, date) DO UPDATE SET {update_clause}",
            values,
        )


def list_metrics(account_id=None, start_date=None, end_date=None):
    """일별 지표 조회 (account/date 범위 필터). 날짜 오름차순."""
    sql = 'SELECT * FROM metrics WHERE 1=1'
    params = []
    if account_id:
        if isinstance(account_id, (list, tuple)):
            sql += f" AND account_id IN ({','.join('?' * len(account_id))})"
            params.extend(account_id)
        else:
            sql += " AND account_id = ?"
            params.append(account_id)
    if start_date:
        sql += " AND date >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND date <= ?"
        params.append(end_date)
    sql += " ORDER BY date ASC, account_id ASC"
    with db_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def get_metric(account_id, date):
    with db_conn() as conn:
        r = conn.execute("SELECT * FROM metrics WHERE account_id=? AND date=?", (account_id, date)).fetchone()
        return dict(r) if r else None


HOURLY_COLS = ["매출", "구매건수", "객단가", "매출액비교", "매출액증감"]


def upsert_metrics_hourly(account_id, date, hourly_rows):
    """hourly_rows: list of dict {hour: int 0~23, 매출, 구매건수, 객단가, ...}.
    같은 (account_id, date, hour) 키는 덮어씀."""
    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        delta_days = (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - dt).days
        if delta_days < 0 or delta_days > 90:
            print(f"[upsert_metrics_hourly] 비정상 날짜 무시: {account_id} {date} ({delta_days}일)")
            return 0
    except (ValueError, TypeError):
        return 0
    cols = ["account_id", "date", "hour"] + HOURLY_COLS + ["updated_at"]
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join(f'"{c}"' for c in cols)
    update_clause = ",".join(f'"{c}"=excluded."{c}"' for c in HOURLY_COLS + ["updated_at"])
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    n = 0
    with db_conn() as conn:
        for r in hourly_rows:
            if r.get("hour") is None:
                continue
            values = [account_id, date, int(r["hour"])] + [r.get(c, 0) or 0 for c in HOURLY_COLS] + [now]
            conn.execute(
                f"INSERT INTO metrics_hourly ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT(account_id, date, hour) DO UPDATE SET {update_clause}",
                values,
            )
            n += 1
    return n


def list_metrics_hourly(account_id, date):
    """단일 일자의 24시간 데이터 (hour 오름차순)."""
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM metrics_hourly WHERE account_id=? AND date=? ORDER BY hour ASC",
            (account_id, date),
        ).fetchall()
        return [dict(r) for r in rows]


# --- 계정 CRUD ---

def list_accounts():
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM accounts ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]


def get_account(account_id):
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
        return dict(row) if row else None


def add_account(cafe24_id, sub_id, password, label="", spreadsheet_id=""):
    with db_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO accounts (id, cafe24_id, sub_id, password, label, spreadsheet_id) VALUES (?, ?, ?, ?, ?, ?)",
            (cafe24_id, cafe24_id, sub_id, password, label, spreadsheet_id),
        )


def update_spreadsheet_id(account_id, spreadsheet_id):
    with db_conn() as conn:
        conn.execute(
            "UPDATE accounts SET spreadsheet_id=? WHERE id=?",
            (spreadsheet_id, account_id),
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


def _extract_single_day(result, date_str):
    """range 결과에서 특정 날짜 1일치만 필터해 단일일자 형태로 변환."""
    def _filter(section):
        if not isinstance(section, dict):
            return section
        out = {}
        for sub, t in section.items():
            if isinstance(t, dict) and "rows" in t:
                rows = [r for r in t.get("rows", []) if r and r[0] == date_str]
                out[sub] = {"headers": t.get("headers", []), "rows": rows}
            else:
                out[sub] = t
        return out
    return {
        "account": result.get("account", ""),
        "date": date_str,
        "매출종합분석": _filter(result.get("매출종합분석", {})),
        "방문자분석": _filter(result.get("방문자분석", {})),
        "처음방문vs재방문": _filter(result.get("처음방문vs재방문", {})),
        "신규회원": _filter(result.get("신규회원", {})),
        # 팝업 데이터는 row[0]이 일자라 그대로 둠 (sheets.extract_metrics에서 result["date"] 매칭)
        "매출종합_상세": result.get("매출종합_상세", {}),
        "구매패턴_상세": result.get("구매패턴_상세", {}),
    }


def get_result(account_id, date_str):
    """단일일자 JSON 우선, 없으면 range JSON(*_to_*.json) 중 해당 날짜 포함하는 것 fallback."""
    account_dir = Path(__file__).parent / "data" / account_id
    single = account_dir / f"{date_str}.json"
    if single.exists():
        with open(single, encoding="utf-8") as f:
            return json.load(f)
    if not account_dir.exists():
        return None
    for f in sorted(account_dir.glob("*_to_*.json")):
        try:
            start, end = f.stem.split("_to_")
            if start <= date_str <= end:
                with open(f, encoding="utf-8") as fh:
                    return _extract_single_day(json.load(fh), date_str)
        except (ValueError, OSError):
            continue
    return None


def list_result_dates(account_id):
    """결과 파일이 있는 모든 일자(단일 + range 펼친 것) 내림차순.
    range 파일은 metrics 테이블에 들어있는 일자만 노출 (실제로 데이터 있는 날만)."""
    account_dir = Path(__file__).parent / "data" / account_id
    if not account_dir.exists():
        return []
    dates = set()
    for f in account_dir.glob("*.json"):
        stem = f.stem
        if "_to_" in stem:
            try:
                start, end = stem.split("_to_")
                # metrics 테이블에 있는 날짜만 (실제 데이터 들어간 날)
                with db_conn() as conn:
                    rows = conn.execute(
                        "SELECT date FROM metrics WHERE account_id=? AND date BETWEEN ? AND ?",
                        (account_id, start, end),
                    ).fetchall()
                for r in rows:
                    dates.add(r[0])
            except ValueError:
                continue
        else:
            dates.add(stem)
    return sorted(dates, reverse=True)


# 초기화
init_db()
