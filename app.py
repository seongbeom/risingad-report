"""Cafe24 애널리틱스 스크래퍼 - Web UI"""

import functools
import os
import subprocess
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from flask import Flask, jsonify, redirect, render_template, request, url_for, session, g

import db
import scraper
import sheets

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "cafe24-scraper-secret-key-change-me")

# 로그인 설정 (.env)
# ADMIN_USERS="alice:pw1,bob:pw2" 형식으로 여러 사용자 지정 가능 (첫 번째가 super admin)
# 미지정 시 기존 ADMIN_USER / ADMIN_PASS 단일 계정으로 fallback (그 단일 계정이 admin)
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "admin")


def _users():
    raw = os.environ.get("ADMIN_USERS", "").strip()
    pairs = []
    if raw:
        for chunk in raw.split(","):
            chunk = chunk.strip()
            if ":" in chunk:
                u, p = chunk.split(":", 1)
                if u and p:
                    pairs.append((u.strip(), p.strip()))
    if not pairs:
        pairs = [(ADMIN_USER, ADMIN_PASS)]
    return pairs


def _is_admin(username):
    pairs = _users()
    return bool(pairs) and pairs[0][0] == username


def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated


@app.context_processor
def _inject_user():
    """모든 템플릿에서 user / is_admin 사용 가능하도록.
    구버전 세션(logged_in 만 있고 username 없는 경우)도 첫 번째 admin 으로 자동 채움."""
    user = session.get("username")
    if not user and session.get("logged_in"):
        pairs = _users()
        if pairs:
            user = pairs[0][0]
            session["username"] = user
    return {"user": user, "is_admin": _is_admin(user) if user else False}

# 잡을 스케줄러 큐로 직렬 실행 (동시 Chromium 구동 방지 — 메모리 보호)
# timezone 명시 (Asia/Seoul) — 안 하면 서버 UTC로 동작해서 한국시간과 9시간 어긋남
scheduler = BackgroundScheduler(
    timezone="Asia/Seoul",
    executors={"default": ThreadPoolExecutor(max_workers=1)},
    job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 3600},
)
scheduler.start()

# 실행 중인 작업 추적
_running = {}  # account_id -> run_id
_run_lock = threading.Lock()  # 수동 실행과 스케줄 잡이 동시에 안 돌도록


SCRAPE_MAX_ATTEMPTS = 3  # 1차 + 재시도 2회 (reCAPTCHA 풀이 운에 의존하기 때문)
SCRAPE_RETRY_DELAY_SEC = 15

# t3.small (2GB) 메모리 가드 - 다음 chromium 띄우기 전 free memory 확보 대기
MIN_FREE_MB_BEFORE_NEXT = 350
MEM_WAIT_MAX_SEC = 90
INTER_ACCOUNT_COOLDOWN_SEC = 8


def _free_memory_mb():
    """/proc/meminfo 의 MemAvailable (kB) 를 MB 로 반환. 실패 시 None."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
    except Exception:
        return None
    return None


def _kill_leftover_chromium():
    """앞 사이클에서 남은 chromium / playwright node 좀비 정리. t3.small 메모리 회복용."""
    try:
        subprocess.run(["pkill", "-9", "-f", "chromium-browser-"], timeout=5, check=False)
        subprocess.run(["pkill", "-9", "-f", "playwright/driver/node"], timeout=5, check=False)
    except Exception:
        pass


def _wait_for_memory(label=""):
    """다음 계정 시작 전 free memory 확보. 부족하면 chromium 정리 후 대기."""
    free = _free_memory_mb()
    if free is None or free >= MIN_FREE_MB_BEFORE_NEXT:
        return
    print(f"[mem] {label} free={free}MB < {MIN_FREE_MB_BEFORE_NEXT}MB, chromium 정리 후 대기")
    _kill_leftover_chromium()
    waited = 0
    while waited < MEM_WAIT_MAX_SEC:
        time.sleep(5)
        waited += 5
        free = _free_memory_mb()
        if free is not None and free >= MIN_FREE_MB_BEFORE_NEXT:
            print(f"[mem] {label} 회복: free={free}MB ({waited}s)")
            return
    print(f"[mem] {label} 회복 안 됨 (free={free}MB) - 그래도 진행")


def _run_scrape_task(account_id, target_date=None, skip_sheet=False):
    """스크래핑 실행 - 직렬화 락 안에서 돌림 (동시 Chromium 방지).
    실패 시 N회 재시도. is_sample(Premium 만료)는 재시도 의미 없으니 즉시 종료.
    target_date 지정 시 해당 일자로, 미지정 시 어제.
    skip_sheet=True 면 Google Sheet write 건너뜀 (라이브 모드용 - 오늘 진행중인 데이터 시트 오염 방지)."""
    with _run_lock:
        account = db.get_account(account_id)
        if not account:
            return

        run_id = db.add_run(account_id)
        _running[account_id] = run_id

        attempts_used = 0
        try:
            results = None
            last_err = None
            for attempt in range(1, SCRAPE_MAX_ATTEMPTS + 1):
                attempts_used = attempt
                try:
                    results = scraper.run_scrape(account, target_date=target_date)
                    break
                except Exception:
                    last_err = traceback.format_exc()
                    print(f"[{account_id}] attempt {attempt}/{SCRAPE_MAX_ATTEMPTS} 실패")
                    if attempt < SCRAPE_MAX_ATTEMPTS:
                        time.sleep(SCRAPE_RETRY_DELAY_SEC)
            if results is None:
                db.finish_run(run_id, "error", error=last_err or "scrape failed after retries", attempts=attempts_used)
                return

            scraped_date = results.get("date") or datetime.now().strftime("%Y-%m-%d")
            result_file = f"data/{account_id}/{scraped_date}.json"

            # 카페24 Premium 만료 등으로 sample(데모) 데이터가 반환된 경우는 시트/DB 모두 스킵
            if results.get("_is_sample"):
                msg = f"[{account_id}] is_sample=True - 카페24 Premium 만료 또는 권한 문제. 시트/DB 입력 스킵"
                print(msg)
                db.finish_run(run_id, "error", error="cafe24 returned sample data (premium expired?)", attempts=attempts_used)
                return

            # DB 저장 (대시보드 쿼리용)
            metrics = sheets.extract_metrics(results)
            db.upsert_metrics(account_id, scraped_date, metrics)

            # 시간별 metrics
            hourly_rows = sheets.extract_hourly_rows(results)
            if hourly_rows:
                n = db.upsert_metrics_hourly(account_id, scraped_date, hourly_rows)
                print(f"[{account_id}] {scraped_date} 시간별 {n}행 upsert")

            spreadsheet_id = account.get("spreadsheet_id") or ""
            if skip_sheet:
                print(f"[{account_id}] live 모드 - 시트 write 스킵")
            elif spreadsheet_id:
                try:
                    sheets.write_result(results, spreadsheet_id=spreadsheet_id)
                except Exception:
                    traceback.print_exc()
            else:
                print(f"[{account_id}] spreadsheet_id 미설정 - 시트 입력 스킵, JSON만 저장됨")

            db.finish_run(run_id, "success", result_file=result_file, attempts=attempts_used)
        except Exception:
            db.finish_run(run_id, "error", error=traceback.format_exc(), attempts=attempts_used or 1)
        finally:
            _running.pop(account_id, None)


def _date_from_run(run):
    """runs row → 결과 JSON 파일 날짜. result_file 경로(date.json)가 있으면 그걸,
    없으면 started_at 첫 10자(타임존 따라 다를 수 있음)로 fallback."""
    if run.get("result_file"):
        return Path(run["result_file"]).stem
    return (run.get("started_at") or "")[:10]


def _short_error(err):
    """traceback 전체에서 사람이 빠르게 읽을 수 있는 한 줄 요약을 뽑는다.
    마지막 비어있지 않은 라인이 보통 'ExceptionType: 메시지' 형태라 그걸 우선."""
    if not err:
        return ""
    lines = [ln.strip() for ln in str(err).splitlines() if ln.strip()]
    if not lines:
        return ""
    last = lines[-1]
    if len(last) > 140:
        last = last[:137] + "..."
    return last


def _scheduled_job(account_id):
    """(레거시) 스케줄러에서 직접 호출. 신규 아키텍처에선 _daily_finalize_job 으로 일원화됨.
    호환을 위해 남겨둠 - 수동 실행은 _run_scrape_task 가 직접 처리."""
    _run_scrape_task(account_id)


def _daily_finalize_job():
    """매일 새벽 글로벌 cron. 어제 데이터를 모든 계정 풀스크랩 + 시트 write.
    라이브가 어제 23:30 직전까지 채워뒀어도 보정용 + 시트 기록을 위해 한 번 더 정리."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    accounts = db.list_accounts()
    print(f"[daily-finalize] {yesterday} {len(accounts)}계정 시작 free={_free_memory_mb()}MB")
    for i, a in enumerate(accounts):
        _wait_for_memory(f"before {a['id']}")
        try:
            _run_scrape_task(a["id"], target_date=yesterday, skip_sheet=False)
        except Exception:
            traceback.print_exc()
        if i < len(accounts) - 1:
            time.sleep(INTER_ACCOUNT_COOLDOWN_SEC)
    _kill_leftover_chromium()
    print(f"[daily-finalize] {yesterday} done free={_free_memory_mb()}MB")


def _live_global_job():
    """글로벌 라이브 인터벌. 활성 시간대면 모든 계정을 직렬로 오늘 데이터 스크랩 (시트 write 안 함)."""
    s = db.get_live_settings()
    if s["interval_min"] <= 0:
        return
    now_h = datetime.now().hour
    # 활성 시간 체크 (start_hour <= now < end_hour, end_hour=24 면 23시까지 포함)
    start, end = s["start_hour"], s["end_hour"]
    if start <= end:
        active = start <= now_h < end
    else:  # end < start (예: 22~6 야간)
        active = now_h >= start or now_h < end
    if not active:
        print(f"[live] {now_h}시는 활성 시간({start}~{end}) 아님 - 스킵")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    accounts = db.list_accounts()
    print(f"[live] {today} {len(accounts)}계정 시작 free={_free_memory_mb()}MB")
    for i, a in enumerate(accounts):
        _wait_for_memory(f"before {a['id']}")
        try:
            _run_scrape_task(a["id"], target_date=today, skip_sheet=True)
        except Exception:
            traceback.print_exc()
        if i < len(accounts) - 1:
            time.sleep(INTER_ACCOUNT_COOLDOWN_SEC)
    _kill_leftover_chromium()
    print(f"[live] {today} cycle done free={_free_memory_mb()}MB")


def reload_schedules():
    """DB 설정을 APScheduler 에 반영. 글로벌 잡 2개:
    - live_global: 오늘 데이터 누적 갱신 (DB only)
    - daily_finalize: 매일 새벽 어제 데이터 풀스크랩 + 시트 write
    계정별 cron은 더 이상 사용 안 함 (단순 시트 write 시각이 글로벌이면 충분)."""
    for job in scheduler.get_jobs():
        if job.id.startswith("scrape_") or job.id in ("live_global", "daily_finalize"):
            scheduler.remove_job(job.id)

    live = db.get_live_settings()
    if live["interval_min"] > 0:
        scheduler.add_job(
            _live_global_job, "interval",
            minutes=live["interval_min"],
            id="live_global", replace_existing=True,
        )
        print(f"[scheduler] live_global: {live['interval_min']}분, {live['start_hour']}~{live['end_hour']}시")

    df = db.get_daily_finalize_settings()
    scheduler.add_job(
        _daily_finalize_job, "cron",
        hour=df["hour"], minute=df["minute"],
        id="daily_finalize", replace_existing=True,
    )
    print(f"[scheduler] daily_finalize: 매일 {df['hour']:02d}:{df['minute']:02d}")


# 서버 시작 시 스케줄 로드
reload_schedules()


# ===== 로그인 =====

@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        u = request.form.get("username", "").strip()
        p = request.form.get("password", "").strip()
        for valid_u, valid_p in _users():
            if u == valid_u and p == valid_p:
                session["logged_in"] = True
                session["username"] = valid_u
                return redirect(url_for("index"))
        return render_template("login.html", error="아이디 또는 비밀번호가 틀렸습니다.")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.pop("logged_in", None)
    session.pop("username", None)
    return redirect(url_for("login_page"))


# ===== 페이지 라우트 =====

@app.route("/")
@login_required
def index():
    accounts = db.list_accounts()
    runs = db.list_runs(limit=30)
    for r in runs:
        r["display_date"] = _date_from_run(r)
        r["hourly_count"] = db.count_metrics_hourly(r["account_id"], r["display_date"]) if r["display_date"] else 0
        r["error_summary"] = _short_error(r.get("error"))
    schedules = {s["account_id"]: s for s in db.list_schedules()}
    live = db.get_live_settings()
    daily = db.get_daily_finalize_settings()
    return render_template(
        "index.html",
        accounts=accounts,
        runs=runs,
        schedules=schedules,
        running=_running,
        live=live,
        daily=daily,
        now=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )


# ===== 계정 API =====

@app.route("/accounts", methods=["POST"])
@login_required
def add_account():
    cafe24_id = request.form["cafe24_id"].strip()
    sub_id = request.form.get("sub_id", "").strip()
    password = request.form["password"].strip()
    label = request.form.get("label", "").strip()
    spreadsheet_id = request.form.get("spreadsheet_id", "").strip()
    db.add_account(cafe24_id, sub_id, password, label, spreadsheet_id)
    return redirect(url_for("index"))


@app.route("/accounts/<account_id>/spreadsheet", methods=["POST"])
@login_required
def update_spreadsheet(account_id):
    sid = request.form.get("spreadsheet_id", "").strip()
    db.update_spreadsheet_id(account_id, sid)
    return redirect(url_for("index"))


@app.route("/admin/backfill_dates", methods=["POST"])
def admin_backfill_dates():
    """localhost 전용 백필 트리거. _run_lock 으로 라이브 잡과 자동 직렬화.
    body: account_id=..&dates=2026-05-08,2026-05-09 (콤마 구분)"""
    remote = request.remote_addr or ""
    if remote not in ("127.0.0.1", "::1", "localhost"):
        return jsonify({"error": "forbidden"}), 403
    account_id = request.form.get("account_id", "").strip()
    dates_raw = request.form.get("dates", "").strip()
    if not account_id or not dates_raw:
        return jsonify({"error": "account_id, dates required"}), 400
    dates = [d.strip() for d in dates_raw.split(",") if d.strip()]
    for d in dates:
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            return jsonify({"error": f"invalid date {d}"}), 400

    def _run_all():
        for d in dates:
            print(f"[backfill] {account_id} {d} 시작", flush=True)
            try:
                _run_scrape_task(account_id, target_date=d, skip_sheet=False)
            except Exception:
                traceback.print_exc()
            print(f"[backfill] {account_id} {d} 끝", flush=True)
        print(f"[backfill] {account_id} 전체 완료 ({len(dates)}건)", flush=True)

    threading.Thread(target=_run_all, daemon=True).start()
    return jsonify({"ok": True, "account_id": account_id, "queued_dates": dates})


@app.route("/accounts/<account_id>/update", methods=["POST"])
@login_required
def update_account_route(account_id):
    sub_id = request.form.get("sub_id", "").strip()
    password = request.form.get("password", "").strip()
    label = request.form.get("label", "").strip()
    db.update_account(
        account_id,
        sub_id=sub_id,
        password=(password if password else None),
        label=label,
    )
    return redirect(url_for("index"))


@app.route("/accounts/<account_id>/delete", methods=["POST"])
@login_required
def delete_account(account_id):
    db.delete_account(account_id)
    reload_schedules()
    return redirect(url_for("index"))


# ===== 스케줄 API =====

@app.route("/schedules", methods=["POST"])
@login_required
def save_schedule():
    account_id = request.form["account_id"]
    hour = int(request.form["hour"])
    minute = int(request.form["minute"])
    enabled = "enabled" in request.form
    db.upsert_schedule(account_id, hour, minute, enabled)
    reload_schedules()
    return redirect(url_for("index"))


@app.route("/schedules/<account_id>/delete", methods=["POST"])
@login_required
def delete_schedule(account_id):
    db.delete_schedule(account_id)
    reload_schedules()
    return redirect(url_for("index"))


@app.route("/settings/scheduler", methods=["POST"])
@login_required
def save_scheduler_settings():
    """글로벌 스케줄러 설정 저장 (라이브 + 데일리 finalize)."""
    try:
        interval = max(0, int(request.form.get("live_interval_min", "0")))
        start = max(0, min(24, int(request.form.get("live_start_hour", "8"))))
        end = max(0, min(24, int(request.form.get("live_end_hour", "24"))))
        df_h = max(0, min(23, int(request.form.get("daily_finalize_hour", "3"))))
        df_m = max(0, min(59, int(request.form.get("daily_finalize_minute", "0"))))
    except ValueError:
        return redirect(url_for("index"))
    db.set_setting("live_interval_min", interval)
    db.set_setting("live_start_hour", start)
    db.set_setting("live_end_hour", end)
    db.set_setting("daily_finalize_hour", df_h)
    db.set_setting("daily_finalize_minute", df_m)
    reload_schedules()
    return redirect(url_for("index"))


# ===== 실행 API =====

@app.route("/run/<account_id>", methods=["POST"])
@login_required
def run_now(account_id):
    if account_id in _running:
        return jsonify({"error": "이미 실행 중입니다"}), 409
    target_date = (request.form.get("date") or "").strip() or None
    if target_date:
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            target_date = None
    t = threading.Thread(target=_run_scrape_task, args=(account_id, target_date), daemon=True)
    t.start()
    return redirect(url_for("index"))


# ===== 결과 API =====

@app.route("/results/<account_id>")
@login_required
def results_page(account_id):
    account = db.get_account(account_id)
    runs = db.list_runs(account_id=account_id, limit=30)
    for r in runs:
        r["display_date"] = _date_from_run(r)
    available_dates = db.list_result_dates(account_id)
    # 가장 최근 성공 결과 로드
    date = request.args.get("date")
    result = None
    if date:
        result = db.get_result(account_id, date)
    elif runs:
        for r in runs:
            if r["status"] == "success" and r["result_file"]:
                d = _date_from_run(r)
                result = db.get_result(account_id, d)
                if result:
                    date = d
                    break
    hourly = db.list_metrics_hourly(account_id, date) if date else []
    return render_template(
        "results.html",
        account=account,
        runs=runs,
        result=result,
        date=date,
        available_dates=available_dates,
        hourly=hourly,
    )


@app.route("/dashboard")
@login_required
def dashboard():
    """데일리 대시보드 - 오늘 누적 + 페이스/예상 + 어제 동시각 비교 + 7일 그리드 (숫자 중심)."""
    now = datetime.now()
    cur_hour = now.hour
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    last_week_same = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    range_start = (now - timedelta(days=8)).strftime("%Y-%m-%d")

    accounts = db.list_accounts()
    all_metrics = db.list_metrics(start_date=range_start, end_date=today)
    by_key = {(m["account_id"], m["date"]): m for m in all_metrics}

    # 최근 7일 hourly (시간대 평균 + 어제 동시각 누적)
    hourly_start = (now - timedelta(days=8)).strftime("%Y-%m-%d")
    all_hourly = db.list_metrics_hourly_range([a["id"] for a in accounts], hourly_start, yesterday)
    # account_id -> list[24] of sales (각 시간 매출 평균 계산용)
    hour_buckets = {}
    # account_id -> date -> dict[hour] = 매출 (어제 동시각 누적 + 페이스 계산용)
    by_acct_date_hour = {}
    for r in all_hourly:
        hour_buckets.setdefault(r["account_id"], [[] for _ in range(24)])[r["hour"]].append(r.get("매출") or 0)
        by_acct_date_hour.setdefault(r["account_id"], {}).setdefault(r["date"], {})[r["hour"]] = r.get("매출") or 0

    def _pct(cur, ref):
        if cur is None or ref is None or not ref:
            return None
        return round((cur - ref) / ref * 100, 1)

    last7_dates = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, 8)]

    rows = []
    for a in accounts:
        aid = a["id"]
        m_today = by_key.get((aid, today), {}) or {}
        m_yest = by_key.get((aid, yesterday), {}) or {}
        m_lw = by_key.get((aid, last_week_same), {}) or {}
        last7_vals_sales = [by_key.get((aid, d), {}).get("매출") for d in last7_dates if by_key.get((aid, d))]
        last7_avg_sales = round(sum(v for v in last7_vals_sales if v is not None) / len(last7_vals_sales)) if last7_vals_sales else None

        today_sales = m_today.get("매출")
        today_orders = m_today.get("구매건수")
        today_visitors = m_today.get("방문자수")
        today_aov = m_today.get("객단가")

        # 어제 동시각 누적 매출 (어제 0~cur_hour 합) — 사과대사과 비교 핵심
        ydh = by_acct_date_hour.get(aid, {}).get(yesterday, {})
        yest_at_hour = sum(ydh.get(h, 0) for h in range(cur_hour + 1))

        # 페이스 계산: 최근 7일 평균에서 0~cur_hour 매출 / 전체 매출 = 누적 비중
        # 평균이 의미 있어야 함 (3일 이상 표본). 매출 0 인 계정 제외
        days_with_data = set()
        sum_at_hour = 0  # 7일 동안 0~cur_hour 매출 합
        sum_full_day = 0  # 7일 동안 전체 매출 합
        for d in last7_dates:
            day_map = by_acct_date_hour.get(aid, {}).get(d)
            if not day_map:
                continue
            days_with_data.add(d)
            for h, v in day_map.items():
                sum_full_day += v
                if h <= cur_hour:
                    sum_at_hour += v
        cum_pct_at_hour = (sum_at_hour / sum_full_day * 100) if sum_full_day else None
        # 도달 예상치 = 현재 매출 / (cum_pct / 100). cum_pct 너무 작으면(<5%) 신뢰 낮음
        expected_eod = None
        if today_sales and cum_pct_at_hour and cum_pct_at_hour >= 5 and len(days_with_data) >= 3:
            expected_eod = round(today_sales / (cum_pct_at_hour / 100))

        # 전환율 (구매건수/방문자수) %
        conv = round(today_orders / today_visitors * 100, 2) if today_orders and today_visitors else None
        conv_yest = round((m_yest.get("구매건수") or 0) / m_yest.get("방문자수") * 100, 2) if m_yest.get("방문자수") else None

        rows.append({
            "id": aid,
            "label": a.get("label") or a["cafe24_id"],
            "cafe24_id": a["cafe24_id"],
            "updated_at": m_today.get("updated_at"),
            "today": {
                "매출": today_sales,
                "구매건수": today_orders,
                "방문자수": today_visitors,
                "객단가": today_aov,
                "전환율": conv,
            },
            "yesterday": {
                "매출": m_yest.get("매출"),
                "구매건수": m_yest.get("구매건수"),
                "방문자수": m_yest.get("방문자수"),
                "객단가": m_yest.get("객단가"),
                "전환율": conv_yest,
            },
            "yest_at_hour": yest_at_hour,  # 어제 같은 시각까지 누적 매출
            "vs_yest_at_hour_pct": _pct(today_sales, yest_at_hour),
            "cum_pct_at_hour": cum_pct_at_hour,  # 7일 평균 기준 현재 시각 진행률
            "expected_eod": expected_eod,
            "last7_avg_sales": last7_avg_sales,
            "vs_yesterday_pct": _pct(today_sales, m_yest.get("매출")),
            "vs_lastweek_pct": _pct(today_sales, m_lw.get("매출")),
            "vs_7avg_pct": _pct(today_sales, last7_avg_sales),
        })

    # 매출 큰 순 정렬 (오늘 매출 없는 계정은 뒤로)
    rows.sort(key=lambda r: (r["today"]["매출"] or 0), reverse=True)

    # 상단 메가 KPI 집계
    sum_today_sales = sum((r["today"]["매출"] or 0) for r in rows)
    sum_today_orders = sum((r["today"]["구매건수"] or 0) for r in rows)
    sum_today_visitors = sum((r["today"]["방문자수"] or 0) for r in rows)
    sum_yest_at_hour = sum((r["yest_at_hour"] or 0) for r in rows)
    sum_yest_full = sum((r["yesterday"]["매출"] or 0) for r in rows)
    sum_expected = sum((r["expected_eod"] or 0) for r in rows)
    active_count = sum(1 for r in rows if (r["today"]["매출"] or 0) > 0)

    mega = {
        "today_sales": sum_today_sales,
        "today_orders": sum_today_orders,
        "today_visitors": sum_today_visitors,
        "yest_at_hour": sum_yest_at_hour,
        "yest_full": sum_yest_full,
        "expected_eod": sum_expected,
        "vs_yest_at_hour_pct": _pct(sum_today_sales, sum_yest_at_hour),
        "vs_yest_full_pct": _pct(sum_expected if sum_expected else sum_today_sales, sum_yest_full),
        "active": active_count,
        "total_accounts": len(rows),
        "cur_hour": cur_hour,
    }

    # 하이라이트: 매출 1위, 가장 큰 ▲ / ▼
    rows_with_sales = [r for r in rows if (r["today"]["매출"] or 0) > 0]
    best = rows_with_sales[0] if rows_with_sales else None
    rows_with_delta = [r for r in rows_with_sales if r["vs_yest_at_hour_pct"] is not None]
    top_gainer = max(rows_with_delta, key=lambda r: r["vs_yest_at_hour_pct"]) if rows_with_delta else None
    top_loser = min(rows_with_delta, key=lambda r: r["vs_yest_at_hour_pct"]) if rows_with_delta else None

    # 최근 9일 (오늘~8일 전) 매출 그리드
    DAYS_KO = ["월", "화", "수", "목", "금", "토", "일"]
    grid_dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(8, -1, -1)]  # 8일전~오늘
    grid_dows = [DAYS_KO[datetime.strptime(d, "%Y-%m-%d").weekday()] for d in grid_dates]
    grid = []
    for a in accounts:
        aid = a["id"]
        cells = []
        for d in grid_dates:
            m = by_key.get((aid, d))
            cells.append({
                "date": d,
                "매출": m.get("매출") if m else None,
                "구매건수": m.get("구매건수") if m else None,
                "is_today": d == today,
            })
        grid.append({"label": a.get("label") or a["cafe24_id"], "id": aid, "cells": cells})

    # 시간대 평균 매출 표 (계정 × 24시간)
    hour_grid = []
    for a in accounts:
        aid = a["id"]
        buckets = hour_buckets.get(aid, [[] for _ in range(24)])
        avg = [round(sum(b) / len(b)) if b else None for b in buckets]
        max_v = max((v for v in avg if v is not None), default=0)
        hour_grid.append({"label": a.get("label") or a["cafe24_id"], "id": aid, "hours": avg, "max_v": max_v})

    capsolver = db.capsolver_stats()
    capsolver["balance"] = scraper.capsolver_balance()

    return render_template(
        "dashboard.html",
        accounts=accounts,
        rows=rows,
        mega=mega,
        best=best,
        top_gainer=top_gainer,
        top_loser=top_loser,
        grid=grid,
        grid_dates=grid_dates,
        grid_dows=grid_dows,
        hour_grid=hour_grid,
        today=today,
        yesterday=yesterday,
        last_week_same=last_week_same,
        capsolver=capsolver,
        now=now.strftime("%H:%M"),
    )


@app.route("/dashboard/range")
@login_required
def dashboard_range():
    """기존 기간 범위 KPI + 차트 대시보드 (옵션)."""
    accounts = db.list_accounts()
    all_ids = [a["id"] for a in accounts]
    selected_ids = request.args.getlist("account_id") or all_ids

    # 기간: 기본 어제로부터 6일 전 ~ 어제 (7일)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_str = request.args.get("end_date") or yesterday
    start_str = request.args.get("start_date") or (datetime.strptime(end_str, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt
        start_str, end_str = end_str, start_str
    range_days = (end_dt - start_dt).days + 1
    # 직전 동일 기간 (비교용)
    prev_end_dt = start_dt - timedelta(days=1)
    prev_start_dt = prev_end_dt - timedelta(days=range_days - 1)
    prev_start_str = prev_start_dt.strftime("%Y-%m-%d")
    prev_end_str = prev_end_dt.strftime("%Y-%m-%d")

    KPI_FIELDS = [
        ("방문자수", "방문자수", "명", "sum"),
        ("매출", "매출", "원", "sum"),
        ("구매건수", "구매건수", "건", "sum"),
        ("객단가", "객단가", "원", "avg"),  # 일평균 객단가
    ]

    def _diff(cur, ref):
        if cur is None or ref is None or not ref:
            return None
        return {"abs": cur - ref, "pct": (cur - ref) / ref * 100}

    def _aggregate(rows, col, mode):
        vals = [r.get(col) for r in rows if r.get(col) is not None]
        if not vals:
            return None
        if mode == "sum":
            return sum(vals)
        if mode == "avg":
            return sum(vals) / len(vals)
        return None

    DAYS_KO = ["월", "화", "수", "목", "금", "토", "일"]

    panels = []
    for aid in selected_ids:
        account = next((a for a in accounts if a["id"] == aid), None)
        if not account:
            continue
        history = db.list_metrics(aid, start_str, end_str)
        prev_history = db.list_metrics(aid, prev_start_str, prev_end_str)
        hourly_rows = db.list_metrics_hourly_range(aid, start_str, end_str)

        kpis = []
        for label, col, unit, mode in KPI_FIELDS:
            v = _aggregate(history, col, mode)
            ref = _aggregate(prev_history, col, mode)
            kpis.append({
                "label": label,
                "value": v,
                "unit": unit,
                "mode": mode,
                "vs_prev": _diff(v, ref),
            })

        # 시간대 평균 (24시간) - 기간 평균 매출/건수
        hour_buckets_sales = [[] for _ in range(24)]
        hour_buckets_orders = [[] for _ in range(24)]
        # 요일×시간 히트맵 (매출 sum)
        dow_hour_sales = [[0] * 24 for _ in range(7)]
        dow_hour_count = [[0] * 24 for _ in range(7)]  # 표본 일자 수
        for r in hourly_rows:
            h = r["hour"]
            hour_buckets_sales[h].append(r.get("매출") or 0)
            hour_buckets_orders[h].append(r.get("구매건수") or 0)
            try:
                d = datetime.strptime(r["date"], "%Y-%m-%d")
                dow = d.weekday()
                dow_hour_sales[dow][h] += (r.get("매출") or 0)
                dow_hour_count[dow][h] += 1
            except Exception:
                pass

        avg_sales_by_hour = [round(sum(b) / len(b)) if b else None for b in hour_buckets_sales]
        avg_orders_by_hour = [round(sum(b) / len(b), 1) if b else None for b in hour_buckets_orders]

        # 요일별 평균 매출 (시간 합쳐 일별 sum → 평균)
        dow_daily = [[] for _ in range(7)]
        for m in history:
            try:
                dow = datetime.strptime(m["date"], "%Y-%m-%d").weekday()
                if m.get("매출") is not None:
                    dow_daily[dow].append(m["매출"])
            except Exception:
                pass
        avg_sales_by_dow = [round(sum(b) / len(b)) if b else None for b in dow_daily]

        # 피크 시간 (기간 평균 기준)
        valid_hours = [(h, avg_sales_by_hour[h]) for h in range(24) if avg_sales_by_hour[h] is not None]
        peak_hour = max(valid_hours, key=lambda x: x[1])[0] if valid_hours else None
        # 누적 매출 곡선 (24시간 - 평균 매출 누적)
        cum = []
        s = 0
        for v in avg_sales_by_hour:
            s += (v or 0)
            cum.append(s)
        cum_total = cum[-1] if cum else 0
        cum_pct = [round(c / cum_total * 100, 1) if cum_total else None for c in cum]

        panels.append({
            "account": account,
            "kpis": kpis,
            "has_data": bool(history),
            "history": history,
            "avg_sales_by_hour": avg_sales_by_hour,
            "avg_orders_by_hour": avg_orders_by_hour,
            "avg_sales_by_dow": avg_sales_by_dow,
            "dow_hour_sales": dow_hour_sales,
            "dow_hour_count": dow_hour_count,
            "peak_hour": peak_hour,
            "cum_pct": cum_pct,
            "hourly_total_days": len({r["date"] for r in hourly_rows}),
        })

    # 기간 내 모든 날짜 + 요일 (그리드용)
    range_dates = []
    range_dows = []
    cur = start_dt
    while cur <= end_dt:
        ds = cur.strftime("%Y-%m-%d")
        range_dates.append(ds)
        range_dows.append(DAYS_KO[cur.weekday()])
        cur += timedelta(days=1)

    # 각 panel 에 daily lookup 추가 (그리드 셀 채울 때 사용)
    for p in panels:
        lookup = {m["date"]: m for m in p["history"]}
        p["daily_lookup"] = {d: lookup.get(d) for d in range_dates}

    # 다계정 비교: 일별 KPI line (옵션 차트 - 유지하되 사용 안 해도 됨)
    compare_charts = []

    # 시간대 평균 다계정 overlay
    hourly_compare = None
    if any(any(v is not None for v in p["avg_sales_by_hour"]) for p in panels):
        hourly_compare = {
            "hours": list(range(24)),
            "datasets": [
                {
                    "label": p["account"]["label"] or p["account"]["cafe24_id"],
                    "sales": p["avg_sales_by_hour"],
                    "orders": p["avg_orders_by_hour"],
                }
                for p in panels
            ],
        }

    return render_template(
        "dashboard_range.html",
        accounts=accounts,
        selected_ids=selected_ids,
        start_date=start_str,
        end_date=end_str,
        range_days=range_days,
        prev_start=prev_start_str,
        prev_end=prev_end_str,
        panels=panels,
        compare_charts=compare_charts,
        hourly_compare=hourly_compare,
        kpi_fields=[k[0] for k in KPI_FIELDS],
        days_ko=DAYS_KO,
        range_dates=range_dates,
        range_dows=range_dows,
    )


@app.route("/api/status")
@login_required
def api_status():
    """실행 상태 확인 (폴링용)"""
    return jsonify({
        "running": list(_running.keys()),
        "jobs": [
            {"id": j.id, "next": str(j.next_run_time)}
            for j in scheduler.get_jobs()
        ],
    })


# ===== 팀 피드백/메모 위젯 =====

def _current_user_or_401():
    user = session.get("username")
    if not user and session.get("logged_in"):
        pairs = _users()
        if pairs:
            user = pairs[0][0]
            session["username"] = user
    return user or None


@app.route("/api/feedback", methods=["GET"])
@login_required
def api_feedback_list():
    user = _current_user_or_401()
    if not user:
        return jsonify({"error": "unauthorized"}), 401
    return jsonify({
        "user": user,
        "is_admin": _is_admin(user),
        "threads": db.list_feedback_threads(),
    })


@app.route("/api/feedback", methods=["POST"])
@login_required
def api_feedback_create():
    user = _current_user_or_401()
    if not user:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    body = (payload.get("body") or "").strip()
    if not body:
        return jsonify({"error": "body 비어있음"}), 400
    thread = db.add_feedback_thread(user, body, time.time())
    return jsonify({"thread": thread})


@app.route("/api/feedback/<int:fid>/reply", methods=["POST"])
@login_required
def api_feedback_reply(fid):
    user = _current_user_or_401()
    if not user:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    body = (payload.get("body") or "").strip()
    if not body:
        return jsonify({"error": "body 비어있음"}), 400
    reply = db.add_feedback_reply(fid, user, body, time.time())
    if reply is None:
        return jsonify({"error": "스레드 루트가 아님"}), 404
    return jsonify({"reply": reply})


@app.route("/api/feedback/<int:fid>/status", methods=["POST"])
@login_required
def api_feedback_status(fid):
    user = _current_user_or_401()
    if not user:
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    status = (payload.get("status") or "").strip()
    if status not in db.FEEDBACK_STATUSES:
        return jsonify({"error": "허용되지 않은 status"}), 400
    res = db.update_feedback_status(fid, status)
    if res is None:
        return jsonify({"error": "스레드 루트가 아님"}), 404
    if not res:
        return jsonify({"error": "잘못된 status"}), 400
    return jsonify({"ok": True, "id": fid, "status": status})


@app.route("/api/feedback/<int:fid>/delete", methods=["POST"])
@login_required
def api_feedback_delete(fid):
    user = _current_user_or_401()
    if not user:
        return jsonify({"error": "unauthorized"}), 401
    target = db.get_feedback(fid)
    if not target:
        return jsonify({"error": "not found"}), 404
    if target["author"] != user and not _is_admin(user):
        return jsonify({"error": "권한 없음"}), 403
    db.delete_feedback(fid)
    return jsonify({"ok": True, "id": fid})


@app.route("/feedback")
@login_required
def feedback_page():
    return render_template("feedback.html", threads=db.list_feedback_threads())


@app.route("/feedback/<int:fid>/status", methods=["POST"])
@login_required
def feedback_status_form(fid):
    """위젯 없이 form-redirect 로 status 변경 (전체 페이지에서)."""
    user = _current_user_or_401()
    if not user:
        return redirect(url_for("login_page"))
    status = request.form.get("status", "").strip()
    if status in db.FEEDBACK_STATUSES:
        db.update_feedback_status(fid, status)
    return redirect(request.referrer or url_for("feedback_page"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=False, port=9090, use_reloader=False)
