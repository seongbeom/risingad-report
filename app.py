"""Cafe24 애널리틱스 스크래퍼 - Web UI"""

import functools
import os
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

# 로그인 설정 (.env 의 ADMIN_USER / ADMIN_PASS 우선)
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "admin")


def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated

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


def _run_scrape_task(account_id):
    """스크래핑 실행 - 직렬화 락 안에서 돌림 (동시 Chromium 방지).
    실패 시 N회 재시도. is_sample(Premium 만료)는 재시도 의미 없으니 즉시 종료."""
    with _run_lock:
        account = db.get_account(account_id)
        if not account:
            return

        run_id = db.add_run(account_id)
        _running[account_id] = run_id

        try:
            results = None
            last_err = None
            for attempt in range(1, SCRAPE_MAX_ATTEMPTS + 1):
                try:
                    results = scraper.run_scrape(account)
                    break
                except Exception:
                    last_err = traceback.format_exc()
                    print(f"[{account_id}] attempt {attempt}/{SCRAPE_MAX_ATTEMPTS} 실패")
                    if attempt < SCRAPE_MAX_ATTEMPTS:
                        time.sleep(SCRAPE_RETRY_DELAY_SEC)
            if results is None:
                db.finish_run(run_id, "error", error=last_err or "scrape failed after retries")
                return

            scraped_date = results.get("date") or datetime.now().strftime("%Y-%m-%d")
            result_file = f"data/{account_id}/{scraped_date}.json"

            # 카페24 Premium 만료 등으로 sample(데모) 데이터가 반환된 경우는 시트/DB 모두 스킵
            if results.get("_is_sample"):
                msg = f"[{account_id}] is_sample=True - 카페24 Premium 만료 또는 권한 문제. 시트/DB 입력 스킵"
                print(msg)
                db.finish_run(run_id, "error", error="cafe24 returned sample data (premium expired?)")
                return

            # DB 저장 (대시보드 쿼리용)
            metrics = sheets.extract_metrics(results)
            db.upsert_metrics(account_id, scraped_date, metrics)

            spreadsheet_id = account.get("spreadsheet_id") or ""
            if spreadsheet_id:
                try:
                    sheets.write_result(results, spreadsheet_id=spreadsheet_id)
                except Exception:
                    traceback.print_exc()
            else:
                print(f"[{account_id}] spreadsheet_id 미설정 - 시트 입력 스킵, JSON만 저장됨")

            db.finish_run(run_id, "success", result_file=result_file)
        except Exception:
            db.finish_run(run_id, "error", error=traceback.format_exc())
        finally:
            _running.pop(account_id, None)


def _date_from_run(run):
    """runs row → 결과 JSON 파일 날짜. result_file 경로(date.json)가 있으면 그걸,
    없으면 started_at 첫 10자(타임존 따라 다를 수 있음)로 fallback."""
    if run.get("result_file"):
        return Path(run["result_file"]).stem
    return (run.get("started_at") or "")[:10]


def _scheduled_job(account_id):
    """스케줄러에서 직접 호출 (max_workers=1로 큐잉돼 직렬 실행됨).
    수동 실행은 별도 thread로 띄우되 _run_lock으로 동일하게 직렬화."""
    _run_scrape_task(account_id)


def reload_schedules():
    """DB의 스케줄을 APScheduler에 반영"""
    # 기존 job 제거
    for job in scheduler.get_jobs():
        if job.id.startswith("scrape_"):
            scheduler.remove_job(job.id)

    # DB에서 활성 스케줄 로드
    for s in db.list_schedules():
        if s["enabled"]:
            job_id = f"scrape_{s['account_id']}"
            scheduler.add_job(
                _scheduled_job,
                "cron",
                hour=s["cron_hour"],
                minute=s["cron_minute"],
                args=[s["account_id"]],
                id=job_id,
                replace_existing=True,
            )


# 서버 시작 시 스케줄 로드
reload_schedules()


# ===== 로그인 =====

@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        if request.form["username"] == ADMIN_USER and request.form["password"] == ADMIN_PASS:
            session["logged_in"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="아이디 또는 비밀번호가 틀렸습니다.")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.pop("logged_in", None)
    return redirect(url_for("login_page"))


# ===== 페이지 라우트 =====

@app.route("/")
@login_required
def index():
    accounts = db.list_accounts()
    runs = db.list_runs(limit=30)
    schedules = {s["account_id"]: s for s in db.list_schedules()}
    return render_template(
        "index.html",
        accounts=accounts,
        runs=runs,
        schedules=schedules,
        running=_running,
        now=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )


# ===== 계정 API =====

@app.route("/accounts", methods=["POST"])
@login_required
def add_account():
    cafe24_id = request.form["cafe24_id"].strip()
    sub_id = request.form["sub_id"].strip()
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


# ===== 실행 API =====

@app.route("/run/<account_id>", methods=["POST"])
@login_required
def run_now(account_id):
    if account_id in _running:
        return jsonify({"error": "이미 실행 중입니다"}), 409
    t = threading.Thread(target=_run_scrape_task, args=(account_id,), daemon=True)
    t.start()
    return redirect(url_for("index"))


# ===== 결과 API =====

@app.route("/results/<account_id>")
@login_required
def results_page(account_id):
    account = db.get_account(account_id)
    runs = db.list_runs(account_id=account_id, limit=30)
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
    return render_template(
        "results.html",
        account=account,
        runs=runs,
        result=result,
        date=date,
    )


@app.route("/dashboard")
@login_required
def dashboard():
    """일별 KPI 대시보드 (단일/다계정 + 전일·전주 비교)."""
    accounts = db.list_accounts()
    all_ids = [a["id"] for a in accounts]
    selected_ids = request.args.getlist("account_id") or all_ids
    date_str = request.args.get("date") or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    base = datetime.strptime(date_str, "%Y-%m-%d")
    prev_day_str = (base - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_week_str = (base - timedelta(days=7)).strftime("%Y-%m-%d")

    KPI_FIELDS = [
        ("방문자수", "방문자수", "명"),
        ("매출", "매출", "원"),
        ("구매건수", "구매건수", "건"),
        ("객단가", "객단가", "원"),
    ]

    def _diff(cur, ref):
        if cur is None or ref is None or not ref:
            return None
        return {"abs": cur - ref, "pct": (cur - ref) / ref * 100}

    history_start = (base - timedelta(days=13)).strftime("%Y-%m-%d")  # 14일 윈도우 (오늘 포함)
    panels = []
    for aid in selected_ids:
        account = next((a for a in accounts if a["id"] == aid), None)
        if not account:
            continue
        cur = db.get_metric(aid, date_str) or {}
        d1 = db.get_metric(aid, prev_day_str) or {}
        d7 = db.get_metric(aid, prev_week_str) or {}
        kpis = []
        for label, col, unit in KPI_FIELDS:
            v = cur.get(col)
            kpis.append({
                "label": label,
                "value": v,
                "unit": unit,
                "vs_day": _diff(v, d1.get(col)),
                "vs_week": _diff(v, d7.get(col)),
            })
        history = db.list_metrics(aid, history_start, date_str)
        panels.append({
            "account": account,
            "kpis": kpis,
            "has_data": bool(cur),
            "history": history,
        })

    # KPI별 다계정 비교 데이터 (선택된 계정이 2개 이상일 때 의미)
    compare_charts = []
    if len(panels) >= 2:
        # 14일치 날짜 라벨 통합
        all_dates = sorted({m["date"] for p in panels for m in p["history"]})
        for label, col, unit in KPI_FIELDS:
            datasets = []
            for p in panels:
                lookup = {m["date"]: m.get(col) for m in p["history"]}
                datasets.append({
                    "label": p["account"]["label"] or p["account"]["cafe24_id"],
                    "data": [lookup.get(d) for d in all_dates],
                })
            compare_charts.append({"label": label, "unit": unit, "labels": all_dates, "datasets": datasets})

    return render_template(
        "dashboard.html",
        accounts=accounts,
        selected_ids=selected_ids,
        date=date_str,
        prev_day=prev_day_str,
        prev_week=prev_week_str,
        panels=panels,
        compare_charts=compare_charts,
        kpi_fields=[k[0] for k in KPI_FIELDS],
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=False, port=9090, use_reloader=False)
