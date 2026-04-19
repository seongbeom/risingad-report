"""Cafe24 애널리틱스 스크래퍼 - Web UI"""

import functools
import threading
import traceback
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, redirect, render_template, request, url_for, session, g

import db
import scraper

app = Flask(__name__)
app.secret_key = "cafe24-scraper-secret-key-change-me"

# 로그인 설정
ADMIN_USER = "admin"
ADMIN_PASS = "admin"


def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated

scheduler = BackgroundScheduler()
scheduler.start()

# 실행 중인 작업 추적
_running = {}  # account_id -> run_id


def _run_scrape_task(account_id):
    """백그라운드 스레드에서 스크래핑 실행"""
    account = db.get_account(account_id)
    if not account:
        return

    run_id = db.add_run(account_id)
    _running[account_id] = run_id

    try:
        results = scraper.run_scrape(account)
        today = datetime.now().strftime("%Y-%m-%d")
        result_file = f"data/{account_id}/{today}.json"
        db.finish_run(run_id, "success", result_file=result_file)
    except Exception as e:
        db.finish_run(run_id, "error", error=traceback.format_exc())
    finally:
        _running.pop(account_id, None)


def _scheduled_job(account_id):
    """스케줄러에서 호출"""
    if account_id in _running:
        return  # 이미 실행 중이면 스킵
    t = threading.Thread(target=_run_scrape_task, args=(account_id,), daemon=True)
    t.start()


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
    db.add_account(cafe24_id, sub_id, password, label)
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
                d = r["started_at"][:10]
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
    app.run(debug=True, port=9090, use_reloader=False)
