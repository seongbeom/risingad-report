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

            # 시간별 metrics
            hourly_rows = sheets.extract_hourly_rows(results)
            if hourly_rows:
                n = db.upsert_metrics_hourly(account_id, scraped_date, hourly_rows)
                print(f"[{account_id}] {scraped_date} 시간별 {n}행 upsert")

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
    for r in runs:
        r["display_date"] = _date_from_run(r)
        r["hourly_count"] = db.count_metrics_hourly(r["account_id"], r["display_date"]) if r["display_date"] else 0
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
    """기간 범위 KPI + 시간별 인사이트 대시보드."""
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

    # 다계정 비교: 일별 KPI line
    compare_charts = []
    if len(panels) >= 2:
        all_dates = sorted({m["date"] for p in panels for m in p["history"]})
        for label, col, unit, _ in KPI_FIELDS:
            datasets = []
            for p in panels:
                lookup = {m["date"]: m.get(col) for m in p["history"]}
                datasets.append({
                    "label": p["account"]["label"] or p["account"]["cafe24_id"],
                    "data": [lookup.get(d) for d in all_dates],
                })
            compare_charts.append({"label": label, "unit": unit, "labels": all_dates, "datasets": datasets})

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
        "dashboard.html",
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
