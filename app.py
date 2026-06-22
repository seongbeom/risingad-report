"""Cafe24 애널리틱스 스크래퍼 - Web UI"""

import functools
import json
import os
import subprocess
import sys
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
import meta
import naver
import criteo
import gfa
import shopbox


SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
SLACK_FEEDBACK_WEBHOOK_URL = os.environ.get("SLACK_FEEDBACK_WEBHOOK_URL", "").strip()


def slack_notify(text, severity="info", webhook_url=None):
    """Slack webhook 알림 발송. webhook URL 없으면 silent skip.
    severity: critical / warn / info / ok / hang / cleanup / report / feedback
    webhook_url 미지정 시 기본 SLACK_WEBHOOK_URL 사용.
    실패해도 main flow 안 막음."""
    target_url = webhook_url or SLACK_WEBHOOK_URL
    if not target_url:
        return
    icons = {
        "critical": "🚨", "warn": "⚠️", "info": "ℹ️", "ok": "✅",
        "hang": "⏰", "cleanup": "🔧", "report": "📊", "feedback": "💬",
    }
    icon = icons.get(severity, "ℹ️")
    payload = {"text": f"{icon} *[cafe24-scraper]* {text}"}

    def _send():
        try:
            import urllib.request, json as _json
            req = urllib.request.Request(
                target_url,
                data=_json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            traceback.print_exc()

    threading.Thread(target=_send, daemon=True).start()


def _criteo_session_check_job():
    """크리테오 크롤 세션 만료 임박/만료 시 Slack 경고 (여유있게 미리)."""
    try:
        st = criteo.session_status()
        print(f"[criteo] session check: {st['message']}", flush=True)
        if st["severity"] in ("warn", "critical"):
            slack_notify(st["message"] + " → 크리테오_세션갱신.command 더블클릭",
                         severity=st["severity"])
    except Exception:
        traceback.print_exc()


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "cafe24-scraper-secret-key-change-me")
# 템플릿 변경을 프로세스 재시작 없이 즉시 반영 — 화면(템플릿)만 바꾸는 배포는 리로드 불필요
# → 라이브 스크래퍼를 안 건드림(deploy.sh 가 .py 변경 없을 때 reload 스킵).
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.auto_reload = True

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
    executors={
        "default": ThreadPoolExecutor(max_workers=1),  # 스크랩 등 무거운 잡 (chromium 직렬)
        "quick": ThreadPoolExecutor(max_workers=2),    # 핑·리로드체크 등 가벼운 잡 — 긴 스크랩에 안 막힘
    },
    job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 3600},
)
scheduler.start()

# 실행 중인 작업 추적
_running = {}  # account_id -> run_id
_run_lock = threading.Lock()  # 수동 실행과 스케줄 잡이 동시에 안 돌도록

# 프로세스 시작 시각 (graceful reload 후 새 프로세스인지 deploy.sh 가 확인)
_PROC_STARTED = datetime.now().isoformat()
# graceful reload 요청 플래그 (배포가 /admin/request_reload 로 세움 → 안전지점에서 os.execv)
_pending_reload = {"at": None}


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


SCRAPE_PER_ATTEMPT_TIMEOUT_SEC = 480  # 8분 - 캡챠 풀이 포함 정상 실행 ~2~3분, 그 이상 hang 으로 간주

# 연속 hang 카운터 — playwright/chromium 누적 상태로 "여러 계정" launch 가 모두 hang 하는
# systemic wedge 감지용. account-level 로 셈: 한 계정이 모든 재시도 hang 해도 +1 (그 계정은
# 스킵하고 다음으로). 서로 다른 계정이 연속 3개 hang 해야 systemic 으로 보고 self-restart.
# 단일 불량 계정(예: 특정 시간대 cinderella) 이 전체 finalize/재시작을 막지 않게 함.
_consecutive_hangs = 0
CONSECUTIVE_HANG_LIMIT = 3


EARLY_PHASES = ("chromium launch", "ensure_login")


def _note_account_hang(account_id, context=""):
    """한 계정이 모든 재시도에서 hang 으로 실패.
    단, hang 시점이 'chromium launch'/'ensure_login' 같은 초기 단계일 때만 systemic 카운터에 반영.
    데이터 페이지(매출/방문자 등)까지 진입한 뒤 느려서 hang 한 건 = 계정/사이트 느림이지
    chromium wedge 가 아니므로 self-restart 대상이 아님 (재시작해도 그 계정은 또 느림)."""
    global _consecutive_hangs
    last_phase = scraper.LAST_PHASE.get(account_id, "")
    is_early = (not last_phase) or any(last_phase.startswith(p) for p in EARLY_PHASES)
    if not is_early:
        # 데이터 단계 도달 후 hang → 그 계정만 스킵, systemic 아님. 카운터 건드리지 않음.
        print(f"[hang] {account_id} 데이터 단계({last_phase})에서 hang → 계정 느림으로 스킵 (systemic 제외) {context}", flush=True)
        slack_notify(
            f"`{account_id}` 느려서 모든 재시도 hang → 스킵 (마지막 단계: {last_phase}). "
            f"브라우저는 정상이라 systemic 아님{(' · ' + context) if context else ''}",
            severity="hang",
        )
        return
    _consecutive_hangs += 1
    print(f"[hang] {account_id} 초기단계({last_phase}) hang (연속 {_consecutive_hangs}/{CONSECUTIVE_HANG_LIMIT}) {context}", flush=True)
    slack_notify(
        f"`{account_id}` 초기단계 hang → 스킵 "
        f"(서로 다른 계정 연속 {_consecutive_hangs}/{CONSECUTIVE_HANG_LIMIT}, chromium wedge 의심){(' · ' + context) if context else ''}",
        severity="hang",
    )
    if _consecutive_hangs >= CONSECUTIVE_HANG_LIMIT:
        _self_restart_service(f"서로 다른 계정 {CONSECUTIVE_HANG_LIMIT}개 초기단계 연속 hang → systemic chromium wedge")
        _consecutive_hangs = 0


def _note_account_success():
    """계정 1개라도 정상 완료 → systemic 카운터 리셋 (연속 아님)."""
    global _consecutive_hangs
    _consecutive_hangs = 0


def _self_restart_service(reason):
    """systemctl restart cafe24 - subprocess 로 비동기 호출.
    Restart=always 이므로 systemd 가 자동 재시작. startup catch-up 으로 누락 회복."""
    print(f"[self-heal] 서비스 self-restart: {reason}", flush=True)
    slack_notify(
        f"🆘 *서비스 self-heal restart* — {reason}\n"
        f"누적 hang 으로 chromium 모든 launch 가 stuck. systemctl restart 트리거.\n"
        f"30초 뒤 startup catch-up 으로 자동 회복 예정.",
        severity="critical",
    )
    try:
        # 약간 지연 후 restart (Slack 알림 전송 시간 확보)
        subprocess.Popen(
            ["bash", "-c", "sleep 5; sudo systemctl restart cafe24"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception:
        traceback.print_exc()


def _graceful_reexec(reason=""):
    """현재 프로세스를 새 코드로 자가 재적재(os.execv). systemctl restart 와 달리 PID 유지.
    안전지점(스크랩 중이 아닐 때, 또는 계정과 계정 사이)에서만 호출할 것.
    chromium/scheduler 정리 후 같은 인자로 재실행 → git pull 된 새 app.py 가 로드됨."""
    print(f"[reload] graceful 자가 리로드: {reason}", flush=True)
    _pending_reload["at"] = None
    try:
        _kill_leftover_chromium()
    except Exception:
        traceback.print_exc()
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    sys.stdout.flush(); sys.stderr.flush()
    os.execv(sys.executable, [sys.executable] + sys.argv)


def _reload_check_job():
    """매분 — reload 요청이 있고 라이브 스크랩이 안 돌고 있으면(idle) 즉시 자가 리로드.
    스크랩 중이면 _live_global_run 이 계정 경계에서 처리하므로 여기선 건너뜀."""
    if not _pending_reload["at"]:
        return
    if _running:  # 스크랩 진행 중 → 계정 경계에서 리로드 (여기선 대기)
        return
    if _live_lock.acquire(blocking=False):
        # 락 잡았다 = 라이브 사이클 안 돎 → 안전하게 리로드
        try:
            _graceful_reexec("idle reload 요청")
        finally:
            _live_lock.release()


class HangTimeout(TimeoutError):
    """스크래핑이 timeout 으로 hang 된 경우 (다른 에러와 구분 — systemic 카운터 판단용)."""


STALL_LIMIT_SEC = 240   # 한 phase 에서 4분 넘게 진전 없으면 hang (느린 계정 보호 + 진짜 멈춤 감지)
HARD_CAP_SEC = 1200     # 전체 20분 하드 캡 (느려도 이건 넘으면 강제 종료)


def _run_scrape_with_timeout(account, target_date, timeout_sec=None):
    """별도 thread 로 run_scrape 호출. 진행기반 watchdog:
    - phase(scraper.LAST_PHASE)가 STALL_LIMIT_SEC 동안 안 바뀌면 = 멈춤 → HangTimeout
    - 전체 HARD_CAP_SEC 넘으면 → HangTimeout
    느리지만 계속 진전하는 계정(rosy001 처럼 phase 가 1~2분마다 넘어감)은 완주시키고,
    진짜 stuck(한 단계서 4분+ 정지)은 빠르게 잡음. timeout_sec 인자는 하위호환용(미사용)."""
    aid = account.get("id", "")
    result_holder = {"result": None, "exc": None}

    def _target():
        try:
            result_holder["result"] = scraper.run_scrape(account, target_date=target_date)
        except Exception as e:
            result_holder["exc"] = e

    scraper.LAST_PHASE.pop(aid, None)
    t = threading.Thread(target=_target, daemon=True)
    t.start()
    start = time.monotonic()
    last_phase = None
    last_change = time.monotonic()
    while t.is_alive():
        t.join(timeout=10)
        if not t.is_alive():
            break
        now = time.monotonic()
        cur_phase = scraper.LAST_PHASE.get(aid)
        if cur_phase != last_phase:
            last_phase = cur_phase
            last_change = now
        stalled = now - last_change
        elapsed = now - start
        if stalled >= STALL_LIMIT_SEC or elapsed >= HARD_CAP_SEC:
            reason = f"{int(stalled)}s 진전없음(phase={last_phase})" if stalled >= STALL_LIMIT_SEC else f"전체 {int(elapsed)}s 초과"
            print(f"[{aid}] scrape hang - {reason} - chromium 강제 종료", flush=True)
            _kill_leftover_chromium()
            t.join(timeout=30)
            raise HangTimeout(f"scrape hang - {reason}")
    if result_holder["exc"]:
        raise result_holder["exc"]
    return result_holder["result"]


PRODUCT_PER_ATTEMPT_TIMEOUT_SEC = 300  # 5분 - 기간변경(달력) 포함 정상 ~1분, 그 이상 hang


def _run_product_with_timeout(account, period="7d", timeout_sec=PRODUCT_PER_ATTEMPT_TIMEOUT_SEC):
    """scrape_product_analytics(period) 를 별도 thread + timeout 으로 감쌈.
    hang 시 chromium kill → HangTimeout raise. systemic 판단은 호출부에서."""
    result_holder = {"result": None, "exc": None}

    def _target():
        try:
            result_holder["result"] = scraper.scrape_product_analytics(account, period=period)
        except Exception as e:
            result_holder["exc"] = e

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=timeout_sec)
    if t.is_alive():
        print(f"[{account['id']}] product scrape timeout {timeout_sec}s - chromium 강제 종료", flush=True)
        _kill_leftover_chromium()
        t.join(timeout=30)
        raise HangTimeout(f"product scrape timeout {timeout_sec}s")
    if result_holder["exc"]:
        raise result_holder["exc"]
    return result_holder["result"]


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
            all_hung = True  # 모든 attempt 가 hang(HangTimeout) 으로 실패했는지
            for attempt in range(1, SCRAPE_MAX_ATTEMPTS + 1):
                attempts_used = attempt
                try:
                    results = _run_scrape_with_timeout(account, target_date, SCRAPE_PER_ATTEMPT_TIMEOUT_SEC)
                    break
                except HangTimeout:
                    last_err = traceback.format_exc()
                    print(f"[{account_id}] attempt {attempt}/{SCRAPE_MAX_ATTEMPTS} hang")
                    if attempt < SCRAPE_MAX_ATTEMPTS:
                        time.sleep(SCRAPE_RETRY_DELAY_SEC)
                except Exception:
                    all_hung = False  # hang 이 아닌 다른 에러 (로그인/파싱 등)
                    last_err = traceback.format_exc()
                    print(f"[{account_id}] attempt {attempt}/{SCRAPE_MAX_ATTEMPTS} 실패")
                    if attempt < SCRAPE_MAX_ATTEMPTS:
                        time.sleep(SCRAPE_RETRY_DELAY_SEC)
            if results is None:
                # 모든 attempt 가 hang 이면 account-level hang 으로 기록 (systemic 카운터),
                # 그 외 에러면 단순 실패 (systemic 아님).
                if all_hung:
                    _note_account_hang(account_id, context=f"date={target_date}")
                db.finish_run(run_id, "error", error=last_err or "scrape failed after retries", attempts=attempts_used)
                return
            _note_account_success()  # 1개라도 성공 → systemic 카운터 리셋

            scraped_date = results.get("date") or datetime.now().strftime("%Y-%m-%d")
            result_file = f"data/{account_id}/{scraped_date}.json"

            # 카페24 Premium 만료 등으로 sample(데모) 데이터가 반환된 경우는 시트/DB 모두 스킵
            if results.get("_is_sample"):
                _sample_today[account_id] = scraped_date  # 자동백필이 이 매장 즉시 재시도 안 하게
                msg = f"[{account_id}] is_sample=True - 카페24 Premium 만료 또는 권한 문제. 시트/DB 입력 스킵"
                print(msg)
                label = account.get("label") or account_id
                # 시도(사이클)마다 매번 알림 — 등록 전까지 계속 리마인드 (쿨다운 없음)
                slack_notify(
                    f"🚨 *{label}* (`{account_id}`) — cafe24가 *샘플(데모) 데이터* 반환 ({scraped_date})\n"
                    f"→ **애널리틱스 Premium 구독 만료 또는 권한 문제** 가능성. 데이터 수집 중단됨.\n"
                    f"cafe24 해당 매장 구독 상태 확인 필요 (등록되면 다음 사이클에 자동 복구).",
                    severity="critical",
                )
                db.finish_run(run_id, "error", error="cafe24 returned sample data (premium expired?)", attempts=attempts_used)
                return

            # DB 저장 (대시보드 쿼리용)
            metrics = sheets.extract_metrics(results)

            # 가드: 빈 스크랩(매출=0 AND 방문자=0)이 기존 정상값을 덮어쓰지 않게.
            # 일 누적 데이터라 정상이면 매출/방문 중 하나는 0이 아님. 둘 다 0 = 스크랩 실패로 간주.
            # (진짜 무매출 매장도 방문자는 보통 1 이상 → 0/0 은 사실상 빈 페이지)
            new_sales = metrics.get("매출") or 0
            new_visitors = metrics.get("방문자수") or 0
            if new_sales == 0 and new_visitors == 0:
                existing = db.get_metric(account_id, scraped_date)
                if existing and ((existing.get("매출") or 0) > 0 or (existing.get("방문자수") or 0) > 0):
                    print(f"[{account_id}] {scraped_date} 빈 스크랩(매출0/방문0) - 기존값(매출 {existing.get('매출')}, 방문 {existing.get('방문자수')}) 보존, 덮어쓰기 스킵", flush=True)
                    slack_notify(
                        f"`{account_id}` {scraped_date} 빈 스크랩(0/0) 감지 → 기존값 보존 (덮어쓰기 차단)",
                        severity="warn",
                    )
                    db.finish_run(run_id, "error", error="empty scrape (sales=0,visitors=0) - 기존값 보존", attempts=attempts_used)
                    return
            db.upsert_metrics(account_id, scraped_date, metrics)

            # 시간별 metrics — 단, 시간별 팝업이 잘못된 기간(누적/타일)을 반환하는 케이스 방어.
            # 시간행 합계가 종합매출(오늘값)의 1.5배 초과면 기간 오류로 간주하고 저장 스킵.
            hourly_rows = sheets.extract_hourly_rows(results)
            # 시간별 방문자(어제 동시각 비교용) 머지 — finalize 등에서만 수집됨
            vis_hourly = results.get("방문자_시간별") or {}
            if vis_hourly:
                vis_hourly = {int(h): (v or 0) for h, v in vis_hourly.items()}
                # 1) 매출 시간 행에 방문자 채우고, 이미 있는 시간대 기록
                existing_hours = set()
                for r in (hourly_rows or []):
                    try:
                        h = int(r.get("hour"))
                        r["방문자"] = vis_hourly.get(h, 0)
                        existing_hours.add(h)
                    except (ValueError, TypeError):
                        pass
                # 2) 매출 없는 시간대 방문자도 행으로 추가 (저빈도 판매 매장 누락 방지)
                if hourly_rows is None:
                    hourly_rows = []
                for h, v in sorted(vis_hourly.items()):
                    if h not in existing_hours:
                        hourly_rows.append({"hour": h, "방문자": v})
            if hourly_rows:
                hsum = sum((r.get("매출") or 0) for r in hourly_rows)
                day_sales = metrics.get("매출") or 0
                if day_sales > 0 and hsum > day_sales * 1.5:
                    print(f"[{account_id}] {scraped_date} 시간별 합({hsum:,}) > 종합매출({day_sales:,})×1.5 — 기간 오류로 시간별 저장 스킵")
                    hourly_rows = None  # 검증에서도 제외
                else:
                    n = db.upsert_metrics_hourly(account_id, scraped_date, hourly_rows)
                    vn = sum(1 for r in hourly_rows if r.get("방문자"))
                    print(f"[{account_id}] {scraped_date} 시간별 {n}행 upsert (방문자 {vn}시간)")

            # 자동 교차검증 — 데이터 정합성 깨지면 사람 눈 없이 즉시 플래그.
            try:
                prev = db.get_metric(account_id, (datetime.strptime(scraped_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"))
                is_partial = (scraped_date == datetime.now().strftime("%Y-%m-%d"))  # 오늘=진행중
                warns = sheets.validate_metrics(metrics, hourly_rows, prev, is_partial=is_partial)
                # 매출 표가 사라진 경우(신형 PRO 화면 전환 등) — 팝업으로 대체수집은 되지만 사람이 알 수 있게 경고
                if metrics.get("_매출표없음"):
                    if metrics.get("매출") is None:
                        # 표도 팝업도 값을 못 줌 = 진짜 수집 실패
                        warns = (warns or []) + ["매출 표·팝업 모두 수집 실패 — 확인 필요"]
                    else:
                        # 표는 없지만 팝업으로 정상 대체수집됨(값 0이어도 정상 가능) — 정보성 안내
                        warns = (warns or []) + ["매출 표 없음(신형화면 의심) — 팝업으로 대체수집 중"]
                if warns:
                    label = account.get("label") or account_id
                    msg = f"[검증] {account_id} {scraped_date}: " + " / ".join(warns)
                    print(msg, flush=True)
                    db.add_sheet_log(account_id, "validate", scraped_date, 0, "warn", " / ".join(warns))
                    _validate_alert(account_id, label, scraped_date, warns)
                else:
                    # 이전 검증경고가 있었으면 재스크랩으로 해소됐으니 클리어
                    if account_id in _validate_warnings:
                        _validate_warnings.pop(account_id, None)
                        print(f"[검증] {account_id} {scraped_date} 정상화 — 경고 해소", flush=True)
                    # 마감된 날 검증 통과 시 'ok' 로그로 과거 warn 을 덮어 현황 자동 정상화
                    if not is_partial:
                        db.add_sheet_log(account_id, "validate", scraped_date, 0, "ok", "정합성 정상")
            except Exception:
                traceback.print_exc()

            # 상품 분석 (라이브/finalize 세션에서 같이 수집됨) — 항목별 date+period 로 저장.
            # daily: date=실제 데이터 날짜 / 7d: date=수집일.
            for prod in (results.get("product_list") or []):
                if prod.get("rows"):
                    try:
                        pdate = prod.get("date") or datetime.now().strftime("%Y-%m-%d")
                        db.upsert_product_metrics(account_id, pdate, prod["rows"], period=prod["period"])
                        print(f"[{account_id}] 상품 [{prod['period']}] {pdate} {len(prod['rows'])}건 저장")
                    except Exception:
                        traceback.print_exc()

            spreadsheet_id = account.get("spreadsheet_id") or ""
            if skip_sheet:
                print(f"[{account_id}] live 모드 - 시트 write 스킵")
            elif spreadsheet_id:
                try:
                    sheets.write_result(results, spreadsheet_id=spreadsheet_id)
                    # 마지막 시트 갱신 시각 기록 (계정 관리 화면 표시용)
                    try:
                        db.set_setting(f"sheet_updated_{account_id}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    except Exception:
                        pass
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
    cycle_deadline = time.monotonic() + max(60 * 60, len(accounts) * 10 * 60)
    print(f"[daily-finalize] {yesterday} {len(accounts)}계정 시작 free={_free_memory_mb()}MB")
    started_at = time.time()
    skipped = 0
    for i, a in enumerate(accounts):
        if time.monotonic() > cycle_deadline:
            skipped = len(accounts) - i
            print(f"[daily-finalize] cycle deadline 초과 - 남은 {skipped}계정 스킵")
            break
        _wait_for_memory(f"before {a['id']}")
        try:
            _run_scrape_task(a["id"], target_date=yesterday, skip_sheet=False)
        except Exception:
            traceback.print_exc()
        if i < len(accounts) - 1:
            time.sleep(INTER_ACCOUNT_COOLDOWN_SEC)
    _kill_leftover_chromium()
    elapsed = int(time.time() - started_at)
    print(f"[daily-finalize] {yesterday} done free={_free_memory_mb()}MB")
    # deadline 으로 중도 종료한 게 아니면 완료 마커 기록 (재시작 후 catch-up 판단용).
    # 중간에 self-heal restart 로 끊기면 이 줄까지 도달 못 하므로 마커 안 찍힘 → 재시작 후 catch-up 발동.
    if skipped == 0:
        try:
            db.set_setting("last_finalize_date", yesterday)
        except Exception:
            traceback.print_exc()
    # 결과 요약을 slack 으로
    try:
        with db.db_conn() as conn:
            since_ts = datetime.fromtimestamp(started_at).strftime("%Y-%m-%d %H:%M:%S")
            ok = conn.execute("SELECT COUNT(*) FROM runs WHERE started_at >= ? AND status='success'", (since_ts,)).fetchone()[0]
            err = conn.execute("SELECT COUNT(*) FROM runs WHERE started_at >= ? AND status='error'", (since_ts,)).fetchone()[0]
            zero_sales = conn.execute(
                "SELECT account_id FROM metrics WHERE date=? AND (매출 IS NULL OR 매출=0)", (yesterday,)
            ).fetchall()
        zero_list = ", ".join(f"`{r['account_id']}`" for r in zero_sales) or "없음"
        sev = "ok" if err == 0 and skipped == 0 else "warn"
        slack_notify(
            f"daily_finalize 완료 ({yesterday})\n"
            f"성공 {ok} · 실패 {err} · 스킵 {skipped} · 소요 {elapsed//60}분\n"
            f"매출 0원 계정: {zero_list}",
            severity="report" if sev == "ok" else "warn",
        )
    except Exception:
        traceback.print_exc()


_live_lock = threading.Lock()  # 라이브 사이클 동시 실행 방지 (chromium 병렬 = OOM 방지)


def _live_global_job():
    """글로벌 라이브 인터벌. 활성 시간대면 모든 계정을 직렬로 오늘 데이터 스크랩 (시트 write 안 함).
    부팅 catch-up 과 cron 이 겹쳐도 chromium 1개만 — 전역 락으로 보호."""
    if not _live_lock.acquire(blocking=False):
        print("[live] 이미 라이브 사이클 진행중 - 중복 실행 스킵")
        return
    try:
        _live_global_run()
    finally:
        _live_lock.release()


def _order_by_staleness(accounts, today):
    """가장 오래 안 갱신된(또는 오늘 한 번도 안 들어온) 매장부터 정렬.
    배포/크래시로 사이클이 중간에 끊겨도 늘 제일 뒤처진 매장부터 채워 → 특정 매장 굶주림(starvation) 방지.
    오늘 metrics updated_at 이 없으면 '' → 최우선, 있으면 오래된 시각 순."""
    try:
        with db.db_conn() as conn:
            upd = {aid: (ts or "") for aid, ts in conn.execute(
                "SELECT account_id, MAX(updated_at) FROM metrics WHERE date=? GROUP BY account_id", (today,))}
        return sorted(accounts, key=lambda a: upd.get(a["id"], ""))
    except Exception:
        traceback.print_exc()
        return accounts


def _live_global_run():
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

    # 사이클 시작 전 orphan chromium 청소 — 이전 사이클이 크래시/재시작으로 남긴 좀비가 있으면
    # 새 chromium 과 2개가 공존해 EPIPE/OOM 유발. 시작 시 깨끗이 비우고 출발.
    _kill_leftover_chromium()
    today = datetime.now().strftime("%Y-%m-%d")
    accounts = _order_by_staleness(db.list_accounts(), today)
    cycle_deadline = time.monotonic() + max(40 * 60, len(accounts) * 9 * 60)
    print(f"[live] {today} {len(accounts)}계정 시작(오래된순) free={_free_memory_mb()}MB deadline={int((cycle_deadline-time.monotonic())/60)}분")
    for i, a in enumerate(accounts):
        if _pending_reload["at"]:
            # 배포 reload 요청 → 계정 경계에서 깨끗이 끊고 새 코드로 자가 리로드
            print(f"[live] reload 요청 감지 — {i}계정 처리 후 리로드 (남은 건 새 코드가 stalest-first 로 이어감)", flush=True)
            _graceful_reexec("라이브 계정 경계")
        if time.monotonic() > cycle_deadline:
            print(f"[live] cycle deadline 초과 - 남은 {len(accounts)-i}계정 스킵")
            break
        _wait_for_memory(f"before {a['id']}")
        try:
            _run_scrape_task(a["id"], target_date=today, skip_sheet=True)
        except Exception:
            traceback.print_exc()
        if i < len(accounts) - 1:
            time.sleep(INTER_ACCOUNT_COOLDOWN_SEC)
    _kill_leftover_chromium()
    # 사이클 끝나면 누락된 계정 자동 백필 (현재 사이클 1회만 추가 시도)
    if time.monotonic() < cycle_deadline:
        _auto_backfill_missing(today, deadline=cycle_deadline)
    print(f"[live] {today} cycle done free={_free_memory_mb()}MB")
    if _pending_reload["at"]:
        _graceful_reexec("라이브 사이클 종료 후")


def _boot_catchup_job():
    """서비스 재시작 직후 1회 — 활성 시간대인데 '오래 안 갱신된 매장'이 있으면 즉시 라이브 1회.
    MAX 가 아니라 매장별로 봐서, 사이클 중간 크래시(앞 매장만 신선, 뒤 매장 오전값)도 복구.
    판단: 오늘 데이터 없는 매장이 있거나, 한 사이클(140분) 넘게 안 갱신된 매장이 있으면."""
    try:
        s = db.get_live_settings()
        if s["interval_min"] <= 0:
            return
        now = datetime.now()
        start, end = s["start_hour"], s["end_hour"]
        active = (start <= now.hour < end) if start <= end else (now.hour >= start or now.hour < end)
        if not active:
            print(f"[boot-catchup] {now.hour}시 비활성 — 스킵")
            return
        today = now.strftime("%Y-%m-%d")
        all_ids = [a["id"] for a in db.list_accounts()]
        if not all_ids:
            return
        ph = ",".join("?" * len(all_ids))
        with db.db_conn() as conn:
            rows = conn.execute(
                f"SELECT account_id, MAX(updated_at) FROM metrics WHERE date=? AND account_id IN ({ph}) GROUP BY account_id",
                [today] + all_ids).fetchall()
        have = {aid: u for aid, u in rows}
        missing = 0
        stale = 0
        oldest = 0
        for aid in all_ids:
            u = have.get(aid)
            if not u:
                missing += 1
                continue
            try:
                m = (now - datetime.strptime(u, "%Y-%m-%d %H:%M:%S")).total_seconds() / 60
                oldest = max(oldest, m)
                if m > 140:  # 한 사이클(약 2시간)을 넘긴 매장 = 진짜 지연
                    stale += 1
            except Exception:
                pass
        if missing > 0 or stale > 0:
            print(f"[boot-catchup] 미수집 {missing}개 / 140분+ 지연 {stale}개 (최고지연 {int(oldest)}분) → 즉시 라이브 1회")
            _live_global_job()
        else:
            print(f"[boot-catchup] 전 매장 최근 갱신(최고지연 {int(oldest)}분) — 스킵")
    except Exception:
        traceback.print_exc()


# (account_id, date) → 자동 백필 시도 시각. 같은 날 같은 계정에 무한 재시도 방지.
_auto_backfill_attempted = {}


_recent_backfill_attempted = {}  # (aid, date) -> 시도 횟수 (무한 재시도 방지)
_sample_today = {}  # account_id -> date. Premium 만료(sample) 매장 — 자동백필 즉시 재시도 스킵용


def _recent_gaps(days=3):
    """최근 N일(오늘 제외) 중 metrics 행이 '아예 없는'(한 번도 못 받은) (aid, date) 목록.
    - 매출 0 인 날은 실제 수집된 것이라 제외 — 진짜 빠진 것만.
    - Premium 만료(오늘 sample 감지)된 매장은 재수집해도 또 sample 이라 제외 (채워지지도 않는데 버튼에 뜨면 헷갈림)."""
    dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, days + 1)]
    today = datetime.now().strftime("%Y-%m-%d")
    gaps = []
    for a in db.list_accounts():
        if _sample_today.get(a["id"]) == today:  # Premium 만료 → 채울 수 없음
            continue
        for d in dates:
            if not db.get_metric(a["id"], d):
                gaps.append((a["id"], d))
    return gaps


def _backfill_recent_missing(days=3, manual=False):
    """최근 N일 중 데이터 행이 없는 (매장,날짜) 재수집 + 시트 기록.
    finalize 가 특정 매장만 실패해 전날치가 비는 갭을 자동/수동 복구. cafe24 7일 윈도우 내라 과거일 가능.
    라이브/finalize 와 chromium 충돌 안 나게 _live_lock 으로 보호. 반환: 처리한 (aid,date) 리스트."""
    if not _live_lock.acquire(blocking=False):
        print("[recent-backfill] 다른 사이클(라이브/finalize) 진행중 - 스킵")
        return []
    done = []
    try:
        cap = 5 if manual else 2
        gaps = [(aid, d) for aid, d in _recent_gaps(days)
                if _recent_backfill_attempted.get((aid, d), 0) < cap]
        if not gaps:
            print("[recent-backfill] 빠진 (매장,날짜) 없음")
            return []
        print(f"[recent-backfill] {len(gaps)}건 재수집 시작: {gaps[:12]}")
        deadline = time.monotonic() + 40 * 60
        for aid, d in gaps:
            if time.monotonic() > deadline:
                print("[recent-backfill] deadline 도달 - 남은 건 다음 회차")
                break
            _recent_backfill_attempted[(aid, d)] = _recent_backfill_attempted.get((aid, d), 0) + 1
            _wait_for_memory(f"recent-backfill {aid} {d}")
            try:
                _run_scrape_task(aid, target_date=d, skip_sheet=False)  # 시트에도 기록
                done.append((aid, d))
            except Exception:
                traceback.print_exc()
            time.sleep(INTER_ACCOUNT_COOLDOWN_SEC)
        _kill_leftover_chromium()
        print(f"[recent-backfill] 완료 {len(done)}건")
    finally:
        _live_lock.release()
    return done


def _auto_backfill_missing(today, deadline=None):
    """라이브 사이클 끝에 누락 계정 자동 백필 1회.
    기준:
    - metrics row 자체 없음 (오늘 단 한 번도 못 받음) → 백필
    - metrics 있지만 매출=0 AND 시간별 0건 (현재 8시 이상일 때만) → 백필
    하루 계정당 최대 1회 자동 시도 (계속 실패하는 계정으로 무한 루프 방지).
    """
    cur_h = datetime.now().hour
    accounts = db.list_accounts()
    targets = []
    for a in accounts:
        aid = a["id"]
        key = (aid, today)
        if _auto_backfill_attempted.get(key, 0) >= 2:  # 하루 계정당 자동 재시도 최대 2회
            continue
        if _sample_today.get(aid) == today:
            continue  # Premium 만료(sample) 매장 — 같은 사이클에 재시도해도 또 sample, 시간만 낭비
        m = db.get_metric(aid, today)
        try:
            h_count = db.count_metrics_hourly(aid, today)
        except Exception:
            h_count = 0
        missing = (
            not m
            or ((m.get("매출") or 0) == 0 and h_count == 0 and cur_h >= 8)
        )
        # 검증경고가 오늘자로 뜬 계정도 재스크랩 대상 (이번 월초 버그 같은 정합성 오류 자동 복구)
        vw = _validate_warnings.get(aid)
        has_validate_issue = bool(vw and vw.get("date") == today)
        if missing or has_validate_issue:
            targets.append(aid)
    if not targets:
        return
    print(f"[auto-backfill] {today} 누락 계정 자동 재시도: {targets}")
    for aid in targets:
        if deadline and time.monotonic() > deadline:
            print(f"[auto-backfill] cycle deadline 도달 - 남은 자동 백필 다음 사이클로")
            break
        _auto_backfill_attempted[(aid, today)] = _auto_backfill_attempted.get((aid, today), 0) + 1
        _wait_for_memory(f"auto-backfill {aid}")
        try:
            _run_scrape_task(aid, target_date=today, skip_sheet=True)
        except Exception:
            traceback.print_exc()
        time.sleep(INTER_ACCOUNT_COOLDOWN_SEC)
    _kill_leftover_chromium()


def _product_collect_job(periods=("7d", "yesterday")):
    """카페24 '상품 분석' top 5 수집. periods 각 기간을 계정별로 수집.
    '7d'=최근7일(페이지 기본), 'yesterday'=전일, 'today'=오늘.
    수집일(date)은 항상 오늘 날짜로 저장하고 period 로 구분."""
    today = datetime.now().strftime("%Y-%m-%d")
    accounts = db.list_accounts()
    plabel = ",".join(periods)
    print(f"[product-collect] {today} periods=[{plabel}] {len(accounts)}계정 시작 free={_free_memory_mb()}MB")
    cycle_deadline = time.monotonic() + max(30 * 60, len(accounts) * len(periods) * 4 * 60)
    ok = 0
    err = 0
    for i, a in enumerate(accounts):
        if time.monotonic() > cycle_deadline:
            print(f"[product-collect] cycle deadline 초과 - 남은 {len(accounts)-i}계정 스킵")
            break
        for period in periods:
            _wait_for_memory(f"product {period} before {a['id']}")
            with _run_lock:  # 라이브 잡과 직렬화
                try:
                    rows = _run_product_with_timeout(a, period=period)
                    if rows:
                        db.upsert_product_metrics(a["id"], today, rows, period=period)
                        print(f"[product-collect] {a['id']} [{period}] {len(rows)}건 저장")
                        ok += 1
                    else:
                        print(f"[product-collect] {a['id']} [{period}] 0건")
                    _note_account_success()
                except HangTimeout:
                    err += 1
                    _note_account_hang(a["id"], context=f"product_collect {period}")
                except Exception:
                    err += 1
                    traceback.print_exc()
            time.sleep(INTER_ACCOUNT_COOLDOWN_SEC)
    _kill_leftover_chromium()
    print(f"[product-collect] {today} [{plabel}] done — 성공 {ok} / 실패 {err}")
    slack_notify(
        f"product-collect 완료 ({today}) [{plabel}] · 성공 {ok} / 실패 {err}",
        severity="report" if err == 0 else "warn",
    )


META_BACKFILL_DAYS = 4  # 매 실행 시 최근 N일 재수집 (어트리뷰션 보정)


def _retry_sheet_write(fn, *args, tries=3):
    """구글시트 일시적 5xx(500/503)·rate limit 시 짧게 재시도. 최종 실패는 그대로 raise."""
    last = None
    for i in range(tries):
        try:
            return fn(*args)
        except Exception as e:
            msg = repr(e)
            transient = any(c in msg for c in ("[500]", "[503]", "[429]", "InternalError",
                                               "Internal error", "currently unavailable", "RATE_LIMIT"))
            last = e
            if not transient or i == tries - 1:
                raise
            time.sleep(2 * (i + 1))  # 2s, 4s 백오프
    raise last


def _meta_collect_job(days=META_BACKFILL_DAYS):
    """메타 광고 성과 수집 → 각 매장 효율시트 메타 칸 기입. 브라우저 없이 API.
    meta_account_id 설정된 매장만. 최근 days 일 재수집(어트리뷰션 보정)."""
    if not os.environ.get("META_ACCESS_TOKEN"):
        print("[meta] META_ACCESS_TOKEN 미설정 - 스킵")
        return
    today = datetime.now()
    since = (today - timedelta(days=days)).strftime("%Y-%m-%d")
    until = today.strftime("%Y-%m-%d")
    accounts = [a for a in db.list_accounts() if (a.get("meta_account_id") or "").strip()]
    print(f"[meta] {since}~{until} {len(accounts)}계정 시작")
    ok_acct = 0
    fail = []
    for a in accounts:
        aid = a["id"]
        lbl = a.get("label") or aid
        ssid = a.get("spreadsheet_id") or ""
        if not ssid:
            continue
        try:
            insights = meta.fetch_insights(a["meta_account_id"], since, until)
            # DB 저장 (대시보드 ROAS 뷰용) — 시트 실패와 무관하게 저장
            for d, m in insights.items():
                try:
                    db.upsert_meta_metric(aid, d, m)
                except Exception:
                    traceback.print_exc()
            # 캠페인별 성과도 수집·저장 (실패해도 계정단위엔 영향 없음)
            try:
                for c in meta.fetch_campaign_insights(a["meta_account_id"], since, until):
                    db.upsert_meta_campaign(aid, c["date"], c["campaign_id"], c.get("campaign_name", ""), c)
            except Exception:
                traceback.print_exc()
            # 광고(소재)별 성과
            try:
                for ad in meta.fetch_ad_insights(a["meta_account_id"], since, until):
                    db.upsert_meta_ad(aid, ad["date"], ad["ad_id"], ad.get("ad_name", ""), ad.get("campaign_name", ""), ad)
            except Exception:
                traceback.print_exc()
            wrote, errs = _retry_sheet_write(meta.write_meta_days, ssid, insights)
            db.set_setting(f"meta_last_{aid}", f"{datetime.now().strftime('%Y-%m-%d %H:%M')} ({wrote}일)")
            print(f"[meta] {lbl} {wrote}일 기입" + (f" · 경고 {errs}" if errs else ""))
            db.add_sheet_log(aid, "meta", f"{since}~{until}", wrote,
                             "ok" if not errs else "warn", "; ".join(errs) if errs else "")
            ok_acct += 1
        except Exception as e:
            fail.append(lbl)
            print(f"[meta] {lbl} 실패: {repr(e)[:160]}")
            try:
                db.add_sheet_log(aid, "meta", f"{since}~{until}", 0, "fail", repr(e)[:280])
            except Exception:
                pass
        time.sleep(1.5)  # 구글 시트 쿼터(분당 읽기) 보호
    db.set_setting("meta_last_run", datetime.now().strftime("%Y-%m-%d %H:%M"))
    print(f"[meta] done — 성공 {ok_acct} / 실패 {len(fail)}")
    slack_notify(
        f"meta-collect 완료 ({until}) · 성공 {ok_acct} / 실패 {len(fail)}" + (f" [{', '.join(fail)}]" if fail else ""),
        severity="report" if not fail else "warn",
    )


def _naver_collect_job(days=META_BACKFILL_DAYS):
    """네이버 검색광고 성과 수집 → 효율시트 네이버칸(KH~KO) 기입 + DB 저장. API라 chromium 무관."""
    today = datetime.now()
    since = (today - timedelta(days=days)).strftime("%Y-%m-%d")
    until = today.strftime("%Y-%m-%d")
    accounts = [a for a in db.list_accounts()
                if (a.get("naver_api_key") or "").strip() and (a.get("naver_customer_id") or "").strip()]
    print(f"[naver] {since}~{until} {len(accounts)}계정 시작")
    ok_acct = 0
    fail = []
    for a in accounts:
        aid = a["id"]
        lbl = a.get("label") or aid
        ssid = a.get("spreadsheet_id") or ""
        creds = (a["naver_api_key"], a["naver_secret"], a["naver_customer_id"])
        try:
            daily = naver.fetch_daily(creds, since, until)
            for d, m in daily.items():
                try:
                    db.upsert_naver_metric(aid, d, m)
                except Exception:
                    traceback.print_exc()
            wrote, errs = (0, [])
            if ssid:
                wrote, errs = _retry_sheet_write(naver.write_to_sheet, ssid, daily)
            db.set_setting(f"naver_last_{aid}", f"{datetime.now().strftime('%Y-%m-%d %H:%M')} ({wrote}일)")
            db.add_sheet_log(aid, "naver", f"{since}~{until}", wrote, "ok" if not errs else "warn",
                             "; ".join(errs) if errs else "")
            print(f"[naver] {lbl} {wrote}일 기입" + (f" · 경고 {errs}" if errs else ""))
            ok_acct += 1
        except Exception as e:
            fail.append(lbl)
            print(f"[naver] {lbl} 실패: {repr(e)[:160]}")
            try:
                db.add_sheet_log(aid, "naver", f"{since}~{until}", 0, "fail", repr(e)[:280])
            except Exception:
                pass
        time.sleep(1.5)
    db.set_setting("naver_last_run", datetime.now().strftime("%Y-%m-%d %H:%M"))
    print(f"[naver] done — 성공 {ok_acct} / 실패 {len(fail)}")


CRITEO_BACKFILL_DAYS = 7  # 클릭후7일 어트리뷰션 보정 위해 최근 7일 재수집


def _criteo_collect_job(days=CRITEO_BACKFILL_DAYS):
    """크리테오 성과 수집(세션 크롤) → 효율시트 크리테오칸 기입 + DB.
    JWT 1회 확보 후 advertiser 루프. chromium 쓰므로 _live_lock 으로 cafe24 와 직렬화."""
    accounts = [a for a in db.list_accounts() if (a.get("criteo_advertiser_id") or "").strip()]
    if not accounts:
        print("[criteo] 대상 계정 없음")
        return
    if not _live_lock.acquire(timeout=600):  # cafe24 스크래핑 끝날 때까지 최대 10분 대기
        print("[criteo] _live_lock 획득 실패 — 다음 차수로 스킵")
        return
    try:
        advs = [a["criteo_advertiser_id"] for a in accounts]
        print(f"[criteo] 최근 {days}일 {len(accounts)}계정 시작")
        try:
            data = criteo.fetch_all_crawl(advs, days=days)
        except Exception as e:
            print(f"[criteo] 수집 실패: {repr(e)[:160]}")
            slack_notify(f"크리테오 수집 실패: {repr(e)[:120]} → 세션 재로그인 확인", severity="warn")
            return
        ok_acct, fail = 0, []
        for a in accounts:
            aid = a["id"]
            lbl = a.get("label") or aid
            ssid = a.get("spreadsheet_id") or ""
            daily = data.get(str(a["criteo_advertiser_id"]))
            if not daily:
                continue
            try:
                for d, m in daily.items():
                    try:
                        db.upsert_criteo_metric(aid, d, m)
                    except Exception:
                        traceback.print_exc()
                wrote, errs = (0, [])
                if ssid:
                    wrote, errs = _retry_sheet_write(criteo.write_to_sheet, ssid, daily)
                db.set_setting(f"criteo_last_{aid}", f"{datetime.now().strftime('%Y-%m-%d %H:%M')} ({wrote}일)")
                db.add_sheet_log(aid, "criteo", f"최근{days}일", wrote,
                                 "ok" if not errs else "warn", "; ".join(errs) if errs else "")
                print(f"[criteo] {lbl} {wrote}일 기입" + (f" · 경고 {errs}" if errs else ""))
                ok_acct += 1
            except Exception as e:
                fail.append(lbl)
                db.add_sheet_log(aid, "criteo", f"최근{days}일", 0, "fail", repr(e)[:280])
                print(f"[criteo] {lbl} 실패: {repr(e)[:160]}")
        db.set_setting("criteo_last_run", datetime.now().strftime("%Y-%m-%d %H:%M"))
        print(f"[criteo] done — 성공 {ok_acct} / 실패 {len(fail)}")
    finally:
        _live_lock.release()


GFA_BACKFILL_DAYS = 7  # 최근 7일 재수집 (전환/매출 후속 보정 대비)


def _gfa_collect_job(days=GFA_BACKFILL_DAYS):
    """네이버 성과형(GFA) 성과 수집(세션 크롤) → 효율시트 네이버성과형칸 기입 + DB.
    개별 광고계정 stats API 호출. chromium 쓰므로 _live_lock 으로 cafe24 와 직렬화.
    권한 미승인 계정은 빈 응답 → 자동 스킵."""
    accounts = [a for a in db.list_accounts() if (a.get("naver_gfa_account_no") or "").strip()]
    if not accounts:
        print("[gfa] 대상 계정 없음")
        return
    if not _live_lock.acquire(timeout=600):  # cafe24 스크래핑 끝날 때까지 최대 10분 대기
        print("[gfa] _live_lock 획득 실패 — 다음 차수로 스킵")
        return
    try:
        nos = [a["naver_gfa_account_no"] for a in accounts]
        print(f"[gfa] 최근 {days}일 {len(accounts)}계정 시작")
        try:
            data = gfa.fetch_all(nos, days=days)
        except Exception as e:
            print(f"[gfa] 수집 실패: {repr(e)[:160]}")
            slack_notify(f"네이버 성과형 수집 실패: {repr(e)[:120]} → 세션 재로그인 확인", severity="warn")
            return
        ok_acct, skipped, fail = 0, [], []
        for a in accounts:
            aid = a["id"]
            lbl = a.get("label") or aid
            ssid = a.get("spreadsheet_id") or ""
            daily = data.get(str(a["naver_gfa_account_no"]))
            if not daily:
                skipped.append(lbl)  # 권한 미승인 or 데이터 없음
                continue
            try:
                for d, m in daily.items():
                    try:
                        db.upsert_gfa_metric(aid, d, m)
                    except Exception:
                        traceback.print_exc()
                wrote, errs = (0, [])
                if ssid:
                    wrote, errs = _retry_sheet_write(gfa.write_to_sheet, ssid, daily)
                db.set_setting(f"gfa_last_{aid}", f"{datetime.now().strftime('%Y-%m-%d %H:%M')} ({wrote}일)")
                db.add_sheet_log(aid, "gfa", f"최근{days}일", wrote,
                                 "ok" if not errs else "warn", "; ".join(errs) if errs else "")
                print(f"[gfa] {lbl} {wrote}일 기입" + (f" · 경고 {errs}" if errs else ""))
                ok_acct += 1
            except Exception as e:
                fail.append(lbl)
                db.add_sheet_log(aid, "gfa", f"최근{days}일", 0, "fail", repr(e)[:280])
                print(f"[gfa] {lbl} 실패: {repr(e)[:160]}")
        db.set_setting("gfa_last_run", datetime.now().strftime("%Y-%m-%d %H:%M"))
        print(f"[gfa] done — 성공 {ok_acct} / 스킵 {len(skipped)} / 실패 {len(fail)}")
    finally:
        _live_lock.release()


SHOPBOX_BACKFILL_DAYS = 14  # 주간/월간 정액이라 넓게 — 입찰원장 일별분할 재계산


def _shopbox_collect_job(days=SHOPBOX_BACKFILL_DAYS):
    """쇼핑박스: 광고비(입찰 일별분할) + 노출/클릭(쇼핑파트너센터 groupBy.nhn 크롤) → shopbox_metrics + 시트.
    크롬 쓰므로(노출/클릭 수집) _live_lock 으로 cafe24 와 직렬화. 매출은 v2(cafe24 유입분석).
    대상: 입찰원장 있거나 쇼핑박스 자격 있는 매장."""
    creds = shopbox._load_accounts()
    accounts = [a for a in db.list_accounts()
                if db.list_shopbox_bids([a["id"]]) or a["id"] in creds]
    if not accounts:
        print("[shopbox] 대상 매장 없음")
        return
    if not _live_lock.acquire(timeout=600):
        print("[shopbox] _live_lock 획득 실패 — 스킵")
        return
    today = datetime.now().date()
    dates = [(today - timedelta(days=i)).isoformat() for i in range(0, days + 1)]
    ok_acct = 0
    try:
      for a in accounts:
        aid = a["id"]; lbl = a.get("label") or aid; ssid = a.get("spreadsheet_id") or ""
        # 노출/클릭 크롤 (자격 있는 매장만, 실패해도 광고비는 진행)
        metrics_by_date = {}
        if aid in creds:
            try:
                metrics_by_date = shopbox.fetch_metrics(aid, days=days)
            except Exception as e:
                print(f"[shopbox] {lbl} 노출/클릭 수집 실패(광고비는 진행): {repr(e)[:120]}")
        # 매출 크롤 (cafe24 애널리틱스 UTM, 모든 대상 매장 — 실패해도 나머지 진행)
        rev_by_date = {}
        try:
            rev_by_date = shopbox.fetch_revenue(aid, days=days)
        except Exception as e:
            print(f"[shopbox] {lbl} 매출 수집 실패(나머지는 진행): {repr(e)[:120]}")
        # 이 매장이 입찰원장을 가진 device — 입찰을 지웠다 다시 넣어도 시트의 옛 광고비가
        # 0으로 덮여 정정되도록(스테일 방지). 한 번도 입찰 없는 매장은 건드리지 않음.
        bid_devs = {b["device"] for b in db.list_shopbox_bids([aid])}
        daily = {}
        for d in dates:
            devmap = {}
            for dev in ("pc", "mo"):
                cost = db.shopbox_daily_cost(aid, dev, d)
                mm = (metrics_by_date.get(d, {}) or {}).get(dev, {})
                rev = (rev_by_date.get(d, {}) or {}).get(dev, 0)
                rec = {}
                if cost > 0 or dev in bid_devs:
                    rec["cost"] = cost
                if mm.get("impressions"):
                    rec["impressions"] = mm["impressions"]
                if mm.get("clicks"):
                    rec["clicks"] = mm["clicks"]
                if rev:
                    rec["revenue"] = rev
                if rec:
                    db.upsert_shopbox_metric(aid, d, dev, rec)
                    devmap[dev] = rec
            if devmap:
                daily[d] = devmap
        if not daily:
            continue
        try:
            wrote, errs = (0, [])
            if ssid:
                wrote, errs = _retry_sheet_write(shopbox.write_to_sheet, ssid, daily)
            db.set_setting(f"shopbox_last_{aid}", f"{datetime.now().strftime('%Y-%m-%d %H:%M')} ({wrote}일)")
            db.add_sheet_log(aid, "shopbox", f"최근{days}일", wrote,
                             "ok" if not errs else "warn", "; ".join(errs) if errs else "")
            print(f"[shopbox] {lbl} {wrote}일 기입" + (f" · 경고 {errs}" if errs else ""))
            ok_acct += 1
        except Exception as e:
            db.add_sheet_log(aid, "shopbox", f"최근{days}일", 0, "fail", repr(e)[:280])
            print(f"[shopbox] {lbl} 실패: {repr(e)[:160]}")
      db.set_setting("shopbox_last_run", datetime.now().strftime("%Y-%m-%d %H:%M"))
      print(f"[shopbox] done — {ok_acct}계정")
    finally:
        _live_lock.release()


def _shopbox_bids_by_acct():
    """account_id → 입찰 원장 리스트 (UI 표시용)."""
    out = {}
    for b in db.list_shopbox_bids():
        out.setdefault(b["account_id"], []).append(b)
    return out


HEALTHCHECK_PING_URL = os.environ.get("HEALTHCHECK_PING_URL", "")  # 외부 데드맨 스위치 (healthchecks.io 등)


def _deadman_ping_job():
    """외부 데드맨 스위치에 '나 살아있음' 핑. 이 잡이 도는 것 자체가 프로세스+스케줄러 생존 증거.
    프로세스가 죽거나 스케줄러가 hang 하면 핑이 끊겨 → 외부 서비스가 사용자에게 알림.
    (앱 내부 Slack 은 앱이 죽으면 못 보내므로, 앱의 '죽음'은 외부에서만 감지 가능.)"""
    if not HEALTHCHECK_PING_URL:
        return
    try:
        import urllib.request
        urllib.request.urlopen(HEALTHCHECK_PING_URL, timeout=10).read()
    except Exception as e:
        print(f"[deadman] 핑 실패: {repr(e)[:80]}", flush=True)


def _gfa_session_check_job():
    """네이버 성과형 크롤 세션 만료 임박/만료 시 Slack 경고."""
    try:
        st = gfa.session_status()
        print(f"[gfa] session check: {st['message']}", flush=True)
        if st["severity"] in ("warn", "critical"):
            slack_notify(st["message"] + " → 네이버성과형_세션갱신.command 더블클릭",
                         severity=st["severity"])
    except Exception:
        traceback.print_exc()


# ===== 세션 갱신 도우미 (웹 '갱신 요청' 버튼 ↔ 맥 백그라운드 도우미) =====
GUARD_TOKEN = os.environ.get("GUARD_TOKEN", "")  # 맥 도우미 인증용 (.env)

# 채널키 → (표시명, 로그인스크립트, .command, status함수)
_GUARD_CHANNELS = [
    ("criteo", "크리테오", "criteo_login.py", "크리테오_세션갱신.command", "CRITEO_UPLOAD"),
    ("gfa", "네이버 성과형(GFA)", "naver_gfa_login.py", "네이버성과형_세션갱신.command", "GFA_UPLOAD"),
]


def _session_guard_payload():
    """각 크롤 채널 세션 상태 + '갱신 요청' 플래그. 웹 패널·맥 도우미 공용.
    세션이 요청일 이후 갱신됐으면 요청 플래그 자동 해제."""
    out = []
    for key, name, login_py, cmd, upload_env in _GUARD_CHANNELS:
        st = (criteo.session_status() if key == "criteo" else gfa.session_status())
        requested = db.get_setting(f"{key}_refresh_requested", "") or ""
        # 요청한 날짜보다 '이후'에 세션이 갱신됐으면 = 요청이 처리된 것 → 플래그 해제.
        # (같은 날 재로그인은 날짜로 구분 불가 → 도우미 ack 로 해제됨)
        if requested and st.get("refreshed_at") and st["refreshed_at"] > requested[:10]:
            db.set_setting(f"{key}_refresh_requested", "")
            requested = ""
        # 만료 예정일 = 오늘 + 남은일수 (미리 갱신할 수 있게 명시)
        import datetime as _dt
        dl = st.get("days_left")
        expires_at = (_dt.date.today() + _dt.timedelta(days=dl)).isoformat() if dl is not None else None
        out.append({
            "key": key, "name": name, "command": cmd, "login_py": login_py, "upload_env": upload_env,
            "severity": st.get("severity"), "days_left": dl, "expires_at": expires_at,
            "refreshed_at": st.get("refreshed_at"), "message": st.get("message"),
            "requested": bool(requested), "requested_at": requested,
        })
    return out




def _disk_free_mb(path="/opt/cafe24"):
    """지정 경로 마운트의 사용 가능 공간 MB. 실패 시 None."""
    try:
        st = os.statvfs(path)
        return (st.f_bavail * st.f_frsize) // (1024 * 1024)
    except Exception:
        return None


# heartbeat 중복 알림 방지 — 같은 사유는 6시간에 한 번만.
_heartbeat_last_alert = {}
HEARTBEAT_ALERT_COOLDOWN_SEC = 6 * 3600


def _heartbeat_alert(key, text, severity="warn"):
    now = time.time()
    last = _heartbeat_last_alert.get(key, 0)
    if now - last < HEARTBEAT_ALERT_COOLDOWN_SEC:
        print(f"[heartbeat] {key} 알림 쿨다운 중 ({int((now-last)/60)}분 경과) — skip")
        return
    _heartbeat_last_alert[key] = now
    slack_notify(text, severity=severity)


# 데이터 검증 경고 — 최근 것 메모리 보관 (대시보드 표시용, 슬랙 안 보냄)
_validate_warnings = {}  # account_id → {date, warns, ts}


def _validate_alert(account_id, label, date, warns):
    _validate_warnings[account_id] = {
        "label": label, "date": date, "warns": warns,
        "ts": datetime.now().strftime("%m-%d %H:%M"),
    }


def _label_map():
    return {a["id"]: (a.get("label") or a["id"]) for a in db.list_accounts()}


_SA_EMAIL_CACHE = {"email": None, "loaded": False}


def _service_account_email():
    """service_account.json 의 client_email (시트 공유 안내용). 1회 읽고 캐시."""
    if not _SA_EMAIL_CACHE["loaded"]:
        _SA_EMAIL_CACHE["loaded"] = True
        try:
            with open(sheets.SERVICE_ACCOUNT_FILE) as f:
                _SA_EMAIL_CACHE["email"] = json.load(f).get("client_email")
        except Exception:
            _SA_EMAIL_CACHE["email"] = None
    return _SA_EMAIL_CACHE["email"]


def _sheet_fail_hints(fail_details):
    """시트 입력 실패 detail 들에서 상황별 조치 힌트 도출 (중복 제거, 순서 유지)."""
    sa = _service_account_email() or "서비스 계정"
    hints = []
    joined = " ".join(fail_details)
    if "404" in joined or "SpreadsheetNotFound" in joined:
        hints.append(f"🔑 시트가 안 열림(404) — 시트 ID가 틀렸거나 공유 안 됨. "
                     f"해당 구글시트를 '{sa}' 에 <b>편집자</b>로 공유하고, 설정에서 시트 URL을 다시 저장하세요.")
    if "탭없음" in joined or "자동생성 실패" in joined:
        hints.append("📑 효율 탭이 없고 자동생성도 실패 — 그 시트에 '효율_26년N월' 형태 탭이 하나라도 있어야 복제 가능. "
                     "최소 1개 효율 탭을 만들어 두세요.")
    if "행없음" in joined:
        hints.append("📅 해당 날짜 행을 못 찾음 — 효율 탭 B열 날짜(YYYY/MM/DD)가 비었거나 형식이 다른지 확인.")
    if "검증불일치" in joined:
        hints.append("⚠️ 기입 후 읽은 값이 API값과 다름 — 시트에 수식/서식이 걸려 값이 바뀌는지 확인.")
    return hints


def _build_freshness(selected_ids, today):
    """대시보드 데이터 신선도 — 각 데이터 종류별 기준일/갱신시각 + 누락/문제 상세.
    반환 항목: {name, basis, upd, status(ok|warn|bad), detail(누락 등 설명), missing:[label]}"""
    out = []
    lbl = _label_map()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        with db.db_conn() as conn:
            ph = ",".join("?" * len(selected_ids))

            # 1) cafe24 매출/방문 (오늘) — 계정별 오늘 데이터 있나 + 갱신 지연
            rows = conn.execute(
                f"SELECT account_id, MAX(updated_at) FROM metrics WHERE date=? AND account_id IN ({ph}) GROUP BY account_id",
                [today] + selected_ids).fetchall()
            have = {r[0]: r[1] for r in rows}
            miss = [lbl.get(a, a) for a in selected_ids if a not in have]
            last_upd = max([v for v in have.values() if v], default=None)
            # 매장별 지연 판정 — MAX(최신1개)가 아니라 매장마다 봐서, 사이클이 못 따라온 매장을 잡음.
            # (한 사이클 ~2시간이라 160분 넘으면 진짜 뒤처진 것)
            now_dt = datetime.now()
            stale_list = []
            for a in selected_ids:
                u = have.get(a)
                if not u:
                    continue
                try:
                    mins = (now_dt - datetime.strptime(u, "%Y-%m-%d %H:%M:%S")).total_seconds() / 60
                    if mins > 160:
                        stale_list.append(f"{lbl.get(a, a)}({int(mins)}분)")
                except Exception:
                    pass
            st = "bad" if miss else ("warn" if stale_list else "ok")
            if miss:
                det = f"오늘 데이터 없음: {', '.join(miss)}"
            elif stale_list:
                det = f"{len(stale_list)}개 매장 갱신 160분+ 지연 (라이브가 못 따라옴): {', '.join(stale_list)}"
            else:
                det = "전 매장 최근 갱신"
            out.append({"name": "cafe24 매출/방문", "basis": "오늘 실시간",
                        "upd": last_upd[5:16] if last_upd else "—", "status": st,
                        "detail": det, "missing": miss, "lines": stale_list})

            # 2) 메타 광고 (어제 확정 기준) — 연결매장 중 어제 데이터 없는 곳
            connected = [a["id"] for a in db.list_accounts()
                         if a["id"] in selected_ids and (a.get("meta_account_id") or "").strip()]
            if connected:
                cph = ",".join("?" * len(connected))
                r = conn.execute(
                    f"SELECT MAX(date), MAX(updated_at) FROM meta_metrics WHERE account_id IN ({cph})",
                    connected).fetchone()
                yhave = {x[0] for x in conn.execute(
                    f"SELECT DISTINCT account_id FROM meta_metrics WHERE date=? AND account_id IN ({cph})",
                    [yesterday] + connected).fetchall()}
                mmiss = [lbl.get(a, a) for a in connected if a not in yhave]
                st = "bad" if (r and r[0] and r[0] < yesterday) else ("warn" if mmiss else "ok")
                det = (f"어제 미수집: {', '.join(mmiss)}" if mmiss else
                       (f"최신 {r[0]} — 어제({yesterday})보다 뒤처짐" if (r and r[0] and r[0] < yesterday) else f"연결 {len(connected)}매장 정상"))
                out.append({"name": "메타 광고", "basis": f"~{r[0]}" if r and r[0] else "없음",
                            "upd": r[1][5:16] if r and r[1] else "—", "status": st,
                            "detail": det, "missing": mmiss})
            else:
                out.append({"name": "메타 광고", "basis": "미연결", "upd": "—",
                            "status": "warn", "detail": "선택 매장 중 메타 광고계정 연결된 곳 없음", "missing": []})

            # 3) 상품 분석 (오늘 daily 기준)
            r = conn.execute(
                f"SELECT MAX(date), MAX(updated_at) FROM product_metrics WHERE account_id IN ({ph})",
                selected_ids).fetchone()
            thave = {x[0] for x in conn.execute(
                f"SELECT DISTINCT account_id FROM product_metrics WHERE date=? AND period='daily' AND account_id IN ({ph})",
                [today] + selected_ids).fetchall()}
            pmiss = [lbl.get(a, a) for a in selected_ids if a not in thave]
            st = "warn" if pmiss else "ok"
            det = (f"오늘 미수집: {', '.join(pmiss)}" if pmiss else "전 매장 정상")
            out.append({"name": "상품 분석", "basis": f"~{r[0]}" if r and r[0] else "없음",
                        "upd": r[1][5:16] if r and r[1] else "—", "status": st,
                        "detail": det, "missing": pmiss})

            # 4) 시트 입력 — (account,channel) 별 '가장 최근' 상태만 봐서 현재 상태 반영.
            #    (이미 해결된 과거 실패가 24h 동안 계속 빨갛게 남는 문제 방지)
            latest = conn.execute(
                "SELECT account_id, channel, status, detail FROM sheet_fill_log s "
                "WHERE id = (SELECT MAX(id) FROM sheet_fill_log s2 "
                "           WHERE s2.account_id=s.account_id AND s2.channel=s.channel) "
                "AND ts >= datetime('now','localtime','-2 day')"
            ).fetchall()
            fails = [f for f in latest if f[2] in ("fail", "warn")]
            if fails:
                lines = [f"{lbl.get(f[0], f[0])}({f[1]}): {f[3][:45]}" for f in fails]
                hints = _sheet_fail_hints([f[3] or "" for f in fails])
                out.append({"name": "시트 입력", "basis": "현재 상태", "upd": "—",
                            "status": "bad", "detail": "; ".join(lines), "lines": lines,
                            "hints": hints, "missing": [lbl.get(f[0], f[0]) for f in fails]})
            else:
                out.append({"name": "시트 입력", "basis": "현재 상태", "upd": "—",
                            "status": "ok", "detail": "최근 입력 전부 정상", "lines": [], "missing": []})
    except Exception:
        traceback.print_exc()
    return out


def _heartbeat_job():
    """매시 정각 self-check. 다음 이상치를 잡아서 slack 알림:
    - 디스크 free < 1GB (warn), < 300MB (critical) — 백업 자동 정리도 시도
    - 메모리 free < 150MB 가 지속 (warn) — 좀비 chromium 자동 정리
    - daily_finalize 가 어제 데이터 0건 (warn)
    - 활성 시간대 + live 잡 등록됐는데 오늘 metrics 0건 (warn)
    - 06:00 product_collect 가 오늘 0건 (warn) — 단 06:00 이후만 검사
    - scheduler 잡 누락 (critical)

    각 사유 6시간 쿨다운으로 중복 알림 방지.
    """
    try:
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        problems = []

        # 1) scheduler 잡 누락
        required_jobs = {"daily_finalize", "db_backup", "daily_restart"}
        live = db.get_live_settings()
        if live["interval_min"] > 0:
            required_jobs.add("live_global")
        registered = {j.id for j in scheduler.get_jobs()}
        missing = required_jobs - registered
        if missing:
            _heartbeat_alert(
                f"missing_jobs",
                f"🆘 스케줄러 잡 누락: {', '.join(sorted(missing))}\n현재 등록: {', '.join(sorted(registered))}",
                severity="critical",
            )
            problems.append(f"missing_jobs={missing}")

        # 2) 디스크
        free_disk = _disk_free_mb()
        if free_disk is not None:
            if free_disk < 300:
                # 즉시 정리 시도
                try:
                    backup_dir = Path("data/backups")
                    if backup_dir.exists():
                        old = sorted(backup_dir.glob("cafe24_*.db"))[:-3]  # 최근 3개 빼고 다 삭제
                        for f in old:
                            f.unlink()
                        print(f"[heartbeat] 디스크 부족 → 백업 {len(old)}개 emergency 정리")
                except Exception:
                    traceback.print_exc()
                _heartbeat_alert(
                    "disk_critical",
                    f"🆘 디스크 위험: free={free_disk}MB (<300MB). 백업 emergency 정리 시도. 수동 점검 필요.",
                    severity="critical",
                )
                problems.append(f"disk={free_disk}MB")
            elif free_disk < 1024:
                _heartbeat_alert(
                    "disk_warn",
                    f"⚠️ 디스크 여유 적음: free={free_disk}MB (<1GB).",
                    severity="warn",
                )
                problems.append(f"disk={free_disk}MB")

        # 3) 메모리 (좀비 chromium 누적 케이스)
        free_mem = _free_memory_mb()
        if free_mem is not None and free_mem < 150 and not _running:
            # 실행 중 잡 없는데도 메모리 부족 → 좀비
            _kill_leftover_chromium()
            new_free = _free_memory_mb()
            print(f"[heartbeat] 좀비 chromium 정리: {free_mem}MB → {new_free}MB")
            if new_free is not None and new_free < 150:
                _heartbeat_alert(
                    "mem_low",
                    f"⚠️ 메모리 부족 지속: free={new_free}MB. chromium 정리 후에도 회복 안 됨.",
                    severity="warn",
                )
                problems.append(f"mem={new_free}MB")

        # 4) 어제 daily_finalize 결과 (08시 이후만 — 03시 잡이 끝날 시간 확보)
        if now.hour >= 8:
            try:
                with db.db_conn() as conn:
                    n_y = conn.execute(
                        "SELECT COUNT(*) FROM metrics WHERE date=? AND 매출>0", (yesterday,)
                    ).fetchone()[0]
                total_acc = len(db.list_accounts())
                if n_y == 0:
                    _heartbeat_alert(
                        f"daily_finalize_empty_{yesterday}",
                        f"⚠️ daily_finalize ({yesterday}) 결과 0건. 03:00 잡이 실패했을 가능성.",
                        severity="warn",
                    )
                    problems.append(f"daily_finalize_empty")
                elif n_y < total_acc * 0.5:
                    _heartbeat_alert(
                        f"daily_finalize_partial_{yesterday}",
                        f"⚠️ daily_finalize ({yesterday}) {n_y}/{total_acc} 계정만 매출>0. 절반 이상 누락.",
                        severity="warn",
                    )
                    problems.append(f"daily_finalize_partial={n_y}/{total_acc}")
            except Exception:
                traceback.print_exc()

        # 5) 오늘 상품(daily) 수집 — 라이브 세션에 piggyback 되므로 라이브 몇 사이클 돈 11시 이후 검사
        if now.hour >= 11:
            try:
                with db.db_conn() as conn:
                    n_p = conn.execute(
                        "SELECT COUNT(DISTINCT account_id) FROM product_metrics WHERE date=? AND period='daily'", (today,)
                    ).fetchone()[0]
                total_acc = len(db.list_accounts())
                if n_p == 0:
                    _heartbeat_alert(
                        f"product_today_empty_{today}",
                        f"⚠️ 오늘({today}) 상품 수집 0계정. 라이브 세션의 상품 수집이 안 되고 있을 가능성.",
                        severity="warn",
                    )
                    problems.append(f"product_today_empty")
                elif n_p < total_acc * 0.5:
                    _heartbeat_alert(
                        f"product_today_partial_{today}",
                        f"⚠️ 오늘({today}) 상품 {n_p}/{total_acc} 계정만 수집됨.",
                        severity="warn",
                    )
                    problems.append(f"product_today_partial={n_p}/{total_acc}")
            except Exception:
                traceback.print_exc()

        # 5b) 광고 채널 수집 끊김 감지 (10시 이후 — meta 07:00/naver 07:10/criteo 06:40/gfa 06:50 후).
        # 토큰·세션이 조용히 만료/차단되는 케이스를 잡는다.
        # 최근 7일 수집되던 채널인데 오늘 갱신(updated_at)된 매장이 0 → 수집 파이프 끊김.
        if now.hour >= 10:
            week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
            for tbl, label, remedy in [
                ("meta_metrics", "메타 광고", "Meta 광고관리자 액세스토큰 만료 의심 → 토큰 갱신 필요"),
                ("naver_metrics", "네이버 검색광고", "네이버 검색광고 API키/시크릿/CUSTOMER_ID 확인"),
                ("criteo_metrics", "크리테오", "세션 만료/차단 → 크리테오_세션갱신.command 더블클릭 재로그인"),
                ("gfa_metrics", "네이버 성과형(GFA)", "세션 만료/차단 → 네이버성과형_세션갱신.command 더블클릭 재로그인")]:
                try:
                    with db.db_conn() as conn:
                        n_recent = conn.execute(
                            f"SELECT COUNT(DISTINCT account_id) FROM {tbl} WHERE date >= ?", (week_ago,)).fetchone()[0]
                        n_today = conn.execute(
                            f"SELECT COUNT(DISTINCT account_id) FROM {tbl} WHERE date(updated_at)=?", (today,)).fetchone()[0]
                    if n_recent > 0 and n_today == 0:
                        _heartbeat_alert(
                            f"{tbl}_collect_gap_{today}",
                            f"⚠️ {label} 수집 끊김: 최근 {n_recent}개 매장 수집되다 오늘 0개 갱신. {remedy}",
                            severity="warn",
                        )
                        problems.append(f"{tbl}_collect_gap")
                except Exception:
                    traceback.print_exc()

        # 5c) 쇼핑박스 — 입찰 진행 중인데 노출/클릭이 0 (입찰원장 있는 매장·디바이스만 → 오경보 없음).
        # 어제(확정일) 기준: 입찰기간이 어제를 포함하는데 어제 노출=0&클릭=0 이면 세션/소재/슬롯 이상.
        if now.hour >= 10:
            try:
                yday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
                active = {(b["account_id"], b["device"]) for b in db.list_shopbox_bids()
                          if b["start_date"] <= yday <= b["end_date"]}
                if active:
                    ymetrics = {(r["account_id"], r["device"]): r
                                for r in db.list_shopbox_metrics(start_date=yday, end_date=yday)}
                    lbl = _label_map()
                    dead = []
                    for aid, dev in active:
                        m = ymetrics.get((aid, dev))
                        if not m or ((m.get("impressions") or 0) == 0 and (m.get("clicks") or 0) == 0):
                            dead.append(f"{lbl.get(aid, aid)} {dev.upper()}")
                    if dead:
                        _heartbeat_alert(
                            f"shopbox_dead_{yday}",
                            f"⚠️ 쇼핑박스 입찰 진행 중인데 어제({yday[5:]}) 노출/클릭 0: {', '.join(sorted(dead))} "
                            f"— 쇼핑파트너센터 세션 만료 또는 소재/슬롯 비활성 의심",
                            severity="warn",
                        )
                        problems.append("shopbox_dead")
            except Exception:
                traceback.print_exc()

        # 6) per-account "오늘 막힘" 감지 (14시 이후).
        # 어제 매출>0 이던 활성 매장인데 오늘 데이터가 없으면(행없음 or 매출0+시간별0) → 그 계정만 콕 집어 알림.
        # rosy001 처럼 특정 계정이 하루종일 hang 하는 케이스를 사람이 바로 인지하도록.
        if now.hour >= 14:
            try:
                stuck = []
                for a in db.list_accounts():
                    aid = a["id"]
                    y = db.get_metric(aid, yesterday)
                    if not y or (y.get("매출") or 0) <= 0:
                        continue  # 어제도 무매출(휴면) 매장은 제외
                    t = db.get_metric(aid, today)
                    try:
                        h = db.count_metrics_hourly(aid, today)
                    except Exception:
                        h = 0
                    if (not t) or ((t.get("매출") or 0) == 0 and h == 0):
                        # 마지막 run 에러 요약
                        last_err = ""
                        try:
                            with db.db_conn() as conn:
                                r = conn.execute(
                                    "SELECT error FROM runs WHERE account_id=? AND status='error' ORDER BY started_at DESC LIMIT 1",
                                    (aid,)).fetchone()
                                if r:
                                    last_err = _short_error(r["error"])
                        except Exception:
                            pass
                        stuck.append((a.get("label") or aid, aid, last_err))
                if stuck:
                    lines = "\n".join(f"• {lbl}(`{aid}`) — {err or '원인불명'}" for lbl, aid, err in stuck)
                    _heartbeat_alert(
                        f"accounts_stuck_{today}",
                        f"🔴 오늘({today}) 데이터 안 들어온 활성 매장 {len(stuck)}곳:\n{lines}\n"
                        f"→ cafe24 쪽 느림/오류 가능. 백필: `/admin/backfill_dates` (account_id, dates={today}, skip_sheet=true)",
                        severity="warn",
                    )
                    problems.append(f"accounts_stuck={[s[1] for s in stuck]}")
            except Exception:
                traceback.print_exc()

        if problems:
            print(f"[heartbeat] {now.strftime('%H:%M')} 이상 감지: {problems}")
        else:
            print(f"[heartbeat] {now.strftime('%H:%M')} OK free_mem={free_mem}MB free_disk={free_disk}MB jobs={len(registered)}")
    except Exception:
        traceback.print_exc()


def _s3_upload_backup(path):
    """백업 파일을 S3 에 업로드(오프사이트). 환경변수 BACKUP_S3_BUCKET 없으면 스킵.
    전용 키(BACKUP_AWS_*)가 있으면 그걸, 없으면 기본 자격증명 체인 사용."""
    bucket = os.environ.get("BACKUP_S3_BUCKET")
    if not bucket:
        return
    try:
        import boto3
        region = os.environ.get("BACKUP_S3_REGION", "ap-northeast-2")
        ak = os.environ.get("BACKUP_AWS_ACCESS_KEY_ID")
        sk = os.environ.get("BACKUP_AWS_SECRET_ACCESS_KEY")
        if ak and sk:
            s3 = boto3.client("s3", region_name=region,
                              aws_access_key_id=ak, aws_secret_access_key=sk)
        else:
            s3 = boto3.client("s3", region_name=region)
        key = f"db-backups/{Path(path).name}"
        s3.upload_file(str(path), bucket, key)
        print(f"[db-backup] S3 업로드 완료 s3://{bucket}/{key}")
    except Exception:
        # 업로드 실패해도 로컬 백업은 이미 있음 — 잡 전체를 죽이지 않음
        print("[db-backup] S3 업로드 실패 (로컬 백업은 정상):")
        traceback.print_exc()


def _db_backup_job():
    """매일 04:00 sqlite DB backup. 7일 이상 된 백업 자동 정리.
    sqlite backup API 를 써서 트랜잭션 중에도 일관성 보장."""
    import sqlite3 as _sq3, shutil
    db_path = Path("data/cafe24.db")
    if not db_path.exists():
        print(f"[db-backup] DB 파일 없음: {db_path}")
        return
    backup_dir = Path("data/backups")
    backup_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    dst = backup_dir / f"cafe24_{ts}.db"
    try:
        src_conn = _sq3.connect(str(db_path))
        dst_conn = _sq3.connect(str(dst))
        with dst_conn:
            src_conn.backup(dst_conn)
        src_conn.close()
        dst_conn.close()
        size_kb = dst.stat().st_size // 1024
        print(f"[db-backup] {dst.name} ({size_kb}KB) 저장 완료")
    except Exception:
        traceback.print_exc()
        return
    # 오프사이트(S3) 업로드 — 인스턴스 소실 대비. 키 없으면 조용히 스킵(로컬 백업은 유지).
    _s3_upload_backup(dst)
    # 7일 이상 백업 정리
    cutoff = datetime.now() - timedelta(days=7)
    removed = 0
    for f in backup_dir.glob("cafe24_*.db"):
        try:
            file_ts = datetime.strptime(f.stem[len("cafe24_"):], "%Y%m%d_%H%M")
            if file_ts < cutoff:
                f.unlink()
                removed += 1
        except Exception:
            pass
    if removed:
        print(f"[db-backup] 오래된 백업 {removed}개 정리")


def reload_schedules():
    """DB 설정을 APScheduler 에 반영. 글로벌 잡 2개:
    - live_global: 오늘 데이터 누적 갱신 (DB only)
    - daily_finalize: 매일 새벽 어제 데이터 풀스크랩 + 시트 write
    계정별 cron은 더 이상 사용 안 함 (단순 시트 write 시각이 글로벌이면 충분)."""
    for job in scheduler.get_jobs():
        if job.id.startswith("scrape_") or job.id in ("live_global", "daily_finalize", "db_backup", "daily_restart", "product_collect", "product_today", "heartbeat", "meta_collect", "naver_collect"):
            scheduler.remove_job(job.id)

    live = db.get_live_settings()
    if live["interval_min"] > 0:
        interval = live["interval_min"]
        # 60 의 약수면 cron 으로 매시 N분 고정 → service restart 영향 없음.
        # 아니면 fallback 으로 interval (restart 직후 +interval 분 후 다음 실행).
        end_hour_cron = live["end_hour"] - 1 if live["end_hour"] < 24 else 23
        hour_range = f"{live['start_hour']}-{end_hour_cron}"
        if 60 % interval == 0:
            minutes_expr = ",".join(str(m) for m in range(0, 60, interval))
            scheduler.add_job(
                _live_global_job, "cron",
                hour=hour_range, minute=minutes_expr,
                id="live_global", replace_existing=True,
            )
            print(f"[scheduler] live_global: cron hour={hour_range} minute={minutes_expr} (매시 {interval}분, restart 무관)")
        else:
            scheduler.add_job(
                _live_global_job, "interval",
                minutes=interval,
                id="live_global", replace_existing=True,
            )
            print(f"[scheduler] live_global: interval {interval}분 (60의 약수 아님 → restart 시 타이밍 밀림)")

        # 재시작 공백 자동 복구 — 부팅 40초 후 1회 catch-up
        scheduler.add_job(
            _boot_catchup_job, "date",
            run_date=datetime.now() + timedelta(seconds=40),
            id="boot_catchup", replace_existing=True,
        )
        print("[scheduler] boot_catchup: 부팅 +40초 (라이브 공백 시 즉시 1회)")

    df = db.get_daily_finalize_settings()
    scheduler.add_job(
        _daily_finalize_job, "cron",
        hour=df["hour"], minute=df["minute"],
        id="daily_finalize", replace_existing=True,
    )
    print(f"[scheduler] daily_finalize: 매일 {df['hour']:02d}:{df['minute']:02d}")

    # DB 백업 - 매일 04:00 (daily_finalize 03:00 끝나고 1시간 뒤)
    scheduler.add_job(
        _db_backup_job, "cron",
        hour=4, minute=0,
        id="db_backup", replace_existing=True,
    )
    print(f"[scheduler] db_backup: 매일 04:00 (7일 보관)")

    # 최근일 누락 자동 백필 — 매일 05:00 (finalize 03:00·backup 04:00 끝나고, 라이브 08:00 전).
    # finalize 가 특정 매장만 실패해 전날치가 빈 경우 자동 복구.
    scheduler.add_job(
        lambda: _backfill_recent_missing(days=3), "cron",
        hour=5, minute=0,
        id="recent_backfill", replace_existing=True,
    )
    print("[scheduler] recent_backfill: 매일 05:00 (최근 3일 빠진 매장 자동 재수집)")

    # 상품 분석은 별도 잡 없음 — 메트릭 세션에 piggyback:
    #   라이브(오늘 메트릭) → 오늘 일별 랭킹 / finalize(어제 메트릭) → 전일 일별 + 최근7일 추세

    # 메타 광고성과 수집 — 매일 07:00 (API라 가벼움, chromium 무관). 최근 4일 재수집.
    scheduler.add_job(
        _meta_collect_job, "cron",
        hour=7, minute=0,
        id="meta_collect", replace_existing=True,
    )
    print(f"[scheduler] meta_collect: 매일 07:00 (최근 {META_BACKFILL_DAYS}일)")

    # 네이버 검색광고 수집 — 매일 07:10 (메타 직후)
    scheduler.add_job(
        _naver_collect_job, "cron",
        hour=7, minute=10,
        id="naver_collect", replace_existing=True,
    )
    print(f"[scheduler] naver_collect: 매일 07:10 (최근 {META_BACKFILL_DAYS}일)")

    # 크리테오 성과 수집(세션 크롤) — 매일 06:40 (chromium 쓰므로 _live_lock 으로 cafe24 와 직렬화)
    scheduler.add_job(
        _criteo_collect_job, "cron",
        hour=6, minute=40,
        id="criteo_collect", replace_existing=True,
        misfire_grace_time=3600,
    )
    print(f"[scheduler] criteo_collect: 매일 06:40 (최근 {CRITEO_BACKFILL_DAYS}일, 크롤)")

    # 크리테오 크롤 세션 만료 체크 — 매일 09:00 (만료 7일 전부터 Slack 경고)
    scheduler.add_job(
        _criteo_session_check_job, "cron",
        hour=9, minute=0,
        id="criteo_session_check", replace_existing=True,
    )
    print("[scheduler] criteo_session_check: 매일 09:00 (만료 7일 전 경고)")

    # 네이버 성과형(GFA) 성과 수집(세션 크롤) — 매일 06:50 (chromium → _live_lock 으로 직렬화)
    scheduler.add_job(
        _gfa_collect_job, "cron",
        hour=6, minute=50,
        id="gfa_collect", replace_existing=True,
        misfire_grace_time=3600,
    )
    print(f"[scheduler] gfa_collect: 매일 06:50 (최근 {GFA_BACKFILL_DAYS}일, 크롤)")

    # 네이버 성과형 크롤 세션 만료 체크 — 매일 09:05 (만료 7일 전부터 Slack 경고)
    scheduler.add_job(
        _gfa_session_check_job, "cron",
        hour=9, minute=5,
        id="gfa_session_check", replace_existing=True,
    )
    print("[scheduler] gfa_session_check: 매일 09:05 (만료 7일 전 경고)")

    # 쇼핑박스 광고비(입찰분할)+노출/클릭(네이버)+매출(cafe24 UTM) → 시트 — 매일 07:30
    scheduler.add_job(
        _shopbox_collect_job, "cron",
        hour=7, minute=30,
        id="shopbox_collect", replace_existing=True,
        misfire_grace_time=3600,
    )
    print(f"[scheduler] shopbox_collect: 매일 07:30 (광고비 분할 + 노출/클릭 크롤, 최근 {SHOPBOX_BACKFILL_DAYS}일)")

    # 매일 04:30 service self-restart - playwright/chromium 누적 상태 리셋.
    # daily_finalize(03:00) + db_backup(04:00) 끝난 뒤. startup catch-up 으로 자동 회복.
    scheduler.add_job(
        lambda: _self_restart_service("daily 04:30 정기 restart - 누적 상태 리셋"),
        "cron", hour=4, minute=30,
        id="daily_restart", replace_existing=True,
    )
    print(f"[scheduler] daily_restart: 매일 04:30 (chromium 누적 상태 리셋)")

    # 매시 정각 self-check (디스크/메모리/잡 누락/잡 결과 누락).
    # 한 번 stuck 돼도 다음 fire 가 막히지 않게 max_instances 3 + misfire_grace_time 50분.
    scheduler.add_job(
        _heartbeat_job, "cron",
        minute=0,
        id="heartbeat", replace_existing=True,
        max_instances=3, misfire_grace_time=3000,
        executor="quick",  # 긴 스크랩에 안 막히게 별도 스레드
    )
    print(f"[scheduler] heartbeat: 매시 정각 self-check")

    # graceful reload 체크 — 매분, idle 일 때 reload 요청 있으면 자가 리로드
    scheduler.add_job(
        _reload_check_job, "interval",
        seconds=30,
        id="reload_check", replace_existing=True,
        max_instances=1, misfire_grace_time=20,
        executor="quick",  # 긴 스크랩에 안 막히게 별도 스레드
    )
    print("[scheduler] reload_check: 30초마다 (idle 시 graceful 리로드)")

    # 외부 데드맨 스위치 핑 — 5분마다. 프로세스/스케줄러 죽으면 핑 끊겨 외부서비스가 알림.
    if HEALTHCHECK_PING_URL:
        scheduler.add_job(
            _deadman_ping_job, "interval",
            minutes=5,
            id="deadman_ping", replace_existing=True,
            max_instances=1, misfire_grace_time=60,
            executor="quick",  # 긴 스크랩에 안 막히게 별도 스레드
        )
        print("[scheduler] deadman_ping: 5분마다 (외부 생존 핑)")
    else:
        print("[scheduler] deadman_ping: 비활성 (HEALTHCHECK_PING_URL 미설정)")

    # 서버 시작 시점이 활성 시간이고 오늘 라이브가 한 번도 안 돌았으면
    # 30초 뒤 1회 강제 트리거. service restart 가 라이브 누락으로 이어지지 않게.
    if live["interval_min"] > 0:
        now_h = datetime.now().hour
        active = (live["start_hour"] <= now_h < live["end_hour"]) if live["start_hour"] <= live["end_hour"] else (now_h >= live["start_hour"] or now_h < live["end_hour"])
        if active:
            today_s = datetime.now().strftime("%Y-%m-%d")
            try:
                ran_today = False
                with db.db_conn() as conn:
                    n = conn.execute("SELECT COUNT(*) FROM metrics WHERE date=? AND 매출 > 0", (today_s,)).fetchone()[0]
                    ran_today = n > 0
                if not ran_today:
                    scheduler.add_job(
                        _live_global_job, "date",
                        run_date=datetime.now() + timedelta(seconds=30),
                        id="live_startup_catchup", replace_existing=True,
                    )
                    print(f"[scheduler] 활성 시간 + 오늘 데이터 없음 → 30초 뒤 라이브 catch-up 1회 트리거")
            except Exception:
                traceback.print_exc()

    # 재시작 후 daily_finalize catch-up.
    # 오늘 finalize 예정 시각이 이미 지났는데 어제 finalize 완료 마커가 없으면
    # (self-heal/정기 restart 로 중단됐단 뜻) 90초 뒤 finalize 1회 재실행 → 시트 누락 자동 회복.
    try:
        now = datetime.now()
        yest = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        finalize_time_today = now.replace(hour=df["hour"], minute=df["minute"], second=0, microsecond=0)
        last_done = db.get_setting("last_finalize_date", "")
        # 04:30 정기 restart 가 03:00 finalize 직후라, finalize 가 정상 완료했으면 마커 == yest.
        if now >= finalize_time_today and last_done != yest:
            scheduler.add_job(
                _daily_finalize_job, "date",
                run_date=now + timedelta(seconds=90),
                id="finalize_startup_catchup", replace_existing=True,
            )
            print(f"[scheduler] finalize 미완료 감지 (last={last_done!r} != {yest}) → 90초 뒤 finalize catch-up 트리거")
    except Exception:
        traceback.print_exc()


# 서버 시작 시 stuck running 정리 (이전 프로세스가 죽었으면 그 run 은 이미 끝났다고 봐야)
_startup_cleanup_info = {"count": 0, "accounts": [], "at": None}


def _cleanup_stuck_runs():
    try:
        with db.db_conn() as conn:
            cur = conn.execute("SELECT id, account_id, started_at FROM runs WHERE status='running'")
            stuck = cur.fetchall()
            if stuck:
                from datetime import datetime as _dt
                now_s = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
                conn.execute(
                    "UPDATE runs SET status='error', finished_at=?, error='startup cleanup - 이전 프로세스 종료로 unfinished 처리' WHERE status='running'",
                    (now_s,),
                )
                _startup_cleanup_info["count"] = len(stuck)
                _startup_cleanup_info["accounts"] = list({r["account_id"] for r in stuck})
                _startup_cleanup_info["at"] = now_s
                print(f"[startup] stuck running {len(stuck)}건 정리: " + ", ".join(f"{r['account_id']}#{r['id']}" for r in stuck))
                slack_notify(
                    f"서비스 startup — stuck running {len(stuck)}건 자동 정리\n계정: " +
                    ", ".join(f"`{r['account_id']}`" for r in stuck),
                    severity="cleanup",
                )
    except Exception:
        traceback.print_exc()


_cleanup_stuck_runs()


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
                return redirect(url_for("dashboard"))
        return render_template("login.html", error="아이디 또는 비밀번호가 틀렸습니다.")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.pop("logged_in", None)
    session.pop("username", None)
    return redirect(url_for("login_page"))


# ===== 페이지 라우트 =====

@app.route("/admin")
@login_required
def index():
    accounts = db.list_accounts()
    for a in accounts:
        try:
            a["monthly_goal"] = int(db.get_setting(f"goal_{a['id']}", "0") or 0)
        except ValueError:
            a["monthly_goal"] = 0
    runs = db.list_runs(limit=30)
    for r in runs:
        r["display_date"] = _date_from_run(r)
        r["hourly_count"] = db.count_metrics_hourly(r["account_id"], r["display_date"]) if r["display_date"] else 0
        r["error_summary"] = _short_error(r.get("error"))
    schedules = {s["account_id"]: s for s in db.list_schedules()}
    live = db.get_live_settings()
    daily = db.get_daily_finalize_settings()

    # 자동 스케줄 현황 (관리 페이지 상단 표시용) — 잡별 주기/다음실행/마지막결과
    _sched_info = {
        "live_global": ("🔴 라이브 스크랩", f"활성시간 매시간 (오늘 매트릭+상품)", None),
        "daily_finalize": ("📋 일일 정리", "매일 03:00 (어제 확정+시트+상품)", None),
        "meta_collect": ("📣 메타 광고 수집", f"매일 07:00 (최근 {META_BACKFILL_DAYS}일 → 효율시트)", db.get_setting("meta_last_run", None)),
        "naver_collect": ("🟢 네이버 검색광고 수집", f"매일 07:10 (최근 {META_BACKFILL_DAYS}일 → 효율시트)", db.get_setting("naver_last_run", None)),
        "criteo_collect": ("🟠 크리테오 수집", f"매일 06:40 (최근 {CRITEO_BACKFILL_DAYS}일, 세션크롤 → 효율시트)", db.get_setting("criteo_last_run", None)),
        "gfa_collect": ("🟢 네이버 성과형(GFA) 수집", f"매일 06:50 (최근 {GFA_BACKFILL_DAYS}일, 세션크롤 → 효율시트)", db.get_setting("gfa_last_run", None)),
        "shopbox_collect": ("🛍 쇼핑박스 (광고비+노출/클릭+매출)", f"매일 07:30 (입찰분할 + 네이버 노출/클릭 + cafe24 UTM 매출 → 효율시트, 최근 {SHOPBOX_BACKFILL_DAYS}일)", db.get_setting("shopbox_last_run", None)),
        "db_backup": ("💾 DB 백업", "매일 04:00", None),
        "daily_restart": ("🔄 정기 재시작", "매일 04:30", None),
    }
    sched_rows = []
    try:
        jobs = {j.id: j for j in scheduler.get_jobs()}
        for jid in ["live_global", "daily_finalize", "meta_collect", "naver_collect",
                    "criteo_collect", "gfa_collect", "shopbox_collect", "db_backup", "daily_restart"]:
            if jid not in _sched_info:
                continue
            name, desc, last = _sched_info[jid]
            j = jobs.get(jid)
            nxt = j.next_run_time.strftime("%m/%d %H:%M") if (j and j.next_run_time) else "-"
            sched_rows.append({"name": name, "desc": desc, "next": nxt, "last": last, "active": jid in jobs})
    except Exception:
        traceback.print_exc()

    # 계정별 마지막 상태 요약 (계정 관리 표 표시용)
    acct_status = {}
    try:
        with db.db_conn() as conn:
            for a in accounts:
                aid = a["id"]
                last_metric = conn.execute(
                    "SELECT MAX(updated_at) FROM metrics WHERE account_id=?", (aid,)).fetchone()[0]
                last_success = conn.execute(
                    "SELECT MAX(started_at) FROM runs WHERE account_id=? AND status='success'", (aid,)).fetchone()[0]
                fail = conn.execute(
                    "SELECT started_at, error FROM runs WHERE account_id=? AND status='error' "
                    "ORDER BY started_at DESC LIMIT 1", (aid,)).fetchone()
                fail_at = fail["started_at"] if fail else None
                fail_err = _short_error(fail["error"]) if fail else None
                # 실패 종류 분류
                fail_kind = ""
                if fail and fail["error"]:
                    e = fail["error"]
                    if "sample" in e:
                        fail_kind = "premium"
                    elif "startup cleanup" in e:
                        fail_kind = "restart"
                    elif "timeout" in e or "hang" in e or "강제 종료" in e:
                        fail_kind = "slow"
                    else:
                        fail_kind = "error"
                acct_status[aid] = {
                    "last_metric": last_metric,
                    "last_success": last_success,
                    "last_sheet": db.get_setting(f"sheet_updated_{aid}", None),
                    "fail_at": fail_at,
                    "fail_err": fail_err,
                    "fail_kind": fail_kind,
                    "meta_last": db.get_setting(f"meta_last_{aid}", None),
                    "criteo_last": db.get_setting(f"criteo_last_{aid}", None),
                    "gfa_last": db.get_setting(f"gfa_last_{aid}", None),
                }
    except Exception:
        traceback.print_exc()

    return render_template(
        "index.html",
        accounts=accounts,
        runs=runs,
        schedules=schedules,
        running=_running,
        live=live,
        daily=daily,
        acct_status=acct_status,
        sched_rows=sched_rows,
        now=datetime.now().strftime("%Y-%m-%d %H:%M"),
        session_statuses=[{**p, "tool": p["command"]} for p in _session_guard_payload()],
        shopbox_bids_by_acct=_shopbox_bids_by_acct(),
    )


# ===== 계정 API =====

@app.route("/accounts", methods=["POST"])
@login_required
def add_account():
    cafe24_id = request.form["cafe24_id"].strip()
    sub_id = request.form.get("sub_id", "").strip()
    password = request.form["password"].strip()
    label = request.form.get("label", "").strip()
    spreadsheet_id = sheets.clean_spreadsheet_id(request.form.get("spreadsheet_id", "").strip())
    db.add_account(cafe24_id, sub_id, password, label, spreadsheet_id)
    return redirect(url_for("index"))


@app.route("/accounts/<account_id>/spreadsheet", methods=["POST"])
@login_required
def update_spreadsheet(account_id):
    sid = sheets.clean_spreadsheet_id(request.form.get("spreadsheet_id", "").strip())
    db.update_spreadsheet_id(account_id, sid)
    return redirect(url_for("index"))


@app.route("/healthz")
def healthz():
    """외부 헬스체크용 endpoint. 인증 없음.
    - scheduler 잡 등록 상태
    - 오늘 라이브 사이클 진행 정도
    - 현재 실행 중인 스크래핑 수
    UptimeRobot 등 외부 모니터링에서 /healthz 5분마다 ping → 200 안 오면 alert."""
    try:
        today_s = datetime.now().strftime("%Y-%m-%d")
        with db.db_conn() as conn:
            ran_today = conn.execute("SELECT COUNT(*) FROM metrics WHERE date=? AND 매출>0", (today_s,)).fetchone()[0]
            total_accounts = conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
        cur_h = datetime.now().hour
        live = db.get_live_settings()
        active_now = live["interval_min"] > 0 and live["start_hour"] <= cur_h < live["end_hour"]
        return jsonify({
            "ok": True,
            "now": datetime.now().isoformat(),
            "started_at": _PROC_STARTED,
            "pending_reload": bool(_pending_reload["at"]),
            "scheduler_jobs": [j.id for j in scheduler.get_jobs()],
            "today_accounts_with_data": ran_today,
            "total_accounts": total_accounts,
            "live_active_now": active_now,
            "running_now": list(_running.keys()),
            "free_mb": _free_memory_mb(),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/admin/backfill_dates", methods=["POST"])
def admin_backfill_dates():
    """localhost 전용 백필 트리거. _run_lock 으로 라이브 잡과 자동 직렬화.
    body:
      account_id=<id> | all   (all 이면 전체 계정)
      dates=2026-05-08,2026-05-09 (콤마 구분)
      skip_sheet=true | false (기본 false → 시트 write 진행)
    """
    remote = request.remote_addr or ""
    if remote not in ("127.0.0.1", "::1", "localhost"):
        return jsonify({"error": "forbidden"}), 403
    account_id = request.form.get("account_id", "").strip()
    dates_raw = request.form.get("dates", "").strip()
    skip_sheet = request.form.get("skip_sheet", "false").lower() == "true"
    if not account_id or not dates_raw:
        return jsonify({"error": "account_id, dates required"}), 400
    dates = [d.strip() for d in dates_raw.split(",") if d.strip()]
    for d in dates:
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            return jsonify({"error": f"invalid date {d}"}), 400

    if account_id == "all":
        target_aids = [a["id"] for a in db.list_accounts()]
    else:
        target_aids = [account_id]

    def _run_all():
        total = len(target_aids) * len(dates)
        cnt = 0
        for aid in target_aids:
            for d in dates:
                cnt += 1
                print(f"[backfill] ({cnt}/{total}) {aid} {d} 시작 skip_sheet={skip_sheet}", flush=True)
                try:
                    _run_scrape_task(aid, target_date=d, skip_sheet=skip_sheet)
                except Exception:
                    traceback.print_exc()
                print(f"[backfill] ({cnt}/{total}) {aid} {d} 끝", flush=True)
        print(f"[backfill] 전체 완료 ({total}건)", flush=True)

    threading.Thread(target=_run_all, daemon=True).start()
    return jsonify({"ok": True, "accounts": target_aids, "queued_dates": dates, "skip_sheet": skip_sheet, "total": len(target_aids) * len(dates)})


@app.route("/admin/product_collect", methods=["POST"])
def admin_product_collect():
    """localhost 전용 product_collect 트리거. service 컨텍스트(DISPLAY=:99) 에서 실행.
    body:
      account_id=<id> | all (선택, 기본 all)
    """
    remote = request.remote_addr or ""
    if remote not in ("127.0.0.1", "::1", "localhost"):
        return jsonify({"error": "forbidden"}), 403
    account_id = request.form.get("account_id", "all").strip()
    period = request.form.get("period", "7d").strip()  # 7d | yesterday | today
    if period not in ("7d", "yesterday", "today"):
        return jsonify({"error": "period must be 7d|yesterday|today"}), 400
    today = datetime.now().strftime("%Y-%m-%d")
    if account_id == "all":
        target = db.list_accounts()
    else:
        a = db.get_account(account_id)
        if not a:
            return jsonify({"error": "account not found"}), 404
        target = [a]

    def _run_one():
        ok = 0
        err = 0
        for i, a in enumerate(target):
            _wait_for_memory(f"product before {a['id']}")
            with _run_lock:
                try:
                    rows = _run_product_with_timeout(a, period=period)
                    if rows:
                        db.upsert_product_metrics(a["id"], today, rows, period=period)
                        print(f"[product-collect-manual] {a['id']} [{period}] {len(rows)}건 저장", flush=True)
                        ok += 1
                    else:
                        print(f"[product-collect-manual] {a['id']} [{period}] 0건", flush=True)
                    _note_account_success()
                except HangTimeout:
                    err += 1
                    _note_account_hang(a["id"], context=f"product_collect-manual {period}")
                except Exception:
                    err += 1
                    traceback.print_exc()
            if i < len(target) - 1:
                time.sleep(INTER_ACCOUNT_COOLDOWN_SEC)
        _kill_leftover_chromium()
        print(f"[product-collect-manual] [{period}] done ok={ok} err={err}", flush=True)
        slack_notify(
            f"product-collect (manual) 완료 ({today}) [{period}] · 성공 {ok} / 실패 {err}",
            severity="report" if err == 0 else "warn",
        )

    threading.Thread(target=_run_one, daemon=True).start()
    return jsonify({"ok": True, "accounts": [a["id"] for a in target], "date": today, "period": period})


@app.route("/admin/meta_collect", methods=["POST"])
def admin_meta_collect():
    """localhost 전용 메타 수집 수동 트리거. body: days(선택, 기본 4)."""
    remote = request.remote_addr or ""
    if remote not in ("127.0.0.1", "::1", "localhost"):
        return jsonify({"error": "forbidden"}), 403
    try:
        days = int(request.form.get("days", str(META_BACKFILL_DAYS)))
    except ValueError:
        days = META_BACKFILL_DAYS
    threading.Thread(target=lambda: _meta_collect_job(days=days), daemon=True).start()
    return jsonify({"ok": True, "days": days})


@app.route("/accounts/<account_id>/meta", methods=["POST"])
@login_required
def update_meta_route(account_id):
    db.update_meta_account_id(account_id, request.form.get("meta_account_id", "").strip())
    return redirect(url_for("index"))


@app.route("/admin/naver_collect", methods=["POST"])
def admin_naver_collect():
    """localhost 전용 네이버 수집 수동 트리거. body: days(선택)."""
    remote = request.remote_addr or ""
    if remote not in ("127.0.0.1", "::1", "localhost"):
        return jsonify({"error": "forbidden"}), 403
    try:
        days = int(request.form.get("days", str(META_BACKFILL_DAYS)))
    except ValueError:
        days = META_BACKFILL_DAYS
    threading.Thread(target=lambda: _naver_collect_job(days=days), daemon=True).start()
    return jsonify({"ok": True, "days": days})


@app.route("/admin/backfill_recent", methods=["POST"])
@login_required
def admin_backfill_recent():
    """최근 N일(기본 3) 빠진 매장 데이터 수동 재수집 트리거 (백그라운드)."""
    try:
        days = int(request.form.get("days", "3"))
    except ValueError:
        days = 3
    threading.Thread(target=lambda: _backfill_recent_missing(days=days, manual=True), daemon=True).start()
    return redirect(url_for("dashboard", backfill="started"))


@app.route("/accounts/<account_id>/naver", methods=["POST"])
@login_required
def update_naver_route(account_id):
    db.update_naver_creds(
        account_id,
        request.form.get("naver_api_key", "").strip(),
        request.form.get("naver_secret", "").strip(),
        request.form.get("naver_customer_id", "").strip(),
    )
    # 입력한 페이지로 복귀 (시트현황 / 설정 어디서든)
    return redirect(request.referrer or url_for("index"))


@app.route("/accounts/<account_id>/criteo", methods=["POST"])
@login_required
def update_criteo_route(account_id):
    db.update_criteo_advertiser_id(account_id, request.form.get("criteo_advertiser_id", "").strip())
    return redirect(request.referrer or url_for("index"))


@app.route("/admin/criteo_collect", methods=["POST"])
def admin_criteo_collect():
    """localhost 전용 크리테오 수집 수동 트리거. body: days(선택)."""
    if (request.remote_addr or "") not in ("127.0.0.1", "::1", "localhost"):
        return jsonify({"error": "forbidden"}), 403
    try:
        days = int(request.form.get("days", str(CRITEO_BACKFILL_DAYS)))
    except ValueError:
        days = CRITEO_BACKFILL_DAYS
    threading.Thread(target=lambda: _criteo_collect_job(days=days), daemon=True).start()
    return jsonify({"ok": True, "days": days})


@app.route("/accounts/<account_id>/gfa", methods=["POST"])
@login_required
def update_gfa_route(account_id):
    db.update_naver_gfa_account_no(account_id, request.form.get("naver_gfa_account_no", "").strip())
    return redirect(request.referrer or url_for("index"))


@app.route("/admin/gfa_collect", methods=["POST"])
def admin_gfa_collect():
    """localhost 전용 네이버 성과형 수집 수동 트리거. body: days(선택)."""
    if (request.remote_addr or "") not in ("127.0.0.1", "::1", "localhost"):
        return jsonify({"error": "forbidden"}), 403
    try:
        days = int(request.form.get("days", str(GFA_BACKFILL_DAYS)))
    except ValueError:
        days = GFA_BACKFILL_DAYS
    threading.Thread(target=lambda: _gfa_collect_job(days=days), daemon=True).start()
    return jsonify({"ok": True, "days": days})


@app.route("/accounts/<account_id>/shopbox_bid", methods=["POST"])
@login_required
def add_shopbox_bid_route(account_id):
    """쇼핑박스 낙찰가(정액 입찰) 입력 — 집행기간 일수로 일별분할됨."""
    f = request.form
    try:
        amount = int((f.get("amount") or "0").replace(",", "").strip() or 0)
    except ValueError:
        amount = 0
    device = (f.get("device") or "pc").strip()
    gender = (f.get("gender") or "f").strip()
    start = (f.get("start_date") or "").strip()
    end = (f.get("end_date") or "").strip()
    if device in ("pc", "mo") and start and end and amount > 0:
        db.add_shopbox_bid(account_id, device, gender, start, end, amount, f.get("memo", ""))
    return redirect(request.referrer or url_for("index"))


@app.route("/shopbox/bid", methods=["POST"])
@login_required
def shopbox_add_bid():
    """전용 쇼핑박스 페이지에서 낙찰가 입력 (account_id 를 폼에서 받음)."""
    f = request.form
    account_id = (f.get("account_id") or "").strip()
    try:
        amount = int((f.get("amount") or "0").replace(",", "").strip() or 0)
    except ValueError:
        amount = 0
    device = (f.get("device") or "pc").strip()
    gender = (f.get("gender") or "f").strip()
    start = (f.get("start_date") or "").strip()
    end = (f.get("end_date") or "").strip()
    if not (account_id and device in ("pc", "mo") and start and end and amount > 0):
        return redirect(url_for("shopbox_page", msg="invalid"))
    if end < start:
        return redirect(url_for("shopbox_page", msg="baddate"))
    # 같은 매장·유형·성별의 기간이 겹치는 입찰이 이미 있으면 차단 (광고비 이중집계 방지).
    # 성별까지 봐야 함 — 같은 주 여성+남성 동시 입찰은 정상(별도 구좌·낙찰가)이라 막으면 안 됨.
    for b in db.list_shopbox_bids([account_id]):
        if b["device"] == device and b["gender"] == gender and not (b["end_date"] < start or b["start_date"] > end):
            return redirect(url_for("shopbox_page", msg="overlap"))
    db.add_shopbox_bid(account_id, device, gender, start, end, amount, f.get("memo", ""))
    _shopbox_apply_bid_cost(account_id, device, start, end)
    return redirect(url_for("shopbox_page", msg="added"))


def _shopbox_apply_bid_cost(account_id, device, start, end):
    """입찰 추가/삭제 직후, 그 기간의 일별 광고비를 즉시 shopbox_metrics 에 반영
    (수집 job 안 기다리고 대시보드에 ROAS 바로 보이게). 최근 60일~오늘로 범위 제한."""
    try:
        s = max(datetime.strptime(start, "%Y-%m-%d").date(), datetime.now().date() - timedelta(days=60))
        e = min(datetime.strptime(end, "%Y-%m-%d").date(), datetime.now().date())
    except Exception:
        return
    d = s
    while d <= e:
        ds = d.isoformat()
        db.set_shopbox_cost(account_id, ds, device, db.shopbox_daily_cost(account_id, device, ds))
        d += timedelta(days=1)


@app.route("/shopbox/bid_bulk", methods=["POST"])
@login_required
def shopbox_add_bid_bulk():
    """여러 매장에 동일 낙찰가 일괄 입력 (낙찰가는 기간별로 전 매장 동일하므로)."""
    f = request.form
    account_ids = [a for a in f.getlist("account_ids") if a]
    try:
        amount = int((f.get("amount") or "0").replace(",", "").strip() or 0)
    except ValueError:
        amount = 0
    device = (f.get("device") or "pc").strip()
    gender = (f.get("gender") or "f").strip()
    start = (f.get("start_date") or "").strip()
    end = (f.get("end_date") or "").strip()
    if not (account_ids and device in ("pc", "mo") and start and end and amount > 0):
        return redirect(url_for("shopbox_page", msg="invalid"))
    if end < start:
        return redirect(url_for("shopbox_page", msg="baddate"))
    added = skipped = 0
    for aid in account_ids:
        dup = any(b["device"] == device and b["gender"] == gender
                  and not (b["end_date"] < start or b["start_date"] > end)
                  for b in db.list_shopbox_bids([aid]))
        if dup:
            skipped += 1
            continue
        db.add_shopbox_bid(aid, device, gender, start, end, amount, f.get("memo", ""))
        _shopbox_apply_bid_cost(aid, device, start, end)
        added += 1
    return redirect(url_for("shopbox_page", msg="bulk", added=added, skipped=skipped))


@app.route("/shopbox_bid/<int:bid_id>/delete", methods=["POST"])
@login_required
def delete_shopbox_bid_route(bid_id):
    bid = next((b for b in db.list_shopbox_bids() if b["id"] == bid_id), None)
    db.delete_shopbox_bid(bid_id)
    if bid:  # 삭제 후 그 기간 광고비 재계산(남은 입찰 반영, 없으면 0) → 시트·대시보드 정정
        _shopbox_apply_bid_cost(bid["account_id"], bid["device"], bid["start_date"], bid["end_date"])
    return redirect(request.referrer or url_for("index"))


@app.route("/shopbox")
@login_required
def shopbox_page():
    """쇼핑박스 낙찰가 입력 전용 페이지 — 입력 UX + 진행중 입찰 + 미입력 경고."""
    accounts = db.list_accounts()
    creds = shopbox._load_accounts()
    label = {a["id"]: (a.get("label") or a["id"]) for a in accounts}
    today = datetime.now().date()
    # 입찰 원장 (활성 우선 → 시작일 최신순)
    bids = []
    for b in db.list_shopbox_bids():
        try:
            s = datetime.strptime(b["start_date"], "%Y-%m-%d").date()
            e = datetime.strptime(b["end_date"], "%Y-%m-%d").date()
            days = (e - s).days + 1
        except Exception:
            days = 0
        bids.append({**b, "label": label.get(b["account_id"], b["account_id"]),
                     "days": days, "daily": round(b["amount"] / days) if days > 0 else 0,
                     "active": days > 0 and s <= today <= e})
    bids.sort(key=lambda r: (not r["active"], r["start_date"]), reverse=False)
    # 쇼핑박스 운영 중(최근7일 노출>0)인데 오늘 유효한 입찰 없는 (매장,device) → 낙찰가 미입력 경고
    win_start = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    running = {}
    for r in db.list_shopbox_metrics(start_date=win_start, end_date=today.strftime("%Y-%m-%d")):
        if (r.get("impressions") or 0) > 0:
            running[(r["account_id"], r["device"])] = True
    has_active_bid = {(b["account_id"], b["device"]) for b in bids if b["active"]}
    missing = [{"label": label.get(a, a), "account_id": a, "device": dv.upper()}
               for (a, dv) in running if (a, dv) not in has_active_bid]
    missing.sort(key=lambda x: x["label"])
    # 입찰 대상 매장 (쇼핑박스 자격 있거나 이미 입찰원장 있는 매장 우선, 없으면 전체)
    bid_accts = [a for a in accounts if a["id"] in creds] or accounts
    # 매장별 현황 — 쇼핑박스 대상 매장의 PC/MO 입찰 상태 한눈에
    overview = []
    for a in bid_accts:
        row = {"label": a.get("label") or a["id"], "id": a["id"]}
        for dv in ("pc", "mo"):
            active = next((b for b in bids if b["account_id"] == a["id"] and b["device"] == dv and b["active"]), None)
            is_running = (a["id"], dv) in running
            row[dv] = {"amount": active["amount"] if active else None,
                       "daily": active["daily"] if active else None,
                       "running": is_running,
                       "warn": is_running and not active}
        overview.append(row)
    overview.sort(key=lambda r: (not (r["pc"]["warn"] or r["mo"]["warn"]), r["label"]))
    return render_template("shopbox.html", active="shopbox",
                           accounts=bid_accts, bids=bids, missing=missing,
                           overview=overview, msg=request.args.get("msg", ""),
                           added=request.args.get("added", ""), skipped=request.args.get("skipped", ""),
                           today=today.strftime("%Y-%m-%d"),
                           last_run=db.get_setting("shopbox_last_run", None))


@app.route("/admin/shopbox_collect", methods=["POST"])
def admin_shopbox_collect():
    """localhost 전용 쇼핑박스 광고비 일별분할→시트 수동 트리거."""
    if (request.remote_addr or "") not in ("127.0.0.1", "::1", "localhost"):
        return jsonify({"error": "forbidden"}), 403
    try:
        days = int(request.form.get("days", str(SHOPBOX_BACKFILL_DAYS)))
    except ValueError:
        days = SHOPBOX_BACKFILL_DAYS
    threading.Thread(target=lambda: _shopbox_collect_job(days=days), daemon=True).start()
    return jsonify({"ok": True, "days": days})


@app.route("/admin/request_reload", methods=["POST"])
def admin_request_reload():
    """localhost 전용 — 배포(deploy.sh)가 git pull 후 호출. 안전지점에서 새 코드로 자가 리로드.
    systemctl restart 와 달리 진행 중인 스크랩을 죽이지 않고 계정 경계/idle 에서 reload."""
    if (request.remote_addr or "") not in ("127.0.0.1", "::1", "localhost"):
        return jsonify({"error": "forbidden"}), 403
    _pending_reload["at"] = datetime.now().isoformat()
    print(f"[reload] reload 요청 접수 — 스크랩 진행중={bool(_running)} (안전지점에서 리로드)", flush=True)
    return jsonify({"ok": True, "scraping": bool(_running), "started_at": _PROC_STARTED})


@app.route("/admin/request_session_refresh", methods=["POST"])
@login_required
def request_session_refresh():
    """웹 '🔄 갱신 요청' 버튼 — 맥 백그라운드 도우미가 감지해 로그인 창을 띄움."""
    import datetime as _dt
    ch = (request.form.get("channel") or "").strip()
    if ch not in {c[0] for c in _GUARD_CHANNELS}:
        return jsonify({"error": "unknown channel"}), 400
    db.set_setting(f"{ch}_refresh_requested", _dt.datetime.now().strftime("%Y-%m-%d %H:%M"))
    name = next(c[1] for c in _GUARD_CHANNELS if c[0] == ch)
    slack_notify(f"🔄 {name} 세션 갱신 요청됨 — 맥 도우미가 곧 로그인 창을 띄웁니다.", severity="info")
    return redirect(request.referrer or url_for("index"))


def _guard_token_ok():
    return GUARD_TOKEN and (request.args.get("token") or request.headers.get("X-Guard-Token")) == GUARD_TOKEN


@app.route("/api/session_guard", methods=["GET"])
def api_session_guard():
    """맥 백그라운드 도우미가 폴링하는 상태 엔드포인트 (token 인증)."""
    if not _guard_token_ok():
        return jsonify({"error": "forbidden"}), 403
    return jsonify({"channels": _session_guard_payload()})


@app.route("/api/session_guard/wait", methods=["GET"])
def api_session_guard_wait():
    """롱폴링 — 갱신 요청이 들어오는 즉시 응답(최대 ~25초 대기 후 타임아웃).
    맥 도우미가 이걸 계속 열어두면 버튼 클릭 → 1초 내 로그인 창이 뜬다."""
    if not _guard_token_ok():
        return jsonify({"error": "forbidden"}), 403
    import time as _t
    deadline = _t.time() + 25
    while _t.time() < deadline:
        payload = _session_guard_payload()
        if any(c["requested"] for c in payload):
            return jsonify({"channels": payload, "pending": True})
        _t.sleep(1.0)
    return jsonify({"channels": _session_guard_payload(), "pending": False})


@app.route("/api/session_guard/ack", methods=["POST"])
def api_session_guard_ack():
    """맥 도우미가 로그인 창을 띄운 뒤 요청 플래그 해제 (재실행 방지)."""
    if not _guard_token_ok():
        return jsonify({"error": "forbidden"}), 403
    ch = (request.form.get("channel") or request.args.get("channel") or "").strip()
    if ch not in {c[0] for c in _GUARD_CHANNELS}:
        return jsonify({"error": "unknown channel"}), 400
    db.set_setting(f"{ch}_refresh_requested", "")
    return jsonify({"ok": True})


@app.route("/settings/target_roas", methods=["POST"])
@login_required
def set_target_roas():
    try:
        v = int(request.form.get("target_roas", "300"))
        db.set_setting("target_roas", str(max(0, v)))
    except ValueError:
        pass
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/accounts/<account_id>/goal", methods=["POST"])
@login_required
def set_account_goal(account_id):
    """매장 월 매출목표(원) 설정. 0/빈값이면 목표 해제."""
    try:
        v = int(request.form.get("monthly_goal", "0").replace(",", "") or "0")
        db.set_setting(f"goal_{account_id}", str(max(0, v)))
    except ValueError:
        pass
    return redirect(request.referrer or url_for("index"))


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


@app.route("/")
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

    all_accounts = db.list_accounts()
    all_ids = [a["id"] for a in all_accounts]
    selected_ids = request.args.getlist("account_id")
    if not selected_ids:
        selected_ids = all_ids
    accounts = [a for a in all_accounts if a["id"] in selected_ids]
    all_metrics = db.list_metrics(start_date=range_start, end_date=today)
    by_key = {(m["account_id"], m["date"]): m for m in all_metrics if m["account_id"] in selected_ids}

    # 최근 8일 hourly (시간대 평균 + 어제 동시각 누적 + 오늘 시간별)
    hourly_start = (now - timedelta(days=8)).strftime("%Y-%m-%d")
    all_hourly = db.list_metrics_hourly_range([a["id"] for a in accounts], hourly_start, today)
    # account_id -> list[24] of sales (각 시간 매출 평균 계산용) — 오늘은 진행중이라 평균 표본에서 제외
    hour_buckets = {}
    # account_id -> date -> dict[hour] = 매출
    by_acct_date_hour = {}
    # 구매건수 시간별 (어제 동시각 비교용)
    by_acct_date_hour_orders = {}
    # 방문자 시간별 (어제 동시각 방문자 비교용)
    by_acct_date_hour_visitors = {}
    for r in all_hourly:
        if r["date"] != today:
            hour_buckets.setdefault(r["account_id"], [[] for _ in range(24)])[r["hour"]].append(r.get("매출") or 0)
        by_acct_date_hour.setdefault(r["account_id"], {}).setdefault(r["date"], {})[r["hour"]] = r.get("매출") or 0
        by_acct_date_hour_orders.setdefault(r["account_id"], {}).setdefault(r["date"], {})[r["hour"]] = r.get("구매건수") or 0
        by_acct_date_hour_visitors.setdefault(r["account_id"], {}).setdefault(r["date"], {})[r["hour"]] = (r["방문자"] if "방문자" in r.keys() else 0) or 0

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
        # 7일 평균 — 매출 0/None 인 일자(미수집)는 제외해야 정확.
        last7_vals_sales = [by_key.get((aid, d), {}).get("매출") for d in last7_dates if by_key.get((aid, d))]
        valid_vals = [v for v in last7_vals_sales if v and v > 0]
        last7_avg_sales = round(sum(valid_vals) / len(valid_vals)) if valid_vals else None

        today_sales = m_today.get("매출")
        today_orders = m_today.get("구매건수")
        today_visitors = m_today.get("방문자수")
        today_aov = m_today.get("객단가")
        today_new_visit = m_today.get("신규방문")
        today_rev_visit = m_today.get("재방문")
        today_uniq_visit = m_today.get("순방문자수")
        today_signup = m_today.get("회원가입")
        today_first_buy = m_today.get("처음구매")
        today_rebuy = m_today.get("재구매")
        today_buy_qty = m_today.get("구매개수")
        today_visit_sales = m_today.get("방문당매출")

        # 어제 동시각 누적 매출 (어제 0~cur_hour 합) — 사과대사과 비교 핵심
        ydh = by_acct_date_hour.get(aid, {}).get(yesterday, {})
        yest_at_hour = sum(ydh.get(h, 0) for h in range(cur_hour + 1))
        # 어제 동시각 누적 구매건수
        ydh_orders = by_acct_date_hour_orders.get(aid, {}).get(yesterday, {})
        yest_orders_at_hour = sum(ydh_orders.get(h, 0) for h in range(cur_hour + 1))
        # 어제 동시각 누적 방문자 (어제 시간별 방문자 0~cur_hour 합). 시간별 방문자 데이터 없으면 None.
        ydh_vis = by_acct_date_hour_visitors.get(aid, {}).get(yesterday, {})
        yest_visitors_at_hour = sum(ydh_vis.get(h, 0) for h in range(cur_hour + 1)) if ydh_vis else None

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
                "신규방문": today_new_visit,
                "재방문": today_rev_visit,
                "순방문자수": today_uniq_visit,
                "회원가입": today_signup,
                "처음구매": today_first_buy,
                "재구매": today_rebuy,
                "구매개수": today_buy_qty,
                "방문당매출": today_visit_sales,
            },
            "yesterday": {
                "매출": m_yest.get("매출"),
                "구매건수": m_yest.get("구매건수"),
                "방문자수": m_yest.get("방문자수"),
                "객단가": m_yest.get("객단가"),
                "전환율": conv_yest,
                "신규방문": m_yest.get("신규방문"),
                "재방문": m_yest.get("재방문"),
                "회원가입": m_yest.get("회원가입"),
                "처음구매": m_yest.get("처음구매"),
                "재구매": m_yest.get("재구매"),
                "구매개수": m_yest.get("구매개수"),
            },
            "yest_at_hour": yest_at_hour,  # 어제 같은 시각까지 누적 매출
            "yest_orders_at_hour": yest_orders_at_hour,  # 어제 같은 시각 누적 구매건수
            "yest_visitors_at_hour": yest_visitors_at_hour,  # 어제 같은 시각 누적 방문자 (None=시간별 없음)
            "vs_yest_at_hour_pct": _pct(today_sales, yest_at_hour),
            "vs_yest_orders_at_hour_pct": _pct(today_orders, yest_orders_at_hour),
            "vs_yest_visitors_at_hour_pct": _pct(today_visitors, yest_visitors_at_hour),
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
    sum_today_new_visit = sum((r["today"]["신규방문"] or 0) for r in rows)
    sum_today_rev_visit = sum((r["today"]["재방문"] or 0) for r in rows)
    sum_today_signup = sum((r["today"]["회원가입"] or 0) for r in rows)
    sum_today_first_buy = sum((r["today"]["처음구매"] or 0) for r in rows)
    sum_today_rebuy = sum((r["today"]["재구매"] or 0) for r in rows)
    sum_today_buy_qty = sum((r["today"]["구매개수"] or 0) for r in rows)
    sum_yest_at_hour = sum((r["yest_at_hour"] or 0) for r in rows)
    sum_yest_full = sum((r["yesterday"]["매출"] or 0) for r in rows)
    sum_yest_orders = sum((r["yesterday"]["구매건수"] or 0) for r in rows)
    sum_yest_visitors = sum((r["yesterday"]["방문자수"] or 0) for r in rows)
    sum_yest_signup = sum((r["yesterday"]["회원가입"] or 0) for r in rows)
    sum_expected = sum((r["expected_eod"] or 0) for r in rows)
    active_count = sum(1 for r in rows if (r["today"]["매출"] or 0) > 0)

    # 파생 KPI
    avg_aov = round(sum_today_sales / sum_today_orders) if sum_today_orders else None
    overall_conv = round(sum_today_orders / sum_today_visitors * 100, 2) if sum_today_visitors else None
    overall_conv_yest = round(sum_yest_orders / sum_yest_visitors * 100, 2) if sum_yest_visitors else None
    rebuy_pct = round(sum_today_rebuy / sum_today_orders * 100, 1) if sum_today_orders else None
    new_visit_pct = round(sum_today_new_visit / (sum_today_new_visit + sum_today_rev_visit) * 100, 1) if (sum_today_new_visit + sum_today_rev_visit) else None
    items_per_order = round(sum_today_buy_qty / sum_today_orders, 2) if sum_today_orders else None

    mega = {
        "today_sales": sum_today_sales,
        "today_orders": sum_today_orders,
        "today_visitors": sum_today_visitors,
        "today_new_visit": sum_today_new_visit,
        "today_rev_visit": sum_today_rev_visit,
        "today_signup": sum_today_signup,
        "today_first_buy": sum_today_first_buy,
        "today_rebuy": sum_today_rebuy,
        "today_buy_qty": sum_today_buy_qty,
        "yest_at_hour": sum_yest_at_hour,
        "yest_full": sum_yest_full,
        "yest_orders": sum_yest_orders,
        "yest_visitors": sum_yest_visitors,
        "yest_signup": sum_yest_signup,
        "expected_eod": sum_expected,
        "vs_yest_at_hour_pct": _pct(sum_today_sales, sum_yest_at_hour),
        "vs_yest_full_pct": _pct(sum_expected if sum_expected else sum_today_sales, sum_yest_full),
        "vs_yest_visitors_pct": _pct(sum_today_visitors, sum_yest_visitors),
        "vs_yest_orders_pct": _pct(sum_today_orders, sum_yest_orders),
        "vs_yest_signup_pct": _pct(sum_today_signup, sum_yest_signup),
        "avg_aov": avg_aov,
        "overall_conv": overall_conv,
        "overall_conv_yest": overall_conv_yest,
        "rebuy_pct": rebuy_pct,
        "new_visit_pct": new_visit_pct,
        "items_per_order": items_per_order,
        "active": active_count,
        "total_accounts": len(rows),
        "cur_hour": cur_hour,
    }

    # 하이라이트: 매출 1위, 가장 큰 ▲ / ▼
    # 분모 (어제 동시각 매출) 가 너무 작으면 % 가 극단적으로 튀어서 misleading.
    # 최소 100,000원 이상인 계정만 top_gainer/loser 후보로.
    MIN_YEST_FOR_HIGHLIGHT = 100_000
    rows_with_sales = [r for r in rows if (r["today"]["매출"] or 0) > 0]
    best = rows_with_sales[0] if rows_with_sales else None
    rows_with_delta = [
        r for r in rows_with_sales
        if r["vs_yest_at_hour_pct"] is not None and (r["yest_at_hour"] or 0) >= MIN_YEST_FOR_HIGHLIGHT
    ]
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

    # ----- 신규: 시간별 누적 매출 (전 계정 합계, 오늘 vs 어제 동시각) -----
    today_hour_map = {}  # hour -> 매출 합
    today_hour_orders = {}  # hour -> 건수 합
    for r in all_hourly:
        if r["date"] == today:
            today_hour_map[r["hour"]] = today_hour_map.get(r["hour"], 0) + (r.get("매출") or 0)
            today_hour_orders[r["hour"]] = today_hour_orders.get(r["hour"], 0) + (r.get("구매건수") or 0)
    yest_hour_map = {}
    for aid in [a["id"] for a in accounts]:
        for h, v in by_acct_date_hour.get(aid, {}).get(yesterday, {}).items():
            yest_hour_map[h] = yest_hour_map.get(h, 0) + v

    hourly_trend = []
    cum_t = 0
    cum_y = 0
    for h in range(24):
        t = today_hour_map.get(h, 0)
        y = yest_hour_map.get(h, 0)
        cum_t += t
        cum_y += y
        hourly_trend.append({
            "hour": h,
            "today_hour": t if h <= cur_hour else None,
            "today_cum": cum_t if h <= cur_hour else None,
            "today_orders": today_hour_orders.get(h, 0) if h <= cur_hour else None,
            "yest_hour": y,
            "yest_cum": cum_y,
            "vs_pct": _pct(cum_t, cum_y) if h <= cur_hour else None,
            "is_now": h == cur_hour,
            "is_future": h > cur_hour,
        })

    # ----- 신규: 요일별 평균 매출 (최근 8일 데이터로) -----
    DAYS_KO_FULL = ["월", "화", "수", "목", "금", "토", "일"]
    dow_buckets = [[] for _ in range(7)]  # 0=월 ~ 6=일
    for (aid, d), m in by_key.items():
        if d == today:
            continue  # 오늘은 진행중이라 제외
        sales = m.get("매출")
        if sales is None:
            continue
        wd = datetime.strptime(d, "%Y-%m-%d").weekday()
        dow_buckets[wd].append(sales)
    dow_summary = []
    today_dow = datetime.now().weekday()
    for i in range(7):
        b = dow_buckets[i]
        # 전 계정 매출 합계 평균 - 일별로 모든 계정 합 후 평균
        # 위 구조는 계정별 매출이라 일별 합으로 변환 필요
        pass

    # 일별 합계 — 단, "활성 계정이 충분한 일자"만 포함해야 부분 데이터로 평균이 왜곡되지 않음.
    # 기준: 매출 > 0 인 계정이 전체 계정의 절반 이상 → 정상 일자로 간주.
    total_accts = len(accounts)
    min_active = max(1, total_accts // 2)
    date_total = {}
    date_active_count = {}
    for (aid, d), m in by_key.items():
        if d == today:
            continue
        date_total[d] = date_total.get(d, 0) + (m.get("매출") or 0)
        if (m.get("매출") or 0) > 0:
            date_active_count[d] = date_active_count.get(d, 0) + 1
    dow_day_buckets = [[] for _ in range(7)]
    for d, total in date_total.items():
        if date_active_count.get(d, 0) < min_active:
            continue  # 부분 데이터/빈 일자 제외
        wd = datetime.strptime(d, "%Y-%m-%d").weekday()
        dow_day_buckets[wd].append(total)
    max_dow_avg = 0
    for i in range(7):
        b = dow_day_buckets[i]
        avg_v = round(sum(b) / len(b)) if b else None
        if avg_v and avg_v > max_dow_avg:
            max_dow_avg = avg_v
        dow_summary.append({
            "dow": DAYS_KO_FULL[i],
            "is_today": i == today_dow,
            "avg": avg_v,
            "n": len(b),
        })
    for d in dow_summary:
        d["pct"] = round(d["avg"] / max_dow_avg * 100) if d["avg"] and max_dow_avg else 0

    # ----- 매장별 시간별 누적 매출 표 (강화) -----
    acct_hourly_cum = []
    # 전체 합계 행 누적
    total_cells_hourly = [0] * 24       # 시간당
    total_cells_cum = [0] * 24          # 누적
    total_yest_hourly = [0] * 24
    total_yest_cum = [0] * 24
    total_orders_hourly = [0] * 24      # 시간당 구매건수

    for r in rows:  # 매출 desc 정렬 그대로 사용
        aid = r["id"]
        today_hours_map = by_acct_date_hour.get(aid, {}).get(today, {})
        yest_hours_map = by_acct_date_hour.get(aid, {}).get(yesterday, {})
        today_orders_map = by_acct_date_hour_orders.get(aid, {}).get(today, {})
        cells = []
        cum_t = 0
        cum_y = 0
        max_hour_val = max(today_hours_map.values()) if today_hours_map else 0
        for h in range(24):
            t = today_hours_map.get(h, 0)
            y = yest_hours_map.get(h, 0)
            cum_t += t
            cum_y += y
            total_cells_hourly[h] += t if h <= cur_hour else 0
            total_yest_hourly[h] += y
            cells.append({
                "hour": h,
                "today_hour": t if h <= cur_hour else None,
                "today_cum": cum_t if h <= cur_hour else None,
                "today_orders": today_orders_map.get(h, 0) if h <= cur_hour else None,
                "yest_hour": y,
                "yest_cum": cum_y,
                "vs_pct": _pct(cum_t, cum_y) if (cum_y and h <= cur_hour) else None,
                "vs_hour_pct": _pct(t, y) if (y and h <= cur_hour) else None,
                "is_now": h == cur_hour,
                "is_future": h > cur_hour,
                # heatmap intensity: 그 매장 안에서 그 시간 매출이 최대 대비 비율 (0~100)
                "heat": round(t / max_hour_val * 100) if (max_hour_val and h <= cur_hour) else 0,
            })
        acct_hourly_cum.append({
            "id": aid,
            "label": r["label"],
            "cells": cells,
            "today_total": r["today"]["매출"] or 0,
            "yest_total": r["yesterday"]["매출"] or 0,
            "expected_eod": r.get("expected_eod"),
            "max_hour": max_hour_val,  # sparkline 정규화용
        })

    # 합계 행 cumulative
    ct = 0; cy = 0
    total_row = []
    max_total_hour = max(total_cells_hourly[:cur_hour+1]) if cur_hour >= 0 else 0
    for h in range(24):
        ct += total_cells_hourly[h] if h <= cur_hour else 0
        cy += total_yest_hourly[h]
        total_row.append({
            "hour": h,
            "today_hour": total_cells_hourly[h] if h <= cur_hour else None,
            "today_cum": ct if h <= cur_hour else None,
            "yest_hour": total_yest_hourly[h],
            "yest_cum": cy,
            "vs_pct": _pct(ct, cy) if (cy and h <= cur_hour) else None,
            "vs_hour_pct": _pct(total_cells_hourly[h], total_yest_hourly[h]) if (total_yest_hourly[h] and h <= cur_hour) else None,
            "is_now": h == cur_hour,
            "is_future": h > cur_hour,
            "heat": round(total_cells_hourly[h] / max_total_hour * 100) if (max_total_hour and h <= cur_hour) else 0,
        })

    # ----- 신규: 전환 깔때기 (방문 → 회원가입 → 구매) -----
    funnel_total = {
        "visitors": mega["today_visitors"],
        "signups": mega["today_signup"],
        "orders": mega["today_orders"],
        "v2s_pct": round(mega["today_signup"] / mega["today_visitors"] * 100, 2) if mega["today_visitors"] else None,
        "s2o_pct": round(mega["today_orders"] / mega["today_signup"] * 100, 1) if mega["today_signup"] else None,
        "v2o_pct": round(mega["today_orders"] / mega["today_visitors"] * 100, 2) if mega["today_visitors"] else None,
    }
    # 매장별 깔때기 — top 5 매출 매장만
    funnel_top = []
    for r in rows[:6]:  # 매출 desc 정렬에서 top 6 (gogofpahs 같은 0 매장 제외 위해 6)
        v = r["today"]["방문자수"] or 0
        s = r["today"]["회원가입"] or 0
        o = r["today"]["구매건수"] or 0
        if v == 0:
            continue
        funnel_top.append({
            "label": r["label"],
            "v": v, "s": s, "o": o,
            "v2s": round(s / v * 100, 2) if v else None,
            "s2o": round(o / s * 100, 1) if s else None,
            "v2o": round(o / v * 100, 2) if v else None,
        })

    # ----- 신규: 매장 효율 랭킹 -----
    # RPV (방문당 매출), 전환율, 객단가 → 정규화 후 종합 점수 (0~100)
    eff_rows = []
    for r in rows:
        v = r["today"]["방문자수"] or 0
        s = r["today"]["매출"] or 0
        o = r["today"]["구매건수"] or 0
        aov = r["today"]["객단가"] or 0
        conv = r["today"]["전환율"] or 0
        rpv = round(s / v) if v else 0  # 방문당 매출
        if s == 0:
            continue
        eff_rows.append({
            "label": r["label"],
            "id": r["id"],
            "sales": s,
            "visitors": v,
            "orders": o,
            "rpv": rpv,
            "conv": conv,
            "aov": aov,
        })
    # 정규화 점수 (각 지표 최대값 100, 가중 평균)
    max_rpv = max((e["rpv"] for e in eff_rows), default=1) or 1
    max_conv = max((e["conv"] for e in eff_rows), default=1) or 1
    max_aov = max((e["aov"] for e in eff_rows), default=1) or 1
    for e in eff_rows:
        norm_rpv = e["rpv"] / max_rpv * 100
        norm_conv = e["conv"] / max_conv * 100
        norm_aov = e["aov"] / max_aov * 100
        # RPV 가장 중요 (40%), 전환율 30%, 객단가 30%
        e["score"] = round(norm_rpv * 0.4 + norm_conv * 0.3 + norm_aov * 0.3, 1)
    eff_rows.sort(key=lambda x: x["score"], reverse=True)
    for i, e in enumerate(eff_rows):
        e["rank"] = i + 1

    # ----- 신규: 이상 매장 감지 (7일 평균 동시각 대비) -----
    anomalies = []
    # 7일간 같은 시각까지 평균 매출 vs 오늘 매출
    for r in rows:
        aid = r["id"]
        today_at_h = sum(by_acct_date_hour.get(aid, {}).get(today, {}).get(h, 0) for h in range(cur_hour + 1))
        # 7일 평균 0~cur_hour 누적 (해당 일자만)
        vals = []
        for d in last7_dates:
            day_map = by_acct_date_hour.get(aid, {}).get(d)
            if not day_map:
                continue
            day_at_h = sum(day_map.get(h, 0) for h in range(cur_hour + 1))
            if day_at_h > 0:
                vals.append(day_at_h)
        if len(vals) < 3:
            continue
        avg7 = sum(vals) / len(vals)
        if avg7 < 100_000:  # 너무 작은 매장은 제외 (분모 작아서 % 폭주)
            continue
        diff_pct = (today_at_h - avg7) / avg7 * 100
        if diff_pct <= -30:
            anomalies.append({
                "type": "down",
                "label": r["label"],
                "today_at_h": today_at_h,
                "avg7": round(avg7),
                "diff_pct": round(diff_pct, 1),
            })
        elif diff_pct >= 50:
            anomalies.append({
                "type": "up",
                "label": r["label"],
                "today_at_h": today_at_h,
                "avg7": round(avg7),
                "diff_pct": round(diff_pct, 1),
            })
    anomalies.sort(key=lambda x: abs(x["diff_pct"]), reverse=True)

    # ----- 신규: 신규 vs 재구매 매출 비중 (매장별) -----
    # 매출 = 처음구매건수 × 객단가 + 재구매건수 × 객단가 (객단가 동일 가정)
    # 정확하진 않지만 추정 (카페24가 처음/재구매 매출 분리 안 줌)
    buyer_mix = []
    for r in rows:
        first = r["today"]["처음구매"] or 0
        repeat = r["today"]["재구매"] or 0
        total_buyers = first + repeat
        sales = r["today"]["매출"] or 0
        if total_buyers == 0 or sales == 0:
            continue
        first_pct = first / total_buyers * 100
        repeat_pct = repeat / total_buyers * 100
        # 추정 매출 (단순 비율 배분)
        est_first_sales = round(sales * first / total_buyers)
        est_repeat_sales = round(sales * repeat / total_buyers)
        buyer_mix.append({
            "label": r["label"],
            "first": first,
            "repeat": repeat,
            "first_pct": round(first_pct, 1),
            "repeat_pct": round(repeat_pct, 1),
            "est_first_sales": est_first_sales,
            "est_repeat_sales": est_repeat_sales,
            "total_sales": sales,
        })
    buyer_mix.sort(key=lambda x: x["total_sales"], reverse=True)

    # ----- 신규: 알람 (주의 필요) -----
    alerts = []
    # -1) 오늘 라이브 0회 (활성 시간인데 데이터 없음) — 가장 critical
    try:
        live_set = db.get_live_settings()
        active_now = live_set["interval_min"] > 0 and live_set["start_hour"] <= cur_hour < live_set["end_hour"]
        with db.db_conn() as conn:
            today_with_data = conn.execute(
                "SELECT COUNT(*) FROM metrics WHERE date=? AND 매출>0", (today,)
            ).fetchone()[0]
            today_runs = conn.execute(
                "SELECT COUNT(*) FROM runs WHERE started_at >= ? AND status='success'", (today + " 00:00:00",)
            ).fetchone()[0]
        if active_now and today_with_data == 0:
            alerts.insert(0, {
                "type": "critical",
                "label": "🚨 오늘 라이브 0회",
                "msg": f"활성 시간({cur_hour}시)인데 오늘 매출 있는 계정 0개. 라이브 사이클이 못 돌고 있음 — 서비스/스케줄러 확인 필요",
            })
        elif active_now and today_with_data < mega.get("total_accounts", 0) // 2:
            # 절반 미만이면 부분 미수집
            alerts.insert(0, {
                "type": "partial",
                "label": "오늘 라이브 부분 미수집",
                "msg": f"활성 시간이지만 {today_with_data}계정만 수집됨. 곧 자동 catch-up 또는 다음 라이브 사이클에서 채워질 예정",
            })
    except Exception:
        traceback.print_exc()
    # -0.5) cafe24 Premium 만료/샘플데이터 (가장 critical — 데이터 자체가 안 들어옴)
    try:
        label_by_id = {a["id"]: (a.get("label") or a["id"]) for a in all_accounts}
        with db.db_conn() as conn:
            sample_rows = conn.execute(
                "SELECT DISTINCT account_id FROM runs "
                "WHERE status='error' AND error LIKE '%sample%' "
                "AND started_at >= ? ORDER BY account_id",
                (today + " 00:00:00",)
            ).fetchall()
        for r in sample_rows:
            aid = r["account_id"]
            lbl = label_by_id.get(aid, aid)
            alerts.insert(0, {
                "type": "critical",
                "label": f"🚨 {lbl} Premium 만료?",
                "msg": f"cafe24가 샘플(데모) 데이터 반환 — 애널리틱스 Premium 구독 만료/권한 문제 가능. "
                       f"`{aid}` 데이터 수집 중단됨 (백필 불가, cafe24 구독 확인 필요)",
            })
    except Exception:
        traceback.print_exc()
    # 0) 서버 startup cleanup (직전 stuck run 들 자동 정리됨)
    if _startup_cleanup_info["count"] > 0:
        alerts.append({
            "type": "stuck",
            "label": "스크래퍼 hang 복구",
            "msg": f"{_startup_cleanup_info['count']}건 stuck running 정리됨 ({', '.join(_startup_cleanup_info['accounts'])}) · {_startup_cleanup_info['at']}",
        })
    # 0-2) DB 에 status='error' 이면서 최근 1시간 내 timeout 으로 종료된 run
    try:
        with db.db_conn() as conn:
            recent_timeouts = conn.execute("""
                SELECT account_id, started_at, error
                FROM runs
                WHERE status='error'
                  AND finished_at >= datetime('now','localtime','-1 hour')
                  AND (error LIKE '%timeout%' OR error LIKE '%hang%' OR error LIKE '%강제 종료%')
                ORDER BY id DESC LIMIT 5
            """).fetchall()
        for r in recent_timeouts:
            alerts.append({
                "type": "timeout",
                "label": f"{r['account_id']} 타임아웃",
                "msg": "8분 watchdog 발동 — chromium 강제 종료됨",
            })
    except Exception:
        pass
    # 1) 예상 종일이 어제 종일보다 -20% 이상 하락 예상
    for r in rows:
        if r["expected_eod"] and r["yesterday"]["매출"]:
            diff = (r["expected_eod"] - r["yesterday"]["매출"]) / r["yesterday"]["매출"] * 100
            if diff <= -20:
                alerts.append({
                    "type": "down",
                    "label": r["label"],
                    "msg": f"예상 종일 {r['expected_eod']:,}원, 어제 대비 {diff:+.1f}%",
                })
    # 2) 미수집 (오늘 매출 0이고 시간 7시 이후)
    if cur_hour >= 7:
        for r in rows:
            if (r["today"]["매출"] or 0) == 0:
                alerts.append({
                    "type": "missing",
                    "label": r["label"],
                    "msg": f"오늘 데이터 없음 (현재 {cur_hour}시)",
                })
    # 3) 캡솔버 실패 5회 이상 today
    cs_today = (db.capsolver_stats() or {}).get("today", {})
    if (cs_today.get("fail") or 0) >= 5:
        alerts.append({
            "type": "capsolver",
            "label": "CapSolver",
            "msg": f"오늘 실패 {cs_today.get('fail')}회 (성공률 {cs_today.get('success_pct')}%)",
        })

    # ----- 신규: 운영 상태 -----
    live_settings = db.get_live_settings()
    daily_settings = db.get_daily_finalize_settings()
    last_live_run = None
    last_runs = db.list_runs(limit=1)
    if last_runs:
        last_live_run = last_runs[0]
    # 다음 라이브 시각 계산
    next_live = None
    if live_settings["interval_min"] > 0:
        # APScheduler interval 기반 다음 실행은 정확히 알 수 없어 추정 (현재 시각 + interval)
        next_live = (now + timedelta(minutes=live_settings["interval_min"])).strftime("%H:%M")
    # 다음 시트 기록 시각 (내일 daily_finalize_hour:minute)
    next_daily_dt = now.replace(hour=daily_settings["hour"], minute=daily_settings["minute"], second=0, microsecond=0)
    if next_daily_dt <= now:
        next_daily_dt = next_daily_dt + timedelta(days=1)
    # 오늘 success run 카운트 (전체 진단 강화)
    try:
        with db.db_conn() as conn:
            today_success_runs = conn.execute(
                "SELECT COUNT(*) FROM runs WHERE started_at >= ? AND status='success'", (today + " 00:00:00",)
            ).fetchone()[0]
            today_failed_runs = conn.execute(
                "SELECT COUNT(*) FROM runs WHERE started_at >= ? AND status='error'", (today + " 00:00:00",)
            ).fetchone()[0]
    except Exception:
        today_success_runs = 0
        today_failed_runs = 0

    # cron 모드면 다음 라이브를 정확히 계산 (다음 정각 또는 분기)
    next_live_label = None
    if live_settings["interval_min"] > 0:
        if live_settings["start_hour"] <= cur_hour < live_settings["end_hour"]:
            interval = live_settings["interval_min"]
            if 60 % interval == 0:
                cur_m = datetime.now().minute
                next_min_slot = ((cur_m // interval) + 1) * interval
                if next_min_slot >= 60:
                    next_h = cur_hour + 1
                    next_min_slot = 0
                else:
                    next_h = cur_hour
                if next_h >= live_settings["end_hour"]:
                    next_live_label = f"내일 {live_settings['start_hour']:02d}:00"
                else:
                    next_live_label = f"{next_h:02d}:{next_min_slot:02d}"
            else:
                next_live_label = (now + timedelta(minutes=interval)).strftime("%H:%M") + " (대략)"
        else:
            next_live_label = f"활성시간({live_settings['start_hour']}시 부터)"

    # 스케줄 잡 목록 (이름/주기/다음실행) — 화면 표시용
    _job_labels = {
        "live_global": "라이브 스크랩 (오늘 매트릭+상품)",
        "daily_finalize": "일일 정리 (어제 확정+시트+상품7일/전일)",
        "meta_collect": "메타 광고 수집 (최근 4일)",
        "db_backup": "DB 백업",
        "daily_restart": "정기 재시작",
        "heartbeat": "self-check",
    }
    jobs_status = []
    try:
        for j in scheduler.get_jobs():
            if j.id not in _job_labels:
                continue
            nxt = j.next_run_time.strftime("%m/%d %H:%M") if j.next_run_time else "-"
            jobs_status.append({"id": j.id, "label": _job_labels.get(j.id, j.id), "next": nxt})
        order = {k: i for i, k in enumerate(["live_global", "daily_finalize", "meta_collect", "db_backup", "daily_restart", "heartbeat"])}
        jobs_status.sort(key=lambda x: order.get(x["id"], 99))
    except Exception:
        traceback.print_exc()

    ops_status = {
        "last_run": last_live_run,
        "next_live": next_live_label,
        "next_daily": next_daily_dt.strftime("%m/%d %H:%M"),
        "live_interval": live_settings["interval_min"],
        "live_active": live_settings["start_hour"] <= cur_hour < live_settings["end_hour"],
        "today_success_runs": today_success_runs,
        "today_failed_runs": today_failed_runs,
        "meta_last": db.get_setting("meta_last_run", None),
        "jobs": jobs_status,
    }

    # ----- 메타 → 시트 자동입력 현황 (전용 카드용) -----
    meta_next = None
    try:
        for j in scheduler.get_jobs():
            if j.id == "meta_collect" and j.next_run_time:
                meta_next = j.next_run_time.strftime("%m/%d %H:%M")
    except Exception:
        pass
    meta_accounts = []
    n_connected = 0
    for a in all_accounts:
        mid = (a.get("meta_account_id") or "").strip()
        if mid:
            n_connected += 1
        meta_accounts.append({
            "label": a.get("label") or a["cafe24_id"],
            "connected": bool(mid),
            "meta_id": mid,
            "last": db.get_setting(f"meta_last_{a['id']}", None),
            "has_sheet": bool(a.get("spreadsheet_id")),
        })
    meta_status = {
        "next_run": meta_next,
        "last_run": db.get_setting("meta_last_run", None),
        "connected": n_connected,
        "total": len(all_accounts),
        "accounts": meta_accounts,
        "backfill_days": META_BACKFILL_DAYS,
        "fill_log": db.list_sheet_log(limit=30),
    }

    # ----- 광고 효율(ROAS) — 메타 광고비 vs 매출 (선택 매장, 오늘/어제) -----
    # 오늘은 진행중(부분), 어제는 확정. 둘 다 제공하고 화면서 탭.
    label_by_id = {a["id"]: (a.get("label") or a["cafe24_id"]) for a in all_accounts}
    meta_rows_all = db.list_meta_metrics(account_ids=selected_ids, start_date=last_week_same, end_date=today)
    meta_by_key = {(r["account_id"], r["date"]): r for r in meta_rows_all}

    def _build_ad_eff(ad_date):
        out = []
        tot = {"spend": 0, "ad_rev": 0, "sales": 0, "purch": 0, "imp": 0, "clk": 0,
               "reach": 0, "lpv": 0, "atc": 0, "ic": 0, "link_clk": 0}
        for aid in selected_ids:
            mm = meta_by_key.get((aid, ad_date))
            if not mm:
                continue
            spend = mm["spend_vat"] or 0
            spend_raw = mm["spend"] or 0
            ad_rev = mm["revenue"] or 0
            sales = (by_key.get((aid, ad_date), {}) or {}).get("매출") or 0
            imp = mm["impressions"] or 0
            clk = mm["clicks"] or 0
            purch = mm["purchases"] or 0
            roas = round(ad_rev / spend * 100) if spend else None
            broas = round(sales / spend * 100) if spend else None
            dep = round(spend / sales * 100, 1) if sales else None
            out.append({
                "label": label_by_id.get(aid, aid), "id": aid,
                "spend": spend, "ad_rev": ad_rev, "roas": roas,
                "sales": sales, "broas": broas, "dep": dep,
                "purch": purch, "imp": imp, "clk": clk,
                # 효율 지표
                "cpm": round(spend / imp * 1000) if imp else None,
                "cpc": round(spend / clk) if clk else None,
                "ctr": round(clk / imp * 100, 2) if imp else None,
                "freq": mm.get("frequency") or 0,
                "cpa": round(spend / purch) if purch else None,
                "reach": mm.get("reach") or 0,
                # 퍼널
                "link_clk": mm.get("link_clicks") or 0, "lpv": mm.get("lpv") or 0,
                "atc": mm.get("atc") or 0, "ic": mm.get("ic") or 0,
            })
            tot["spend"] += spend; tot["ad_rev"] += ad_rev; tot["sales"] += sales
            tot["purch"] += purch; tot["imp"] += imp; tot["clk"] += clk
            tot["reach"] += mm.get("reach") or 0; tot["lpv"] += mm.get("lpv") or 0
            tot["atc"] += mm.get("atc") or 0; tot["ic"] += mm.get("ic") or 0
            tot["link_clk"] += mm.get("link_clicks") or 0
        out.sort(key=lambda x: x["spend"], reverse=True)
        tot["roas"] = round(tot["ad_rev"] / tot["spend"] * 100) if tot["spend"] else None
        tot["broas"] = round(tot["sales"] / tot["spend"] * 100) if tot["spend"] else None
        tot["dep"] = round(tot["spend"] / tot["sales"] * 100, 1) if tot["sales"] else None
        tot["cpm"] = round(tot["spend"] / tot["imp"] * 1000) if tot["imp"] else None
        tot["cpc"] = round(tot["spend"] / tot["clk"]) if tot["clk"] else None
        tot["ctr"] = round(tot["clk"] / tot["imp"] * 100, 2) if tot["imp"] else None
        tot["cpa"] = round(tot["spend"] / tot["purch"]) if tot["purch"] else None
        return out, tot

    ad_eff_yday, ad_eff_yday_tot = _build_ad_eff(yesterday)
    ad_eff_today, ad_eff_today_tot = _build_ad_eff(today)
    ad_eff = {
        "yesterday": {"date": yesterday, "rows": ad_eff_yday, "tot": ad_eff_yday_tot},
        "today": {"date": today, "rows": ad_eff_today, "tot": ad_eff_today_tot},
        "has_data": bool(ad_eff_yday or ad_eff_today),
    }

    # 캠페인별 성과 (어제) — 광고비 큰 순
    ad_campaigns = []
    for c in db.list_meta_campaigns(account_ids=selected_ids, date=yesterday):
        sp = c["spend_vat"] or 0
        rev = c["revenue"] or 0
        ad_campaigns.append({
            "store": label_by_id.get(c["account_id"], c["account_id"]),
            "name": c["campaign_name"] or "(이름없음)",
            "spend": sp, "rev": rev,
            "roas": round(rev / sp * 100) if sp else None,
            "purch": c["purchases"] or 0,
        })
    ad_campaigns.sort(key=lambda x: x["spend"], reverse=True)
    ad_campaigns = ad_campaigns[:20]

    # 광고(소재)별 성과 (어제) — 광고비 큰 순 top
    ad_creatives = []
    for c in db.list_meta_ads(account_ids=selected_ids, date=yesterday):
        sp = c["spend_vat"] or 0
        rev = c["revenue"] or 0
        ad_creatives.append({
            "store": label_by_id.get(c["account_id"], c["account_id"]),
            "name": c["ad_name"] or "(이름없음)", "campaign": c["campaign_name"] or "",
            "spend": sp, "rev": rev, "roas": round(rev / sp * 100) if sp else None,
            "purch": c["purchases"] or 0,
        })
    ad_creatives.sort(key=lambda x: x["spend"], reverse=True)
    ad_creatives = ad_creatives[:20]

    # ROAS 추세 (선택매장 합계, 최근 8일) — 일별 광고비/광고매출/ROAS
    ad_trend = []
    trend_dates = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7, -1, -1)]
    for d in trend_dates:
        sp = sum((meta_by_key.get((aid, d), {}) or {}).get("spend_vat") or 0 for aid in selected_ids)
        rev = sum((meta_by_key.get((aid, d), {}) or {}).get("revenue") or 0 for aid in selected_ids)
        ad_trend.append({"date": d[5:], "spend": sp, "rev": rev,
                         "roas": round(rev / sp * 100) if sp else None})

    # 주간/월간 광고 요약 (WoW / MoM) — 선택매장 합계
    _mrows = db.list_meta_metrics(account_ids=selected_ids,
                                  start_date=(now - timedelta(days=70)).strftime("%Y-%m-%d"),
                                  end_date=today)
    _mday = {}
    for r in _mrows:
        _mday[r["date"]] = (r["spend_vat"] or 0, r["revenue"] or 0, r["purchases"] or 0)

    def _sum_range(d_from, d_to):
        sp = rv = pu = 0
        cur = d_from
        while cur <= d_to:
            ds = cur.strftime("%Y-%m-%d")
            if ds in _mday:
                sp += _mday[ds][0]; rv += _mday[ds][1]; pu += _mday[ds][2]
            cur += timedelta(days=1)
        return {"spend": sp, "rev": rv, "purch": pu, "roas": round(rv / sp * 100) if sp else None}

    def _delta(cur, prev):
        if not prev:
            return None
        return round((cur - prev) / prev * 100, 1)

    yday = now - timedelta(days=1)
    # 주간: 최근 7일(어제까지) vs 이전 7일
    wk_this = _sum_range(yday - timedelta(days=6), yday)
    wk_last = _sum_range(yday - timedelta(days=13), yday - timedelta(days=7))
    # 월간: 이번달 1일~어제 vs 지난달 1일~지난달 같은 일자
    month_start = now.replace(day=1)
    mtd_this = _sum_range(month_start, yday)
    last_month_end = month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    lm_same = last_month_start + timedelta(days=(yday - month_start).days)
    if lm_same > last_month_end:
        lm_same = last_month_end
    mtd_last = _sum_range(last_month_start, lm_same)
    ad_summary = {
        "week": {"this": wk_this, "last": wk_last,
                 "spend_d": _delta(wk_this["spend"], wk_last["spend"]),
                 "rev_d": _delta(wk_this["rev"], wk_last["rev"]),
                 "roas_d": _delta(wk_this["roas"] or 0, wk_last["roas"] or 0) if wk_last["roas"] else None},
        "month": {"this": mtd_this, "last": mtd_last,
                  "this_label": now.strftime("%m월 1~%d일") % () if False else f"{now.month}월 1~{yday.day}일",
                  "last_label": f"{last_month_start.month}월 1~{lm_same.day}일",
                  "spend_d": _delta(mtd_this["spend"], mtd_last["spend"]),
                  "rev_d": _delta(mtd_this["rev"], mtd_last["rev"]),
                  "roas_d": _delta(mtd_this["roas"] or 0, mtd_last["roas"] or 0) if mtd_last["roas"] else None},
        "has_data": bool(_mday),
    }

    # ===== 네이버 검색광고 대시보드 (API 연동분) =====
    naver_rows_all = db.list_naver_metrics(
        account_ids=selected_ids,
        start_date=(now - timedelta(days=8)).strftime("%Y-%m-%d"), end_date=today)
    naver_by_key = {(r["account_id"], r["date"]): r for r in naver_rows_all}
    naver_connected = [a["id"] for a in all_accounts if a["id"] in selected_ids
                       and (a.get("naver_api_key") or "").strip() and (a.get("naver_customer_id") or "").strip()]

    def _build_naver_eff(d):
        out = []
        tot = {"imp": 0, "clk": 0, "cost": 0, "conv": 0, "rev": 0, "sales": 0}
        for aid in selected_ids:
            nm = naver_by_key.get((aid, d))
            if not nm:
                continue
            imp = nm["impressions"] or 0; clk = nm["clicks"] or 0; cost = nm["cost"] or 0
            rev = nm["revenue"] or 0
            conv_raw = nm["conversions"]  # 당일 구매완료 집계전이면 None → '집계중' 표시
            conv = conv_raw or 0
            sales = (by_key.get((aid, d), {}) or {}).get("매출") or 0
            out.append({
                "label": label_by_id.get(aid, aid), "id": aid,
                "imp": imp, "clk": clk, "cost": cost, "conv": conv_raw, "rev": rev, "sales": sales,
                "ctr": round(clk / imp * 100, 2) if imp else None,
                "cpc": round(cost / clk) if clk else None,
                "cvr": round(conv / clk * 100, 2) if (clk and conv_raw is not None) else None,
                "cpa": round(cost / conv) if (conv and conv_raw is not None) else None,
                "roas": round(rev / cost * 100) if cost else None,
                "broas": round(sales / cost * 100) if cost else None,
            })
            tot["imp"] += imp; tot["clk"] += clk; tot["cost"] += cost
            tot["conv"] += conv; tot["rev"] += rev; tot["sales"] += sales
            if conv_raw is not None:
                tot["_any_conv"] = True
        out.sort(key=lambda x: x["cost"], reverse=True)
        tot["ctr"] = round(tot["clk"] / tot["imp"] * 100, 2) if tot["imp"] else None
        tot["cpc"] = round(tot["cost"] / tot["clk"]) if tot["clk"] else None
        # 당일처럼 구매완료 집계전(전 매장 None)이면 전환/CVR/CPA 는 '집계중'(None)
        if not tot.get("_any_conv"):
            tot["conv"] = None; tot["cvr"] = None; tot["cpa"] = None
        else:
            tot["cvr"] = round(tot["conv"] / tot["clk"] * 100, 2) if tot["clk"] else None
            tot["cpa"] = round(tot["cost"] / tot["conv"]) if tot["conv"] else None
        tot["roas"] = round(tot["rev"] / tot["cost"] * 100) if tot["cost"] else None
        tot["broas"] = round(tot["sales"] / tot["cost"] * 100) if tot["cost"] else None
        return out, tot

    ne_y, ne_yt = _build_naver_eff(yesterday)
    ne_t, ne_tt = _build_naver_eff(today)
    naver_eff = {
        "yesterday": {"date": yesterday, "rows": ne_y, "tot": ne_yt},
        "today": {"date": today, "rows": ne_t, "tot": ne_tt},
        "connected": len(naver_connected),
        "has_data": bool(ne_y or ne_t),
    }
    # 8일 추세 (선택매장 합계)
    naver_trend = []
    for d in trend_dates:
        cost = sum((naver_by_key.get((aid, d), {}) or {}).get("cost") or 0 for aid in selected_ids)
        rev = sum((naver_by_key.get((aid, d), {}) or {}).get("revenue") or 0 for aid in selected_ids)
        clk = sum((naver_by_key.get((aid, d), {}) or {}).get("clicks") or 0 for aid in selected_ids)
        naver_trend.append({"date": d[5:], "cost": cost, "rev": rev, "clk": clk,
                            "roas": round(rev / cost * 100) if cost else None})

    # ===== 크리테오 · 네이버 성과형(GFA) — 네이버검색과 동일 스키마(imp/clk/cost/conv/rev) =====
    def _build_simple_eff(src_by_key, d):
        out = []
        tot = {"imp": 0, "clk": 0, "cost": 0, "conv": 0, "rev": 0, "sales": 0}
        for aid in selected_ids:
            m = src_by_key.get((aid, d))
            if not m:
                continue
            imp = m["impressions"] or 0; clk = m["clicks"] or 0; cost = m["cost"] or 0
            rev = m["revenue"] or 0; conv = m["conversions"] or 0
            sales = (by_key.get((aid, d), {}) or {}).get("매출") or 0
            out.append({
                "label": label_by_id.get(aid, aid), "id": aid,
                "imp": imp, "clk": clk, "cost": cost, "conv": conv, "rev": rev, "sales": sales,
                "ctr": round(clk / imp * 100, 2) if imp else None,
                "cpc": round(cost / clk) if clk else None,
                "cpa": round(cost / conv) if conv else None,
                "roas": round(rev / cost * 100) if cost else None,
                "broas": round(sales / cost * 100) if cost else None,
            })
            tot["imp"] += imp; tot["clk"] += clk; tot["cost"] += cost
            tot["conv"] += conv; tot["rev"] += rev; tot["sales"] += sales
        out.sort(key=lambda x: x["cost"], reverse=True)
        tot["ctr"] = round(tot["clk"] / tot["imp"] * 100, 2) if tot["imp"] else None
        tot["cpc"] = round(tot["cost"] / tot["clk"]) if tot["clk"] else None
        tot["cpa"] = round(tot["cost"] / tot["conv"]) if tot["conv"] else None
        tot["roas"] = round(tot["rev"] / tot["cost"] * 100) if tot["cost"] else None
        tot["broas"] = round(tot["sales"] / tot["cost"] * 100) if tot["cost"] else None
        return out, tot

    def _eff_bundle(src_by_key, connected_n):
        y, yt = _build_simple_eff(src_by_key, yesterday)
        t, tt = _build_simple_eff(src_by_key, today)
        return {"yesterday": {"date": yesterday, "rows": y, "tot": yt},
                "today": {"date": today, "rows": t, "tot": tt},
                "connected": connected_n, "has_data": bool(y or t)}

    def _simple_trend(src_by_key):
        tr = []
        for d in trend_dates:
            cost = sum((src_by_key.get((aid, d), {}) or {}).get("cost") or 0 for aid in selected_ids)
            rev = sum((src_by_key.get((aid, d), {}) or {}).get("revenue") or 0 for aid in selected_ids)
            clk = sum((src_by_key.get((aid, d), {}) or {}).get("clicks") or 0 for aid in selected_ids)
            tr.append({"date": d[5:], "cost": cost, "rev": rev, "clk": clk,
                       "roas": round(rev / cost * 100) if cost else None})
        return tr

    _window_start = (now - timedelta(days=8)).strftime("%Y-%m-%d")
    criteo_by_key = {(r["account_id"], r["date"]): r for r in
                     db.list_criteo_metrics(account_ids=selected_ids, start_date=_window_start, end_date=today)}
    gfa_by_key = {(r["account_id"], r["date"]): r for r in
                  db.list_gfa_metrics(account_ids=selected_ids, start_date=_window_start, end_date=today)}
    criteo_conn = sum(1 for a in all_accounts if a["id"] in selected_ids and (a.get("criteo_advertiser_id") or "").strip())
    gfa_conn = sum(1 for a in all_accounts if a["id"] in selected_ids and (a.get("naver_gfa_account_no") or "").strip())
    criteo_eff = _eff_bundle(criteo_by_key, criteo_conn)
    gfa_eff = _eff_bundle(gfa_by_key, gfa_conn)
    criteo_trend = _simple_trend(criteo_by_key)
    gfa_trend = _simple_trend(gfa_by_key)

    # 쇼핑박스 — (account,date)별 PC+MO 합산 (device 차원 제거)
    shopbox_by_key = {}
    for r in db.list_shopbox_metrics(account_ids=selected_ids, start_date=_window_start, end_date=today):
        agg = shopbox_by_key.setdefault((r["account_id"], r["date"]), {"cost": 0, "revenue": 0, "impressions": 0, "clicks": 0})
        agg["cost"] += r.get("cost") or 0
        agg["revenue"] += r.get("revenue") or 0
        agg["impressions"] += r.get("impressions") or 0
        agg["clicks"] += r.get("clicks") or 0
    shopbox_conn = sum(1 for a in all_accounts if a["id"] in selected_ids and db.list_shopbox_bids([a["id"]]))

    # ===== 전 채널 한눈 요약 — 어제 + 통합(블렌디드) + 전주동요일 대비 + MTD =====
    _mt = ad_eff["yesterday"]["tot"]; _nt = naver_eff["yesterday"]["tot"]
    _ct = criteo_eff["yesterday"]["tot"]; _gt = gfa_eff["yesterday"]["tot"]

    def _ch_daysum(src, ck, rk, d):
        c = sum((src.get((aid, d), {}) or {}).get(ck) or 0 for aid in selected_ids)
        r = sum((src.get((aid, d), {}) or {}).get(rk) or 0 for aid in selected_ids)
        return c, r

    def _ch_fieldsum(src, field, d):
        return sum((src.get((aid, d), {}) or {}).get(field) or 0 for aid in selected_ids)

    def _pct_delta(cur, prev):
        return round((cur - prev) / prev * 100) if prev else None

    _last7 = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(6, -1, -1)]

    # 채널: (표시명, 색, by_key, 광고비필드, 매출필드, 연동수, 어제전환)
    # 메타는 'spend'(부가세 미포함) 사용 — 타 채널(cost)과 기준 통일해 통합지표 왜곡 방지.
    _CH = [
        ("메타", "#1877f2", meta_by_key, "spend", "revenue", None, _mt.get("purch")),
        ("네이버 검색", "#03c75a", naver_by_key, "cost", "revenue", naver_eff["connected"], _nt.get("conv")),
        ("크리테오", "#f76b1c", criteo_by_key, "cost", "revenue", criteo_eff["connected"], _ct.get("conv")),
        ("네이버 성과형", "#2e7d32", gfa_by_key, "cost", "revenue", gfa_eff["connected"], _gt.get("conv")),
        ("쇼핑박스", "#7e57c2", shopbox_by_key, "cost", "revenue", shopbox_conn, None),
    ]
    _mstart = now.replace(day=1).strftime("%Y-%m-%d")
    _mtd_fn = {"메타": db.list_meta_metrics, "네이버 검색": db.list_naver_metrics,
               "크리테오": db.list_criteo_metrics, "네이버 성과형": db.list_gfa_metrics,
               "쇼핑박스": db.list_shopbox_metrics}
    _cs_rows = []
    for name, color, src, ck, rk, conn, conv in _CH:
        cy, ry = _ch_daysum(src, ck, rk, yesterday)
        cw, rw = _ch_daysum(src, ck, rk, last_week_same)
        clk_y = _ch_fieldsum(src, "clicks", yesterday)
        imp_y = _ch_fieldsum(src, "impressions", yesterday)
        mrows = _mtd_fn[name](account_ids=selected_ids, start_date=_mstart, end_date=today)
        mc = sum(r.get(ck) or 0 for r in mrows); mr = sum(r.get(rk) or 0 for r in mrows)
        _cs_rows.append({
            "name": name, "color": color, "cost": cy, "rev": ry, "conv": conv,
            "roas": round(ry / cy * 100) if cy else None, "connected": conn,
            "wow_cost": _pct_delta(cy, cw), "wow_rev": _pct_delta(ry, rw),
            "mtd_cost": mc, "mtd_rev": mr, "mtd_roas": round(mr / mc * 100) if mc else None,
            "cpc": round(cy / clk_y) if clk_y else None,
            "cpm": round(cy / imp_y * 1000) if imp_y else None,
            "cpa": round(cy / conv) if conv else None,
            "spark": [_ch_daysum(src, ck, rk, d)[0] for d in _last7],
        })
    _cs_tc = sum(r["cost"] for r in _cs_rows); _cs_tr = sum(r["rev"] for r in _cs_rows)
    _cs_tc_w = sum(_ch_daysum(s, ck, rk, last_week_same)[0] for _, _, s, ck, rk, _, _ in _CH)
    _mtd_tc = sum(r["mtd_cost"] for r in _cs_rows); _mtd_tr = sum(r["mtd_rev"] for r in _cs_rows)
    # 매장 전체매출(블렌디드 분모) — 광고매출 합산은 채널 중복집계라, 통합지표는 매장 실매출 기준
    _srev_y = sum((by_key.get((aid, yesterday), {}) or {}).get("매출") or 0 for aid in selected_ids)
    _srev_w = sum((by_key.get((aid, last_week_same), {}) or {}).get("매출") or 0 for aid in selected_ids)
    _srev_m = sum((m.get("매출") or 0) for m in db.list_metrics(start_date=_mstart, end_date=today)
                  if m["account_id"] in selected_ids)
    channel_summary = {
        "date": yesterday, "last_week": last_week_same, "rows": _cs_rows,
        "tot": {"cost": _cs_tc, "rev": _cs_tr, "roas": round(_cs_tr / _cs_tc * 100) if _cs_tc else None,
                "wow_cost": _pct_delta(_cs_tc, _cs_tc_w)},
        # 통합(블렌디드): 전체 광고비 ÷ 전체 매장 실매출
        "blended": {
            "ad_spend": _cs_tc, "store_rev": _srev_y,
            "broas": round(_srev_y / _cs_tc * 100) if _cs_tc else None,
            "dep": round(_cs_tc / _srev_y * 100, 1) if _srev_y else None,
            "broas_w": round(_srev_w / _cs_tc_w * 100) if _cs_tc_w else None,
        },
        "mtd": {"cost": _mtd_tc, "rev": _mtd_tr, "store_rev": _srev_m,
                "broas": round(_srev_m / _mtd_tc * 100) if _mtd_tc else None,
                "dep": round(_mtd_tc / _srev_m * 100, 1) if _srev_m else None},
        "has_data": _cs_tc > 0 or _cs_tr > 0,
    }

    # ===== 쇼핑박스 PC/MO 상세 (노출·클릭·CTR·CPC·광고비·매출·ROAS) =====
    _sb_label = {a["id"]: (a.get("label") or a["id"]) for a in accounts}

    def _sb_blank():
        return {"imp": 0, "clk": 0, "cost": 0, "rev": 0}

    def _sb_eff(d):
        d["ctr"] = round(d["clk"] / d["imp"] * 100, 2) if d["imp"] else None
        d["cpc"] = round(d["cost"] / d["clk"]) if d["clk"] else None
        d["cpm"] = round(d["cost"] / d["imp"] * 1000) if d["imp"] else None
        d["roas"] = round(d["rev"] / d["cost"] * 100) if d["cost"] else None
        return d

    def _sb_agg_dev(rows):
        out = {"pc": _sb_blank(), "mo": _sb_blank()}
        for r in rows:
            dev = out.get(r["device"])
            if dev is None:
                continue
            dev["imp"] += r.get("impressions") or 0
            dev["clk"] += r.get("clicks") or 0
            dev["cost"] += r.get("cost") or 0
            dev["rev"] += r.get("revenue") or 0
        for dev in out.values():
            _sb_eff(dev)
        return out

    _sb_rows_t = db.list_shopbox_metrics(account_ids=selected_ids, start_date=today, end_date=today)
    _sb_rows_y = db.list_shopbox_metrics(account_ids=selected_ids, start_date=yesterday, end_date=yesterday)
    _sb_rows_m = db.list_shopbox_metrics(account_ids=selected_ids, start_date=_mstart, end_date=today)
    _sb_t = _sb_agg_dev(_sb_rows_t)
    _sb_y = _sb_agg_dev(_sb_rows_y)
    _sb_m = _sb_agg_dev(_sb_rows_m)
    # 매장별(어제) — device별 행, 활동 있는 매장만
    _sb_store = {}
    for r in _sb_rows_y:
        if not ((r.get("impressions") or 0) or (r.get("clicks") or 0) or (r.get("cost") or 0) or (r.get("revenue") or 0)):
            continue
        st = _sb_store.setdefault(r["account_id"], {})
        dev = st.setdefault(r["device"], _sb_blank())
        dev["imp"] += r.get("impressions") or 0
        dev["clk"] += r.get("clicks") or 0
        dev["cost"] += r.get("cost") or 0
        dev["rev"] += r.get("revenue") or 0
    _sb_store_rows = []
    for aid, devs in _sb_store.items():
        for dv in ("pc", "mo"):
            if dv in devs:
                _sb_store_rows.append({"label": _sb_label.get(aid, aid), "device": dv.upper(), **_sb_eff(devs[dv])})
    _sb_store_rows.sort(key=lambda x: -(x["rev"] or 0))
    _sb7 = {d: {"imp": 0, "rev": 0} for d in _last7}
    for r in db.list_shopbox_metrics(account_ids=selected_ids, start_date=_last7[0], end_date=_last7[-1]):
        if r["date"] in _sb7:
            _sb7[r["date"]]["imp"] += r.get("impressions") or 0
            _sb7[r["date"]]["rev"] += r.get("revenue") or 0
    shopbox_detail = {
        "date": yesterday, "today_date": today, "t": _sb_t, "y": _sb_y, "m": _sb_m, "stores": _sb_store_rows,
        "has_data": bool(_sb_rows_t or _sb_rows_y or _sb_rows_m),
        "last_run": db.get_setting("shopbox_last_run", None),
        "spark_imp": [_sb7[d]["imp"] for d in _last7],
        "spark_rev": [_sb7[d]["rev"] for d in _last7],
        "spark_days": [d[5:] for d in _last7],
    }

    # 매장별 월 매출목표 vs 이번달 누적(MTD) 달성률
    month_start_s = now.replace(day=1).strftime("%Y-%m-%d")
    days_in_month = ((now.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)).day
    day_of_month = now.day
    mtd_rows = db.list_metrics(start_date=month_start_s, end_date=today)
    mtd_sales = {}
    for r in mtd_rows:
        if r["account_id"] in selected_ids:
            mtd_sales[r["account_id"]] = mtd_sales.get(r["account_id"], 0) + (r.get("매출") or 0)
    goals = []
    for a in accounts:
        aid = a["id"]
        try:
            goal = int(db.get_setting(f"goal_{aid}", "0") or 0)
        except ValueError:
            goal = 0
        if goal <= 0:
            continue
        actual = mtd_sales.get(aid, 0)
        # 진행 기대치 = 목표 × (경과일/총일수) — 페이스 판단용
        expected = goal * day_of_month / days_in_month
        goals.append({
            "label": a.get("label") or aid, "id": aid,
            "goal": goal, "actual": actual,
            "pct": round(actual / goal * 100, 1) if goal else 0,
            "pace": round(actual / expected * 100) if expected else None,  # 100=정상페이스
            "projected": round(actual / day_of_month * days_in_month) if day_of_month else 0,
        })
    goals.sort(key=lambda x: x["pct"], reverse=True)

    # 광고 알림 — 어제 기준: ROAS 급락(7일평균 대비), 빈도 과다, 광고의존도 과다
    ad_alerts = []
    for r in ad_eff_yday:
        aid = r["id"]
        # 최근 7일(어제 제외 이전) 평균 ROAS
        prev = []
        for i in range(2, 9):
            d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            mm = meta_by_key.get((aid, d))
            if mm and (mm["spend_vat"] or 0) > 0:
                prev.append((mm["revenue"] or 0) / mm["spend_vat"] * 100)
        avg_roas = sum(prev) / len(prev) if prev else None
        if r["roas"] is not None and avg_roas and avg_roas > 0 and r["roas"] < avg_roas * 0.7:
            ad_alerts.append({"label": r["label"], "msg": f"ROAS 급락: {r['roas']}% (7일평균 {avg_roas:.0f}% 대비 -30%↓)"})
        if r["freq"] and r["freq"] >= 3:
            ad_alerts.append({"label": r["label"], "msg": f"광고 피로도 높음: 빈도 {r['freq']} (같은 사람 반복 노출)"})
        if r["dep"] is not None and r["dep"] >= 60:
            ad_alerts.append({"label": r["label"], "msg": f"광고 의존도 높음: {r['dep']}% (매출 대부분이 광고)"})

    # 메타 외 채널(네이버검색·크리테오·GFA)도 ROAS 급락 감지 — 어제 vs 직전 7일평균 -30%↓
    def _roas_drop_alerts(eff_rows, src_by_key, ch_label):
        for r in eff_rows:
            aid = r["id"]
            if r.get("roas") is None or (r.get("cost") or 0) <= 0:
                continue
            prev = []
            for i in range(2, 9):
                d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
                m = src_by_key.get((aid, d))
                if m and (m["cost"] or 0) > 0:
                    prev.append((m["revenue"] or 0) / m["cost"] * 100)
            avg = sum(prev) / len(prev) if prev else None
            if avg and avg > 0 and r["roas"] < avg * 0.7:
                ad_alerts.append({"label": f"{r['label']} · {ch_label}",
                                  "msg": f"{ch_label} ROAS 급락: {r['roas']}% (7일평균 {avg:.0f}% 대비 -30%↓)"})

    _roas_drop_alerts(ne_y, naver_by_key, "네이버검색")
    _roas_drop_alerts(criteo_eff["yesterday"]["rows"], criteo_by_key, "크리테오")
    _roas_drop_alerts(gfa_eff["yesterday"]["rows"], gfa_by_key, "네이버성과형")

    # 실행 제안(자동 인사이트) — 어제 기준 + 목표 ROAS 대비
    target_roas = int(db.get_setting("target_roas", "300") or 300)
    ad_insights = []
    for r in ad_eff_yday:
        if r["roas"] is None:
            continue
        if r["roas"] >= target_roas * 1.4 and r["spend"] > 0:
            ad_insights.append({"kind": "up", "label": r["label"],
                "msg": f"ROAS {r['roas']}% (목표 {target_roas}% 크게 상회) → 광고비 증액 검토 (현재 {r['spend']:,}원)"})
        elif r["roas"] < target_roas:
            ad_insights.append({"kind": "down", "label": r["label"],
                "msg": f"ROAS {r['roas']}% < 목표 {target_roas}% → 소재/타겟 점검 또는 감액"})
        if r["freq"] and r["freq"] >= 3:
            ad_insights.append({"kind": "fatigue", "label": r["label"],
                "msg": f"빈도 {r['freq']} → 소재 교체 권장 (같은 사람 반복 노출, 효율 하락 신호)"})
        if r["atc"] and r["atc"] >= 10 and r["purch"] is not None:
            a2p = r["purch"] / r["atc"] * 100
            if a2p < 20:
                ad_insights.append({"kind": "funnel", "label": r["label"],
                    "msg": f"장바구니→구매 {a2p:.0f}% 낮음 → 결제/상세페이지 이탈 점검"})
    # 메타 외 채널(네이버검색·크리테오·GFA)도 증액/감액 인사이트 (목표 ROAS 대비)
    def _channel_insights(eff_rows, ch_label):
        for r in eff_rows:
            if r.get("roas") is None or (r.get("cost") or 0) <= 0:
                continue
            if r["roas"] >= target_roas * 1.4:
                ad_insights.append({"kind": "up", "label": f"{r['label']} · {ch_label}",
                    "msg": f"{ch_label} ROAS {r['roas']}% (목표 {target_roas}% 크게 상회) → 광고비 증액 검토 (현재 {r['cost']:,}원)"})
            elif r["roas"] < target_roas:
                ad_insights.append({"kind": "down", "label": f"{r['label']} · {ch_label}",
                    "msg": f"{ch_label} ROAS {r['roas']}% < 목표 {target_roas}% → 소재/타겟 점검 또는 감액"})
    _channel_insights(ne_y, "네이버검색")
    _channel_insights(criteo_eff["yesterday"]["rows"], "크리테오")
    _channel_insights(gfa_eff["yesterday"]["rows"], "네이버성과형")

    # 정렬: 기회(up) 먼저, 그다음 경고
    _order = {"up": 0, "down": 1, "fatigue": 2, "funnel": 3}
    ad_insights.sort(key=lambda x: _order.get(x["kind"], 9))

    # ----- 신규: 매장별 7일 트렌드 (평균/최고/최저/추세) -----
    # 매출 0인 일자는 미수집(백필 안 한 일자)일 가능성이 커서 트렌드 왜곡됨 → 제외.
    # 진짜 영업 안 한 0원 일자가 있다면 분석에서 빠지지만 가짜 0 포함보다 안전.
    trend_rows = []
    for a in accounts:
        aid = a["id"]
        vals = []
        for d in last7_dates:
            m = by_key.get((aid, d))
            if m and (m.get("매출") or 0) > 0:
                vals.append((d, m["매출"]))
        if not vals:
            trend_rows.append({"label": a.get("label") or a["cafe24_id"], "id": aid, "avg": None, "best": None, "worst": None, "trend": None, "n": 0})
            continue
        sales_vals = [v for _, v in vals]
        avg_v = round(sum(sales_vals) / len(sales_vals))
        t_best = max(vals, key=lambda x: x[1])
        t_worst = min(vals, key=lambda x: x[1])
        # 추세: 앞 3일 평균 vs 뒤 3일 평균
        if len(vals) >= 4:
            half = len(vals) // 2
            first_avg = sum(v for _, v in vals[:half]) / half
            last_avg = sum(v for _, v in vals[half:]) / (len(vals) - half)
            trend_pct = _pct(last_avg, first_avg)
        else:
            trend_pct = None
        # 오늘은 진행중이라 종일 평균과 직접 비교하면 misleading.
        # 페이스 보정해서 예상 종일을 평균과 비교 (그 계정의 expected_eod 사용).
        today_sales_now = by_key.get((aid, today), {}).get("매출")
        # 현재 row 의 expected_eod 가져오기 (rows 에 있는 동일 aid 찾기)
        expected_for_aid = None
        for rr in rows:
            if rr["id"] == aid:
                expected_for_aid = rr.get("expected_eod")
                break
        trend_rows.append({
            "label": a.get("label") or a["cafe24_id"],
            "id": aid,
            "avg": avg_v,
            "best_date": t_best[0],
            "best_v": t_best[1],
            "worst_date": t_worst[0],
            "worst_v": t_worst[1],
            "trend": trend_pct,
            "n": len(vals),
            "today_sales": today_sales_now,
            "expected_eod": expected_for_aid,
        })
    trend_rows.sort(key=lambda r: r["avg"] or 0, reverse=True)

    capsolver = db.capsolver_stats()
    capsolver["balance"] = scraper.capsolver_balance()

    # ---- 카페24 상품 분석 ----
    # 데이터 모델: daily(date=실제날짜) → 오늘=date=today, 전일=date=yesterday / 7d(date=수집일).
    # 탭 키: 'today'|'yesterday'|'7d'.  product_by_period = {탭: {category: {account_id: [rows]}}}
    product_by_period = {}
    product_periods = []
    product_dates = {}     # 탭 → 표시 날짜
    product_collect_date = None

    def _group_product(prows):
        d = {}
        for prow in prows:
            d.setdefault(prow["category"], {}).setdefault(prow["account_id"], []).append(prow)
        return d

    try:
        # 오늘 / 전일 = daily 시계열에서 해당 날짜
        for tab, d in (("today", today), ("yesterday", yesterday)):
            grouped = _group_product(db.list_product_metrics(account_id=selected_ids, date=d, period="daily"))
            if grouped:
                product_by_period[tab] = grouped
                product_periods.append(tab)
                product_dates[tab] = d
        # 최근7일 추세 = period='7d' 의 가장 최근 수집일
        with db.db_conn() as conn:
            r = conn.execute(
                "SELECT MAX(date) FROM product_metrics WHERE period='7d' AND account_id IN ({})".format(
                    ",".join(["?"] * len(selected_ids))),
                selected_ids,
            ).fetchone()
            d7 = r[0] if r else None
        if d7:
            grouped = _group_product(db.list_product_metrics(account_id=selected_ids, date=d7, period="7d"))
            if grouped:
                product_by_period["7d"] = grouped
                product_periods.append("7d")
                product_dates["7d"] = d7
        product_collect_date = product_dates.get(product_periods[0]) if product_periods else None
    except Exception:
        traceback.print_exc()
    product_data = product_by_period.get(product_periods[0], {}) if product_periods else {}

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
        hourly_trend=hourly_trend,
        acct_hourly_cum=acct_hourly_cum,
        total_row=total_row,
        funnel_total=funnel_total,
        funnel_top=funnel_top,
        eff_rows=eff_rows,
        anomalies=anomalies,
        buyer_mix=buyer_mix,
        cur_hour=cur_hour,
        all_accounts=all_accounts,
        selected_ids=selected_ids,
        dow_summary=dow_summary,
        alerts=alerts,
        validate_warnings=[{"id": k, **v} for k, v in _validate_warnings.items()
                           if v.get("date") in (today, yesterday)],
        ops_status=ops_status,
        trend_rows=trend_rows,
        today=today,
        yesterday=yesterday,
        last_week_same=last_week_same,
        capsolver=capsolver,
        product_data=product_data,
        product_by_period=product_by_period,
        product_periods=product_periods,
        product_dates=product_dates,
        product_collect_date=product_collect_date,
        freshness=_build_freshness(selected_ids, today),
        recent_gaps=[{"label": _label_map().get(aid, aid), "date": d}
                     for aid, d in _recent_gaps(days=3)],
        product_running=list(_running.keys()),
        meta_status=meta_status,
        ad_eff=ad_eff,
        ad_campaigns=ad_campaigns,
        ad_creatives=ad_creatives,
        ad_trend=ad_trend,
        ad_alerts=ad_alerts,
        ad_insights=ad_insights,
        target_roas=target_roas,
        ad_summary=ad_summary,
        naver_eff=naver_eff,
        naver_trend=naver_trend,
        criteo_eff=criteo_eff,
        criteo_trend=criteo_trend,
        gfa_eff=gfa_eff,
        gfa_trend=gfa_trend,
        channel_summary=channel_summary,
        shopbox_detail=shopbox_detail,
        goals=goals,
        goal_month=now.month,
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

    # 광고(메타) 기간 요약 — 선택기간 vs 직전 동일기간
    def _meta_sum(s, e):
        rows = db.list_meta_metrics(account_ids=selected_ids, start_date=s, end_date=e)
        sp = sum(r["spend_vat"] or 0 for r in rows)
        rv = sum(r["revenue"] or 0 for r in rows)
        pu = sum(r["purchases"] or 0 for r in rows)
        return {"spend": sp, "rev": rv, "purch": pu, "roas": round(rv / sp * 100) if sp else None}
    _ad_cur = _meta_sum(start_str, end_str)
    _ad_prev = _meta_sum(prev_start_str, prev_end_str)
    def _pd(c, p):
        return round((c - p) / p * 100, 1) if p else None
    ad_range = {
        "cur": _ad_cur, "prev": _ad_prev,
        "spend_d": _pd(_ad_cur["spend"], _ad_prev["spend"]),
        "rev_d": _pd(_ad_cur["rev"], _ad_prev["rev"]),
        "roas_d": _pd(_ad_cur["roas"] or 0, _ad_prev["roas"] or 0) if _ad_prev["roas"] else None,
        "has_data": _ad_cur["spend"] > 0 or _ad_prev["spend"] > 0,
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
        ad_range=ad_range,
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
    # Slack 피드백 채널로 알림
    preview = body if len(body) <= 500 else body[:500] + "…"
    slack_notify(
        f"새 피드백 등록 *#{thread.get('id', '?')}* by `{user}`\n>>> {preview}",
        severity="feedback",
        webhook_url=SLACK_FEEDBACK_WEBHOOK_URL,
    )
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
    preview = body if len(body) <= 300 else body[:300] + "…"
    slack_notify(
        f"피드백 *#{fid}* 답글 by `{user}`\n>>> {preview}",
        severity="feedback",
        webhook_url=SLACK_FEEDBACK_WEBHOOK_URL,
    )
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


# 우리가 채우는 시트/탭 전체 정의
# 우리가 채우는 모든 영역(=시트의 모든 채널)을 한 표로. 채널마다 한 줄.
#   way:    "api"(공식 API) | "crawler"(API 없음, 크롤러로 가능) | "formula"(시트 수식) | "none"(자동화 부적합)
#   status: "live"(지금 자동 작동중) | "manual"(지금 수동 입력중)
#   group:  화면 구분용 라벨 (cafe24 / 효율)
SHEET_TARGETS = [
    # ── cafe24 자연 지표 탭 ──
    {"tab": "월별 탭 (26년N월)", "group": "cafe24 월별탭", "what": "cafe24 자연 지표",
     "fields": "매출·방문자·신규/재방문·구매건수·전환율·객단가·회원가입 (B~U 18칸)",
     "way": "crawler", "status": "live", "last_key": None,  # metrics 최신으로 대체
     "need": "—  (크롤러 가동 중, 추가 불필요)"},

    # ── 효율탭: 지금 자동 작동 중 ──
    {"tab": "메타 (FB/IG)", "group": "효율탭 광고채널", "what": "메타 광고성과",
     "fields": "노출·클릭·광고비(+VAT)·전환·매출",
     "way": "api", "status": "live", "last_key": "meta_last_run",
     "need": "—  (토큰 발급 완료, 연결 매장만)"},
    {"tab": "네이버 검색광고", "group": "효율탭 광고채널", "what": "네이버 검색광고",
     "fields": "노출·클릭·광고비(VAT없음)·전환·매출",
     "way": "api", "status": "live", "last_key": "naver_last_run",
     "need": "나머지 매장 API키·시크릿·CUSTOMER_ID (계정관리에서 입력)"},

    # ── 효율탭: 세션 크롤로 자동 작동 중 ──
    {"tab": "크리테오", "group": "효율탭 광고채널", "what": "크리테오",
     "fields": "노출·클릭·광고비·전환·매출 (클릭후7일)",
     "way": "crawler", "status": "live", "last_key": "criteo_last_run",
     "need": "—  (세션 크롤 가동 중, 월 1회 재로그인 / advertiser ID 입력 매장만)"},

    # ── 효율탭: 공식 API 있음 → 연동하면 자동화 (지금은 수기) ──
    {"tab": "구글", "group": "효율탭 광고채널", "what": "구글 광고",
     "fields": "노출·클릭·광고비·전환·매출",
     "way": "api", "status": "manual", "last_key": None,
     "need": "Google Ads API developer token + OAuth"},
    {"tab": "틱톡", "group": "효율탭 광고채널", "what": "틱톡 광고",
     "fields": "노출·클릭·광고비·전환·매출",
     "way": "api", "status": "manual", "last_key": None,
     "need": "TikTok for Business 앱 + access token"},
    {"tab": "카카오 DA", "group": "효율탭 광고채널", "what": "카카오 디스플레이",
     "fields": "노출·클릭·광고비·전환·매출",
     "way": "api", "status": "manual", "last_key": None,
     "need": "카카오모먼트 API 키 + 광고계정 ID"},
    {"tab": "카카오 모객", "group": "효율탭 광고채널", "what": "카카오 모객",
     "fields": "노출·클릭·광고비·전환·매출",
     "way": "api", "status": "manual", "last_key": None,
     "need": "카카오모먼트 API 키"},
    {"tab": "카카오 메세지", "group": "효율탭 광고채널", "what": "카카오 메시지",
     "fields": "발송·클릭·광고비·전환·매출",
     "way": "api", "status": "manual", "last_key": None,
     "need": "카카오모먼트 API 키"},

    {"tab": "네이버 성과형(GFA)", "group": "효율탭 광고채널", "what": "네이버 디스플레이",
     "fields": "노출·클릭·광고비·전환(구매완료)·매출(구매완료전환매출액)",
     "way": "crawler", "status": "live", "last_key": "gfa_last_run",
     "need": "—  (세션 크롤 가동 중, 권한 승인된 매장만 / 월 1회 재로그인)"},

    # ── 효율탭: 공식 API 없음/제한 → 크롤러로 가능 ──
    {"tab": "네이버 쇼핑박스 PC", "group": "효율탭 광고채널", "what": "쇼핑박스 PC",
     "fields": "노출·클릭·광고비·전환·매출",
     "way": "crawler", "status": "manual", "last_key": None,
     "need": "공식 API 없음 → 보장형 관리자 크롤러 구축 필요"},
    {"tab": "네이버 쇼핑박스 MO (트렌드픽)", "group": "효율탭 광고채널", "what": "쇼핑박스 모바일",
     "fields": "노출·클릭·광고비·전환·매출",
     "way": "crawler", "status": "manual", "last_key": None,
     "need": "공식 API 없음 → 관리자 크롤러 구축 필요"},
    {"tab": "다음 쇼핑박스", "group": "효율탭 광고채널", "what": "다음 쇼핑박스",
     "fields": "노출·클릭·광고비·전환·매출",
     "way": "crawler", "status": "manual", "last_key": None,
     "need": "공식 API 없음 → 카카오 보장형 관리자 크롤러 필요"},
    {"tab": "네이트 CPC", "group": "효율탭 광고채널", "what": "네이트 CPC",
     "fields": "노출·클릭·광고비·전환·매출",
     "way": "crawler", "status": "manual", "last_key": None,
     "need": "공식 API 없음 → 관리자 크롤러 검토"},

    # ── 효율탭: 소액/비주력 → 수기 유지 ──
    {"tab": "모비온", "group": "효율탭 광고채널", "what": "모비온 리타겟팅",
     "fields": "광고비·매출",
     "way": "none", "status": "manual", "last_key": None,
     "need": "소액·비주력 → 수기 유지 (필요시 크롤러 검토)"},
    {"tab": "아이센드", "group": "효율탭 광고채널", "what": "아이센드 메시지",
     "fields": "발송·광고비·매출",
     "way": "none", "status": "manual", "last_key": None,
     "need": "소액 → 수기 유지"},

    # ── 효율탭: 자동화 부적합 / 수식 ──
    {"tab": "채널별 카페24 전환매출", "group": "효율탭 합계/전환", "what": "채널별 전환매출",
     "fields": "매출·ROAS",
     "way": "none", "status": "manual", "last_key": None,
     "need": "cafe24 멀티채널 7일기여·멀티터치 → 일별 정확도 낮음, 자동화 부적합"},
    {"tab": "Total 합계", "group": "효율탭 합계/전환", "what": "전체 합계",
     "fields": "노출·클릭·광고비·매출·ROAS",
     "way": "formula", "status": "live", "last_key": None,
     "need": "—  (시트 SUM 수식, 자동 계산)"},
]


# cafe24 월별 탭에 우리가 자동 기입하는 칸 (write_result 기준, 열 → 지표)
CAFE24_SHEET_MAP = [
    {"col": "B", "name": "매출", "mode": "auto", "note": "매출종합(당일 확정/실시간)"},
    {"col": "C", "name": "(목표/비교)", "mode": "formula", "note": "시트 수식·수기"},
    {"col": "D", "name": "방문자수", "mode": "auto", "note": "전체방문자수"},
    {"col": "E", "name": "방문당매출", "mode": "auto", "note": "매출/방문"},
    {"col": "F", "name": "신규방문", "mode": "auto", "note": "전체방문자 분해"},
    {"col": "G", "name": "재방문", "mode": "auto", "note": "전체방문자 분해"},
    {"col": "H", "name": "순방문자수", "mode": "auto", "note": "순방문자수"},
    {"col": "I", "name": "순방문비중", "mode": "auto", "note": "%"},
    {"col": "J", "name": "신규비중", "mode": "auto", "note": "%"},
    {"col": "K", "name": "재방문비중", "mode": "auto", "note": "%"},
    {"col": "L", "name": "구매건수", "mode": "auto", "note": "매출종합"},
    {"col": "M", "name": "전환율", "mode": "auto", "note": "%"},
    {"col": "N", "name": "구매개수", "mode": "auto", "note": "매출종합"},
    {"col": "O", "name": "합구매", "mode": "auto", "note": "처음+재구매"},
    {"col": "P", "name": "처음구매", "mode": "auto", "note": "처음구매vs재구매"},
    {"col": "Q", "name": "처음구매비중", "mode": "auto", "note": "%"},
    {"col": "R", "name": "재구매", "mode": "auto", "note": "처음구매vs재구매"},
    {"col": "S", "name": "(여백/수기)", "mode": "manual", "note": "비고 등"},
    {"col": "T", "name": "객단가", "mode": "auto", "note": "1인당매출(AOV)"},
    {"col": "U", "name": "회원가입", "mode": "auto", "note": "신규회원수"},
]



# 채널명 → 입력 모드 (셀 색칠용)
CHANNEL_MODE = {
    "Total": "formula", "카페24": "formula",
    "메타": "auto", "네이버 검색광고": "auto",
    "네이버성과형": "manual", "네이버 쇼핑박스 PC": "manual",
    "네이버 쇼핑박스 MO (트렌드픽)": "manual", "다음 쇼핑박스": "manual",
    "틱톡": "manual", "카카오 DA": "manual", "카카오 모객": "manual",
    "카카오 메세지": "manual", "아이센드": "manual", "모비온": "manual",
    "네이트CPC": "manual", "구글": "todo",
    "크리테오": "todo",
}

# 효율시트 채널×지표 레이아웃은 '고정 구조'다. 한 번 추출해 DB(settings)에 스냅샷으로 저장하고,
# 페이지는 그 스냅샷 + 우리 DB값(메타/네이버)만으로 즉시 렌더. 시트 재읽기는 새로고침(?refresh=1) 때만.
_EFF_LAYOUT_KEY = "eff_layout_snapshot"
# 지표 라벨 → DB 필드 (자동 채널만 값 채움. CTR/CPC/CVR/ROAS/객단가는 시트 수식이라 비움)
_EFF_METRIC_FIELD = {"노출량(Imps)": "imp", "클릭수(Clicks)": "clk", "광고비": "cost",
                     "전환수": "conv", "매출": "rev"}
_EFF_AUTO_SRC = {"메타": "meta", "네이버 검색광고": "naver"}


def _refresh_eff_layout():
    """효율시트 헤더 2줄을 읽어 채널×지표 레이아웃 스냅샷을 DB에 저장. 새로고침 때만 호출."""
    sample = db.get_account("cinderella1009") or (db.list_accounts()[0] if db.list_accounts() else None)
    ssid = sample.get("spreadsheet_id") if sample else None
    if not ssid:
        raise RuntimeError("기준 매장 시트 미설정")
    gc = sheets.get_client()
    sh = gc.open_by_key(sheets.clean_spreadsheet_id(ssid))
    eff = sheets.efficiency_sheet_name(datetime.now().strftime("%Y-%m-%d"))
    try:
        ws = sh.worksheet(eff)
    except Exception:
        ws = sh.worksheet(sheets.efficiency_sheet_name((datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")))
    bat = ws.batch_get(["A38:ZZ38", "A39:ZZ39"])
    chan = bat[0][0] if bat and bat[0] else []
    met = bat[1][0] if len(bat) > 1 and bat[1] else []
    starts = [(i, v.strip()) for i, v in enumerate(chan) if v.strip() and v.strip() != "구분"]
    layout = []
    for k, (ci, name) in enumerate(starts):
        end = starts[k + 1][0] if k + 1 < len(starts) else len(met)
        mets = [met[c].strip() for c in range(ci, end) if c < len(met) and met[c].strip()]
        if mets:
            layout.append({"name": name, "metrics": mets})
    snap = {"layout": layout, "tab": ws.title, "ts": datetime.now().strftime("%Y-%m-%d %H:%M")}
    db.set_setting(_EFF_LAYOUT_KEY, json.dumps(snap, ensure_ascii=False))
    return snap


def _eff_mini_view(force=False):
    """효율시트 미니뷰 — 저장된 레이아웃 스냅샷 + DB값(메타/네이버 오늘 합계)으로 즉시 렌더.
    시트는 새로고침(force) 또는 스냅샷이 아직 없을 때만 읽음. 반환 (grid|None, err|None)."""
    err = None
    snap = None
    if force:
        try:
            snap = _refresh_eff_layout()
        except Exception as e:
            err = repr(e)[:150]; traceback.print_exc()
    if snap is None:
        raw = db.get_setting(_EFF_LAYOUT_KEY, None)
        if raw:
            try:
                snap = json.loads(raw)
            except Exception:
                snap = None
    if snap is None and err is None:  # 최초 1회 자동 추출
        try:
            snap = _refresh_eff_layout()
        except Exception as e:
            err = repr(e)[:150]; traceback.print_exc()
    if snap is None:
        return None, (err or "레이아웃 스냅샷 없음")

    # 오늘 DB값 (전 매장 합계)
    today = datetime.now().strftime("%Y-%m-%d")
    all_ids = [a["id"] for a in db.list_accounts()]

    def _sum(rows, field):
        return sum(r.get(field) or 0 for r in rows)
    mrows = [r for r in db.list_meta_metrics(account_ids=all_ids, start_date=today, end_date=today)]
    nrows = [r for r in db.list_naver_metrics(account_ids=all_ids, start_date=today, end_date=today)]
    src_vals = {
        "meta": {"imp": _sum(mrows, "impressions"), "clk": _sum(mrows, "clicks"),
                 "cost": _sum(mrows, "spend_vat"), "conv": _sum(mrows, "purchases"), "rev": _sum(mrows, "revenue")},
        "naver": {"imp": _sum(nrows, "impressions"), "clk": _sum(nrows, "clicks"),
                  "cost": _sum(nrows, "cost"), "conv": _sum(nrows, "conversions"), "rev": _sum(nrows, "revenue")},
    }

    def _fmt(v):
        return "{:,.0f}".format(v) if v else ""

    blocks, cells = [], {}
    for b in snap["layout"]:
        name = b["name"]; mets = b["metrics"]
        blocks.append({"name": name, "mode": CHANNEL_MODE.get(name, "manual"), "metrics": mets})
        src = _EFF_AUTO_SRC.get(name)
        vals = []
        for m in mets:
            if src and m in _EFF_METRIC_FIELD:
                vals.append(_fmt(src_vals[src][_EFF_METRIC_FIELD[m]]))
            else:
                vals.append("")  # 수기/미연동/시트수식 칸
        cells[name] = vals
    grid = {"blocks": blocks, "rows": [{"date": today[5:].replace("-", "/") + " (오늘·DB)", "cells": cells}],
            "tab": snap.get("tab", "효율시트"), "snap_ts": snap.get("ts", "")}
    return grid, err


@app.route("/admin/sheet_channels")
@login_required
def sheet_channels():
    """효율시트를 실제 모양 그대로 재현 — 채널×지표 셀을 입력방식 색으로."""
    last = {
        "메타": db.get_setting("meta_last_run", None),
        "네이버 검색광고": db.get_setting("naver_last_run", None),
    }
    conn_rows = []
    for a in db.list_accounts():
        conn_rows.append({
            "id": a["id"],
            "label": a.get("label") or a["cafe24_id"],
            "sheet": bool(a.get("spreadsheet_id")),
            "meta": bool((a.get("meta_account_id") or "").strip()),
            "naver": bool((a.get("naver_api_key") or "").strip() and (a.get("naver_customer_id") or "").strip()),
            "criteo": bool((a.get("criteo_advertiser_id") or "").strip()),
            "gfa": bool((a.get("naver_gfa_account_no") or "").strip()),
            "naver_api_key": a.get("naver_api_key") or "",
            "naver_secret": a.get("naver_secret") or "",
            "naver_customer_id": a.get("naver_customer_id") or "",
        })

    # 효율시트 미니뷰 — 저장된 레이아웃 스냅샷 + DB값으로 즉시 렌더. 시트는 ?refresh=1 때만 읽음.
    sheet_grid, sheet_err = _eff_mini_view(force=bool(request.args.get("refresh")))

    # SHEET_TARGETS 에 마지막 갱신시각 채우기
    today_s = datetime.now().strftime("%Y-%m-%d")
    cafe24_last = None
    try:
        with db.db_conn() as conn:
            r = conn.execute("SELECT MAX(updated_at) FROM metrics WHERE date=?", (today_s,)).fetchone()
            cafe24_last = r[0][5:16] if r and r[0] else None
    except Exception:
        pass
    targets = []
    n_live = n_possible = n_stuck = 0
    for t in SHEET_TARGETS:
        last_at = None
        if t["last_key"]:
            last_at = db.get_setting(t["last_key"], None)
        elif t["status"] == "live" and t["what"].startswith("cafe24"):
            last_at = cafe24_last
        # 분류: live(자동중) / possible(연동·크롤러하면 자동화) / stuck(수기유지)
        if t["status"] == "live":
            cls = "live"; n_live += 1
        elif t["way"] in ("api", "crawler"):
            cls = "possible"; n_possible += 1
        else:
            cls = "stuck"; n_stuck += 1
        targets.append({**t, "last_at": last_at, "cls": cls})
    counts = {"live": n_live, "possible": n_possible, "stuck": n_stuck}

    return render_template("sheet_channels.html",
                           last=last, conn_rows=conn_rows,
                           sheet_grid=sheet_grid, sheet_err=sheet_err,
                           targets=targets, counts=counts, cafe24_map=CAFE24_SHEET_MAP,
                           cafe24_last=cafe24_last, active="channels")


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
