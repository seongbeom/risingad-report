"""세션 갱신 도우미 (맥 백그라운드) — launchd 가 10분마다 실행.

하는 일:
1) EC2 /api/session_guard 폴링 (token 인증)
2) 세션 만료 임박(warn)/만료(critical) → macOS 알림 (12h 쿨다운)
3) 웹에서 '🔄 갱신 요청' 눌린 채널 → 로그인 스크립트 자동 실행(브라우저 창) + 알림 + 서버에 ack
   (ack 로 요청 플래그 해제 → 중복 실행 방지)

stdlib 만 사용(launchd 의 system python3 로 구동). 로그인 창은 repo 의 venv python 으로 띄움.
설정: .env 의 GUARD_TOKEN, GUARD_SERVER_URL. 상태: data/guard_state.json
"""
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, date
from pathlib import Path

REPO = Path(__file__).resolve().parent
ENV = REPO / ".env"
STATE = REPO / "data" / "guard_state.json"
VENV_PY = REPO / "venv" / "bin" / "python3"
NOTIFY_COOLDOWN_H = 12       # 같은 사유 알림 쿨다운
LAUNCH_COOLDOWN_MIN = 15     # 로그인 창 중복 실행 방지


def _read_env():
    cfg = {}
    if ENV.exists():
        for line in ENV.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def _notify(title, msg):
    """macOS 알림 배너."""
    try:
        safe_t = title.replace('"', "'")
        safe_m = msg.replace('"', "'")
        subprocess.run(
            ["osascript", "-e",
             f'display notification "{safe_m}" with title "{safe_t}" sound name "Glass"'],
            check=False, timeout=10)
    except Exception as e:
        print(f"[guard] notify 실패: {e}", flush=True)


def _load_state():
    try:
        return json.loads(STATE.read_text())
    except Exception:
        return {}


def _save_state(s):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(s, ensure_ascii=False, indent=2))


def _http_get(url):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode("utf-8"))


def _http_post(url, data):
    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=body)
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status


def _launch_login(login_py, upload_env):
    """로그인 스크립트를 venv python 으로 실행 (헤디드 브라우저 창이 뜸)."""
    env = dict(os.environ)
    env[upload_env] = "1"  # 로그인 후 EC2 자동 업로드
    py = str(VENV_PY) if VENV_PY.exists() else sys.executable
    subprocess.Popen([py, str(REPO / login_py)], cwd=str(REPO), env=env)


def main():
    cfg = _read_env()
    token = cfg.get("GUARD_TOKEN", "")
    server = cfg.get("GUARD_SERVER_URL", "http://52.79.112.252:9090").rstrip("/")
    if not token:
        print("[guard] GUARD_TOKEN 미설정(.env) — 종료", flush=True)
        return
    try:
        payload = _http_get(f"{server}/api/session_guard?token={urllib.parse.quote(token)}")
    except Exception as e:
        print(f"[guard] 폴링 실패: {e}", flush=True)
        return

    state = _load_state()
    now = time.time()
    now_iso = datetime.now().strftime("%Y-%m-%d %H:%M")

    for ch in payload.get("channels", []):
        key = ch["key"]; name = ch["name"]
        sev = ch.get("severity"); days = ch.get("days_left")

        # 1) 갱신 요청 → 로그인 창 자동 실행
        if ch.get("requested"):
            last_launch = state.get(f"{key}_launched_at", 0)
            if now - last_launch > LAUNCH_COOLDOWN_MIN * 60:
                print(f"[guard] {name} 갱신 요청 감지 → 로그인 창 실행", flush=True)
                _notify(f"{name} 세션 갱신", "로그인 창을 엽니다 — 로그인하면 자동 저장·업로드됩니다.")
                try:
                    _launch_login(ch["login_py"], ch["upload_env"])
                    state[f"{key}_launched_at"] = now
                    # 서버 요청 플래그 해제 (중복 실행 방지)
                    _http_post(f"{server}/api/session_guard/ack?token={urllib.parse.quote(token)}",
                               {"channel": key})
                except Exception as e:
                    print(f"[guard] {name} 로그인 실행 실패: {e}", flush=True)
            continue  # 요청 처리한 채널은 만료알림 생략

        # 2) 만료 임박/만료 → 알림 (쿨다운)
        if sev in ("warn", "critical"):
            last_notify = state.get(f"{key}_notified_at", 0)
            if now - last_notify > NOTIFY_COOLDOWN_H * 3600:
                if sev == "critical":
                    _notify(f"🔴 {name} 세션 만료", f"수집이 끊깁니다 — 관리화면에서 '🔄 갱신 요청' 누르거나 {ch['command']} 더블클릭.")
                else:
                    _notify(f"⚠️ {name} 세션 {days}일 후 만료", "여유 있을 때 갱신하세요 — 관리화면 '🔄 갱신 요청'.")
                state[f"{key}_notified_at"] = now

    state["last_run"] = now_iso
    _save_state(state)
    print(f"[guard] 점검 완료 {now_iso}", flush=True)


if __name__ == "__main__":
    main()
