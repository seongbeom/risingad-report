"""세션 갱신 도우미 (맥 백그라운드) — launchd KeepAlive 로 상시 구동.

원리(롱폴링): EC2 /api/session_guard/wait 를 '열어두면' 서버가 갱신 요청이 들어오는
즉시 응답한다(없으면 ~25초 후 타임아웃 → 곧장 재연결). 그래서 웹 버튼 클릭 → 1초 내 로그인 창.

하는 일:
1) 갱신 요청 감지 → 로그인 스크립트 실행(브라우저 창) + macOS 알림 + 서버 ack(중복 방지)
2) 세션 만료 임박(warn)/만료(critical) → macOS 알림 (12h 쿨다운)

stdlib 만 사용(launchd system python3). 로그인 창은 repo 의 venv python 으로 띄움.
설정: .env 의 GUARD_TOKEN, GUARD_SERVER_URL. 상태: data/guard_state.json
실행: 인자 없으면 상시 롱폴링 루프, '--once' 면 1회만 점검(설치 테스트용).
"""
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent
ENV = REPO / ".env"
STATE = REPO / "data" / "guard_state.json"
VENV_PY = REPO / "venv" / "bin" / "python3"
NOTIFY_COOLDOWN_H = 12       # 같은 사유 알림 쿨다운
LAUNCH_COOLDOWN_MIN = 15     # 로그인 창 중복 실행 방지
ERROR_BACKOFF_SEC = 5        # 연결 실패 시 재시도 간격


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
    try:
        safe_t = title.replace('"', "'"); safe_m = msg.replace('"', "'")
        subprocess.run(["osascript", "-e",
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


def _http_get(url, timeout=35):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _http_post(url, data, timeout=15):
    body = urllib.parse.urlencode(data).encode("utf-8")
    with urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=timeout) as r:
        return r.status


def _launch_login(login_py, upload_env):
    """로그인 스크립트를 venv python 으로 실행(헤디드 브라우저 창)."""
    env = dict(os.environ)
    env[upload_env] = "1"  # 로그인 후 EC2 자동 업로드
    py = str(VENV_PY) if VENV_PY.exists() else sys.executable
    subprocess.Popen([py, str(REPO / login_py)], cwd=str(REPO), env=env)


def _process(channels, server, token):
    """폴링 응답 처리: 요청 채널 로그인 실행 + ack, 만료 임박 알림."""
    state = _load_state()
    now = time.time()
    for ch in channels:
        key = ch["key"]; name = ch["name"]
        sev = ch.get("severity"); days = ch.get("days_left")
        if ch.get("requested"):
            if now - state.get(f"{key}_launched_at", 0) > LAUNCH_COOLDOWN_MIN * 60:
                print(f"[guard] {name} 갱신 요청 감지 → 로그인 창 실행", flush=True)
                _notify(f"{name} 세션 갱신", "로그인 창을 엽니다 — 로그인하면 자동 저장·업로드됩니다.")
                try:
                    _launch_login(ch["login_py"], ch["upload_env"])
                    state[f"{key}_launched_at"] = now
                    _http_post(f"{server}/api/session_guard/ack?token={urllib.parse.quote(token)}",
                               {"channel": key})
                except Exception as e:
                    print(f"[guard] {name} 로그인 실행 실패: {e}", flush=True)
            continue
        if sev in ("warn", "critical"):
            if now - state.get(f"{key}_notified_at", 0) > NOTIFY_COOLDOWN_H * 3600:
                if sev == "critical":
                    _notify(f"🔴 {name} 세션 만료", f"수집이 끊깁니다 — 관리화면 '🔄 갱신 요청' 또는 {ch['command']} 더블클릭.")
                else:
                    _notify(f"⚠️ {name} 세션 {days}일 후 만료", "여유 있을 때 갱신하세요 — 관리화면 '🔄 갱신 요청'.")
                state[f"{key}_notified_at"] = now
    state["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _save_state(state)


def main():
    cfg = _read_env()
    token = cfg.get("GUARD_TOKEN", "")
    server = cfg.get("GUARD_SERVER_URL", "http://52.79.112.252:9090").rstrip("/")
    if not token:
        print("[guard] GUARD_TOKEN 미설정(.env) — 종료", flush=True)
        return
    once = "--once" in sys.argv
    qtoken = urllib.parse.quote(token)

    if once:
        # 설치 테스트용 1회 점검 (롱폴링 아님)
        try:
            payload = _http_get(f"{server}/api/session_guard?token={qtoken}", timeout=15)
            _process(payload.get("channels", []), server, token)
            print(f"[guard] 1회 점검 완료 {datetime.now():%H:%M:%S}", flush=True)
        except Exception as e:
            print(f"[guard] 점검 실패: {e}", flush=True)
        return

    # 상시 롱폴링 루프 (launchd KeepAlive 가 죽으면 되살림)
    print(f"[guard] 롱폴링 시작 → {server}", flush=True)
    while True:
        try:
            payload = _http_get(f"{server}/api/session_guard/wait?token={qtoken}", timeout=35)
            _process(payload.get("channels", []), server, token)
            # 응답 직후 즉시 재연결 (pending 처리했어도 곧바로 다시 열어둠)
        except Exception as e:
            print(f"[guard] 폴링 오류({e}) — {ERROR_BACKOFF_SEC}s 후 재시도", flush=True)
            time.sleep(ERROR_BACKOFF_SEC)


if __name__ == "__main__":
    main()
