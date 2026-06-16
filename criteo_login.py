"""크리테오 세션 갱신 도구 (로컬, 월 1회).

브라우저 한 번 띄워서 로그인하면 → 세션 저장 → EC2 자동 업로드.
영구 프로필(data/criteo_profile/)을 써서 '이 기기 기억'이 유지되므로,
다음 갱신부터는 보통 2FA 없이 통과(이미 로그인돼 있으면 즉시 저장).

사용: criteo_login.command 더블클릭  (또는 venv/bin/python3 criteo_login.py)
"""
import datetime
import json
import subprocess
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).parent
DATA = ROOT / "data"
PROFILE = DATA / "criteo_profile"
SESSION = DATA / "criteo_session.json"
META = DATA / "criteo_session_meta.json"

# EC2 업로드 (deploy.sh 와 동일)
KEY = Path.home() / "cafe24_migration" / "cafe24-new-key.pem"
HOST = "ubuntu@52.79.112.252"
REMOTE_DATA = "/opt/cafe24/data"

START_URL = "https://marketing.criteo.com/"
SESSION_VALID_DAYS = 30


AUTH_MARKERS = ("login.criteo", "okta", "/authorize", "/oauth2", "signin", "/login")


def _logged_in(page):
    """로그인 완료 여부 — 인증(okta/login) 페이지가 아니고 criteo 앱 호스트에 있으면 True."""
    u = (page.url or "").lower()
    if any(x in u for x in AUTH_MARKERS):
        return False
    # 인증 페이지만 아니면 criteo.com 어느 앱 화면이든 로그인된 것으로 간주
    return "criteo.com" in u and "about:blank" not in u


def _upload():
    """세션·메타 파일을 EC2로 scp. 실패해도 로컬엔 저장돼 있으니 안내만."""
    if not KEY.exists():
        print(f"⚠️  SSH 키 없음({KEY}) — EC2 업로드 건너뜀. 로컬엔 저장됨.")
        return False
    ok = True
    for f in (SESSION, META):
        try:
            subprocess.run(
                ["scp", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=12",
                 "-i", str(KEY), str(f), f"{HOST}:{REMOTE_DATA}/{f.name}"],
                check=True, capture_output=True, timeout=60)
        except Exception as e:
            ok = False
            print(f"⚠️  업로드 실패({f.name}): {e}")
    if ok:
        print(f"✅ EC2 업로드 완료 → {HOST}:{REMOTE_DATA}")
    return ok


def main():
    DATA.mkdir(exist_ok=True)
    PROFILE.mkdir(parents=True, exist_ok=True)
    print("브라우저를 띄웁니다. 크리테오에 로그인하세요 (이미 로그인돼 있으면 자동 진행).")
    print("로그인 시 'Keep me logged in for 30 days' 체크 권장.\n")
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(PROFILE), headless=False,
            args=["--disable-blink-features=AutomationControlled"],
            viewport={"width": 1400, "height": 950},
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)

        # 로그인 완료까지 대기 (최대 6분 폴링)
        import time
        deadline = time.time() + 360
        last = None
        while time.time() < deadline:
            cur = page.url
            if cur != last:
                print(f"  현재 위치: {cur}")
                last = cur
            if _logged_in(page):
                page.wait_for_timeout(2000)
                if _logged_in(page):
                    break
            time.sleep(2)
        else:
            print("❌ 6분 내 로그인 감지 실패. 현재 위치:", page.url)
            ctx.close()
            sys.exit(1)

        # 세션 저장
        ctx.storage_state(path=str(SESSION))
        META.write_text(json.dumps({
            "refreshed_at": datetime.date.today().isoformat(),
            "valid_days": SESSION_VALID_DAYS,
        }, ensure_ascii=False, indent=2))
        ctx.close()

    exp = datetime.date.today() + datetime.timedelta(days=SESSION_VALID_DAYS)
    print(f"\n✅ 세션 저장 완료 → {SESSION.name}")
    print(f"   다음 갱신 권장일: {exp.isoformat()} (약 {SESSION_VALID_DAYS}일 뒤)")
    # 테스트 단계: 기본은 로컬 저장만. 운영 전환 시 CRITEO_UPLOAD=1 로 EC2 업로드.
    import os
    if os.environ.get("CRITEO_UPLOAD") == "1" or "--upload" in sys.argv:
        _upload()
    else:
        print("   (로컬 저장만 — EC2 업로드는 운영 전환 후 CRITEO_UPLOAD=1)")
    print("\n끝났습니다. 이 창은 닫아도 됩니다.")


if __name__ == "__main__":
    main()
