"""네이버 성과형(GFA) 세션 갱신 도구 (로컬, 월 1회).

ads.naver.com(네이버 광고주센터) 로그인 → 세션 저장 → (운영전환 시 EC2 업로드).
영구 프로필 사용 → 첫 로그인 시 '이 브라우저는 2단계 인증 없이 로그인합니다' 체크하면
이후 갱신은 2FA(폰 푸시) 없이 ID/PW 만으로 진행됨.

사용: naver_gfa_login.command 더블클릭  (또는 venv/bin/python3 naver_gfa_login.py)
"""
import datetime
import json
import subprocess
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).parent
DATA = ROOT / "data"
PROFILE = DATA / "naver_gfa_profile"
SESSION = DATA / "naver_gfa_session.json"
META = DATA / "naver_gfa_session_meta.json"

KEY = Path.home() / "cafe24_migration" / "cafe24-new-key.pem"
HOST = "ubuntu@52.79.112.252"
REMOTE_DATA = "/opt/cafe24/data"

START_URL = "https://ads.naver.com/"
SESSION_VALID_DAYS = 30


def _logged_in(page):
    """ads.naver.com 진입 + nid 로그인 화면 아님 → 로그인 완료."""
    u = (page.url or "").lower()
    if "nid.naver.com" in u or "/login" in u or "about:blank" in u:
        return False
    return "ads.naver.com" in u


def _upload():
    if not KEY.exists():
        print(f"⚠️  SSH 키 없음({KEY}) — EC2 업로드 건너뜀. 로컬엔 저장됨.")
        return
    for f in (SESSION, META):
        try:
            subprocess.run(
                ["scp", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=12",
                 "-i", str(KEY), str(f), f"{HOST}:{REMOTE_DATA}/{f.name}"],
                check=True, capture_output=True, timeout=60)
        except Exception as e:
            print(f"⚠️  업로드 실패({f.name}): {e}")
    print(f"✅ EC2 업로드 완료 → {HOST}:{REMOTE_DATA}")


def main():
    DATA.mkdir(exist_ok=True)
    PROFILE.mkdir(parents=True, exist_ok=True)
    print("브라우저를 띄웁니다. ads.naver.com 에 로그인하세요.")
    print("★ 첫 로그인 시: '로그인 상태 유지' 체크 + 폰 2단계 인증 승인 +")
    print("  '이 브라우저는 2단계 인증 없이 로그인합니다' 체크 → 다음부터 폰 인증 불필요.\n")
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(PROFILE), headless=False,
            args=["--disable-blink-features=AutomationControlled"],
            viewport={"width": 1400, "height": 950},
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)

        deadline = time.time() + 360
        last = None
        while time.time() < deadline:
            cur = page.url
            if cur != last:
                print(f"  현재 위치: {cur}")
                last = cur
            if _logged_in(page):
                page.wait_for_timeout(2500)
                if _logged_in(page):
                    break
            time.sleep(2)
        else:
            print("❌ 6분 내 로그인 감지 실패. 현재 위치:", page.url)
            ctx.close()
            sys.exit(1)

        ctx.storage_state(path=str(SESSION))
        META.write_text(json.dumps({
            "refreshed_at": datetime.date.today().isoformat(),
            "valid_days": SESSION_VALID_DAYS,
        }, ensure_ascii=False, indent=2))
        ctx.close()

    exp = datetime.date.today() + datetime.timedelta(days=SESSION_VALID_DAYS)
    print(f"\n✅ 세션 저장 완료 → {SESSION.name}")
    print(f"   다음 갱신 권장일: {exp.isoformat()} (약 {SESSION_VALID_DAYS}일 뒤)")
    import os
    if os.environ.get("GFA_UPLOAD") == "1" or "--upload" in sys.argv:
        _upload()
    else:
        print("   (로컬 저장만 — 운영 전환 후 GFA_UPLOAD=1)")
    print("\n끝났습니다. 이 창은 닫아도 됩니다.")


if __name__ == "__main__":
    main()
