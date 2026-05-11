"""Camoufox 캡챠 우회 효과 측정.
3개 부운영자 계정으로 로그인 흐름 따라가며 (1) 챌린지 popup 뜨는지 (2) checkbox 누르면 자동 통과되는지 (3) 메모리 사용 확인."""
import sys
import time
import traceback
import resource

from camoufox.sync_api import Camoufox

ACCOUNTS = [
    {"id": "cinderella1009", "cafe24_id": "cinderella1009", "sub_id": "cinderellacs", "password": "fhakfhak2026!!1"},
    {"id": "ghostbin", "cafe24_id": "ghostbin", "sub_id": "risingad1", "password": "fkdlwld123"},
    {"id": "woodique", "cafe24_id": "woodique", "sub_id": "risingad", "password": "fkdlwlddoem1245"},
]
LOGIN_URL_SUB = "https://eclogin.cafe24.com/Shop/?url=Init&login_mode=2"


def _state(page):
    s = {"url": page.url, "redirected": "eclogin.cafe24.com" not in page.url}
    try:
        anchor = page.locator("iframe[title*='reCAPTCHA']").first
        s["anchor_count"] = page.locator("iframe[title*='reCAPTCHA']").count()
        s["anchor_visible"] = anchor.is_visible() if s["anchor_count"] else False
    except Exception:
        s["anchor_count"], s["anchor_visible"] = 0, False
    try:
        bframe = page.locator("iframe[title*='reCAPTCHA 보안문자']").first
        s["bframe_count"] = page.locator("iframe[title*='reCAPTCHA 보안문자']").count()
        if s["bframe_count"]:
            box = bframe.bounding_box()
            s["challenge_open"] = bool(box and box.get("width", 0) > 100)
        else:
            s["challenge_open"] = False
    except Exception:
        s["bframe_count"], s["challenge_open"] = 0, False
    return s


def _mem_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024  # macOS bytes, linux KB→MB approx


def _try_check_anchor(page):
    """reCAPTCHA anchor iframe 내부의 체크박스 클릭 시도. 자동 토큰 발급 가능한지 확인."""
    try:
        # anchor iframe 안의 #recaptcha-anchor 클릭
        for fr in page.frames:
            if "anchor" in (fr.url or ""):
                cb = fr.locator("#recaptcha-anchor")
                if cb.count() > 0:
                    cb.click(timeout=5000)
                    return True
        return False
    except Exception as e:
        print(f"   checkbox click 실패: {e}")
        return False


def test_account(account, headless=True):
    print(f"\n=== [{account['id']}] {account['cafe24_id']} / sub={account['sub_id']} ===")
    t0 = time.time()
    out = {"id": account["id"]}
    try:
        with Camoufox(headless=headless, humanize=True, locale="ko-KR") as browser:
            page = browser.new_page()
            page.goto(LOGIN_URL_SUB, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)

            s1 = _state(page)
            print(f"  도착: anchor={s1['anchor_count']} challenge={s1['challenge_open']}")

            page.fill("#mall_id", account["cafe24_id"])
            page.fill("#userid", account["sub_id"])
            page.fill("#userpasswd", account["password"])
            page.wait_for_timeout(1500)

            # reCAPTCHA checkbox 직접 클릭 → 위험점수 낮으면 challenge 안 뜨고 자동 통과
            checked = _try_check_anchor(page)
            print(f"  체크박스 클릭: {checked}")
            page.wait_for_timeout(4000)  # 토큰 발급 대기

            s2 = _state(page)
            print(f"  체크박스 후: challenge_open={s2['challenge_open']}")
            out["challenge_after_checkbox"] = s2["challenge_open"]

            # 로그인 버튼 클릭
            try:
                page.click("button.btnStrong.large", timeout=8000)
            except Exception as e:
                print(f"  로그인 클릭 실패: {e}")
                out["click_err"] = str(e)[:100]

            # 도메인 변경 대기
            deadline = time.time() + 15
            while time.time() < deadline:
                if "eclogin.cafe24.com" not in page.url:
                    break
                page.wait_for_timeout(500)

            s3 = _state(page)
            print(f"  최종: url={s3['url'][:70]} redirected={s3['redirected']} challenge={s3['challenge_open']}")
            out.update({
                "redirected": s3["redirected"],
                "challenge_final": s3["challenge_open"],
                "elapsed": round(time.time() - t0, 1),
            })
    except Exception as e:
        out["fatal"] = str(e)[:200]
        traceback.print_exc()
    out["peak_mem_mb"] = _mem_mb()
    return out


if __name__ == "__main__":
    headless = "--headed" not in sys.argv
    print(f"Camoufox 테스트 (headless={headless})")
    results = []
    for acc in ACCOUNTS:
        try:
            results.append(test_account(acc, headless=headless))
        except KeyboardInterrupt:
            break
        time.sleep(3)

    print("\n=== 요약 ===")
    for r in results:
        print(r)
