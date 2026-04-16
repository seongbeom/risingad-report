"""
Cafe24 로그인 자동화 스크립트
stealth 모드로 reCAPTCHA 우회 + 자동 로그인
"""

import os

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

load_dotenv()

LOGIN_URL = "https://eclogin.cafe24.com/Shop/?url=Init&login_mode=2"
SESSION_FILE = "session.json"

CAFE24_ID = os.environ["CAFE24_ID"]
CAFE24_SUB_ID = os.environ["CAFE24_SUB_ID"]
CAFE24_PW = os.environ["CAFE24_PW"]


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=300)
        context = browser.new_context()
        stealth = Stealth()
        page = context.new_page()
        stealth.apply_stealth_sync(page)

        page.goto(LOGIN_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)

        # reCAPTCHA iframe 내부 클릭
        recaptcha_frame = page.frame_locator("iframe[title='reCAPTCHA']")
        recaptcha_frame.locator(".rc-anchor-checkbox-holder").click()
        page.wait_for_timeout(1000)

        # 아이디/부운영자아이디/비밀번호 자동 입력
        page.fill("#mall_id", CAFE24_ID)
        page.fill("#userid", CAFE24_SUB_ID)
        page.fill("#userpasswd", CAFE24_PW)
        print(f"아이디({CAFE24_ID}), 부운영자({CAFE24_SUB_ID}), 비밀번호 자동 입력 완료")

        # 로그인 버튼 클릭
        page.click("button.btnStrong.large")
        print("로그인 버튼 클릭 완료")

        # 로그인 결과 대기
        try:
            page.wait_for_url(
                lambda url: "eclogin.cafe24.com" not in url,
                timeout=300_000,
            )
            print(f"로그인 성공! 현재 URL: {page.url}")
            context.storage_state(path=SESSION_FILE)
            print(f"세션이 {SESSION_FILE}에 저장되었습니다.")
        except Exception as e:
            print(f"타임아웃 또는 오류: {e}")

        browser.close()


if __name__ == "__main__":
    main()
