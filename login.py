"""
Cafe24 로그인 테스트 스크립트
테스트 아이디/비번 자동 입력, reCAPTCHA + 로그인은 직접 클릭
"""

from playwright.sync_api import sync_playwright

LOGIN_URL = "https://eclogin.cafe24.com/Shop/?url=Init&login_mode=2"
SESSION_FILE = "session.json"

TEST_ID = "testuser123"
TEST_PW = "testpass123"


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=300)
        context = browser.new_context()
        page = context.new_page()

        page.goto(LOGIN_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)

        # reCAPTCHA iframe 내부 클릭
        recaptcha_frame = page.frame_locator("iframe[title='reCAPTCHA']")
        recaptcha_frame.locator(".rc-anchor-checkbox-holder").click()
        page.wait_for_timeout(1000)

        # 아이디/비밀번호 자동 입력
        page.fill("#mall_id", TEST_ID)
        page.fill("#userpasswd", TEST_PW)
        print(f"아이디({TEST_ID}), 비밀번호 자동 입력 완료")
        print()
        print("이제 직접 해주세요:")
        print("  1. reCAPTCHA 체크박스 클릭")
        print("  2. 로그인 버튼 클릭")
        print()
        print("5분 내에 완료해주세요...")

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
