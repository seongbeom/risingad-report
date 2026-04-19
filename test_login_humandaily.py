"""humandaily 대표관리자 로그인 테스트 (부운영자 ID 없이)"""
from playwright.sync_api import sync_playwright
from recognizer import Detector
from recognizer.agents.playwright import SyncChallenger

KO_ALIAS = {
    "자동차": "car", "차": "car", "차량": "car",
    "택시": "taxi", "버스": "bus", "오토바이": "motorcycle",
    "자전거": "bicycle", "보트": "boat", "배": "boat",
    "트랙터": "tractor", "계단": "stair",
    "야자수": "palm tree", "야자나무": "palm tree",
    "소화전": "fire hydrant",
    "주차 미터기": "parking meter", "주차미터기": "parking meter",
    "횡단보도": "crosswalk", "신호등": "traffic light",
    "다리": "bridge", "산": "mountain", "산 또는 언덕": "mountain",
    "굴뚝": "chimney",
}

LOGIN_URL = "https://eclogin.cafe24.com/Shop/?url=Init&login_mode=1"

detector = Detector()
for ko, en in KO_ALIAS.items():
    detector.challenge_alias[ko] = en

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, slow_mo=300)
    context = browser.new_context()
    page = context.new_page()

    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(3000)

    page.fill("#mall_id", "humandaily")
    page.fill("#userpasswd", "fhakfhak2026!!")
    print("아이디/비밀번호 입력 완료")

    # reCAPTCHA 체크박스 확인
    recaptcha_iframe = page.query_selector("iframe[title*='reCAPTCHA']")
    if recaptcha_iframe:
        print("reCAPTCHA iframe 발견 - 풀기 시도")
        challenger = SyncChallenger(page, click_timeout=3000)
        challenger.detector = detector
        challenger.solve_recaptcha()
        print("reCAPTCHA 통과")
    else:
        print("reCAPTCHA iframe 없음 - 바로 로그인 시도")

    page.wait_for_timeout(1000)
    page.click("button.btnStrong.large")
    print("로그인 버튼 클릭")

    # 결과 대기
    page.wait_for_timeout(5000)
    print(f"현재 URL: {page.url}")

    if "eclogin.cafe24.com" not in page.url:
        print("로그인 성공!")
        context.storage_state(path="data/session_humandaily.json")
        print("세션 저장 완료")
    else:
        # 에러 메시지 확인
        error = page.evaluate("document.querySelector('.error, .alert, [class*=error]')?.textContent || ''")
        print(f"로그인 실패. 에러: {error}")
        page.screenshot(path="screenshot_login_fail.png")

    browser.close()
