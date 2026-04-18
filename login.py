"""
Cafe24 로그인 자동화 - Playwright + Recognizer (AI reCAPTCHA solver)
recognizer: YOLOv8 + CLIP 기반 이미지 챌린지 자동 풀기
"""

import os

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from recognizer import Detector
from recognizer.agents.playwright import SyncChallenger

load_dotenv()

LOGIN_URL = "https://eclogin.cafe24.com/Shop/?url=Init&login_mode=2"
SESSION_FILE = "session.json"

CAFE24_ID = os.environ["CAFE24_ID"]
CAFE24_SUB_ID = os.environ["CAFE24_SUB_ID"]
CAFE24_PW = os.environ["CAFE24_PW"]

# 한국어 reCAPTCHA 라벨 → 영어 매핑
KO_ALIAS = {
    "자동차": "car", "차": "car", "차량": "car",
    "택시": "taxi",
    "버스": "bus",
    "오토바이": "motorcycle",
    "자전거": "bicycle",
    "보트": "boat", "배": "boat",
    "트랙터": "tractor",
    "계단": "stair",
    "야자수": "palm tree", "야자나무": "palm tree",
    "소화전": "fire hydrant",
    "주차 미터기": "parking meter", "주차미터기": "parking meter",
    "횡단보도": "crosswalk",
    "신호등": "traffic light",
    "다리": "bridge",
    "산": "mountain", "산 또는 언덕": "mountain",
    "굴뚝": "chimney",
}


def main():
    # recognizer Detector에 한국어 alias 추가
    detector = Detector()
    for ko, en in KO_ALIAS.items():
        detector.challenge_alias[ko] = en

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=300)
        context = browser.new_context()
        page = context.new_page()

        page.goto(LOGIN_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)

        # 아이디/부운영자아이디/비밀번호 입력
        page.fill("#mall_id", CAFE24_ID)
        page.fill("#userid", CAFE24_SUB_ID)
        page.fill("#userpasswd", CAFE24_PW)
        print(f"아이디({CAFE24_ID}), 부운영자({CAFE24_SUB_ID}), 비밀번호 입력 완료")

        # reCAPTCHA 자동 풀기
        print("reCAPTCHA 풀기 시도 중...")
        challenger = SyncChallenger(page, click_timeout=3000)
        challenger.detector = detector
        challenger.solve_recaptcha()
        print("reCAPTCHA 통과!")

        page.wait_for_timeout(1000)

        # 로그인 버튼 클릭
        page.click("button.btnStrong.large")
        print("로그인 버튼 클릭 완료")

        # 로그인 결과 대기
        try:
            page.wait_for_url(
                lambda url: "eclogin.cafe24.com" not in url,
                timeout=60000,
            )
            print(f"로그인 성공! 현재 URL: {page.url}")
            context.storage_state(path=SESSION_FILE)
            print(f"세션이 {SESSION_FILE}에 저장되었습니다.")
        except Exception as e:
            print(f"타임아웃 또는 오류: {e}")

        browser.close()


if __name__ == "__main__":
    main()
