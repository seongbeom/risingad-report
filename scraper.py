"""
Cafe24 애널리틱스 스크래퍼
- 로그인 (세션 재활용 / reCAPTCHA 자동 풀기)
- 4가지 데이터셋 스크래핑:
  1. 매출종합분석
  2. 방문자수
  3. 처음방문vs재방문 구매
  4. 신규회원수
- 다중 계정 지원
"""

import json
import os
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright
from recognizer import Detector
from recognizer.agents.playwright import SyncChallenger

LOGIN_URL_MAIN = "https://eclogin.cafe24.com/Shop/?url=Init&login_mode=1"
LOGIN_URL_SUB = "https://eclogin.cafe24.com/Shop/?url=Init&login_mode=2"
DATA_DIR = Path(__file__).parent / "data"

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


def _session_path(account_id):
    DATA_DIR.mkdir(exist_ok=True)
    return DATA_DIR / f"session_{account_id}.json"


def _result_path(account_id, date_str):
    account_dir = DATA_DIR / account_id
    account_dir.mkdir(parents=True, exist_ok=True)
    return account_dir / f"{date_str}.json"


def _is_main_admin(account):
    """부운영자 ID가 없거나 쇼핑몰ID와 같으면 대표관리자"""
    sub_id = account.get("sub_id", "").strip()
    return not sub_id or sub_id == account["cafe24_id"]


def login(page, account):
    """로그인 - 대표관리자(2필드) / 부운영자(3필드) 자동 구분, 캡챠 없으면 스킵"""
    cafe24_id = account["cafe24_id"]
    sub_id = account.get("sub_id", "")
    password = account["password"]
    main_admin = _is_main_admin(account)

    login_url = LOGIN_URL_MAIN if main_admin else LOGIN_URL_SUB
    page.goto(login_url, wait_until="domcontentloaded")
    page.wait_for_timeout(3000)

    page.fill("#mall_id", cafe24_id)
    if not main_admin:
        page.fill("#userid", sub_id)
    page.fill("#userpasswd", password)

    # reCAPTCHA: 있으면 풀고, 없으면 스킵
    recaptcha_iframe = page.query_selector("iframe[title*='reCAPTCHA']")
    if recaptcha_iframe:
        detector = Detector()
        for ko, en in KO_ALIAS.items():
            detector.challenge_alias[ko] = en
        challenger = SyncChallenger(page, click_timeout=3000)
        challenger.detector = detector
        try:
            challenger.solve_recaptcha()
        except Exception:
            pass  # invisible 등 풀 수 없는 경우 그냥 진행

    page.wait_for_timeout(1000)
    page.click("button.btnStrong.large")
    page.wait_for_url(lambda url: "eclogin.cafe24.com" not in url, timeout=60000)


def close_popups(page):
    page.wait_for_timeout(2000)
    for selector in [
        "button.close",
        "button:has-text('닫기')",
        "button:has-text('확인')",
        ".layerClose",
    ]:
        try:
            while page.locator(selector).first.is_visible():
                page.locator(selector).first.click()
                page.wait_for_timeout(500)
        except Exception:
            pass


def ensure_login(page, context, account):
    base = f"https://{account['cafe24_id']}.cafe24.com"
    page.goto(f"{base}/disp/admin/shop1/main/dashboard", wait_until="domcontentloaded")
    page.wait_for_timeout(2000)

    if "eclogin.cafe24.com" in page.url:
        login(page, account)
        session_file = str(_session_path(account["id"]))
        context.storage_state(path=session_file)

    close_popups(page)


def set_period_today(frame, page):
    period_texts = ["7일", "1개월", "3개월", "6개월", "오늘"]
    for text in period_texts:
        btn = frame.query_selector(f"button:has-text('{text}')")
        if btn:
            btn_text = btn.evaluate("el => el.textContent?.trim() || ''")
            if btn_text == text:
                if text == "오늘":
                    return
                btn.click()
                page.wait_for_timeout(1000)
                frame.locator("text=오늘").first.click()
                page.wait_for_timeout(1000)
                break

    search_btn = frame.query_selector("button:has-text('조회')")
    if search_btn:
        search_btn.click()
        page.wait_for_timeout(5000)


def _click_calendar_day(frame, page, day_num):
    """달력 팝업에서 특정 날짜(일) 클릭. 이전달 날짜(29,30,31)와 구분."""
    cells = frame.query_selector_all("td button")
    # 달력 셀에서 해당 날짜 찾기 (이전달/다음달 구분)
    found_first = False
    for cell in cells:
        if not cell.is_visible():
            continue
        text = cell.evaluate("el => el.textContent?.trim() || ''")
        if text == str(day_num):
            if day_num <= 28 or found_first:
                # 28일 이하면 바로 클릭, 29~31은 두번째 등장 (당월)을 클릭
                cell.click()
                page.wait_for_timeout(500)
                return True
            if day_num >= 29:
                found_first = True  # 첫번째는 이전달, 다음번이 당월
    return False


def set_period_range(frame, page, start_date, end_date):
    """기간 설정: '기간 선택' 모드에서 시작일/종료일 달력으로 지정 → 조회"""
    # 1) 기간 드롭다운 열기
    period_texts = ["7일", "1개월", "3개월", "6개월", "오늘"]
    for text in period_texts:
        btn = frame.query_selector(f"button:has-text('{text}')")
        if btn:
            btn_text = btn.evaluate("el => el.textContent?.trim() || ''")
            if btn_text == text:
                btn.click()
                page.wait_for_timeout(1000)
                break

    # 2) '기간 선택' 클릭
    period_option = frame.locator("text=기간 선택").first
    period_option.click()
    page.wait_for_timeout(1500)

    # 3) 시작일/종료일 버튼 클릭 → 달력에서 날짜 선택
    start_day = int(start_date.split("-")[2])
    end_day = int(end_date.split("-")[2])

    def _date_buttons():
        out = []
        for btn in frame.query_selector_all("button"):
            t = btn.evaluate("el => el.textContent?.trim() || ''")
            if len(t) == 10 and t.startswith("202") and t.count("-") == 2:
                out.append(btn)
        return out

    btns = _date_buttons()
    if len(btns) >= 1:
        btns[0].click()
        page.wait_for_timeout(1500)
        _click_calendar_day(frame, page, start_day)
        page.wait_for_timeout(1000)

    btns = _date_buttons()  # 시작일 변경 후 DOM이 바뀔 수 있으니 재조회
    if len(btns) >= 2:
        btns[1].click()
        page.wait_for_timeout(1500)
        _click_calendar_day(frame, page, end_day)
        page.wait_for_timeout(1000)

    # 4) 조회 클릭
    search_btn = frame.query_selector("button:has-text('조회')")
    if search_btn:
        search_btn.click()
        page.wait_for_timeout(5000)


def scrape_table(frame, table_index=0):
    tables = frame.query_selector_all("table")
    if table_index >= len(tables):
        return {"headers": [], "rows": []}

    table = tables[table_index]
    headers = table.evaluate("""el => {
        const ths = el.querySelectorAll('thead th');
        return Array.from(ths).map(th => th.textContent?.trim() || '');
    }""")
    rows = table.evaluate("""el => {
        const trs = el.querySelectorAll('tbody tr');
        return Array.from(trs).map(tr => {
            const tds = tr.querySelectorAll('td');
            return Array.from(tds).map(td => td.textContent?.trim() || '');
        });
    }""")
    return {"headers": headers, "rows": rows}


def scrape_sales(frame, page, period_fn=None):
    (period_fn or set_period_today)(frame, page)
    result = {}
    result["매출종합"] = scrape_table(frame, 0)
    result["구매단계"] = scrape_table(frame, 1)
    result["1인당매출"] = scrape_table(frame, 2)
    result["결제수단"] = scrape_table(frame, 3)
    return result


def scrape_visitors(frame, page, period_fn=None):
    (period_fn or set_period_today)(frame, page)
    result = {}
    result["전체방문자수"] = scrape_table(frame, 0)
    result["순방문자수"] = scrape_table(frame, 1)
    result["처음온방문자수"] = scrape_table(frame, 2)
    result["다시온방문자수"] = scrape_table(frame, 3)
    return result


def scrape_first_vs_repeat(frame, page, period_fn=None):
    (period_fn or set_period_today)(frame, page)
    pattern_tab = frame.query_selector("button:has-text('구매패턴')")
    if pattern_tab:
        pattern_tab.click()
        page.wait_for_timeout(3000)
    result = {}
    result["처음방문vs재방문"] = scrape_table(frame, 0)
    result["처음구매vs재구매"] = scrape_table(frame, 1)
    return result


def scrape_new_members(frame, page, period_fn=None):
    (period_fn or set_period_today)(frame, page)
    member_tab = frame.query_selector("button:has-text('회원 분석')")
    if member_tab:
        member_tab.click()
        page.wait_for_timeout(3000)
    result = {}
    result["신규회원수"] = scrape_table(frame, 0)
    result["회원별구매현황"] = scrape_table(frame, 1)
    return result


SALES_POPUP_URL = "https://ca-web.cafe24data.com/sales/popup/summary"
PATTERNS_POPUP_URL = "https://ca-web.cafe24data.com/customers/buyers/popup/purchase-patterns"


def scrape_popup(context, popup_url, start_date, end_date):
    """팝업 페이지(매출종합/구매패턴 전체보기)를 별도 탭으로 열어 일별 테이블 추출.
    반환: {table_index: {headers, rows}}"""
    p = context.new_page()
    try:
        url = f"{popup_url}?device_type=total&period=custom&start_date={start_date}&end_date={end_date}"
        p.goto(url, wait_until="networkidle", timeout=30000)
        p.wait_for_timeout(2000)
        out = {}
        for i, t in enumerate(p.query_selector_all("table")):
            headers = t.evaluate(
                "el => Array.from(el.querySelectorAll('thead th')).map(th => th.textContent?.trim() || '')"
            )
            rows = t.evaluate(
                "el => Array.from(el.querySelectorAll('tbody tr')).map(tr => Array.from(tr.querySelectorAll('td')).map(td => td.textContent?.trim() || ''))"
            )
            if headers:
                out[i] = {"headers": headers, "rows": rows}
        return out
    finally:
        p.close()


def run_scrape(account):
    """계정 하나에 대해 전체 스크래핑 실행. 결과 dict 반환."""
    cafe24_id = account["cafe24_id"]
    base = f"https://{cafe24_id}.cafe24.com"
    today = datetime.now().strftime("%Y-%m-%d")

    urls = {
        "sales": f"{base}/disp/admin/shop1/menu/cafe24analytics?type=sales",
        "visitors": f"{base}/disp/admin/shop1/menu/cafe24analytics?type=customers-visitors",
        "buyers": f"{base}/disp/admin/shop1/menu/cafe24analytics?type=customers-buyers",
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)

        session_file = _session_path(account["id"])
        if session_file.exists():
            context = browser.new_context(storage_state=str(session_file))
        else:
            context = browser.new_context()

        page = context.new_page()
        ensure_login(page, context, account)

        results = {"account": cafe24_id, "date": today}

        # 1. 매출분석
        page.goto(urls["sales"], wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["매출종합분석"] = scrape_sales(frame, page)

        # 2. 방문자분석
        page.goto(urls["visitors"], wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["방문자분석"] = scrape_visitors(frame, page)

        # 3. 처음방문vs재방문
        page.goto(urls["buyers"], wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["처음방문vs재방문"] = scrape_first_vs_repeat(frame, page)

        # 4. 신규회원
        page.goto(urls["buyers"], wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["신규회원"] = scrape_new_members(frame, page)

        # 5/6. 매출종합/구매패턴 전체보기 팝업 (구매개수, 처음·재구매 건수)
        results["매출종합_상세"] = scrape_popup(context, SALES_POPUP_URL, today, today)
        results["구매패턴_상세"] = scrape_popup(context, PATTERNS_POPUP_URL, today, today)

        # 세션 저장
        context.storage_state(path=str(session_file))
        browser.close()

    # 결과 파일 저장
    result_file = _result_path(account["id"], today)
    with open(result_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    return results


def run_scrape_range(account, start_date, end_date):
    """날짜 범위 스크래핑. 테이블에 일별 여러 행이 반환됨."""
    cafe24_id = account["cafe24_id"]
    base = f"https://{cafe24_id}.cafe24.com"

    urls = {
        "sales": f"{base}/disp/admin/shop1/menu/cafe24analytics?type=sales",
        "visitors": f"{base}/disp/admin/shop1/menu/cafe24analytics?type=customers-visitors",
        "buyers": f"{base}/disp/admin/shop1/menu/cafe24analytics?type=customers-buyers",
    }

    def period_fn(frame, page):
        set_period_range(frame, page, start_date, end_date)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)

        session_file = _session_path(account["id"])
        if session_file.exists():
            context = browser.new_context(storage_state=str(session_file))
        else:
            context = browser.new_context()

        page = context.new_page()
        ensure_login(page, context, account)

        results = {"account": cafe24_id, "start_date": start_date, "end_date": end_date}

        # 1. 매출분석
        page.goto(urls["sales"], wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["매출종합분석"] = scrape_sales(frame, page, period_fn)

        # 2. 방문자분석
        page.goto(urls["visitors"], wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["방문자분석"] = scrape_visitors(frame, page, period_fn)

        # 3. 처음방문vs재방문
        page.goto(urls["buyers"], wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["처음방문vs재방문"] = scrape_first_vs_repeat(frame, page, period_fn)

        # 4. 신규회원
        page.goto(urls["buyers"], wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["신규회원"] = scrape_new_members(frame, page, period_fn)

        # 5. 매출종합 전체보기 팝업 (구매개수 포함)
        results["매출종합_상세"] = scrape_popup(context, SALES_POPUP_URL, start_date, end_date)

        # 6. 처음구매vs재구매 전체보기 팝업 (처음/재구매 구매건수 포함)
        results["구매패턴_상세"] = scrape_popup(context, PATTERNS_POPUP_URL, start_date, end_date)

        # 세션 저장
        context.storage_state(path=str(session_file))
        browser.close()

    # 결과 파일 저장
    result_file = _result_path(account["id"], f"{start_date}_to_{end_date}")
    with open(result_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    return results


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    account = {
        "id": os.environ["CAFE24_ID"],
        "cafe24_id": os.environ["CAFE24_ID"],
        "sub_id": os.environ["CAFE24_SUB_ID"],
        "password": os.environ["CAFE24_PW"],
    }
    results = run_scrape(account)
    print(json.dumps(results, ensure_ascii=False, indent=2))
