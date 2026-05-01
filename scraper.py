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
from datetime import datetime, timedelta
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
    """기간 설정: 기간 선택 모드 → 시작일 button → 캘린더에서 day → 종료일 button → 캘린더에서 day → 조회.
    카페24 캘린더는 default로 현재 월(이번 달)이 떠있어서 navigate 없이 바로 day 클릭이 정상."""
    start_day = int(start_date.split("-")[2])
    end_day = int(end_date.split("-")[2])

    # 1) 기간 드롭다운 → 기간 선택
    for text in ["7일", "1개월", "3개월", "6개월", "오늘"]:
        b = frame.query_selector(f"button:has-text('{text}')")
        if b and b.evaluate("el => el.textContent?.trim() || ''") == text:
            b.click()
            page.wait_for_timeout(800)
            opt = frame.locator("text=기간 선택").first
            if opt.count() > 0:
                opt.click()
                page.wait_for_timeout(1200)
            break

    def _date_btns():
        out = []
        for b in frame.query_selector_all("button"):
            t = b.evaluate("el => el.textContent?.trim() || ''")
            if len(t) == 10 and t[:2] == "20" and t[4] == "-" and t[7] == "-":
                out.append((t, b))
        return out

    def _pick_day_cell(day_num):
        """캘린더 셀 중 day_num 매칭. 29~31은 이전달과 당월 둘 다 등장하므로 두 번째 매칭 클릭."""
        cells = frame.query_selector_all("td button")
        matches = [c for c in cells if c.is_visible() and c.evaluate("el => el.textContent?.trim() || ''") == str(day_num)]
        if not matches:
            return False
        if day_num >= 29 and len(matches) >= 2:
            target = matches[1]
        else:
            target = matches[0]
        target.click()
        page.wait_for_timeout(800)
        return True

    # 2) 시작일 button click → 캘린더 popup → start_day 클릭
    btns = _date_btns()
    if len(btns) >= 1:
        btns[0][1].click()
        page.wait_for_timeout(1200)
        _pick_day_cell(start_day)

    # 3) 종료일 button click → 캘린더 popup → end_day 클릭
    btns = _date_btns()
    if len(btns) >= 2:
        btns[1][1].click()
        page.wait_for_timeout(1200)
        _pick_day_cell(end_day)

    # 4) 시작/종료 텍스트 검증
    btns = _date_btns()
    if btns:
        print(f"[set_period_range] 적용된 버튼 텍스트: {[t for t,_ in btns]}")

    # 5) 조회 클릭
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


def _attach_sample_detector(page_or_context):
    """카페24 ca-internal API 응답에 'is_sample': True가 있으면 데모 데이터.
    detector dict의 'is_sample' 플래그를 set 해서 호출자가 확인 가능하게 함."""
    detector = {"is_sample": False}

    def on_response(resp):
        try:
            if "ca-internal.cafe24data.com/ca2/" in resp.url and resp.status == 200:
                ct = resp.headers.get("content-type", "")
                if "json" in ct:
                    body = resp.json()
                    if isinstance(body, dict) and body.get("is_sample") is True:
                        detector["is_sample"] = True
        except Exception:
            pass

    page_or_context.on("response", on_response)
    return detector


def scrape_popup(context, popup_url, start_date, end_date):
    """팝업 페이지(매출종합/구매패턴 전체보기)를 별도 탭으로 열어 일별 테이블 추출.
    반환: {table_index: {headers, rows}}"""
    p = context.new_page()
    try:
        url = f"{popup_url}?device_type=total&period=custom&start_date={start_date}&end_date={end_date}"
        p.goto(url, wait_until="domcontentloaded", timeout=30000)
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


def scrape_popup_hourly_via_admin(page, context, frame, target_date):
    """어드민 매출분석 frame의 '매출종합 분석' 카드 안 '전체보기' 버튼 클릭으로
    popup 새 탭을 열어 시간 단위 24시간 테이블 추출.
    부운영자 계정에서 ca-web URL 직접 navigate는 인증 토큰 누락으로 401 떨어지기 때문에
    화면 클릭 흐름으로 popup을 열어야 한다.
    호출 전 set_period_range(target_date, target_date) 가 이미 frame 에 적용되어 있어야 함."""
    # 매출종합 분석 카드의 '전체보기' (frame 내 첫 번째)
    try:
        btn = frame.locator("button:has-text('전체보기')").first
        with context.expect_page(timeout=20000) as new_page_info:
            btn.click()
        p = new_page_info.value
    except Exception as e:
        print(f"[scrape_popup_hourly_via_admin] 전체보기 popup 실패: {e}")
        return {}

    try:
        p.wait_for_load_state("domcontentloaded", timeout=20000)
        try:
            p.wait_for_selector("table tbody tr", timeout=15000)
        except Exception:
            p.wait_for_timeout(8000)

        # 표시 기준 → '시간 단위'
        sel = p.locator("select").first
        if sel.count() > 0:
            try:
                sel.select_option(label="시간 단위", timeout=5000)
                p.wait_for_timeout(2000)
            except Exception:
                pass

        # 조회 클릭
        for txt in ["조회하기", "조회"]:
            btn = p.locator(f"button:has-text('{txt}')").first
            try:
                if btn.count() > 0 and btn.is_visible():
                    btn.click()
                    break
            except Exception:
                continue

        # 시간단위 데이터 도착 대기
        try:
            p.wait_for_function(
                """() => {
                    const tables = document.querySelectorAll('table');
                    for (const t of tables) {
                        const headers = Array.from(t.querySelectorAll('thead th')).map(th => th.textContent?.trim() || '');
                        if (headers.some(h => h.includes('일시') || h.includes('시간'))) {
                            const rows = t.querySelectorAll('tbody tr');
                            if (rows.length >= 5) return true;
                        }
                    }
                    return false;
                }""",
                timeout=20000,
            )
        except Exception:
            pass
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


def scrape_popup_hourly(context, popup_url, target_date):
    """[deprecated] ca-web URL 직접 navigate 방식. 부운영자 계정에서 인증 토큰 누락으로 401.
    어드민 진입 흐름의 scrape_popup_hourly_via_admin 사용 권장. 백워드 호환용으로 유지."""
    p = context.new_page()
    try:
        url = f"{popup_url}?device_type=total&period=custom&start_date={target_date}&end_date={target_date}"
        p.goto(url, wait_until="domcontentloaded", timeout=30000)
        # 초기 데이터 로드 대기 (빠른 계정은 5초, 느린 계정은 10~12초 필요)
        try:
            p.wait_for_selector("table tbody tr", timeout=15000)
        except Exception:
            p.wait_for_timeout(8000)

        # 표시 기준 select → '시간 단위'
        sel = p.locator("select").first
        if sel.count() > 0:
            try:
                sel.select_option(label="시간 단위", timeout=5000)
                p.wait_for_timeout(2000)
            except Exception:
                pass

        # 조회 버튼 클릭
        for txt in ["조회하기", "조회"]:
            btn = p.locator(f"button:has-text('{txt}')").first
            try:
                if btn.count() > 0 and btn.is_visible():
                    btn.click()
                    break
            except Exception:
                continue

        # 시간단위 데이터 도착 대기 — 24행 또는 충분히 많은 row 가 들어올 때까지
        deadline_ms = 20000
        try:
            p.wait_for_function(
                """() => {
                    const tables = document.querySelectorAll('table');
                    for (const t of tables) {
                        const headers = Array.from(t.querySelectorAll('thead th')).map(th => th.textContent?.trim() || '');
                        if (headers.some(h => h.includes('일시') || h.includes('시간'))) {
                            const rows = t.querySelectorAll('tbody tr');
                            if (rows.length >= 5) return true;
                        }
                    }
                    return false;
                }""",
                timeout=deadline_ms,
            )
        except Exception:
            pass
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


def run_scrape(account, target_date=None):
    """계정 하나에 대해 전체 스크래핑 실행. 결과 dict 반환.
    target_date 미지정 시 어제 날짜 사용 (당일은 부분 데이터라 부정확).
    """
    cafe24_id = account["cafe24_id"]
    base = f"https://{cafe24_id}.cafe24.com"
    target_date = target_date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    urls = {
        "sales": f"{base}/disp/admin/shop1/menu/cafe24analytics?type=sales",
        "visitors": f"{base}/disp/admin/shop1/menu/cafe24analytics?type=customers-visitors",
        "buyers": f"{base}/disp/admin/shop1/menu/cafe24analytics?type=customers-buyers",
    }

    def period_fn(frame, page):
        set_period_range(frame, page, target_date, target_date)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)

        session_file = _session_path(account["id"])
        if session_file.exists():
            context = browser.new_context(storage_state=str(session_file))
        else:
            context = browser.new_context()

        page = context.new_page()
        sample_detector = _attach_sample_detector(page)
        ensure_login(page, context, account)

        results = {"account": cafe24_id, "date": target_date}

        # 1. 매출분석
        page.goto(urls["sales"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["매출종합분석"] = scrape_sales(frame, page, period_fn)

        # 2. 방문자분석
        page.goto(urls["visitors"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["방문자분석"] = scrape_visitors(frame, page, period_fn)

        # 3. 처음방문vs재방문
        page.goto(urls["buyers"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["처음방문vs재방문"] = scrape_first_vs_repeat(frame, page, period_fn)

        # 4. 신규회원
        page.goto(urls["buyers"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["신규회원"] = scrape_new_members(frame, page, period_fn)

        # 5/6. 매출종합/구매패턴 전체보기 팝업 (구매개수, 처음·재구매 건수)
        results["매출종합_상세"] = scrape_popup(context, SALES_POPUP_URL, target_date, target_date)
        results["구매패턴_상세"] = scrape_popup(context, PATTERNS_POPUP_URL, target_date, target_date)

        # 7. 시간 단위 매출 - 어드민 매출분석 화면 다시 진입 후 '전체보기' 클릭으로 popup
        try:
            page.goto(urls["sales"], wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(5000)
            sales_frame = page.frame("adminFrameContent")
            if sales_frame:
                set_period_range(sales_frame, page, target_date, target_date)
                results["매출종합_시간별"] = scrape_popup_hourly_via_admin(page, context, sales_frame, target_date)
            else:
                results["매출종합_시간별"] = {}
        except Exception as e:
            print(f"[hourly] 실패 - 시간별 스킵: {e}")
            results["매출종합_시간별"] = {}

        results["_is_sample"] = sample_detector["is_sample"]

        # 세션 저장
        context.storage_state(path=str(session_file))
        browser.close()

    # 결과 파일 저장
    result_file = _result_path(account["id"], target_date)
    with open(result_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    return results


def run_scrape_range(account, start_date, end_date):

    """날짜 범위 스크래핑. 테이블에 일별 여러 행이 반환됨."""
    cafe24_id = account["cafe24_id"]
    base = f"https://{cafe24_id}.cafe24.com"

    # URL 파라미터로 날짜 주면 백엔드 routing이 이상해지는 케이스 발견 → 캘린더 클릭만으로 진행
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
        sample_detector = _attach_sample_detector(page)
        ensure_login(page, context, account)

        results = {"account": cafe24_id, "start_date": start_date, "end_date": end_date}

        # 1. 매출분석
        page.goto(urls["sales"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["매출종합분석"] = scrape_sales(frame, page, period_fn)

        # 2. 방문자분석
        page.goto(urls["visitors"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["방문자분석"] = scrape_visitors(frame, page, period_fn)

        # 3. 처음방문vs재방문
        page.goto(urls["buyers"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["처음방문vs재방문"] = scrape_first_vs_repeat(frame, page, period_fn)

        # 4. 신규회원
        page.goto(urls["buyers"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            results["신규회원"] = scrape_new_members(frame, page, period_fn)

        # 5. 매출종합 전체보기 팝업 (구매개수 포함)
        results["매출종합_상세"] = scrape_popup(context, SALES_POPUP_URL, start_date, end_date)

        # 6. 처음구매vs재구매 전체보기 팝업 (처음/재구매 구매건수 포함)
        results["구매패턴_상세"] = scrape_popup(context, PATTERNS_POPUP_URL, start_date, end_date)

        results["_is_sample"] = sample_detector["is_sample"]

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
