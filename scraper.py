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
import re
import time as _time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright

LOGIN_URL_MAIN = "https://eclogin.cafe24.com/Shop/?url=Init&login_mode=1"
LOGIN_URL_SUB = "https://eclogin.cafe24.com/Shop/?url=Init&login_mode=2"
DATA_DIR = Path(__file__).parent / "data"

CAPSOLVER_API_KEY = os.environ.get("CAPSOLVER_API_KEY", "")
CAPSOLVER_BASE = "https://api.capsolver.com"

# 계정별 마지막 진입 phase 기록 (app.py 가 hang 시 systemic 여부 판단에 사용).
# 'chromium launch' / 'ensure_login' 단계 hang = 진짜 wedge 후보,
# 그 이후(데이터 페이지) hang = 계정/사이트 느림 → systemic 아님.
LAST_PHASE = {}


def capsolver_solve_recaptcha_v2(sitekey, page_url, timeout=120, account_id=None):
    """CapSolver API 로 reCAPTCHA v2 풀기. g-recaptcha-response 토큰 문자열 반환.
    실패 시 RuntimeError raise. db.capsolver_calls 에 호출 기록 남김."""
    if not CAPSOLVER_API_KEY:
        raise RuntimeError("CAPSOLVER_API_KEY 환경변수 미설정")
    t0 = _time.time()
    import db as _db
    try:
        create = requests.post(f"{CAPSOLVER_BASE}/createTask", json={
            "clientKey": CAPSOLVER_API_KEY,
            "task": {
                "type": "ReCaptchaV2TaskProxyLess",
                "websiteURL": page_url,
                "websiteKey": sitekey,
            },
        }, timeout=15).json()
        if create.get("errorId") != 0:
            raise RuntimeError(f"createTask 실패: {create.get('errorDescription') or create}")
        task_id = create.get("taskId")
        if not task_id:
            raise RuntimeError(f"taskId 없음: {create}")

        deadline = _time.time() + timeout
        while _time.time() < deadline:
            _time.sleep(2)
            r = requests.post(f"{CAPSOLVER_BASE}/getTaskResult", json={
                "clientKey": CAPSOLVER_API_KEY,
                "taskId": task_id,
            }, timeout=15).json()
            if r.get("errorId") != 0:
                raise RuntimeError(f"getTaskResult 실패: {r.get('errorDescription') or r}")
            if r.get("status") == "ready":
                token = (r.get("solution") or {}).get("gRecaptchaResponse")
                if not token:
                    raise RuntimeError(f"solution 비어있음: {r}")
                _db.log_capsolver_call(account_id, True, int((_time.time() - t0) * 1000))
                return token
        raise RuntimeError(f"타임아웃 ({timeout}s)")
    except Exception as e:
        try:
            _db.log_capsolver_call(account_id, False, int((_time.time() - t0) * 1000), str(e))
        except Exception:
            pass
        raise


def capsolver_balance():
    """남은 잔액(USD) 조회. 실패 시 None."""
    if not CAPSOLVER_API_KEY:
        return None
    try:
        r = requests.post(f"{CAPSOLVER_BASE}/getBalance", json={"clientKey": CAPSOLVER_API_KEY}, timeout=10).json()
        if r.get("errorId") == 0:
            return r.get("balance")
    except Exception:
        pass
    return None


def _inject_recaptcha_token(page, token):
    """발급받은 토큰을 페이지의 g-recaptcha-response hidden field 에 주입.
    cafe24 는 별도 callback 안 호출해도 form_check() 가 토큰 텍스트로 검증함."""
    page.evaluate(
        """(token) => {
            // 1) 표준 hidden textarea
            const els = document.querySelectorAll('[name="g-recaptcha-response"], #g-recaptcha-response');
            els.forEach(el => { el.value = token; el.innerHTML = token; });
            // 2) reCAPTCHA 가 hidden 으로 만든 textarea도 강제로 채움
            document.querySelectorAll('textarea').forEach(t => {
                if (t.id && t.id.startsWith('g-recaptcha-response')) {
                    t.value = token; t.innerHTML = token;
                }
            });
            // 3) reCAPTCHA 콜백이 등록돼 있으면 호출 시도
            try {
                if (typeof ___grecaptcha_cfg !== 'undefined') {
                    Object.keys(___grecaptcha_cfg.clients || {}).forEach(k => {
                        const client = ___grecaptcha_cfg.clients[k];
                        const walk = (obj) => {
                            for (const key in obj) {
                                if (obj[key] && typeof obj[key] === 'object') {
                                    if (typeof obj[key].callback === 'function') {
                                        try { obj[key].callback(token); } catch(e){}
                                    } else { walk(obj[key]); }
                                }
                            }
                        };
                        walk(client);
                    });
                }
            } catch(e) {}
        }""",
        token,
    )


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

    # fill() 은 input.value 만 세팅하고 keystroke event 안 발생시킴.
    # cafe24 비번 필드가 keypress/input listener 로 클라이언트 hash 등 추가 처리를 한다면
    # type() 으로 keystroke 발생시켜야 함.
    page.click("#mall_id")
    page.fill("#mall_id", "")
    page.type("#mall_id", cafe24_id, delay=30)
    if not main_admin:
        page.click("#userid")
        page.fill("#userid", "")
        page.type("#userid", sub_id, delay=30)
    page.click("#userpasswd")
    page.fill("#userpasswd", "")
    page.type("#userpasswd", password, delay=30)
    # blur 처리 + 페이지 내부 listener 가 hash 등 처리할 시간 확보
    try:
        page.evaluate("document.activeElement && document.activeElement.blur && document.activeElement.blur()")
    except Exception:
        pass
    page.wait_for_timeout(500)

    # reCAPTCHA: CapSolver API 로 v2 토큰 받아 hidden field 에 주입
    # iframe 안 떠 있으면 캡챠 없는 케이스 (세션 신뢰도 높음) - skip
    # 단, iframe 이 늦게 렌더링되는 경우가 있어 최대 5초까지 기다림
    recaptcha_iframe = None
    for _ in range(10):
        recaptcha_iframe = page.query_selector("iframe[title*='reCAPTCHA']")
        if recaptcha_iframe:
            break
        # g-recaptcha div 가 있는데 iframe 이 아직 안 뜬 경우 강제 시도
        if page.query_selector("div.g-recaptcha, [data-sitekey]"):
            page.wait_for_timeout(500)
            continue
        # g-recaptcha div 자체가 아예 없으면 캡챠가 안 뜨는 페이지 - 즉시 break
        break
    if not recaptcha_iframe:
        # 명시적 g-recaptcha div 가 있는데 iframe 만 없으면 추측 가능
        gdiv = page.query_selector("div.g-recaptcha, [data-sitekey]")
        if gdiv:
            sitekey_attr = gdiv.get_attribute("data-sitekey")
            if sitekey_attr:
                print(f"[login] iframe 미생성 but g-recaptcha div 발견, sitekey={sitekey_attr} - 강제 풀이")
                token = capsolver_solve_recaptcha_v2(sitekey_attr, page.url, timeout=120, account_id=account.get("id"))
                _inject_recaptcha_token(page, token)
                page.wait_for_timeout(800)
                recaptcha_iframe = "__handled__"
    if recaptcha_iframe:
        # iframe src 의 k= 파라미터에서 sitekey 추출
        src = recaptcha_iframe.get_attribute("src") or ""
        m = re.search(r"[?&]k=([\w-]+)", src)
        sitekey = m.group(1) if m else "6LehBQQTAAAAADqgKwu7R9xDHt3FB8VPiZnk0iK-"
        print(f"[login] CapSolver 요청 sitekey={sitekey}")
        try:
            token = capsolver_solve_recaptcha_v2(sitekey, page.url, timeout=120, account_id=account.get("id"))
            print(f"[login] CapSolver 토큰 수신 (len={len(token)})")
            _inject_recaptcha_token(page, token)
            page.wait_for_timeout(800)
        except Exception as e:
            raise RuntimeError(f"CapSolver 풀이 실패: {e}")

    # 토큰 주입 후 로그인 클릭 (iframe 닫혀있으므로 intercept 없음)
    try:
        page.click("button.btnStrong.large", timeout=10000)
    except Exception as e:
        raise RuntimeError(f"로그인 버튼 클릭 실패: {e}")

    # 로그인 클릭 후 도메인 빠져나가길 기다림. 캡챠 답이 틀려 서버가 거절하면 URL 안 바뀜.
    # 60s 그대로 기다리면 외부 재시도 루프와 함께 1계정에 3분 낭비됨 → 20s 로 줄이고
    # 서버 거절 신호(에러 텍스트 / 챌린지 iframe 재출현)면 즉시 raise.
    import time as _t
    deadline = _t.time() + 20
    rejected_reason = None
    while _t.time() < deadline:
        if "eclogin.cafe24.com" not in page.url:
            return  # 정상 도메인 변경
        try:
            # 1) 비밀번호/아이디 거절 - false positive 방지 위해 정확한 에러 문구만
            #    (페이지에 "아이디/비밀번호 찾기" 같은 링크가 항상 있어서 광범위 매칭 X)
            if page.locator(
                "text=/비밀번호가 일치하지|비밀번호를 다시|회원정보가 일치하지|잘못된 아이디|등록되지 않은 아이디|존재하지 않는 아이디/"
            ).count() > 0:
                rejected_reason = "비밀번호/아이디 거절"
                break
            # 2) 보안문자 거절
            if page.locator(
                "text=/보안문자.*일치|보안문자.*다시|보안문자가 일치하지|입력하신 보안문자|reCAPTCHA.*다시/"
            ).count() > 0:
                rejected_reason = "보안문자 거절"
                break
            # 3) 챌린지 popup(bframe) 재출현 → 서버 토큰 거절 후 재챌린지
            if page.locator("iframe[title*='reCAPTCHA 보안문자']").count() > 0:
                box = page.locator("iframe[title*='reCAPTCHA 보안문자']").first.bounding_box()
                if box and box.get("width", 0) > 100 and box.get("height", 0) > 100:
                    rejected_reason = "캡챠 챌린지 재출현 - 토큰 거절"
                    break
        except Exception:
            pass
        page.wait_for_timeout(500)

    # 거절 케이스든 timeout 케이스든 body text 캡처해서 정확한 카페24 메시지 보여주기
    try:
        body_text = page.evaluate("""() => {
            const visible = [];
            // 흔히 에러 메시지가 들어가는 컨테이너 우선
            const sels = ['.eLoginInfo', '.errorBox', '.error', '.alert', '.notice', '.info', '#err_msg', '.tit', 'p', 'div'];
            const seen = new Set();
            for (const sel of sels) {
                document.querySelectorAll(sel).forEach(el => {
                    const t = (el.innerText || '').trim();
                    if (t && t.length > 3 && t.length < 200 && !seen.has(t)) {
                        seen.add(t);
                        visible.push(t);
                    }
                });
                if (visible.length >= 10) break;
            }
            return visible.slice(0, 8).join(' | ');
        }""")
    except Exception:
        body_text = "(unable to read page)"
    cur_url = page.url

    # 디버그용 스크린샷 (account_id 알 수 있으면 저장)
    aid = account.get("id") or "unknown"
    try:
        from pathlib import Path
        Path("data/debug").mkdir(parents=True, exist_ok=True)
        shot = f"data/debug/login_fail_{aid}_{int(_t.time())}.png"
        page.screenshot(path=shot, full_page=True)
        print(f"[login] 실패 스크린샷 저장: {shot}")
    except Exception:
        shot = None

    if rejected_reason:
        raise RuntimeError(f"로그인 거절: {rejected_reason} | url={cur_url} | msg={body_text[:300]!r}")
    raise RuntimeError(f"로그인 URL 변화 timeout (20s) | url={cur_url} | msg={body_text[:300]!r}")


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


def _wait_loading_idle(frame, page, timeout_ms=20000):
    """카페24 분석 페이지의 반투명 로딩 마스크(bg-white/30 z-10)가 사라질 때까지 대기.
    이 마스크가 떠있으면 dropdown/캘린더 클릭이 pointer event 가로채여 timeout 남."""
    try:
        overlay = frame.locator("div.bg-white\\/30").first
        if overlay.count() > 0:
            overlay.wait_for(state="hidden", timeout=timeout_ms)
    except Exception:
        pass
    page.wait_for_timeout(200)


def set_period_range(frame, page, start_date, end_date):
    """기간 설정: 기간 선택 모드 → 시작일 button → 캘린더에서 day → 종료일 button → 캘린더에서 day → 조회.
    카페24 캘린더는 default로 현재 월(이번 달)이 떠있어서 navigate 없이 바로 day 클릭이 정상."""
    start_day = int(start_date.split("-")[2])
    end_day = int(end_date.split("-")[2])

    # 0) default 7일 데이터 로딩이 끝날 때까지 대기 (이게 끝나야 dropdown 클릭 가능)
    _wait_loading_idle(frame, page)

    # 1) 기간 드롭다운 → 기간 선택
    for text in ["7일", "1개월", "3개월", "6개월", "오늘"]:
        b = frame.query_selector(f"button:has-text('{text}')")
        if b and b.evaluate("el => el.textContent?.trim() || ''") == text:
            b.click()
            page.wait_for_timeout(800)
            _wait_loading_idle(frame, page)
            opt = frame.locator("text=기간 선택").first
            if opt.count() > 0:
                try:
                    opt.click(timeout=15000)
                except Exception:
                    # dropdown 이 닫혔으면 버튼 다시 눌러 열고 재시도
                    b.click()
                    page.wait_for_timeout(800)
                    _wait_loading_idle(frame, page)
                    opt = frame.locator("text=기간 선택").first
                    if opt.count() > 0:
                        opt.click(timeout=15000)
                page.wait_for_timeout(1200)
            break

    def _date_btns():
        out = []
        for b in frame.query_selector_all("button"):
            t = b.evaluate("el => el.textContent?.trim() || ''")
            if len(t) == 10 and t[:2] == "20" and t[4] == "-" and t[7] == "-":
                out.append((t, b))
        return out

    def _click_target_date(target_date_str, idx):
        """캘린더 popup이 열려있다 가정. target_date 가 잡히도록 trial-and-error.
        - 현재 캘린더에서 target_d 매칭 enabled (outside/disabled 아닌) 셀 찾아 클릭
        - 클릭 후 idx(0=시작일, 1=종료일) button 텍스트가 target_date 가 됐는지 검증
        - 안 됐으면 prev 한 번 누르고 재시도. 캘린더 popup이 닫혔으면 button 다시 클릭
        - 최대 24개월 prev (RDP 헤더 못 읽어도 작동)."""
        target_d = int(target_date_str[8:10])
        for attempt in range(24):
            cand = None
            for c in frame.query_selector_all("td button"):
                try:
                    if not c.is_visible():
                        continue
                    if c.evaluate("el => el.textContent?.trim() || ''") != str(target_d):
                        continue
                    skip = c.evaluate("""el => {
                        if (el.disabled) return true;
                        if (el.getAttribute('aria-disabled') === 'true') return true;
                        const cls = ' ' + (el.className || '') + ' ';
                        return cls.includes(' day-outside ')
                            || cls.includes(' rdp-day_outside ')
                            || cls.includes(' rdp-day_disabled ');
                    }""")
                    if not skip:
                        cand = c
                        break
                except Exception:
                    continue

            if cand:
                try:
                    cand.click()
                    page.wait_for_timeout(800)
                except Exception:
                    pass
                btns = _date_btns()
                if len(btns) > idx and btns[idx][0] == target_date_str:
                    return True
                # 잘못된 month 였음 → 캘린더 popup 다시 열기
                if len(btns) > idx:
                    try:
                        btns[idx][1].click()
                        page.wait_for_timeout(1000)
                    except Exception:
                        pass

            # target month 아직 안 보임 → prev 한 번
            prev_btn = frame.locator("button[aria-label='Go to previous month']").first
            try:
                if prev_btn.count() > 0 and prev_btn.is_visible():
                    prev_btn.click()
                    page.wait_for_timeout(400)
                else:
                    return False
            except Exception:
                return False
        return False

    # 2) 시작일 button click → 캘린더 popup → start_date 클릭 (trial-and-error month nav)
    btns = _date_btns()
    if len(btns) >= 1:
        btns[0][1].click()
        page.wait_for_timeout(1200)
        _click_target_date(start_date, 0)

    # 3) 종료일 button click → 캘린더 popup → end_date 클릭
    btns = _date_btns()
    if len(btns) >= 2:
        btns[1][1].click()
        page.wait_for_timeout(1200)
        _click_target_date(end_date, 1)

    # 4) 시작/종료 텍스트 검증
    btns = _date_btns()
    if btns:
        print(f"[set_period_range] 적용된 버튼 텍스트: {[t for t,_ in btns]}")

    # 5) 조회 클릭 (이전 로딩이 안 끝났으면 대기)
    _wait_loading_idle(frame, page)
    search_btn = frame.query_selector("button:has-text('조회')")
    if search_btn:
        search_btn.click()
        page.wait_for_timeout(5000)
        _wait_loading_idle(frame, page)


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

    aid = account.get("id", cafe24_id)
    def _phase(name):
        LAST_PHASE[aid] = name
        print(f"[{aid}] phase: {name}", flush=True)

    with sync_playwright() as p:
        _phase("chromium launch")
        browser = p.chromium.launch(headless=False, slow_mo=100)

        session_file = _session_path(account["id"])
        if session_file.exists():
            context = browser.new_context(storage_state=str(session_file))
        else:
            context = browser.new_context()

        # 모든 page/context 호출의 default timeout 강제 60s.
        # 누락된 wait_for_*, click, fill 등이 무한 대기로 chromium hang 시키는 케이스 방지.
        context.set_default_timeout(60000)
        context.set_default_navigation_timeout(60000)
        page = context.new_page()
        sample_detector = _attach_sample_detector(page)
        _phase("ensure_login")
        ensure_login(page, context, account)

        results = {"account": cafe24_id, "date": target_date}

        # 1. 매출분석
        _phase("매출종합분석 진입")
        page.goto(urls["sales"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            _phase("매출종합분석 추출")
            results["매출종합분석"] = scrape_sales(frame, page, period_fn)

        # 2. 방문자분석
        _phase("방문자분석 진입")
        page.goto(urls["visitors"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            _phase("방문자분석 추출")
            results["방문자분석"] = scrape_visitors(frame, page, period_fn)

        # 3. 처음방문vs재방문
        _phase("처음방문vs재방문 진입")
        page.goto(urls["buyers"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            _phase("처음방문vs재방문 추출")
            results["처음방문vs재방문"] = scrape_first_vs_repeat(frame, page, period_fn)

        # 4. 신규회원
        _phase("신규회원 진입")
        page.goto(urls["buyers"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        frame = page.frame("adminFrameContent")
        if frame:
            _phase("신규회원 추출")
            results["신규회원"] = scrape_new_members(frame, page, period_fn)

        # 5/6. 매출종합/구매패턴 전체보기 팝업 (구매개수, 처음·재구매 건수)
        _phase("매출종합 팝업")
        results["매출종합_상세"] = scrape_popup(context, SALES_POPUP_URL, target_date, target_date)
        _phase("구매패턴 팝업")
        results["구매패턴_상세"] = scrape_popup(context, PATTERNS_POPUP_URL, target_date, target_date)

        # 7. 시간 단위 매출
        _phase("시간별 매출")
        try:
            if _is_main_admin(account):
                results["매출종합_시간별"] = scrape_popup_hourly(context, SALES_POPUP_URL, target_date)
            else:
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
        _phase("세션 저장")
        context.storage_state(path=str(session_file))
        _phase("browser close")
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

        # 모든 page/context 호출의 default timeout 강제 60s.
        # 누락된 wait_for_*, click, fill 등이 무한 대기로 chromium hang 시키는 케이스 방지.
        context.set_default_timeout(60000)
        context.set_default_navigation_timeout(60000)
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


def scrape_product_analytics(account):
    """카페24 애널리틱스 '상품 분석' 페이지 → 베스트/급상승/전환율/판매액 top 5 추출.
    무료 등급에서 노출되는 데이터만 가져옴 (Premium '전체보기'는 사용 안 함)."""
    import re as _re
    cafe24_id = account["cafe24_id"]
    aid = account.get("id", cafe24_id)
    base = f"https://{cafe24_id}.cafe24.com"

    def _phase(name):
        print(f"[{aid}] product phase: {name}", flush=True)

    with sync_playwright() as p:
        _phase("chromium launch")
        browser = p.chromium.launch(headless=False, slow_mo=100)
        session_file = _session_path(account["id"])
        if session_file.exists():
            context = browser.new_context(storage_state=str(session_file))
        else:
            context = browser.new_context()
        context.set_default_timeout(60000)
        context.set_default_navigation_timeout(60000)
        page = context.new_page()
        _phase("ensure_login")
        ensure_login(page, context, account)

        # 카페24 애널리틱스 dashboard 로딩
        _phase("dashboard 진입")
        page.goto(f"{base}/disp/admin/shop1/menu/cafe24analytics?type=best",
                  wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(6000)

        # iframe 안의 '상품 분석' 클릭 → /products/by-product
        _phase("상품 분석 클릭")
        af = next((f for f in page.frames if f.name == "adminFrameContent"), None)
        if not af:
            browser.close()
            raise RuntimeError("adminFrameContent iframe 못 찾음")
        try:
            af.locator("text=상품 분석").first.click(timeout=10000)
        except Exception as e:
            browser.close()
            raise RuntimeError(f"'상품 분석' 클릭 실패: {e}")
        page.wait_for_timeout(5000)

        af = next((f for f in page.frames if f.name == "adminFrameContent"), None)
        _phase(f"by-product 페이지 (url={af.url})")
        # 추가 wait — table 렌더 완료
        page.wait_for_timeout(3000)

        # Radix tooltip: 각 trigger 를 순차 hover 해서 풀텍스트 읽어옴. 한 번에 하나만 가능.
        # 결과: tooltip 매핑 {trigger-button-index → fulltext}
        _phase("tooltip 풀텍스트 수집")
        tooltip_map = af.evaluate(r"""async () => {
            const sleep = (ms) => new Promise(r => setTimeout(r, ms));
            const triggers = Array.from(document.querySelectorAll('button[data-slot="tooltip-trigger"]'));
            const out = [];
            for (let i = 0; i < triggers.length; i++) {
                const btn = triggers[i];
                // Radix 는 pointer 이벤트로 tooltip 띄움
                const rect = btn.getBoundingClientRect();
                const eventInit = {bubbles: true, cancelable: true, clientX: rect.left + 4, clientY: rect.top + 4, pointerType: 'mouse'};
                btn.dispatchEvent(new PointerEvent('pointerover', eventInit));
                btn.dispatchEvent(new PointerEvent('pointerenter', eventInit));
                btn.dispatchEvent(new MouseEvent('mouseover', eventInit));
                btn.dispatchEvent(new MouseEvent('mouseenter', eventInit));
                btn.focus();
                await sleep(60);
                // tooltip 텍스트 읽기 — aria-describedby 우선, fallback role=tooltip
                let txt = '';
                const id = btn.getAttribute('aria-describedby');
                if (id) {
                    const tt = document.getElementById(id);
                    if (tt) txt = (tt.textContent || '').replace(/\s+/g, ' ').trim();
                }
                if (!txt) {
                    const tt2 = document.querySelector('[role="tooltip"]');
                    if (tt2) txt = (tt2.textContent || '').replace(/\s+/g, ' ').trim();
                }
                out.push({idx: i, text: txt, visible: (btn.textContent || '').trim()});
                // hover 해제
                btn.dispatchEvent(new PointerEvent('pointerleave', eventInit));
                btn.dispatchEvent(new MouseEvent('mouseleave', eventInit));
                btn.blur();
                await sleep(20);
            }
            return out;
        }""")
        # trigger-button 의 DOM 순서대로 매핑된 풀텍스트.
        # 같은 trigger 가 fresh evaluate 에서도 같은 순서로 잡히도록 셀별 trigger 인덱스를 evaluate 안에서 부여함.

        tables = af.evaluate(r"""(tooltipMap) => {
            const triggers = Array.from(document.querySelectorAll('button[data-slot="tooltip-trigger"]'));
            const triggerIndex = new Map();
            triggers.forEach((b, i) => triggerIndex.set(b, i));
            const lookup = new Map();
            (tooltipMap || []).forEach(m => lookup.set(m.idx, m.text));

            const cellFullText = (td) => {
                const tr = td.querySelector('button[data-slot="tooltip-trigger"]');
                if (tr) {
                    const idx = triggerIndex.get(tr);
                    const txt = lookup.get(idx);
                    if (txt) return txt;
                }
                return (td.textContent || '').replace(/\s+/g, ' ').trim();
            };
            const out = [];
            document.querySelectorAll('table').forEach((t, i) => {
                const headers = Array.from(t.querySelectorAll('thead th, thead td')).map(th => (th.textContent || '').replace(/\s+/g, ' ').trim());
                const rows = Array.from(t.querySelectorAll('tbody tr')).map(r =>
                    Array.from(r.querySelectorAll('td')).map(td => ({text: cellFullText(td)}))
                );
                out.push({idx: i, headers, rows});
            });
            return out;
        }""", tooltip_map)

        # category 매핑 — 헤더 키워드로 의미 추정
        def _classify(headers):
            joined = " ".join(headers)
            if "증감" in joined:
                return "급상승_변화량"
            if "노출" in joined and "%" in joined:
                return "전환율_TOP"
            if "판매금액" in joined:
                return "베스트_매출"
            if "판매액" in joined:
                return "판매액_순위"
            return None

        def _parse_product_pair(s):
            """'상품명(상품번호)' → (name, no). 매칭 실패 시 (s, None)."""
            m = _re.match(r"^(.+?)\((\d+)\)$", s.strip())
            if m:
                return m.group(1).strip(), m.group(2)
            return s.strip(), None

        result_rows = []
        for t in tables:
            cat = _classify(t["headers"])
            if not cat:
                continue
            for rank_idx, r in enumerate(t["rows"], start=1):
                if not r:
                    continue
                # 상품명 컬럼 위치
                prod_col = None
                for i, h in enumerate(t["headers"]):
                    if "상품명" in h:
                        prod_col = i
                        break
                if prod_col is None:
                    prod_col = 1 if len(r) > 1 else 0
                cell = r[prod_col] if prod_col < len(r) else {"text": ""}
                name, no = _parse_product_pair(cell.get("text", "") if isinstance(cell, dict) else cell)
                # 순위
                rank = rank_idx
                if t["headers"] and "순위" in t["headers"][0]:
                    first = r[0]
                    try:
                        rank = int(first.get("text", "") if isinstance(first, dict) else first)
                    except (ValueError, IndexError, AttributeError):
                        pass
                # raw 저장 — text 만 평탄화해서 보관 (디버깅용)
                flat_row = [(c.get("text", "") if isinstance(c, dict) else c) for c in r]
                result_rows.append({
                    "category": cat,
                    "rank": rank,
                    "product_no": no,
                    "product_name": name,
                    "raw": {"headers": t["headers"], "row": flat_row},
                })

        _phase(f"추출 완료 — {len(result_rows)}건")
        context.storage_state(path=str(session_file))
        browser.close()

    return result_rows


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
