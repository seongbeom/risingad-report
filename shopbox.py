"""네이버 쇼핑박스(PC 주간 / MO 월간 정액입찰) 시트 기입 + 일별분할.

광고비: 입찰 낙찰가(정액)를 집행기간 일수로 분할 (db.shopbox_daily_cost).
노출·클릭: 쇼핑파트너센터 광고리포트 (v2, 추후).
매출: cafe24 유입분석 UTM (v2, 추후).

효율시트의 '쇼핑박스 PC' / '쇼핑박스 MO' 채널 블록을 동적 탐색해 일별 기입.
(criteo/gfa write_to_sheet 패턴 동일)
"""
import json as _json
import re as _re
from pathlib import Path as _Path

_DATA = _Path(__file__).parent / "data"
_ACCOUNTS_FILE = _DATA / "shopbox_accounts.json"
_GROUPBY_URL = "https://adcenter.shopping.naver.com/p/report/ad/trendreport/groupBy.nhn"
_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def _load_accounts():
    try:
        return _json.loads(_ACCOUNTS_FILE.read_text())
    except Exception:
        return {}


def _device_from_url(url):
    """소재 cntsLinkUrl 의 utm_campaign → 'pc' | 'mo' | None (문서 키: pc / mo / knowledge_shopping=mo)."""
    m = _re.search(r"utm_campaign=([^&]+)", url or "")
    if not m:
        return None
    c = m.group(1).lower()
    if c == "pc":
        return "pc"
    if c == "mo" or "knowledge_shopping" in c:
        return "mo"
    return None


def fetch_metrics(account_id, days=14):
    """쇼핑박스 노출/클릭 수집 (세션 크롤 + groupBy.nhn API).
    로그인 → 대시보드에서 groupBy 요청바디·소재(slotNo→device) 캡처 → groupBy(slotNo,DAY) 재호출.
    반환 {date: {'pc': {impressions,clicks}, 'mo': {...}}}. 자격 없거나 로그인 실패시 예외."""
    import datetime as _dt
    import time as _time
    from playwright.sync_api import sync_playwright
    acc = _load_accounts().get(account_id)
    if not acc:
        raise RuntimeError(f"shopbox 자격 없음: {account_id}")
    profile = _DATA / f"shopbox_profile_{account_id}"
    profile.mkdir(parents=True, exist_ok=True)
    base_body = {"v": None}
    slot_dev = {}

    def _on_req(req):
        if "groupBy.nhn" in req.url and req.method == "POST":
            try:
                b = _json.loads(req.post_data or "{}")
                if b.get("granularity") == "DAY" and base_body["v"] is None:
                    base_body["v"] = b
            except Exception:
                pass

    def _on_resp(resp):
        if "adCntsList.nhn" in resp.url:
            try:
                for r in _json.loads(resp.text()):
                    dev = _device_from_url(r.get("cntsLinkUrl", ""))
                    sn = str(r.get("slotNo") or "")
                    if sn and dev:
                        slot_dev[sn] = dev
            except Exception:
                pass

    out = {}
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            str(profile), headless=True, user_agent=_UA,
            args=["--disable-blink-features=AutomationControlled"],
            viewport={"width": 1500, "height": 980})
        pg = ctx.pages[0] if ctx.pages else ctx.new_page()
        pg.on("request", _on_req)
        pg.on("response", _on_resp)
        pg.goto("https://center.shopping.naver.com/report/ad/dashboard",
                wait_until="domcontentloaded", timeout=60000)
        pg.wait_for_timeout(3000)
        # 로그인 필요시 폼 입력
        if "/login" in pg.url:
            for fr in [pg] + list(pg.frames):
                try:
                    txt = fr.locator("#login_username, input[type=text]").first
                    pw = fr.locator("#login_password, input[type=password]").first
                    if txt.count() and pw.count():
                        txt.fill(acc["id"]); pw.fill(acc["pw"])
                        fr.locator("button:has-text('로그인'), .btn_login, button[type=submit]").first.click(timeout=5000)
                        break
                except Exception:
                    continue
            pg.wait_for_timeout(4000)
            pg.goto("https://center.shopping.naver.com/report/ad/dashboard",
                    wait_until="networkidle", timeout=60000)
        pg.wait_for_load_state("networkidle", timeout=30000)
        pg.wait_for_timeout(6000)
        if base_body["v"] is None:
            ctx.close()
            raise RuntimeError("groupBy 요청 캡처 실패 — 로그인 만료 의심")
        # slotNo 별 일자 노출/클릭 재호출
        today = _dt.date.today()
        body = dict(base_body["v"])
        body["strtDateTime"] = (today - _dt.timedelta(days=days)).strftime("%Y-%m-%dT00:00")
        body["endDateTime"] = (today + _dt.timedelta(days=1)).strftime("%Y-%m-%dT00:00")
        body["granularity"] = "DAY"
        # slotNo + expsTrtrCd(placement코드) 차원. placement코드는 주차 무관 고정이라,
        # 현재주 소재(utm)로 코드→device 를 학습해 과거주 슬롯(소재없음)도 PC/MO 판별 → cross-week 백필.
        body["dimensions"] = ["slotNo", "expsTrtrCd"]
        resp = ctx.request.post(_GROUPBY_URL, data=_json.dumps(body),
                                headers={"content-type": "application/json"})
        recs = _json.loads(resp.text()) if resp.status == 200 else []
        # 1) 현재주 소재로 device 확정된 슬롯 → 그 placement코드를 device로 학습
        code_dev = {}
        for r in recs:
            dev = slot_dev.get(str(r.get("slotNo") or ""))
            code = str(r.get("expsTrtrCd") or "")
            if dev and code:
                code_dev.setdefault(code, dev)
        # 2) 집계: 소재 utm(현재주) 우선, 없으면 학습된 placement코드로 device 판별(과거주)
        for r in recs:
            sn = str(r.get("slotNo") or "")
            dev = slot_dev.get(sn) or code_dev.get(str(r.get("expsTrtrCd") or ""))
            if not dev:
                continue  # device 매핑 실패(소재없고 placement코드도 미학습) 스킵
            ymdhm = r.get("ymdhm") or ""
            if len(ymdhm) < 8:
                continue
            d = f"{ymdhm[:4]}-{ymdhm[4:6]}-{ymdhm[6:8]}"
            slot = out.setdefault(d, {}).setdefault(dev, {"impressions": 0, "clicks": 0})
            slot["impressions"] += int(r.get("vExpsCnt") or 0)
            slot["clicks"] += int(r.get("vClkCnt") or 0)
        ctx.close()
    return out


_CA2_CAMPAIGNS = "https://ca-internal.cafe24data.com/ca2/adsources/campaigns"


def fetch_revenue(account_id, days=14):
    """쇼핑박스 매출 수집 (cafe24 애널리틱스 UTM 유형별분석 = adsources/campaigns).
    cafe24 스토어 세션으로 유입분석(traffic) 페이지 로드 → 자동발생 ca2 요청에서 Bearer 토큰 확보
    → 일자별 adsources/campaigns 재호출 → utm_campaign 으로 PC/MO 매출 합산.
    PC = campaign 'pc', MO = campaign 'mo' + 'knowledge_shopping'(스펙 일치, 타채널은 sa/gfa/cv/숫자라 충돌없음).
    반환 {date: {'pc': revenue, 'mo': revenue}}. 매출 없는 날/디바이스는 생략. tier=premium 필요(없으면 빈값)."""
    import datetime as _dt
    import scraper
    import db
    from playwright.sync_api import sync_playwright
    acc = db.get_account(account_id)
    if not acc:
        raise RuntimeError(f"cafe24 계정 없음: {account_id}")
    base = "https://%s.cafe24.com" % acc["cafe24_id"]
    auth = {"h": None}

    def _on_req(req):
        if "ca-internal.cafe24data.com/ca2/" in req.url and not auth["h"]:
            a = req.headers.get("authorization")
            if a:
                auth["h"] = a

    out = {}
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        sf = scraper._session_path(account_id)
        ctx = b.new_context(storage_state=str(sf)) if sf.exists() else b.new_context()
        ctx.set_default_timeout(60000)
        pg = ctx.new_page()
        pg.on("request", _on_req)
        scraper.ensure_login(pg, ctx, acc)
        pg.goto(base + "/disp/admin/shop1/menu/cafe24analytics?type=traffic",
                wait_until="networkidle", timeout=40000)
        pg.wait_for_timeout(6000)
        if not auth["h"]:
            # 폴백: 유입/광고채널 메뉴 클릭으로 ca2 요청 유발
            outer = next((f for f in pg.frames if "cafe24analytics" in (f.url or "")), pg.main_frame)
            for label in ("유입 분석", "광고 채널 분석"):
                try:
                    loc = outer.get_by_text(label, exact=False).first
                    if loc.count():
                        loc.click(force=True, timeout=4000)
                        pg.wait_for_timeout(4000)
                except Exception:
                    pass
                if auth["h"]:
                    break
        af = next((f for f in pg.frames if "cafe24data" in (f.url or "")), None)
        if not auth["h"] or af is None:
            b.close()
            raise RuntimeError("cafe24 애널리틱스 토큰/프레임 확보 실패 — 세션 만료 의심")
        today = _dt.date.today()
        for i in range(0, days + 1):
            d = today - _dt.timedelta(days=i)
            ds = d.strftime("%Y-%m-%d")
            q = (f"device_type=total&start_date={ds}&end_date={ds}&sort=order_amount"
                 f"&order=desc&limit=300&conversion_timeframe=30d&offset=0&tier=premium")
            try:
                r = af.evaluate(
                    """async (args)=>{const[u,a]=args;
                       const r=await fetch("https://ca-internal.cafe24data.com/ca2/adsources/campaigns?"+u,{headers:{authorization:a}});
                       return [r.status, await r.text()];}""",
                    [q, auth["h"]])
            except Exception:
                continue
            if r[0] != 200:
                continue
            try:
                arr = _json.loads(r[1]).get("campaigns", [])
            except Exception:
                continue
            pc = sum(int(x.get("order_amount") or 0) for x in arr if x.get("campaign") == "pc")
            mo = sum(int(x.get("order_amount") or 0) for x in arr
                     if x.get("campaign") in ("mo", "knowledge_shopping"))
            dev = {}
            if pc:
                dev["pc"] = pc
            if mo:
                dev["mo"] = mo
            if dev:
                out[ds] = dev
        b.close()
    return out


DEVICE_LABELS = {
    "pc": ["쇼핑박스 PC", "쇼핑박스PC", "네이버 쇼핑박스 PC"],
    "mo": ["쇼핑박스 MO", "쇼핑박스MO", "네이버 쇼핑박스 MO (트렌드픽)", "네이버 쇼핑박스 MO"],
}
# 채널 블록 안에서 찾을 지표 (있는 것만 기입 — 쇼핑박스는 보통 노출/클릭/광고비/매출)
_METRIC_SUBS = [("노출", "impressions"), ("클릭", "clicks"), ("광고비", "cost"), ("매출", "revenue")]


def _shopbox_cols(grid, device):
    """효율시트 [일별 성과] 블록에서 device('pc'|'mo') 쇼핑박스 채널의 지표 컬럼 letter.
    효율탭은 [주차별 성과비교]/[주차별 성과]/[전일자 성과비교]/[일별 성과] 여러 블록이 있고
    쇼핑박스 라벨이 여러 블록 헤더에 중복 등장 → 반드시 '일별 성과' 블록(날짜행이 따라오는)을 골라야 함.
    grid = ws.get_all_values() (전체). 반환 {impressions?,clicks?,cost,revenue?} (cost 필수). 없으면 None."""
    from gspread.utils import rowcol_to_a1
    labels = DEVICE_LABELS[device]
    # 1) [일별 성과] 섹션 시작행 (B열 마커) — 없으면 0부터
    daily_anchor = 0
    for ri, row in enumerate(grid):
        b = (row[1] if len(row) > 1 else "") or ""
        if "일별" in b and "성과" in b and "비교" not in b:
            daily_anchor = ri
            break
    # 2) anchor 이후 채널 라벨 행
    ch_row = ch_col = None
    for ri in range(daily_anchor, len(grid)):
        for ci, c in enumerate(grid[ri]):
            if (c or "").strip() in labels:
                ch_row, ch_col = ri, ci
                break
        if ch_row is not None:
            break
    if ch_row is None:
        return None
    # 3) 채널 블록 너비 = 다음(다른) 채널 라벨 전까지
    ch = grid[ch_row]
    nxt = len(ch)
    for i in range(ch_col + 1, len(ch)):
        if (ch[i] or "").strip():
            nxt = i
            break
    # 4) 지표 라벨 행 — 헤더 다음 1~3행 중 노출/클릭/광고비/매출 있는 행
    cols = {}
    for mr in range(ch_row + 1, min(ch_row + 4, len(grid))):
        met = grid[mr]
        found = {}
        for i in range(ch_col, min(nxt, len(met))):
            label = (met[i] or "").strip()
            for sub, key in _METRIC_SUBS:
                if key not in found and sub in label:
                    found[key] = rowcol_to_a1(1, i + 1).rstrip("1")
        if found:
            cols = found
            break
    return cols if "cost" in cols else None


def write_to_sheet(spreadsheet_id, daily):
    """daily = {date: {'pc': {cost,impressions,clicks,revenue}, 'mo': {...}}} 효율탭 기입.
    각 device 블록을 동적탐색해, 그 블록에 존재하는 지표만 기입. 반환 (written, errors)."""
    import sheets
    from collections import defaultdict
    import datetime as _dt
    gc = sheets.get_client()
    sh = gc.open_by_key(sheets.clean_spreadsheet_id(spreadsheet_id))
    by_tab = defaultdict(dict)
    for d, devmap in daily.items():
        by_tab[sheets.efficiency_sheet_name(d)][d] = devmap
    written, errors = 0, []
    for eff_name, days in by_tab.items():
        try:
            ws = sh.worksheet(eff_name)
        except Exception:
            try:
                _d = _dt.datetime.strptime(next(iter(days)), "%Y-%m-%d")
                ws = sheets._ensure_efficiency_sheet(sh, _d)
            except Exception as ce:
                errors.append(f"{eff_name} 자동생성 실패: {repr(ce)[:50]}")
                continue
        grid = ws.get_all_values()
        cols = {dev: _shopbox_cols(grid, dev) for dev in ("pc", "mo")}
        if not any(cols.values()):
            errors.append(f"{eff_name} 쇼핑박스 PC/MO 칸 못 찾음 — 스킵")
            continue
        # 날짜→행: [일별 성과] 블록이 뒤쪽이라 마지막 매칭이 일별행 (dict 후승)
        rowmap = {(row[1] if len(row) > 1 else "").strip(): i
                  for i, row in enumerate(grid, start=1)}
        data = []
        for d, devmap in days.items():
            row = rowmap.get(d.replace("-", "/"))
            if not row:
                errors.append(f"{d} 행없음")
                continue
            wrote_any = False
            for dev in ("pc", "mo"):
                c = cols.get(dev)
                m = devmap.get(dev)
                if not c or not m:
                    continue
                for key, colletter in c.items():
                    if m.get(key) is not None:
                        data.append({"range": f"{colletter}{row}", "values": [[m[key]]]})
                        wrote_any = True
            if wrote_any:
                written += 1
        if data:
            ws.batch_update(data, value_input_option="USER_ENTERED")
            mism = sheets.verify_cells(ws, data)
            if mism:
                errors.append(f"⚠️기입검증실패 {len(mism)}셀(엉뚱한칸 의심): "
                              + "; ".join(f"{r}={w}≠시트{h}" for r, w, h in mism[:3]))
    return written, errors
