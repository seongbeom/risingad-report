"""네이버 성과형(GFA) 디스플레이 광고 성과 수집 — 개별 광고계정 stats API (세션 크롤).

핵심 API (세션 쿠키 + x-xsrf-token):
  GET ads.naver.com/apis/stats/v1/adAccounts/{adAccountNo}/stats/reportPerformance
      ?startDate=&endDate=&reportAdUnit=AD_ACCOUNT&reportFilterListString=[]&pageNumber=1&pageSize=10
  응답 reportPerformanceDetailResponseList[0]:
    impCount=노출 clickCount=클릭 sales=광고비 purchaseConvCount=구매완료수(전환) purchaseConvSales=구매완료전환매출액(매출)
  ⚠️ 관리계정 합산 보고서는 매출 ₩0 버그 → 반드시 개별계정 API 사용.

설정: data/naver_gfa_session.json (네이버성과형_세션갱신.command 로 갱신)
"""
import datetime
import json
from pathlib import Path

DATA = Path(__file__).parent / "data"
SESSION_FILE = DATA / "naver_gfa_session.json"
SESSION_META = DATA / "naver_gfa_session_meta.json"
WARN_DAYS = 7

# 시트 효율탭 '네이버성과형' 블록 라벨 (매장마다 컬럼 위치 다름 → 동적 탐색)
CHANNEL_LABEL = "네이버성과형"


def session_status():
    """세션 상태. 반환 {ok, days_left, refreshed_at, severity, message}."""
    if not SESSION_FILE.exists() or not SESSION_META.exists():
        return {"ok": False, "days_left": None, "refreshed_at": None,
                "severity": "critical",
                "message": "네이버 성과형 세션 없음 — 최초 로그인 필요(네이버성과형_세션갱신.command)"}
    meta = json.loads(SESSION_META.read_text())
    if meta.get("dead_reason"):
        return {"ok": False, "days_left": None, "refreshed_at": None,
                "severity": "critical",
                "message": "네이버 성과형 세션 만료 — 재로그인 필요(네이버성과형_세션갱신.command)"}
    refreshed = datetime.date.fromisoformat(meta["refreshed_at"])
    valid = int(meta.get("valid_days", 30))
    days_left = (refreshed + datetime.timedelta(days=valid) - datetime.date.today()).days
    if days_left <= 0:
        sev, msg = "critical", f"네이버 성과형 세션 만료(추정) — 재로그인 필요 (갱신일 {refreshed})"
    elif days_left <= WARN_DAYS:
        sev, msg = "warn", f"네이버 성과형 세션 {days_left}일 뒤 만료 — 여유 있을 때 재로그인 권장"
    else:
        sev, msg = "ok", f"네이버 성과형 세션 정상 ({days_left}일 남음)"
    return {"ok": days_left > 0, "days_left": days_left,
            "refreshed_at": meta["refreshed_at"], "severity": sev, "message": msg}


def mark_session_dead(reason=""):
    SESSION_META.parent.mkdir(parents=True, exist_ok=True)
    SESSION_META.write_text(json.dumps({
        "refreshed_at": (datetime.date.today() - datetime.timedelta(days=999)).isoformat(),
        "valid_days": 30, "dead_reason": reason,
    }, ensure_ascii=False, indent=2))


# ===== 수집 (개별 광고계정 stats API) =====
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
STATS_URL = "https://ads.naver.com/apis/stats/v1/adAccounts/{no}/stats/reportPerformance"


def fetch_all(account_nos, days=7, session_path=None):
    """각 광고계정 × 최근 days일(오늘 제외) 일별 성과. 반환 {account_no(str): {date: metrics}}.
    metrics: impressions/clicks/cost/conversions(구매완료수)/revenue(구매완료전환매출액).
    세션 만료 시 mark_session_dead + 예외."""
    import urllib.parse
    from playwright.sync_api import sync_playwright
    sp = session_path or str(SESSION_FILE)
    if not Path(sp).exists():
        mark_session_dead("세션 파일 없음")
        raise RuntimeError("naver_gfa_session.json 없음 — 네이버성과형 로그인 필요")
    account_nos = [str(a) for a in account_nos]
    daydates = [(datetime.date.today() - datetime.timedelta(days=i)).isoformat()
                for i in range(1, days + 1)]  # 어제부터 N일 (GFA 보고서는 오늘 제외)
    out = {}
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        ctx = b.new_context(storage_state=sp, user_agent=UA)
        pg = ctx.new_page()
        pg.goto("https://ads.naver.com/", wait_until="domcontentloaded", timeout=40000)
        pg.wait_for_timeout(2500)
        if "nid.naver" in pg.url or "/login" in pg.url.lower():
            b.close()
            mark_session_dead(f"로그인 튕김 url={pg.url[:50]}")
            raise RuntimeError("네이버 성과형 세션 만료 — 재로그인 필요(네이버성과형_세션갱신.command)")
        xsrf = next((c["value"] for c in ctx.cookies() if c["name"].upper() == "XSRF-TOKEN"), "")
        for no in account_nos:
            hdr = {"x-xsrf-token": xsrf, "accept": "application/json",
                   "referer": f"https://ads.naver.com/manage/ad-accounts/{no}/da/report/performance"}
            daily = {}
            for d in daydates:
                url = (STATS_URL.format(no=no) +
                       f"?startDate={d}&endDate={d}&reportAdUnit=AD_ACCOUNT"
                       f"&reportFilterListString=[]&pageNumber=1&pageSize=10")
                try:
                    resp = ctx.request.get(url, headers=hdr)
                    if resp.status != 200:
                        continue
                    lst = json.loads(resp.text()).get("reportPerformanceDetailResponseList") or []
                    if not lst:
                        continue
                    r = lst[0]
                    daily[d] = {
                        "impressions": int(float(r.get("impCount", 0) or 0)),
                        "clicks": int(float(r.get("clickCount", 0) or 0)),
                        "cost": round(float(r.get("sales", 0) or 0)),
                        "conversions": int(float(r.get("purchaseConvCount", 0) or 0)),
                        "revenue": round(float(r.get("purchaseConvSales", 0) or 0)),
                    }
                except Exception as e:
                    print(f"[gfa] {no} {d} 실패: {repr(e)[:80]}", flush=True)
            out[no] = daily
        b.close()
    return out


# ===== 시트 쓰기 (효율탭 '네이버성과형' 블록, 매장마다 위치 달라 동적 탐색) =====
_METRIC_SUBS = [("노출", "impressions"), ("클릭수", "clicks"), ("광고비", "cost"),
                ("전환", "conversions"), ("매출", "revenue")]


def _gfa_cols(ws):
    """효율시트 [일별 성과] 블록의 '네이버성과형' 지표 컬럼 letter 동적탐색.
    효율탭은 [주차별]/[전일자성과비교]/[일별 성과] 여러 블록에 '네이버성과형'이 중복 등장 →
    반드시 '일별 성과' 블록(날짜행이 따라오는)을 골라야 함(다른 블록 잡으면 엉뚱한 칸 기입).
    (Z=쇼핑박스PC, AJ=쇼핑박스MO 와 헷갈리지 않게 라벨 정확히 '네이버성과형' 매칭)"""
    from gspread.utils import rowcol_to_a1
    grid = ws.get_all_values()
    daily_anchor = 0
    for ri, row in enumerate(grid):
        b = (row[1] if len(row) > 1 else "") or ""
        if "일별" in b and "성과" in b and "비교" not in b:
            daily_anchor = ri
            break
    ch_row = ch_col = None
    for ri in range(daily_anchor, len(grid)):
        for ci, c in enumerate(grid[ri]):
            if (c or "").strip() == CHANNEL_LABEL:
                ch_row, ch_col = ri, ci
                break
        if ch_row is not None:
            break
    if ch_row is None:
        return None
    ch = grid[ch_row]
    nxt = len(ch)
    for i in range(ch_col + 1, len(ch)):
        if (ch[i] or "").strip():
            nxt = i
            break
    for mr in range(ch_row + 1, min(ch_row + 4, len(grid))):
        met = grid[mr]
        cols = {}
        for i in range(ch_col, min(nxt, len(met))):
            label = (met[i] or "").strip()
            for sub, key in _METRIC_SUBS:
                if key not in cols and sub in label:
                    cols[key] = rowcol_to_a1(1, i + 1).rstrip("1")
        if "cost" in cols:
            return cols
    return None


def write_to_sheet(spreadsheet_id, daily):
    """효율탭 네이버성과형 칸에 일자별 기입 (크리테오와 동일 구조, 컬럼 동적탐색)."""
    import sheets
    from collections import defaultdict
    import datetime as _dt
    gc = sheets.get_client()
    sh = gc.open_by_key(sheets.clean_spreadsheet_id(spreadsheet_id))
    by_tab = defaultdict(dict)
    for d, m in daily.items():
        by_tab[sheets.efficiency_sheet_name(d)][d] = m
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
        cols = _gfa_cols(ws)
        if not cols:
            errors.append(f"{eff_name} '네이버성과형' 컬럼 못 찾음 — 스킵")
            continue
        col_b = ws.col_values(2)
        rowmap = {(v or "").strip(): i for i, v in enumerate(col_b, start=1)}
        data = []
        for d, m in days.items():
            row = rowmap.get(d.replace("-", "/"))
            if not row:
                errors.append(f"{d} 행없음")
                continue
            wrote_any = False
            for key in ("impressions", "clicks", "cost", "conversions", "revenue"):
                if key in cols and m.get(key) is not None:
                    data.append({"range": f"{cols[key]}{row}", "values": [[m[key]]]})
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
