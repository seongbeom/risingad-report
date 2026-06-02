"""Google Sheets API 연동 - 서비스 계정 인증"""

import json
import re
from datetime import datetime
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

SERVICE_ACCOUNT_FILE = Path(__file__).parent / "service_account.json"
DATA_DIR = Path(__file__).parent / "data"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

# 기본 스프레드시트 ID
DEFAULT_SPREADSHEET_ID = "1ePBlTcMUS0FdEQUnVuOLZIuFYa6_Y9nX1wGrX61ELOg"

# 시트 컬럼 순서 (기존 시트와 동일)
COLUMNS = [
    "날짜", "매출", "ROI", "방문자수", "방문당매출", "신규방문", "재방문",
    "순방문자수", "순방문비중", "신규비중", "재방문비중", "구매건수", "전환율",
    "구매개수", "합구매", "처음구매", "처음구매 비중", "재구매", "재구매비중",
    "객단가", "회원가입", "광고비총액", "비중", "cac",
]


def get_client():
    creds = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_FILE), scopes=SCOPES)
    return gspread.authorize(creds)


def parse_number(s):
    """'1,234,000' / '265개' / '186명' / '187건' -> 1234000 / 265 / 186 / 187"""
    if not s:
        return 0
    cleaned = re.sub(r"[^\d-]", "", str(s))
    return int(cleaned) if cleaned else 0


# 템플릿 시트 이름 (월별 시트 생성 시 복제 원본)
TEMPLATE_SHEET = "26년 4월"
SOURCE_EFFICIENCY_SHEET = "효율_26년4월"


def clean_spreadsheet_id(raw):
    """붙여넣기 실수 방지 — URL/쿼리 꼬리표가 섞여도 순수 스프레드시트 ID만 추출.
    예) '.../d/<ID>/edit?gid=..' , '<ID>/edit#gid=..' , 'https://docs.google.com/...' → '<ID>'"""
    raw = (raw or "").strip()
    if not raw:
        return raw
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", raw)
    if m:
        return m.group(1)
    # 'ID/edit?...' 또는 'ID#...' 또는 순수 ID — 첫 ID 토큰만
    m = re.match(r"([a-zA-Z0-9_-]{20,})", raw)
    if m:
        return m.group(1)
    return raw


def month_sheet_name(date_str):
    """'2026-04-25' -> '26년 4월'"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"{dt.year - 2000}년 {dt.month}월"


def efficiency_sheet_name(date_str):
    """'2026-04-25' -> '효율_26년4월'"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"효율_{dt.year - 2000}년{dt.month}월"


def _cell(row, idx, default=0):
    """row[idx]를 parse_number로 변환. row 가 짧거나 None 이면 default. 부분 테이블 방어용."""
    if not row or len(row) <= idx:
        return default
    return parse_number(row[idx])


def _pick_row(rows, target_date):
    """여러 날짜 행 중 target_date(YYYY-MM-DD) 와 일치하는 행 선택.
    cafe24 가 기간(예: 최근7일) 테이블을 돌려줄 때 첫 행이 오늘이 아닐 수 있어
    (월 첫날 등) A열 일자가 target 과 맞는 행을 골라야 한다.
    일자 매칭 실패 시 마지막 행(보통 최신일) → 그것도 없으면 첫 행."""
    if not rows:
        return None
    if target_date:
        # A열 일자 정규화 비교 (2026-06-01 / 2026.06.01 / 2026/06/01 모두 허용)
        tnorm = target_date.replace("-", "").replace(".", "").replace("/", "")
        for r in rows:
            if r and r[0]:
                rnorm = str(r[0]).strip().replace("-", "").replace(".", "").replace("/", "").replace(" ", "")
                if rnorm == tnorm:
                    return r
    # 매칭 실패 — 단일행이면 그거, 멀티행이면 마지막(최신)
    return rows[-1] if len(rows) > 1 else rows[0]


def extract_metrics(result):
    """스크래핑 결과 → 시트 지표 dict.
    cafe24 가 부분/빈 테이블을 돌려주는 경우가 있어 모든 row 인덱싱은 _cell 로 방어한다."""
    m = {}
    missing = []
    target_date = result.get("date")

    # 매출종합분석 — cafe24 가 기간(최근7일) 테이블을 줄 때 target_date 행을 골라야 함
    sales = result.get("매출종합분석", {})
    if sales.get("매출종합", {}).get("rows"):
        row = _pick_row(sales["매출종합"]["rows"], target_date)
        m["매출"] = _cell(row, 1)
        m["구매건수"] = _cell(row, 2)
        if not row or len(row) < 3:
            missing.append(f"매출종합 row 컬럼 부족 ({row})")
    else:
        missing.append("매출종합 rows 없음")

    if sales.get("1인당매출", {}).get("rows"):
        row = _pick_row(sales["1인당매출"]["rows"], target_date)
        m["방문당매출"] = _cell(row, 1)
        m["객단가"] = _cell(row, 2)

    # 방문자분석
    visitors = result.get("방문자분석", {})
    if visitors.get("전체방문자수", {}).get("rows"):
        row = _pick_row(visitors["전체방문자수"]["rows"], target_date)
        m["방문자수"] = _cell(row, 1)
        m["신규방문"] = _cell(row, 2)
        m["재방문"] = _cell(row, 3)

    if visitors.get("순방문자수", {}).get("rows"):
        row = _pick_row(visitors["순방문자수"]["rows"], target_date)
        m["순방문자수"] = _cell(row, 1)

    # 비중/전환율 계산
    v = m.get("방문자수", 0)
    if v > 0:
        m["순방문비중"] = round(m.get("순방문자수", 0) / v * 100, 1)
        m["신규비중"] = round(m.get("신규방문", 0) / v * 100)
        m["재방문비중"] = round(m.get("재방문", 0) / v * 100)
        m["전환율"] = round(m.get("구매건수", 0) / v * 100, 2)

    # 처음구매vs재구매
    buy = result.get("처음방문vs재방문", {})
    if buy.get("처음구매vs재구매", {}).get("rows"):
        row = _pick_row(buy["처음구매vs재구매"]["rows"], target_date)
        m["처음구매액"] = _cell(row, 1)
        m["재구매액"] = _cell(row, 2)

    # 신규회원
    members = result.get("신규회원", {})
    if members.get("신규회원수", {}).get("rows"):
        row = _pick_row(members["신규회원수"]["rows"], target_date)
        m["회원가입"] = _cell(row, 1)

    # 매출종합_상세 팝업 → 구매개수 (당일 row만)
    sd = result.get("매출종합_상세") or {}
    # 키가 int(1) 또는 str("1") 둘 다 가능 (json 직렬화 후 str)
    sd_t1 = sd.get(1) or sd.get("1") or {}
    if sd_t1.get("rows"):
        # 당일 일자에 해당하는 row 찾기 (없으면 첫 row)
        target_date = result.get("date")
        target = next((r for r in sd_t1["rows"] if r and r[0] == target_date), sd_t1["rows"][0])
        if target and len(target) >= 4:
            m["구매개수"] = parse_number(target[3])

    # 구매패턴_상세 팝업 → 처음구매 건수, 재구매 건수
    pd_ = result.get("구매패턴_상세") or {}
    pd_t1 = pd_.get(1) or pd_.get("1") or {}
    if pd_t1.get("rows"):
        target_date = result.get("date")
        target = next((r for r in pd_t1["rows"] if r and r[0] == target_date), pd_t1["rows"][0])
        if target and len(target) >= 6:
            m["처음구매"] = parse_number(target[4])
            m["재구매"] = parse_number(target[5])

    # 합구매(=구매개수/구매건수), 처음구매 비중(=처음구매/구매건수)
    cnt = m.get("구매건수", 0)
    if cnt > 0:
        if "구매개수" in m:
            m["합구매"] = round(m["구매개수"] / cnt, 2)
        if "처음구매" in m:
            m["처음구매비중"] = round(m["처음구매"] / cnt * 100, 2)

    if missing:
        print(f"[extract_metrics] 일부 테이블 비정상: {missing}")
    return m


def validate_metrics(metrics, hourly_rows=None, prev_metrics=None, is_partial=False):
    """저장 전 데이터 정합성 교차검증. 논리적으로 안 맞는 값을 잡아냄.
    반환: list of 경고 문자열 (비어있으면 정상).
    이번 '월초 7일윈도우 첫행' 같은 버그를 사람 눈 없이 당일 잡기 위함.
    is_partial=True (오늘 진행중 누적)면 '전일 대비 급변'(하루 총합 비교) 체크는 건너뜀 —
    아침엔 오늘 누적이 어제 종일보다 작아 무조건 오경보가 나기 때문."""
    warns = []
    sales = metrics.get("매출") or 0
    cnt = metrics.get("구매건수") or 0
    visitors = metrics.get("방문자수") or 0
    aov = metrics.get("객단가") or 0
    new_v = metrics.get("신규방문") or 0
    re_v = metrics.get("재방문") or 0
    conv = metrics.get("전환율")

    # 1) 매출 ≈ 객단가 × 구매건수 (±10%)
    if sales > 0 and aov > 0 and cnt > 0:
        expect = aov * cnt
        if abs(sales - expect) > expect * 0.12:
            warns.append(f"매출({sales:,}) ≠ 객단가×구매건수({expect:,}) — 12%+ 오차")

    # 2) 시간별 합 ≈ 종합매출 (시간별 데이터 있을 때)
    if hourly_rows and sales > 0:
        hsum = sum((r.get("매출") or 0) for r in hourly_rows)
        if hsum > sales * 1.5:
            warns.append(f"시간별합({hsum:,}) > 종합매출({sales:,})×1.5 — 기간 오류 의심")
        elif hsum > 0 and hsum < sales * 0.5:
            warns.append(f"시간별합({hsum:,}) < 종합매출({sales:,})×0.5 — 부분수집 의심")

    # 3) 신규+재방문 ≈ 방문자수 (±5%)
    if visitors > 0 and (new_v + re_v) > 0:
        s = new_v + re_v
        if abs(s - visitors) > visitors * 0.05:
            warns.append(f"신규+재방문({s:,}) ≠ 방문자수({visitors:,})")

    # 4) 전환율 ≈ 구매건수/방문자 (있을 때, ±0.3%p)
    if conv is not None and visitors > 0 and cnt >= 0:
        calc = cnt / visitors * 100
        if abs(conv - calc) > 0.5:
            warns.append(f"전환율({conv}%) ≠ 구매÷방문({calc:.2f}%)")

    # 5) 전일 대비 급변 (10배↑ 또는 1/10↓) — 이번 버그 패턴(9.24M이 실제론 2.4M)
    #    오늘 진행중(is_partial) 데이터는 하루 총합 비교가 무의미 → 마감된 날에만 검사.
    if (not is_partial) and prev_metrics and (prev_metrics.get("매출") or 0) > 0 and sales > 0:
        ratio = sales / prev_metrics["매출"]
        if ratio > 8 or ratio < 0.12:
            warns.append(f"매출 전일 대비 {ratio:.1f}배 급변 ({prev_metrics['매출']:,}→{sales:,})")

    return warns


DEFAULT_HOURLY_HEADERS = ["일시", "구매자수", "구매건수", "구매개수", "매출액", "비교값", "증감"]


def extract_hourly_rows(result):
    """result['매출종합_시간별'] popup 테이블에서 시간 row 파싱.
    cafe24 시간단위 매출종합 컬럼: ['일시', '구매자수', '구매건수', '구매개수', '매출액', '비교값', '증감'].
    매출 적은 계정은 매출 0인 시간 행을 cafe24가 생략해서 1~3행만 올 수도 있으므로 row count 필터 안 함.
    헤더에 '일시'/'시간' 포함된 테이블이 hourly (합계 테이블은 '구분'으로 시작).
    반환: list of dict {hour, 매출, 구매건수, 객단가, 매출액비교, 매출액증감}.
    """
    section = result.get("매출종합_시간별", {})
    if not isinstance(section, dict):
        return []
    table = None
    for v in section.values():
        if not isinstance(v, dict):
            continue
        headers = v.get("headers") or []
        if any("일시" in h or "시간" in h for h in headers) and v.get("rows"):
            table = v
            break
    if not table:
        return []

    headers = table.get("headers") or []

    def col_index(*candidates):
        # 1) 실제 헤더에서 매칭
        for cand in candidates:
            for i, h in enumerate(headers):
                if cand in h:
                    return i
        # 2) 헤더가 truncate 됐을 수 있음 (cafe24가 적은 데이터 day엔 headers 일부만 줌)
        # 표준 컬럼 순서로 fallback
        for cand in candidates:
            for i, h in enumerate(DEFAULT_HOURLY_HEADERS):
                if cand in h:
                    return i
        return None

    i_hour = col_index("일시", "시간") or 0
    i_sales = col_index("매출액", "매출")
    i_orders = col_index("구매건수")
    i_compare = col_index("비교값", "매출액비교", "비교")
    i_change = col_index("증감", "매출액증감")

    out = []
    seen_hours = set()
    for row in table.get("rows", []):
        if not row:
            continue
        ts = str(row[i_hour]) if i_hour < len(row) else ""
        m = re.search(r"(\d{1,2})\s*시", ts) or re.search(r"\b(\d{1,2}):\d{2}", ts) or re.search(r"\s(\d{1,2})$", ts) or re.search(r"^(\d{1,2})$", ts)
        if not m:
            continue
        h = int(m.group(1))
        if not 0 <= h <= 23 or h in seen_hours:
            continue
        seen_hours.add(h)
        get = lambda i: parse_number(row[i]) if (i is not None and i < len(row)) else 0
        매출 = get(i_sales)
        구매건수 = get(i_orders)
        out.append({
            "hour": h,
            "매출": 매출,
            "구매건수": 구매건수,
            "객단가": (매출 // 구매건수) if 구매건수 else 0,
            "매출액비교": get(i_compare),
            "매출액증감": get(i_change),
        })
    return out


def find_date_row(worksheet, dt):
    """시트에서 날짜(datetime)에 해당하는 행 찾기.
    A열은 날짜 시리얼(예 46129) 또는 'MM월 DD일' 텍스트 둘 다 호환."""
    serial = (dt - datetime(1899, 12, 30)).days
    label_candidates = {
        f"{dt.month:02d}월 {dt.day:02d}일",
        f"{dt.month}월 {dt.day}일",
    }
    col_a = worksheet.col_values(1)
    for i, val in enumerate(col_a):
        v = val.strip()
        if not v:
            continue
        if v in label_candidates:
            return i + 1
        if v.isdigit() and int(v) == serial:
            return i + 1
    return None


def _ensure_efficiency_sheet(spreadsheet, dt):
    """효율_26년X월이 없으면 효율_26년4월 복제 후 날짜·광고비 비움.
    월별 시트의 V열이 이 시트의 G33~G63을 참조하므로 월별 시트 생성 전에 호출돼야 함."""
    import calendar
    name = efficiency_sheet_name(dt.strftime("%Y-%m-%d"))
    try:
        return spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        pass

    # 소스 템플릿(효율_26년4월) 우선, 없으면 이 스프레드시트의 가장 최근 효율 탭으로 복제
    try:
        template = spreadsheet.worksheet(SOURCE_EFFICIENCY_SHEET)
    except gspread.exceptions.WorksheetNotFound:
        def _eff_key(title):
            m = re.match(r"효율_(\d+)년(\d+)월", title)
            return (int(m.group(1)), int(m.group(2))) if m else (-1, -1)
        eff_tabs = [w for w in spreadsheet.worksheets() if w.title.startswith("효율_")]
        if not eff_tabs:
            raise  # 효율 탭이 하나도 없음 → 진짜 수동 셋업 필요
        template = max(eff_tabs, key=lambda w: _eff_key(w.title))
        print(f"  ℹ 템플릿 '{SOURCE_EFFICIENCY_SHEET}' 없음 → '{template.title}' 복제로 대체")
    new_ws = template.duplicate(new_sheet_name=name, insert_sheet_index=template.index + 1)

    # B33~B63: 새 월의 1~말일 (남는 행은 빈값)
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    date_updates = []
    for day in range(1, 32):
        if day <= last_day:
            date_updates.append([datetime(dt.year, dt.month, day).strftime("%Y/%m/%d")])
        else:
            date_updates.append([""])
    new_ws.update("B33:B63", date_updates, value_input_option="USER_ENTERED")

    # G33~G63: 광고비 비우기 (사용자가 새 월에 직접 입력)
    new_ws.update("G33:G63", [[""] for _ in range(31)], value_input_option="USER_ENTERED")

    # 복제 원본(지난달)의 '입력값'이 일별 데이터 행에 그대로 남으므로 제거 — 수식·헤더는 보존.
    # (안 지우면 새 월 탭의 미래 날짜에 지난달 숫자가 그대로 보임)
    _clear_eff_stale_inputs(new_ws)

    print(f"시트 '{name}' 생성 완료 ('{SOURCE_EFFICIENCY_SHEET}' 복제 + 날짜·광고비 갱신 + 잔존 입력값 정리)")
    return new_ws


def _col_letter(i):
    s = ""; i += 1
    while i:
        i, r = divmod(i - 1, 26); s = chr(65 + r) + s
    return s


def _clear_eff_stale_inputs(ws):
    """효율탭 일별 데이터 행(B열=날짜)에서 '리터럴 입력값'만 비움. 수식·헤더·날짜는 보존.
    클론 직후 지난달 잔존 데이터 제거용. 실패해도 생성 자체엔 영향 없음."""
    try:
        valf = ws.get("A33:ZZ70", value_render_option="FORMATTED_VALUE")
        forf = ws.get("A33:ZZ70", value_render_option="FORMULA")
        clears = []
        for ri, vr in enumerate(valf):
            d = vr[1].strip() if len(vr) > 1 else ""
            if not re.match(r"20\d\d/\d\d/\d\d", d):
                continue  # 날짜 행만 (헤더/구분 행은 건너뜀)
            fr = forf[ri] if ri < len(forf) else []
            for ci in range(2, len(vr)):  # A·B(날짜) 보존
                if vr[ci].strip() not in ("", "-") and not (ci < len(fr) and str(fr[ci]).startswith("=")):
                    clears.append(_col_letter(ci) + str(33 + ri))
        if clears:
            ws.batch_update([{"range": c, "values": [[""]]} for c in clears],
                            value_input_option="USER_ENTERED")
            print(f"  ↳ 클론 잔존 입력값 {len(clears)}칸 정리")
    except Exception as e:
        print(f"  ⚠ 잔존 입력값 정리 실패(생성은 정상): {repr(e)[:60]}")


def _ensure_month_sheet(spreadsheet, sheet_name, dt):
    """월별 시트가 없으면 템플릿(26년 4월) 복제 후 데이터/날짜 갱신.
    효율 시트가 없으면 그것도 같이 자동 생성."""
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        pass

    # 효율 시트 먼저 생성 (월별 시트의 V열이 효율 시트를 참조)
    _ensure_efficiency_sheet(spreadsheet, dt)

    template = spreadsheet.worksheet(TEMPLATE_SHEET)
    new_ws = template.duplicate(new_sheet_name=sheet_name, insert_sheet_index=0)

    # A열의 날짜 시리얼을 새 월로 갱신: row 2~32 = 1일~31일
    import calendar
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    base = datetime(1899, 12, 30)
    a_updates = []
    for day in range(1, 32):
        row_num = day + 1  # row 2 = 1일
        if day <= last_day:
            d = datetime(dt.year, dt.month, day)
            serial = (d - base).days
            a_updates.append([serial])
        else:
            a_updates.append([""])
    new_ws.update(f"A2:A32", a_updates, value_input_option="USER_ENTERED")

    # 데이터 컬럼(B,D~M,N,O,P,Q,R,T,U) 비우기 (수식 컬럼 C/S/W/X 보존)
    blank_rows = [[""] * 24 for _ in range(31)]  # 사용 안함 - 컬럼별로 처리
    blank_b = [[""] for _ in range(31)]
    blank_dm = [[""] * 10 for _ in range(31)]   # D~M (10 cols)
    blank_nr = [[""] * 5 for _ in range(31)]    # N~R (5 cols)
    blank_tu = [[""] * 2 for _ in range(31)]    # T~U (2 cols)
    new_ws.batch_update([
        {"range": "B2:B32", "values": blank_b},
        {"range": "D2:M32", "values": blank_dm},
        {"range": "N2:R32", "values": blank_nr},
        {"range": "T2:U32", "values": blank_tu},
    ], value_input_option="USER_ENTERED")

    # V열의 광고비 시트 참조를 새 월의 효율 시트로 치환
    src_eff = SOURCE_EFFICIENCY_SHEET            # 효율_26년4월
    dst_eff = efficiency_sheet_name(dt.strftime("%Y-%m-%d"))  # 효율_26년X월
    v_formulas = new_ws.get(f"V2:V32", value_render_option="FORMULA")
    new_v = []
    for row in v_formulas:
        cell = row[0] if row else ""
        if isinstance(cell, str) and src_eff in cell:
            cell = cell.replace(src_eff, dst_eff)
        new_v.append([cell])
    new_ws.update("V2:V32", new_v, value_input_option="USER_ENTERED")

    print(f"시트 '{sheet_name}' 생성 완료 (템플릿 '{TEMPLATE_SHEET}' 복제 + 광고비 참조 → '{dst_eff}')")
    return new_ws


def write_result(result, spreadsheet_id=None, sheet_name=None):
    """스크래핑 결과를 Google Sheets에 입력. sheet_name 미지정 시 'YY년 M월' 자동.
    수식 컬럼(C ROI, S 재구매비중, W 비중, X cac, V 광고비총액)은 건드리지 않음."""
    spreadsheet_id = clean_spreadsheet_id(spreadsheet_id or DEFAULT_SPREADSHEET_ID)
    date_str = result.get("date", datetime.now().strftime("%Y-%m-%d"))
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    # 비정상 날짜 가드: 스크래퍼가 캘린더에서 엉뚱한 달을 클릭하면 23년 1월 같은
    # 과거 데이터가 들어오는 경우가 있음. 미래 또는 90일 이전 날짜는 시트 안 건드림.
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    delta_days = (today - dt).days
    if delta_days < 0 or delta_days > 90:
        print(f"[write_result] 비정상 날짜 감지 ({date_str}, today에서 {delta_days}일) - 시트 입력 스킵")
        return None
    sheet_name = sheet_name or month_sheet_name(date_str)

    metrics = extract_metrics(result)

    client = get_client()
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = _ensure_month_sheet(spreadsheet, sheet_name, dt)

    row_num = find_date_row(worksheet, dt)
    if row_num is None:
        print(f"[{sheet_name}] {date_str} 행을 찾을 수 없음 - skip")
        return None

    m = metrics
    pct = lambda v: f"{v}%" if v != "" and v is not None else ""

    # B (매출)
    b_val = m.get("매출", "")
    # D~M (방문자수, 방문당매출, 신규방문, 재방문, 순방문자수, 순방문비중, 신규비중, 재방문비중, 구매건수, 전환율)
    d_m_vals = [
        m.get("방문자수", ""),
        m.get("방문당매출", ""),
        m.get("신규방문", ""),
        m.get("재방문", ""),
        m.get("순방문자수", ""),
        pct(m.get("순방문비중", "")),
        pct(m.get("신규비중", "")),
        pct(m.get("재방문비중", "")),
        m.get("구매건수", ""),
        pct(m.get("전환율", "")),
    ]
    # N~R (구매개수, 합구매, 처음구매, 처음구매 비중, 재구매)
    n_r_vals = [
        m.get("구매개수", ""),
        m.get("합구매", ""),
        m.get("처음구매", ""),
        pct(m.get("처음구매비중", "")),
        m.get("재구매", ""),
    ]
    # T~U (객단가, 회원가입)
    t_u_vals = [m.get("객단가", ""), m.get("회원가입", "")]

    worksheet.batch_update([
        {"range": f"B{row_num}", "values": [[b_val]]},
        {"range": f"D{row_num}:M{row_num}", "values": [d_m_vals]},
        {"range": f"N{row_num}:R{row_num}", "values": [n_r_vals]},
        {"range": f"T{row_num}:U{row_num}", "values": [t_u_vals]},
    ], value_input_option="USER_ENTERED")

    print(f"[{sheet_name}] {date_str} 데이터 입력 완료 (행 {row_num})")
    return row_num


def write_all_results(account_id, spreadsheet_id=None, sheet_name=None):
    """계정의 모든 결과 파일을 시트에 입력"""
    account_dir = DATA_DIR / account_id
    if not account_dir.exists():
        print(f"결과 디렉토리 없음: {account_dir}")
        return

    sheet_name = sheet_name or f"test_{account_id}"
    for result_file in sorted(account_dir.glob("*.json")):
        with open(result_file, encoding="utf-8") as f:
            result = json.load(f)
        try:
            write_result(result, spreadsheet_id, sheet_name)
        except Exception as e:
            print(f"오류 ({result_file.name}): {e}")


if __name__ == "__main__":
    import sys

    if not SERVICE_ACCOUNT_FILE.exists():
        print(f"서비스 계정 키 파일이 필요합니다: {SERVICE_ACCOUNT_FILE}")
        print("GCP 콘솔에서 서비스 계정 JSON 키를 다운로드하여 위 경로에 넣어주세요.")
        sys.exit(1)

    # humandaily 결과를 테스트 시트에 입력
    write_all_results("humandaily", sheet_name="test_자동입력")
