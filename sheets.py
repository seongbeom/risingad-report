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


def month_sheet_name(date_str):
    """'2026-04-25' -> '26년 4월'"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"{dt.year - 2000}년 {dt.month}월"


def efficiency_sheet_name(date_str):
    """'2026-04-25' -> '효율_26년4월'"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"효율_{dt.year - 2000}년{dt.month}월"


def extract_metrics(result):
    """스크래핑 결과 → 시트 지표 dict"""
    m = {}

    # 매출종합분석
    sales = result.get("매출종합분석", {})
    if sales.get("매출종합", {}).get("rows"):
        row = sales["매출종합"]["rows"][0]
        m["매출"] = parse_number(row[1])
        m["구매건수"] = parse_number(row[2])

    if sales.get("1인당매출", {}).get("rows"):
        row = sales["1인당매출"]["rows"][0]
        m["방문당매출"] = parse_number(row[1])
        m["객단가"] = parse_number(row[2])

    # 방문자분석
    visitors = result.get("방문자분석", {})
    if visitors.get("전체방문자수", {}).get("rows"):
        row = visitors["전체방문자수"]["rows"][0]
        m["방문자수"] = parse_number(row[1])
        m["신규방문"] = parse_number(row[2])
        m["재방문"] = parse_number(row[3])

    if visitors.get("순방문자수", {}).get("rows"):
        row = visitors["순방문자수"]["rows"][0]
        m["순방문자수"] = parse_number(row[1])

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
        row = buy["처음구매vs재구매"]["rows"][0]
        m["처음구매액"] = parse_number(row[1])
        m["재구매액"] = parse_number(row[2])

    # 신규회원
    members = result.get("신규회원", {})
    if members.get("신규회원수", {}).get("rows"):
        row = members["신규회원수"]["rows"][0]
        m["회원가입"] = parse_number(row[1])

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

    return m


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

    template = spreadsheet.worksheet(SOURCE_EFFICIENCY_SHEET)
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

    print(f"시트 '{name}' 생성 완료 ('{SOURCE_EFFICIENCY_SHEET}' 복제 + 날짜·광고비 갱신)")
    print(f"  ⚠ 다른 raw 데이터(노출/클릭 등 채널별 컬럼)는 이전달 값이 남아있을 수 있음 — 직접 정리 필요")
    return new_ws


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
    spreadsheet_id = spreadsheet_id or DEFAULT_SPREADSHEET_ID
    date_str = result.get("date", datetime.now().strftime("%Y-%m-%d"))
    dt = datetime.strptime(date_str, "%Y-%m-%d")
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
