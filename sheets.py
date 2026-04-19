"""Google Sheets API 연동 - 서비스 계정 인증"""

import json
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
    """'1,234,000' -> 1234000"""
    if not s:
        return 0
    return int(s.replace(",", ""))


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

    return m


def build_row(date_str, metrics):
    """시트 한 행 데이터 생성 (기존 시트 컬럼 순서)"""
    m = metrics

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_label = f"{dt.month:02d}월 {dt.day:02d}일"

    return [
        date_label,                                      # 날짜
        m.get("매출", ""),                                # 매출
        "",                                              # ROI (광고비 필요)
        m.get("방문자수", ""),                             # 방문자수
        m.get("방문당매출", ""),                            # 방문당매출
        m.get("신규방문", ""),                              # 신규방문
        m.get("재방문", ""),                                # 재방문
        m.get("순방문자수", ""),                            # 순방문자수
        f'{m.get("순방문비중", "")}%' if m.get("순방문비중") else "",
        f'{m.get("신규비중", "")}%' if m.get("신규비중") else "",
        f'{m.get("재방문비중", "")}%' if m.get("재방문비중") else "",
        m.get("구매건수", ""),                              # 구매건수
        f'{m.get("전환율", "")}%' if m.get("전환율") else "",
        "",                                              # 구매개수
        "",                                              # 합구매
        "",                                              # 처음구매 건수
        "",                                              # 처음구매 비중
        "",                                              # 재구매 건수
        "",                                              # 재구매비중
        m.get("객단가", ""),                                # 객단가
        m.get("회원가입", ""),                               # 회원가입
        "",                                              # 광고비총액
        "",                                              # 비중
        "",                                              # cac
    ]


def find_date_row(worksheet, date_label):
    """시트에서 날짜 행 찾기 (없으면 빈 행 반환)"""
    col_a = worksheet.col_values(1)
    for i, val in enumerate(col_a):
        if val.strip() == date_label:
            return i + 1  # 1-indexed
    # 없으면 다음 빈 행
    return len(col_a) + 1


def write_result(result, spreadsheet_id=None, sheet_name="test_자동입력"):
    """스크래핑 결과를 Google Sheets에 입력"""
    spreadsheet_id = spreadsheet_id or DEFAULT_SPREADSHEET_ID
    date_str = result.get("date", datetime.now().strftime("%Y-%m-%d"))
    metrics = extract_metrics(result)
    row_data = build_row(date_str, metrics)

    client = get_client()
    spreadsheet = client.open_by_key(spreadsheet_id)

    # 시트 찾기 또는 생성
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=50, cols=len(COLUMNS))
        # 헤더 입력
        worksheet.update("A1", [COLUMNS])
        # 헤더 서식 (볼드)
        worksheet.format("A1:X1", {"textFormat": {"bold": True}})
        print(f"시트 '{sheet_name}' 생성 완료")

    # 날짜 행 찾기
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_label = f"{dt.month:02d}월 {dt.day:02d}일"
    row_num = find_date_row(worksheet, date_label)

    # 데이터 쓰기
    cell_range = f"A{row_num}"
    worksheet.update(cell_range, [row_data])
    print(f"[{sheet_name}] {date_label} 데이터 입력 완료 (행 {row_num})")

    return row_data


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
