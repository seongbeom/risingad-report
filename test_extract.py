"""extract_metrics 회귀 테스트 — 월초 7일윈도우 버그 등 재발 방지.
실행: python3 test_extract.py  (gspread 없는 환경 위해 import 최소화)
"""
import sys, importlib.util

# sheets.py 를 gspread 없이 부분 로드하기 위해, 필요한 함수만 직접 가져온다.
# (sheets 상단 import gspread 때문에 모듈 전체 import 가 실패할 수 있어 우회)
spec = importlib.util.spec_from_file_location("_sheets_src", "sheets.py")
try:
    import gspread  # noqa
    import sheets as S
    HAVE = True
except Exception:
    HAVE = False

PASS = 0
FAIL = 0

def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name}")


def make_result(rows, date):
    """매출종합 멀티행 result 모킹."""
    return {
        "date": date,
        "매출종합분석": {"매출종합": {
            "headers": ["일자", "매출액", "구매건수(건)", "매출액 비교", "매출액 증감"],
            "rows": rows,
        }},
    }


def test_pick_row():
    print("[월초 7일윈도우 — 오늘행 선택]")
    # 6/1: 윈도우 5/26~6/1, 첫행=5/26(과거), 끝행=6/1(오늘)
    rows = [
        ["2026-05-26", "9,240,000", "183", "9,518,000", "-278,000"],
        ["2026-05-27", "8,980,000", "190", "9,240,000", "-260,000"],
        ["2026-06-01", "2,465,000", "47", "8,361,000", "-5,896,000"],
    ]
    r = make_result(rows, "2026-06-01")
    m = S.extract_metrics(r)
    check("오늘(6/1) 매출=2,465,000 (첫행 9.24M 아님)", m.get("매출") == 2465000)
    check("오늘 구매건수=47", m.get("구매건수") == 47)

    print("[평소 — 첫행이 오늘인 경우도 정상]")
    rows2 = [["2026-05-15", "5,000,000", "100", "0", "0"]]
    m2 = S.extract_metrics(make_result(rows2, "2026-05-15"))
    check("단일행 매출=5,000,000", m2.get("매출") == 5000000)

    print("[날짜 매칭 실패 시 마지막행 fallback]")
    rows3 = [["2026-05-30", "1,000,000", "10", "0", "0"], ["2026-05-31", "2,000,000", "20", "0", "0"]]
    m3 = S.extract_metrics(make_result(rows3, "2026-06-99"))  # 없는 날짜
    check("매칭실패→마지막행 2,000,000", m3.get("매출") == 2000000)


def test_validate():
    print("[교차검증 — 버그 패턴 감지]")
    # 시간별 합이 종합매출의 20배 (이번 시간별 버그)
    w = S.validate_metrics({"매출": 6749800, "구매건수": 145, "방문자수": 30000, "객단가": 46550},
                           hourly_rows=[{"매출": 138344200}])
    check("시간별합 20배 → 경고", any("시간별합" in x for x in w))

    # 매출 ≠ 객단가×구매건수
    w2 = S.validate_metrics({"매출": 1000000, "구매건수": 10, "객단가": 50000})  # 기대 500k
    check("매출≠객단가×건수 → 경고", any("객단가" in x for x in w2))

    # 정상 데이터 → 경고 없음
    w3 = S.validate_metrics({"매출": 500000, "구매건수": 10, "객단가": 50000, "방문자수": 1000,
                             "신규방문": 600, "재방문": 400, "전환율": 1.0},
                            hourly_rows=[{"매출": 480000}])
    check("정상 데이터 → 경고 0", len(w3) == 0)

    # 전일 대비 10배 급변
    w4 = S.validate_metrics({"매출": 9240000, "구매건수": 183, "객단가": 50491},
                            prev_metrics={"매출": 800000})
    check("전일比 11배 급변 → 경고", any("급변" in x for x in w4))


if __name__ == "__main__":
    if not HAVE:
        print("⚠️  gspread 미설치 환경 — 서버(venv)에서 실행하세요: /opt/cafe24/venv/bin/python test_extract.py")
        sys.exit(0)
    print("=== extract_metrics 회귀 테스트 ===")
    test_pick_row()
    test_validate()
    print(f"\n결과: {PASS} 통과 / {FAIL} 실패")
    sys.exit(1 if FAIL else 0)
