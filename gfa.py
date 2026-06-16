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
