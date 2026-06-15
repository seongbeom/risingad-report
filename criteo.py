"""Criteo Marketing Solutions API 성과 수집 — Authorization Code 방식.

앱 1개(client_id/secret)로 consent 1회 받으면, /advertisers/me 가 권한받은
advertiser 전부를 반환 → advertiser_id 바꿔가며 14개 매장 일별 성과 수집.
브라우저 없이 HTTP 만 사용 (메타/네이버 모듈과 동일 구조).

토큰: access_token 15분 / refresh_token 6개월. data/criteo_token.json 에 보관,
매 호출 전 만료 체크 후 refresh.

설정(.env): CRITEO_CLIENT_ID, CRITEO_CLIENT_SECRET, CRITEO_REDIRECT_URI
"""
import json
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path

API = "https://api.criteo.com"
CONSENT = "https://consent.criteo.com"
VERSION = "2026-01"
CURRENCY = "KRW"

# 전환/매출 어트리뷰션: 그들의 '전일보고서' 기준 = 클릭 후 7일(post-click 7day).
# (영상 분석: 디스플레이수/클릭수/비용/매출(클릭후1일·7일)/ROAS 컬럼 사용)
CONV_METRIC = "SalesPc7d"          # 전환수(클릭후 7일)
REV_METRIC = "RevenueGeneratedPc7d"  # 전환매출(클릭후 7일)
CONV_METRIC_1D = "SalesPc1d"
REV_METRIC_1D = "RevenueGeneratedPc1d"
TIMEZONE = "Asia/Seoul"  # 전일보고서 타임존과 일치

TOKEN_FILE = Path(__file__).parent / "data" / "criteo_token.json"


def _cfg():
    cid = os.environ.get("CRITEO_CLIENT_ID", "")
    secret = os.environ.get("CRITEO_CLIENT_SECRET", "")
    redirect = os.environ.get("CRITEO_REDIRECT_URI", "")
    return cid, secret, redirect


# ===== OAuth =====
def consent_url(state="cafe24"):
    """광고주 동의 URL. 브라우저로 열어 승인 → redirect_uri?code=... 로 돌아옴."""
    cid, _, redirect = _cfg()
    if not (cid and redirect):
        raise RuntimeError("CRITEO_CLIENT_ID / CRITEO_REDIRECT_URI 미설정")
    q = urllib.parse.urlencode({
        "response_type": "code",
        "client_id": cid,
        "redirect_uri": redirect,
        "state": state,
    })
    return f"{CONSENT}/request?{q}"


def _token_post(body):
    data = urllib.parse.urlencode(body).encode()
    req = urllib.request.Request(f"{API}/oauth2/token", data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=40) as r:
        return json.load(r)


def _save_token(tok):
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    # access_token 은 15분 — 만료시각 기록(60초 여유)
    tok["_expires_at"] = time.time() + int(tok.get("expires_in", 900)) - 60
    TOKEN_FILE.write_text(json.dumps(tok, ensure_ascii=False, indent=2))
    return tok


def exchange_code(code):
    """authorization code → access_token + refresh_token (최초 1회, consent 직후)."""
    cid, secret, redirect = _cfg()
    tok = _token_post({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect,
        "client_id": cid,
        "client_secret": secret,
    })
    return _save_token(tok)


def _refresh():
    cid, secret, _ = _cfg()
    if not TOKEN_FILE.exists():
        raise RuntimeError("criteo_token.json 없음 — consent(exchange_code) 먼저 필요")
    cur = json.loads(TOKEN_FILE.read_text())
    rt = cur.get("refresh_token")
    if not rt:
        raise RuntimeError("refresh_token 없음 — 재동의 필요")
    tok = _token_post({
        "grant_type": "refresh_token",
        "refresh_token": rt,
        "client_id": cid,
        "client_secret": secret,
    })
    # refresh 응답에 새 refresh_token 이 없으면 기존 것 유지
    tok.setdefault("refresh_token", rt)
    return _save_token(tok)


def _access_token():
    """유효한 access_token 반환 (만료시 자동 refresh)."""
    if TOKEN_FILE.exists():
        cur = json.loads(TOKEN_FILE.read_text())
        if cur.get("access_token") and time.time() < cur.get("_expires_at", 0):
            return cur["access_token"]
    return _refresh()["access_token"]


def _api(method, path, body=None):
    token = _access_token()
    url = f"{API}/{VERSION}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if data:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


# ===== 데이터 =====
def list_advertisers():
    """consent 받은 advertiser 목록. 반환 [{"id","name"}]."""
    res = _api("GET", "/advertisers/me")
    out = []
    for d in res.get("data", []):
        out.append({"id": str(d.get("id")),
                    "name": (d.get("attributes") or {}).get("advertiserName", "")})
    return out


def fetch_daily(advertiser_id, since, until):
    """advertiser 일별 성과. 반환 {date: {impressions,clicks,cost,conversions,revenue}}.
    since/until: 'YYYY-MM-DD'. spend(AdvertiserCost)는 부가세 미포함."""
    body = {
        "advertiserIds": str(advertiser_id),
        "startDate": f"{since}T00:00:00.000Z",
        "endDate": f"{until}T00:00:00.000Z",
        "format": "json",
        "timezone": TIMEZONE,
        "currency": CURRENCY,
        "dimensions": ["AdvertiserId", "Day"],
        "metrics": ["Displays", "Clicks", "AdvertiserCost",
                    CONV_METRIC, REV_METRIC, CONV_METRIC_1D, REV_METRIC_1D],
    }
    res = _api("POST", "/statistics/report", body)
    out = {}
    # json 응답: {"Rows":[{...}]} 또는 {"data":[...]} — 둘 다 방어
    rows = res.get("Rows") or res.get("rows") or res.get("data") or []
    for r in rows:
        # 날짜 키는 'Day' (값 예: '2026-06-01')
        day = r.get("Day") or r.get("day")
        if not day:
            continue
        day = str(day)[:10]
        out[day] = {
            "impressions": int(float(r.get("Displays", 0) or 0)),
            "clicks": int(float(r.get("Clicks", 0) or 0)),
            "cost": round(float(r.get("AdvertiserCost", 0) or 0)),
            "conversions": int(float(r.get(CONV_METRIC, 0) or 0)),       # 클릭후 7일
            "revenue": round(float(r.get(REV_METRIC, 0) or 0)),           # 클릭후 7일
            "conversions_1d": int(float(r.get(CONV_METRIC_1D, 0) or 0)),
            "revenue_1d": round(float(r.get(REV_METRIC_1D, 0) or 0)),
        }
    return out


# ===== 크롤 세션 상태/경고 (criteo_login.py 가 저장한 메타 기반) =====
SESSION_FILE = Path(__file__).parent / "data" / "criteo_session.json"
SESSION_META = Path(__file__).parent / "data" / "criteo_session_meta.json"
WARN_DAYS = 7  # 만료 N일 전부터 경고


def session_status():
    """크롤 세션 상태. 반환 {ok, days_left, refreshed_at, severity, message}.
    severity: ok / warn / critical (없음/만료)."""
    import datetime
    if not SESSION_FILE.exists() or not SESSION_META.exists():
        return {"ok": False, "days_left": None, "refreshed_at": None,
                "severity": "critical",
                "message": "크리테오 세션 없음 — 최초 로그인 필요(criteo_login)"}
    meta = json.loads(SESSION_META.read_text())
    refreshed = datetime.date.fromisoformat(meta["refreshed_at"])
    valid = int(meta.get("valid_days", 30))
    days_left = (refreshed + datetime.timedelta(days=valid) - datetime.date.today()).days
    if days_left <= 0:
        sev, msg = "critical", f"크리테오 세션 만료(추정) — 재로그인 필요 (갱신일 {refreshed})"
    elif days_left <= WARN_DAYS:
        sev, msg = "warn", f"크리테오 세션 {days_left}일 뒤 만료 — 여유 있을 때 재로그인 권장"
    else:
        sev, msg = "ok", f"크리테오 세션 정상 ({days_left}일 남음)"
    return {"ok": days_left > 0, "days_left": days_left,
            "refreshed_at": meta["refreshed_at"], "severity": sev, "message": msg}


def mark_session_dead(reason=""):
    """크롤 중 로그인 페이지로 튕겼을 때 호출 — 메타를 만료 처리해 다음 상태체크가 경고."""
    import datetime
    SESSION_META.parent.mkdir(parents=True, exist_ok=True)
    SESSION_META.write_text(json.dumps({
        "refreshed_at": (datetime.date.today() - datetime.timedelta(days=999)).isoformat(),
        "valid_days": 30, "dead_reason": reason,
    }, ensure_ascii=False, indent=2))


# ===== 시트 쓰기 (효율 탭 크리테오 칸) =====
# 매장마다 채널 컬럼 위치가 달라(예: dazs01은 GI가 모비온) → '크리테오' 라벨을 시트에서
# 동적 탐색해서 노출/클릭/광고비/전환/매출 컬럼을 자동 결정. (메타처럼 하드코딩하면 오기입)
CHANNEL_LABEL = "크리테오"
# 지표 라벨(부분일치) → 우리 키. 채널 블록 내 첫 매칭 컬럼 사용.
_METRIC_SUBS = [("노출", "impressions"), ("클릭수", "clicks"), ("광고비", "cost"),
                ("전환", "conversions"), ("매출", "revenue")]


def _criteo_cols(ws):
    """시트에서 '크리테오' 블록의 {impressions,clicks,cost,conversions,revenue} 컬럼 letter 탐색.
    헤더 영역(채널 라벨행 + 바로 아래 지표행) 기준. 없으면 None."""
    from gspread.utils import rowcol_to_a1
    grid = ws.get("A28:OZ34")  # 헤더 영역(일별성과 채널/지표 라벨)
    ch_row = None
    for ri, row in enumerate(grid):
        if any((c or "").strip() == CHANNEL_LABEL for c in row):
            ch_row = ri
            break
    if ch_row is None:
        return None
    ch = grid[ch_row]
    cri_c = next(i for i, c in enumerate(ch) if (c or "").strip() == CHANNEL_LABEL)
    nxt = len(ch)
    for i in range(cri_c + 1, len(ch)):
        if (ch[i] or "").strip():  # 다음 채널 라벨 → 블록 끝
            nxt = i
            break
    met = grid[ch_row + 1] if ch_row + 1 < len(grid) else []
    cols = {}
    for i in range(cri_c, min(nxt, len(met))):
        label = (met[i] or "").strip()
        for sub, key in _METRIC_SUBS:
            if key not in cols and sub in label:
                cols[key] = rowcol_to_a1(1, i + 1).rstrip("1")
    if not all(k in cols for _, k in _METRIC_SUBS):
        return None
    return cols


def write_to_sheet(spreadsheet_id, daily):
    """효율탭 크리테오 칸에 일자별 기입 (메타/네이버 write 와 동일 구조, 단 컬럼은 동적 탐색).
    daily: {date: metrics}. 반환 (written, [errors])."""
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
        cols = _criteo_cols(ws)
        if not cols:
            errors.append(f"{eff_name} '크리테오' 컬럼 못 찾음 — 스킵")
            continue
        col_b = ws.col_values(2)
        rowmap = {(v or "").strip(): i for i, v in enumerate(col_b, start=1)}
        data = []
        for d, m in days.items():
            row = rowmap.get(d.replace("-", "/"))
            if not row:
                errors.append(f"{d} 행없음")
                continue
            for key in ("impressions", "clicks", "cost", "conversions", "revenue"):
                data.append({"range": f"{cols[key]}{row}", "values": [[m[key]]]})
            written += 1
        if data:
            ws.batch_update(data, value_input_option="USER_ENTERED")
    return written, errors


# ===== CLI (부트스트랩/테스트) =====
if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
    if cmd == "consent":
        print(consent_url())
    elif cmd == "exchange":
        tok = exchange_code(sys.argv[2])
        print("저장됨. refresh_token 길이:", len(tok.get("refresh_token", "")))
    elif cmd == "advertisers":
        for a in list_advertisers():
            print(a["id"], a["name"])
    elif cmd == "report":
        adv, since, until = sys.argv[2], sys.argv[3], sys.argv[4]
        print(json.dumps(fetch_daily(adv, since, until), ensure_ascii=False, indent=2))
    else:
        print("usage: criteo.py [consent | exchange <code> | advertisers | report <advId> <since> <until>]")
