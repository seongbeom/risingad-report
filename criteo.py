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

# 전환/매출 어트리뷰션 윈도우: post-click+post-view 30일 (광고주 KPI 합의되면 변경)
CONV_METRIC = "SalesAllPc30d"
REV_METRIC = "RevenueGeneratedAllPc30d"

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
        "timezone": "UTC",
        "currency": CURRENCY,
        "dimensions": ["AdvertiserId", "Day"],
        "metrics": ["Displays", "Clicks", "AdvertiserCost", CONV_METRIC, REV_METRIC],
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
            "conversions": int(float(r.get(CONV_METRIC, 0) or 0)),
            "revenue": round(float(r.get(REV_METRIC, 0) or 0)),
        }
    return out


# ===== 시트 쓰기 (효율 탭 크리테오 칸) =====
# TODO: 효율시트의 크리테오 블록 컬럼 확정 필요 (메타=AZ.., 네이버=KH.. 처럼)
#   확정되면 아래 채우고 write_to_sheet 활성화.
CRITEO_COLS = {"impressions": "", "clicks": "", "cost": "", "conversions": "", "revenue": ""}


def write_to_sheet(spreadsheet_id, daily):
    """효율탭 크리테오 칸에 일자별 기입 (메타/네이버 write 와 동일 구조).
    daily: {date: metrics}. 반환 (written, [errors])."""
    if not all(CRITEO_COLS.values()):
        return 0, ["CRITEO_COLS 미설정 — 효율시트 크리테오 컬럼 확정 후 활성화"]
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
        col_b = ws.col_values(2)
        rowmap = {(v or "").strip(): i for i, v in enumerate(col_b, start=1)}
        data = []
        for d, m in days.items():
            row = rowmap.get(d.replace("-", "/"))
            if not row:
                errors.append(f"{d} 행없음")
                continue
            for key, col in CRITEO_COLS.items():
                data.append({"range": f"{col}{row}", "values": [[m[key]]]})
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
