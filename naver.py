"""네이버 검색광고 성과 수집 — Search Ad API (HMAC 서명).
효율시트 네이버 검색광고 칸(KH~KO) 자동입력.

검증결과(신데렐라 5/1): 노출/클릭/광고비/매출 API=시트 정확히 일치.
  - 광고비: 부가세 미포함 (메타와 달리 그대로)
  - 전환수: AD_CONVERSION 보고서로 '구매완료(purchase)'만 집계 (ccnt 는 구매+장바구니 합산이라
    CVR 과대 → 2026-06 구매완료만으로 변경). 보고서 실패 시 ccnt 폴백.

계정별 자격: (api_key, secret_key, customer_id) — DB accounts 에 저장.
"""
import base64
import hashlib
import hmac
import json
import os
import time
import urllib.parse
import urllib.request

BASE = "https://api.searchad.naver.com"

# 시트 네이버 검색광고 블록 (1-indexed 컬럼): KH=노출 KI=클릭 KK=광고비 KM=전환수 KO=매출
# KJ(CTR)/KL(CPC)/KN(CVR)/KP(ROAS)/KQ(객단가) 는 시트 수식 → 안 건드림
SHEET_COLS = {"impressions": "KH", "clicks": "KI", "cost": "KK", "conversions": "KM", "revenue": "KO"}


def _sign(secret, ts, method, path):
    msg = f"{ts}.{method}.{path}"
    return base64.b64encode(hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()).decode()


def _call(creds, method, path, query=None):
    key, secret, cid = creds
    ts = str(int(time.time() * 1000))
    url = BASE + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    req = urllib.request.Request(url, method=method)
    req.add_header("X-Timestamp", ts)
    req.add_header("X-API-KEY", key)
    req.add_header("X-Customer", str(cid))
    req.add_header("X-Signature", _sign(secret, ts, method, path))
    with urllib.request.urlopen(req, timeout=40) as r:
        return json.load(r)


def _signed_request(creds, method, path, body=None, full_url=None):
    """POST 바디 / 다운로드 URL 까지 지원하는 서명 요청. raw bytes 반환.
    서명은 항상 path 기준(다운로드도 URL 의 path 부분으로 서명)."""
    key, secret, cid = creds
    ts = str(int(time.time() * 1000))
    url = full_url or (BASE + path)
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-Timestamp", ts)
    req.add_header("X-API-KEY", key)
    req.add_header("X-Customer", str(cid))
    req.add_header("X-Signature", _sign(secret, ts, method, path))
    if data:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def fetch_purchase_conversions(creds, since, until):
    """AD_CONVERSION 대용량 보고서로 '구매완료(purchase)' 전환만 일자별 합산.
    네이버 /stats 의 ccnt 는 구매+장바구니 등 전체라 CVR 이 과대 → 구매완료만 분리.
    보고서 컬럼(탭구분): ... [10]전환유형 [11]전환수 [12]전환매출액.
    반환 {date: {"cnt": int, "amt": int}}. 실패한 날은 키 없음(호출측 ccnt 폴백)."""
    import datetime
    out = {}
    d0 = datetime.datetime.strptime(since, "%Y-%m-%d").date()
    d1 = datetime.datetime.strptime(until, "%Y-%m-%d").date()
    cur = d0
    while cur <= d1:
        ds = cur.strftime("%Y-%m-%d")
        try:
            job = json.loads(_signed_request(
                creds, "POST", "/stat-reports",
                {"reportTp": "AD_CONVERSION", "statDt": ds + "T00:00:00.000Z"}))
            jid = job["reportJobId"]
            dl = None
            for _ in range(30):
                time.sleep(2)
                st = json.loads(_signed_request(creds, "GET", f"/stat-reports/{jid}"))
                if st.get("status") == "BUILT":
                    dl = st.get("downloadUrl"); break
                if st.get("status") in ("ERROR", "NONE"):
                    break
            if dl:
                path = urllib.parse.urlparse(dl).path
                txt = _signed_request(creds, "GET", path, full_url=dl).decode("utf-8", "replace")
                cnt = amt = 0
                for ln in txt.strip().split("\n"):
                    f = ln.split("\t")
                    if len(f) >= 13 and f[10].strip() == "purchase":
                        cnt += int(float(f[11] or 0))
                        amt += int(float(f[12] or 0))
                out[ds] = {"cnt": cnt, "amt": amt}
            # 보고서 정리(쿼터 보호)
            try:
                _signed_request(creds, "DELETE", f"/stat-reports/{jid}")
            except Exception:
                pass
        except Exception as e:
            print(f"[naver] {ds} 구매전환 보고서 실패(ccnt 폴백): {repr(e)[:90]}", flush=True)
        cur += datetime.timedelta(days=1)
    return out


def verify(creds):
    """자격 유효성 — 캠페인 목록 조회로 확인. (개수, 첫 캠페인명) 반환, 실패 시 예외."""
    camps = _call(creds, "GET", "/ncc/campaigns")
    return len(camps), (camps[0]["name"] if camps else None)


def fetch_daily(creds, since, until):
    """일자별 계정 합산 성과. 반환: {date: {impressions,clicks,cost,conversions,revenue}}.
    /stats 는 단일 시점 합산만 줘서 날짜별로 각각 호출."""
    camps = _call(creds, "GET", "/ncc/campaigns")
    ids = [c["nccCampaignId"] for c in camps]
    if not ids:
        return {}
    out = {}
    # 구매완료(purchase) 전환만 먼저 보고서로 확보 (실패한 날은 ccnt 폴백)
    purchases = fetch_purchase_conversions(creds, since, until)
    # since~until 각 날짜 순회
    import datetime
    d0 = datetime.datetime.strptime(since, "%Y-%m-%d").date()
    d1 = datetime.datetime.strptime(until, "%Y-%m-%d").date()
    cur = d0
    fields = ["impCnt", "clkCnt", "salesAmt", "ccnt", "convAmt"]
    while cur <= d1:
        ds = cur.strftime("%Y-%m-%d")
        try:
            res = _call(creds, "GET", "/stats", {
                "ids": ",".join(ids),
                "fields": json.dumps(fields),
                "timeRange": json.dumps({"since": ds, "until": ds}),
            })
            t = {"impressions": 0, "clicks": 0, "cost": 0, "conversions": 0, "revenue": 0}
            ccnt_all = conv_amt_all = 0
            for r in res.get("data", []):
                t["impressions"] += int(r.get("impCnt", 0) or 0)
                t["clicks"] += int(r.get("clkCnt", 0) or 0)
                t["cost"] += int(r.get("salesAmt", 0) or 0)        # 부가세 미포함
                ccnt_all += int(r.get("ccnt", 0) or 0)
                conv_amt_all += int(r.get("convAmt", 0) or 0)
            # 전환수/매출: 구매완료만(보고서) 우선.
            p = purchases.get(ds)
            today_s = datetime.date.today().strftime("%Y-%m-%d")
            if p is not None:
                t["conversions"] = p["cnt"]
                t["revenue"] = p["amt"]
            elif ds == today_s:
                # 당일은 구매완료 보고서가 아직 없음(네이버 미생성). 장바구니 섞인 ccnt 를 쓰면
                # CVR 이 과대 → 전환은 비워두고(집계중) 내일 백필 때 채움. 매출은 그대로(장바구니 금액 0).
                t["conversions"] = None
                t["revenue"] = conv_amt_all
            else:
                t["conversions"] = ccnt_all
                t["revenue"] = conv_amt_all
            out[ds] = t
        except Exception as e:
            print(f"[naver] {ds} stats 실패: {repr(e)[:120]}", flush=True)
        cur += datetime.timedelta(days=1)
    return out


def write_to_sheet(spreadsheet_id, daily):
    """효율탭 네이버 검색광고 칸에 일자별 기입. 탭별 1회 읽기+1회 batch write (쿼터 보호).
    daily: {date: metrics}. 반환 (written, [errors])."""
    import sheets
    from collections import defaultdict
    gc = sheets.get_client()
    sh = gc.open_by_key(sheets.clean_spreadsheet_id(spreadsheet_id))
    by_tab = defaultdict(dict)
    for d, m in daily.items():
        by_tab[sheets.efficiency_sheet_name(d)][d] = m
    written = 0
    errors = []
    import datetime as _dt
    for eff_name, days in by_tab.items():
        try:
            ws = sh.worksheet(eff_name)
        except Exception:
            # 효율 탭 없으면 자동 생성 (월초 재발 버그 방지) — 템플릿 복제
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
            # None(당일 구매완료 집계전)은 빈칸으로 기입 — 장바구니 섞인 값 안 보이게
            conv_v = m["conversions"] if m["conversions"] is not None else ""
            data += [
                {"range": f"{SHEET_COLS['impressions']}{row}", "values": [[m["impressions"]]]},
                {"range": f"{SHEET_COLS['clicks']}{row}", "values": [[m["clicks"]]]},
                {"range": f"{SHEET_COLS['cost']}{row}", "values": [[m["cost"]]]},
                {"range": f"{SHEET_COLS['conversions']}{row}", "values": [[conv_v]]},
                {"range": f"{SHEET_COLS['revenue']}{row}", "values": [[m["revenue"]]]},
            ]
            written += 1
        if data:
            ws.batch_update(data, value_input_option="USER_ENTERED")
            mism = sheets.verify_cells(ws, data)
            if mism:
                errors.append(f"⚠️기입검증실패 {len(mism)}셀(엉뚱한칸 의심): "
                              + "; ".join(f"{r}={w}≠시트{h}" for r, w, h in mism[:3]))
    return written, errors
