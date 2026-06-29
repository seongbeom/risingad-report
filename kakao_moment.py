"""카카오모먼트 광고 성과 수집 — 비즈니스 토큰 API (apis.moment.kakao.com).

캠페인을 유형별로 분류해 일별 노출/클릭/광고비를 상품(DA/모객/메세지)별 집계 → 효율시트 기입.
  - 카카오 DA    : DISPLAY / TALK_BIZ_BOARD / PRODUCT_CATALOG (구매·방문·도달)
  - 카카오 모객  : objective.detailType == ADD_FRIEND (플친늘리기)
  - 카카오 메세지: objective.detailType == SEND_MESSAGE (TALK_CHANNEL 메시지)
  - (DAUM_SHOPPING = 다음 쇼핑박스, 별도 채널)

인증: data/kakao_token.json (비즈니스 토큰, refresh 자동). 발급/갱신은 /kakao/start.
리포트 rate limit: 5초 throttle(429) → 호출 간 간격 + 백오프.
"""
import datetime
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DATA = Path(__file__).parent / "data"
APP = DATA / "kakao_app.json"
TOKEN = DATA / "kakao_token.json"
TOKEN_META = DATA / "kakao_token_meta.json"
TYPE_CACHE = DATA / "kakao_campaign_types.json"

BASE = "https://apis.moment.kakao.com/openapi/v4"
TOKEN_URL = "https://kauth.kakao.com/oauth/business/token"
WARN_DAYS = 10

# 카카오모먼트 광고계정 ID → 우리 cafe24 매장(account_id)
ACCOUNT_MAP = {
    "243711": "cinderella1009",   # 신데렐라 자사몰 메세지
    "375451": "cinderella1009",   # 신데렐라 보조(OFF)
    "142306": "ghostbin",         # 아리엘(=아리엘스타일 매장)
    "816426": "vient24",          # 비엔트
    "622541": "awesomestyle1004", # 어썸스타일
    "895979": "humandaily",       # 휴먼데일리
    # "278193": "alilang415",     # 미스유+옷잘+줌마 공용계정 — 옷잘 단독분리 불가 → 제외(사용자 요청)
    "891999": "mrseon",           # 보라카이 라이징(=보라카이맨)
}

# 시트 채널 라벨 (효율시트 [일별 성과] 블록, 매장마다 컬럼 위치 다름 → 동적탐색)
PRODUCT_LABELS = {
    "da": "카카오 DA",
    "moac": "카카오 모객",
    "msg": "카카오 메세지",
}
REPORT_THROTTLE = 5.2   # 초 (카카오 5초 throttle + 여유)
BATCH = 5               # campaignId 배치 크기 (카카오 한도=5)


def _cfg():
    return json.loads(APP.read_text())


def session_status():
    """토큰 상태 (refresh_token 기준 만료 추정). criteo/gfa 와 동일 포맷."""
    if not TOKEN.exists() or not TOKEN_META.exists():
        return {"ok": False, "days_left": None, "refreshed_at": None,
                "severity": "critical",
                "message": "카카오모먼트 토큰 없음 — 최초 인증 필요(/kakao/start)"}
    meta = json.loads(TOKEN_META.read_text())
    refreshed = datetime.date.fromisoformat(meta["refreshed_at"])
    valid = int(meta.get("valid_days", 60))
    days_left = (refreshed + datetime.timedelta(days=valid) - datetime.date.today()).days
    if days_left <= 0:
        sev, msg = "critical", f"카카오모먼트 토큰 만료(추정) — 재인증 필요 (/kakao/start, 갱신일 {refreshed})"
    elif days_left <= WARN_DAYS:
        sev, msg = "warn", f"카카오모먼트 토큰 {days_left}일 뒤 만료 — 재인증 권장(/kakao/start)"
    else:
        sev, msg = "ok", f"카카오모먼트 토큰 정상 ({days_left}일 남음)"
    return {"ok": days_left > 0, "days_left": days_left,
            "refreshed_at": meta["refreshed_at"], "severity": sev, "message": msg}


# ===== 토큰 =====
def _load_token():
    return json.loads(TOKEN.read_text())


def _refresh(tok):
    cfg = _cfg()
    form = {"grant_type": "refresh_token", "client_id": cfg["rest_api_key"].strip(),
            "refresh_token": tok["refresh_token"]}
    secret = (cfg.get("client_secret") or "").strip()
    if secret:
        form["client_secret"] = secret
    req = urllib.request.Request(TOKEN_URL, data=urllib.parse.urlencode(form).encode(), method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        new = json.loads(r.read().decode())
    merged = dict(tok)
    merged.update(new)
    TOKEN.write_text(json.dumps(merged, ensure_ascii=False, indent=2))
    rt_exp = new.get("refresh_token_expires_in")
    if rt_exp:
        TOKEN_META.write_text(json.dumps({
            "refreshed_at": datetime.date.today().isoformat(),
            "valid_days": int(rt_exp // 86400)}, ensure_ascii=False, indent=2))
    return merged


class _Api:
    def __init__(self):
        self.tok = _load_token()
        self.at = self.tok["access_token"]
        self._last_report = 0.0

    def _req(self, path, acc=None, throttle=False):
        if throttle:
            wait = REPORT_THROTTLE - (time.time() - self._last_report)
            if wait > 0:
                time.sleep(wait)
        h = {"Authorization": "Bearer " + self.at}
        if acc:
            h["adAccountId"] = str(acc)
        url = path if path.startswith("http") else BASE + path
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=40) as r:
                body = r.read().decode()
            if throttle:
                self._last_report = time.time()
            return 200, body
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if e.code == 401:  # access token 만료 → refresh 후 1회 재시도
                self.tok = _refresh(self.tok)
                self.at = self.tok["access_token"]
                return self._req(path, acc, throttle)
            if e.code == 429:  # rate limit → 백오프 후 재시도(1회)
                time.sleep(REPORT_THROTTLE)
                if throttle:
                    self._last_report = time.time()
                try:
                    with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=40) as r:
                        return 200, r.read().decode()
                except urllib.error.HTTPError as e2:
                    return e2.code, e2.read().decode()
            return e.code, body

    def list_accounts(self):
        st, body = self._req("/adAccounts")
        return json.loads(body).get("content", []) if st == 200 else []

    def list_campaigns(self, acc):
        st, body = self._req("/campaigns", acc)
        return json.loads(body).get("content", []) if st == 200 else []

    def campaign_type(self, acc, cid):
        st, body = self._req(f"/campaigns/{cid}", acc)
        if st != 200:
            return None
        d = json.loads(body)
        ctg = d.get("campaignTypeGoal") or {}
        obj = d.get("objective") or {}
        return ctg.get("campaignType"), obj.get("detailType")

    def campaign_report(self, acc, cids, day):
        """campaignId 배치의 일별 BASIC 지표. 반환 {campaign_id: {imp,click,cost}}."""
        csv = ",".join(str(i) for i in cids)
        st, body = self._req(
            f"/campaigns/report?campaignId={csv}&start={day}&end={day}&metricsGroup=BASIC",
            acc, throttle=True)
        out = {}
        if st == 200:
            for row in json.loads(body).get("data", []):
                cid = (row.get("dimensions") or {}).get("campaign_id")
                m = row.get("metrics") or {}
                out[str(cid)] = {"impressions": int(m.get("imp", 0) or 0),
                                 "clicks": int(m.get("click", 0) or 0),
                                 "cost": round(float(m.get("cost", 0) or 0))}
        return st, out

    def campaign_conv(self, acc, cids, day):
        """campaignId 배치의 일별 전환/매출 (PIXEL_SDK_CONVERSION).
        전환수=conv_purchase_7d, 매출=conv_purchase_p_7d (클릭후 7일 어트리뷰션, criteo/gfa와 통일).
        반환 {campaign_id: {conversions, revenue}}."""
        csv = ",".join(str(i) for i in cids)
        st, body = self._req(
            f"/campaigns/report?campaignId={csv}&start={day}&end={day}&metricsGroup=PIXEL_SDK_CONVERSION",
            acc, throttle=True)
        out = {}
        if st == 200:
            for row in json.loads(body).get("data", []):
                cid = (row.get("dimensions") or {}).get("campaign_id")
                m = row.get("metrics") or {}
                out[str(cid)] = {"conversions": int(m.get("conv_purchase_7d", 0) or 0),
                                 "revenue": round(float(m.get("conv_purchase_p_7d", 0) or 0))}
        return st, out

    def account_report(self, acc, day):
        """계정 총합 cost(BASIC) — 전체 광고비(spend유무 판단·잔차 계산용). 반환 int 또는 None."""
        st, body = self._req(
            f"/adAccounts/report?start={day}&end={day}&metricsGroup=BASIC", acc, throttle=True)
        if st != 200:
            return None
        data = json.loads(body).get("data", [])
        if not data:
            return 0
        return round(float((data[0].get("metrics") or {}).get("cost", 0) or 0))

    def account_msg_send(self, acc, day):
        """계정 단위 메시지 발송수(msg_send). ⚠️ MESSAGE 그룹의 cost는 메시지전용이 아니라
        계정총합이라 못 씀 → 발송수만 신호로 사용, 메시지 광고비는 잔차로 계산."""
        st, body = self._req(
            f"/adAccounts/report?start={day}&end={day}&metricsGroup=MESSAGE", acc, throttle=True)
        if st != 200:
            return 0
        data = json.loads(body).get("data", [])
        if not data:
            return 0
        return int((data[0].get("metrics") or {}).get("msg_send", 0) or 0)


def _classify(campaign_type, detail_type):
    if detail_type == "SEND_MESSAGE":
        return "msg"
    if detail_type == "ADD_FRIEND":
        return "moac"
    if campaign_type == "DAUM_SHOPPING":
        return "daum"   # 다음 쇼핑박스(별도 채널 — 현재 미기입)
    return "da"


def _load_type_cache():
    if TYPE_CACHE.exists():
        try:
            return json.loads(TYPE_CACHE.read_text())
        except Exception:
            return {}
    return {}


def fetch_all(days=7, account_map=None):
    """카카오모먼트 매장별 일별 상품(da/moac/msg) 노출/클릭/광고비.
    반환 {store_account_id: {date(YYYY-MM-DD): {product: {impressions,clicks,cost}}}}."""
    amap = account_map or ACCOUNT_MAP
    api = _Api()
    type_cache = _load_type_cache()
    today = datetime.date.today()
    daydates = [(today - datetime.timedelta(days=i)).isoformat() for i in range(1, days + 1)]

    out = {}
    recon = []  # 정합성: (store, date, account_total, captured_sum, gap)
    for kakao_acc, store in amap.items():
        camps = api.list_campaigns(kakao_acc)
        if not camps:
            continue
        # 캠페인 유형 분류 (캐시 우선, 신규만 상세조회)
        prod_of = {}
        new_cached = False
        for cp in camps:
            cid = str(cp["id"])
            key = f"{kakao_acc}:{cid}"
            if key in type_cache:
                prod_of[cid] = type_cache[key]
            else:
                ct = api.campaign_type(kakao_acc, cid)
                prod = _classify(*ct) if ct else "da"
                type_cache[key] = prod
                prod_of[cid] = prod
                new_cached = True
        if new_cached:
            TYPE_CACHE.write_text(json.dumps(type_cache, ensure_ascii=False, indent=2))

        all_ids = [str(cp["id"]) for cp in camps]
        store_out = out.setdefault(store, {})
        for d in daydates:
            day = d.replace("-", "")
            # spend 있는 날만 캠페인 분해 (계정 총합 0이면 스킵)
            total = api.account_report(kakao_acc, day)
            if not total:
                continue
            day_prod = store_out.setdefault(d, {})
            captured = 0
            for i in range(0, len(all_ids), BATCH):
                batch = all_ids[i:i + BATCH]
                st, rep = api.campaign_report(kakao_acc, batch, day)       # 노출/클릭/광고비
                stc, conv = api.campaign_conv(kakao_acc, batch, day)       # 전환/매출
                for cid in (set(rep) | set(conv)):
                    b = rep.get(cid, {}); cv = conv.get(cid, {})
                    imp = b.get("impressions", 0); clk = b.get("clicks", 0); cst = b.get("cost", 0)
                    cn = cv.get("conversions", 0); rv = cv.get("revenue", 0)
                    if not (imp or clk or cst or cn or rv):
                        continue
                    captured += cst
                    prod = prod_of.get(cid, "da")
                    if prod == "daum":
                        continue  # 다음쇼핑박스(정액)는 카카오 채널 아님 → 미기입
                    p = day_prod.setdefault(prod, {"impressions": 0, "clicks": 0, "cost": 0,
                                                   "conversions": 0, "revenue": 0})
                    p["impressions"] += imp; p["clicks"] += clk; p["cost"] += cst
                    p["conversions"] += cn; p["revenue"] += rv
            # 메시지 광고비: 캠페인엔 안 잡히고 계정 잔차로만 존재. msg_send>0(발송 있던 날)의
            # 잔차(계정총합 − 캠페인합)를 메세지로 귀속 → 절대 계정총합 초과 안 함(이중집계 방지).
            # (잔차에 다음쇼핑박스 정액이 섞일 수 있으나, 발송 있는 날만 잡아 과대만 차단)
            residual = total - captured
            if residual > 0 and api.account_msg_send(kakao_acc, day) > 0:
                p = day_prod.setdefault("msg", {"impressions": 0, "clicks": 0, "cost": 0,
                                                "conversions": 0, "revenue": 0})
                p["cost"] = residual
                captured += residual
            # 정합성: 계정총합 vs 분류합(DA+모객+메세지) 차이(=다음쇼핑박스 정액 등). 미분류 기록.
            gap = total - captured
            recon.append((store, d, total, captured, gap))
    out["_recon"] = recon
    return out


# ===== 시트 기입 (효율탭 [일별 성과] 블록, 매장마다 컬럼 위치 달라 동적탐색) =====
# 상품별 지표 (시트에 존재하는 칸만 기입 — present-keys-only)
_METRIC_SUBS = [("노출", "impressions"), ("클릭", "clicks"), ("광고비", "cost"),
                ("전환", "conversions"), ("매출", "revenue"), ("친구추가", "friends")]
# 각 상품이 기입 시도하는 키 (시트에 없는 칸은 자동 스킵)
_PRODUCT_KEYS = {
    "da":   ["impressions", "clicks", "cost", "conversions", "revenue"],
    "moac": ["impressions", "clicks", "cost"],
    "msg":  ["cost", "revenue"],
}


def _kakao_cols(grid, product):
    """효율시트 [일별 성과] 블록에서 카카오 상품(da/moac/msg) 지표 컬럼 letter 동적탐색.
    채널 라벨이 [주차별]/[전일자]/[일별성과] 여러 블록에 중복 등장 → 반드시 [일별 성과] 블록만."""
    from gspread.utils import rowcol_to_a1
    label = PRODUCT_LABELS[product]
    daily_anchor = 0
    for ri, row in enumerate(grid):
        b = (row[1] if len(row) > 1 else "") or ""
        if "일별" in b and "성과" in b and "비교" not in b:
            daily_anchor = ri
            break
    ch_row = ch_col = None
    for ri in range(daily_anchor, len(grid)):
        for ci, c in enumerate(grid[ri]):
            if (c or "").strip() == label:
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
            lab = (met[i] or "").strip()
            for sub, key in _METRIC_SUBS:
                if key not in cols and sub in lab:
                    cols[key] = rowcol_to_a1(1, i + 1).rstrip("1")
        if "cost" in cols:
            return cols
    return None


def write_to_sheet(spreadsheet_id, store_daily):
    """효율탭 카카오 DA/모객/메세지 칸에 일자별 기입. store_daily={date:{product:{metrics}}}.
    반환 (written, errors). 엉뚱한 칸 방지 verify_cells 포함."""
    import sheets
    from collections import defaultdict
    import datetime as _dt
    gc = sheets.get_client()
    sh = gc.open_by_key(sheets.clean_spreadsheet_id(spreadsheet_id))
    by_tab = defaultdict(dict)
    for d, prods in store_daily.items():
        if d.startswith("_"):
            continue
        by_tab[sheets.efficiency_sheet_name(d)][d] = prods
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
        # 상품별 컬럼 1회 탐색
        prod_cols = {}
        for product in PRODUCT_LABELS:
            c = _kakao_cols(grid, product)
            if c:
                prod_cols[product] = c
        if not prod_cols:
            errors.append(f"{eff_name} 카카오 DA/모객/메세지 칸 못 찾음 — 스킵")
            continue
        col_b = ws.col_values(2)
        rowmap = {(v or "").strip(): i for i, v in enumerate(col_b, start=1)}
        data = []
        for d, prods in days.items():
            row = rowmap.get(d.replace("-", "/"))
            if not row:
                errors.append(f"{d} 행없음")
                continue
            wrote_any = False
            for product, metrics in prods.items():
                cols = prod_cols.get(product)
                if not cols:
                    continue
                for key in _PRODUCT_KEYS.get(product, []):
                    if key in cols and metrics.get(key) is not None:
                        data.append({"range": f"{cols[key]}{row}", "values": [[metrics[key]]]})
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


if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    res = fetch_all(days=n)
    for store, daily in res.items():
        if store == "_recon":
            continue
        print(f"\n## {store}")
        print(json.dumps(daily, ensure_ascii=False))
    print("\n## 정합성(_recon) store/date/계정총합/캠페인합/미분류gap:")
    for r in res.get("_recon", []):
        flag = "  ⚠️미분류큼" if r[4] and r[4] > 10000 else ""
        print(f"  {r[0]:<16} {r[1]} 총{r[2]:>8} 캡처{r[3]:>8} gap{r[4]:>8}{flag}")
