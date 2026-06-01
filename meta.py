"""Meta(Facebook/Instagram) 광고 성과 수집 — Marketing API (Ads Insights).
브라우저 없이 HTTP 만 사용. cafe24 스크래퍼와 같은 서버에서 가벼운 일일 잡으로 동작.

수집 지표(시트 메타 칸 매핑):
  impressions → AZ 노출량
  clicks      → BA 클릭수 (전체 클릭)
  spend       → BC 광고비  (단, 시트는 부가세 10% 포함 → spend*1.1)
  purchase    → BE 전환수
  purchase value → BG 매출
  (CTR/CPC/CVR/ROAS/객단가는 시트 수식이 자동 계산)
"""
import json
import os
import urllib.parse
import urllib.request

GRAPH_VER = "v21.0"
VAT_MULTIPLIER = 1.1  # 시트 광고비는 부가세 10% 포함

# 구매 전환 action_type 우선순위 (위에서부터 먼저 잡힘)
_PURCHASE_TYPES = ["purchase", "offsite_conversion.fb_pixel_purchase", "omni_purchase"]


def _token():
    return os.environ.get("META_ACCESS_TOKEN", "")


def _pick_action(actions, types=_PURCHASE_TYPES):
    if not actions:
        return 0.0
    for t in types:
        for a in actions:
            if a.get("action_type") == t:
                try:
                    return float(a.get("value", 0))
                except (TypeError, ValueError):
                    return 0.0
    return 0.0


def _action_val(actions, type_name):
    """단일 action_type 값 (퍼널 단계용)."""
    if not actions:
        return 0
    for a in actions:
        if a.get("action_type") == type_name:
            try:
                return int(float(a.get("value", 0)))
            except (TypeError, ValueError):
                return 0
    return 0


def _row_to_metrics(row):
    """insights 한 행 → 표준 metrics dict (계정/캠페인 공통)."""
    imp = int(float(row.get("impressions", 0) or 0))
    clk = int(float(row.get("clicks", 0) or 0))
    spend = float(row.get("spend", 0) or 0)
    acts = row.get("actions")
    return {
        "impressions": imp,
        "clicks": clk,
        "spend": round(spend),
        "spend_vat": round(spend * VAT_MULTIPLIER),
        "purchases": int(_pick_action(acts)),
        "revenue": round(_pick_action(row.get("action_values"))),
        # 추가 지표
        "reach": int(float(row.get("reach", 0) or 0)),
        "frequency": round(float(row.get("frequency", 0) or 0), 2),
        "link_clicks": int(float(row.get("inline_link_clicks", 0) or 0)),
        "lpv": _action_val(acts, "landing_page_view"),
        "atc": _action_val(acts, "add_to_cart"),
        "ic": _action_val(acts, "initiate_checkout"),
    }


def fetch_insights(ad_account_id, since, until, token=None):
    """ad_account_id(act_ 접두사 없어도 됨)의 since~until 일별 insights.
    반환: {date_str: {impressions, clicks, spend, spend_vat, purchases, revenue}}
    실패 시 RuntimeError."""
    token = token or _token()
    if not token:
        raise RuntimeError("META_ACCESS_TOKEN 미설정")
    acct = ad_account_id if str(ad_account_id).startswith("act_") else f"act_{ad_account_id}"
    params = {
        "access_token": token,
        "fields": "impressions,clicks,spend,reach,frequency,inline_link_clicks,actions,action_values",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": "1",
        "level": "account",
    }
    url = f"https://graph.facebook.com/{GRAPH_VER}/{acct}/insights?" + urllib.parse.urlencode(params)
    out = {}
    while url:
        with urllib.request.urlopen(url, timeout=40) as r:
            payload = json.load(r)
        for row in payload.get("data", []):
            out[row.get("date_start")] = _row_to_metrics(row)
        url = payload.get("paging", {}).get("next")
    return out


def fetch_campaign_insights(ad_account_id, since, until, token=None):
    """캠페인별 일별 insights. 반환: list of {date, campaign_id, campaign_name, ...metrics}."""
    token = token or _token()
    if not token:
        raise RuntimeError("META_ACCESS_TOKEN 미설정")
    acct = ad_account_id if str(ad_account_id).startswith("act_") else f"act_{ad_account_id}"
    params = {
        "access_token": token,
        "fields": "campaign_id,campaign_name,impressions,clicks,spend,reach,frequency,inline_link_clicks,actions,action_values",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": "1",
        "level": "campaign",
    }
    url = f"https://graph.facebook.com/{GRAPH_VER}/{acct}/insights?" + urllib.parse.urlencode(params)
    out = []
    while url:
        with urllib.request.urlopen(url, timeout=40) as r:
            payload = json.load(r)
        for row in payload.get("data", []):
            m = _row_to_metrics(row)
            m["date"] = row.get("date_start")
            m["campaign_id"] = row.get("campaign_id")
            m["campaign_name"] = row.get("campaign_name", "")
            out.append(m)
        url = payload.get("paging", {}).get("next")
    return out


def fetch_ad_insights(ad_account_id, since, until, token=None):
    """광고(소재)별 일별 insights. 반환: list of {date, ad_id, ad_name, campaign_name, ...metrics}."""
    token = token or _token()
    if not token:
        raise RuntimeError("META_ACCESS_TOKEN 미설정")
    acct = ad_account_id if str(ad_account_id).startswith("act_") else f"act_{ad_account_id}"
    params = {
        "access_token": token,
        "fields": "ad_id,ad_name,campaign_name,impressions,clicks,spend,actions,action_values",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": "1",
        "level": "ad",
        "limit": "200",
    }
    url = f"https://graph.facebook.com/{GRAPH_VER}/{acct}/insights?" + urllib.parse.urlencode(params)
    out = []
    while url:
        with urllib.request.urlopen(url, timeout=40) as r:
            payload = json.load(r)
        for row in payload.get("data", []):
            m = _row_to_metrics(row)
            m["date"] = row.get("date_start")
            m["ad_id"] = row.get("ad_id")
            m["ad_name"] = row.get("ad_name", "")
            m["campaign_name"] = row.get("campaign_name", "")
            out.append(m)
        url = payload.get("paging", {}).get("next")
    return out


def verify_token(token=None):
    """토큰 유효성 + 이름 반환. 실패 시 예외."""
    token = token or _token()
    url = f"https://graph.facebook.com/{GRAPH_VER}/me?" + urllib.parse.urlencode({"access_token": token})
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.load(r)


# ===== 시트 쓰기 (효율 탭 메타 칸) =====
# 메타 블록 열 (1-indexed): AZ=노출 BA=클릭 BC=광고비 BE=전환 BG=매출
#   BB/BD/BF/BH/BI 는 시트 수식(CTR/CPC/CVR/ROAS/객단가) → 건드리지 않음
META_COLS = {"impressions": "AZ", "clicks": "BA", "spend_vat": "BC", "purchases": "BE", "revenue": "BG"}


def _find_daily_row(ws, date_str):
    """효율 탭 '일별 성과' 섹션에서 B열 == 'YYYY/MM/DD' 인 행번호(1-indexed). 없으면 None."""
    target = date_str.replace("-", "/")  # 2026-05-22 → 2026/05/22
    col_b = ws.col_values(2)  # B열 전체
    for i, v in enumerate(col_b, start=1):
        if (v or "").strip() == target:
            return i
    return None


def write_meta_days(spreadsheet_id, insights):
    """여러 날짜를 한 시트에 한 번에 기입 (구글 API 호출 최소화 — 쿼터 보호).
    insights: {date_str: metrics}. 같은 달끼리 묶어 탭별로 1회 읽기 + 1회 batch write.
    반환: (written_days, [errors])."""
    import sheets
    from collections import defaultdict
    gc = sheets.get_client()
    sh = gc.open_by_key(sheets.clean_spreadsheet_id(spreadsheet_id))
    # 날짜를 효율탭(월)별로 그룹
    by_tab = defaultdict(dict)
    for d, m in insights.items():
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
        col_b = ws.col_values(2)  # 탭당 1회만 읽기
        rowmap = {}
        for i, v in enumerate(col_b, start=1):
            rowmap[(v or "").strip()] = i
        data = []
        for d, m in days.items():
            row = rowmap.get(d.replace("-", "/"))
            if not row:
                errors.append(f"{d} 행없음")
                continue
            data += [
                {"range": f"{META_COLS['impressions']}{row}", "values": [[m["impressions"]]]},
                {"range": f"{META_COLS['clicks']}{row}", "values": [[m["clicks"]]]},
                {"range": f"{META_COLS['spend_vat']}{row}", "values": [[m["spend_vat"]]]},
                {"range": f"{META_COLS['purchases']}{row}", "values": [[m["purchases"]]]},
                {"range": f"{META_COLS['revenue']}{row}", "values": [[m["revenue"]]]},
            ]
            written += 1
        if data:
            ws.batch_update(data, value_input_option="USER_ENTERED")  # 탭당 1회만 쓰기
            # read-back 검증: 가장 최근 날짜 1개만 다시 읽어 내가 쓴 값과 일치하는지 확인.
            try:
                last_d = max(days.keys())
                row = rowmap.get(last_d.replace("-", "/"))
                if row:
                    rng = f"{META_COLS['impressions']}{row}:{META_COLS['revenue']}{row}"  # AZ~BG
                    got = ws.get(rng, value_render_option="UNFORMATTED_VALUE")
                    flat = got[0] if got else []
                    # AZ,BA,BB,BC,BD,BE,BF,BG 순서 — 우리가 쓴 건 AZ(0)/BA(1)/BC(3)/BE(5)/BG(7)
                    def _gi(i):
                        try:
                            return int(float(flat[i])) if i < len(flat) and flat[i] != "" else 0
                        except (ValueError, TypeError):
                            return 0
                    exp = days[last_d]
                    pairs = [("노출", _gi(0), exp["impressions"]), ("클릭", _gi(1), exp["clicks"]),
                             ("광고비", _gi(3), exp["spend_vat"]), ("전환", _gi(5), exp["purchases"]),
                             ("매출", _gi(7), exp["revenue"])]
                    bad = [f"{n}(시트{g}≠API{e})" for n, g, e in pairs if g != e]
                    if bad:
                        errors.append(f"{last_d} 검증불일치: {', '.join(bad)}")
            except Exception as e:
                errors.append(f"readback err: {repr(e)[:60]}")
    return written, errors
