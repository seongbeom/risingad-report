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
        "fields": "impressions,clicks,spend,actions,action_values",
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
            d = row.get("date_start")
            imp = int(float(row.get("impressions", 0) or 0))
            clk = int(float(row.get("clicks", 0) or 0))
            spend = float(row.get("spend", 0) or 0)
            purch = int(_pick_action(row.get("actions")))
            rev = round(_pick_action(row.get("action_values")))
            out[d] = {
                "impressions": imp,
                "clicks": clk,
                "spend": round(spend),
                "spend_vat": round(spend * VAT_MULTIPLIER),  # 시트 입력용(부가세 포함)
                "purchases": purch,
                "revenue": rev,
            }
        # 페이지네이션
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


def write_meta_to_sheet(spreadsheet_id, date_str, metrics):
    """효율_26년N월 탭의 date_str 행 메타 칸(AZ/BA/BC/BE/BG)에 값 기입.
    metrics: fetch_insights 의 한 날짜 dict. 파생 칸(수식)은 안 건드림.
    반환: (ok, msg)."""
    import sheets  # 지연 import (순환 방지)
    gc = sheets.get_client()
    sh = gc.open_by_key(spreadsheet_id)
    eff_name = sheets.efficiency_sheet_name(date_str)
    try:
        ws = sh.worksheet(eff_name)
    except Exception:
        return False, f"효율 탭 '{eff_name}' 없음"
    row = _find_daily_row(ws, date_str)
    if not row:
        return False, f"{date_str} 행 못 찾음 ({eff_name})"
    # 5개 셀 batch 업데이트 (수식 칸은 제외)
    data = [
        {"range": f"{META_COLS['impressions']}{row}", "values": [[metrics["impressions"]]]},
        {"range": f"{META_COLS['clicks']}{row}", "values": [[metrics["clicks"]]]},
        {"range": f"{META_COLS['spend_vat']}{row}", "values": [[metrics["spend_vat"]]]},
        {"range": f"{META_COLS['purchases']}{row}", "values": [[metrics["purchases"]]]},
        {"range": f"{META_COLS['revenue']}{row}", "values": [[metrics["revenue"]]]},
    ]
    ws.batch_update(data, value_input_option="USER_ENTERED")
    return True, f"{eff_name} R{row} 기입"
