"""네이버 쇼핑박스(PC 주간 / MO 월간 정액입찰) 시트 기입 + 일별분할.

광고비: 입찰 낙찰가(정액)를 집행기간 일수로 분할 (db.shopbox_daily_cost).
노출·클릭: 쇼핑파트너센터 광고리포트 (v2, 추후).
매출: cafe24 유입분석 UTM (v2, 추후).

효율시트의 '쇼핑박스 PC' / '쇼핑박스 MO' 채널 블록을 동적 탐색해 일별 기입.
(criteo/gfa write_to_sheet 패턴 동일)
"""
DEVICE_LABELS = {
    "pc": ["쇼핑박스 PC", "쇼핑박스PC", "네이버 쇼핑박스 PC"],
    "mo": ["쇼핑박스 MO", "쇼핑박스MO", "네이버 쇼핑박스 MO (트렌드픽)", "네이버 쇼핑박스 MO"],
}
# 채널 블록 안에서 찾을 지표 (있는 것만 기입 — 쇼핑박스는 보통 노출/클릭/광고비/매출)
_METRIC_SUBS = [("노출", "impressions"), ("클릭", "clicks"), ("광고비", "cost"), ("매출", "revenue")]


def _shopbox_cols(ws, device):
    """효율시트 일별 채널행(28~34)에서 device('pc'|'mo') 쇼핑박스 블록의 지표 컬럼 letter.
    반환 {impressions?,clicks?,cost,revenue?} (최소 cost 있어야 유효). 없으면 None."""
    from gspread.utils import rowcol_to_a1
    labels = DEVICE_LABELS[device]
    grid = ws.get("A28:OZ34")
    ch_row = None
    ch_col = None
    for ri, row in enumerate(grid):
        for ci, c in enumerate(row):
            if (c or "").strip() in labels:
                ch_row, ch_col = ri, ci
                break
        if ch_row is not None:
            break
    if ch_row is None:
        return None
    ch = grid[ch_row]
    # 다음 채널 라벨 전까지가 이 블록
    nxt = len(ch)
    for i in range(ch_col + 1, len(ch)):
        if (ch[i] or "").strip():
            nxt = i
            break
    met = grid[ch_row + 1] if ch_row + 1 < len(grid) else []
    cols = {}
    for i in range(ch_col, min(nxt, len(met))):
        label = (met[i] or "").strip()
        for sub, key in _METRIC_SUBS:
            if key not in cols and sub in label:
                cols[key] = rowcol_to_a1(1, i + 1).rstrip("1")
    return cols if "cost" in cols else None


def write_to_sheet(spreadsheet_id, daily):
    """daily = {date: {'pc': {cost,impressions,clicks,revenue}, 'mo': {...}}} 효율탭 기입.
    각 device 블록을 동적탐색해, 그 블록에 존재하는 지표만 기입. 반환 (written, errors)."""
    import sheets
    from collections import defaultdict
    import datetime as _dt
    gc = sheets.get_client()
    sh = gc.open_by_key(sheets.clean_spreadsheet_id(spreadsheet_id))
    by_tab = defaultdict(dict)
    for d, devmap in daily.items():
        by_tab[sheets.efficiency_sheet_name(d)][d] = devmap
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
        cols = {dev: _shopbox_cols(ws, dev) for dev in ("pc", "mo")}
        if not any(cols.values()):
            errors.append(f"{eff_name} 쇼핑박스 PC/MO 칸 못 찾음 — 스킵")
            continue
        col_b = ws.col_values(2)
        rowmap = {(v or "").strip(): i for i, v in enumerate(col_b, start=1)}
        data = []
        for d, devmap in days.items():
            row = rowmap.get(d.replace("-", "/"))
            if not row:
                errors.append(f"{d} 행없음")
                continue
            wrote_any = False
            for dev in ("pc", "mo"):
                c = cols.get(dev)
                m = devmap.get(dev)
                if not c or not m:
                    continue
                for key, colletter in c.items():
                    if m.get(key) is not None:
                        data.append({"range": f"{colletter}{row}", "values": [[m[key]]]})
                        wrote_any = True
            if wrote_any:
                written += 1
        if data:
            ws.batch_update(data, value_input_option="USER_ENTERED")
    return written, errors
