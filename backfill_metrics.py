"""기존 JSON 결과 파일들을 metrics 테이블로 백필."""
import json
from pathlib import Path

import db
import sheets

DATA_DIR = Path(__file__).parent / "data"


def _filter_to_single(result, target_date):
    """다중일 결과를 target_date 단일일자 형태로 필터링."""

    def _filter(section):
        if not isinstance(section, dict):
            return section
        out = {}
        for sub, t in section.items():
            if isinstance(t, dict) and "rows" in t:
                rows = [r for r in t.get("rows", []) if r and r[0] == target_date]
                out[sub] = {"headers": t.get("headers", []), "rows": rows}
            else:
                out[sub] = t
        return out

    return {
        "account": result.get("account", ""),
        "date": target_date,
        "매출종합분석": _filter(result.get("매출종합분석", {})),
        "방문자분석": _filter(result.get("방문자분석", {})),
        "처음방문vs재방문": _filter(result.get("처음방문vs재방문", {})),
        "신규회원": _filter(result.get("신규회원", {})),
        # 팝업은 row[0]이 일자라 extract_metrics가 result["date"]로 직접 매칭
        "매출종합_상세": result.get("매출종합_상세", {}),
        "구매패턴_상세": result.get("구매패턴_상세", {}),
    }


def backfill():
    total = 0
    for account_dir in DATA_DIR.iterdir():
        if not account_dir.is_dir():
            continue
        account_id = account_dir.name
        for f in sorted(account_dir.glob("*.json")):
            with open(f, encoding="utf-8") as fh:
                result = json.load(fh)

            # 단일일자 vs 다중일자 구분
            if "date" in result:
                # 단일일자
                metrics = sheets.extract_metrics(result)
                db.upsert_metrics(account_id, result["date"], metrics)
                print(f"  [{account_id}] {result['date']} (single) 매출={metrics.get('매출',0):>10,}")
                total += 1
            elif "start_date" in result and "end_date" in result:
                # 다중일자: 매출종합 rows에서 모든 날짜 추출
                rows = result.get("매출종합분석", {}).get("매출종합", {}).get("rows", [])
                for row in rows:
                    if not row or len(row) < 2:
                        continue
                    date = row[0]
                    single = _filter_to_single(result, date)
                    metrics = sheets.extract_metrics(single)
                    db.upsert_metrics(account_id, date, metrics)
                    print(f"  [{account_id}] {date} (range) 매출={metrics.get('매출',0):>10,}")
                    total += 1
    print(f"\n총 {total}건 upsert 완료")


if __name__ == "__main__":
    db.init_db()
    backfill()
