"""4/17 ~ 어제까지 양 계정 시간 단위 매출 popup 1일씩 백필.
각 일자에 대해 popup 페이지 진입 + '시간 단위' 토글 + 24시간 테이블 추출 + db upsert.
세션 재활용을 위해 한 계정당 1개 browser context로 일자 루프."""
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright

import db
import scraper
import sheets


def daterange(start_str, end_str):
    s = datetime.strptime(start_str, "%Y-%m-%d")
    e = datetime.strptime(end_str, "%Y-%m-%d")
    cur = s
    while cur <= e:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)


def backfill_one(account_id, start_date, end_date):
    account = db.get_account(account_id)
    if not account:
        print(f"[{account_id}] 계정 없음")
        return

    sess_path = scraper._session_path(account_id)
    print(f"\n===== {account_id} {start_date} ~ {end_date} =====")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)
        context = browser.new_context(storage_state=str(sess_path)) if sess_path.exists() else browser.new_context()
        page = context.new_page()
        scraper._attach_sample_detector(page)
        scraper.ensure_login(page, context, account)

        for d in daterange(start_date, end_date):
            try:
                section = scraper.scrape_popup_hourly(context, scraper.SALES_POPUP_URL, d)
            except Exception as e:
                print(f"  [{d}] 스크래핑 실패: {e}")
                continue
            fake = {"매출종합_시간별": section}
            rows = sheets.extract_hourly_rows(fake)
            if not rows:
                print(f"  [{d}] 시간별 row 0건 (스킵)")
                continue
            n = db.upsert_metrics_hourly(account_id, d, rows)
            total_sales = sum(r["매출"] for r in rows)
            total_orders = sum(r["구매건수"] for r in rows)
            print(f"  [{d}] {n}시간 매출합 {total_sales:>10,} 구매합 {total_orders}")

        context.storage_state(path=str(sess_path))
        browser.close()


if __name__ == "__main__":
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start = sys.argv[1] if len(sys.argv) > 1 else "2026-04-17"
    end = sys.argv[2] if len(sys.argv) > 2 else yesterday
    accounts = sys.argv[3].split(",") if len(sys.argv) > 3 else ["humandaily", "cinderella1009"]
    for aid in accounts:
        backfill_one(aid, start, end)
