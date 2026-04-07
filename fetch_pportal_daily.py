"""Daily P-Portal differential data fetcher.
Downloads the latest diff CSV and ingests into dashboard DB.
Designed to run via cron or APScheduler.
"""
import csv
import io
import os
import sqlite3
import urllib.request
import zipfile
from datetime import datetime, timedelta

DB_PATH = os.environ.get("DATA_DIR", "/app/data") + "/monitor.db"
BASE_URL = "https://api.p-portal.go.jp/pps-web-biz/UAB03/OAB0301"

CATEGORY_KEYWORDS = {
    "construction": ["工事", "修繕", "改修", "建設", "舗装", "設置工", "撤去工", "解体", "補修"],
    "service": ["業務委託", "委託", "役務", "保守", "点検", "清掃", "管理業務", "運営", "警備"],
    "goods": ["購入", "調達", "納入", "物品", "備品", "機器", "車両", "リース", "賃貸借", "燃料"],
    "consulting": ["設計", "測量", "調査", "コンサル", "計画策定"],
    "it": ["システム", "ソフトウェア", "ネットワーク", "サーバ", "データ", "ICT", "DX", "AI"],
}

def classify(title):
    for cat, kws in CATEGORY_KEYWORDS.items():
        for kw in kws:
            if kw in title:
                return cat
    return "other"

def fetch_and_ingest(date_str=None):
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    filename = f"successful_bid_record_info_diff_{date_str}.zip"
    url = f"{BASE_URL}?fileversion=v001&filename={filename}"

    print(f"[P-Portal] Fetching {filename}...")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=60)
        data = resp.read()
    except Exception as e:
        print(f"  Download failed: {e}")
        return 0

    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
        csv_name = zf.namelist()[0]
        csv_data = zf.read(csv_name).decode("utf-8-sig")
    except Exception as e:
        print(f"  Unzip failed: {e}")
        return 0

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    inserted = 0

    for row in csv.reader(io.StringIO(csv_data)):
        if len(row) < 7:
            continue
        case_num, title, award_date, amount_str, proc_type, org_code, winner = row[:7]
        title = title.strip().strip('"')
        if not title:
            continue

        try:
            amount = float(amount_str.strip().strip('"'))
            amount_disp = f"{int(amount):,}円"
        except (ValueError, TypeError):
            amount_disp = None

        category = classify(title)

        try:
            cur.execute("""INSERT OR IGNORE INTO procurement_items
                (muni_code, detected_at, title, item_type, deadline, amount, method, url, raw_json, department, category)
                VALUES (?, ?, ?, 'award', NULL, ?, ?, ?, ?, NULL, ?)""",
                ("NATIONAL", award_date.strip().strip('"'), title, amount_disp,
                 winner.strip().strip('"'),
                 f"https://www.p-portal.go.jp/pps-web-biz/UAA01/OAA0121?SEARCH_WORD={case_num.strip().strip(chr(34))}",
                 f'{{"source":"p-portal-diff","date":"{date_str}"}}',
                 category))
            inserted += cur.rowcount
        except Exception:
            pass

    conn.commit()
    conn.close()
    print(f"  Inserted: {inserted} new award records")
    return inserted

if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else None
    fetch_and_ingest(date)
