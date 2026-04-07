"""Ingest P-Portal 落札実績オープンデータ into dashboard SQLite DB.
Columns: case_number, title, award_date, amount, procedure_type, org_code, winner_name, corporate_number
"""
import csv
import sqlite3
import sys
import os
import glob
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "/app/data/monitor.db")

PROCEDURE_MAP = {
    "S1": "general_competitive",
    "S2": "general_competitive",
    "S3": "designated_competitive",
    "S4": "negotiated",
    "S5": "other",
    "W1": "general_competitive",
    "W2": "designated_competitive",
    "W3": "negotiated",
    "W4": "other",
}

CATEGORY_KEYWORDS = {
    "construction": ["工事", "修繕", "改修", "建設", "舗装", "設置工", "撤去工", "解体", "補修", "塗装", "防水"],
    "service": ["業務委託", "委託", "役務", "保守", "点検", "清掃", "管理業務", "運営", "警備", "派遣", "運転"],
    "goods": ["購入", "調達", "納入", "物品", "備品", "機器", "車両", "薬品", "リース", "賃貸借", "燃料"],
    "consulting": ["設計", "測量", "調査", "コンサル", "計画策定", "検討"],
    "it": ["システム", "ソフトウェア", "ネットワーク", "サーバ", "データ", "ICT", "DX", "AI", "デジタル"],
}

def classify_category(title):
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in title:
                return cat
    return "other"

def classify_department(org_code):
    prefix = org_code[:4] if org_code else ""
    dept_map = {
        "8001": "内閣府", "8002": "国土交通省", "8003": "法務省",
        "8004": "経済産業省", "8005": "厚生労働省", "8006": "文部科学省",
        "8007": "農林水産省", "8008": "環境省", "8009": "防衛省",
        "8010": "総務省", "8011": "財務省", "8012": "外務省",
    }
    return dept_map.get(prefix, f"機関{prefix}")

def ingest_csv(csv_path, conn, year_label):
    cursor = conn.cursor()
    inserted = 0
    skipped = 0

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 7:
                continue
            case_number, title, award_date, amount_str, proc_type, org_code, winner_name = row[:7]
            corp_number = row[7] if len(row) > 7 else ""

            title = title.strip().strip('"')
            if not title:
                continue

            try:
                amount_val = float(amount_str.strip().strip('"'))
                amount_display = f"{int(amount_val):,}円"
            except (ValueError, TypeError):
                amount_val = None
                amount_display = None

            item_type = PROCEDURE_MAP.get(proc_type.strip().strip('"'), "other")
            category = classify_category(title)
            department = classify_department(org_code.strip().strip('"'))

            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO procurement_items
                    (muni_code, detected_at, title, item_type, deadline, amount, method, url,
                     raw_json, department, division, budget_range, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    "NATIONAL",
                    award_date.strip().strip('"'),
                    title,
                    "award",
                    None,
                    amount_display,
                    winner_name.strip().strip('"'),
                    f"https://www.p-portal.go.jp/pps-web-biz/UAA01/OAA0121?SEARCH_WORD={case_number.strip().strip(chr(34))}",
                    f'{{"source":"p-portal","year":"{year_label}","case":"{case_number.strip().strip(chr(34))}","corp":"{corp_number.strip().strip(chr(34))}"}}',
                    department,
                    None,
                    f"{int(amount_val):,}" if amount_val else None,
                    category,
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                skipped += 1

    conn.commit()
    return inserted, skipped

def main():
    csv_dir = sys.argv[1] if len(sys.argv) > 1 else "/tmp"

    conn = sqlite3.connect(DB_PATH)

    total_inserted = 0
    total_skipped = 0

    for year in [2019, 2020, 2021, 2022, 2023, 2026]:
        pattern = os.path.join(csv_dir, f"pportal_{year}", "*.csv")
        files = glob.glob(pattern)
        for csv_path in files:
            print(f"Processing {csv_path}...")
            inserted, skipped = ingest_csv(csv_path, conn, str(year))
            print(f"  Inserted: {inserted}, Skipped: {skipped}")
            total_inserted += inserted
            total_skipped += skipped

    # Also try diff files
    diff_pattern = os.path.join(csv_dir, "pportal_diff", "*.csv")
    for csv_path in glob.glob(diff_pattern):
        print(f"Processing diff {csv_path}...")
        inserted, skipped = ingest_csv(csv_path, conn, "2026_diff")
        print(f"  Inserted: {inserted}, Skipped: {skipped}")
        total_inserted += inserted
        total_skipped += skipped

    conn.close()
    print(f"\nTotal: Inserted {total_inserted}, Skipped {total_skipped}")

if __name__ == "__main__":
    main()
