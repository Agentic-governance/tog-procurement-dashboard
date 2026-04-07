"""Extract procurement items from existing snapshots using regex.
No GPT needed - pure text pattern matching on already-fetched pages.
"""
import sqlite3
import re
from datetime import datetime

DB_PATH = "/app/data/monitor.db"

CATEGORY_KW = {
    "construction": ["工事", "修繕", "改修", "建設", "舗装", "設置工", "撤去工", "解体", "補修", "塗装", "防水", "配管"],
    "service": ["業務委託", "委託", "役務", "保守", "点検", "清掃", "管理業務", "運営", "警備", "派遣", "収集"],
    "goods": ["購入", "調達", "納入", "物品", "備品", "機器", "車両", "リース", "賃貸借", "燃料"],
    "consulting": ["設計", "測量", "調査", "コンサル", "計画策定", "策定支援"],
    "it": ["システム", "ソフトウェア", "ネットワーク", "サーバ", "データ", "ICT", "DX", "AI", "デジタル", "GIS", "LINE"],
}

def classify(title):
    for cat, kws in CATEGORY_KW.items():
        for kw in kws:
            if kw in title:
                return cat
    return "other"

def detect_type(title):
    if "プロポーザル" in title or "企画提案" in title or "公募型" in title:
        return "proposal"
    if "落札" in title or "入札結果" in title:
        return "award"
    if "指名" in title:
        return "designated_competitive"
    if "一般競争" in title or "制限付" in title or "条件付" in title:
        return "general_competitive"
    return "other"

def extract_items_from_text(text, muni_code, url):
    items = []
    seen = set()

    # Pattern 1: Lines that look like bid items
    for line in text.split('\n'):
        line = line.strip()
        if len(line) < 10 or len(line) > 200:
            continue

        # Must contain a procurement keyword
        has_kw = any(kw in line for kw in [
            '入札', '公告', 'プロポーザル', '公募', '落札', '見積',
            '工事', '委託業務', '業務委託', '購入', '賃貸借'
        ])
        # But not navigation/menu text
        is_nav = any(kw in line for kw in [
            'トップページ', 'ホーム', 'サイトマップ', 'アクセス', 'お問い合わせ',
            '検索', 'メニュー', '文字サイズ', 'ログイン', 'パスワード',
            '個人情報', 'プライバシー', '著作権', '免責事項'
        ])

        if has_kw and not is_nav and line not in seen:
            seen.add(line)
            # Extract date if present
            date_match = re.search(r'(20\d{2})[年/\-](\d{1,2})[月/\-](\d{1,2})', line)
            reiwa_match = re.search(r'令和(\d+)年(\d{1,2})月(\d{1,2})日', line)

            date_str = None
            if date_match:
                y, m, d = date_match.groups()
                date_str = f"{y}-{int(m):02d}-{int(d):02d}"
            elif reiwa_match:
                ry, m, d = reiwa_match.groups()
                y = int(ry) + 2018
                date_str = f"{y}-{int(m):02d}-{int(d):02d}"

            if not date_str:
                date_str = datetime.now().strftime("%Y-%m-%d")

            # Clean title
            title = re.sub(r'^[\s\-・•►▶▷→]+', '', line)
            title = re.sub(r'\s*\(PDF[^)]*\)\s*$', '', title)
            title = re.sub(r'\s*（PDF[^）]*）\s*$', '', title)
            title = title.strip()

            if len(title) > 8:
                items.append({
                    'muni_code': muni_code,
                    'detected_at': date_str,
                    'title': title[:200],
                    'item_type': detect_type(title),
                    'category': classify(title),
                    'url': url,
                })

    return items

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Get all uncovered municipalities with snapshots
    cur.execute('''
    SELECT s.muni_code, s.url, s.text_content
    FROM snapshots s
    LEFT JOIN (
        SELECT DISTINCT muni_code FROM procurement_items WHERE muni_code != 'NATIONAL'
    ) p ON s.muni_code = p.muni_code
    WHERE p.muni_code IS NULL
    AND s.text_content IS NOT NULL
    AND length(s.text_content) > 100
    ''')

    all_rows = cur.fetchall()
    print(f"Processing {len(all_rows)} uncovered municipality snapshots...")

    total_inserted = 0
    munis_with_items = 0

    for muni_code, url, text in all_rows:
        items = extract_items_from_text(text, muni_code, url)
        if items:
            munis_with_items += 1
            for item in items[:20]:  # max 20 per muni
                try:
                    cur.execute("""INSERT OR IGNORE INTO procurement_items
                        (muni_code, detected_at, title, item_type, deadline, amount, method, url, department, category)
                        VALUES (?, ?, ?, ?, NULL, NULL, '', ?, NULL, ?)""",
                        (item['muni_code'], item['detected_at'], item['title'],
                         item['item_type'], item['url'], item['category']))
                    total_inserted += cur.rowcount
                except Exception:
                    pass

    conn.commit()

    cur.execute("SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code != 'NATIONAL'")
    total_munis = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM procurement_items")
    total_items = cur.fetchone()[0]

    print(f"\nResults:")
    print(f"  Municipalities with extracted items: {munis_with_items}")
    print(f"  Items inserted: {total_inserted}")
    print(f"  Total muni codes now: {total_munis}")
    print(f"  Total items now: {total_items}")

    conn.close()

if __name__ == "__main__":
    main()
