#!/usr/bin/env python3
"""Deep cleaning: Remove non-procurement items from database.
Removes navigation text, news articles, event announcements, and other noise.
"""
import sqlite3
import re

DB = "/app/data/monitor.db"

# Patterns that indicate NON-procurement content
NOISE_PATTERNS = [
    # Navigation/menu text
    "%の一覧%", "%ページ%先頭%", "%カテゴリ%", "%サイト内検索%",
    "%トップページ%", "%ホームページ%リニューアル%",
    "%文字サイズ%", "%背景色%", "%読み上げ%",
    # News/events (not procurement)
    "%感染%発生%", "%コロナ%", "%ワクチン%接種%",
    "%体験会%開催%", "%イベント%開催%", "%お祭り%",
    "%マラソン%大会%参加%", "%写真コンテスト%",
    # Health/welfare notices
    "%熱中症%", "%インフルエンザ%", "%食中毒%",
    "%予防接種%お知らせ%",
    # General announcements (not procurement)
    "%ふるさと納税%お礼%", "%ゆるキャラ%",
    "%広報%発行%", "%議会だより%発行%",
    # Very short generic titles
]

# Patterns that DEFINITELY ARE procurement
KEEP_PATTERNS = [
    "%入札%", "%公告%", "%プロポーザル%", "%落札%",
    "%工事%", "%業務委託%", "%委託%業務%",
    "%購入%", "%調達%", "%契約%",
    "%発注%", "%見積%", "%仕様書%",
    "%指名%競争%", "%一般競争%", "%随意契約%",
    "%審査結果%", "%事業者%選定%", "%事業者%募集%",
    "%予定価格%", "%最低制限%",
]

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    before = cur.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    print(f"Items before cleaning: {before}", flush=True)

    # Step 1: Remove items that match noise patterns AND don't match keep patterns
    removed = 0

    for noise in NOISE_PATTERNS:
        # Only remove if NOT matching any keep pattern
        keep_conditions = " AND ".join([f"title NOT LIKE '{k}'" for k in KEEP_PATTERNS[:10]])
        cur.execute(f"""DELETE FROM procurement_items
            WHERE muni_code != 'NATIONAL'
            AND item_type != 'award'
            AND title LIKE '{noise}'
            AND {keep_conditions}""")
        removed += cur.rowcount

    # Step 2: Remove items with very short titles that are clearly not procurement
    cur.execute("""DELETE FROM procurement_items
        WHERE muni_code != 'NATIONAL'
        AND item_type != 'award'
        AND length(title) < 8""")
    removed += cur.rowcount
    print(f"Removed short titles (<8 chars): {cur.rowcount}", flush=True)

    # Step 3: Remove duplicate titles within same municipality (keep one)
    cur.execute("""DELETE FROM procurement_items WHERE rowid NOT IN (
        SELECT MIN(rowid) FROM procurement_items GROUP BY muni_code, title
    )""")
    deduped = cur.rowcount
    print(f"Deduplicated: {deduped}", flush=True)

    conn.commit()

    after = cur.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    print(f"\nItems after cleaning: {after}", flush=True)
    print(f"Total removed: {before - after}", flush=True)
    print(f"Noise removed: {removed}", flush=True)

    # Recount coverage
    cur.execute("SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code != 'NATIONAL'")
    print(f"Municipality coverage: {cur.fetchone()[0]}", flush=True)

    cur.execute("SELECT COUNT(*) FROM (SELECT muni_code FROM procurement_items WHERE muni_code != 'NATIONAL' GROUP BY muni_code HAVING COUNT(*) >= 10)")
    print(f"Munis with 10+ items: {cur.fetchone()[0]}", flush=True)

    cur.execute("SELECT COUNT(*) FROM (SELECT muni_code FROM procurement_items WHERE muni_code != 'NATIONAL' GROUP BY muni_code HAVING COUNT(*) >= 50)")
    print(f"Munis with 50+ items: {cur.fetchone()[0]}", flush=True)

    # Sample remaining items
    cur.execute("SELECT title FROM procurement_items WHERE muni_code != 'NATIONAL' AND item_type != 'award' ORDER BY RANDOM() LIMIT 15")
    print("\nSample remaining items:", flush=True)
    for (t,) in cur.fetchall():
        print(f"  {t[:80]}", flush=True)

    conn.close()

if __name__ == "__main__":
    main()
