import sqlite3, json
conn = sqlite3.connect("/app/data/monitor.db")
conn.row_factory = sqlite3.Row

def show(label, sql):
    print(f"\n=== {label} ===")
    for r in conn.execute(sql).fetchall():
        iid = r["item_id"]
        title = (r["title"] or "?")[:60]
        method = r["method"] or "?"
        rj = r["rj_len"] or 0
        muni = r["muni_code"] or "?"
        print(f"  {iid} [{method[:10]}] muni={muni} rj={rj} {title}")

show("A. plan", """
    SELECT item_id, title, method, muni_code, deadline, amount, LENGTH(raw_json) as rj_len
    FROM procurement_items
    WHERE (title LIKE '%計画策定%' OR title LIKE '%企画%' OR title LIKE '%プロポーザル%')
    AND title NOT LIKE '%工事%'
    ORDER BY rj_len DESC LIMIT 10
""")

show("B. research", """
    SELECT item_id, title, method, muni_code, deadline, amount, LENGTH(raw_json) as rj_len
    FROM procurement_items
    WHERE (title LIKE '%調査%' OR title LIKE '%研究%' OR title LIKE '%分析%')
    AND title NOT LIKE '%工事%' AND title NOT LIKE '%設計%'
    ORDER BY rj_len DESC LIMIT 10
""")

show("C. general_service", """
    SELECT item_id, title, method, muni_code, deadline, amount, LENGTH(raw_json) as rj_len
    FROM procurement_items
    WHERE method LIKE '%(役務)%'
    AND title NOT LIKE '%調査%' AND title NOT LIKE '%企画%'
    AND title NOT LIKE '%工事%' AND title NOT LIKE '%設計%'
    AND title NOT LIKE '%研究%' AND title NOT LIKE '%分析%'
    AND title NOT LIKE '%計画策定%'
    ORDER BY rj_len DESC LIMIT 10
""")

show("D. goods", """
    SELECT item_id, title, method, muni_code, deadline, amount, LENGTH(raw_json) as rj_len
    FROM procurement_items
    WHERE (method LIKE '%(物品)%' OR title LIKE '%賃貸借%' OR title LIKE '%購入%')
    AND title NOT LIKE '%工事%'
    ORDER BY rj_len DESC LIMIT 10
""")
