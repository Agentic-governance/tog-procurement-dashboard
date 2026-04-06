import sqlite3, json
conn = sqlite3.connect("/app/data/monitor.db")
conn.row_factory = sqlite3.Row

print("=== service_plan candidates ===")
rows = conn.execute("""
    SELECT item_id, title, method, muni_code, deadline, LENGTH(raw_json) as rj_len,
           CASE WHEN raw_json LIKE '%ProjectDescription%' THEN 1 ELSE 0 END as has_desc
    FROM procurement_items
    WHERE (title LIKE '%計画策定%' OR title LIKE '%プロポーザル%')
    AND title NOT LIKE '%工事%' AND title NOT LIKE '%設計%'
    AND item_id NOT IN (SELECT item_id FROM bid_projects)
    ORDER BY rj_len DESC LIMIT 15
""").fetchall()
for r in rows:
    print(f"  {r['item_id']:>7} rj={r['rj_len']:>5} desc={r['has_desc']} muni={r['muni_code'] or '?':>8} {(r['title'] or '?')[:60]}")

print()
print("=== service_research candidates ===")
rows = conn.execute("""
    SELECT item_id, title, method, muni_code, deadline, LENGTH(raw_json) as rj_len,
           CASE WHEN raw_json LIKE '%ProjectDescription%' THEN 1 ELSE 0 END as has_desc
    FROM procurement_items
    WHERE (title LIKE '%調査%業務%' OR title LIKE '%研究%業務%' OR title LIKE '%分析%業務%')
    AND title NOT LIKE '%工事%' AND title NOT LIKE '%設計%'
    AND item_id NOT IN (SELECT item_id FROM bid_projects)
    ORDER BY rj_len DESC LIMIT 15
""").fetchall()
for r in rows:
    print(f"  {r['item_id']:>7} rj={r['rj_len']:>5} desc={r['has_desc']} muni={r['muni_code'] or '?':>8} {(r['title'] or '?')[:60]}")

print()
print("=== goods candidates ===")
rows = conn.execute("""
    SELECT item_id, title, method, muni_code, deadline, LENGTH(raw_json) as rj_len,
           CASE WHEN raw_json LIKE '%ProjectDescription%' THEN 1 ELSE 0 END as has_desc
    FROM procurement_items
    WHERE (method LIKE '%(物品)%' OR title LIKE '%賃貸借%' OR title LIKE '%購入%')
    AND title NOT LIKE '%工事%'
    AND item_id NOT IN (SELECT item_id FROM bid_projects)
    ORDER BY rj_len DESC LIMIT 10
""").fetchall()
for r in rows:
    print(f"  {r['item_id']:>7} rj={r['rj_len']:>5} desc={r['has_desc']} muni={r['muni_code'] or '?':>8} {(r['title'] or '?')[:60]}")

print()
print("=== DB stats ===")
total = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
with_desc = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE raw_json LIKE '%ProjectDescription%'").fetchone()[0]
no_deadline = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline IS NULL").fetchone()[0]
no_amount = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE amount IS NULL OR amount = ''").fetchone()[0]
no_dept = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE department IS NULL OR department = ''").fetchone()[0]
no_muni = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code IS NULL OR muni_code = ''").fetchone()[0]
dup_titles = conn.execute("SELECT COUNT(*) FROM (SELECT title, muni_code, COUNT(*) as c FROM procurement_items GROUP BY title, muni_code HAVING c > 1)").fetchone()[0]
distinct_munis = conn.execute("SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code IS NOT NULL AND muni_code != ''").fetchone()[0]
print(f"  total={total}, with_desc={with_desc}, distinct_munis={distinct_munis}")
print(f"  no_deadline={no_deadline} ({no_deadline/total*100:.1f}%)")
print(f"  no_amount={no_amount} ({no_amount/total*100:.1f}%)")
print(f"  no_dept={no_dept} ({no_dept/total*100:.1f}%)")
print(f"  no_muni={no_muni} ({no_muni/total*100:.1f}%)")
print(f"  dup_title_muni_groups={dup_titles}")
