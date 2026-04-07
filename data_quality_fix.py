#!/usr/bin/env python3
"""Fix data quality issues in dashboard DB."""
import sqlite3

DB = "/app/data/monitor.db"
conn = sqlite3.connect(DB)
cur = conn.cursor()

# Fix extreme future dates
cur.execute("SELECT COUNT(*) FROM procurement_items WHERE detected_at > '2026-12-31'")
print(f"Extreme future dates: {cur.fetchone()[0]}", flush=True)
cur.execute("UPDATE procurement_items SET detected_at = '2026-04-08' WHERE detected_at > '2026-12-31'")
print(f"Fixed: {cur.rowcount}", flush=True)

# Reclassify NULL/None categories
for cat, keywords in [
    ("service", ["%委託%", "%役務%", "%保守%", "%清掃%", "%運営%", "%警備%", "%派遣%", "%収集%"]),
    ("construction", ["%工事%", "%修繕%", "%改修%", "%建設%", "%舗装%", "%解体%", "%補修%"]),
    ("goods", ["%購入%", "%物品%", "%リース%", "%賃貸借%", "%車両%", "%機器%"]),
    ("it", ["%システム%", "%DX%", "%AI%", "%デジタル%", "%ICT%", "%ネットワーク%", "%サーバ%", "%GIS%"]),
    ("consulting", ["%調査%", "%設計%", "%測量%", "%コンサル%", "%策定%"]),
]:
    for kw in keywords:
        cur.execute(f"UPDATE procurement_items SET category = ? WHERE (category IS NULL OR category = 'None') AND title LIKE ?",
            (cat, kw))

cur.execute("UPDATE procurement_items SET category = 'other' WHERE category IS NULL OR category = 'None'")
print(f"Remaining NULL→other: {cur.rowcount}", flush=True)

# Also improve 'other' classification
reclassified = 0
for cat, keywords in [
    ("service", ["%保守%", "%清掃%", "%運営%", "%警備%", "%派遣%", "%収集%", "%管理業務%"]),
    ("it", ["%ネットワーク%", "%デジタル%", "%ICT%", "%サーバ%", "%ソフトウェア%"]),
    ("construction", ["%塗装%", "%防水%", "%配管%", "%電気%"]),
    ("consulting", ["%コンサル%", "%検討%"]),
]:
    for kw in keywords:
        cur.execute("UPDATE procurement_items SET category = ? WHERE category = 'other' AND title LIKE ?", (cat, kw))
        reclassified += cur.rowcount
print(f"Reclassified other→specific: {reclassified}", flush=True)

conn.commit()

cur.execute("SELECT category, COUNT(*), ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM procurement_items),1) FROM procurement_items GROUP BY category ORDER BY COUNT(*) DESC")
print("\nFinal category distribution:", flush=True)
for cat, cnt, pct in cur.fetchall():
    print(f"  {cat}: {cnt} ({pct}%)", flush=True)
conn.close()
