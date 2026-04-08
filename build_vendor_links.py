#!/usr/bin/env python3
"""Build vendor-department link table from award data."""
import sqlite3
from datetime import datetime

DB = "/app/data/monitor.db"
conn = sqlite3.connect(DB)
cur = conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS vendor_muni_links (
    vendor TEXT NOT NULL,
    department TEXT NOT NULL,
    contract_count INTEGER,
    primary_category TEXT,
    total_amount_yen REAL,
    updated_at TEXT,
    PRIMARY KEY (vendor, department)
)""")

cur.execute("""
INSERT OR REPLACE INTO vendor_muni_links (vendor, department, contract_count, primary_category, total_amount_yen, updated_at)
SELECT
    method, department, COUNT(*),
    (SELECT category FROM procurement_items pi2
     WHERE pi2.method = procurement_items.method AND pi2.department = procurement_items.department
     GROUP BY category ORDER BY COUNT(*) DESC LIMIT 1),
    SUM(CASE WHEN amount LIKE '%円' THEN CAST(REPLACE(REPLACE(amount, ',', ''), '円', '') AS REAL) ELSE 0 END),
    datetime('now')
FROM procurement_items
WHERE item_type = 'award'
AND method IS NOT NULL AND method != ''
AND department IS NOT NULL AND department != ''
GROUP BY method, department
HAVING COUNT(*) >= 2
""")
print(f"Links created: {cur.rowcount}", flush=True)

conn.commit()

cur.execute("SELECT COUNT(*) FROM vendor_muni_links")
print(f"Total links: {cur.fetchone()[0]}", flush=True)
cur.execute("SELECT COUNT(DISTINCT vendor) FROM vendor_muni_links")
print(f"Unique vendors: {cur.fetchone()[0]}", flush=True)
cur.execute("SELECT COUNT(DISTINCT department) FROM vendor_muni_links")
print(f"Unique departments: {cur.fetchone()[0]}", flush=True)

cur.execute("SELECT vendor, department, contract_count FROM vendor_muni_links ORDER BY contract_count DESC LIMIT 10")
print("\nTop links:", flush=True)
for v, d, c in cur.fetchall():
    print(f"  {v[:30]} x {d[:25]}: {c}", flush=True)

conn.close()
