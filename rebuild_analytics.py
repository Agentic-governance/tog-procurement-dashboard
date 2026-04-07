#!/usr/bin/env python3
"""Full analytics rebuild with subsidy linkages."""
import sqlite3, json
from datetime import datetime

DB = "/app/data/monitor.db"
conn = sqlite3.connect(DB)
cur = conn.cursor()

print("Rebuilding analytics...", flush=True)
cur.execute("DELETE FROM procurement_analytics")

# National monthly
cur.execute("SELECT strftime('%Y-%m', detected_at), COUNT(*) FROM procurement_items WHERE muni_code='NATIONAL' GROUP BY 1")
for m, c in cur.fetchall():
    if m:
        cur.execute("INSERT INTO procurement_analytics (muni_code,metric,period,value,updated_at) VALUES ('NATIONAL','monthly_count',?,?,?)",
            (m, c, datetime.now().isoformat()))

# Category
cur.execute("SELECT category, COUNT(*) FROM procurement_items GROUP BY category")
for cat, c in cur.fetchall():
    cur.execute("INSERT INTO procurement_analytics (muni_code,metric,period,value,updated_at) VALUES ('NATIONAL','category_count',?,?,?)",
        (cat or 'unknown', c, datetime.now().isoformat()))

# Top vendors per category
for cat in ["construction","service","it","consulting","goods"]:
    cur.execute("SELECT method, COUNT(*) FROM procurement_items WHERE item_type='award' AND category=? AND method IS NOT NULL AND method != '' GROUP BY method ORDER BY COUNT(*) DESC LIMIT 10", (cat,))
    for rank, (v, c) in enumerate(cur.fetchall(), 1):
        cur.execute("INSERT INTO procurement_analytics (muni_code,metric,period,value,detail_json,updated_at) VALUES ('NATIONAL',?,?,?,?,?)",
            ("top_vendor_"+cat, "rank_"+str(rank), c,
             json.dumps({"vendor": v}, ensure_ascii=False),
             datetime.now().isoformat()))

# Subsidy linkages
linkages = [
    ("digital_001", "system_standardization", ["システム","標準化"]),
    ("soumu_001", "dx_promotion", ["DX","デジタル"]),
    ("mlit_001", "infrastructure", ["道路","橋梁","下水道"]),
    ("moe_001", "decarbonization", ["脱炭素","省エネ"]),
    ("cfa_001", "childcare", ["子育て","保育"]),
    ("mhlw_002", "care_tech", ["介護","ロボット"]),
    ("jta_001", "inbound", ["観光","インバウンド"]),
    ("mext_001", "giga_school", ["タブレット","GIGA"]),
    ("maff_002", "smart_agri", ["農業","スマート"]),
    ("fdma_001", "fire_facility", ["消防","消火"]),
]
print("Subsidy linkages:", flush=True)
for sid, label, kws in linkages:
    conditions = " OR ".join(["title LIKE '%" + k + "%'" for k in kws])
    cur.execute("SELECT COUNT(*) FROM procurement_items WHERE " + conditions)
    cnt = cur.fetchone()[0]
    cur.execute("INSERT INTO procurement_analytics (muni_code,metric,period,value,detail_json,updated_at) VALUES ('NATIONAL','subsidy_linkage',?,?,?,?)",
        (sid, cnt, json.dumps({"label": label, "items": cnt}, ensure_ascii=False), datetime.now().isoformat()))
    print(f"  {label}: {cnt}", flush=True)

# Prefecture coverage
cur.execute("SELECT substr(muni_code,1,2), COUNT(DISTINCT muni_code), COUNT(*) FROM procurement_items WHERE muni_code != 'NATIONAL' AND length(muni_code)=6 GROUP BY 1")
for pref, munis, items in cur.fetchall():
    cur.execute("INSERT INTO procurement_analytics (muni_code,metric,period,value,updated_at) VALUES (?,'coverage','munis',?,?)",
        (pref+"0000", munis, datetime.now().isoformat()))

# Amount by category
cur.execute("SELECT category, SUM(CAST(REPLACE(REPLACE(amount,',',''),'円','') AS REAL)) FROM procurement_items WHERE amount LIKE '%円' GROUP BY category")
for cat, total in cur.fetchall():
    if total:
        cur.execute("INSERT INTO procurement_analytics (muni_code,metric,period,value,updated_at) VALUES ('NATIONAL','amount_total',?,?,?)",
            (cat, total, datetime.now().isoformat()))

conn.commit()
cur.execute("SELECT COUNT(*) FROM procurement_analytics")
print(f"Analytics rows: {cur.fetchone()[0]}", flush=True)
conn.close()
print("Done!", flush=True)
