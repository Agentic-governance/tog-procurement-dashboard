#!/usr/bin/env python3
"""Build municipality context from fiscal_indicators + procurement_items.
Creates a per-municipality intelligence profile for sales strategy.
"""
import sqlite3
import json
from datetime import datetime

DB = "/app/data/monitor.db"

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Create context table
    cur.execute("""CREATE TABLE IF NOT EXISTS muni_context (
        muni_code TEXT PRIMARY KEY,
        muni_name TEXT,
        pref_name TEXT,
        fiscal_strength_index REAL,
        current_balance_ratio REAL,
        procurement_count INTEGER,
        top_category TEXT,
        top_category_count INTEGER,
        proposal_count INTEGER,
        award_count INTEGER,
        it_count INTEGER,
        construction_count INTEGER,
        service_count INTEGER,
        has_dx_items INTEGER DEFAULT 0,
        has_giga_items INTEGER DEFAULT 0,
        has_childcare_items INTEGER DEFAULT 0,
        has_disaster_items INTEGER DEFAULT 0,
        context_json TEXT,
        updated_at TEXT
    )""")

    # Build context for each municipality
    cur.execute("""
    SELECT fi.muni_code, fi.muni_name, fi.pref_name,
           fi.fiscal_strength_index, fi.current_balance_ratio
    FROM fiscal_indicators fi
    """)
    munis = cur.fetchall()
    print(f"Building context for {len(munis)} municipalities...", flush=True)

    built = 0
    for code, name, pref, fsi, cbr in munis:
        # Procurement stats
        cur.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code=?", (code,))
        proc_count = cur.fetchone()[0]

        cur.execute("SELECT category, COUNT(*) as c FROM procurement_items WHERE muni_code=? GROUP BY category ORDER BY c DESC LIMIT 1", (code,))
        top_cat_row = cur.fetchone()
        top_cat = top_cat_row[0] if top_cat_row else None
        top_cat_count = top_cat_row[1] if top_cat_row else 0

        cur.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code=? AND item_type='proposal'", (code,))
        proposal_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code=? AND item_type='award'", (code,))
        award_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code=? AND category='it'", (code,))
        it_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code=? AND category='construction'", (code,))
        construction_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code=? AND category='service'", (code,))
        service_count = cur.fetchone()[0]

        # Theme flags
        cur.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code=? AND (title LIKE '%DX%' OR title LIKE '%デジタル%' OR title LIKE '%AI%')", (code,))
        has_dx = 1 if cur.fetchone()[0] > 0 else 0

        cur.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code=? AND (title LIKE '%GIGA%' OR title LIKE '%タブレット%')", (code,))
        has_giga = 1 if cur.fetchone()[0] > 0 else 0

        cur.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code=? AND (title LIKE '%子育て%' OR title LIKE '%保育%')", (code,))
        has_childcare = 1 if cur.fetchone()[0] > 0 else 0

        cur.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code=? AND (title LIKE '%防災%' OR title LIKE '%消防%')", (code,))
        has_disaster = 1 if cur.fetchone()[0] > 0 else 0

        # Context JSON
        context = {
            "fiscal_tier": "high" if (fsi or 0) >= 0.8 else ("mid" if (fsi or 0) >= 0.4 else "low"),
            "procurement_tier": "active" if proc_count >= 50 else ("moderate" if proc_count >= 10 else "sparse"),
            "primary_category": top_cat,
            "themes": [],
        }
        if has_dx: context["themes"].append("DX")
        if has_giga: context["themes"].append("GIGA")
        if has_childcare: context["themes"].append("childcare")
        if has_disaster: context["themes"].append("disaster")

        cur.execute("""INSERT OR REPLACE INTO muni_context
            (muni_code, muni_name, pref_name, fiscal_strength_index, current_balance_ratio,
             procurement_count, top_category, top_category_count, proposal_count, award_count,
             it_count, construction_count, service_count,
             has_dx_items, has_giga_items, has_childcare_items, has_disaster_items,
             context_json, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (code, name, pref, fsi, cbr, proc_count, top_cat, top_cat_count,
             proposal_count, award_count, it_count, construction_count, service_count,
             has_dx, has_giga, has_childcare, has_disaster,
             json.dumps(context, ensure_ascii=False), datetime.now().isoformat()))
        built += 1

    conn.commit()
    print(f"Context built for {built} municipalities", flush=True)

    # Summary stats
    cur.execute("SELECT COUNT(*) FROM muni_context WHERE has_dx_items=1")
    print(f"  DX active: {cur.fetchone()[0]}", flush=True)
    cur.execute("SELECT COUNT(*) FROM muni_context WHERE has_giga_items=1")
    print(f"  GIGA active: {cur.fetchone()[0]}", flush=True)
    cur.execute("SELECT COUNT(*) FROM muni_context WHERE has_childcare_items=1")
    print(f"  Childcare active: {cur.fetchone()[0]}", flush=True)
    cur.execute("SELECT COUNT(*) FROM muni_context WHERE procurement_count >= 50")
    print(f"  Active procurers (50+): {cur.fetchone()[0]}", flush=True)
    cur.execute("SELECT COUNT(*) FROM muni_context WHERE procurement_count >= 10")
    print(f"  Moderate procurers (10+): {cur.fetchone()[0]}", flush=True)

    conn.close()

if __name__ == "__main__":
    main()
