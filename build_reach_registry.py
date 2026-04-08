#!/usr/bin/env python3
"""Build municipality reach registry — records HOW each municipality was reached
and HOW to reach it again. This is the foundation for reproducible operations.
"""
import sqlite3
import csv
import json
from datetime import datetime

DB = "/app/data/monitor.db"

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Create reach registry table
    cur.execute("""CREATE TABLE IF NOT EXISTS muni_reach_registry (
        muni_code TEXT PRIMARY KEY,
        muni_name TEXT,
        pref_name TEXT,

        -- How we reached this municipality
        reach_method TEXT,           -- govcrawl | snapshot_extract | live_fetch | playwright | webfetch_agent | kkj_api | p_portal | manual
        reach_url TEXT,              -- The URL that worked
        reach_date TEXT,             -- When we last successfully reached

        -- Data sources available
        has_procurement_page INTEGER DEFAULT 0,  -- 1 if we found a procurement URL
        procurement_url TEXT,                     -- Specific procurement page URL
        has_e_bidding INTEGER DEFAULT 0,          -- 1 if uses electronic bidding system
        e_bidding_system TEXT,                    -- efftis, supercals, e-tokyo, etc.
        has_sitemap INTEGER DEFAULT 0,

        -- Access characteristics
        requires_playwright INTEGER DEFAULT 0,    -- 1 if site needs JS rendering
        waf_blocked INTEGER DEFAULT 0,           -- 1 if WAF blocks access
        ssl_error INTEGER DEFAULT 0,             -- 1 if SSL issues
        redirect_url TEXT,                       -- If domain redirected

        -- Data quality
        item_count INTEGER DEFAULT 0,
        has_deadlines INTEGER DEFAULT 0,
        has_amounts INTEGER DEFAULT 0,
        freshness_days INTEGER,                  -- Days since last item

        -- Reproducibility
        last_successful_fetch TEXT,
        fetch_frequency TEXT,                    -- daily | weekly | monthly | manual
        notes TEXT,

        updated_at TEXT
    )""")

    # Load municipality master
    with open("/app/procurement_url_master_v2.csv") as f:
        master = {r["muni_code"]: r for r in csv.DictReader(f)}

    # Load fiscal indicators for names
    cur.execute("SELECT muni_code, muni_name, pref_name FROM fiscal_indicators")
    fiscal = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    # Get per-municipality item stats
    cur.execute("""
    SELECT muni_code, COUNT(*) as cnt,
        SUM(CASE WHEN deadline IS NOT NULL AND deadline != '' THEN 1 ELSE 0 END) as dl_cnt,
        SUM(CASE WHEN amount IS NOT NULL AND amount != '' THEN 1 ELSE 0 END) as amt_cnt,
        MAX(detected_at) as latest,
        GROUP_CONCAT(DISTINCT url) as urls
    FROM procurement_items
    WHERE muni_code != 'NATIONAL'
    GROUP BY muni_code
    """)

    entries = 0
    for code, cnt, dl_cnt, amt_cnt, latest, urls in cur.fetchall():
        name = fiscal.get(code, (master.get(code, {}).get("muni_name", ""), ""))[0]
        pref = fiscal.get(code, ("", master.get(code, {}).get("pref_code", "")))[1]

        # Determine reach method from URLs and data patterns
        url_list = (urls or "").split(",")
        reach_method = "unknown"
        reach_url = ""
        procurement_url = ""
        requires_playwright = 0
        has_e_bidding = 0
        e_bidding_system = ""

        for u in url_list:
            if not u or u == "":
                continue
            if "kkj.go.jp" in u:
                reach_method = "kkj_api"
                reach_url = u
            elif "p-portal" in u:
                reach_method = "p_portal"
                reach_url = u
            elif "efftis.jp" in u:
                has_e_bidding = 1
                e_bidding_system = "efftis"
                reach_url = u
            elif "e-procurement" in u or "e-nyusatsu" in u:
                has_e_bidding = 1
                e_bidding_system = "e-procurement"
                reach_url = u
            elif u.startswith("http"):
                if reach_method == "unknown":
                    reach_method = "web_fetch"
                    reach_url = u
                if "nyusatsu" in u or "keiyaku" in u or "nyuusatsu" in u:
                    procurement_url = u

        # If no URL at all, it came from snapshot extraction
        if reach_method == "unknown":
            if cnt > 0:
                reach_method = "snapshot_extract"
                base = master.get(code, {}).get("base_url", "")
                reach_url = base

        # Freshness
        freshness_days = None
        if latest:
            try:
                last_date = datetime.strptime(latest[:10], "%Y-%m-%d")
                freshness_days = (datetime.now() - last_date).days
            except:
                pass

        m = master.get(code, {})
        if not procurement_url:
            procurement_url = m.get("procurement_url", "")

        cur.execute("""INSERT OR REPLACE INTO muni_reach_registry
            (muni_code, muni_name, pref_name, reach_method, reach_url, reach_date,
             has_procurement_page, procurement_url, has_e_bidding, e_bidding_system,
             requires_playwright, item_count, has_deadlines, has_amounts,
             freshness_days, last_successful_fetch, fetch_frequency, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (code, name, pref, reach_method, reach_url, latest,
             1 if procurement_url else 0, procurement_url, has_e_bidding, e_bidding_system,
             requires_playwright, cnt, 1 if dl_cnt > 0 else 0, 1 if amt_cnt > 0 else 0,
             freshness_days, latest, "daily" if reach_method in ("kkj_api", "p_portal") else "weekly",
             datetime.now().isoformat()))
        entries += 1

    # Also add unreached municipalities
    cur.execute("SELECT DISTINCT muni_code FROM procurement_items WHERE muni_code != 'NATIONAL'")
    reached = set(r[0] for r in cur.fetchall())

    for code, m in master.items():
        if code not in reached:
            name = fiscal.get(code, (m.get("muni_name", ""), ""))[0]
            pref = fiscal.get(code, ("", m.get("pref_code", "")))[1]
            cur.execute("""INSERT OR IGNORE INTO muni_reach_registry
                (muni_code, muni_name, pref_name, reach_method, reach_url,
                 has_procurement_page, procurement_url, item_count, fetch_frequency, updated_at)
                VALUES (?, ?, ?, 'unreached', ?, ?, ?, 0, 'manual', ?)""",
                (code, name, pref, m.get("base_url", ""),
                 1 if m.get("procurement_url") else 0, m.get("procurement_url", ""),
                 datetime.now().isoformat()))
            entries += 1

    conn.commit()

    # Summary
    cur.execute("SELECT reach_method, COUNT(*), SUM(item_count) FROM muni_reach_registry GROUP BY reach_method ORDER BY COUNT(*) DESC")
    print(f"Registry entries: {entries}", flush=True)
    print("\nReach method distribution:", flush=True)
    for method, cnt, items in cur.fetchall():
        print(f"  {method}: {cnt} municipalities, {items or 0} items", flush=True)

    cur.execute("SELECT COUNT(*) FROM muni_reach_registry WHERE has_procurement_page = 1")
    print(f"\nWith procurement URL: {cur.fetchone()[0]}", flush=True)
    cur.execute("SELECT COUNT(*) FROM muni_reach_registry WHERE has_e_bidding = 1")
    print(f"With e-bidding system: {cur.fetchone()[0]}", flush=True)
    cur.execute("SELECT COUNT(*) FROM muni_reach_registry WHERE freshness_days IS NOT NULL AND freshness_days <= 30")
    print(f"Fresh data (≤30 days): {cur.fetchone()[0]}", flush=True)
    cur.execute("SELECT COUNT(*) FROM muni_reach_registry WHERE freshness_days IS NOT NULL AND freshness_days > 90")
    print(f"Stale data (>90 days): {cur.fetchone()[0]}", flush=True)

    conn.close()

if __name__ == "__main__":
    main()
