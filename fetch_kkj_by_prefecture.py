#!/usr/bin/env python3
"""Fetch KKJ API by prefecture (LG_Code) with 7-day windows.
This captures municipal bids that keyword search misses.
"""
import sqlite3
import urllib.request
import urllib.parse
import ssl
import re
import time
from datetime import datetime, timedelta

DB = "/app/data/monitor.db"
KKJ_API = "https://www.kkj.go.jp/api/"
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

def classify_cat(title):
    for c, ks in [("construction",["工事","修繕","改修","建設","舗装","解体","補修","塗装"]),
                  ("service",["業務委託","委託","役務","保守","清掃","運営","警備","給食"]),
                  ("goods",["購入","調達","物品","リース","賃貸借","車両"]),
                  ("consulting",["設計","測量","調査","コンサル","策定"]),
                  ("it",["システム","ソフトウェア","ネットワーク","DX","AI","ICT"])]:
        for k in ks:
            if k in title: return c
    return "other"

def fetch_pref_window(pref_code, date_from, date_to):
    """Fetch all bids for one prefecture in one date window."""
    records = []
    start = 1
    while True:
        params = urllib.parse.urlencode({
            "LG_Code": f"{pref_code:02d}",
            "CFT_Issue_Date": f"{date_from}/{date_to}",
            "Count": "100",
            "Start": str(start)
        })
        try:
            req = urllib.request.Request(f"{KKJ_API}?{params}", headers={"User-Agent": "Mozilla/5.0"})
            xml = urllib.request.urlopen(req, timeout=30, context=SSL_CTX).read().decode("utf-8")

            batch = []
            for m in re.finditer(r'<SearchResult>(.*?)</SearchResult>', xml, re.DOTALL):
                block = m.group(1)
                def ex(tag):
                    mm = re.search(rf'<{tag}>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</{tag}>', block, re.DOTALL)
                    return mm.group(1).strip() if mm else ""

                title = ex("ProjectName")
                if not title: continue

                cft = ex("CftIssueDate")
                org = ex("OrganizationName")
                city_code = ex("CityCode")
                city_name = ex("CityName")
                ext_url = ex("ExternalDocumentURI")
                desc = ex("ProjectDescription")

                deadline = ""
                dl_m = re.search(r'令和(\d+)年(\d+)月(\d+)日', desc[:500])
                if dl_m:
                    y = int(dl_m.group(1)) + 2018
                    if 2024 <= y <= 2027:
                        deadline = f"{y}-{int(dl_m.group(2)):02d}-{int(dl_m.group(3)):02d}"

                detected = cft[:10] if cft else date_from
                muni_code = city_code if city_code and len(city_code) == 6 else f"{pref_code:02d}0000"

                batch.append({
                    "title": title, "detected_at": detected, "deadline": deadline,
                    "org": org, "url": ext_url, "category": classify_cat(title),
                    "muni_code": muni_code
                })

            records.extend(batch)

            # Check if more pages
            hits_m = re.search(r'<SearchHits>(\d+)</SearchHits>', xml)
            total_hits = int(hits_m.group(1)) if hits_m else 0
            if start + 100 > total_hits or len(batch) < 100:
                break
            start += 100
            time.sleep(0.2)
        except:
            break

    return records

def main():
    conn = sqlite3.connect(DB, timeout=30)
    cur = conn.cursor()

    end_date = datetime.now()
    total_inserted = 0

    for pref in range(1, 48):
        pref_inserted = 0
        current = end_date - timedelta(days=120)

        while current < end_date:
            window_end = min(current + timedelta(days=7), end_date)
            d1 = current.strftime("%Y-%m-%d")
            d2 = window_end.strftime("%Y-%m-%d")

            records = fetch_pref_window(pref, d1, d2)
            for r in records:
                cur.execute("""INSERT OR IGNORE INTO procurement_items
                    (muni_code, detected_at, title, item_type, deadline, method, url, department, category)
                    VALUES (?, ?, ?, 'general_competitive', ?, '', ?, ?, ?)""",
                    (r["muni_code"], r["detected_at"], r["title"], r["deadline"],
                     r["url"], r["org"], r["category"]))
                pref_inserted += cur.rowcount

            current = window_end
            time.sleep(0.2)

        conn.commit()
        total_inserted += pref_inserted
        if pref_inserted > 0:
            print(f"  Pref {pref:02d}: +{pref_inserted}", flush=True)

    cur.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline >= date('now')")
    open_bids = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM procurement_items")
    total = cur.fetchone()[0]

    print(f"\nTotal new from prefecture scan: {total_inserted}", flush=True)
    print(f"Open bids: {open_bids}", flush=True)
    print(f"Total items: {total}", flush=True)
    conn.close()

if __name__ == "__main__":
    main()
