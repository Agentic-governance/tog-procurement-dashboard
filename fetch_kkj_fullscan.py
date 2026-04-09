#!/usr/bin/env python3
"""Full KKJ API scan — bypass 1000-item limit by splitting date ranges.
Instead of 60-day window → split into 7-day windows × many queries.
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

KEYWORDS = ["入札", "業務", "工事", "調達", "委託", "購入", "賃貸借", "修繕", "設計", "システム",
            "保守", "清掃", "警備", "検査", "測量", "給食", "印刷", "草刈", "電気", "塗装"]

def classify_cat(title):
    for c, ks in [("construction",["工事","修繕","改修","建設","舗装","解体","補修","塗装","防水"]),
                  ("service",["業務委託","委託","役務","保守","清掃","運営","警備","給食"]),
                  ("goods",["購入","調達","物品","リース","賃貸借","車両","機器"]),
                  ("consulting",["設計","測量","調査","コンサル","策定"]),
                  ("it",["システム","ソフトウェア","ネットワーク","DX","AI","ICT"])]:
        for k in ks:
            if k in title: return c
    return "other"

def fetch_window(date_from, date_to, keyword):
    """Fetch one keyword for one date window."""
    params = urllib.parse.urlencode({
        "Query": keyword,
        "CFT_Issue_Date": f"{date_from}/{date_to}",
        "Count": "1000"
    })
    url = f"{KKJ_API}?{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=30, context=SSL_CTX)
        xml = resp.read().decode("utf-8")

        records = []
        for m in re.finditer(r'<SearchResult>(.*?)</SearchResult>', xml, re.DOTALL):
            block = m.group(1)
            def ex(tag):
                mm = re.search(rf'<{tag}>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</{tag}>', block, re.DOTALL)
                return mm.group(1).strip() if mm else ""

            title = ex("ProjectName")
            if not title:
                continue

            cft = ex("CftIssueDate")
            org = ex("OrganizationName")
            ext_url = ex("ExternalDocumentURI")
            desc = ex("ProjectDescription")

            # Extract deadline
            deadline = ""
            dl_m = re.search(r'令和(\d+)年(\d+)月(\d+)日', desc[:500])
            if dl_m:
                y = int(dl_m.group(1)) + 2018
                if 2024 <= y <= 2027:
                    deadline = f"{y}-{int(dl_m.group(2)):02d}-{int(dl_m.group(3)):02d}"

            detected = ""
            if cft:
                dm = re.match(r'(\d{4}-\d{2}-\d{2})', cft)
                if dm: detected = dm.group(1)
            if not detected:
                detected = date_from

            records.append({
                "title": title, "detected_at": detected, "deadline": deadline,
                "org": org, "url": ext_url, "category": classify_cat(title)
            })

        # Check if we hit the 1000 limit
        hits_m = re.search(r'<SearchHits>(\d+)</SearchHits>', xml)
        total_hits = int(hits_m.group(1)) if hits_m else 0

        return records, total_hits
    except Exception as e:
        return [], 0

def main():
    conn = sqlite3.connect(DB, timeout=30)
    cur = conn.cursor()

    # Scan 7-day windows for the last 120 days
    end_date = datetime.now()
    window_days = 7
    total_inserted = 0
    total_records = 0
    windows_over_limit = 0

    for kw in KEYWORDS:
        kw_inserted = 0
        current = end_date - timedelta(days=120)

        while current < end_date:
            window_end = min(current + timedelta(days=window_days), end_date)
            d1 = current.strftime("%Y-%m-%d")
            d2 = window_end.strftime("%Y-%m-%d")

            records, total_hits = fetch_window(d1, d2, kw)

            if total_hits >= 1000:
                windows_over_limit += 1

            for r in records:
                cur.execute("""INSERT OR IGNORE INTO procurement_items
                    (muni_code, detected_at, title, item_type, deadline, method, url, department, category)
                    VALUES (?, ?, ?, 'general_competitive', ?, '', ?, ?, ?)""",
                    ("NATIONAL", r["detected_at"], r["title"], r["deadline"],
                     r["url"], r["org"], r["category"]))
                kw_inserted += cur.rowcount

            total_records += len(records)
            current = window_end
            time.sleep(0.3)

        conn.commit()
        total_inserted += kw_inserted
        if kw_inserted > 0:
            print(f"  [{kw}]: +{kw_inserted}", flush=True)

    cur.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline >= date('now')")
    open_bids = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM procurement_items")
    total = cur.fetchone()[0]

    print(f"\nResults:", flush=True)
    print(f"  Total records fetched: {total_records}", flush=True)
    print(f"  New items inserted: {total_inserted}", flush=True)
    print(f"  Windows over 1000 limit: {windows_over_limit}", flush=True)
    print(f"  Open bids now: {open_bids}", flush=True)
    print(f"  Total items: {total}", flush=True)

    conn.close()

if __name__ == "__main__":
    main()
