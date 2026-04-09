#!/usr/bin/env python3
"""CYDEEN PAN system scraper (Hitachi Systems).
The PAN030 search endpoint accepts ALL params as GET query strings.
This is effectively a REST API.

Covers 30+ Osaka-region municipalities.
"""
import sqlite3
import urllib.request
import urllib.parse
import ssl
import http.cookiejar
import re
import time
from datetime import datetime, timedelta

DB = "/app/data/monitor.db"
BASE = "https://www.nyusatsu.ebid-osaka.jp"

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

# Known KIKAN_NO codes (Osaka region)
MUNICIPALITIES = {
    "0203": ("271047", "豊中市"),
    "0209": ("271233", "守口市"),
    "0210": ("271101", "枚方市"),
    "0212": ("271128", "八尾市"),
    "0214": ("271845", "富田林市"),
    "0220": ("271209", "箕面市"),
    "0221": ("271829", "柏原市"),
    "0223": ("271241", "門真市"),
    "0227": ("271276", "東大阪市"),
}

def classify_cat(t):
    for c, ks in [("construction",["工事","修繕","改修","建設","舗装","解体"]),
                  ("service",["業務委託","委託","役務","保守","清掃","運営","警備"]),
                  ("goods",["購入","物品","リース","賃貸借","車両"]),
                  ("consulting",["設計","測量","調査","コンサル","策定"]),
                  ("it",["システム","DX","AI","デジタル","ICT","ネットワーク"])]:
        for k in ks:
            if k in t: return c
    return "other"

def classify_type(t):
    if any(k in t for k in ["プロポーザル","企画提案","公募型"]): return "proposal"
    if any(k in t for k in ["落札","結果"]): return "award"
    if any(k in t for k in ["一般競争","制限付"]): return "general_competitive"
    if "指名" in t: return "designated_competitive"
    return "other"

def scrape_municipality(kikan_no, muni_code, muni_name):
    """Scrape all open bids for one municipality using PAN030 GET endpoint."""
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj),
        urllib.request.HTTPSHandler(context=SSL_CTX)
    )

    items = []

    try:
        # Step 1: Get session via PAN010
        init_url = f"{BASE}/pan/PAN010.do?KIKAN_NO={kikan_no}&SCREEN_ID=PAN010"
        opener.open(urllib.request.Request(init_url, headers={"User-Agent": "Mozilla/5.0"}), timeout=15)

        # Step 2: Search last 120 days via PAN030 (GET!)
        date_from = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
        date_to = datetime.now().strftime("%Y%m%d")

        params = urllib.parse.urlencode({
            "KIKAN_NO": kikan_no,
            "SCREEN_ID": "PAN030",
            "PARAM": "1",
            "KOKOKU_DATE_FROM": date_from,
            "KOKOKU_DATE_TO": date_to,
            "HYOJI_KENSU": "100",
            "INDEX": "0",
        })

        search_url = f"{BASE}/pan/PAN030.do?{params}"
        resp = opener.open(urllib.request.Request(search_url, headers={"User-Agent": "Mozilla/5.0"}), timeout=15)
        html = resp.read().decode("utf-8", errors="replace")

        # Parse results
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if len(cells) >= 3:
                texts = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
                full = ' '.join(texts)
                if any(k in full for k in ["工事","業務","委託","購入","設計","調査","プロポーザル"]):
                    title = texts[0] if len(texts[0]) > 5 else full[:100]
                    if len(title) > 5:
                        deadline = None
                        dl_m = re.search(r'(\d{4})/(\d{2})/(\d{2})', full)
                        if dl_m:
                            deadline = f"{dl_m.group(1)}-{dl_m.group(2)}-{dl_m.group(3)}"
                        items.append({
                            "title": title[:200],
                            "type": classify_type(title),
                            "deadline": deadline,
                            "category": classify_cat(title),
                        })

        # Check for "案件情報" links (CONTROL_NO based)
        control_nos = re.findall(r'CONTROL_NO=(\d+)', html)
        if control_nos:
            print(f"  Found {len(control_nos)} CONTROL_NOs", flush=True)

    except Exception as e:
        print(f"  Error: {str(e)[:60]}", flush=True)

    return items

def main():
    conn = sqlite3.connect(DB, timeout=30)
    cur = conn.cursor()
    total_inserted = 0

    for kikan_no, (muni_code, muni_name) in MUNICIPALITIES.items():
        items = scrape_municipality(kikan_no, muni_code, muni_name)
        for item in items:
            cur.execute("""INSERT OR IGNORE INTO procurement_items
                (muni_code, detected_at, title, item_type, deadline, url, category)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (muni_code, datetime.now().strftime("%Y-%m-%d"), item["title"],
                 item["type"], item["deadline"],
                 f"{BASE}/pan/PAN010.do?KIKAN_NO={kikan_no}",
                 item["category"]))
            total_inserted += cur.rowcount

        conn.commit()
        if items:
            print(f"  {muni_name}: {len(items)} items", flush=True)
        time.sleep(0.5)

    print(f"\nTotal inserted: {total_inserted}", flush=True)
    conn.close()

if __name__ == "__main__":
    main()
