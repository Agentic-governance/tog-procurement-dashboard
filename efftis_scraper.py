#!/usr/bin/env python3
"""efftis (Toshiba) e-bidding system scraper.
Accesses PPI (入札情報公開) pages for all municipalities using the reverse-engineered URL structure.

URL pattern:
  Session init: {base}/PPUBC00100?kikanno={org_code}
  Navigation:   {base}/PPUBC00100!link?screenId={screen}&chotatsu_kbn={kbn}&organizationNumber={org}
  Download:     {base}/PPUBC01200!download?targetNo={id}

Screens: PPUBC01200=公告一覧, PPUBC01300=予定, PPUBC00400=結果, PPUBC00700=発注見通し
chotatsu_kbn: 00=工事, 01=コンサル, 11=物品, 20=その他
"""
import sqlite3
import urllib.request
import ssl
import http.cookiejar
import re
import time
import json
from datetime import datetime

DB = "/app/data/monitor.db"

# efftis instances and their municipalities
EFFTIS_INSTANCES = {
    "toyama": {
        "base": "https://toyama.efftis.jp/ebid02/PPI/Public",
        "municipalities": {
            "162019": "富山市",
            "162027": "高岡市",
            "162035": "魚津市",
            "162043": "氷見市",
            "162051": "滑川市",
            "162060": "黒部市",
            "162078": "砺波市",
            "162086": "小矢部市",
            "162094": "南砺市",
            "162108": "射水市",
            "163210": "舟橋村",
            "163228": "上市町",
            "163236": "立山町",
            "163422": "入善町",
            "163431": "朝日町",
        },
    },
}

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

SCREENS = [
    ("PPUBC01200", "入札公告"),
    ("PPUBC01300", "入札予定"),
    ("PPUBC00400", "入札結果"),
]
KBNS = [("00", "工事"), ("01", "コンサル"), ("11", "物品")]

def classify_cat(title):
    for c, ks in [("construction",["工事","修繕","改修","建設","舗装","解体"]),
                  ("service",["業務委託","委託","役務","保守","清掃","運営"]),
                  ("goods",["購入","物品","リース","賃貸借"]),
                  ("consulting",["設計","測量","調査","コンサル"]),
                  ("it",["システム","DX","AI","デジタル","ICT"])]:
        for k in ks:
            if k in title: return c
    return "other"

def extract_items(html, muni_code, screen_type):
    """Extract bid items from efftis HTML response."""
    items = []
    # Parse table rows with bid information
    # efftis uses table-based layout
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(cells) >= 3:
            # Clean cell content
            clean_cells = []
            for cell in cells:
                text = re.sub(r'<[^>]+>', '', cell).strip()
                text = re.sub(r'\s+', ' ', text)
                clean_cells.append(text)

            # Look for bid-like content
            full_text = ' '.join(clean_cells)
            if any(k in full_text for k in ["工事", "業務", "委託", "購入", "設計", "調査", "測量"]):
                title = clean_cells[0] if clean_cells[0] and len(clean_cells[0]) > 5 else full_text[:100]
                if len(title) > 5:
                    # Extract deadline if present
                    deadline = None
                    dl_m = re.search(r'令和(\d+)年(\d+)月(\d+)日', full_text)
                    if dl_m:
                        y = int(dl_m.group(1)) + 2018
                        if 2024 <= y <= 2027:
                            deadline = f"{y}-{int(dl_m.group(2)):02d}-{int(dl_m.group(3)):02d}"

                    items.append({
                        "title": title[:200],
                        "type": "award" if screen_type == "入札結果" else "general_competitive",
                        "deadline": deadline,
                        "category": classify_cat(title),
                    })
    return items

def scrape_instance(instance_name, config):
    """Scrape all municipalities for one efftis instance."""
    base = config["base"]
    results = {}

    for muni_code, muni_name in config["municipalities"].items():
        muni_items = []

        # Create session
        cj = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(cj),
            urllib.request.HTTPSHandler(context=SSL_CTX)
        )

        try:
            # Step 1: Init session
            init_url = f"{base}/PPUBC00100?kikanno={muni_code}"
            opener.open(urllib.request.Request(init_url, headers={"User-Agent": "Mozilla/5.0"}), timeout=10)

            # Step 2: Navigate to each screen × category
            for screen_id, screen_name in SCREENS:
                for kbn, kbn_name in KBNS:
                    nav_url = f"{base}/PPUBC00100!link?screenId={screen_id}&chotatsu_kbn={kbn}&organizationNumber={muni_code}"
                    try:
                        resp = opener.open(urllib.request.Request(nav_url, headers={"User-Agent": "Mozilla/5.0"}), timeout=10)
                        html = resp.read().decode("utf-8", errors="replace")
                        items = extract_items(html, muni_code, screen_name)
                        muni_items.extend(items)
                    except:
                        pass
                    time.sleep(0.3)
        except:
            pass

        if muni_items:
            results[muni_code] = {
                "name": muni_name,
                "items": muni_items,
                "count": len(muni_items),
            }
            print(f"  {muni_name} ({muni_code}): {len(muni_items)} items", flush=True)

        time.sleep(0.5)

    return results

def main():
    conn = sqlite3.connect(DB, timeout=30)
    cur = conn.cursor()
    total_inserted = 0

    for instance_name, config in EFFTIS_INSTANCES.items():
        print(f"\n=== {instance_name} ===", flush=True)
        results = scrape_instance(instance_name, config)

        for muni_code, data in results.items():
            for item in data["items"]:
                cur.execute("""INSERT OR IGNORE INTO procurement_items
                    (muni_code, detected_at, title, item_type, deadline, url, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (muni_code, datetime.now().strftime("%Y-%m-%d"), item["title"],
                     item["type"], item["deadline"],
                     f"{config['base']}/PPUBC00100?kikanno={muni_code}",
                     item["category"]))
                total_inserted += cur.rowcount

        conn.commit()

    print(f"\nTotal inserted: {total_inserted}", flush=True)
    conn.close()

if __name__ == "__main__":
    main()
