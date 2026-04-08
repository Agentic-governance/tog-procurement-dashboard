#!/usr/bin/env python3
"""Enhanced KKJ API fetcher with deadline extraction."""
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
    for c, ks in [("construction",["工事","修繕","改修","建設","舗装","解体"]),
                  ("service",["業務委託","委託","役務","保守","点検","清掃","運営"]),
                  ("goods",["購入","調達","納入","物品","リース","賃貸借","車両"]),
                  ("consulting",["設計","測量","調査","コンサル","計画策定"]),
                  ("it",["システム","ソフトウェア","ネットワーク","サーバ","DX","AI","ICT"])]:
        for k in ks:
            if k in title: return c
    return "other"

def parse_kkj_xml(xml_text):
    """Parse KKJ XML response — custom parser for CDATA-heavy format."""
    records = []
    # Simple regex-based parser for this specific XML format
    for match in re.finditer(r'<SearchResult>(.*?)</SearchResult>', xml_text, re.DOTALL):
        block = match.group(1)

        def extract(tag):
            m = re.search(rf'<{tag}>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</{tag}>', block, re.DOTALL)
            return m.group(1).strip() if m else ""

        title = extract("ProjectName")
        if not title:
            continue

        cft_date = extract("CftIssueDate")  # Publication date
        org = extract("OrganizationName")
        pref = extract("PrefectureName")
        city_code = extract("CityCode")
        city_name = extract("CityName")
        ext_url = extract("ExternalDocumentURI")
        description = extract("ProjectDescription")

        # Extract deadline from description
        deadline = ""
        dl_match = re.search(r'(?:入札|開札|締切|提出期限).*?(?:令和|平成)?\d+年\d+月\d+日', description)
        if dl_match:
            dm = re.search(r'(?:令和(\d+)|平成(\d+))?年?(\d+)月(\d+)日', dl_match.group())
            if dm:
                reiwa, heisei, month, day = dm.groups()
                if reiwa:
                    year = int(reiwa) + 2018
                elif heisei:
                    year = int(heisei) + 1988
                else:
                    year = datetime.now().year
                deadline = f"{year}-{int(month):02d}-{int(day):02d}"

        # Extract date from CftIssueDate
        detected_at = ""
        if cft_date:
            dm = re.match(r'(\d{4})-(\d{2})-(\d{2})', cft_date)
            if dm:
                detected_at = dm.group(0)

        # Determine muni_code
        muni_code = "NATIONAL"
        if city_code and len(city_code) == 6:
            muni_code = city_code

        records.append({
            "title": title,
            "detected_at": detected_at or datetime.now().strftime("%Y-%m-%d"),
            "deadline": deadline,
            "org": org,
            "muni_code": muni_code,
            "url": ext_url,
            "category": classify_cat(title),
        })

    return records

def main():
    date_to = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")

    keywords = ["入札", "業務", "工事", "調達", "委託", "購入", "賃貸借", "修繕", "設計", "システム"]
    seen = set()
    all_records = []

    for kw in keywords:
        if len(all_records) >= 5000:
            break
        params = urllib.parse.urlencode({"Query": kw, "CFT_Issue_Date": f"{date_from}/{date_to}", "Count": "1000"})
        url = f"{KKJ_API}?{params}"
        print(f"KKJ [{kw}]: ", end="", flush=True)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            resp = urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
            xml_text = resp.read().decode("utf-8")
            records = parse_kkj_xml(xml_text)
            new = 0
            for r in records:
                key = r["title"]
                if key not in seen:
                    seen.add(key)
                    all_records.append(r)
                    new += 1
            print(f"{len(records)} parsed, {new} new (total: {len(all_records)})", flush=True)
        except Exception as e:
            print(f"error: {str(e)[:60]}", flush=True)
        time.sleep(0.5)

    print(f"\nTotal records: {len(all_records)}", flush=True)
    with_deadline = sum(1 for r in all_records if r["deadline"])
    print(f"With deadline: {with_deadline}", flush=True)

    # Insert to DB
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    inserted = 0
    for r in all_records:
        cur.execute("""INSERT OR IGNORE INTO procurement_items
            (muni_code, detected_at, title, item_type, deadline, method, url, department, category)
            VALUES (?, ?, ?, 'general_competitive', ?, '', ?, ?, ?)""",
            (r["muni_code"], r["detected_at"], r["title"], r["deadline"],
             r["url"], r["org"], r["category"]))
        inserted += cur.rowcount
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline IS NOT NULL AND deadline != ''")
    print(f"\nInserted: {inserted}", flush=True)
    print(f"Total items with deadline: {cur.fetchone()[0]}", flush=True)
    cur.execute("SELECT COUNT(*) FROM procurement_items")
    print(f"Total items: {cur.fetchone()[0]}", flush=True)
    conn.close()

if __name__ == "__main__":
    main()
