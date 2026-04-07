"""Fetch central government ministry RSS feeds and insert as news items.
All P1 sources from Track B analysis.
"""
import sqlite3
import xml.etree.ElementTree as ET
import urllib.request
import ssl
import os
from datetime import datetime

DB_PATH = os.environ.get("DATA_DIR", "/app/data") + "/monitor.db"

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

GOV_RSS_FEEDS = [
    ("総務省", "https://www.soumu.go.jp/news.rdf", "policy"),
    ("国土交通省_プレス", "https://www.mlit.go.jp/pressrelease.rdf", "policy"),
    ("国土交通省_災害", "https://www.mlit.go.jp/saigai.rdf", "disaster"),
    ("農林水産省_新着", "http://www.maff.go.jp/j/new/rss.xml", "policy"),
    ("農林水産省_報道", "http://www.maff.go.jp/j/press/rss.xml", "policy"),
]

# NHK regional RSS (major stations)
NHK_STATIONS = [
    ("sapporo", "01"), ("aomori", "02"), ("morioka", "03"), ("sendai", "04"),
    ("akita", "05"), ("yamagata", "06"), ("fukushima", "07"),
    ("mito", "08"), ("utsunomiya", "09"), ("maebashi", "10"),
    ("yokohama", "14"), ("niigata", "15"),
    ("toyama", "16"), ("kanazawa", "17"), ("fukui", "18"),
    ("kofu", "19"), ("nagano", "20"), ("gifu", "21"),
    ("shizuoka", "22"), ("nagoya", "23"), ("tsu", "24"),
    ("otsu", "25"), ("kyoto", "26"), ("osaka", "27"),
    ("kobe", "28"), ("nara", "29"), ("wakayama", "30"),
    ("tottori", "31"), ("matsue", "32"), ("okayama", "33"),
    ("hiroshima", "34"), ("yamaguchi", "35"),
    ("tokushima", "36"), ("takamatsu", "37"), ("matsuyama", "38"),
    ("kochi", "39"), ("fukuoka", "40"), ("saga", "41"),
    ("nagasaki", "42"), ("kumamoto", "43"), ("oita", "44"),
    ("miyazaki", "45"), ("kagoshima", "46"), ("okinawa", "47"),
]

CATEGORY_KW = {
    "procurement": ["入札", "調達", "公告", "契約", "落札"],
    "budget": ["予算", "補正", "決算", "歳出", "財政"],
    "policy": ["計画", "方針", "条例", "DX", "脱炭素"],
    "disaster": ["災害", "防災", "避難", "地震", "台風"],
    "construction": ["工事", "建設", "道路", "橋梁", "施設整備"],
    "personnel": ["人事", "採用", "職員"],
}

def classify_news(title):
    for cat, kws in CATEGORY_KW.items():
        for kw in kws:
            if kw in title:
                return cat
    return "other"

def fetch_rss(url, timeout=30):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 ToG-Intelligence/1.0"})
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  Error fetching {url}: {e}")
        return None

def parse_rss(xml_text):
    items = []
    try:
        root = ET.fromstring(xml_text)
        # Handle RSS 1.0 (RDF), RSS 2.0, and Atom
        ns = {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
              "rss": "http://purl.org/rss/1.0/",
              "dc": "http://purl.org/dc/elements/1.1/",
              "atom": "http://www.w3.org/2005/Atom"}

        # RSS 1.0 (RDF)
        for item in root.findall(".//rss:item", ns):
            title = item.findtext("rss:title", "", ns).strip()
            link = item.findtext("rss:link", "", ns).strip()
            date = item.findtext("dc:date", "", ns).strip()
            desc = item.findtext("rss:description", "", ns)
            if title:
                items.append({"title": title, "url": link, "date": date, "summary": desc or ""})

        # RSS 2.0
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            date = (item.findtext("pubDate") or item.findtext("dc:date", "", ns) or "").strip()
            desc = item.findtext("description") or ""
            if title and {"title": title} not in [{"title": i["title"]} for i in items]:
                items.append({"title": title, "url": link, "date": date, "summary": desc[:500]})

        # Atom
        for entry in root.findall(".//atom:entry", ns):
            title = (entry.findtext("atom:title", "", ns) or "").strip()
            link_el = entry.find("atom:link", ns)
            link = link_el.get("href", "") if link_el is not None else ""
            date = (entry.findtext("atom:published", "", ns) or entry.findtext("atom:updated", "", ns) or "").strip()
            summary = entry.findtext("atom:summary", "", ns) or ""
            if title:
                items.append({"title": title, "url": link, "date": date, "summary": summary[:500]})
    except ET.ParseError:
        pass
    return items

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    total_inserted = 0

    # Central government feeds
    for source_name, url, default_cat in GOV_RSS_FEEDS:
        print(f"Fetching {source_name}...")
        xml = fetch_rss(url)
        if not xml:
            continue
        items = parse_rss(xml)
        count = 0
        for item in items:
            cat = classify_news(item["title"]) if classify_news(item["title"]) != "other" else default_cat
            try:
                cur.execute("""INSERT OR IGNORE INTO muni_news
                    (muni_code, title, source, url, published_at, summary, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    ("NATIONAL", item["title"], source_name, item["url"],
                     item["date"] or datetime.now().isoformat(), item["summary"][:500], cat))
                count += cur.rowcount
            except Exception:
                pass
        total_inserted += count
        print(f"  {len(items)} items parsed, {count} new inserted")

    # NHK regional feeds
    nhk_total = 0
    for station, pref_code in NHK_STATIONS:
        url = f"https://www3.nhk.or.jp/lnews/{station}/nhk_{station}.xml"
        xml = fetch_rss(url)
        if not xml:
            continue
        items = parse_rss(xml)
        count = 0
        for item in items:
            cat = classify_news(item["title"])
            try:
                cur.execute("""INSERT OR IGNORE INTO muni_news
                    (muni_code, title, source, url, published_at, summary, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (pref_code + "0000", item["title"], f"NHK_{station}",
                     item["url"], item["date"] or datetime.now().isoformat(),
                     item["summary"][:500], cat))
                count += cur.rowcount
            except Exception:
                pass
        nhk_total += count
        if count > 0:
            print(f"  NHK {station}: {len(items)} items, {count} new")

    total_inserted += nhk_total
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM muni_news")
    print(f"\nTotal: {total_inserted} new news items inserted")
    print(f"Total news in DB: {cur.fetchone()[0]}")
    conn.close()

if __name__ == "__main__":
    main()
