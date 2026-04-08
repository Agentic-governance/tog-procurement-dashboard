#!/usr/bin/env python3
"""Auto-discover procurement page URLs from base URLs using common path patterns.
Then fetch and extract procurement items.
"""
import sqlite3
import csv
import urllib.request
import ssl
import re
import time
from datetime import datetime

DB = "/app/data/monitor.db"
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

# Common procurement page paths (ordered by likelihood)
PATHS = [
    "/nyusatsu/", "/keiyaku/", "/sangyo/nyusatsu/",
    "/business/nyusatsu/", "/shisei/nyusatsu/", "/gyosei/nyusatsu/",
    "/kurashi/sangyo_business/nyusatsujoho/",
    "/soshiki/keiyaku/", "/category/3-1-0-0-0-0-0-0-0-0.html",
    "/hotnews/category/32.html", "/hotnews/category/113.html",
    "/site/nyusatukannrennjyouhou/", "/zaisei/keiyaku-kanri/",
    "/jigyosha/nyusatsu/", "/jigyosha/keiyaku/",
    "/shisei/jigyosha/nyusatsu/", "/bunya/nyusatsu/",
]

PROC_KW = ["入札", "公告", "プロポーザル", "公募", "落札", "工事", "委託業務", "業務委託", "購入", "見積"]
NAV_KW = ["トップページ", "ホーム", "サイトマップ", "検索", "メニュー", "文字サイズ",
          "ログイン", "個人情報", "プライバシー", "著作権", "Copyright", "JavaScript",
          "背景色", "読み上げ", "Foreign", "language"]

def classify_cat(t):
    for c, ks in [("construction",["工事","修繕","改修","建設","舗装","解体"]),
                  ("service",["業務委託","委託","役務","保守","清掃","運営","警備"]),
                  ("goods",["購入","物品","リース","賃貸借","車両"]),
                  ("consulting",["設計","測量","調査","コンサル","策定"]),
                  ("it",["システム","DX","AI","デジタル","ICT","ネットワーク","LINE"])]:
        for k in ks:
            if k in t: return c
    return "other"

def classify_type(t):
    if any(k in t for k in ["プロポーザル","企画提案","公募型"]): return "proposal"
    if any(k in t for k in ["落札","入札結果"]): return "award"
    if any(k in t for k in ["一般競争","制限付"]): return "general_competitive"
    return "other"

def try_fetch(url, timeout=8):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"})
        resp = urllib.request.urlopen(req, timeout=timeout, context=CTX)
        if resp.status == 200:
            return resp.read().decode("utf-8", errors="replace")
    except:
        pass
    return None

def extract(html):
    text = re.sub(r"<[^>]+>", "\n", html)
    items = []
    seen = set()
    for line in text.split("\n"):
        ln = line.strip()
        if 12 < len(ln) < 150 and any(k in ln for k in PROC_KW) and not any(k in ln for k in NAV_KW) and ln not in seen:
            seen.add(ln)
            items.append(ln)
    return items

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT muni_code FROM procurement_items WHERE muni_code != 'NATIONAL'")
    covered = set(r[0] for r in cur.fetchall())

    with open("/app/procurement_url_master_v2.csv") as f:
        munis = [(r["muni_code"], r.get("base_url","").strip()) for r in csv.DictReader(f)
                 if r["muni_code"] not in covered and r.get("base_url","").strip()]

    print(f"Uncovered with base_url: {len(munis)}", flush=True)

    total_inserted = 0
    munis_added = 0
    urls_tried = 0

    for code, base_url in munis:
        base = base_url.rstrip("/")
        found = False

        for path in PATHS[:8]:  # Try top 8 paths
            url = base + path
            urls_tried += 1
            html = try_fetch(url)
            if html and len(html) > 500:
                items = extract(html)
                if items:
                    munis_added += 1
                    found = True
                    for title in items[:10]:
                        cur.execute("INSERT OR IGNORE INTO procurement_items (muni_code,detected_at,title,item_type,url,category) VALUES (?,?,?,?,?,?)",
                            (code, datetime.now().strftime("%Y-%m-%d"), title[:200], classify_type(title), url, classify_cat(title)))
                        total_inserted += cur.rowcount
                    break
            time.sleep(0.15)

        if not found:
            # Try the base URL itself
            html = try_fetch(base_url)
            if html:
                items = extract(html)
                if items:
                    munis_added += 1
                    for title in items[:10]:
                        cur.execute("INSERT OR IGNORE INTO procurement_items (muni_code,detected_at,title,item_type,url,category) VALUES (?,?,?,?,?,?)",
                            (code, datetime.now().strftime("%Y-%m-%d"), title[:200], classify_type(title), base_url, classify_cat(title)))
                        total_inserted += cur.rowcount

    conn.commit()
    cur.execute("SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code != 'NATIONAL'")
    print(f"URLs tried: {urls_tried}", flush=True)
    print(f"Munis added: {munis_added}", flush=True)
    print(f"Items inserted: {total_inserted}", flush=True)
    print(f"Total munis: {cur.fetchone()[0]}", flush=True)
    conn.close()

if __name__ == "__main__":
    main()
