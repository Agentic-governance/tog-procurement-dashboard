#!/usr/bin/env python3
"""Safe Playwright fetch — creates new browser for each municipality."""
import sqlite3
import csv
import re
import time
import subprocess
import json
from datetime import datetime

DB = "/home/deploy/procurement_monitor/data/monitor.db"
CSV = "/home/deploy/procurement_monitor/procurement_url_master_v2.csv"

PROC_KW = ["入札", "公告", "プロポーザル", "公募", "落札", "工事", "委託業務",
           "業務委託", "購入", "発注見通し", "随意契約", "契約締結", "事業者選定"]
NAV_KW = ["トップページ", "ホーム", "サイトマップ", "検索", "メニュー", "文字サイズ",
          "ログイン", "個人情報", "Copyright", "背景色", "Foreign"]

PATHS = ["/nyusatsu/", "/keiyaku/", "/jigyosha/nyusatsu/", "/shisei/nyusatsu/"]

def classify_cat(t):
    for c, ks in [("construction",["工事","修繕","改修"]),("service",["委託","役務","保守"]),
                  ("goods",["購入","物品","リース"]),("consulting",["設計","調査","コンサル"]),
                  ("it",["システム","DX","AI","デジタル","ICT"])]:
        for k in ks:
            if k in t: return c
    return "other"

def classify_type(t):
    if "プロポーザル" in t or "公募型" in t: return "proposal"
    if "落札" in t or "入札結果" in t: return "award"
    if "一般競争" in t: return "general_competitive"
    return "other"

def fetch_with_playwright(url, timeout=15):
    """Run Playwright as subprocess to avoid EPIPE crashes."""
    script = f'''
import json
from playwright.sync_api import sync_playwright
import time
with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_page()
    pg.set_default_timeout({timeout*1000})
    try:
        pg.goto("{url}", wait_until="domcontentloaded", timeout={timeout*1000})
        time.sleep(1.5)
        html = pg.content()
        print(json.dumps({{"html": html[:50000]}}))
    except:
        print(json.dumps({{"html": ""}}))
    b.close()
'''
    try:
        result = subprocess.run(
            ["python3", "-c", script],
            capture_output=True, text=True, timeout=timeout+10
        )
        if result.stdout.strip():
            data = json.loads(result.stdout.strip())
            return data.get("html", "")
    except:
        pass
    return ""

def extract(html):
    text = re.sub(r"<[^>]+>", "\n", html)
    items = []
    seen = set()
    for ln in text.split("\n"):
        ln = ln.strip()
        if 12 < len(ln) < 200 and any(k in ln for k in PROC_KW) and not any(k in ln for k in NAV_KW) and ln not in seen:
            seen.add(ln)
            items.append(ln)
    return items

def main():
    conn = sqlite3.connect(DB, timeout=30)
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT muni_code FROM procurement_items WHERE muni_code != 'NATIONAL'")
    covered = set(r[0] for r in cur.fetchall())

    with open(CSV) as f:
        munis = [(r["muni_code"], r.get("muni_name",""), r.get("base_url","").strip())
                 for r in csv.DictReader(f) if r["muni_code"] not in covered and r.get("base_url","").strip()]

    print(f"Uncovered: {len(munis)}", flush=True)
    total = 0
    added = 0

    for i, (code, name, base_url) in enumerate(munis):
        if i % 10 == 0:
            print(f"Progress: {i}/{len(munis)}, added={added}", flush=True)

        urls = [base_url.rstrip('/') + path for path in PATHS] + [base_url]
        found = False

        for url in urls:
            html = fetch_with_playwright(url)
            if html and len(html) > 500:
                items = extract(html)
                if items:
                    added += 1
                    found = True
                    for title in items[:10]:
                        cur.execute("INSERT OR IGNORE INTO procurement_items (muni_code,detected_at,title,item_type,url,category) VALUES (?,?,?,?,?,?)",
                            (code, datetime.now().strftime("%Y-%m-%d"), title[:200], classify_type(title), url, classify_cat(title)))
                        total += cur.rowcount
                    conn.commit()
                    print(f"  ✓ {name}: {len(items)} items", flush=True)
                    break
            time.sleep(0.3)

    conn.commit()
    cur.execute("SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code != 'NATIONAL'")
    print(f"\nAdded: {added}, Items: {total}, Total munis: {cur.fetchone()[0]}", flush=True)
    conn.close()

if __name__ == "__main__":
    main()
