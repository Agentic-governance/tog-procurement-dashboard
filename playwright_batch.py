#!/usr/bin/env python3
"""Batch Playwright fetch for uncovered municipalities — simpler version."""
import sqlite3
import csv
import re
import time
from datetime import datetime
from playwright.sync_api import sync_playwright

DB = "/home/deploy/procurement_monitor/data/monitor.db"
CSV = "/home/deploy/procurement_monitor/procurement_url_master_v2.csv"

PROC_KW = ["入札", "公告", "プロポーザル", "公募", "落札", "工事", "委託業務",
           "業務委託", "購入", "発注見通し", "随意契約", "契約締結", "事業者選定", "審査結果"]
NAV_KW = ["トップページ", "ホーム", "サイトマップ", "検索", "メニュー", "文字サイズ",
          "ログイン", "個人情報", "Copyright", "背景色", "Foreign"]

PATHS = ["/nyusatsu/", "/keiyaku/", "/jigyosha/nyusatsu/",
         "/shisei/nyusatsu/", "/gyosei/nyusatsu/", "/business/nyusatsu/"]

def classify_cat(t):
    for c, ks in [("construction",["工事","修繕","改修"]),("service",["委託","役務","保守","清掃"]),
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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
        page = context.new_page()
        page.set_default_timeout(12000)

        for i, (code, name, base_url) in enumerate(munis):
            if i % 10 == 0:
                print(f"Progress: {i}/{len(munis)}, added={added}", flush=True)

            urls = [base_url.rstrip('/') + path for path in PATHS[:4]] + [base_url]
            found = False

            for url in urls:
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=12000)
                    time.sleep(1.5)
                    html = page.content()
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
                except Exception:
                    pass
                time.sleep(0.3)

        browser.close()

    conn.commit()
    cur.execute("SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code != 'NATIONAL'")
    print(f"\nAdded: {added}, Items: {total}, Total munis: {cur.fetchone()[0]}", flush=True)
    conn.close()

if __name__ == "__main__":
    main()
