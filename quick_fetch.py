#!/usr/bin/env python3
"""Quick fetch uncovered municipalities - simplified version."""
import sqlite3, csv, urllib.request, ssl, re, time, sys
from datetime import datetime

DB = "/app/data/monitor.db"
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect(DB)
cur = conn.cursor()
cur.execute("SELECT DISTINCT muni_code FROM procurement_items WHERE muni_code != 'NATIONAL'")
covered = set(r[0] for r in cur.fetchall())

with open("/app/procurement_url_master_v2.csv") as f:
    munis = [(r["muni_code"], r.get("muni_name",""), r.get("procurement_url","").strip())
             for r in csv.DictReader(f) if r["muni_code"] not in covered and r.get("procurement_url","").strip()]

print(f"Uncovered: {len(munis)}", flush=True)
added = 0
items = 0

for code, name, url in munis:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"})
        html = urllib.request.urlopen(req, timeout=8, context=CTX).read().decode("utf-8", errors="replace")
        text = re.sub(r"<[^>]+>", "\n", html)
        found = False
        for line in text.split("\n"):
            ln = line.strip()
            if 12 < len(ln) < 150 and any(k in ln for k in ["入札","公告","プロポーザル","落札","工事委託","業務委託"]):
                if not any(k in ln for k in ["トップ","ホーム","検索","メニュー","ログイン","個人情報","Copyright"]):
                    cat = "other"
                    if any(k in ln for k in ["工事","修繕","改修","建設"]): cat = "construction"
                    elif any(k in ln for k in ["業務委託","委託","役務"]): cat = "service"
                    elif any(k in ln for k in ["購入","物品","リース"]): cat = "goods"
                    elif any(k in ln for k in ["設計","測量","調査"]): cat = "consulting"
                    elif any(k in ln for k in ["システム","DX","AI","デジタル"]): cat = "it"
                    it = "proposal" if "プロポーザル" in ln else ("award" if "落札" in ln else "other")
                    cur.execute("INSERT OR IGNORE INTO procurement_items (muni_code,detected_at,title,item_type,url,category) VALUES (?,?,?,?,?,?)",
                        (code, datetime.now().strftime("%Y-%m-%d"), ln[:200], it, url, cat))
                    if cur.rowcount > 0:
                        items += 1
                        found = True
        if found:
            added += 1
        time.sleep(0.2)
    except:
        pass

conn.commit()
print(f"Added: {added} munis, {items} items", flush=True)
cur.execute("SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code != 'NATIONAL'")
print(f"Total munis: {cur.fetchone()[0]}", flush=True)
cur.execute("SELECT COUNT(*) FROM procurement_items")
print(f"Total items: {cur.fetchone()[0]}", flush=True)
conn.close()
