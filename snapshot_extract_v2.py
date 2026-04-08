#!/usr/bin/env python3
"""Enhanced snapshot extraction v2 — catches more patterns.
Improvements over v1:
1. Additional keywords: 発注見通し, 随意契約, 指名競争, 総合評価, オープンカウンター, 公募
2. Date extraction from Japanese era (令和/平成)
3. Better title cleaning (remove menu artifacts)
4. Extract from deeper text content (not just line-by-line)
"""
import sqlite3
import re
from datetime import datetime

DB = "/app/data/monitor.db"

# Expanded keywords
PROC_KW = [
    "入札", "公告", "プロポーザル", "公募", "落札", "見積",
    "工事", "委託業務", "業務委託", "購入",
    "発注見通し", "随意契約", "指名競争", "総合評価",
    "オープンカウンター", "契約締結", "入札参加",
    "制限付一般競争", "条件付一般競争", "指名型",
    "企画提案", "企画競争", "事業者選定", "事業者募集",
    "契約候補者", "優先交渉権", "審査結果",
    "仕様書", "設計書", "予定価格", "最低制限価格",
]

NAV_KW = [
    "トップページ", "ホーム", "サイトマップ", "検索", "メニュー", "文字サイズ",
    "ログイン", "個人情報", "プライバシー", "著作権", "Copyright", "JavaScript",
    "背景色", "読み上げ", "Foreign", "language", "お問い合わせ先", "電話番号",
    "ページの先頭", "本文へ", "スマートフォン版", "閲覧補助", "ふりがな",
    "English", "中文", "한국어", "Português",
]

def classify_cat(t):
    for c, ks in [
        ("construction", ["工事","修繕","改修","建設","舗装","解体","補修","塗装","防水","配管","架橋","耐震"]),
        ("service", ["業務委託","委託","役務","保守","点検","清掃","管理業務","運営","警備","派遣","収集","調理","給食","運行"]),
        ("goods", ["購入","調達","納入","物品","備品","機器","車両","リース","賃貸借","燃料","薬品"]),
        ("consulting", ["設計","測量","調査","コンサル","計画策定","策定支援","検討","アドバイザリー"]),
        ("it", ["システム","ソフトウェア","ネットワーク","サーバ","データ","ICT","DX","AI","デジタル","GIS","LINE","kintone","iPad","タブレット"]),
    ]:
        for k in ks:
            if k in t: return c
    return "other"

def classify_type(t):
    if any(k in t for k in ["プロポーザル","企画提案","企画競争","公募型","事業者選定","事業者募集"]): return "proposal"
    if any(k in t for k in ["落札","入札結果","審査結果","契約締結","契約候補"]): return "award"
    if any(k in t for k in ["一般競争","制限付","条件付","総合評価"]): return "general_competitive"
    if "指名" in t: return "designated_competitive"
    if "随意" in t: return "negotiated"
    return "other"

def extract_items(text, muni_code, url):
    items = []
    seen = set()

    for line in text.split('\n'):
        ln = line.strip()
        if len(ln) < 10 or len(ln) > 250:
            continue

        has_kw = any(kw in ln for kw in PROC_KW)
        is_nav = any(kw in ln for kw in NAV_KW)

        if has_kw and not is_nav and ln not in seen:
            seen.add(ln)
            # Clean
            title = re.sub(r'^[\s\-・•►▶▷→\|]+', '', ln)
            title = re.sub(r'\s*\(PDF[^)]*\)\s*$', '', title)
            title = re.sub(r'\s*（PDF[^）]*）\s*$', '', title)
            title = re.sub(r'\s*\[.*?\]\s*$', '', title)
            title = title.strip()

            if len(title) > 8:
                # Extract date
                date_str = datetime.now().strftime("%Y-%m-%d")
                dm = re.search(r'(20\d{2})[年/\-](\d{1,2})[月/\-](\d{1,2})', ln)
                rm = re.search(r'令和(\d+)年(\d{1,2})月(\d{1,2})日', ln)
                if dm:
                    date_str = f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"
                elif rm:
                    date_str = f"{int(rm.group(1))+2018}-{int(rm.group(2)):02d}-{int(rm.group(3)):02d}"

                items.append({
                    'title': title[:200],
                    'date': date_str,
                    'type': classify_type(title),
                    'category': classify_cat(title),
                })

    return items

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Get ALL snapshots (including already-covered munis for enrichment)
    cur.execute("""
    SELECT s.muni_code, s.url, s.text_content
    FROM snapshots s
    WHERE s.text_content IS NOT NULL AND length(s.text_content) > 100
    """)
    all_rows = cur.fetchall()
    print(f"Processing {len(all_rows)} snapshots (v2 enhanced)...", flush=True)

    total_inserted = 0
    munis_enriched = 0

    for muni_code, url, text in all_rows:
        items = extract_items(text, muni_code, url)
        if items:
            new_for_muni = 0
            for item in items[:20]:
                cur.execute("""INSERT OR IGNORE INTO procurement_items
                    (muni_code, detected_at, title, item_type, deadline, amount, method, url, department, category)
                    VALUES (?, ?, ?, ?, NULL, NULL, '', ?, NULL, ?)""",
                    (muni_code, item['date'], item['title'], item['type'], url, item['category']))
                if cur.rowcount > 0:
                    total_inserted += 1
                    new_for_muni += 1
            if new_for_muni > 0:
                munis_enriched += 1

    conn.commit()
    cur.execute("SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code != 'NATIONAL'")
    print(f"Munis enriched: {munis_enriched}", flush=True)
    print(f"New items inserted: {total_inserted}", flush=True)
    print(f"Total muni codes: {cur.fetchone()[0]}", flush=True)
    cur.execute("SELECT COUNT(*) FROM procurement_items")
    print(f"Total items: {cur.fetchone()[0]}", flush=True)
    conn.close()

if __name__ == "__main__":
    main()
