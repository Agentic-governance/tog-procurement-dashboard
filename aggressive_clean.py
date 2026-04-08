#!/usr/bin/env python3
"""Aggressive data cleaning based on critical audit findings.
Remove items that are clearly NOT procurement:
- Consumer info, health notices, event announcements
- Pages about switching electricity
- Generic municipal info pages
- Items with contract period text as title
"""
import sqlite3
import re

DB = "/app/data/monitor.db"

# Titles containing these are DEFINITELY NOT procurement
NON_PROCUREMENT = [
    # Consumer/social services
    "クーリング・オフ", "悪質商法", "消費者", "暴力", "DV",
    "避難所", "避難場所", "交通事故",
    # Health
    "感染症", "インフルエンザ", "コロナ", "ワクチン", "食中毒",
    "熱中症", "がん検診", "歯科健診",
    # Events/culture
    "体験会", "教室", "講座", "セミナー", "フェスティバル",
    "マラソン大会", "写真コンテスト", "文化祭",
    "お祭り", "花火", "イルミネーション",
    # General municipal
    "ゆるキャラ", "マスコットキャラ", "キッくん",
    "動画で学ぼう", "移住ガイド",
    "広報誌", "議会だより",
    # Electricity switching pages (not procurement)
    "電力プラン", "再エネプラン", "エポスプラン",
    "でんき", "電気料金", "電力会社",
    # Contract period text parsed as titles
    "契約締結日から",
    # Navigation
    "ページの先頭", "本文へ", "サイトマップ",
    "お問い合わせ一覧", "組織から探す",
]

# These MUST be kept even if they match above
ALWAYS_KEEP = [
    "入札", "公告", "プロポーザル", "落札", "契約締結結果",
    "発注", "見積", "仕様書", "予定価格",
    "工事", "委託業務", "業務委託",
]

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    before = cur.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    print(f"Before: {before}", flush=True)

    removed = 0

    for pattern in NON_PROCUREMENT:
        # Only remove if NOT matching procurement keywords
        keep_sql = " AND ".join([f"title NOT LIKE '%{k}%'" for k in ALWAYS_KEEP])
        cur.execute(f"""DELETE FROM procurement_items
            WHERE muni_code != 'NATIONAL'
            AND item_type != 'award'
            AND title LIKE '%{pattern}%'
            AND {keep_sql}""")
        if cur.rowcount > 0:
            removed += cur.rowcount
            print(f"  Removed '{pattern}': {cur.rowcount}", flush=True)

    # Remove items that are just file references (PDF/Excel links with no title)
    cur.execute("""DELETE FROM procurement_items
        WHERE muni_code != 'NATIONAL' AND item_type != 'award'
        AND (title LIKE '%(PDF%' OR title LIKE '%(Excel%' OR title LIKE '%ファイル%KB)')
        AND length(title) < 30""")
    if cur.rowcount > 0:
        removed += cur.rowcount
        print(f"  Removed file references: {cur.rowcount}", flush=True)

    # Remove items where title is just a date
    cur.execute("""DELETE FROM procurement_items
        WHERE muni_code != 'NATIONAL' AND item_type != 'award'
        AND title GLOB '[0-9][0-9][0-9][0-9]年*月*日*'
        AND length(title) < 20""")
    if cur.rowcount > 0:
        removed += cur.rowcount
        print(f"  Removed date-only titles: {cur.rowcount}", flush=True)

    conn.commit()
    after = cur.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    print(f"\nAfter: {after}", flush=True)
    print(f"Removed: {removed} ({removed*100//before}%)", flush=True)

    # Quality check: sample remaining items
    cur.execute("SELECT title FROM procurement_items WHERE muni_code != 'NATIONAL' AND item_type != 'award' ORDER BY RANDOM() LIMIT 10")
    print("\nRemaining sample:", flush=True)
    for (t,) in cur.fetchall():
        print(f"  {t[:80]}", flush=True)

    conn.close()

if __name__ == "__main__":
    main()
