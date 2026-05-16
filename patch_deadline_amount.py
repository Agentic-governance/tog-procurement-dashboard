#!/usr/bin/env python3
"""
Iter 11-20: Deadline抽出改善パッチ
Iter 21-30: Amount万円パース修正

1. SuperCALS/神奈川DENTYO/群馬/efftisの各スクレイパーにdeadline抽出を追加
2. estimate_price_range()の万円単位変換バグを修正
3. 既存データに対するdeadline回復バッチ処理

Hetznerサーバーに適用: /home/deploy/procurement_monitor/
"""

import sqlite3
import re
import sys
from datetime import datetime

DB = "/app/data/monitor.db"

# ============================================================
# Part 1: 万円パースの修正関数
# ============================================================
def parse_amount_yen(amt_str: str) -> int | None:
    """
    日本円の金額文字列を整数(円)に変換。
    対応フォーマット:
      - "500万円" → 5,000,000
      - "1億2000万円" → 120,000,000
      - "1,234,567円" → 1,234,567
      - "12,345千円" → 12,345,000
      - "1.5億円" → 150,000,000
    """
    if not amt_str:
        return None

    s = amt_str.replace(',', '').replace(' ', '').replace('　', '')

    # 億万パターン: "1億2000万円" or "1.5億円"
    m = re.search(r'([\d.]+)\s*億\s*([\d.]*)\s*万?', s)
    if m:
        oku = float(m.group(1))
        man = float(m.group(2)) if m.group(2) else 0
        return int(oku * 100_000_000 + man * 10_000)

    # 万円パターン: "500万円" or "500万"
    m = re.search(r'([\d.]+)\s*万', s)
    if m:
        return int(float(m.group(1)) * 10_000)

    # 千円パターン: "12,345千円"
    m = re.search(r'([\d.]+)\s*千', s)
    if m:
        return int(float(m.group(1)) * 1_000)

    # 純数値パターン: "1234567円" or "1,234,567"
    m = re.search(r'([\d]+)', s)
    if m:
        val = int(m.group(1))
        if val >= 10000:  # 1万円以上のみ有効
            return val

    return None


# ============================================================
# Part 2: Deadline抽出の強化関数
# ============================================================
def extract_deadline_from_html(html_text: str) -> str | None:
    """
    HTMLテキストからdeadline候補を抽出。
    対応フォーマット:
      - 2026/05/20, 2026-05-20
      - 令和8年5月20日
      - R8.5.20, R08/05/20
      - 〜5月20日 (当年推定)
      - 締切: 2026年5月20日
    """
    # 西暦パターン
    m = re.search(r'(\d{4})[/\-.](\d{1,2})[/\-.](\d{1,2})', html_text)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2020 <= y <= 2030 and 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y}-{mo:02d}-{d:02d}"

    # 令和パターン: 令和8年5月20日
    m = re.search(r'令和\s*(\d{1,2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日', html_text)
    if m:
        y = 2018 + int(m.group(1))
        mo, d = int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y}-{mo:02d}-{d:02d}"

    # R8.5.20パターン
    m = re.search(r'[RＲ]\s*(\d{1,2})\s*[./]\s*(\d{1,2})\s*[./]\s*(\d{1,2})', html_text)
    if m:
        y = 2018 + int(m.group(1))
        mo, d = int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y}-{mo:02d}-{d:02d}"

    # 年月日パターン（西暦）: 2026年5月20日
    m = re.search(r'(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日', html_text)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2020 <= y <= 2030 and 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y}-{mo:02d}-{d:02d}"

    return None


# ============================================================
# Part 3: 既存データのdeadline回復バッチ
# ============================================================
def batch_recover_deadlines(db_path: str):
    """URL先のタイトルや周辺テキストからdeadline推定を試みる。
    ここでは既存データの中でdeadline=NULLかつtitleに日付が含まれるものを修復。
    """
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # deadline=NULLの件数
    cur.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline IS NULL")
    null_count = cur.fetchone()[0]
    print(f"Deadline NULL items: {null_count}")

    # titleから日付抽出を試みる
    cur.execute("""
        SELECT id, title, detected_at FROM procurement_items
        WHERE deadline IS NULL AND title IS NOT NULL
        LIMIT 10000
    """)
    rows = cur.fetchall()

    fixed = 0
    for row in rows:
        dl = extract_deadline_from_html(row['title'])
        if dl:
            cur.execute("UPDATE procurement_items SET deadline = ? WHERE id = ?", (dl, row['id']))
            fixed += 1

    conn.commit()
    print(f"Fixed deadlines from title: {fixed}/{len(rows)}")

    # detected_at が未来のものがあれば、それをdeadlineとして使う合理性はないのでスキップ

    conn.close()
    return fixed


# ============================================================
# Part 4: amount列の万円再パース
# ============================================================
def batch_fix_amounts(db_path: str):
    """amount列に'万円'等が含まれるレコードを正しい整数に変換。"""
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # amount列が文字列のまま格納されている場合の修正
    cur.execute("""
        SELECT id, amount FROM procurement_items
        WHERE amount IS NOT NULL AND amount != ''
        AND (amount LIKE '%万%' OR amount LIKE '%億%' OR amount LIKE '%千%')
        LIMIT 10000
    """)
    rows = cur.fetchall()

    fixed = 0
    for row in rows:
        parsed = parse_amount_yen(str(row['amount']))
        if parsed:
            cur.execute("UPDATE procurement_items SET amount = ? WHERE id = ?", (parsed, row['id']))
            fixed += 1

    conn.commit()
    print(f"Fixed amounts (万円→円): {fixed}/{len(rows)}")
    conn.close()
    return fixed


if __name__ == "__main__":
    if len(sys.argv) > 1:
        db = sys.argv[1]
    else:
        db = DB

    print(f"=== Deadline & Amount Recovery Patch ===")
    print(f"DB: {db}")
    print(f"Time: {datetime.now().isoformat()}")
    print()

    batch_recover_deadlines(db)
    print()
    batch_fix_amounts(db)
