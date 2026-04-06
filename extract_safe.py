#!/usr/bin/env python3
"""
Safe extraction: INSERT OR IGNORE (never deletes existing items).
Skips municipalities that already have municipal items.
Extracts department, category, budget_range fields.

Usage:
    python3 extract_safe.py
"""

import sqlite3
import json
import os
import sys
import re
import time
import urllib.request
import ssl
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA_DIR = Path(os.environ.get('DATA_DIR', Path(__file__).parent / "data"))
DB_PATH = DATA_DIR / "monitor.db"
MASTER_CSV = Path(__file__).parent / "procurement_url_master_v2.csv"

OPENAI_MODEL = "gpt-4o-mini"
MAX_TEXT_CHARS = 6000
DAILY_BUDGET_USD = 5.0

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

TODAY = datetime.now()
TWO_WEEKS_AGO = TODAY - timedelta(days=14)
DATE_FROM = TWO_WEEKS_AGO.strftime('%Y年%m月%d日')

CATEGORY_RULES = [
    ('construction', ['工事', '修繕', '改修', '建設', '舗装', '設置工', '撤去工', '解体']),
    ('service', ['業務委託', '委託', '役務', '保守', '点検', '清掃', '管理業務', '運営', '警備', '派遣']),
    ('goods', ['購入', '調達', '納入', '物品', '備品', '機器', '車両', '薬品', 'リース', '賃貸借']),
    ('consulting', ['設計', '測量', '調査', 'コンサル', '計画策定', '検討']),
    ('it', ['システム', 'ソフトウェア', 'ネットワーク', 'サーバ', 'データ', 'ICT', 'DX', 'AI']),
]

def classify(title):
    if not title:
        return 'other'
    for key, keywords in CATEGORY_RULES:
        for kw in keywords:
            if kw in title:
                return key
    return 'other'


def load_master():
    import csv
    entries = {}
    with open(MASTER_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            entries[row['muni_code']] = row
    return entries


def has_recent_dates(text):
    now = datetime.now()
    year = now.year
    month = now.month
    prev_month = month - 1 if month > 1 else 12

    patterns = [
        f'{year}年{month}月', f'{year}年{prev_month}月',
        f'{year}/{month:02d}', f'{year}/{month}/',
        f'{year}-{month:02d}',
        f'令和{year-2018}年{month}月', f'令和{year-2018}年{prev_month}月',
        f'R{year-2018}.{month:02d}', f'R{year-2018}/{month:02d}',
    ]
    for d in range(max(1, now.day - 14), now.day + 1):
        patterns.append(f'{month}月{d}日')
        patterns.append(f'{month}/{d}')

    text_check = text[:20000]
    return any(p in text_check for p in patterns)


def call_gpt(muni_code, muni_name, text, api_key):
    if len(text) > MAX_TEXT_CHARS:
        text = text[:MAX_TEXT_CHARS] + "\n... (以下省略)"

    prompt = f"""以下は「{muni_name}」（自治体コード: {muni_code}）の調達・入札情報ページのテキストです。
このページから、{DATE_FROM}以降の調達・入札案件を全て抽出してください。

ページテキスト:
---
{text}
---

以下のJSON配列形式で回答してください。案件がなければ空配列[]を返してください。

[
  {{
    "title": "案件名",
    "type": "general_competitive|designated_competitive|negotiated|proposal|other",
    "deadline": "YYYY-MM-DD or null",
    "published": "YYYY-MM-DD or null",
    "amount": "金額（文字列） or null",
    "method": "入札方式の詳細 or null",
    "department": "担当部署名 or null",
    "budget_range": "予定価格帯(例: 100万未満, 100-500万, 500-1000万, 1000万以上) or null"
  }}
]

重要:
- {DATE_FROM}より前の案件は含めないこと
- 日付は全てYYYY-MM-DD形式に変換すること（令和→西暦）
- department は発注課・担当課を抽出すること（例: 総務課, 建設課, 教育委員会）
- JSONのみを返してください。説明は不要です。"""

    body = json.dumps({
        "model": OPENAI_MODEL,
        "max_tokens": 3000,
        "messages": [
            {"role": "system", "content": "You are a procurement data extraction assistant. Return only valid JSON arrays. Extract procurement/bidding items from Japanese municipal government pages."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
    }).encode('utf-8')

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )

    resp = urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
    result = json.loads(resp.read().decode('utf-8'))
    text_resp = result['choices'][0]['message']['content'].strip()

    if text_resp.startswith('```'):
        text_resp = re.sub(r'^```(?:json)?\s*', '', text_resp)
        text_resp = re.sub(r'\s*```$', '', text_resp)

    items = json.loads(text_resp)
    usage = result.get('usage', {})
    return items, usage


def main():
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set")
        sys.exit(1)

    master = load_master()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    # Ensure dedup index exists
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_dedup ON procurement_items(muni_code, title, detected_at)")
    except Exception:
        pass

    # Find municipalities that already have items (skip them)
    existing = set()
    for row in conn.execute("SELECT DISTINCT muni_code FROM procurement_items WHERE muni_code != 'NATIONAL'"):
        existing.add(row[0])
    print(f"Municipalities with existing items: {len(existing)} (will skip)", flush=True)

    # Get latest snapshot per municipality
    rows = conn.execute("""
        SELECT s.muni_code, s.text_content, s.url
        FROM snapshots s
        WHERE s.content_hash IS NOT NULL
        AND s.fetched_at = (
            SELECT MAX(fetched_at) FROM snapshots
            WHERE muni_code = s.muni_code AND content_hash IS NOT NULL
        )
    """).fetchall()

    print(f"Total snapshots with content: {len(rows)}", flush=True)

    # Filter: skip existing, skip short text, check for recent dates
    candidates = []
    for row in rows:
        mc = row['muni_code']
        if mc in existing:
            continue
        text = row['text_content'] or ''
        if len(text) < 50:
            continue
        if has_recent_dates(text):
            candidates.append(row)

    print(f"New candidates (with recent dates, not yet extracted): {len(candidates)}", flush=True)

    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    total_items = 0
    total_input_tokens = 0
    total_output_tokens = 0
    cost = 0.0
    errors = 0
    processed = 0

    print(f"\nExtracting from {len(candidates)} pages...", flush=True)
    print(f"Date range: {DATE_FROM} ~ {TODAY.strftime('%Y年%m月%d日')}", flush=True)
    print(f"Model: {OPENAI_MODEL}, Budget: ${DAILY_BUDGET_USD:.2f}\n", flush=True)

    for i, row in enumerate(candidates):
        mc = row['muni_code']
        info = master.get(mc, {})
        name = info.get('muni_name', mc)
        text = row['text_content']

        if cost > DAILY_BUDGET_USD:
            print(f"\nBudget cap reached (${cost:.2f}). Stopping.", flush=True)
            break

        try:
            items, usage = call_gpt(mc, name, text, api_key)
            in_tok = usage.get('prompt_tokens', 0)
            out_tok = usage.get('completion_tokens', 0)
            total_input_tokens += in_tok
            total_output_tokens += out_tok
            cost += in_tok * 0.15 / 1_000_000 + out_tok * 0.60 / 1_000_000

            new_count = 0
            if items:
                for item in items:
                    title = item.get('title', '')
                    if not title:
                        continue
                    cat = classify(title)
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO procurement_items
                                (muni_code, detected_at, title, item_type,
                                 deadline, amount, method, url,
                                 department, category, budget_range, raw_json)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (mc, now_str, title,
                              item.get('type'),
                              item.get('deadline') or item.get('published'),
                              item.get('amount'),
                              item.get('method'),
                              row['url'],
                              item.get('department'),
                              cat,
                              item.get('budget_range'),
                              json.dumps(item, ensure_ascii=False)))
                        if conn.execute("SELECT changes()").fetchone()[0] > 0:
                            new_count += 1
                    except Exception:
                        pass
                total_items += new_count

            processed += 1
            if (i + 1) % 10 == 0 or new_count > 0:
                print(f"  [{i+1}/{len(candidates)}] {mc} {name}: {new_count} new"
                      f"  (total={total_items}, ${cost:.3f})", flush=True)

        except Exception as e:
            errors += 1
            if (i + 1) % 10 == 0:
                print(f"  [{i+1}/{len(candidates)}] {mc} {name}: ERROR {str(e)[:80]}", flush=True)

        if (i + 1) % 50 == 0:
            conn.commit()

        time.sleep(0.1)

    conn.commit()
    conn.close()

    print(f"\n{'='*50}", flush=True)
    print(f"=== Safe Extraction Complete ===", flush=True)
    print(f"  Pages processed:  {processed}", flush=True)
    print(f"  Items extracted:  {total_items}", flush=True)
    print(f"  Errors:           {errors}", flush=True)
    print(f"  Input tokens:     {total_input_tokens:,}", flush=True)
    print(f"  Output tokens:    {total_output_tokens:,}", flush=True)
    print(f"  Estimated cost:   ${cost:.4f}", flush=True)
    print(f"{'='*50}", flush=True)


if __name__ == '__main__':
    main()
