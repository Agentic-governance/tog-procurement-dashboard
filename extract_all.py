#!/usr/bin/env python3
"""
Extract ALL procurement items from current snapshots (not just diffs).
Sends each page's text to GPT-4o-mini to extract items from the last 2 weeks.
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
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_DIR = Path(os.environ.get('DATA_DIR', Path(__file__).parent / "data"))
DB_PATH = DATA_DIR / "monitor.db"
MASTER_CSV = Path(__file__).parent / "procurement_url_master_v2.csv"

OPENAI_MODEL = "gpt-4o-mini"
MAX_WORKERS = 5           # Concurrent API calls
MAX_TEXT_CHARS = 6000     # Max text to send per page
DAILY_BUDGET_USD = 5.0

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

# Date range
TODAY = datetime.now()
TWO_WEEKS_AGO = TODAY - timedelta(days=14)
DATE_FROM = TWO_WEEKS_AGO.strftime('%Y年%m月%d日')
DATE_CUTOFF = TWO_WEEKS_AGO.strftime('%Y-%m-%d')


def load_master():
    import csv
    entries = {}
    with open(MASTER_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            entries[row['muni_code']] = row
    return entries


def has_recent_dates(text):
    """Quick check: does text contain dates within the last ~30 days?"""
    now = datetime.now()
    # Match patterns like 2026年3月, 令和8年3月, 2026/3, 2026-03
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
    # Also check for recent day patterns
    for d in range(max(1, now.day - 14), now.day + 1):
        patterns.append(f'{month}月{d}日')
        patterns.append(f'{month}/{d}')

    text_lower = text[:20000]  # Only check first 20K chars
    return any(p in text_lower for p in patterns)


def call_gpt(muni_code, muni_name, text, api_key):
    """Call GPT-4o-mini to extract procurement items."""
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
    "department": "担当部署 or null"
  }}
]

重要:
- {DATE_FROM}より前の案件は含めないこと
- 日付は全てYYYY-MM-DD形式に変換すること（令和→西暦）
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

    # Handle markdown code blocks
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

    # Get latest snapshot per municipality (only those with content)
    rows = conn.execute("""
        SELECT s.muni_code, s.text_content, s.url
        FROM snapshots s
        WHERE s.content_hash IS NOT NULL
        AND s.fetched_at = (
            SELECT MAX(fetched_at) FROM snapshots
            WHERE muni_code = s.muni_code AND content_hash IS NOT NULL
        )
    """).fetchall()

    print(f"Total snapshots with content: {len(rows)}")

    # Pre-filter: pages likely to have recent dates
    candidates = []
    for row in rows:
        text = row['text_content'] or ''
        if len(text) < 50:
            continue
        if has_recent_dates(text):
            candidates.append(row)

    print(f"Pages with recent dates (pre-filter): {len(candidates)}")
    print(f"Skipped (no recent dates): {len(rows) - len(candidates)}")

    # Clear old extraction results
    conn.execute("DELETE FROM procurement_items")
    conn.commit()

    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    total_items = 0
    total_input_tokens = 0
    total_output_tokens = 0
    cost = 0.0
    errors = 0

    print(f"\nExtracting items from {len(candidates)} pages...")
    print(f"Date range: {DATE_FROM} ~ {TODAY.strftime('%Y年%m月%d日')}")
    print(f"Model: {OPENAI_MODEL}")
    print(f"Budget cap: ${DAILY_BUDGET_USD:.2f}\n")

    for i, row in enumerate(candidates):
        mc = row['muni_code']
        info = master.get(mc, {})
        name = info.get('muni_name', mc)
        text = row['text_content']

        if cost > DAILY_BUDGET_USD:
            print(f"\nBudget cap reached (${cost:.2f}). Stopping.")
            break

        try:
            items, usage = call_gpt(mc, name, text, api_key)
            in_tok = usage.get('prompt_tokens', 0)
            out_tok = usage.get('completion_tokens', 0)
            total_input_tokens += in_tok
            total_output_tokens += out_tok
            cost += in_tok * 0.15 / 1_000_000 + out_tok * 0.60 / 1_000_000

            if items:
                for item in items:
                    conn.execute("""
                        INSERT INTO procurement_items
                            (muni_code, detected_at, title, item_type,
                             deadline, amount, method, url, raw_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (mc, now_str, item.get('title'),
                          item.get('type'),
                          item.get('deadline') or item.get('published'),
                          item.get('amount'),
                          item.get('method'),
                          row['url'],
                          json.dumps(item, ensure_ascii=False)))
                total_items += len(items)

            status = f"{len(items)} items" if items else "0"
            if (i + 1) % 20 == 0 or items:
                print(f"  [{i+1}/{len(candidates)}] {mc} {name}: {status}"
                      f"  (total={total_items}, ${cost:.3f})", flush=True)

        except Exception as e:
            errors += 1
            if (i + 1) % 20 == 0:
                print(f"  [{i+1}/{len(candidates)}] {mc} {name}: ERROR {str(e)[:80]}",
                      flush=True)

        if (i + 1) % 50 == 0:
            conn.commit()

        # Small delay to avoid rate limits
        time.sleep(0.1)

    conn.commit()
    conn.close()

    print(f"\n{'='*50}")
    print(f"=== Extraction Complete ===")
    print(f"  Pages processed:  {min(i+1, len(candidates))}")
    print(f"  Items extracted:  {total_items}")
    print(f"  Errors:           {errors}")
    print(f"  Input tokens:     {total_input_tokens:,}")
    print(f"  Output tokens:    {total_output_tokens:,}")
    print(f"  Estimated cost:   ${cost:.4f}")
    print(f"{'='*50}")


if __name__ == '__main__':
    main()
