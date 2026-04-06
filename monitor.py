#!/usr/bin/env python3
"""
Procurement Monitor — Daily change detection for municipal procurement pages.

1. Fetches all verified procurement URLs
2. Compares with previous snapshot (text diff)
3. For changed pages, calls Claude API to extract new procurement items
4. Outputs structured JSON feed

Usage:
    python3 monitor.py                # Run full cycle
    python3 monitor.py --fetch-only   # Only fetch snapshots, no analysis
    python3 monitor.py --analyze-only # Only analyze existing diffs
"""

import sqlite3
import csv
import hashlib
import json
import os
import sys
import ssl
import time
import urllib.request
import difflib
import re
import argparse
from datetime import datetime, timezone
from pathlib import Path
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Config ──────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get('DATA_DIR', BASE_DIR / "data"))
DB_PATH = DATA_DIR / "monitor.db"
MASTER_CSV = BASE_DIR / "procurement_url_master_v2.csv"
OUTPUT_DIR = BASE_DIR / "output"
MAX_WORKERS = 10          # Concurrent HTTP requests
REQUEST_TIMEOUT = 15      # Seconds
OPENAI_MODEL = "gpt-4o-mini"
MAX_DIFF_CHARS = 8000     # Max diff text to send to API
DAILY_BUDGET_USD = 2.0    # Safety cap

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE


# ── HTML to Text ────────────────────────────────────────────────────────────

class HTMLTextExtractor(HTMLParser):
    """Strip HTML tags, keep meaningful text."""

    SKIP_TAGS = {'script', 'style', 'noscript', 'svg', 'path', 'meta', 'link'}

    def __init__(self):
        super().__init__()
        self.parts = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.SKIP_TAGS:
            self._skip = True
        if tag.lower() in ('br', 'p', 'div', 'li', 'tr', 'h1', 'h2', 'h3', 'h4', 'dt', 'dd'):
            self.parts.append('\n')

    def handle_endtag(self, tag):
        if tag.lower() in self.SKIP_TAGS:
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            text = data.strip()
            if text:
                self.parts.append(text)

    def get_text(self):
        return '\n'.join(self.parts)


def html_to_text(html_body):
    """Convert HTML to plain text for diffing."""
    extractor = HTMLTextExtractor()
    try:
        extractor.feed(html_body)
    except Exception:
        # Fallback: regex strip
        text = re.sub(r'<script[^>]*>.*?</script>', '', html_body, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '\n', text)
        return text
    return extractor.get_text()


# ── Database ────────────────────────────────────────────────────────────────

def init_db(conn):
    """Create tables if not exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS snapshots (
            muni_code TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            url TEXT NOT NULL,
            status_code INTEGER,
            content_hash TEXT,
            text_content TEXT,
            error TEXT,
            PRIMARY KEY (muni_code, fetched_at)
        );

        CREATE TABLE IF NOT EXISTS diffs (
            muni_code TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            prev_fetched_at TEXT,
            curr_fetched_at TEXT,
            diff_text TEXT,
            diff_lines INTEGER,
            analyzed INTEGER DEFAULT 0,
            PRIMARY KEY (muni_code, detected_at)
        );

        CREATE TABLE IF NOT EXISTS procurement_items (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            muni_code TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            title TEXT,
            item_type TEXT,
            deadline TEXT,
            amount TEXT,
            method TEXT,
            url TEXT,
            raw_json TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        -- News articles from Google News RSS and other sources
        CREATE TABLE IF NOT EXISTS muni_news (
            news_id INTEGER PRIMARY KEY AUTOINCREMENT,
            muni_code TEXT NOT NULL,
            title TEXT NOT NULL,
            source TEXT,
            url TEXT,
            published_at TEXT,
            summary TEXT,
            category TEXT,
            fetched_at TEXT DEFAULT (datetime('now')),
            UNIQUE(muni_code, url)
        );

        -- Crawl schedule tracking
        CREATE TABLE IF NOT EXISTS crawl_schedule (
            schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            last_run TEXT,
            next_run TEXT,
            status TEXT DEFAULT 'idle',
            result_summary TEXT
        );

        -- Pre-computed analytics cache
        CREATE TABLE IF NOT EXISTS procurement_analytics (
            muni_code TEXT NOT NULL,
            metric TEXT NOT NULL,
            period TEXT,
            value REAL,
            detail_json TEXT,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (muni_code, metric, period)
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_muni
            ON snapshots(muni_code, fetched_at DESC);
        CREATE INDEX IF NOT EXISTS idx_items_detected
            ON procurement_items(detected_at);
        CREATE INDEX IF NOT EXISTS idx_items_muni
            ON procurement_items(muni_code);
        CREATE INDEX IF NOT EXISTS idx_news_muni
            ON muni_news(muni_code, published_at DESC);
        CREATE INDEX IF NOT EXISTS idx_news_category
            ON muni_news(category, published_at DESC);
        CREATE INDEX IF NOT EXISTS idx_analytics_muni
            ON procurement_analytics(muni_code, metric);
    """)

    # Add new columns to procurement_items (safe migration for existing DBs)
    for col, coltype in [('department', 'TEXT'), ('division', 'TEXT'),
                         ('budget_range', 'TEXT'), ('category', 'TEXT')]:
        try:
            conn.execute(f"ALTER TABLE procurement_items ADD COLUMN {col} {coltype}")
        except Exception:
            pass  # Column already exists
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_category ON procurement_items(category)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_department ON procurement_items(department)")

    # ── Phase 2 tables: Pipeline, Preferences, Alerts, Intelligence ──
    conn.executescript("""
        -- Item pipeline: track bid preparation status per item
        CREATE TABLE IF NOT EXISTS item_pipeline (
            item_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'discovered',
            -- discovered, reviewing, preparing, submitted, waiting, won, lost, passed
            priority TEXT DEFAULT 'normal',  -- low, normal, high, critical
            assignee TEXT,
            notes TEXT,
            go_nogo TEXT,  -- go, nogo, pending
            estimated_amount TEXT,
            tags TEXT,  -- JSON array
            updated_at TEXT DEFAULT (datetime('now')),
            created_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (item_id)
        );

        -- Pipeline status history (audit trail)
        CREATE TABLE IF NOT EXISTS pipeline_history (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL,
            old_status TEXT,
            new_status TEXT NOT NULL,
            changed_at TEXT DEFAULT (datetime('now')),
            note TEXT
        );

        -- User preferences (per-browser, cookie-based until auth)
        CREATE TABLE IF NOT EXISTS user_prefs (
            user_id TEXT NOT NULL,  -- cookie-based UUID
            pref_key TEXT NOT NULL,
            pref_value TEXT,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (user_id, pref_key)
        );

        -- Alert rules
        CREATE TABLE IF NOT EXISTS alert_rules (
            rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            name TEXT NOT NULL,
            conditions TEXT NOT NULL,  -- JSON: {"pref":["13","14"],"cat":["it"],"min_amount":"1000万"}
            channels TEXT DEFAULT '["browser"]',  -- JSON array
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );

        -- Alert matches (fired alerts)
        CREATE TABLE IF NOT EXISTS alert_matches (
            match_id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER NOT NULL,
            item_id INTEGER NOT NULL,
            matched_at TEXT DEFAULT (datetime('now')),
            seen INTEGER DEFAULT 0,
            UNIQUE(rule_id, item_id)
        );

        -- Item similarity cache
        CREATE TABLE IF NOT EXISTS item_similarity (
            item_id INTEGER NOT NULL,
            similar_item_id INTEGER NOT NULL,
            score REAL NOT NULL,
            reason TEXT,
            computed_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (item_id, similar_item_id)
        );

        -- Municipality procurement patterns (pre-computed)
        CREATE TABLE IF NOT EXISTS muni_patterns (
            muni_code TEXT NOT NULL,
            pattern_type TEXT NOT NULL,  -- 'seasonal', 'category_mix', 'vendor_concentration', 'cycle'
            detail_json TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (muni_code, pattern_type)
        );

        -- Bid preparation checklists
        CREATE TABLE IF NOT EXISTS bid_checklists (
            checklist_id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL,
            item_type TEXT,  -- general_competitive, proposal, etc.
            items_json TEXT NOT NULL,  -- JSON array of {task, done, due_date}
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_pipeline_status ON item_pipeline(status);
        CREATE INDEX IF NOT EXISTS idx_pipeline_priority ON item_pipeline(priority);
        CREATE INDEX IF NOT EXISTS idx_alert_rules_user ON alert_rules(user_id, active);
        CREATE INDEX IF NOT EXISTS idx_alert_matches_rule ON alert_matches(rule_id, seen);
        CREATE INDEX IF NOT EXISTS idx_similarity_item ON item_similarity(item_id, score DESC);
        CREATE INDEX IF NOT EXISTS idx_muni_patterns ON muni_patterns(muni_code);
    """)
    conn.commit()


def get_latest_snapshot(conn, muni_code):
    """Get the most recent snapshot for a municipality."""
    row = conn.execute("""
        SELECT fetched_at, content_hash, text_content
        FROM snapshots
        WHERE muni_code = ? AND content_hash IS NOT NULL
        ORDER BY fetched_at DESC LIMIT 1
    """, (muni_code,)).fetchone()
    return row


# ── Fetcher ─────────────────────────────────────────────────────────────────

def fetch_url(url):
    """Fetch a URL and return (status, html_body) or (None, error_msg)."""
    try:
        req = urllib.request.Request(url)
        req.add_header('User-Agent',
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        resp = urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=SSL_CTX)
        body = resp.read().decode('utf-8', errors='ignore')
        return resp.status, body
    except Exception as e:
        return None, str(e)[:200]


# ── Analyzer (OpenAI GPT API) ─────────────────────────────────────────────

def analyze_diff(muni_code, muni_name, diff_text, api_key):
    """Call OpenAI GPT API to extract procurement items from diff text."""
    import urllib.request
    import json

    # Truncate if too long
    if len(diff_text) > MAX_DIFF_CHARS:
        diff_text = diff_text[:MAX_DIFF_CHARS] + "\n... (truncated)"

    prompt = f"""以下は「{muni_name}」（自治体コード: {muni_code}）の調達情報ページの更新差分です。
この差分から、新規の調達・入札案件を抽出してください。

差分テキスト:
---
{diff_text}
---

以下のJSON配列形式で回答してください。案件がなければ空配列[]を返してください。
日付がない場合はnull、金額が不明ならnullを入れてください。

[
  {{
    "title": "案件名",
    "type": "general_competitive|designated_competitive|negotiated|proposal|other",
    "deadline": "YYYY-MM-DD or null",
    "amount": "金額（文字列） or null",
    "method": "入札方式の詳細",
    "url": "案件詳細のURL or null",
    "department": "発注部署（例: 総務課, 建設課） or null",
    "division": "担当係 or null",
    "budget_range": "予定金額帯（例: 1000万円以上, 100-500万円） or null",
    "category": "construction|service|goods|consulting|it|other"
  }}
]

JSONのみを返してください。説明は不要です。"""

    body = json.dumps({
        "model": OPENAI_MODEL,
        "max_tokens": 2000,
        "messages": [
            {"role": "system", "content": "You are a procurement data extraction assistant. Return only valid JSON arrays."},
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

    try:
        resp = urllib.request.urlopen(req, timeout=30)
        result = json.loads(resp.read().decode('utf-8'))
        text = result['choices'][0]['message']['content'].strip()

        # Parse JSON from response
        # Handle markdown code blocks
        if text.startswith('```'):
            text = re.sub(r'^```(?:json)?\s*', '', text)
            text = re.sub(r'\s*```$', '', text)

        items = json.loads(text)
        usage = result.get('usage', {})
        return items, {
            'input_tokens': usage.get('prompt_tokens', 0),
            'output_tokens': usage.get('completion_tokens', 0),
        }
    except Exception as e:
        return None, {'error': str(e)[:200]}


# ── Main ────────────────────────────────────────────────────────────────────

def load_master(csv_path):
    """Load procurement URL master CSV."""
    entries = []
    with open(csv_path, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row['confidence'] in ('high',) and row['procurement_url']:
                entries.append({
                    'muni_code': row['muni_code'],
                    'muni_name': row['muni_name'],
                    'url': row['procurement_url'],
                })
    return entries


def _fetch_worker(entry):
    """Worker: fetch URL and extract text (no DB access)."""
    mc = entry['muni_code']
    url = entry['url']
    name = entry['muni_name']
    status, body = fetch_url(url)

    if status is None:
        return mc, name, url, 'error', None, None, body  # body=error msg

    text = html_to_text(body)
    content_hash = hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]
    return mc, name, url, 'ok', status, content_hash, text


def run_fetch(conn, entries):
    """Fetch all URLs and detect changes."""
    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

    stats = {'new': 0, 'unchanged': 0, 'changed': 0, 'error': 0}
    changed = []

    print(f"Fetching {len(entries)} URLs with {MAX_WORKERS} workers...",
          flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_worker, e): e for e in entries}
        for i, future in enumerate(as_completed(futures)):
            mc, name, url, result, status, content_hash, text = future.result()

            if result == 'error':
                # Store error snapshot
                conn.execute("""
                    INSERT OR REPLACE INTO snapshots
                        (muni_code, fetched_at, url, status_code, error)
                    VALUES (?, ?, ?, NULL, ?)
                """, (mc, now_str, url, text))  # text=error msg
                stats['error'] += 1
            else:
                # Compare with previous BEFORE inserting new
                prev = get_latest_snapshot(conn, mc)

                # Store new snapshot
                conn.execute("""
                    INSERT OR REPLACE INTO snapshots
                        (muni_code, fetched_at, url, status_code,
                         content_hash, text_content)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (mc, now_str, url, status, content_hash, text))

                if prev is None:
                    stats['new'] += 1
                elif prev[1] == content_hash:
                    stats['unchanged'] += 1
                else:
                    # Changed — compute diff
                    prev_lines = (prev[2] or '').splitlines()
                    curr_lines = text.splitlines()
                    diff = list(difflib.unified_diff(
                        prev_lines, curr_lines,
                        fromfile='previous', tofile='current',
                        lineterm='', n=1))
                    added = [l[1:] for l in diff
                             if l.startswith('+') and not l.startswith('+++')]
                    diff_text = '\n'.join(added)
                    if diff_text.strip():
                        conn.execute("""
                            INSERT OR REPLACE INTO diffs
                                (muni_code, detected_at, prev_fetched_at,
                                 curr_fetched_at, diff_text, diff_lines)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (mc, now_str, prev[0], now_str,
                              diff_text, len(added)))
                        changed.append((mc, name, diff_text))
                        stats['changed'] += 1
                    else:
                        stats['unchanged'] += 1

            if (i + 1) % 50 == 0:
                conn.commit()
                print(f"  [{i+1}/{len(entries)}] "
                      f"new={stats['new']} unchanged={stats['unchanged']} "
                      f"changed={stats['changed']} error={stats['error']}",
                      flush=True)

    conn.commit()

    print(f"\n=== Fetch Summary ===")
    print(f"  New (first snapshot):  {stats['new']}")
    print(f"  Unchanged:             {stats['unchanged']}")
    print(f"  Changed:               {stats['changed']}")
    print(f"  Error:                 {stats['error']}")

    return changed


def run_analyze(conn, changed_list, entries_dict):
    """Analyze diffs with OpenAI GPT API."""
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set. Skipping analysis.")
        print("  Set it with: export OPENAI_API_KEY=sk-...")
        return []

    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    all_items = []
    total_tokens = 0
    cost_estimate = 0.0

    print(f"\nAnalyzing {len(changed_list)} changed pages...")

    for i, (mc, name, diff_text) in enumerate(changed_list):
        if not diff_text or len(diff_text.strip()) < 10:
            continue

        # Cost safety check
        if cost_estimate > DAILY_BUDGET_USD:
            print(f"  Budget cap reached (${cost_estimate:.2f}). Stopping analysis.")
            break

        items, usage = analyze_diff(mc, name, diff_text, api_key)

        if items is None:
            print(f"  [{i+1}] {mc} {name}: API error - {usage.get('error', '?')}")
            continue

        input_tokens = usage.get('input_tokens', 0)
        output_tokens = usage.get('output_tokens', 0)
        total_tokens += input_tokens + output_tokens
        # GPT-4o-mini pricing: $0.15/M input, $0.60/M output
        cost_estimate += input_tokens * 0.15 / 1_000_000 + output_tokens * 0.60 / 1_000_000

        if items:
            print(f"  [{i+1}] {mc} {name}: {len(items)} new items found")
            for item in items:
                conn.execute("""
                    INSERT INTO procurement_items
                        (muni_code, detected_at, title, item_type, deadline,
                         amount, method, url, raw_json,
                         department, division, budget_range, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (mc, now_str, item.get('title'), item.get('type'),
                      item.get('deadline'), item.get('amount'),
                      item.get('method'), item.get('url'),
                      json.dumps(item, ensure_ascii=False),
                      item.get('department'), item.get('division'),
                      item.get('budget_range'), item.get('category')))
                all_items.append({**item, 'muni_code': mc, 'muni_name': name})
        else:
            print(f"  [{i+1}] {mc} {name}: no procurement items in diff")

        # Mark diff as analyzed
        conn.execute("""
            UPDATE diffs SET analyzed = 1
            WHERE muni_code = ? AND detected_at = ?
        """, (mc, now_str))

    conn.commit()

    print(f"\n=== Analysis Summary ===")
    print(f"  Pages analyzed:  {len(changed_list)}")
    print(f"  Items extracted: {len(all_items)}")
    print(f"  Total tokens:    {total_tokens:,}")
    print(f"  Est. cost:       ${cost_estimate:.4f}")

    return all_items


def write_output(items, output_dir):
    """Write structured JSON output."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime('%Y-%m-%d')
    out_file = output_dir / f"procurement_feed_{today}.json"

    feed = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "date": today,
        "total_items": len(items),
        "items": items,
    }

    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)

    print(f"\nOutput written to: {out_file}")
    return out_file


def main():
    parser = argparse.ArgumentParser(description='Procurement Monitor')
    parser.add_argument('--fetch-only', action='store_true',
                        help='Only fetch snapshots, skip analysis')
    parser.add_argument('--analyze-only', action='store_true',
                        help='Only analyze existing unanalyzed diffs')
    parser.add_argument('--master', default=str(MASTER_CSV),
                        help='Path to procurement URL master CSV')
    args = parser.parse_args()

    # Init
    conn = sqlite3.connect(str(DB_PATH))
    init_db(conn)

    # Load master
    entries = load_master(args.master)
    entries_dict = {e['muni_code']: e for e in entries}
    print(f"Loaded {len(entries)} municipalities from master CSV")

    if args.analyze_only:
        # Get unanalyzed diffs
        rows = conn.execute("""
            SELECT muni_code, diff_text FROM diffs
            WHERE analyzed = 0 AND diff_text IS NOT NULL
            ORDER BY detected_at DESC
        """).fetchall()
        changed = [(mc, entries_dict.get(mc, {}).get('muni_name', mc), diff)
                    for mc, diff in rows]
        print(f"Found {len(changed)} unanalyzed diffs")
    elif args.fetch_only:
        changed = run_fetch(conn, entries)
        print(f"\nFetch complete. {len(changed)} pages changed.")
        print("Run with --analyze-only to analyze diffs.")
        conn.close()
        return
    else:
        changed = run_fetch(conn, entries)

    # Analyze
    if changed:
        items = run_analyze(conn, changed, entries_dict)
        if items:
            write_output(items, OUTPUT_DIR)
    else:
        print("\nNo changes detected.")

    conn.close()


if __name__ == '__main__':
    main()
