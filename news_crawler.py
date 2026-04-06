#!/usr/bin/env python3
"""
News Crawler — Fetch municipal news from Google News RSS.

Usage:
    python3 news_crawler.py                    # Crawl next batch of ~100
    python3 news_crawler.py --batch 50         # Custom batch size
    python3 news_crawler.py --muni 011002      # Single municipality
    python3 news_crawler.py --all              # All municipalities (slow)
"""

import csv
import json
import os
import re
import sqlite3
import sys
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

try:
    import feedparser
except ImportError:
    print("ERROR: feedparser not installed. Run: pip install feedparser")
    sys.exit(1)

try:
    import httpx
except ImportError:
    print("ERROR: httpx not installed. Run: pip install httpx")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get('DATA_DIR', BASE_DIR / "data"))
DB_PATH = DATA_DIR / "monitor.db"
MASTER_CSV = BASE_DIR / "procurement_url_master_v2.csv"

GNEWS_RSS = "https://news.google.com/rss/search"
RATE_LIMIT = 1.0  # seconds between requests
DEFAULT_BATCH = 100

# News category classification rules
NEWS_CATEGORIES = [
    ('procurement', ['入札', '調達', '公告', '契約', '落札', '発注', '仕様書']),
    ('budget', ['予算', '補正', '決算', '歳出', '歳入', '財政']),
    ('policy', ['計画', '方針', '条例', '規則', 'DX', '脱炭素', 'SDGs']),
    ('personnel', ['人事', '採用', '職員', '任命', '退職', '異動']),
    ('disaster', ['災害', '防災', '避難', '地震', '台風', '豪雨', '復旧']),
    ('construction', ['工事', '建設', '道路', '橋梁', '公園', '施設整備']),
]


def classify_news(title: str) -> str:
    """Classify news article by title keywords."""
    if not title:
        return 'other'
    for category, keywords in NEWS_CATEGORIES:
        for kw in keywords:
            if kw in title:
                return category
    return 'other'


# ── Database ────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # Ensure table exists
    conn.execute("""
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
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_news_muni ON muni_news(muni_code, published_at DESC)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crawl_schedule (
            schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            last_run TEXT,
            next_run TEXT,
            status TEXT DEFAULT 'idle',
            result_summary TEXT
        )
    """)
    conn.commit()
    return conn


def load_master():
    """Load municipality names from master CSV."""
    entries = {}
    if not MASTER_CSV.exists():
        return entries
    with open(MASTER_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            entries[row['muni_code']] = row['muni_name']
    return entries


def get_batch_offset(conn) -> int:
    """Get the current batch offset for rotation."""
    row = conn.execute(
        "SELECT result_summary FROM crawl_schedule WHERE job_type = 'news_fetch' LIMIT 1"
    ).fetchone()
    if row and row['result_summary']:
        try:
            data = json.loads(row['result_summary'])
            return data.get('next_offset', 0)
        except (json.JSONDecodeError, TypeError):
            pass
    return 0


def save_batch_offset(conn, offset: int, stats: dict):
    """Save the batch offset for next rotation."""
    summary = json.dumps({**stats, 'next_offset': offset}, ensure_ascii=False)
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    conn.execute("""
        INSERT OR REPLACE INTO crawl_schedule (schedule_id, job_type, last_run, status, result_summary)
        VALUES (
            (SELECT schedule_id FROM crawl_schedule WHERE job_type = 'news_fetch'),
            'news_fetch', ?, 'idle', ?
        )
    """, (now, summary))
    if conn.execute("SELECT changes()").fetchone()[0] == 0:
        conn.execute(
            "INSERT INTO crawl_schedule (job_type, last_run, status, result_summary) VALUES (?, ?, 'idle', ?)",
            ('news_fetch', now, summary)
        )
    conn.commit()


# ── RSS Fetching ────────────────────────────────────────────────────────────

def fetch_news_rss(muni_name: str) -> list:
    """Fetch Google News RSS for a municipality. Returns list of article dicts."""
    query = f"{muni_name} (入札 OR 調達 OR 予算 OR 行政)"
    url = f"{GNEWS_RSS}?q={quote(query)}&hl=ja&gl=JP&ceid=JP:ja"

    try:
        resp = httpx.get(url, timeout=15.0, headers={
            'User-Agent': 'procurement-dashboard/2.0 (news-crawler)'
        })
        if resp.status_code != 200:
            return []

        feed = feedparser.parse(resp.text)
        articles = []
        for entry in feed.entries[:20]:  # Max 20 per municipality
            pub = ''
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                try:
                    pub = time.strftime('%Y-%m-%dT%H:%M:%S', entry.published_parsed)
                except Exception:
                    pub = entry.get('published', '')

            articles.append({
                'title': entry.get('title', ''),
                'url': entry.get('link', ''),
                'published_at': pub,
                'source': entry.get('source', {}).get('title', 'Google News'),
                'summary': _clean_summary(entry.get('summary', '')),
            })
        return articles
    except Exception as e:
        print(f"  ERROR fetching news for {muni_name}: {e}")
        return []


def _clean_summary(html: str) -> str:
    """Strip HTML tags from RSS summary."""
    text = re.sub(r'<[^>]+>', '', html)
    text = text.strip()
    return text[:500] if text else ''


# ── Main ────────────────────────────────────────────────────────────────────

def crawl_batch(batch_size: int = DEFAULT_BATCH, single_muni: str = None,
                crawl_all: bool = False):
    """Crawl news for a batch of municipalities."""
    conn = get_db()
    master = load_master()

    if not master:
        print("ERROR: No municipalities loaded from master CSV")
        return

    muni_list = sorted(master.items())  # Sort for consistent rotation

    if single_muni:
        muni_list = [(mc, name) for mc, name in muni_list if mc == single_muni]
        if not muni_list:
            print(f"ERROR: Municipality {single_muni} not found")
            return
    elif not crawl_all:
        offset = get_batch_offset(conn)
        if offset >= len(muni_list):
            offset = 0
        muni_list = muni_list[offset:offset + batch_size]
        next_offset = offset + batch_size
        if next_offset >= len(master):
            next_offset = 0

    stats = {'fetched': 0, 'new_articles': 0, 'skipped': 0, 'errors': 0}
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

    print(f"=== News Crawler ({datetime.now().isoformat()}) ===")
    print(f"Batch: {len(muni_list)} municipalities")
    print()

    for i, (muni_code, muni_name) in enumerate(muni_list):
        print(f"[{i+1}/{len(muni_list)}] {muni_name} ({muni_code})...", end=' ', flush=True)

        articles = fetch_news_rss(muni_name)
        stats['fetched'] += 1

        new_count = 0
        for article in articles:
            if not article['url']:
                continue
            category = classify_news(article['title'])
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO muni_news
                        (muni_code, title, source, url, published_at, summary, category, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (muni_code, article['title'], article['source'],
                      article['url'], article['published_at'],
                      article['summary'], category, now))
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    new_count += 1
            except Exception:
                stats['errors'] += 1

        stats['new_articles'] += new_count
        print(f"{len(articles)} articles, {new_count} new")

        if (i + 1) % 20 == 0:
            conn.commit()

        # Rate limit
        if i < len(muni_list) - 1:
            time.sleep(RATE_LIMIT)

    conn.commit()

    if not single_muni and not crawl_all:
        save_batch_offset(conn, next_offset, stats)

    print(f"\n=== Summary ===")
    print(f"  Fetched: {stats['fetched']} municipalities")
    print(f"  New articles: {stats['new_articles']}")
    print(f"  Errors: {stats['errors']}")

    conn.close()
    return stats


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='News Crawler')
    parser.add_argument('--batch', type=int, default=DEFAULT_BATCH,
                        help=f'Batch size (default: {DEFAULT_BATCH})')
    parser.add_argument('--muni', type=str, help='Single municipality code')
    parser.add_argument('--all', action='store_true', help='Crawl all municipalities')
    args = parser.parse_args()

    crawl_batch(batch_size=args.batch, single_muni=args.muni, crawl_all=args.all)
