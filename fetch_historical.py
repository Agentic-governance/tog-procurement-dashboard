#!/usr/bin/env python3
"""
Fetch 1 year of national procurement data from KKJ + P-Portal + MCP.
Does NOT delete existing items — uses deduplication.

Usage:
    python3 fetch_historical.py
"""

import sqlite3
import csv
import io
import json
import os
import ssl
import sys
import time
import zipfile
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA_DIR = Path(os.environ.get('DATA_DIR', Path(__file__).parent / "data"))
DB_PATH = DATA_DIR / "monitor.db"

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

NOW_STR = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

# ── KKJ API ──────────────────────────────────────────────────────────────

KKJ_API = "https://www.kkj.go.jp/api/"

PROCEDURE_TYPES = {"1": "一般競争入札", "2": "簡易公募型競争入札", "3": "簡易指名競争入札"}
CATEGORIES = {"1": "物品", "2": "工事", "3": "役務"}
KEYWORDS = ["入札", "業務", "工事", "調達", "委託", "購入", "賃貸借", "修繕"]

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


def parse_kkj_xml(xml_text):
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    records = []
    for sr in root.findall(".//SearchResult"):
        r = {}
        for field in ['ProjectName', 'OrganizationName', 'PrefectureName',
                       'CityName', 'CftIssueDate', 'PeriodEndTime',
                       'TenderSubmissionDeadline', 'Category', 'ProcedureType',
                       'ExternalDocumentURI', 'Location', 'LgCode']:
            elem = sr.find(field)
            if elem is not None and elem.text:
                r[field] = elem.text.strip()
        records.append(r)
    return records


def fetch_kkj_window(date_from, date_to):
    """Fetch all KKJ records for a date window."""
    seen_keys = set()
    all_records = []

    for kw in KEYWORDS:
        params = urllib.parse.urlencode({
            "Query": kw,
            "CFT_Issue_Date": f"{date_from}/{date_to}",
            "Count": "1000",
        })
        url = f"{KKJ_API}?{params}"

        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            resp = urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
            xml_text = resp.read().decode('utf-8')
            records = parse_kkj_xml(xml_text)

            new = 0
            for r in records:
                key = (r.get('ProjectName', ''), r.get('OrganizationName', ''))
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_records.append(r)
                    new += 1
        except Exception as e:
            print(f"    [{kw}] error: {str(e)[:60]}", flush=True)
        time.sleep(0.5)

    return all_records


def fetch_kkj_historical():
    """Fetch KKJ data in 2-week windows going back 12 months."""
    all_items = []
    today = datetime.now()

    # 26 windows of 2 weeks each = 1 year
    for i in range(26):
        window_end = today - timedelta(days=14 * i)
        window_start = today - timedelta(days=14 * (i + 1))
        d_from = window_start.strftime('%Y-%m-%d')
        d_to = window_end.strftime('%Y-%m-%d')

        print(f"  KKJ window {i+1}/26: {d_from} ~ {d_to}", end=" ", flush=True)
        records = fetch_kkj_window(d_from, d_to)
        print(f"→ {len(records)} records", flush=True)

        for r in records:
            title = r.get('ProjectName', '')
            if not title:
                continue
            org = r.get('OrganizationName', '')
            proc_type = r.get('ProcedureType', '')
            type_label = PROCEDURE_TYPES.get(proc_type, proc_type)
            cat = r.get('Category', '')
            cat_label = CATEGORIES.get(cat, cat)
            method = f"{type_label}" + (f" ({cat_label})" if cat_label else "")
            deadline = r.get('TenderSubmissionDeadline') or r.get('PeriodEndTime')

            # Use CftIssueDate as detected_at for historical accuracy
            cft_date = r.get('CftIssueDate', '')
            detected_at = cft_date if cft_date else d_from + 'T00:00:00'

            all_items.append({
                'muni_code': 'NATIONAL',
                'detected_at': detected_at,
                'title': title,
                'item_type': 'general_competitive' if proc_type == '1' else 'other',
                'deadline': deadline,
                'amount': None,
                'method': method,
                'url': r.get('ExternalDocumentURI'),
                'department': org,
                'category': classify(title),
                'raw': r,
            })

        if not records:
            # If window returns 0, KKJ may not have historical data this far back
            # Continue anyway in case gaps
            pass

    return all_items


# ── P-Portal ─────────────────────────────────────────────────────────────

PPORTAL_BASE = "https://api.p-portal.go.jp/pps-web-biz/UAB03/OAB0301"

def fetch_pportal_year(year):
    """Download full year award results from p-portal."""
    if year >= 2019:
        era = f"r{year - 2018:02d}"
    else:
        era = f"h{year - 1988:02d}"

    url = f"{PPORTAL_BASE}?fileversion=v001&filename=successful_bid_record_info_all_{era}.zip"
    print(f"  P-Portal {year} ({era}): {url}", flush=True)

    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        resp = urllib.request.urlopen(req, timeout=180, context=SSL_CTX)
        data = resp.read()
    except Exception as e:
        print(f"    Error: {e}", flush=True)
        return []

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            names = zf.namelist()
            csv_name = next((n for n in names if n.endswith('.csv')), names[0])
            csv_data = zf.read(csv_name).decode('utf-8', errors='replace')
    except Exception as e:
        print(f"    Unzip error: {e}", flush=True)
        return []

    CSV_FIELDS = ["case_number", "title", "award_date", "award_amount",
                  "contract_type", "agency_code", "winner_name", "corporate_number"]

    records = []
    for row in csv.reader(io.StringIO(csv_data)):
        if len(row) < 8:
            continue
        r = dict(zip(CSV_FIELDS, row))
        title = r.get('title', '')
        if not title or title == 'title':
            continue
        amount = r.get('award_amount')
        if amount:
            try:
                amount = f"{int(amount):,}円"
            except (ValueError, TypeError):
                pass

        award_date = r.get('award_date', '')
        # Normalize date format
        if len(award_date) == 8:
            award_date = f"{award_date[:4]}-{award_date[4:6]}-{award_date[6:]}"

        records.append({
            'muni_code': 'NATIONAL',
            'detected_at': award_date + 'T00:00:00' if award_date else NOW_STR,
            'title': title,
            'item_type': 'award',
            'deadline': award_date,
            'amount': amount,
            'method': f"落札: {r.get('winner_name', '不明')}",
            'url': None,
            'department': r.get('agency_code', ''),
            'category': classify(title),
            'raw': r,
        })

    print(f"    Parsed: {len(records)} awards", flush=True)
    return records


# ── MCP Bid Search ───────────────────────────────────────────────────────

GOVT_MCP_URL = os.environ.get('GOVT_MCP_URL', 'http://78.46.57.151:8003/mcp/')

def fetch_mcp_bids():
    """Fetch bids from government procurement MCP using broad keyword searches."""
    try:
        import httpx
    except ImportError:
        print("  MCP: httpx not available, skipping", flush=True)
        return []

    all_items = []
    search_keywords = ["入札", "業務委託", "システム", "工事", "調達", "購入", "設計"]

    for kw in search_keywords:
        print(f"  MCP bids [{kw}]: ", end="", flush=True)
        try:
            # Initialize session
            client = httpx.Client(timeout=30.0)
            headers = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
            init_body = {
                "jsonrpc": "2.0", "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "historical-fetch", "version": "1.0"}
                }
            }
            resp = client.post(GOVT_MCP_URL, json=init_body, headers=headers)
            sid = resp.headers.get("mcp-session-id", "")

            # Search bids
            if sid:
                headers["Mcp-Session-Id"] = sid
            body = {
                "jsonrpc": "2.0", "id": 2,
                "method": "tools/call",
                "params": {"name": "govt_search_bids", "arguments": {
                    "query": kw, "count": 500
                }}
            }
            resp = client.post(GOVT_MCP_URL, json=body, headers=headers)
            data = resp.json()
            content = data.get('result', {}).get('content', [])
            results = []
            for c in content:
                if c.get('type') == 'text':
                    try:
                        parsed = json.loads(c['text'])
                        results = parsed.get('results', parsed.get('data', {}).get('results', []))
                    except (json.JSONDecodeError, TypeError):
                        pass

            new = 0
            for b in results:
                if not isinstance(b, dict):
                    continue
                title = b.get('ProjectName', '')
                if not title:
                    continue
                cft_date = (b.get('CftIssueDate', '') or '')[:10]
                detected = cft_date + 'T00:00:00' if cft_date else NOW_STR
                all_items.append({
                    'muni_code': 'NATIONAL',
                    'detected_at': detected,
                    'title': title,
                    'item_type': 'general_competitive',
                    'deadline': b.get('TenderSubmissionDeadline') or b.get('PeriodEndTime'),
                    'amount': None,
                    'method': b.get('OrganizationName', ''),
                    'url': b.get('ExternalDocumentURI'),
                    'department': b.get('OrganizationName', ''),
                    'category': classify(title),
                    'raw': b,
                })
                new += 1
            print(f"{new} bids", flush=True)
            client.close()
        except Exception as e:
            print(f"error: {str(e)[:60]}", flush=True)
        time.sleep(0.5)

    return all_items


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    # Ensure table has new columns
    for col, coltype in [('department', 'TEXT'), ('division', 'TEXT'),
                         ('budget_range', 'TEXT'), ('category', 'TEXT')]:
        try:
            conn.execute(f"ALTER TABLE procurement_items ADD COLUMN {col} {coltype}")
        except Exception:
            pass
    conn.commit()

    # Create dedup index
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_dedup ON procurement_items(muni_code, title, detected_at)")
    except Exception:
        pass

    before = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    print(f"Items before: {before}\n", flush=True)

    all_items = []

    # 1. KKJ historical (1 year, 2-week windows)
    print("=== KKJ Historical (1 year) ===", flush=True)
    kkj_items = fetch_kkj_historical()
    print(f"  Total KKJ items: {len(kkj_items)}\n", flush=True)
    all_items.extend(kkj_items)

    # 2. P-Portal awards (2025 + 2026)
    print("=== P-Portal Awards ===", flush=True)
    for year in [2025, 2026]:
        pp_items = fetch_pportal_year(year)
        all_items.extend(pp_items)
    print(flush=True)

    # 3. MCP bid search (broad keywords)
    print("=== MCP Bid Search ===", flush=True)
    mcp_items = fetch_mcp_bids()
    print(f"  Total MCP items: {len(mcp_items)}\n", flush=True)
    all_items.extend(mcp_items)

    # Insert with deduplication
    print(f"=== Inserting {len(all_items)} items (with dedup) ===", flush=True)
    inserted = 0
    skipped = 0
    for item in all_items:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO procurement_items
                    (muni_code, detected_at, title, item_type,
                     deadline, amount, method, url, department, category, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (item['muni_code'], item['detected_at'], item['title'], item['item_type'],
                  item.get('deadline'), item.get('amount'),
                  item.get('method'), item.get('url'),
                  item.get('department'), item.get('category'),
                  json.dumps(item.get('raw', {}), ensure_ascii=False, default=str)))
            if conn.execute("SELECT changes()").fetchone()[0] > 0:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            skipped += 1

        if (inserted + skipped) % 1000 == 0:
            conn.commit()
            print(f"  Progress: {inserted} inserted, {skipped} skipped", flush=True)

    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    print(f"\n=== Results ===", flush=True)
    print(f"  Before: {before}", flush=True)
    print(f"  Inserted: {inserted}", flush=True)
    print(f"  Skipped (dup): {skipped}", flush=True)
    print(f"  After: {after}", flush=True)

    # Date range check
    r = conn.execute("SELECT MIN(detected_at), MAX(detected_at) FROM procurement_items").fetchone()
    print(f"  Date range: {r[0][:10]} to {r[1][:10]}", flush=True)

    # National vs municipal
    nat = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code = 'NATIONAL'").fetchone()[0]
    muni = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code != 'NATIONAL'").fetchone()[0]
    print(f"  National: {nat}, Municipal: {muni}", flush=True)

    conn.close()


if __name__ == '__main__':
    main()
