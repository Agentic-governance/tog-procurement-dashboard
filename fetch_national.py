#!/usr/bin/env python3
"""
Fetch national government procurement data from kkj.go.jp and p-portal.go.jp.
Inserts results into the same procurement_items table used by the dashboard.

Sources:
  1. kkj.go.jp XML API — 入札公告 (active bids, last 2 weeks)
  2. p-portal.go.jp open data — 落札結果 (award results, current year)
"""

import sqlite3
import csv
import io
import json
import os
import sys
import ssl
import zipfile
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA_DIR = Path(os.environ.get('DATA_DIR', Path(__file__).parent / "data"))
DB_PATH = DATA_DIR / "monitor.db"

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

TODAY = datetime.now()
TWO_WEEKS_AGO = TODAY - timedelta(days=14)
NOW_STR = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

# ── KKJ (入札公告) ──────────────────────────────────────────────────────────

KKJ_API = "https://www.kkj.go.jp/api/"

PROCEDURE_TYPES = {
    "1": "一般競争入札",
    "2": "簡易公募型競争入札",
    "3": "簡易指名競争入札",
}

CATEGORIES = {
    "1": "物品",
    "2": "工事",
    "3": "役務",
}


def fetch_kkj_notices(date_from, date_to, max_total=5000):
    """Fetch bid notices from kkj.go.jp XML API.

    Uses multiple keyword queries to maximize coverage since wildcard * doesn't work.
    API max is 1000 per request.
    """
    import urllib.parse

    # Broad keywords that cover most procurement notices
    keywords = ["入札", "業務", "工事", "調達", "委託", "購入", "賃貸借", "修繕"]
    seen_keys = set()
    all_records = []

    for kw in keywords:
        if len(all_records) >= max_total:
            break

        params = urllib.parse.urlencode({
            "Query": kw,
            "CFT_Issue_Date": f"{date_from}/{date_to}",
            "Count": "1000",
        })
        url = f"{KKJ_API}?{params}"
        print(f"  KKJ [{kw}]: ", end="", flush=True)

        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            resp = urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
            xml_text = resp.read().decode('utf-8')
            records = parse_kkj_xml(xml_text, silent=True)

            # Deduplicate by ProjectName + OrganizationName
            new = 0
            for r in records:
                key = (r.get('ProjectName', ''), r.get('OrganizationName', ''))
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_records.append(r)
                    new += 1

            print(f"{len(records)} found, {new} new (total: {len(all_records)})")
        except Exception as e:
            print(f"error: {str(e)[:80]}")

    return all_records


def parse_kkj_xml(xml_text, silent=False):
    """Parse KKJ XML response into records."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        if not silent:
            print(f"  XML parse error: {e}")
        return []

    hits_elem = root.find(".//SearchHits")
    total = int(hits_elem.text) if hits_elem is not None else 0
    if not silent:
        print(f"  KKJ total hits: {total}")

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

        desc_elem = sr.find("ProjectDescription")
        if desc_elem is not None and desc_elem.text:
            r['ProjectDescription'] = desc_elem.text.strip()[:500]

        records.append(r)

    return records


def kkj_to_items(records):
    """Convert KKJ records to procurement_items format."""
    items = []
    for r in records:
        title = r.get('ProjectName', '')
        if not title:
            continue

        org = r.get('OrganizationName', '')
        pref = r.get('PrefectureName', '')
        city = r.get('CityName', '')
        location = f"{pref}{city}" if pref else r.get('Location', '')

        # Deadline
        deadline = r.get('TenderSubmissionDeadline') or r.get('PeriodEndTime')

        # Type mapping
        proc_type = r.get('ProcedureType', '')
        type_label = PROCEDURE_TYPES.get(proc_type, proc_type)
        cat = r.get('Category', '')
        cat_label = CATEGORIES.get(cat, cat)

        method = f"{type_label}" + (f" ({cat_label})" if cat_label else "")

        item = {
            'muni_code': 'NATIONAL',
            'title': title,
            'item_type': 'general_competitive' if proc_type == '1' else 'other',
            'deadline': deadline,
            'amount': None,
            'method': method,
            'url': r.get('ExternalDocumentURI'),
            'department': org,
            'location': location,
            'source': 'kkj',
            'raw': r,
        }
        items.append(item)

    return items


# ── P-Portal (落札結果) ──────────────────────────────────────────────────

PPORTAL_BASE = "https://api.p-portal.go.jp/pps-web-biz/UAB03/OAB0301"

CSV_FIELDS = [
    "case_number", "title", "award_date", "award_amount",
    "contract_type", "agency_code", "winner_name", "corporate_number",
]


def fetch_pportal_awards(year=None):
    """Download award results from p-portal.go.jp open data."""
    year = year or TODAY.year

    if year >= 2019:
        era = f"r{year - 2018:02d}"
    else:
        era = f"h{year - 1988:02d}"

    url = f"{PPORTAL_BASE}?fileversion=v001&filename=successful_bid_record_info_all_{era}.zip"
    print(f"  P-Portal: {url}")

    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0')

    try:
        resp = urllib.request.urlopen(req, timeout=120, context=SSL_CTX)
        data = resp.read()
    except Exception as e:
        print(f"  P-Portal fetch error: {e}")
        return []

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            names = zf.namelist()
            csv_name = next((n for n in names if n.endswith('.csv')), names[0])
            csv_data = zf.read(csv_name).decode('utf-8')
    except Exception as e:
        print(f"  P-Portal unzip error: {e}")
        return []

    # Parse CSV
    records = []
    for row in csv.reader(io.StringIO(csv_data)):
        if len(row) < 8:
            continue
        records.append(dict(zip(CSV_FIELDS, row)))

    print(f"  P-Portal total records: {len(records)}")

    # Filter to last 2 weeks
    cutoff = TWO_WEEKS_AGO.strftime('%Y%m%d')
    recent = [r for r in records if (r.get('award_date', '') or '') >= cutoff]
    print(f"  P-Portal recent (>= {cutoff}): {len(recent)}")

    return recent


def pportal_to_items(records):
    """Convert p-portal records to procurement_items format."""
    items = []
    for r in records:
        title = r.get('title', '')
        if not title:
            continue

        amount = r.get('award_amount')
        if amount:
            try:
                amount = f"{int(amount):,}円"
            except (ValueError, TypeError):
                pass

        item = {
            'muni_code': 'NATIONAL',
            'title': title,
            'item_type': 'award',
            'deadline': r.get('award_date'),
            'amount': amount,
            'method': f"落札: {r.get('winner_name', '不明')}",
            'url': None,
            'department': r.get('agency_code'),
            'source': 'p-portal',
            'raw': r,
        }
        items.append(item)

    return items


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    conn = sqlite3.connect(str(DB_PATH))

    # Remove old national items
    deleted = conn.execute(
        "DELETE FROM procurement_items WHERE muni_code = 'NATIONAL'"
    ).rowcount
    conn.commit()
    print(f"Cleared {deleted} old NATIONAL items\n")

    all_items = []

    # 1. KKJ — 入札公告 (last 2 weeks)
    print("=== KKJ 入札公告 ===")
    date_from = TWO_WEEKS_AGO.strftime('%Y-%m-%d')
    date_to = TODAY.strftime('%Y-%m-%d')
    kkj_records = fetch_kkj_notices(date_from, date_to)
    kkj_items = kkj_to_items(kkj_records)
    print(f"  Extracted: {len(kkj_items)} bid notices\n")
    all_items.extend(kkj_items)

    # 2. P-Portal — 落札結果 (currently unavailable - API returns 400)
    # print("=== P-Portal 落札結果 ===")
    # pp_records = fetch_pportal_awards()
    # pp_items = pportal_to_items(pp_records)
    # print(f"  Extracted: {len(pp_items)} award results\n")
    # all_items.extend(pp_items)
    print("=== P-Portal ===\n  Skipped (API currently unavailable)\n")

    # Insert into DB
    print(f"Total national items: {len(all_items)}")
    for item in all_items:
        conn.execute("""
            INSERT INTO procurement_items
                (muni_code, detected_at, title, item_type,
                 deadline, amount, method, url, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ('NATIONAL', NOW_STR, item['title'], item['item_type'],
              item.get('deadline'), item.get('amount'),
              item.get('method'), item.get('url'),
              json.dumps(item.get('raw', {}), ensure_ascii=False)))

    conn.commit()

    # Verify
    total = conn.execute(
        "SELECT COUNT(*) FROM procurement_items WHERE muni_code = 'NATIONAL'"
    ).fetchone()[0]
    print(f"\nInserted into DB: {total} NATIONAL items")

    grand_total = conn.execute(
        "SELECT COUNT(*) FROM procurement_items"
    ).fetchone()[0]
    print(f"Grand total items: {grand_total}")

    conn.close()


if __name__ == '__main__':
    main()
