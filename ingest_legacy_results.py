import sqlite3
import json
from datetime import datetime

DB = '/app/data/monitor.db'  # Docker container内パス


def classify_type(method):
    if not method:
        return 'other'
    if '一般競争' in method or '制限付' in method:
        return 'general_competitive'
    if 'プロポーザル' in method:
        return 'proposal'
    if '指名' in method:
        return 'designated_competitive'
    if '随意' in method:
        return 'negotiated'
    return 'other'


conn = sqlite3.connect(DB)
cur = conn.cursor()

with open('/tmp/legacy_scrape_results.json') as f:
    items = json.load(f)

inserted = 0
for item in items:
    title = item.get('title', '')
    if len(title) < 5:
        continue

    muni = item.get('muni_code', '')
    if not muni:
        continue

    cur.execute(
        'INSERT OR IGNORE INTO procurement_items (muni_code,detected_at,title,item_type,deadline,department,url,category) VALUES (?,?,?,?,?,?,?,?)',
        (
            muni,
            datetime.now().strftime('%Y-%m-%d'),
            title[:200],
            classify_type(item.get('method', '')),
            item.get('deadline'),
            item.get('department', '')[:100],
            item.get('url', ''),
            item.get('category', 'other'),
        ),
    )
    inserted += cur.rowcount

conn.commit()
print(f'Inserted: {inserted}/{len(items)}')
conn.close()
