#!/usr/bin/env python3
"""
audit/coverage_audit.py — Coverage Audit & Important Item Scoring
Extracted from Night 14 n14_zero_miss.py

Handles:
- important_v3_score calculation (6-axis)
- Coverage audit: score distribution, capture gaps, source analysis
- Monthly volume trends
"""

import sqlite3
import json
import re
from datetime import datetime

__all__ = [
    'important_item_score_v3',
    'batch_score_v3',
    'run_coverage_audit',
    'get_capture_rate',
    'get_monthly_trend',
]

# Keyword sets for scoring
PROPOSAL_KEYWORDS = ['プロポーザル', '公募型', '企画提案', '企画競争']
HIGH_VALUE_KEYWORDS = ['計画策定', 'システム', 'DX', 'AI', '運営', '管理', '委託',
                       '設計', '調査', '分析', 'コンサル']
NEGATIVE_KEYWORDS = ['入札', '見積合わせ', '価格', '単価契約']


def important_item_score_v3(title, category, item_type, method, deadline,
                             amount, attachment_count, muni_code):
    """
    6-axis scoring for procurement item importance.

    Axes:
    1. Type match (0-40): proposal/consulting/IT
    2. Budget signal (0-15): amount present and significant
    3. Attachment (0-10): has attachments
    4. Keyword (0-20): proposal/high-value keywords
    5. Deadline proximity (0-15): within 30/60 days
    6. Negative signals (-10 to 0): price-only, goods
    """
    score = 0.0
    title = title or ''

    # 1. Type match (0-40)
    if item_type == 'proposal':
        score += 40
    elif item_type == 'designated_competitive':
        score += 15
    elif item_type == 'general_competitive':
        score += 10

    if category == 'consulting':
        score += 15
    elif category == 'it':
        score += 12
    elif category == 'service':
        score += 8
    elif category == 'construction':
        score += 3

    # 2. Budget signal (0-15)
    if amount:
        try:
            amt_str = re.sub(r'[^\d]', '', str(amount))
            if amt_str:
                amt_num = int(amt_str)
                if amt_num > 50000000:
                    score += 15
                elif amt_num > 10000000:
                    score += 12
                elif amt_num > 1000000:
                    score += 8
                elif amt_num > 100000:
                    score += 4
        except (ValueError, TypeError):
            pass

    # 3. Attachment (0-10)
    if attachment_count and attachment_count > 3:
        score += 10
    elif attachment_count and attachment_count > 0:
        score += 5

    # 4. Keywords (0-20)
    for kw in PROPOSAL_KEYWORDS:
        if kw in title:
            score += 10
            break
    for kw in HIGH_VALUE_KEYWORDS:
        if kw in title:
            score += 5
            break

    # 5. Deadline proximity (0-15)
    if deadline:
        try:
            for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']:
                try:
                    dl = datetime.strptime(deadline[:19], fmt)
                    days_left = (dl - datetime.now()).days
                    if 0 < days_left <= 14:
                        score += 15
                    elif 14 < days_left <= 30:
                        score += 10
                    elif 30 < days_left <= 60:
                        score += 5
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    # 6. Negative signals
    for kw in NEGATIVE_KEYWORDS:
        if kw in title:
            score -= 5
            break
    if category == 'goods':
        score -= 5

    return max(0, min(100, score))


def batch_score_v3(conn):
    """Rescore all procurement items with v3."""
    c = conn.cursor()

    # Ensure column exists
    try:
        c.execute('ALTER TABLE procurement_items ADD COLUMN important_v3_score REAL')
    except Exception:
        pass

    c.execute('''SELECT item_id, title, category, item_type, method, deadline,
                        amount, attachment_count, muni_code
                 FROM procurement_items''')
    items = c.fetchall()

    batch = []
    for item in items:
        item_id, title, category, item_type, method, deadline, amount, att_count, muni = item
        score = important_item_score_v3(title, category, item_type, method,
                                         deadline, amount, att_count, muni)
        batch.append((score, item_id))

    c.executemany('UPDATE procurement_items SET important_v3_score = ? WHERE item_id = ?', batch)
    conn.commit()

    return len(batch)


def run_coverage_audit(conn):
    """Run full coverage audit and return results."""
    c = conn.cursor()

    # Score distribution
    c.execute('''SELECT
        SUM(CASE WHEN important_v3_score >= 50 THEN 1 ELSE 0 END) as critical,
        SUM(CASE WHEN important_v3_score >= 30 AND important_v3_score < 50 THEN 1 ELSE 0 END) as high,
        SUM(CASE WHEN important_v3_score >= 15 AND important_v3_score < 30 THEN 1 ELSE 0 END) as medium,
        SUM(CASE WHEN important_v3_score < 15 OR important_v3_score IS NULL THEN 1 ELSE 0 END) as low,
        COUNT(*) as total
        FROM procurement_items''')
    row = c.fetchone()
    dist = {'critical': row[0], 'high': row[1], 'medium': row[2], 'low': row[3], 'total': row[4]}

    # Capture rate
    c.execute('SELECT COUNT(*) FROM bid_projects')
    tracked = c.fetchone()[0]

    c.execute('''SELECT COUNT(*) FROM procurement_items
                 WHERE important_v3_score >= 30
                 AND item_id NOT IN (SELECT item_id FROM bid_projects)''')
    untracked = c.fetchone()[0]

    c.execute('''SELECT COUNT(*) FROM procurement_items
                 WHERE important_v3_score >= 50
                 AND item_id IN (SELECT item_id FROM bid_projects)''')
    critical_tracked = c.fetchone()[0]

    # Type distribution
    c.execute('''SELECT item_type, COUNT(*) FROM procurement_items
                 WHERE important_v3_score >= 30
                 GROUP BY item_type ORDER BY COUNT(*) DESC''')
    actionable_types = dict(c.fetchall())

    return {
        'score_distribution': dist,
        'tracked': tracked,
        'untracked_critical_high': untracked,
        'critical_tracked': critical_tracked,
        'critical_capture_rate': round(critical_tracked / max(1, dist['critical']) * 100, 1),
        'actionable_types': actionable_types
    }


def get_capture_rate(conn):
    """Quick capture rate calculation."""
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM bid_projects')
    tracked = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM procurement_items WHERE important_v3_score >= 30')
    actionable = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM procurement_items WHERE important_v3_score >= 50')
    critical = c.fetchone()[0]
    c.execute('''SELECT COUNT(*) FROM procurement_items
                 WHERE important_v3_score >= 50
                 AND item_id IN (SELECT item_id FROM bid_projects)''')
    critical_tracked = c.fetchone()[0]

    return {
        'tracked': tracked,
        'actionable': actionable,
        'critical': critical,
        'critical_tracked': critical_tracked,
        'capture_rate_pct': round(critical_tracked / max(1, critical) * 100, 1)
    }


def get_monthly_trend(conn):
    """Get monthly ingestion volume trend."""
    c = conn.cursor()
    c.execute('''SELECT substr(detected_at, 1, 7) as month, COUNT(*)
                 FROM procurement_items
                 WHERE detected_at IS NOT NULL
                 GROUP BY month
                 ORDER BY month DESC
                 LIMIT 12''')
    return [{'month': r[0], 'count': r[1]} for r in c.fetchall()]
