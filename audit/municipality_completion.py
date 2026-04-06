#!/usr/bin/env python3
"""
audit/municipality_completion.py — Municipality Data Completeness Audit
Extracted from Day 1 Sprint Section 6

Handles:
- Municipality data completeness scoring
- Missing data detection
- Municipality opportunity sheets
- Procurement appetite / propensity analysis
"""

import sqlite3
import json
from datetime import datetime

__all__ = [
    'audit_municipality_completeness',
    'get_municipality_opportunity_sheet',
    'get_priority_municipalities',
    'get_completeness_summary',
]


def audit_municipality_completeness(conn, muni_code):
    """Audit data completeness for a single municipality."""
    c = conn.cursor()

    completeness = {
        'muni_code': muni_code,
        'checks': {},
        'score': 0,
        'max_score': 100
    }

    # 1. Has procurement items? (25 pts)
    c.execute('SELECT COUNT(*) FROM procurement_items WHERE muni_code = ?', (muni_code,))
    item_count = c.fetchone()[0]
    completeness['checks']['procurement_items'] = item_count
    if item_count > 0:
        completeness['score'] += 15
        if item_count >= 10:
            completeness['score'] += 10

    # 2. Has news data? (20 pts)
    c.execute('SELECT COUNT(*) FROM muni_news WHERE muni_code = ?', (muni_code,))
    news_count = c.fetchone()[0]
    completeness['checks']['news'] = news_count
    if news_count > 0:
        completeness['score'] += 10
        if news_count >= 5:
            completeness['score'] += 10

    # 3. Has context pack? (20 pts)
    c.execute('''SELECT quality_score FROM municipality_context_packs
                 WHERE muni_code = ? AND pack_type = 'comprehensive' ''', (muni_code,))
    ctx = c.fetchone()
    completeness['checks']['context_pack'] = bool(ctx)
    completeness['checks']['context_quality'] = ctx[0] if ctx else 0
    if ctx:
        completeness['score'] += 10
        if ctx[0] >= 50:
            completeness['score'] += 10

    # 4. Has bid_projects? (15 pts)
    c.execute('''SELECT COUNT(*) FROM bid_projects bp
                 JOIN procurement_items pi ON pi.item_id = bp.item_id
                 WHERE pi.muni_code = ?''', (muni_code,))
    project_count = c.fetchone()[0]
    completeness['checks']['bid_projects'] = project_count
    if project_count > 0:
        completeness['score'] += 15

    # 5. Has parsed attachments? (10 pts)
    c.execute('''SELECT COUNT(*) FROM attachment_parsed ap
                 JOIN attachment_assets aa ON aa.id = ap.asset_id
                 JOIN procurement_items pi ON pi.item_id = aa.procurement_item_id
                 WHERE pi.muni_code = ?''', (muni_code,))
    parsed_count = c.fetchone()[0]
    completeness['checks']['parsed_attachments'] = parsed_count
    if parsed_count > 0:
        completeness['score'] += 10

    # 6. Has patterns? (10 pts)
    c.execute('''SELECT COUNT(*) FROM municipality_bid_patterns
                 WHERE muni_code = ?''', (muni_code,))
    pattern_count = c.fetchone()[0]
    completeness['checks']['patterns'] = pattern_count
    if pattern_count > 0:
        completeness['score'] += 10

    return completeness


def get_municipality_opportunity_sheet(conn, muni_code):
    """Generate an opportunity sheet for a municipality."""
    c = conn.cursor()

    sheet = {'muni_code': muni_code}

    # Procurement appetite
    c.execute('''SELECT category, COUNT(*) FROM procurement_items
                 WHERE muni_code = ? GROUP BY category
                 ORDER BY COUNT(*) DESC''', (muni_code,))
    sheet['procurement_categories'] = dict(c.fetchall())

    c.execute('''SELECT item_type, COUNT(*) FROM procurement_items
                 WHERE muni_code = ? GROUP BY item_type
                 ORDER BY COUNT(*) DESC''', (muni_code,))
    sheet['item_types'] = dict(c.fetchall())

    # Proposal propensity
    c.execute('''SELECT COUNT(*) FROM procurement_items
                 WHERE muni_code = ? AND (item_type = 'proposal'
                 OR title LIKE '%プロポーザル%')''', (muni_code,))
    proposal_count = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM procurement_items WHERE muni_code = ?', (muni_code,))
    total = c.fetchone()[0]

    sheet['proposal_propensity'] = round(proposal_count / max(1, total) * 100, 1)
    sheet['total_items'] = total

    # Timing / recent activity
    c.execute('''SELECT MAX(detected_at), MIN(detected_at) FROM procurement_items
                 WHERE muni_code = ?''', (muni_code,))
    dates = c.fetchone()
    sheet['last_activity'] = dates[0]
    sheet['first_activity'] = dates[1]

    # Policy signals from context pack
    c.execute('''SELECT content_json FROM municipality_context_packs
                 WHERE muni_code = ? AND pack_type = 'comprehensive' ''', (muni_code,))
    ctx = c.fetchone()
    if ctx:
        pack = json.loads(ctx[0])
        sheet['policy_signals'] = pack.get('policy_signals', [])
        sheet['issue_signals'] = pack.get('issue_signals', [])
        sheet['budget_signals'] = pack.get('budget_signals', [])
    else:
        sheet['policy_signals'] = []
        sheet['issue_signals'] = []
        sheet['budget_signals'] = []

    # Active bid projects
    c.execute('''SELECT bp.project_id, bp.status, bp.strategy_type, pi.title
                 FROM bid_projects bp
                 JOIN procurement_items pi ON pi.item_id = bp.item_id
                 WHERE pi.muni_code = ?
                 ORDER BY bp.project_id DESC LIMIT 10''', (muni_code,))
    sheet['active_projects'] = [
        {'project_id': r[0], 'status': r[1], 'strategy': r[2], 'title': (r[3] or '')[:60]}
        for r in c.fetchall()
    ]

    return sheet


def get_priority_municipalities(conn, limit=50):
    """Get municipalities prioritized by opportunity."""
    c = conn.cursor()

    c.execute('''SELECT pi.muni_code,
                        COUNT(*) as item_count,
                        SUM(CASE WHEN pi.important_v3_score >= 30 THEN 1 ELSE 0 END) as actionable,
                        SUM(CASE WHEN pi.item_type = 'proposal' THEN 1 ELSE 0 END) as proposals,
                        MAX(pi.important_v3_score) as max_score
                 FROM procurement_items pi
                 WHERE pi.muni_code IS NOT NULL
                 GROUP BY pi.muni_code
                 HAVING actionable > 0
                 ORDER BY actionable DESC, proposals DESC
                 LIMIT ?''', (limit,))

    return [{
        'muni_code': r[0], 'total_items': r[1], 'actionable': r[2],
        'proposals': r[3], 'max_score': r[4]
    } for r in c.fetchall()]


def get_completeness_summary(conn):
    """Get overall municipality completeness summary."""
    c = conn.cursor()

    c.execute('SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code IS NOT NULL')
    total_munis = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM municipality_context_packs')
    with_context = c.fetchone()[0]

    c.execute('''SELECT COUNT(DISTINCT pi.muni_code) FROM procurement_items pi
                 JOIN bid_projects bp ON bp.item_id = pi.item_id
                 WHERE pi.muni_code IS NOT NULL''')
    with_projects = c.fetchone()[0]

    c.execute('SELECT COUNT(DISTINCT muni_code) FROM muni_news')
    with_news = c.fetchone()[0]

    return {
        'total_municipalities': total_munis,
        'with_context_pack': with_context,
        'with_bid_projects': with_projects,
        'with_news': with_news,
        'context_coverage_pct': round(with_context / max(1, total_munis) * 100, 1)
    }
