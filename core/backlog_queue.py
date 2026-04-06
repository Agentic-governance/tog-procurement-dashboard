#!/usr/bin/env python3
"""
core/backlog_queue.py — Backlog Queue Management
Extracted from day1_hardening_sprint.py

Handles:
- backlog_queue table creation
- Priority queue population from procurement_items
- Auto-generation of bid_projects from proposal-type items
- Queue status tracking and burn-down metrics
"""

import sqlite3
import json
import re
from datetime import datetime, timedelta

__all__ = [
    'create_backlog_tables',
    'populate_backlog_queue',
    'auto_generate_bid_projects',
    'get_backlog_stats',
    'get_backlog_items',
    'update_backlog_status',
    'compute_priority_rank',
]


def create_backlog_tables(conn):
    """Create backlog_queue and related tables."""
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS backlog_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        procurement_item_id INTEGER NOT NULL,
        important_v3_score REAL,
        backlog_type TEXT NOT NULL,
        priority_rank INTEGER,
        auto_action_status TEXT DEFAULT 'pending',
        auto_action_notes TEXT,
        bid_project_id INTEGER,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    )''')
    conn.commit()


def is_deadline_near(deadline_str, days=30):
    """Check if deadline is within N days from now."""
    if not deadline_str:
        return False
    try:
        for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']:
            try:
                dt = datetime.strptime(deadline_str[:19], fmt)
                return dt <= datetime.now() + timedelta(days=days)
            except ValueError:
                continue
    except Exception:
        pass
    return False


def compute_priority_rank(score, item_type, category, deadline, att_count, amount):
    """Compute priority rank (lower = higher priority)."""
    rank = 1000 - int(score or 0) * 10

    if item_type == 'proposal':
        rank -= 500
    if category in ('consulting', 'it', 'service'):
        rank -= 100
    if deadline and is_deadline_near(deadline):
        rank -= 300
    if att_count and att_count > 0:
        rank -= 50

    if amount:
        try:
            amt_num = int(re.sub(r'[^\d]', '', str(amount)))
            if amt_num > 10000000:
                rank -= 200
            elif amt_num > 1000000:
                rank -= 100
        except (ValueError, TypeError):
            pass

    return max(1, rank)


def classify_backlog_type(item_type, category, score, deadline, att_count, main_spec, title):
    """Classify an item into a backlog type."""
    if item_type == 'proposal':
        return 'proposal_candidate'
    if category in ('consulting', 'it') and (score or 0) >= 30:
        return 'plan_research_candidate'
    if (score or 0) >= 50:
        return 'exploit_candidate'
    if deadline and is_deadline_near(deadline):
        return 'deadline_urgent'
    if att_count and att_count > 0 and not main_spec:
        return 'attachment_missing'
    if title and ('プロポーザル' in title or '公募型' in title or '企画提案' in title):
        return 'proposal_candidate'
    return 'standard'


def populate_backlog_queue(conn, min_score=15):
    """Populate backlog_queue with actionable items not in bid_projects."""
    c = conn.cursor()
    c.execute("DELETE FROM backlog_queue")

    c.execute('''SELECT pi.item_id, pi.important_v3_score, pi.item_type, pi.category,
                        pi.deadline, pi.amount, pi.muni_code, pi.title,
                        pi.attachment_count, pi.main_spec_identified
                 FROM procurement_items pi
                 WHERE pi.important_v3_score >= ?
                 AND pi.item_id NOT IN (SELECT item_id FROM bid_projects)
                 ORDER BY pi.important_v3_score DESC''', (min_score,))
    items = c.fetchall()

    queued = 0
    for item in items:
        item_id, score, item_type, category, deadline, amount, muni_code, title, att_count, main_spec = item

        backlog_type = classify_backlog_type(item_type, category, score, deadline, att_count, main_spec, title)
        rank = compute_priority_rank(score, item_type, category, deadline, att_count, amount)

        c.execute('''INSERT INTO backlog_queue
                     (procurement_item_id, important_v3_score, backlog_type, priority_rank)
                     VALUES (?, ?, ?, ?)''',
                  (item_id, score, backlog_type, rank))
        queued += 1

    conn.commit()
    return queued


def auto_generate_bid_projects(conn, max_items=200):
    """Auto-create bid_projects for untracked proposal-type items."""
    c = conn.cursor()

    c.execute('''SELECT pi.item_id, pi.title, pi.important_v3_score, pi.category,
                        pi.muni_code, pi.deadline, pi.amount, pi.method, pi.item_type
                 FROM procurement_items pi
                 WHERE (pi.item_type = 'proposal'
                        OR pi.title LIKE '%プロポーザル%'
                        OR pi.title LIKE '%公募型%'
                        OR pi.title LIKE '%企画提案%')
                 AND pi.item_id NOT IN (SELECT item_id FROM bid_projects)
                 ORDER BY pi.important_v3_score DESC
                 LIMIT ?''', (max_items,))
    proposals = c.fetchall()

    created = 0
    for p in proposals:
        item_id, title, score, category, muni_code, deadline, amount, method, item_type = p

        if category == 'consulting':
            project_type = 'service_research'
        elif category == 'it':
            project_type = 'service_general'
        elif any(kw in (title or '') for kw in ['計画', '策定', '調査']):
            project_type = 'service_plan'
        elif '研究' in (title or '') or '分析' in (title or ''):
            project_type = 'service_research'
        else:
            project_type = 'service_general'

        priority = 'high' if (score or 0) >= 50 else 'normal' if (score or 0) >= 30 else 'low'
        structural_priority = 'critical' if (score or 0) >= 50 else 'high' if (score or 0) >= 30 else 'medium'

        context = {
            'auto_generated': True,
            'source': 'backlog_queue',
            'important_v3_score': score,
            'original_type': item_type,
            'muni_code': muni_code
        }

        c.execute('''INSERT INTO bid_projects
                     (item_id, project_type, status, priority, strategy_type,
                      structural_priority, automation_readiness, context_json,
                      created_at, updated_at)
                     VALUES (?, ?, 'intake', ?, 'explore', ?, 0.0, ?, datetime('now'), datetime('now'))''',
                  (item_id, project_type, priority, structural_priority,
                   json.dumps(context, ensure_ascii=False)))

        c.execute('''UPDATE backlog_queue SET auto_action_status = 'bid_project_created',
                     bid_project_id = ?, updated_at = datetime('now')
                     WHERE procurement_item_id = ?''',
                  (c.lastrowid, item_id))
        created += 1

    conn.commit()
    return created


def get_backlog_stats(conn):
    """Get backlog queue statistics."""
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM backlog_queue")
    total = c.fetchone()[0]

    c.execute("SELECT backlog_type, COUNT(*) FROM backlog_queue GROUP BY backlog_type ORDER BY COUNT(*) DESC")
    by_type = dict(c.fetchall())

    c.execute("SELECT auto_action_status, COUNT(*) FROM backlog_queue GROUP BY auto_action_status")
    by_status = dict(c.fetchall())

    c.execute("SELECT COUNT(*) FROM backlog_queue WHERE auto_action_status = 'pending'")
    pending = c.fetchone()[0]

    return {
        'total': total,
        'pending': pending,
        'by_type': by_type,
        'by_status': by_status
    }


def get_backlog_items(conn, backlog_type=None, limit=50, offset=0):
    """Get backlog items with details."""
    c = conn.cursor()

    query = '''SELECT bq.id, bq.procurement_item_id, bq.important_v3_score,
                      bq.backlog_type, bq.priority_rank, bq.auto_action_status,
                      pi.title, pi.category, pi.muni_code, pi.deadline
               FROM backlog_queue bq
               JOIN procurement_items pi ON pi.item_id = bq.procurement_item_id'''
    params = []

    if backlog_type:
        query += ' WHERE bq.backlog_type = ?'
        params.append(backlog_type)

    query += ' ORDER BY bq.priority_rank ASC LIMIT ? OFFSET ?'
    params.extend([limit, offset])

    c.execute(query, params)
    items = []
    for r in c.fetchall():
        items.append({
            'id': r[0], 'item_id': r[1], 'score': r[2],
            'backlog_type': r[3], 'priority_rank': r[4],
            'status': r[5], 'title': r[6], 'category': r[7],
            'muni_code': r[8], 'deadline': r[9]
        })
    return items


def update_backlog_status(conn, backlog_id, new_status, notes=None):
    """Update a backlog item's status."""
    c = conn.cursor()
    c.execute('''UPDATE backlog_queue SET auto_action_status = ?,
                 auto_action_notes = ?, updated_at = datetime('now')
                 WHERE id = ?''', (new_status, notes, backlog_id))
    conn.commit()
    return c.rowcount > 0
