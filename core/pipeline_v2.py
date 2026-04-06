#!/usr/bin/env python3
"""
core/pipeline_v2.py — 10-Stage Pipeline V2 Management
Extracted from Night 14 / Day 1 Sprint

Handles:
- Pipeline stage definitions and transitions
- Stage evaluation for individual projects
- Batch stage assignment
- Bottleneck analysis
- Stage dashboard data
"""

import sqlite3
import json
from datetime import datetime

__all__ = [
    'PIPELINE_V2_STAGES',
    'create_pipeline_tables',
    'evaluate_project_stage',
    'batch_assign_stages',
    'get_stage_distribution',
    'get_bottleneck_analysis',
    'advance_stage',
    'get_project_pipeline_detail',
]

PIPELINE_V2_STAGES = [
    'discovered',
    'captured',
    'detail_fetched',
    'attachment_fetched',
    'parsed',
    'context_enriched',
    'proposal_ready',
    'bundle_ready',
    'dryrun_ready',
    'submission_ready',
]

STAGE_INDEX = {s: i for i, s in enumerate(PIPELINE_V2_STAGES)}


def create_pipeline_tables(conn):
    """Create pipeline_v2_stages tracking table."""
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS pipeline_v2_stages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id INTEGER NOT NULL,
        current_stage TEXT NOT NULL,
        previous_stage TEXT,
        stage_entered_at TEXT DEFAULT (datetime('now')),
        gate_checks_json TEXT,
        notes TEXT
    )''')
    conn.commit()


def evaluate_project_stage(conn, project_id):
    """Evaluate the current pipeline stage for a project."""
    c = conn.cursor()

    c.execute('''SELECT bp.item_id, bp.status, bp.strategy_type,
                        bp.readiness_level, bp.submission_ready
                 FROM bid_projects bp WHERE bp.project_id = ?''', (project_id,))
    bp = c.fetchone()
    if not bp:
        return None

    item_id, status, strategy, readiness, submission_ready = bp

    c.execute('''SELECT attachment_count, main_spec_identified, title
                 FROM procurement_items WHERE item_id = ?''', (item_id,))
    pi = c.fetchone()
    att_count = pi[0] if pi else 0
    main_spec = pi[1] if pi else 0
    title = pi[2] if pi else ''

    # Gate checks
    c.execute('''SELECT COUNT(*) FROM attachment_parsed ap
                 JOIN attachment_assets aa ON aa.id = ap.asset_id
                 WHERE aa.procurement_item_id = ?''', (item_id,))
    parsed_count = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM proposal_drafts WHERE project_id = ?', (project_id,))
    has_proposal = c.fetchone()[0] > 0

    c.execute('SELECT COUNT(*) FROM submission_bundles WHERE bid_project_id = ?', (project_id,))
    has_bundle = c.fetchone()[0] > 0

    c.execute('SELECT COUNT(*) FROM spec_parses WHERE item_id = ?', (item_id,))
    has_spec_parse = c.fetchone()[0] > 0

    c.execute('''SELECT COUNT(*) FROM municipality_context_packs
                 WHERE muni_code = (SELECT muni_code FROM procurement_items WHERE item_id = ?)''',
              (item_id,))
    has_context = c.fetchone()[0] > 0

    # Stage determination
    if submission_ready:
        stage = 'submission_ready'
    elif readiness == 'dryrun_ready' or (has_bundle and has_proposal):
        stage = 'dryrun_ready'
    elif has_bundle:
        stage = 'bundle_ready'
    elif has_proposal and has_context:
        stage = 'proposal_ready'
    elif has_context and parsed_count > 0:
        stage = 'context_enriched'
    elif parsed_count > 0 or has_spec_parse:
        stage = 'parsed'
    elif att_count and att_count > 0:
        stage = 'attachment_fetched'
    elif main_spec or title:
        stage = 'detail_fetched'
    elif status == 'intake':
        stage = 'captured'
    else:
        stage = 'discovered'

    gate_checks = {
        'has_attachments': bool(att_count),
        'parsed_count': parsed_count,
        'has_proposal': has_proposal,
        'has_bundle': has_bundle,
        'has_spec_parse': has_spec_parse,
        'has_context': has_context,
        'submission_ready': bool(submission_ready),
        'readiness_level': readiness
    }

    return {
        'project_id': project_id,
        'stage': stage,
        'stage_index': STAGE_INDEX.get(stage, -1),
        'gate_checks': gate_checks,
        'title': title[:80] if title else ''
    }


def batch_assign_stages(conn):
    """Assign pipeline stages to all bid_projects."""
    c = conn.cursor()
    c.execute("DELETE FROM pipeline_v2_stages")

    c.execute('SELECT project_id FROM bid_projects ORDER BY project_id')
    project_ids = [r[0] for r in c.fetchall()]

    stage_counts = {s: 0 for s in PIPELINE_V2_STAGES}
    assignments = []

    for pid in project_ids:
        result = evaluate_project_stage(conn, pid)
        if not result:
            continue

        stage = result['stage']
        stage_counts[stage] = stage_counts.get(stage, 0) + 1

        c.execute('''INSERT INTO pipeline_v2_stages
                     (project_id, current_stage, stage_entered_at, gate_checks_json)
                     VALUES (?, ?, datetime('now'), ?)''',
                  (pid, stage, json.dumps(result['gate_checks'])))

        assignments.append(result)

    conn.commit()
    return stage_counts, assignments


def get_stage_distribution(conn):
    """Get current stage distribution."""
    c = conn.cursor()
    c.execute('SELECT current_stage, COUNT(*) FROM pipeline_v2_stages GROUP BY current_stage')
    dist = dict(c.fetchall())

    # Fill missing stages
    for s in PIPELINE_V2_STAGES:
        if s not in dist:
            dist[s] = 0

    return dist


def get_bottleneck_analysis(conn):
    """Identify pipeline bottlenecks."""
    dist = get_stage_distribution(conn)
    c = conn.cursor()

    bottlenecks = []
    for stage in PIPELINE_V2_STAGES:
        count = dist.get(stage, 0)
        if count >= 5:
            # Get blocked reasons
            c.execute('''SELECT gate_checks_json FROM pipeline_v2_stages
                         WHERE current_stage = ? LIMIT 5''', (stage,))
            checks = [json.loads(r[0]) for r in c.fetchall() if r[0]]

            missing = set()
            for ch in checks:
                if not ch.get('has_attachments'):
                    missing.add('attachments')
                if ch.get('parsed_count', 0) == 0:
                    missing.add('parsed_text')
                if not ch.get('has_proposal'):
                    missing.add('proposal')
                if not ch.get('has_context'):
                    missing.add('context_pack')

            bottlenecks.append({
                'stage': stage,
                'count': count,
                'common_missing': list(missing)
            })

    return bottlenecks


def advance_stage(conn, project_id, notes=None):
    """Re-evaluate and potentially advance a project's stage."""
    c = conn.cursor()

    # Get current stage
    c.execute('SELECT current_stage FROM pipeline_v2_stages WHERE project_id = ?', (project_id,))
    current = c.fetchone()
    prev_stage = current[0] if current else None

    # Re-evaluate
    result = evaluate_project_stage(conn, project_id)
    if not result:
        return None

    new_stage = result['stage']

    if current:
        c.execute('''UPDATE pipeline_v2_stages SET current_stage = ?,
                     previous_stage = ?, stage_entered_at = datetime('now'),
                     gate_checks_json = ?, notes = ?
                     WHERE project_id = ?''',
                  (new_stage, prev_stage, json.dumps(result['gate_checks']),
                   notes, project_id))
    else:
        c.execute('''INSERT INTO pipeline_v2_stages
                     (project_id, current_stage, previous_stage, gate_checks_json, notes)
                     VALUES (?, ?, ?, ?, ?)''',
                  (project_id, new_stage, prev_stage,
                   json.dumps(result['gate_checks']), notes))

    conn.commit()

    return {
        'project_id': project_id,
        'previous_stage': prev_stage,
        'new_stage': new_stage,
        'advanced': STAGE_INDEX.get(new_stage, 0) > STAGE_INDEX.get(prev_stage, 0) if prev_stage else True
    }


def get_project_pipeline_detail(conn, project_id):
    """Get detailed pipeline info for a single project."""
    result = evaluate_project_stage(conn, project_id)
    if not result:
        return None

    c = conn.cursor()
    c.execute('''SELECT stage_entered_at, previous_stage, notes
                 FROM pipeline_v2_stages WHERE project_id = ?''', (project_id,))
    hist = c.fetchone()

    result['entered_at'] = hist[0] if hist else None
    result['previous_stage'] = hist[1] if hist else None
    result['notes'] = hist[2] if hist else None

    # What's needed to advance
    stage = result['stage']
    idx = STAGE_INDEX.get(stage, 0)
    if idx < len(PIPELINE_V2_STAGES) - 1:
        next_stage = PIPELINE_V2_STAGES[idx + 1]
        gates = result['gate_checks']
        needs = []
        if next_stage == 'attachment_fetched' and not gates.get('has_attachments'):
            needs.append('Fetch attachments')
        if next_stage == 'parsed' and gates.get('parsed_count', 0) == 0:
            needs.append('Parse attachment text')
        if next_stage == 'context_enriched' and not gates.get('has_context'):
            needs.append('Build municipality context pack')
        if next_stage == 'proposal_ready' and not gates.get('has_proposal'):
            needs.append('Generate proposal draft')
        if next_stage == 'bundle_ready' and not gates.get('has_bundle'):
            needs.append('Build submission bundle')
        result['next_stage'] = next_stage
        result['advancement_needs'] = needs
    else:
        result['next_stage'] = None
        result['advancement_needs'] = []

    return result
