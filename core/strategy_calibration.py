#!/usr/bin/env python3
"""core/strategy_calibration.py - Win probability and EV calibration."""
import sqlite3
import json

__all__ = [
    'calibrate_win_probability',
    'compute_expected_value',
    'get_calibration_factors',
    'batch_calibrate',
]

def calibrate_win_probability(conn, project_id):
    c = conn.cursor()
    c.execute("""SELECT bp.project_type, bp.priority, bp.status,
                        pi.important_v3_score, pi.deadline, pi.method, pi.amount
                 FROM bid_projects bp
                 JOIN procurement_items pi ON pi.item_id = bp.item_id
                 WHERE bp.project_id = ?""", (project_id,))
    row = c.fetchone()
    if not row:
        return None
    ptype, priority, status, score, deadline, method, amount = row

    base_wp = 0.1
    if ptype == 'service_plan': base_wp += 0.1
    if ptype == 'service_research': base_wp += 0.05
    if priority == 'critical': base_wp += 0.15
    elif priority == 'high': base_wp += 0.1
    if deadline: base_wp += 0.05
    if method and 'プロポーザル' in method: base_wp += 0.1
    if (score or 0) >= 30: base_wp += 0.1

    c.execute("SELECT COUNT(*) FROM proposal_drafts WHERE project_id=?", (project_id,))
    if c.fetchone()[0] > 0: base_wp += 0.1
    c.execute("SELECT COUNT(*) FROM submission_bundles WHERE bid_project_id=?", (project_id,))
    if c.fetchone()[0] > 0: base_wp += 0.05

    return min(0.65, round(base_wp, 4))

def compute_expected_value(win_probability, amount_str):
    try:
        amount = float(amount_str) if amount_str else 0
    except (ValueError, TypeError):
        amount = 0
    return round(win_probability * amount, 0)

def get_calibration_factors(conn, project_id):
    c = conn.cursor()
    c.execute("""SELECT bp.project_type, bp.priority, pi.important_v3_score, pi.method
                 FROM bid_projects bp JOIN procurement_items pi ON pi.item_id = bp.item_id
                 WHERE bp.project_id=?""", (project_id,))
    row = c.fetchone()
    if not row: return {}
    return {
        'project_type': row[0], 'priority': row[1],
        'score': row[2], 'method': row[3]
    }

def batch_calibrate(conn, limit=100):
    c = conn.cursor()
    c.execute("""SELECT project_id FROM bid_projects
                 WHERE win_probability IS NULL OR win_probability = 0
                 LIMIT ?""", (limit,))
    ids = [r[0] for r in c.fetchall()]
    calibrated = 0
    for pid in ids:
        wp = calibrate_win_probability(conn, pid)
        if wp:
            c.execute("UPDATE bid_projects SET win_probability=?, updated_at=datetime('now') WHERE project_id=?",
                      (wp, pid))
            calibrated += 1
    conn.commit()
    return calibrated
