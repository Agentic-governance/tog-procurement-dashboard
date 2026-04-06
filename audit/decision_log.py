#!/usr/bin/env python3
"""audit/decision_log.py - Decision audit logging."""
import sqlite3
import json

__all__ = [
    'log_decision',
    'get_decision_history',
    'get_entity_decisions',
]

def log_decision(conn, decision_type, rationale, bid_project_id=None, inputs=None, output=None):
    c = conn.cursor()
    c.execute("""INSERT INTO decision_audit_log (bid_project_id, decision_type, inputs_json, output_json, rationale)
                 VALUES (?, ?, ?, ?, ?)""",
              (bid_project_id or 0, decision_type,
               json.dumps(inputs, ensure_ascii=False) if inputs else None,
               json.dumps(output, ensure_ascii=False) if output else None,
               rationale))
    conn.commit()

def get_decision_history(conn, limit=100):
    c = conn.cursor()
    c.execute("SELECT * FROM decision_audit_log ORDER BY created_at DESC LIMIT ?", (limit,))
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in c.fetchall()]

def get_entity_decisions(conn, bid_project_id):
    c = conn.cursor()
    c.execute("SELECT * FROM decision_audit_log WHERE bid_project_id=? ORDER BY created_at DESC",
              (bid_project_id,))
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in c.fetchall()]
