#!/usr/bin/env python3
"""audit/write_guard.py - Write endpoint guard management."""
import sqlite3
import json
import re

__all__ = [
    'check_write_guard',
    'get_all_guards',
    'update_guard',
]

def _pattern_to_regex(pattern):
    escaped = re.escape(pattern)
    return escaped.replace(r'\*', '.*')

def check_write_guard(conn, method, path):
    c = conn.cursor()
    c.execute("SELECT endpoint_pattern, guard_type, description FROM write_guard_config WHERE enabled=1")
    for ep_pattern, guard_type, desc in c.fetchall():
        parts = ep_pattern.split(' ', 1)
        if len(parts) != 2:
            continue
        pat_method, pat_path = parts
        if pat_method != method:
            continue
        regex = _pattern_to_regex(pat_path)
        if re.match(regex, path):
            return {'allowed': guard_type != 'block', 'guard_type': guard_type, 'description': desc}
    return {'allowed': True, 'guard_type': 'none'}

def get_all_guards(conn):
    c = conn.cursor()
    c.execute("SELECT * FROM write_guard_config ORDER BY id")
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, r)) for r in c.fetchall()]

def update_guard(conn, guard_id, enabled):
    c = conn.cursor()
    c.execute("UPDATE write_guard_config SET enabled=? WHERE id=?", (enabled, guard_id))
    conn.commit()
