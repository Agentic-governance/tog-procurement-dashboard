#!/usr/bin/env python3
"""core/submission_bundle.py - Submission bundle management."""
import sqlite3
import json

__all__ = [
    'create_bundle_for_project',
    'get_bundle_status',
    'update_bundle_component',
    'check_bundle_completeness',
    'get_bundle_blockers',
]

REQUIRED_DOCS_BY_TYPE = {
    'service_plan': ['企画提案書', '見積書', '業務実績書', '会社概要', '実施体制図'],
    'service_research': ['調査提案書', '見積書', '実績一覧', '会社概要'],
    'service_general': ['提案書', '見積書', '会社概要'],
    'goods_standard': ['仕様適合証明書', '見積書', '納入実績書'],
    'construction': ['施工計画書', '見積書', '実績調書', '技術者資格証明'],
}

def create_bundle_for_project(conn, project_id, project_type):
    c = conn.cursor()
    docs = REQUIRED_DOCS_BY_TYPE.get(project_type, ['提案書', '見積書', '会社概要'])
    c.execute("""INSERT OR IGNORE INTO submission_bundles
                 (bid_project_id, package_version, package_status, package_integrity_score,
                  required_docs_json, missing_components_json, blocker_summary_json,
                  created_at, updated_at)
                 VALUES (?, 1, 'incomplete', 0, ?, ?, ?, datetime('now'), datetime('now'))""",
              (project_id,
               json.dumps(docs, ensure_ascii=False),
               json.dumps(docs, ensure_ascii=False),
               json.dumps({'missing_count': len(docs), 'items': docs}, ensure_ascii=False)))
    conn.commit()
    return docs

def get_bundle_status(conn, project_id):
    c = conn.cursor()
    c.execute("SELECT * FROM submission_bundles WHERE bid_project_id=?", (project_id,))
    row = c.fetchone()
    if not row:
        return None
    cols = [d[0] for d in c.description]
    return dict(zip(cols, row))

def update_bundle_component(conn, project_id, doc_name, provided=True):
    c = conn.cursor()
    bundle = get_bundle_status(conn, project_id)
    if not bundle:
        return False
    required = json.loads(bundle['required_docs_json'] or '[]')
    missing = json.loads(bundle['missing_components_json'] or '[]')
    if provided and doc_name in missing:
        missing.remove(doc_name)
    elif not provided and doc_name not in missing:
        missing.append(doc_name)
    integrity = round((len(required) - len(missing)) / max(1, len(required)) * 100, 1)
    status = 'complete' if not missing else 'incomplete'
    c.execute("""UPDATE submission_bundles SET missing_components_json=?, package_integrity_score=?,
                 package_status=?, updated_at=datetime('now') WHERE bid_project_id=?""",
              (json.dumps(missing, ensure_ascii=False), integrity, status, project_id))
    conn.commit()
    return True

def check_bundle_completeness(conn, project_id):
    bundle = get_bundle_status(conn, project_id)
    if not bundle:
        return {'complete': False, 'reason': 'no_bundle'}
    missing = json.loads(bundle['missing_components_json'] or '[]')
    return {
        'complete': len(missing) == 0,
        'integrity_score': bundle['package_integrity_score'],
        'missing': missing,
        'total_required': len(json.loads(bundle['required_docs_json'] or '[]'))
    }

def get_bundle_blockers(conn, project_id):
    bundle = get_bundle_status(conn, project_id)
    if not bundle:
        return []
    blockers = json.loads(bundle['blocker_summary_json'] or '{}')
    return blockers.get('items', [])
