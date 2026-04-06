#!/usr/bin/env python3
"""core/required_docs.py - Required documents management."""
import json

__all__ = [
    'get_required_docs',
    'classify_doc_type',
    'check_doc_availability',
    'get_doc_template',
]

DOC_TEMPLATES = {
    '企画提案書': {'sections': ['概要', '実施方針', '実施体制', 'スケジュール', '見積概要'], 'min_pages': 5},
    '調査提案書': {'sections': ['調査目的', '調査方法', '体制', '期間', '費用'], 'min_pages': 3},
    '見積書': {'sections': ['項目', '数量', '単価', '合計'], 'format': 'table'},
    '業務実績書': {'sections': ['案件名', '期間', '内容', '発注者'], 'format': 'table'},
    '会社概要': {'sections': ['社名', '所在地', '資本金', '従業員数', '事業内容']},
    '実施体制図': {'sections': ['責任者', '担当者', '役割分担'], 'format': 'diagram'},
    '実績一覧': {'sections': ['案件名', '期間', '受注額', '発注者'], 'format': 'table'},
    '提案書': {'sections': ['概要', '方針', '体制', '見積'], 'min_pages': 3},
}

REQUIRED_DOCS_BY_TYPE = {
    'service_plan': ['企画提案書', '見積書', '業務実績書', '会社概要', '実施体制図'],
    'service_research': ['調査提案書', '見積書', '実績一覧', '会社概要'],
    'service_general': ['提案書', '見積書', '会社概要'],
    'goods_standard': ['仕様適合証明書', '見積書', '納入実績書'],
    'construction': ['施工計画書', '見積書', '実績調書', '技術者資格証明'],
}

def get_required_docs(project_type):
    return REQUIRED_DOCS_BY_TYPE.get(project_type, ['提案書', '見積書', '会社概要'])

def classify_doc_type(filename):
    filename_lower = filename.lower()
    mapping = {
        '提案': '企画提案書',
        '見積': '見積書',
        '実績': '業務実績書',
        '概要': '会社概要',
        '体制': '実施体制図',
    }
    for key, doc_type in mapping.items():
        if key in filename_lower:
            return doc_type
    return 'その他'

def check_doc_availability(conn, project_id):
    c = conn.cursor()
    c.execute("""SELECT sa.artifact_type, sa.file_name
                 FROM submission_artifacts sa
                 WHERE sa.project_id=?""", (project_id,))
    available = [(r[0], r[1]) for r in c.fetchall()]
    return available

def get_doc_template(doc_type):
    return DOC_TEMPLATES.get(doc_type, {'sections': ['内容'], 'min_pages': 1})
