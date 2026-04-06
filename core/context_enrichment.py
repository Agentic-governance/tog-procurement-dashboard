#!/usr/bin/env python3
"""
core/context_enrichment.py — Municipality Context & Proposal Enrichment
Extracted from Day 1 Sprint Step 4

Handles:
- Municipality context pack generation from muni_news + procurement data
- Proposal context injection
- Generic proposal detection and remediation
- Context scoring
"""

import sqlite3
import json
from datetime import datetime

__all__ = [
    'create_context_tables',
    'build_context_pack',
    'build_all_context_packs',
    'enrich_proposal_with_context',
    'batch_enrich_proposals',
    'score_context',
    'detect_generic_proposals',
    'get_context_stats',
]

# Policy / budget / issue keyword dictionaries
POLICY_KEYWORDS = [
    '総合計画', '基本計画', 'DX', 'デジタル', 'SDGs', '脱炭素', '地方創生',
    '子育て', '高齢者', '防災', '観光振興', '産業振興', 'まちづくり',
    '移住', '定住', '空き家', '再生可能エネルギー', 'ICT',
    '公共施設', '統廃合', 'AI', 'スマート', 'GX', 'カーボンニュートラル'
]

BUDGET_KEYWORDS = [
    '予算', '補正予算', '決算', '財政', '交付金', '補助金', '基金', '起債',
    'ふるさと納税', '財政健全化'
]

ISSUE_KEYWORDS = [
    '人口減少', '少子高齢化', '過疎', '空き家', '災害', '感染症',
    '人手不足', '財政難', '老朽化', '待機児童', '買い物難民',
    '交通弱者', '防犯', '環境問題', 'インフラ老朽化', '地域活性化'
]


def create_context_tables(conn):
    """Create municipality_context_packs table."""
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS municipality_context_packs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        muni_code TEXT NOT NULL,
        pack_type TEXT NOT NULL,
        content_json TEXT NOT NULL,
        quality_score REAL,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now')),
        UNIQUE(muni_code, pack_type)
    )''')
    conn.commit()


def _extract_signals(text):
    """Extract policy/budget/issue signals from text."""
    policy = [kw for kw in POLICY_KEYWORDS if kw in text]
    budget = [kw for kw in BUDGET_KEYWORDS if kw in text]
    issues = [kw for kw in ISSUE_KEYWORDS if kw in text]
    return policy, budget, issues


def build_context_pack(conn, muni_code):
    """Build a context pack for a single municipality."""
    c = conn.cursor()

    # News data
    c.execute('''SELECT title, summary, source, published_at
                 FROM muni_news WHERE muni_code = ?
                 ORDER BY published_at DESC LIMIT 20''', (muni_code,))
    news = c.fetchall()

    # Procurement patterns
    c.execute('''SELECT category, COUNT(*) FROM procurement_items
                 WHERE muni_code = ? GROUP BY category''', (muni_code,))
    proc_patterns = c.fetchall()

    policy_all, budget_all, issue_all = [], [], []
    for n in news:
        text = (n[0] or '') + ' ' + (n[1] or '')
        p, b, i = _extract_signals(text)
        policy_all.extend(p)
        budget_all.extend(b)
        issue_all.extend(i)

    quality_score = min(100,
                        len(set(policy_all)) * 10 +
                        len(set(budget_all)) * 15 +
                        len(set(issue_all)) * 10 +
                        len(news) * 2)

    pack = {
        'muni_code': muni_code,
        'news_count': len(news),
        'policy_signals': list(set(policy_all))[:10],
        'budget_signals': list(set(budget_all))[:5],
        'issue_signals': list(set(issue_all))[:10],
        'procurement_categories': {r[0]: r[1] for r in proc_patterns if r[0]},
        'total_items': sum(r[1] for r in proc_patterns),
        'quality_score': quality_score,
        'generated_at': datetime.now().isoformat()
    }

    c.execute('''INSERT OR REPLACE INTO municipality_context_packs
                 (muni_code, pack_type, content_json, quality_score, updated_at)
                 VALUES (?, 'comprehensive', ?, ?, datetime('now'))''',
              (muni_code, json.dumps(pack, ensure_ascii=False), quality_score))
    conn.commit()
    return pack


def build_all_context_packs(conn):
    """Build context packs for all municipalities with bid_projects or high-score items."""
    c = conn.cursor()

    c.execute('''SELECT DISTINCT pi.muni_code FROM procurement_items pi
                 JOIN bid_projects bp ON bp.item_id = pi.item_id
                 WHERE pi.muni_code IS NOT NULL
                 UNION
                 SELECT DISTINCT muni_code FROM procurement_items
                 WHERE important_v3_score >= 30 AND muni_code IS NOT NULL''')
    munis = [r[0] for r in c.fetchall()]

    built = 0
    for mc in munis:
        build_context_pack(conn, mc)
        built += 1
    return built


def score_context(content_markdown):
    """Score proposal context richness (0-100)."""
    if not content_markdown:
        return 0

    score = 0
    if any(kw in content_markdown for kw in ['政策', '総合計画', '基本計画', 'DX推進', 'SDGs']):
        score += 20
    if any(kw in content_markdown for kw in ['予算', '財政', '補助金', '交付金']):
        score += 15
    if any(kw in content_markdown for kw in ['人口', '高齢化', '少子', '過疎']):
        score += 10
    if any(kw in content_markdown for kw in ['自治体', '地域', '市', '町', '村']):
        score += 10
    if any(kw in content_markdown for kw in ['課題', '問題', 'ニーズ', '要望']):
        score += 10
    if '自治体の現状と課題' in content_markdown:
        score += 15
    if '政策/計画との整合' in content_markdown or '政策との整合' in content_markdown:
        score += 10
    if '地域特性' in content_markdown:
        score += 10

    return min(100, score)


def _build_context_section(pack):
    """Build markdown context section from a context pack."""
    sections = []

    if pack.get('issue_signals'):
        issues = '、'.join(pack['issue_signals'][:5])
        sections.append(f"## 自治体の現状と課題\n\n当該自治体では、{issues}等の課題が認識されています。")

    if pack.get('policy_signals'):
        policies = '、'.join(pack['policy_signals'][:5])
        sections.append(f"## 政策/計画との整合\n\n自治体の主要政策方針として、{policies}等が推進されています。本提案はこれらの方針と整合する形で設計します。")

    if pack.get('budget_signals'):
        budgets = '、'.join(pack['budget_signals'][:3])
        sections.append(f"## 予算・施策背景\n\n予算関連シグナル：{budgets}")

    if pack.get('procurement_categories'):
        top = sorted(pack['procurement_categories'].items(), key=lambda x: x[1], reverse=True)[:3]
        cats_str = ', '.join(f'{k}({v}件)' for k, v in top)
        sections.append(f"## 地域特性・行政文脈\n\n当該自治体の調達傾向：{cats_str}\n総調達件数：{pack.get('total_items', 'N/A')}件")

    return '\n\n'.join(sections) + '\n\n' if sections else ''


def enrich_proposal_with_context(conn, draft_id):
    """Inject context into a single proposal draft."""
    c = conn.cursor()

    c.execute('''SELECT pd.content_markdown, pi.muni_code
                 FROM proposal_drafts pd
                 JOIN bid_projects bp ON bp.project_id = pd.project_id
                 JOIN procurement_items pi ON pi.item_id = bp.item_id
                 WHERE pd.draft_id = ?''', (draft_id,))
    row = c.fetchone()
    if not row or not row[0]:
        return False

    content, muni_code = row
    if not muni_code:
        return False

    c.execute('''SELECT content_json FROM municipality_context_packs
                 WHERE muni_code = ? AND pack_type = 'comprehensive' ''', (muni_code,))
    pack_row = c.fetchone()
    if not pack_row:
        return False

    pack = json.loads(pack_row[0])
    context_block = _build_context_section(pack)
    if not context_block:
        return False

    if '## 提案概要' in content:
        new_content = content.replace('## 提案概要', context_block + '## 提案概要')
    elif '# ' in content:
        nl = content.find('\n', content.find('# '))
        if nl > 0:
            new_content = content[:nl] + '\n\n' + context_block + content[nl:]
        else:
            new_content = content + '\n\n' + context_block
    else:
        new_content = context_block + content

    c.execute('UPDATE proposal_drafts SET content_markdown = ? WHERE draft_id = ?',
              (new_content, draft_id))
    conn.commit()
    return True


def batch_enrich_proposals(conn):
    """Enrich all generic proposals with context."""
    c = conn.cursor()
    c.execute('''SELECT pd.draft_id FROM proposal_drafts pd
                 JOIN bid_projects bp ON bp.project_id = pd.project_id
                 WHERE pd.status = 'draft' ''')
    draft_ids = [r[0] for r in c.fetchall()]

    enriched = 0
    for did in draft_ids:
        if enrich_proposal_with_context(conn, did):
            enriched += 1
    return enriched


def detect_generic_proposals(conn):
    """Detect proposals that lack municipality context."""
    c = conn.cursor()
    c.execute('SELECT draft_id, content_markdown FROM proposal_drafts WHERE status = \'draft\'')
    drafts = c.fetchall()

    generic = []
    for draft_id, content in drafts:
        if score_context(content) < 15:
            generic.append(draft_id)

    return {
        'total_drafts': len(drafts),
        'generic_count': len(generic),
        'generic_pct': round(len(generic) / len(drafts) * 100, 1) if drafts else 0,
        'generic_ids': generic
    }


def get_context_stats(conn):
    """Get overall context enrichment statistics."""
    c = conn.cursor()

    c.execute('SELECT COUNT(*) FROM municipality_context_packs')
    packs = c.fetchone()[0]

    c.execute('SELECT AVG(quality_score) FROM municipality_context_packs')
    avg_pack_quality = c.fetchone()[0] or 0

    c.execute('SELECT content_markdown FROM proposal_drafts WHERE status = \'draft\'')
    drafts = c.fetchall()
    scores = [score_context(d[0]) for d in drafts if d[0]]
    avg_context = sum(scores) / len(scores) if scores else 0
    generic = sum(1 for s in scores if s < 15)

    return {
        'context_packs': packs,
        'avg_pack_quality': round(avg_pack_quality, 1),
        'total_proposals': len(drafts),
        'avg_context_score': round(avg_context, 1),
        'generic_count': generic,
        'generic_pct': round(generic / len(drafts) * 100, 1) if drafts else 0
    }
