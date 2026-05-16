#!/usr/bin/env python3
"""
Procurement Dashboard v3.0 — Commercial-grade procurement intelligence SaaS.

Features: 1,742 municipality monitoring, MCP-powered analytics, news feed,
          scheduled crawling, Redis cache, 6-tab municipality detail.
"""

import sqlite3
import csv
import io
import json
import os
import re
import time
import logging
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
from urllib.parse import quote
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Query, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse as StarletteJSONResponse
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import uvicorn

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    HAS_SCHEDULER = True
except ImportError:
    HAS_SCHEDULER = False

from muni_dashboard import fetch_muni_dashboard_data, render_muni_dashboard
import bid_engine

logger = logging.getLogger("dashboard")

BASE_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get('DATA_DIR', BASE_DIR / "data"))
DB_PATH = DATA_DIR / "monitor.db"
MASTER_CSV = BASE_DIR / "procurement_url_master_v2.csv"

# Scheduler state
_scheduler_jobs = {}


# ── Scheduled Jobs ─────────────────────────────────────────────────────────

async def run_municipal_fetch():
    """Daily municipal procurement page fetch + diff detection."""
    logger.info("Starting municipal fetch job")
    try:
        from monitor import load_master as load_monitor_master, init_db, run_fetch, run_analyze
        conn = sqlite3.connect(str(DB_PATH))
        init_db(conn)
        entries = load_monitor_master(str(MASTER_CSV))
        entries_dict = {e['muni_code']: e for e in entries}
        changed = run_fetch(conn, entries)
        if changed:
            run_analyze(conn, changed, entries_dict)
        conn.close()
        _scheduler_jobs['municipal_fetch'] = {
            'last_run': datetime.now(timezone.utc).isoformat(),
            'result': f'{len(changed)} changes detected'
        }
        logger.info(f"Municipal fetch complete: {len(changed)} changes")
    except Exception as e:
        logger.error(f"Municipal fetch failed: {e}")
        _scheduler_jobs['municipal_fetch'] = {
            'last_run': datetime.now(timezone.utc).isoformat(),
            'result': f'ERROR: {e}'
        }


async def run_national_fetch():
    """Daily national procurement fetch from KKJ + p-portal."""
    logger.info("Starting national fetch job")
    try:
        from fetch_national import main as fetch_national_main
        fetch_national_main()
        _scheduler_jobs['national_fetch'] = {
            'last_run': datetime.now(timezone.utc).isoformat(),
            'result': 'completed'
        }
    except Exception as e:
        logger.error(f"National fetch failed: {e}")
        _scheduler_jobs['national_fetch'] = {
            'last_run': datetime.now(timezone.utc).isoformat(),
            'result': f'ERROR: {e}'
        }


async def run_news_fetch():
    """Periodic news crawl (batch of 100 municipalities, rotating)."""
    logger.info("Starting news fetch job")
    try:
        from news_crawler import crawl_batch
        stats = crawl_batch(batch_size=100)
        _scheduler_jobs['news_fetch'] = {
            'last_run': datetime.now(timezone.utc).isoformat(),
            'result': f'{stats.get("new_articles", 0)} new articles' if stats else 'completed'
        }
    except Exception as e:
        logger.error(f"News fetch failed: {e}")
        _scheduler_jobs['news_fetch'] = {
            'last_run': datetime.now(timezone.utc).isoformat(),
            'result': f'ERROR: {e}'
        }


async def refresh_analytics():
    """Daily analytics cache refresh."""
    logger.info("Starting analytics refresh")
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

        # Monthly item counts per municipality
        for row in conn.execute("""
            SELECT muni_code, strftime('%Y-%m', detected_at) as period, COUNT(*) as cnt
            FROM procurement_items
            GROUP BY muni_code, period
        """).fetchall():
            conn.execute("""
                INSERT OR REPLACE INTO procurement_analytics
                    (muni_code, metric, period, value, updated_at)
                VALUES (?, 'monthly_count', ?, ?, ?)
            """, (row['muni_code'], row['period'], row['cnt'], now))

        # Category distribution per municipality
        for row in conn.execute("""
            SELECT muni_code, category, COUNT(*) as cnt
            FROM procurement_items WHERE category IS NOT NULL
            GROUP BY muni_code, category
        """).fetchall():
            conn.execute("""
                INSERT OR REPLACE INTO procurement_analytics
                    (muni_code, metric, period, value, updated_at)
                VALUES (?, 'category_count', ?, ?, ?)
            """, (row['muni_code'], row['category'], row['cnt'], now))

        conn.commit()
        conn.close()
        _scheduler_jobs['analytics_refresh'] = {
            'last_run': now, 'result': 'completed'
        }
        logger.info("Analytics refresh complete")
    except Exception as e:
        logger.error(f"Analytics refresh failed: {e}")


# ── Lifespan ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app):
    # Startup: create indexes
    if DB_PATH.exists():
        conn = sqlite3.connect(str(DB_PATH))
        from monitor import init_db
        init_db(conn)
        bid_engine.init_bid_tables(conn)
        bid_engine.init_security(conn)
        bid_engine.init_night4_tables(conn)
        bid_engine.init_night5_tables(conn)
        bid_engine.init_night6_tables(conn)
        bid_engine.init_night6plus_tables(conn)
        bid_engine.init_night7_tables(conn)
        bid_engine.init_night8_tables(conn)
        bid_engine.init_audit_tables(conn)
        bid_engine.migrate_pipeline_to_projects(conn)
        for sql in [
            "CREATE INDEX IF NOT EXISTS idx_items_deadline ON procurement_items(deadline)",
            "CREATE INDEX IF NOT EXISTS idx_items_title ON procurement_items(title)",
            "CREATE INDEX IF NOT EXISTS idx_snap_muni ON snapshots(muni_code, fetched_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_diffs_detected ON diffs(detected_at DESC)",
        ]:
            try:
                conn.execute(sql)
            except Exception:
                pass
        conn.commit()
        conn.close()

    # Start scheduler
    scheduler = None
    if HAS_SCHEDULER:
        scheduler = AsyncIOScheduler()
        # Municipal fetch: daily 6:00 JST (21:00 UTC)
        scheduler.add_job(run_municipal_fetch, 'cron', hour=21, minute=0, id='municipal_fetch')
        # National fetch: daily 7:00 JST (22:00 UTC)
        scheduler.add_job(run_national_fetch, 'cron', hour=22, minute=0, id='national_fetch')
        # News fetch: every 6 hours
        scheduler.add_job(run_news_fetch, 'interval', hours=6, id='news_fetch')
        # Analytics refresh: daily 8:00 JST (23:00 UTC)
        scheduler.add_job(refresh_analytics, 'cron', hour=23, minute=0, id='analytics_refresh')
        scheduler.start()
        logger.info("Scheduler started with 4 jobs")

    yield

    if scheduler:
        scheduler.shutdown()
        logger.info("Scheduler shutdown")


app = FastAPI(title="ToG Dashboard v3.0", lifespan=lifespan)


# ── Timing middleware ──────────────────────────────────────────────────────

@app.middleware("http")
async def add_timing(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - t0
    response.headers["X-Response-Time"] = f"{elapsed:.3f}s"
    response.headers["Server-Timing"] = f"total;dur={elapsed*1000:.0f}"
    return response


# ── Data Access ────────────────────────────────────────────────────────────
# get_db() is defined above with persistent connection + mmap


def load_master():
    entries = {}
    csv_path = str(MASTER_CSV)
    if not os.path.exists(csv_path):
        return entries
    with open(csv_path, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            entries[row['muni_code']] = row
    return entries


MASTER = load_master()
PREF_NAMES = {
    '01':'北海道','02':'青森','03':'岩手','04':'宮城','05':'秋田','06':'山形','07':'福島',
    '08':'茨城','09':'栃木','10':'群馬','11':'埼玉','12':'千葉','13':'東京','14':'神奈川',
    '15':'新潟','16':'富山','17':'石川','18':'福井','19':'山梨','20':'長野','21':'岐阜',
    '22':'静岡','23':'愛知','24':'三重','25':'滋賀','26':'京都','27':'大阪','28':'兵庫',
    '29':'奈良','30':'和歌山','31':'鳥取','32':'島根','33':'岡山','34':'広島','35':'山口',
    '36':'徳島','37':'香川','38':'愛媛','39':'高知','40':'福岡','41':'佐賀','42':'長崎',
    '43':'熊本','44':'大分','45':'宮崎','46':'鹿児島','47':'沖縄',
}

# Category classification by keywords
CATEGORY_TOOLTIPS = {
    'construction': '道路・建築・設備工事など',
    'it': 'システム開発・クラウド・DXなど',
    'consulting': '設計・測量・調査・計画策定など',
    'service': '委託業務・保守・清掃・警備など',
    'goods': '物品購入・車両・リース・燃料など',
    'other': 'その他の調達案件',
}

CATEGORY_RULES = [
    ('construction', '工事', [
        '工事', '修繕', '改修', '建設', '舗装', '設置工', '撤去工', '解体',
        '建築', '塗装', '防水', '耐震', '配管', '建替', '増築', '改築',
        '造成', '掘削', '基礎', '仮設', '鉄骨', '外壁', '屋根', '内装',
        'タイル', '左官', '電気工事', '空調工事', '給排水', '下水道工事',
        '道路', '橋梁', 'トンネル', '堤防', '護岸', '砂防', '河川',
    ]),
    ('it', 'IT・システム', [
        'システム', 'ソフトウェア', 'ネットワーク', 'サーバ', 'データ',
        'ICT', 'DX', 'AI', 'クラウド', 'セキュリティ', 'LAN',
        'IP伝送', '電子計算', '情報処理', 'プログラム', 'アプリ',
        'デジタル', 'ウェブ', 'ホームページ', 'サイバー',
    ]),
    ('consulting', 'コンサル', [
        '設計', '測量', '調査', 'コンサル', '計画策定', '検討',
        '鑑定', '評価', '審査', '分析', '診断', '策定',
        'アセスメント', 'モニタリング',
    ]),
    ('service', '役務・委託', [
        '業務委託', '委託', '役務', '保守', '点検', '清掃',
        '管理業務', '運営', '警備', '派遣', '人材',
        '除草', '草刈', '除排雪', '除雪', '排雪',
        '廃棄物', '処分', '処理', '汚泥', '収集',
        '運搬', '輸送', '運送', '配達', '配送',
        '印刷', '封入', '製本', '刊行',
        '検査', '試験', '健診', '検診',
        '支援', '補助業務', '事務', '相談',
        '広報', '周知', '啓発',
        '整備事業', '保育間伐', '造林事業', '植付', '下刈',
        '地拵', '間伐', '除伐', '森林整備', '森林環境',
        '借上', '賃借',
        '講習', '研修', 'セミナー',
        '単価契約', '請負', '業務',
    ]),
    ('goods', '物品', [
        '購入', '調達', '納入', '物品', '備品', '機器',
        '車両', '薬品', 'リース', '賃貸借',
        '買入', '売払', '交換契約',
        '燃料', '重油', 'ガス', '灯油', '軽油', '電気',
        '食品', '食料', '食材', '給食',
        'トナー', '文具', '消耗品',
        '医薬品', '医療', 'ワクチン',
        '被服', '制服',
        '自動車', '巡視艇', '船舶',
        '砕石', '資材', '材料',
        '供給契約',
    ]),
]

# Convenience lookup: category key → label
CATEGORIES = {k: l for k, l, _ in CATEGORY_RULES}
CATEGORIES['other'] = 'その他'


# ── In-memory stats cache (avoids repeated full-table scans) ──────────────

_stats_cache = {}
_STATS_TTL = 300  # 5 minutes


def _cache_get(key):
    entry = _stats_cache.get(key)
    if entry and time.time() - entry['ts'] < _STATS_TTL:
        return entry['data']
    return None


def _cache_set(key, data):
    _stats_cache[key] = {'data': data, 'ts': time.time()}
    # Evict old entries
    if len(_stats_cache) > 100:
        now = time.time()
        expired = [k for k, v in _stats_cache.items() if now - v['ts'] > _STATS_TTL]
        for k in expired:
            del _stats_cache[k]


# Persistent DB connection (reused across requests, WAL-safe for reads)
_db_conn = None


def get_db():
    global _db_conn
    if _db_conn is None:
        _db_conn = sqlite3.connect(str(DB_PATH))
        _db_conn.row_factory = sqlite3.Row
        _db_conn.execute("PRAGMA journal_mode=WAL")
        _db_conn.execute("PRAGMA cache_size=-16000")  # 16MB cache
        _db_conn.execute("PRAGMA mmap_size=268435456")  # 256MB mmap
        _db_conn.execute("PRAGMA temp_store=MEMORY")
    return _db_conn


def classify_item(title: str) -> tuple:
    """Return (category_key, category_label) based on title keywords."""
    if not title:
        return ('other', 'その他')
    for key, label, keywords in CATEGORY_RULES:
        for kw in keywords:
            if kw in title:
                return (key, label)
    return ('other', 'その他')


def deadline_info(deadline_str: str) -> dict:
    """Compute deadline countdown and urgency level."""
    if not deadline_str or deadline_str == 'z':
        return {'days': None, 'label': '', 'cls': ''}
    try:
        dl = datetime.strptime(deadline_str[:10], '%Y-%m-%d').date()
        today = date.today()
        delta = (dl - today).days
        if delta < 0:
            return {'days': delta, 'label': '終了', 'cls': 'dl-past'}
        elif delta == 0:
            return {'days': 0, 'label': '本日〆切', 'cls': 'dl-today'}
        elif delta <= 3:
            return {'days': delta, 'label': f'残{delta}日', 'cls': 'dl-urgent'}
        elif delta <= 7:
            return {'days': delta, 'label': f'残{delta}日', 'cls': 'dl-soon'}
        else:
            return {'days': delta, 'label': f'残{delta}日', 'cls': 'dl-ok'}
    except (ValueError, TypeError):
        return {'days': None, 'label': '', 'cls': ''}


def resolve_name(mc, raw_json=None):
    """Resolve muni_code to display name and prefecture."""
    if mc == 'NATIONAL':
        raw = {}
        if raw_json:
            try:
                raw = json.loads(raw_json)
            except Exception:
                pass
        return raw.get('OrganizationName', '国'), '国'
    muni = MASTER.get(mc, {})
    return muni.get('muni_name', mc), PREF_NAMES.get(mc[:2], '')


TYPE_LABELS = {
    'general_competitive': '一般競争',
    'designated_competitive': '指名競争',
    'negotiated': '随意契約',
    'proposal': 'プロポーザル',
    'award': '落札結果',
    'other': 'その他',
}

CATEGORY_COLORS = {
    'construction': '#dc2626',
    'service': '#2563eb',
    'goods': '#16a34a',
    'consulting': '#9333ea',
    'it': '#0891b2',
    'other': '#6b7280',
}

# ── HTML Template ──────────────────────────────────────────────────────────

CSS = """
:root {
  --bg: #f0f2f5; --card: #fff; --border: #e2e8f0; --primary: #1e40af;
  --primary-light: #3b82f6; --text: #1e293b; --muted: #64748b;
  --success: #16a34a; --warning: #d97706; --danger: #dc2626;
  --hover: #f8fafc; --shadow: 0 1px 3px rgba(0,0,0,0.08);
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, 'Segoe UI', 'Hiragino Sans', sans-serif;
       background: var(--bg); color: var(--text); line-height: 1.6; font-size: 14px; }
.wrap { max-width: 1400px; margin: 0 auto; padding: 0 1rem; }

/* Header */
header { background: linear-gradient(135deg, #1e3a8a 0%, #1e40af 100%);
         color: white; padding: 0; position: sticky; top: 0; z-index: 100;
         box-shadow: 0 2px 8px rgba(0,0,0,0.15); }
.header-top { display: flex; align-items: center; justify-content: space-between;
              padding: 0.7rem 0; }
.header-top h1 { font-size: 1.2rem; font-weight: 700; letter-spacing: 0.02em; }
.header-top h1 span { font-weight: 400; font-size: 0.75rem; opacity: 0.7; margin-left: 0.5rem; }
.header-meta { font-size: 0.75rem; opacity: 0.7; }
nav { display: flex; gap: 0; border-top: 1px solid rgba(255,255,255,0.15); }
nav a { color: rgba(255,255,255,0.8); text-decoration: none; padding: 0.6rem 1.2rem;
        font-size: 0.85rem; font-weight: 500; transition: all 0.15s;
        border-bottom: 2px solid transparent; }
nav a:hover { background: rgba(255,255,255,0.1); color: white; }
nav a.active { color: white; border-bottom-color: #60a5fa; background: rgba(255,255,255,0.08); }

/* Cards */
.kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
           gap: 0.8rem; margin: 1.2rem 0; }
.kpi { background: var(--card); border: 1px solid var(--border); border-radius: 10px;
       padding: 1rem 1.2rem; box-shadow: var(--shadow); }
.kpi .kpi-label { font-size: 0.72rem; color: var(--muted); text-transform: uppercase;
                  letter-spacing: 0.05em; font-weight: 600; }
.kpi .kpi-value { font-size: 1.8rem; font-weight: 800; margin: 0.2rem 0; color: var(--text); }
.kpi .kpi-sub { font-size: 0.78rem; color: var(--muted); }
.kpi-accent { border-left: 3px solid var(--primary); }

/* Section */
.section { margin-bottom: 1.5rem; }
.section-header { display: flex; align-items: center; justify-content: space-between;
                  margin-bottom: 0.8rem; }
.section-header h2 { font-size: 1.1rem; font-weight: 700; }
.section-header .actions { display: flex; gap: 0.5rem; }

/* Toolbar */
.toolbar { background: var(--card); border: 1px solid var(--border); border-radius: 10px;
           padding: 0.8rem 1rem; margin-bottom: 1rem; box-shadow: var(--shadow); }
.toolbar-row { display: flex; gap: 0.5rem; flex-wrap: wrap; align-items: center; }
.toolbar select, .toolbar input[type=text] {
  padding: 0.45rem 0.7rem; border: 1px solid var(--border); border-radius: 6px;
  font-size: 0.85rem; background: white; color: var(--text); outline: none;
  transition: border-color 0.15s; }
.toolbar select:focus, .toolbar input:focus { border-color: var(--primary-light); }
.toolbar input[type=text] { min-width: 220px; }
.btn { padding: 0.45rem 1rem; border-radius: 6px; font-size: 0.85rem; font-weight: 600;
       cursor: pointer; border: none; transition: all 0.15s; text-decoration: none;
       display: inline-flex; align-items: center; gap: 0.3rem; }
.btn-primary { background: var(--primary); color: white; }
.btn-primary:hover { background: var(--primary-light); }
.btn-outline { background: white; color: var(--text); border: 1px solid var(--border); }
.btn-outline:hover { background: var(--hover); border-color: var(--primary-light); }
.btn-sm { padding: 0.3rem 0.7rem; font-size: 0.78rem; }

/* Table */
.tbl-wrap { background: var(--card); border: 1px solid var(--border); border-radius: 10px;
            overflow: hidden; box-shadow: var(--shadow); }
table { width: 100%; border-collapse: collapse; }
th { background: #f8fafc; padding: 0.55rem 0.8rem; text-align: left;
     font-size: 0.75rem; font-weight: 700; color: var(--muted);
     text-transform: uppercase; letter-spacing: 0.04em;
     border-bottom: 2px solid var(--border); white-space: nowrap; }
th.sortable { cursor: pointer; user-select: none; }
th.sortable:hover { color: var(--primary); }
td { padding: 0.55rem 0.8rem; border-bottom: 1px solid #f1f5f9; font-size: 0.85rem;
     vertical-align: top; }
tr:hover td { background: #f8fafc; }
.item-title-cell { max-width: 420px; }
.item-title-cell a { color: var(--text); font-weight: 500; }
.item-title-cell a:hover { color: var(--primary); }

/* Badges */
.badge { display: inline-block; padding: 0.12rem 0.45rem; border-radius: 4px;
         font-size: 0.7rem; font-weight: 700; white-space: nowrap; }
.cat-construction { background: #fef2f2; color: #dc2626; }
.cat-service { background: #eff6ff; color: #2563eb; }
.cat-goods { background: #f0fdf4; color: #16a34a; }
.cat-consulting { background: #faf5ff; color: #9333ea; }
.cat-it { background: #ecfeff; color: #0891b2; }
.cat-other { background: #f9fafb; color: #6b7280; }

/* Deadline badges */
.dl-badge { display: inline-block; padding: 0.15rem 0.5rem; border-radius: 10px;
            font-size: 0.72rem; font-weight: 700; white-space: nowrap; }
.dl-past { background: #f3f4f6; color: #9ca3af; text-decoration: line-through; }
.dl-today { background: #fef2f2; color: #dc2626; animation: pulse 1.5s infinite; }
.dl-urgent { background: #fff7ed; color: #ea580c; }
.dl-soon { background: #fefce8; color: #ca8a04; }
.dl-ok { background: #f0fdf4; color: #16a34a; }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.6; } }

/* Pref tag */
.pref-tag { display: inline-block; background: #e0e7ff; color: #3730a3;
            padding: 0.08rem 0.4rem; border-radius: 3px; font-size: 0.7rem;
            font-weight: 600; margin-right: 0.3rem; }
.pref-national { background: #fef3c7; color: #92400e; }

/* Pagination */
.pagination { display: flex; justify-content: center; align-items: center;
              gap: 0.3rem; padding: 1rem 0; }
.pagination a, .pagination span {
  padding: 0.35rem 0.7rem; border-radius: 6px; font-size: 0.85rem;
  text-decoration: none; border: 1px solid var(--border); color: var(--text); }
.pagination a:hover { background: var(--primary-light); color: white; border-color: var(--primary-light); }
.pagination .current { background: var(--primary); color: white; border-color: var(--primary);
                       font-weight: 700; }
.pagination .disabled { color: var(--muted); pointer-events: none; opacity: 0.5; }

/* Status */
.status-ok { color: var(--success); }
.status-err { color: var(--danger); }

/* Responsive (Iter 22 enhanced) */
@media (max-width: 768px) {
  .kpi-row { grid-template-columns: repeat(2, 1fr); gap:0.5rem; }
  .kpi { padding:0.7rem; }
  .kpi .kpi-value { font-size:1.3rem; }
  .toolbar-row { flex-direction: column; }
  .toolbar input[type=text] { min-width: 100%; }
  .toolbar select { width:100%; }
  td, th { padding: 0.4rem 0.5rem; font-size: 0.8rem; }
  .item-title-cell { max-width: 180px; }
  nav { flex-wrap:wrap; }
  nav a { padding: 0.4rem 0.6rem; font-size: 0.78rem; }
  .header-top h1 { font-size:1rem; }
  .pref-grid { grid-template-columns:repeat(auto-fill, minmax(140px, 1fr)); }
  .tbl-scroll { max-height:60vh; }
  .filter-bar { gap:0.3rem; }
  .export-dd { position:static; }
  .export-menu { position:fixed; bottom:0; left:0; right:0; border-radius:12px 12px 0 0; }
}
@media (max-width: 480px) {
  .kpi-row { grid-template-columns: 1fr 1fr; }
  .pref-grid { grid-template-columns:repeat(2, 1fr); }
  .wrap { padding:0 0.5rem; }
}

/* Footer */
footer { text-align: center; padding: 1.5rem 0; color: var(--muted); font-size: 0.75rem;
         border-top: 1px solid var(--border); margin-top: 2rem; }
footer .timing { font-family: monospace; background: #f1f5f9; padding: 0.15rem 0.4rem;
                 border-radius: 3px; }

/* Item card mode */
.item-card { background: var(--card); border: 1px solid var(--border); border-radius: 8px;
             padding: 0.8rem 1rem; margin-bottom: 0.6rem; box-shadow: var(--shadow);
             display: flex; gap: 1rem; align-items: flex-start; }
.item-card:hover { border-color: var(--primary-light); }
.item-card .ic-main { flex: 1; }
.item-card .ic-title { font-weight: 600; font-size: 0.95rem; margin-bottom: 0.3rem; }
.item-card .ic-title a { color: var(--text); text-decoration: none; }
.item-card .ic-title a:hover { color: var(--primary); }
.item-card .ic-meta { font-size: 0.8rem; color: var(--muted); display: flex;
                      flex-wrap: wrap; gap: 0.5rem; align-items: center; }
.item-card .ic-deadline { text-align: center; min-width: 60px; }

/* Diff */
.diff { background: #1e1e2e; color: #cdd6f4; padding: 1rem; border-radius: 8px;
        font-family: 'JetBrains Mono', monospace; font-size: 0.8rem;
        white-space: pre-wrap; max-height: 400px; overflow-y: auto; }

/* Empty state */
.empty { text-align: center; padding: 3rem 1rem; color: var(--muted); }
.empty .empty-icon { font-size: 2.5rem; margin-bottom: 0.5rem; }

/* Iter7: Sticky table header */
.tbl-scroll { max-height: 70vh; overflow-y: auto; }
.tbl-scroll thead th { position: sticky; top: 0; z-index: 10;
  box-shadow: 0 2px 4px rgba(0,0,0,0.06); }

/* Iter8: Active filter badges */
.filter-bar { display:flex; gap:0.4rem; flex-wrap:wrap; align-items:center;
              margin-bottom:0.8rem; }
.filter-chip { display:inline-flex; align-items:center; gap:0.2rem;
               background:#e0e7ff; color:#3730a3; padding:0.15rem 0.5rem;
               border-radius:12px; font-size:0.75rem; font-weight:600; }

/* Iter10: Expandable rows */
.row-details { display:none; }
.row-details.open { display:table-row; }
.row-details td { padding:0.8rem 1.2rem !important; background:#f8fafc;
                  border-left:3px solid var(--primary-light); }
.btn-expand { background:none; border:1px solid var(--border); color:var(--muted);
              cursor:pointer; font-size:0.7rem; padding:0.1rem 0.35rem;
              border-radius:4px; transition:all 0.15s; line-height:1; }
.btn-expand:hover { background:var(--hover); color:var(--primary); border-color:var(--primary-light); }

/* Iter11: Dark mode */
[data-theme="dark"] { --bg:#0f172a; --card:#1e293b; --border:#334155; --text:#f1f5f9;
  --muted:#94a3b8; --hover:#334155; --shadow:0 1px 3px rgba(0,0,0,0.3); }
[data-theme="dark"] th { background:#1e293b; }
[data-theme="dark"] tr:hover td { background:#334155; }
[data-theme="dark"] .toolbar select, [data-theme="dark"] .toolbar input[type=text] {
  background:#1e293b; color:#f1f5f9; border-color:#475569; }
[data-theme="dark"] .diff { background:#0f172a; }
[data-theme="dark"] .kpi { border-color:#334155; }
[data-theme="dark"] .tbl-wrap { border-color:#334155; }
[data-theme="dark"] .row-details td { background:#1a2332; }
[data-theme="dark"] .filter-chip { background:#312e81; color:#c7d2fe; }

/* Iter12: Export dropdown */
.export-dd { position:relative; display:inline-block; }
.export-menu { display:none; position:absolute; top:100%; right:0; background:var(--card);
               border:1px solid var(--border); border-radius:8px;
               box-shadow:0 4px 12px rgba(0,0,0,0.15); z-index:50; min-width:220px;
               padding:0.3rem 0; }
.export-menu.show { display:block; }
.export-menu a { display:block; padding:0.5rem 1rem; text-decoration:none;
                 color:var(--text); font-size:0.82rem; transition:background 0.1s; }
.export-menu a:hover { background:var(--hover); }

/* Iter19: Search highlight */
mark.hl { background:#fef08a; color:inherit; padding:0 0.1rem; border-radius:2px; }
[data-theme="dark"] mark.hl { background:#854d0e; color:#fef3c7; }

/* Iter20: Term tooltip */
.term-tip { border-bottom:1px dotted var(--muted); cursor:help; }

/* Iter21: Progress bar for AJAX loading */
.progress-bar { width:100%; height:3px; background:var(--border); border-radius:2px;
                overflow:hidden; margin-top:0.5rem; }
.progress-bar .fill { height:100%; background:var(--primary-light);
                      animation:progress-indeterminate 1.5s infinite; }
@keyframes progress-indeterminate {
  0% { width:0; margin-left:0; }
  50% { width:60%; margin-left:20%; }
  100% { width:0; margin-left:100%; }
}

/* Iter23: Freshness indicator */
.freshness { display:inline-flex; align-items:center; gap:0.3rem; font-size:0.72rem; }
.freshness-dot { width:8px; height:8px; border-radius:50%; display:inline-block; }
.fresh-ok { background:var(--success); }
.fresh-stale { background:var(--warning); }
.fresh-old { background:var(--danger); }

/* Iter24: Prefecture summary grid */
.pref-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(180px, 1fr));
             gap:0.5rem; margin-bottom:1.2rem; }
.pref-card { background:var(--card); border:1px solid var(--border); border-radius:8px;
             padding:0.6rem 0.8rem; transition:all 0.15s; text-decoration:none; color:inherit; }
.pref-card:hover { border-color:var(--primary-light); box-shadow:0 2px 6px rgba(0,0,0,0.08); }

/* Iter25: Favorite star */
.fav-star { cursor:pointer; font-size:1rem; background:none; border:none;
            color:var(--muted); transition:color 0.15s; padding:0; line-height:1; }
.fav-star.active { color:#f59e0b; }
.fav-star:hover { color:#f59e0b; }

/* Iter27: Print styles */
@media print {
  header, nav, footer, .toolbar, .pagination, .btn, .export-dd,
  .fav-star, #theme-toggle, #kb-help, .filter-bar, .progress-bar { display:none !important; }
  body { font-size:11px; background:white; color:black; }
  .wrap { max-width:100%; padding:0; }
  .kpi-row { page-break-inside:avoid; }
  .tbl-wrap { box-shadow:none; border:1px solid #ccc; }
  table { font-size:10px; }
  .row-details { display:none !important; }
}

/* Iter29: Notification dot */
.nav-badge { position:relative; }
.nav-badge::after { content:''; position:absolute; top:4px; right:4px;
  width:7px; height:7px; border-radius:50%; background:var(--danger); }

/* Iter30: Back to top */
.back-top { position:fixed; bottom:2rem; right:2rem; width:40px; height:40px;
  background:var(--primary); color:white; border:none; border-radius:50%;
  cursor:pointer; font-size:1.2rem; box-shadow:0 2px 8px rgba(0,0,0,0.2);
  display:none; z-index:90; transition:opacity 0.2s; line-height:40px; text-align:center; }
.back-top:hover { background:var(--primary-light); }
[data-theme="dark"] .back-top { background:#475569; }

/* Iter31: Saved searches */
.saved-searches { position:relative; display:inline-block; }
.saved-list { display:none; position:absolute; top:100%; left:0; background:var(--card);
  border:1px solid var(--border); border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.15);
  z-index:50; min-width:260px; padding:0.3rem 0; max-height:300px; overflow-y:auto; }
.saved-list.show { display:block; }
.saved-list a { display:flex; justify-content:space-between; align-items:center;
  padding:0.4rem 0.8rem; text-decoration:none; color:var(--text);
  font-size:0.82rem; transition:background 0.1s; }
.saved-list a:hover { background:var(--hover); }
.saved-list .del-saved { color:var(--muted); cursor:pointer; font-size:0.7rem; }
.saved-list .del-saved:hover { color:var(--danger); }

/* Iter32: Recently viewed */
.recent-munis { display:flex; gap:0.4rem; flex-wrap:wrap; margin-bottom:0.8rem; }
.recent-chip { display:inline-flex; align-items:center; gap:0.2rem; background:var(--card);
  border:1px solid var(--border); padding:0.15rem 0.5rem; border-radius:12px;
  font-size:0.75rem; text-decoration:none; color:var(--text); transition:all 0.15s; }
.recent-chip:hover { border-color:var(--primary-light); background:var(--hover); }

/* Iter34: Toast notifications */
.toast-container { position:fixed; bottom:3rem; left:50%; transform:translateX(-50%);
  z-index:200; display:flex; flex-direction:column; gap:0.5rem; }
.toast { background:var(--card); border:1px solid var(--border); border-radius:8px;
  padding:0.6rem 1.2rem; box-shadow:0 4px 12px rgba(0,0,0,0.15);
  font-size:0.85rem; animation:toast-in 0.3s ease; }
@keyframes toast-in { from { opacity:0; transform:translateY(10px); } to { opacity:1; transform:translateY(0); } }

/* Iter35: Column visibility toggle */
.col-toggle { display:inline-flex; gap:0.3rem; flex-wrap:wrap; }
.col-toggle label { font-size:0.72rem; color:var(--muted); cursor:pointer;
  display:inline-flex; align-items:center; gap:0.15rem; }
.col-toggle input { width:12px; height:12px; }

/* Iter37: Category legend */
.cat-legend { display:flex; gap:0.6rem; flex-wrap:wrap; margin-bottom:0.5rem; }
.cat-legend-item { display:inline-flex; align-items:center; gap:0.2rem;
  font-size:0.72rem; color:var(--muted); }
.cat-dot { width:8px; height:8px; border-radius:50%; display:inline-block; }

/* Iter46: Copy button */
.btn-copy { background:none; border:1px solid var(--border); color:var(--muted);
  cursor:pointer; font-size:0.65rem; padding:0.1rem 0.3rem; border-radius:3px;
  transition:all 0.15s; margin-left:0.3rem; }
.btn-copy:hover { color:var(--primary); border-color:var(--primary-light); }

/* Iter49: Compact view toggle */
table.compact-view td { padding:0.3rem 0.5rem; font-size:0.8rem; }
table.compact-view th { padding:0.35rem 0.5rem; }

/* Iter50: Changelog modal */
.modal-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,0.5);
  z-index:300; align-items:center; justify-content:center; }
.modal-overlay.show { display:flex; }
.modal { background:var(--card); border-radius:12px; max-width:500px; width:90%;
  max-height:80vh; overflow-y:auto; padding:1.5rem; box-shadow:0 8px 32px rgba(0,0,0,0.2); }
.modal h3 { font-size:1.1rem; margin-bottom:1rem; }
.modal .close-modal { float:right; background:none; border:none; font-size:1.2rem;
  cursor:pointer; color:var(--muted); }

/* Pipeline board */
.pipe-board { display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr));
  gap:0.8rem; margin-bottom:1rem; }
.pipe-col { background:var(--bg); border:1px solid var(--border); border-radius:10px;
  min-height:200px; }
.pipe-col-header { padding:0.6rem 0.8rem; display:flex; justify-content:space-between;
  align-items:center; border-bottom:1px solid var(--border); }
.pipe-count { background:var(--border); color:var(--muted); font-size:0.72rem;
  font-weight:700; padding:0.1rem 0.4rem; border-radius:10px; }
.pipe-cards { padding:0.5rem; display:flex; flex-direction:column; gap:0.4rem; }
.pipe-card { background:var(--card); border:1px solid var(--border); border-radius:8px;
  padding:0.6rem; cursor:default; transition:box-shadow 0.15s; }
.pipe-card:hover { box-shadow:0 2px 8px rgba(0,0,0,0.1); }
[data-theme="dark"] .pipe-col { background:#0f172a; border-color:#334155; }
[data-theme="dark"] .pipe-card { background:#1e293b; border-color:#334155; }

/* Add to pipeline button */
.btn-pipe { background:#eff6ff; color:#1e40af; border:1px solid #bfdbfe; border-radius:4px;
  cursor:pointer; font-size:0.65rem; padding:0.1rem 0.3rem; font-weight:600;
  transition:all 0.15s; }
.btn-pipe:hover { background:#1e40af; color:white; }
.btn-pipe.added { background:#f0fdf4; color:#16a34a; border-color:#bbf7d0; cursor:default; }

/* Scorecard */
.score-bar { height:6px; background:var(--border); border-radius:3px; overflow:hidden; }
.score-fill { height:100%; border-radius:3px; transition:width 0.3s; }
"""

def layout(title: str, content: str, active: str = '', timing: float = 0):
    nav_items = [
        ('/', 'dashboard', 'ダッシュボード'),
        ('/items', 'items', '案件一覧'),
        ('/pipeline', 'pipeline', 'パイプライン'),
        ('/municipalities', 'munis', '自治体'),
        ('/market', 'market', 'マーケット'),
        ('/analytics', 'analytics', '分析'),
        ('/templates', 'templates', 'テンプレート'),
        ('/learning', 'learning', '学習'),
        ('/alerts', 'alerts', 'アラート'),
        ('/changes', 'changes', '変更'),
        ('/api-docs', 'api-docs', 'API'),
    ]
    # External link to agent (separate port)
    agent_link = '<a href="http://78.46.57.151:8010/" target="_blank" style="margin-left:auto;background:#3b82f6;color:white;padding:0.25rem 0.7rem;border-radius:6px;font-size:0.8rem;font-weight:600;text-decoration:none">AI Agent</a>'
    nav_html = ''.join(
        f'<a href="{href}" class="{"active" if key == active else ""}" aria-label="{label}">{label}</a>'
        for href, key, label in nav_items
    )
    timing_html = f'<span class="timing">{timing*1000:.0f}ms</span>' if timing else ''
    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} - 調達情報モニター</title>
<style>{CSS}</style>
</head>
<body>
<header>
  <div class="wrap">
    <div class="header-top">
      <h1>ToG Dashboard<span>v3.0</span></h1>
      <div style="display:flex;align-items:center;gap:0.8rem">
        <span class="header-meta">{len(MASTER):,} 自治体 + 国</span>
        <button id="theme-toggle" onclick="toggleTheme()" style="background:rgba(255,255,255,0.15);border:1px solid rgba(255,255,255,0.25);color:white;padding:0.2rem 0.5rem;border-radius:5px;cursor:pointer;font-size:0.78rem">Dark</button>
      </div>
    </div>
    <nav>{nav_html}{agent_link}</nav>
  </div>
</header>
<div class="wrap" style="padding-top:1rem">
{content}
</div>
<div class="toast-container" id="toast-container"></div>
<div class="modal-overlay" id="changelog-modal" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="modal">
    <button class="close-modal" onclick="this.closest('.modal-overlay').classList.remove('show')">&#10005;</button>
    <h3>ToG Dashboard v3.0 更新履歴</h3>
    <div style="font-size:0.85rem;line-height:1.8">
      <div style="font-weight:700;margin-top:0.5rem">v3.0 (2026-03)</div>
      <ul style="margin:0.3rem 0 0.8rem 1.2rem;padding:0">
        <li>MCP連携自治体プロファイル (6タブ構成)</li>
        <li>案件カテゴリ自動分類 (6カテゴリ)</li>
        <li>予算帯・部署フィルタ追加</li>
        <li>ダークモード対応</li>
        <li>キーボードショートカット</li>
        <li>検索条件の保存・復元</li>
        <li>お気に入り自治体</li>
        <li>CSV出力 (全列/簡易版)</li>
        <li>都道府県サマリグリッド</li>
        <li>分析ダッシュボード (Chart.js)</li>
        <li>ニュースフィード統合</li>
        <li>スケジューラ (APScheduler)</li>
        <li>Redis キャッシュ対応</li>
        <li>レスポンシブ・モバイル最適化</li>
        <li>印刷用CSS</li>
      </ul>
      <div style="font-weight:700">v2.0 (2026-03)</div>
      <ul style="margin:0.3rem 0 0 1.2rem;padding:0">
        <li>1,742自治体 + 国の入札監視</li>
        <li>GPT-4o-mini による差分解析</li>
        <li>自治体別プロファイルページ</li>
      </ul>
    </div>
  </div>
</div>
<button class="back-top" id="back-top" onclick="window.scrollTo({{top:0,behavior:'smooth'}})">&#8593;</button>
<footer>
  ToG Dashboard v3.0 | {len(MASTER):,} municipalities monitored
  {f' | Response: {timing_html}' if timing_html else ''}
  | <button onclick="document.getElementById('kb-help').style.display=document.getElementById('kb-help').style.display==='none'?'block':'none'" style="background:none;border:1px solid var(--border);border-radius:4px;padding:0 0.4rem;cursor:pointer;font-size:0.75rem;color:var(--muted)">? ショートカット</button>
  | <button onclick="document.getElementById('changelog-modal').classList.add('show')" style="background:none;border:1px solid var(--border);border-radius:4px;padding:0 0.4rem;cursor:pointer;font-size:0.75rem;color:var(--muted)">v3.0 更新履歴</button>
  <div id="kb-help" style="display:none;text-align:left;margin:0.5rem auto;max-width:300px;background:var(--card);border:1px solid var(--border);border-radius:8px;padding:0.8rem;font-size:0.8rem">
    <div style="font-weight:700;margin-bottom:0.3rem">キーボードショートカット</div>
    <div><kbd style="background:#f1f5f9;padding:0.1rem 0.3rem;border-radius:3px;font-size:0.75rem">/</kbd> 検索フォーカス</div>
    <div><kbd style="background:#f1f5f9;padding:0.1rem 0.3rem;border-radius:3px;font-size:0.75rem">Esc</kbd> フォーカス解除</div>
    <div><kbd style="background:#f1f5f9;padding:0.1rem 0.3rem;border-radius:3px;font-size:0.75rem">e</kbd> エクスポートメニュー</div>
  </div>
</footer>
<script>
function toggleTheme(){{
  const d=document.documentElement,btn=document.getElementById('theme-toggle');
  const isDark=d.getAttribute('data-theme')==='dark';
  d.setAttribute('data-theme',isDark?'':'dark');
  btn.textContent=isDark?'Dark':'Light';
  localStorage.setItem('tog-theme',isDark?'':'dark');
}}
(function(){{const t=localStorage.getItem('tog-theme');
  if(t==='dark'){{document.documentElement.setAttribute('data-theme','dark');
    const b=document.getElementById('theme-toggle');if(b)b.textContent='Light';}}}})();
// Iter30: back-to-top
window.addEventListener('scroll',function(){{
  const b=document.getElementById('back-top');
  if(b) b.style.display=window.scrollY>300?'block':'none';
}});
// Iter29: track last visit for notification badge
localStorage.setItem('tog-last-visit',Date.now().toString());
// Iter26: auto-refresh on dashboard (5 min)
if(window.location.pathname==='/'){{
  setTimeout(function(){{window.location.reload();}},300000);
}}
// Iter34: toast function
function showToast(msg){{
  const c=document.getElementById('toast-container');if(!c)return;
  const t=document.createElement('div');t.className='toast';t.textContent=msg;
  c.appendChild(t);setTimeout(()=>t.remove(),3000);
}}
// Iter33: share link
function shareLink(){{
  navigator.clipboard.writeText(window.location.href).then(()=>showToast('URLをコピーしました'));
}}
// Iter32: track recently viewed municipality (stores code+name)
(function(){{
  const m=window.location.pathname.match(/\\/municipality\\/([0-9]+)/);
  if(m){{
    const name=document.title.split(' - ')[0]||m[1];
    let recent=JSON.parse(localStorage.getItem('tog-recent-munis')||'[]');
    recent=recent.filter(r=>r.code!==m[1]);
    recent.unshift({{code:m[1],name:name}});
    if(recent.length>10) recent=recent.slice(0,10);
    localStorage.setItem('tog-recent-munis',JSON.stringify(recent));
  }}
}})();
</script>
</body>
</html>"""


# ── Pagination Helper ──────────────────────────────────────────────────────

def paginate_html(total: int, page: int, per_page: int, base_url: str) -> str:
    """Generate pagination links."""
    total_pages = max(1, (total + per_page - 1) // per_page)
    if total_pages <= 1:
        return ''
    parts = ['<div class="pagination">']
    if page > 1:
        parts.append(f'<a href="{base_url}&page={page-1}">&#8592; 前</a>')
    else:
        parts.append('<span class="disabled">&#8592; 前</span>')

    # Show page numbers
    start = max(1, page - 3)
    end = min(total_pages, page + 3)
    if start > 1:
        parts.append(f'<a href="{base_url}&page=1">1</a>')
        if start > 2:
            parts.append('<span style="border:none">...</span>')
    for p in range(start, end + 1):
        if p == page:
            parts.append(f'<span class="current">{p}</span>')
        else:
            parts.append(f'<a href="{base_url}&page={p}">{p}</a>')
    if end < total_pages:
        if end < total_pages - 1:
            parts.append('<span style="border:none">...</span>')
        parts.append(f'<a href="{base_url}&page={total_pages}">{total_pages}</a>')

    if page < total_pages:
        parts.append(f'<a href="{base_url}&page={page+1}">次 &#8594;</a>')
    else:
        parts.append('<span class="disabled">次 &#8594;</span>')
    parts.append('</div>')
    return ''.join(parts)


def esc(s):
    """HTML escape."""
    if not s:
        return ''
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def highlight(text: str, query: str) -> str:
    """Highlight search terms in text (Iter 19)."""
    if not query or not text:
        return esc(text)
    escaped = esc(text)
    terms = [t.strip() for t in query.replace('|', ' ').split() if t.strip()]
    for term in terms:
        et = esc(term)
        # Case-insensitive replace preserving original case
        idx = escaped.lower().find(et.lower())
        if idx >= 0:
            original = escaped[idx:idx+len(et)]
            escaped = escaped[:idx] + f'<mark class="hl">{original}</mark>' + escaped[idx+len(et):]
    return escaped


# Procurement term tooltips (Iter 20)
TERM_TIPS = {
    '一般競争': '最も競争性の高い入札方式。参加要件を満たす全ての事業者が参加可能',
    '指名競争': '発注者が指名した事業者のみが参加できる入札方式',
    '随意契約': '競争入札によらず、発注者が特定の事業者を選定して契約',
    'プロポーザル': '技術力や企画力を評価して選定する方式。価格だけでなく品質を重視',
    '総合評価': '価格と技術力の双方を評価して落札者を決定する方式',
    '公募型': '公募により参加希望者を募る方式',
}


# ── Routes ─────────────────────────────────────────────────────────────────



# ══════════════════════════════════════════════════════════════════════════════
# NIGHT 8 — SECURITY MIDDLEWARE
# ══════════════════════════════════════════════════════════════════════════════

class SecurityMiddleware(BaseHTTPMiddleware):
    """Middleware that enforces auth on all write endpoints."""

    async def dispatch(self, request: Request, call_next):
        method = request.method
        path = request.url.path

        # Classify the request
        action_type = bid_engine.classify_request(method, path)

        # Read requests and exempt paths pass through
        if action_type in ("read", "exempt"):
            return await call_next(request)

        # For write/dangerous_write, check auth
        try:
            conn = get_db()
        except Exception:
            return StarletteJSONResponse(
                {"error": "Database unavailable"}, status_code=503
            )

        auth_header = request.headers.get("authorization", "")
        query_token = request.query_params.get("token", "")
        token_provided, token_valid = bid_engine.verify_admin_token_from_header(
            auth_header, query_token, conn
        )
        ip_address = request.client.host if request.client else "unknown"

        if action_type == "write":
            if not token_valid:
                bid_engine.log_write_action(
                    conn, path, method, token_provided, token_valid,
                    action_type, blocked=True,
                    block_reason="Missing or invalid admin token",
                    ip_address=ip_address
                )
                return StarletteJSONResponse(
                    {"error": "Admin token required for write operations. "
                     "Pass Authorization: Bearer <token> header or ?token=<token> query param."},
                    status_code=403
                )
            bid_engine.log_write_action(
                conn, path, method, token_provided, token_valid,
                action_type, blocked=False, ip_address=ip_address
            )
            return await call_next(request)

        if action_type == "dangerous_write":
            if not token_valid:
                bid_engine.log_write_action(
                    conn, path, method, token_provided, token_valid,
                    action_type, blocked=True,
                    block_reason="Missing or invalid admin token",
                    ip_address=ip_address
                )
                return StarletteJSONResponse(
                    {"error": "Admin token required for dangerous write operations. "
                     "Pass Authorization: Bearer <token> header or ?token=<token> query param."},
                    status_code=403
                )
            # Check dry_run
            dry_run_param = request.query_params.get("dry_run", "true").lower()
            if dry_run_param != "false":
                bid_engine.log_write_action(
                    conn, path, method, token_provided, token_valid,
                    action_type, blocked=True,
                    block_reason="dry_run must be explicitly set to false",
                    ip_address=ip_address
                )
                return StarletteJSONResponse(
                    {"error": "Dangerous write blocked: default is dry_run=true. "
                     "Pass ?dry_run=false to confirm execution.",
                     "dry_run": True,
                     "action_type": "dangerous_write",
                     "endpoint": path},
                    status_code=403
                )
            bid_engine.log_write_action(
                conn, path, method, token_provided, token_valid,
                action_type, blocked=False, ip_address=ip_address
            )
            return await call_next(request)

        # Unknown — pass through but log
        return await call_next(request)


app.add_middleware(SecurityMiddleware)


@app.get("/", response_class=HTMLResponse)
async def dashboard_page():
    t0 = time.perf_counter()
    conn = get_db()

    # Use cached dashboard stats (5-min TTL avoids repeated full-table scans)
    cached = _cache_get('dashboard_stats')
    if cached:
        n_snapshots, n_items_total, n_national, n_changes, n_news, cat_counts, n_expiring_3d, pref_rows_raw = cached
    else:
        n_snapshots = conn.execute("SELECT COUNT(DISTINCT muni_code) FROM snapshots").fetchone()[0]
        n_items_total = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
        n_national = conn.execute(
            "SELECT COUNT(*) FROM procurement_items WHERE muni_code = 'NATIONAL'").fetchone()[0]
        n_changes = conn.execute(
            "SELECT COUNT(*) FROM diffs WHERE detected_at > datetime('now', '-30 days')").fetchone()[0]
        try:
            n_news = conn.execute("SELECT COUNT(*) FROM muni_news").fetchone()[0]
        except Exception:
            n_news = 0
        cat_counts = {}
        for row in conn.execute("SELECT COALESCE(category, 'other') as cat, COUNT(*) as cnt FROM procurement_items GROUP BY cat").fetchall():
            cat_counts[row['cat']] = row['cnt']
        n_expiring_3d = conn.execute("""
            SELECT COUNT(*) FROM procurement_items
            WHERE deadline IS NOT NULL AND deadline >= date('now') AND deadline <= date('now', '+3 days')
        """).fetchone()[0]
        pref_rows_raw = [dict(r) for r in conn.execute("""
            SELECT SUBSTR(muni_code, 1, 2) as pref, COUNT(*) as cnt
            FROM procurement_items WHERE muni_code != 'NATIONAL'
            GROUP BY pref ORDER BY cnt DESC LIMIT 8
        """).fetchall()]
        _cache_set('dashboard_stats', (n_snapshots, n_items_total, n_national, n_changes, n_news, cat_counts, n_expiring_3d, pref_rows_raw))

    n_monitored = len(MASTER)
    n_municipal = n_items_total - n_national

    # Iter 40: item count trend (compare 30d vs previous 30d)
    try:
        n_recent = conn.execute(
            "SELECT COUNT(*) FROM procurement_items WHERE detected_at > datetime('now', '-30 days')"
        ).fetchone()[0]
        n_prev = conn.execute(
            "SELECT COUNT(*) FROM procurement_items WHERE detected_at > datetime('now', '-60 days') AND detected_at <= datetime('now', '-30 days')"
        ).fetchone()[0]
        if n_prev > 0:
            trend_pct = ((n_recent - n_prev) / n_prev) * 100
            if trend_pct > 5:
                trend_html = f'<span style="color:var(--success);font-weight:600">&#9650; {trend_pct:.0f}%</span>'
            elif trend_pct < -5:
                trend_html = f'<span style="color:var(--danger);font-weight:600">&#9660; {abs(trend_pct):.0f}%</span>'
            else:
                trend_html = '<span style="color:var(--muted)">&#9644; 横ばい</span>'
        else:
            trend_html = ''
    except Exception:
        trend_html = ''

    # Iter 23: Data freshness
    try:
        latest_item = conn.execute(
            "SELECT MAX(detected_at) FROM procurement_items"
        ).fetchone()[0]
        if latest_item:
            hours_ago = (datetime.now() - datetime.strptime(latest_item[:19], '%Y-%m-%dT%H:%M:%S')).total_seconds() / 3600
            if hours_ago < 24:
                freshness_cls, freshness_label = 'fresh-ok', f'{hours_ago:.0f}時間前に更新'
            elif hours_ago < 72:
                freshness_cls, freshness_label = 'fresh-stale', f'{hours_ago/24:.0f}日前に更新'
            else:
                freshness_cls, freshness_label = 'fresh-old', f'{hours_ago/24:.0f}日前に更新'
        else:
            freshness_cls, freshness_label = 'fresh-old', 'データなし'
    except Exception:
        freshness_cls, freshness_label = 'fresh-stale', '不明'

    # Recent items (fast — uses idx_items_detected index, LIMIT 15)
    recent_items = conn.execute("""
        SELECT * FROM procurement_items ORDER BY detected_at DESC LIMIT 15
    """).fetchall()

    # Category bar chart data
    cat_html = ''
    max_cat = max(cat_counts.values()) if cat_counts else 1
    for key, lbl, _ in CATEGORY_RULES:
        cnt = cat_counts.get(key, 0)
        pct = cnt / max_cat * 100 if max_cat else 0
        color = CATEGORY_COLORS.get(key, '#6b7280')
        cat_html += f"""
        <a href="/items?cat={key}" style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.3rem;
           text-decoration:none;color:inherit;border-radius:4px;padding:2px 0;transition:background 0.15s"
           onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">
          <span style="width:80px;font-size:0.8rem;text-align:right;color:var(--muted)">{lbl}</span>
          <div style="flex:1;background:#f1f5f9;border-radius:4px;height:22px;overflow:hidden">
            <div style="width:{pct:.0f}%;background:{color};height:100%;border-radius:4px;
                        min-width:{2 if cnt else 0}px"></div>
          </div>
          <span style="width:50px;font-size:0.8rem;font-weight:600">{cnt:,}</span>
        </a>"""
    other_cnt = cat_counts.get('other', 0)
    other_pct = other_cnt / max_cat * 100 if max_cat else 0
    cat_html += f"""
    <a href="/items?cat=other" style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.3rem;
       text-decoration:none;color:inherit;border-radius:4px;padding:2px 0;transition:background 0.15s"
       onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">
      <span style="width:80px;font-size:0.8rem;text-align:right;color:var(--muted)">その他</span>
      <div style="flex:1;background:#f1f5f9;border-radius:4px;height:22px;overflow:hidden">
        <div style="width:{other_pct:.0f}%;background:#6b7280;height:100%;border-radius:4px;
                    min-width:{2 if other_cnt else 0}px"></div>
      </div>
      <span style="width:50px;font-size:0.8rem;font-weight:600">{other_cnt:,}</span>
    </a>"""

    # Prefecture ranking (clickable)
    pref_html = ''
    for row in pref_rows_raw:
        pname = PREF_NAMES.get(row['pref'], row['pref'])
        pref_html += f"""<a href="/items?pref={row['pref']}" style="display:flex;justify-content:space-between;
          padding:0.25rem 0.3rem;border-bottom:1px solid #f1f5f9;font-size:0.85rem;
          text-decoration:none;color:inherit;border-radius:4px;transition:background 0.15s"
          onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">
          <span>{pname}</span><span style="font-weight:600">{row['cnt']:,}件 &#8594;</span>
        </a>"""

    # Recent items HTML
    items_html = ''
    for item in recent_items:
        name, pref = resolve_name(item['muni_code'], item['raw_json'])
        cat_key, cat_label = classify_item(item['title'])
        dl = deadline_info(item['deadline'])
        dl_html = f'<span class="dl-badge {dl["cls"]}">{dl["label"]}</span>' if dl['label'] else ''
        pref_cls = 'pref-national' if item['muni_code'] == 'NATIONAL' else ''
        title_display = esc(item['title'] or '(無題)')
        if item['url']:
            title_display = f'<a href="{esc(item["url"])}" target="_blank">{title_display}</a>'

        items_html += f"""
        <div class="item-card">
          <div class="ic-main">
            <div class="ic-title">{title_display}</div>
            <div class="ic-meta">
              <span class="pref-tag {pref_cls}">{esc(pref)}</span>
              <span>{esc(name)}</span>
              <span class="badge cat-{cat_key}">{cat_label}</span>
              <span style="color:var(--muted)">{item['detected_at'][:10]}</span>
            </div>
          </div>
          <div class="ic-deadline">{dl_html}</div>
        </div>"""

    elapsed = time.perf_counter() - t0

    content = f"""
    <div class="kpi-row">
      <a href="/items" class="kpi kpi-accent" style="text-decoration:none;color:inherit" aria-label="総案件数 {n_items_total:,}件">
        <div class="kpi-label">総案件数</div>
        <div class="kpi-value">{n_items_total:,}</div>
        <div class="kpi-sub">国: {n_national:,} / 自治体: {n_municipal:,} {trend_html}
          <span class="freshness" style="margin-left:0.5rem"><span class="freshness-dot {freshness_cls}"></span>{freshness_label}</span>
        </div>
      </a>
      <a href="/municipalities" class="kpi" style="text-decoration:none;color:inherit">
        <div class="kpi-label">監視自治体</div>
        <div class="kpi-value">{n_snapshots:,}</div>
        <div class="kpi-sub">/ {n_monitored:,} 登録 (99.7%)</div>
      </a>
      <a href="/items?days=3" class="kpi" style="text-decoration:none;color:inherit">
        <div class="kpi-label">3日以内〆切</div>
        <div class="kpi-value" style="color:var(--danger)">{n_expiring_3d:,}</div>
        <div class="kpi-sub">要対応 &#8594;</div>
      </a>
      <div class="kpi">
        <div class="kpi-label">ニュース</div>
        <div class="kpi-value">{n_news:,}</div>
        <div class="kpi-sub">自治体関連記事</div>
      </div>
      <a href="/changes" class="kpi" style="text-decoration:none;color:inherit">
        <div class="kpi-label">変更検知 (30日)</div>
        <div class="kpi-value">{n_changes:,}</div>
        <div class="kpi-sub">ページ更新 &#8594;</div>
      </a>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem">
      <div class="tbl-wrap" style="padding:1rem">
        <div style="font-weight:700;font-size:0.9rem;margin-bottom:0.8rem">カテゴリ別件数</div>
        {cat_html}
      </div>
      <div class="tbl-wrap" style="padding:1rem">
        <div style="font-weight:700;font-size:0.9rem;margin-bottom:0.8rem">都道府県別 TOP 8</div>
        {pref_html}
      </div>
    </div>

    <div class="section">
      <div class="section-header">
        <h2>新着案件</h2>
        <div class="actions">
          <a href="/items" class="btn btn-outline btn-sm">全件表示 &#8594;</a>
        </div>
      </div>
      {items_html if items_html else '<div class="empty"><div class="empty-icon">&#128270;</div><p>まだ案件データがありません</p></div>'}
    </div>
    """

    return layout("ダッシュボード", content, active='dashboard', timing=elapsed)


@app.get("/items", response_class=HTMLResponse)
async def items_page(
    days: int = Query(30, ge=1, le=365),
    pref: str = Query(None),
    q: str = Query(None),
    cat: str = Query(None),
    dept: str = Query(None),
    budget: str = Query(None),
    sort: str = Query('date_desc'),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
):
    t0 = time.perf_counter()
    conn = get_db()

    where = "WHERE p.detected_at > datetime('now', ?)"
    params = [f'-{days} days']
    if pref:
        if pref == 'NATIONAL':
            where += " AND p.muni_code = 'NATIONAL'"
        else:
            where += " AND p.muni_code LIKE ?"
            params.append(f'{pref}%')
    if q:
        # Support AND (space) and OR (|)
        if '|' in q:
            or_terms = [t.strip() for t in q.split('|') if t.strip()]
            or_clauses = ' OR '.join(['(p.title LIKE ? OR p.method LIKE ?)'] * len(or_terms))
            where += f" AND ({or_clauses})"
            for t in or_terms:
                params.extend([f'%{t}%', f'%{t}%'])
        else:
            and_terms = [t.strip() for t in q.split() if t.strip()]
            for t in and_terms:
                where += " AND (p.title LIKE ? OR p.method LIKE ?)"
                params.extend([f'%{t}%', f'%{t}%'])
    if dept:
        where += " AND p.department = ?"
        params.append(dept)
    if budget:
        where += " AND p.budget_range = ?"
        params.append(budget)
    if cat and cat != 'all':
        where += " AND COALESCE(p.category, 'other') = ?"
        params.append(cat)

    total = conn.execute(f"SELECT COUNT(*) FROM procurement_items p {where}", params).fetchone()[0]

    # Sort
    order_map = {
        'date_desc': 'p.detected_at DESC',
        'date_asc': 'p.detected_at ASC',
        'deadline_asc': "COALESCE(p.deadline, '9999-12-31') ASC",
        'deadline_desc': "COALESCE(p.deadline, '0000-01-01') DESC",
        'title_asc': 'p.title ASC',
        'title_desc': 'p.title DESC',
        'dept_asc': "COALESCE(p.department, '') ASC",
        'dept_desc': "COALESCE(p.department, '') DESC",
    }
    order = order_map.get(sort, 'p.detected_at DESC')

    offset = (page - 1) * per_page
    items = conn.execute(f"""
        SELECT p.* FROM procurement_items p {where}
        ORDER BY {order} LIMIT ? OFFSET ?
    """, params + [per_page, offset]).fetchall()

    # Quick stats: category breakdown using SQL GROUP BY (efficient)
    # Build where clause WITHOUT category filter for stats overview
    stats_where = "WHERE p.detected_at > datetime('now', ?)"
    stats_params = [f'-{days} days']
    if pref:
        if pref == 'NATIONAL':
            stats_where += " AND p.muni_code = 'NATIONAL'"
        else:
            stats_where += " AND p.muni_code LIKE ?"
            stats_params.append(f'{pref}%')
    if q:
        if '|' in q:
            or_terms = [t.strip() for t in q.split('|') if t.strip()]
            or_clauses = ' OR '.join(['(p.title LIKE ? OR p.method LIKE ?)'] * len(or_terms))
            stats_where += f" AND ({or_clauses})"
            for t in or_terms:
                stats_params.extend([f'%{t}%', f'%{t}%'])
        else:
            and_terms = [t.strip() for t in q.split() if t.strip()]
            for t in and_terms:
                stats_where += " AND (p.title LIKE ? OR p.method LIKE ?)"
                stats_params.extend([f'%{t}%', f'%{t}%'])
    if dept:
        stats_where += " AND p.department = ?"
        stats_params.append(dept)
    if budget:
        stats_where += " AND p.budget_range = ?"
        stats_params.append(budget)
    quick_cats = {}
    for r in conn.execute(f"SELECT COALESCE(p.category, 'other') as cat, COUNT(*) as cnt FROM procurement_items p {stats_where} GROUP BY cat", stats_params).fetchall():
        quick_cats[r['cat']] = r['cnt']

    # Get distinct departments for filter dropdown
    dept_rows = conn.execute("""
        SELECT DISTINCT department FROM procurement_items
        WHERE department IS NOT NULL AND department != '' ORDER BY department
    """).fetchall()

    # Fetch bid project status for displayed items
    _bid_map = {}
    _item_ids = [it['item_id'] for it in items]
    if _item_ids:
        _placeholders = ','.join('?' * len(_item_ids))
        for _bp in conn.execute(f"SELECT item_id, project_id, status, hard_fail_risk_score FROM bid_projects WHERE item_id IN ({_placeholders})", _item_ids).fetchall():
            _bid_map[_bp['item_id']] = {'project_id': _bp['project_id'], 'status': _bp['status'], 'hf': _bp['hard_fail_risk_score'] or 0}

    # Build table rows
    rows_html = ""
    for idx, item in enumerate(items):
        mc = item['muni_code']
        name, pref_name = resolve_name(mc, item['raw_json'])
        cat_key = item['category'] or 'other'
        cat_label = dict((k, l) for k, l, _ in CATEGORY_RULES).get(cat_key, 'その他')
        dl = deadline_info(item['deadline'])
        dl_html = f'<span class="dl-badge {dl["cls"]}">{dl["label"]}</span>' if dl['label'] else '<span style="color:var(--muted);font-size:0.75rem" title="締切日が公告に記載されていません">未定</span>'
        pref_cls = 'pref-national' if mc == 'NATIONAL' else ''
        dept_display = esc(item['department'] or '') if item['department'] else '<span style="color:var(--muted)">-</span>'

        title_text = highlight(item['title'] or '(無題)', q) if q else esc(item['title'] or '(無題)')
        copy_btn = f'<button class="btn-copy" onclick="navigator.clipboard.writeText(\'{esc(item["title"] or "").replace(chr(39), "")}\');showToast(\'コピーしました\')" title="案件名をコピー">&#128203;</button>'
        title_cell = (f'<a href="{esc(item["url"])}" target="_blank">{title_text}</a>' if item['url'] else title_text) + copy_btn

        # Detail row content (Iter 20: add term tooltips)
        method_raw = item['method'] or '-'
        method_display = esc(method_raw)
        for term, tip in TERM_TIPS.items():
            if term in method_raw:
                method_display = method_display.replace(esc(term), f'<span class="term-tip" title="{esc(tip)}">{esc(term)}</span>')
        url_display = f'<a href="{esc(item["url"])}" target="_blank" style="word-break:break-all;font-size:0.8rem">{esc(item["url"][:80])}</a>' if item['url'] else '-'
        amount_display = esc(item['amount'] or '未定')
        budget_display = esc(item['budget_range'] or '-') if item['budget_range'] else '-'

        rows_html += f"""<tr>
          <td data-sort="{item['detected_at']}" style="white-space:nowrap">{item['detected_at'][:10]}</td>
          <td data-sort="{esc(name)}"><span class="pref-tag {pref_cls}">{esc(pref_name)}</span> {esc(name)}</td>
          <td class="item-title-cell" data-sort="{esc(item['title'] or '')}">{title_cell}</td>
          <td><span class="badge cat-{cat_key}" title="{CATEGORY_TOOLTIPS.get(cat_key, '')}">{cat_label}</span></td>
          <td data-sort="{item['department'] or ''}">{dept_display}</td>
          <td data-sort="{item['deadline'] or 'z'}" style="white-space:nowrap">{dl_html}</td>
          <td style="white-space:nowrap">{esc(item['amount']) or '<span style="color:var(--muted)">-</span>'}</td>
          <td style="white-space:nowrap">{'<a href="/bid-workbench/' + str(_bid_map[item["item_id"]]["project_id"]) + '" class="badge" style="background:var(--primary);color:white;font-size:0.68rem;text-decoration:none">' + bid_engine.BID_STATUS_MAP.get(_bid_map[item["item_id"]]["status"], ("","","",""))[1] + '</a>' + (' <span class="badge" style="background:#dc2626;color:white;font-size:0.6rem" title="Hard Fail Risk">HF:' + str(int(_bid_map[item["item_id"]].get("hf",0))) + '</span>' if _bid_map[item["item_id"]].get("hf",0) > 0 else '') if item['item_id'] in _bid_map else '<button class="btn btn-outline btn-sm" style="font-size:0.68rem;padding:0.1rem 0.4rem" onclick="startBid(' + str(item["item_id"]) + ',this)">&#9889; 準備開始</button>'}</td>
          <td>
            <button class="btn-expand" onclick="toggleRow(this)" title="詳細">&#9662;</button>
          </td>
        </tr>
        <tr class="row-details" id="detail-{idx}">
          <td colspan="9">
            <div style="display:grid;grid-template-columns:100px 1fr 100px 1fr;gap:0.4rem 1rem;font-size:0.83rem">
              <span style="color:var(--muted)">契約方法:</span><span>{method_display}</span>
              <span style="color:var(--muted)">金額:</span><span>{amount_display}</span>
              <span style="color:var(--muted)">予算帯:</span><span>{budget_display}</span>
              <span style="color:var(--muted)">URL:</span><span>{url_display}</span>
            </div>
            <div style="margin-top:0.5rem;display:flex;gap:0.5rem">
              <button class="btn btn-outline btn-sm" style="font-size:0.72rem" onclick="loadScorecard({item['item_id']},this)">GO/NO-GO スコア</button>
              <button class="btn btn-outline btn-sm" style="font-size:0.72rem" onclick="loadSimilar({item['item_id']},this)">類似案件</button>
            </div>
            <div id="scorecard-{item['item_id']}" style="margin-top:0.4rem"></div>
            <div id="similar-{item['item_id']}" style="margin-top:0.4rem"></div>
          </td>
        </tr>"""

    # Filters
    pref_options = ''.join(
        f'<option value="{k}" {"selected" if pref == k else ""}>{v}</option>'
        for k, v in sorted(PREF_NAMES.items())
    )
    pref_options = f'<option value="NATIONAL" {"selected" if pref == "NATIONAL" else ""}>国</option>' + pref_options

    cat_options = ''.join(
        f'<option value="{key}" {"selected" if cat == key else ""}>{lbl}</option>'
        for key, lbl, _ in CATEGORY_RULES
    )

    dept_options = ''.join(
        f'<option value="{esc(r["department"])}" {"selected" if dept == r["department"] else ""}>{esc(r["department"])}</option>'
        for r in dept_rows
    )

    budget_ranges = [
        ('100万未満', '100万未満'), ('100-500万', '100-500万'),
        ('500-1000万', '500-1000万'), ('1000万以上', '1000万以上'),
    ]
    budget_options = ''.join(
        f'<option value="{v}" {"selected" if budget == v else ""}>{label}</option>'
        for v, label in budget_ranges
    )

    sort_options = ''.join(f'<option value="{k}" {"selected" if sort == k else ""}>{v}</option>' for k, v in [
        ('date_desc', '検出日 新→古'), ('date_asc', '検出日 古→新'),
        ('deadline_asc', '〆切 近→遠'), ('deadline_desc', '〆切 遠→近'),
        ('title_asc', '案件名 A→Z'), ('title_desc', '案件名 Z→A'),
        ('dept_asc', '部署 A→Z'), ('dept_desc', '部署 Z→A'),
    ])

    # Build base URL for pagination
    qp = []
    if pref:
        qp.append(f'pref={quote(pref)}')
    if q:
        qp.append(f'q={quote(q)}')
    if cat:
        qp.append(f'cat={quote(cat)}')
    if dept:
        qp.append(f'dept={quote(dept)}')
    if budget:
        qp.append(f'budget={quote(budget)}')
    qp.append(f'days={days}')
    qp.append(f'sort={sort}')
    if per_page != 50:
        qp.append(f'per_page={per_page}')
    base_url = '/items?' + '&'.join(qp)
    pag_html = paginate_html(total, page, per_page, base_url)

    from_item = offset + 1
    to_item = min(offset + per_page, total)
    elapsed = time.perf_counter() - t0

    # Active filter bar (Iter 8)
    active_filters = []
    if pref:
        pname = PREF_NAMES.get(pref, '国' if pref == 'NATIONAL' else pref)
        active_filters.append(f'<span class="filter-chip">{esc(pname)}</span>')
    if cat and cat != 'all':
        clbl = dict((k, l) for k, l, _ in CATEGORY_RULES).get(cat, 'その他')
        active_filters.append(f'<span class="filter-chip">{clbl}</span>')
    if dept:
        active_filters.append(f'<span class="filter-chip">部署: {esc(dept[:12])}</span>')
    if budget:
        active_filters.append(f'<span class="filter-chip">{esc(budget)}</span>')
    if q:
        active_filters.append(f'<span class="filter-chip">検索: {esc(q[:20])}</span>')
    if days != 30:
        active_filters.append(f'<span class="filter-chip">{days}日間</span>')
    filter_bar_html = ''
    if active_filters:
        filter_bar_html = '<div class="filter-bar"><span style="font-size:0.75rem;color:var(--muted)">絞込中:</span>' + ''.join(active_filters) + '</div>'
    else:
        filter_bar_html = '<div class="filter-bar" id="restore-bar" style="display:none"><a href="#" id="restore-link" style="font-size:0.78rem;color:var(--primary)">前回のフィルタを復元</a></div>'
        filter_bar_html += """<script>(function(){var s=localStorage.getItem('tog-items-filters');
if(s && s!=='days=30&sort=date_desc'){var b=document.getElementById('restore-bar'),l=document.getElementById('restore-link');
if(b&&l){b.style.display='flex';l.href='/items?'+s;}}})()</script>"""

    # Quick stats bar
    quick_stats_html = '<div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:0.8rem">'
    for key, lbl, _ in CATEGORY_RULES:
        cnt = quick_cats.get(key, 0)
        if cnt > 0:
            color = CATEGORY_COLORS.get(key, '#6b7280')
            quick_stats_html += f'<span class="badge cat-{key}" style="font-size:0.78rem;padding:0.2rem 0.6rem">{lbl}: {cnt:,}</span>'
    other_cnt = quick_cats.get('other', 0)
    if other_cnt > 0:
        quick_stats_html += f'<span class="badge cat-other" style="font-size:0.78rem;padding:0.2rem 0.6rem">その他: {other_cnt:,}</span>'
    quick_stats_html += '</div>'

    content = f"""
    <div class="section">
      <div class="section-header">
        <h2>案件一覧</h2>
        <div class="actions">
          <div class="saved-searches">
            <button class="btn btn-outline btn-sm" onclick="document.getElementById('saved-list').classList.toggle('show')">&#9733; 保存検索 &#9662;</button>
            <div id="saved-list" class="saved-list"></div>
          </div>
          <button class="btn btn-outline btn-sm" onclick="saveSearch()" title="現在の検索条件を保存">&#128190; 検索を保存</button>
          <button class="btn btn-outline btn-sm" onclick="shareLink()" title="URLをコピー">&#128279; 共有</button>
          <div class="export-dd">
            <button class="btn btn-outline btn-sm" onclick="document.getElementById('exp-menu').classList.toggle('show')">&#128190; 出力 &#9662;</button>
            <div id="exp-menu" class="export-menu">
              <a href="/api/export?{('&'.join(qp))}" download="procurement_all.csv">全列 CSV</a>
              <a href="/api/export?{('&'.join(qp))}&cols=compact" download="procurement_compact.csv">簡易版 (日付・発注者・案件名・〆切)</a>
            </div>
          </div>
        </div>
      </div>

      <div class="toolbar">
        <form method="get" action="/items">
          <div class="toolbar-row">
            <select name="pref" onchange="this.form.submit()">
              <option value="">全都道府県</option>
              {pref_options}
            </select>
            <select name="cat" onchange="this.form.submit()">
              <option value="all">全カテゴリ</option>
              {cat_options}
              <option value="other" {"selected" if cat == "other" else ""}>その他</option>
            </select>
            <select name="dept" onchange="this.form.submit()">
              <option value="">全部署</option>
              {dept_options}
            </select>
            <select name="budget" onchange="this.form.submit()">
              <option value="">全予算帯</option>
              {budget_options}
            </select>
            <select name="days" onchange="this.form.submit()">
              <option value="7" {"selected" if days==7 else ""}>7日間</option>
              <option value="14" {"selected" if days==14 else ""}>14日間</option>
              <option value="30" {"selected" if days==30 else ""}>30日間</option>
              <option value="90" {"selected" if days==90 else ""}>90日間</option>
              <option value="365" {"selected" if days==365 else ""}>1年間</option>
            </select>
            <select name="sort" onchange="this.form.submit()">
              {sort_options}
            </select>
            <select name="per_page" onchange="this.form.submit()" style="width:80px" title="表示件数">
              <option value="25" {"selected" if per_page==25 else ""}>25件</option>
              <option value="50" {"selected" if per_page==50 else ""}>50件</option>
              <option value="100" {"selected" if per_page==100 else ""}>100件</option>
              <option value="200" {"selected" if per_page==200 else ""}>200件</option>
            </select>
            <input name="q" type="text" value="{esc(q or '')}"
                   placeholder="キーワード (スペースでAND、| でOR)" />
            <button type="submit" class="btn btn-primary">検索</button>
            <a href="/items" class="btn btn-outline btn-sm" style="margin-left:0.3rem">クリア</a>
          </div>
          <div style="margin-top:0.4rem;display:flex;gap:0.3rem;flex-wrap:wrap;align-items:center">
            <span style="font-size:0.72rem;color:var(--muted)">地域:</span>
            <a href="/items?pref=13&days={days}" class="btn btn-outline btn-sm" style="font-size:0.7rem;padding:0.1rem 0.4rem{';background:#e0e7ff' if pref=='13' else ''}">東京</a>
            <a href="/items?pref=27&days={days}" class="btn btn-outline btn-sm" style="font-size:0.7rem;padding:0.1rem 0.4rem{';background:#e0e7ff' if pref=='27' else ''}">大阪</a>
            <a href="/items?pref=23&days={days}" class="btn btn-outline btn-sm" style="font-size:0.7rem;padding:0.1rem 0.4rem{';background:#e0e7ff' if pref=='23' else ''}">愛知</a>
            <a href="/items?pref=14&days={days}" class="btn btn-outline btn-sm" style="font-size:0.7rem;padding:0.1rem 0.4rem{';background:#e0e7ff' if pref=='14' else ''}">神奈川</a>
            <a href="/items?pref=01&days={days}" class="btn btn-outline btn-sm" style="font-size:0.7rem;padding:0.1rem 0.4rem{';background:#e0e7ff' if pref=='01' else ''}">北海道</a>
            <a href="/items?pref=40&days={days}" class="btn btn-outline btn-sm" style="font-size:0.7rem;padding:0.1rem 0.4rem{';background:#e0e7ff' if pref=='40' else ''}">福岡</a>
            <a href="/items?pref=NATIONAL&days={days}" class="btn btn-outline btn-sm" style="font-size:0.7rem;padding:0.1rem 0.4rem{';background:#fef3c7' if pref=='NATIONAL' else ''}">国</a>
          </div>
        </form>
      </div>

      {filter_bar_html}
      {quick_stats_html}

      <div style="display:flex;justify-content:flex-end;gap:0.5rem;margin-bottom:0.3rem">
        <label style="font-size:0.72rem;color:var(--muted);cursor:pointer;display:flex;align-items:center;gap:0.2rem">
          <input type="checkbox" id="compact-toggle" onchange="document.getElementById('items-table').classList.toggle('compact-view',this.checked)" style="width:12px;height:12px"> コンパクト表示
        </label>
      </div>
      <div class="cat-legend">
        {''.join(f'<span class="cat-legend-item"><span class="cat-dot" style="background:{CATEGORY_COLORS.get(k,"#6b7280")}"></span>{l}</span>' for k,l,_ in CATEGORY_RULES)}
        <span class="cat-legend-item"><span class="cat-dot" style="background:#6b7280"></span>その他</span>
      </div>
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem">
        <span style="font-size:0.85rem;color:var(--muted)">
          {total:,}件中 {from_item}-{to_item}件を表示
        </span>
        <span style="font-size:0.78rem;color:var(--muted)">{elapsed*1000:.0f}ms</span>
      </div>

      <div class="tbl-wrap tbl-scroll">
        <table id="items-table">
          <thead>
          <tr>
            <th class="sortable" data-col="0" style="width:90px">検出日 <span class="sa"></span></th>
            <th class="sortable" data-col="1">発注者 <span class="sa"></span></th>
            <th class="sortable" data-col="2">案件名 <span class="sa"></span></th>
            <th style="width:80px">カテゴリ</th>
            <th class="sortable" data-col="4" style="width:90px">部署 <span class="sa"></span></th>
            <th class="sortable" data-col="5" style="width:80px">〆切 <span class="sa"></span></th>
            <th style="width:100px">金額</th>
            <th style="width:80px">入札準備</th>
            <th style="width:30px"><button class="btn-expand" onclick="toggleAllRows()" title="全展開/全折畳" style="font-size:0.6rem">&#9662;&#9662;</button></th>
          </tr>
          </thead>
          <tbody>
          {rows_html if rows_html else '<tr><td colspan="9"><div class="empty"><div class="empty-icon">&#128270;</div><p>該当する案件がありません</p></div></td></tr>'}
          </tbody>
        </table>
      </div>

      {pag_html}
    </div>

    <script>
    (function() {{
      const table = document.getElementById('items-table');
      if (!table) return;
      const tbody = table.querySelector('tbody');
      const headers = table.querySelectorAll('th.sortable');
      let currentCol = -1, ascending = true;
      headers.forEach(th => {{
        th.addEventListener('click', () => {{
          const col = parseInt(th.dataset.col);
          if (currentCol === col) ascending = !ascending;
          else {{ currentCol = col; ascending = true; }}
          headers.forEach(h => h.querySelector('.sa').textContent = '');
          th.querySelector('.sa').textContent = ascending ? ' \\u25B2' : ' \\u25BC';
          const rows = Array.from(tbody.querySelectorAll('tr'));
          rows.sort((a, b) => {{
            if (!a.cells[col] || !b.cells[col]) return 0;
            const aV = a.cells[col].dataset.sort || a.cells[col].textContent.trim();
            const bV = b.cells[col].dataset.sort || b.cells[col].textContent.trim();
            return ascending ? aV.localeCompare(bV, 'ja') : bV.localeCompare(aV, 'ja');
          }});
          rows.forEach(r => tbody.appendChild(r));
        }});
      }});
    }})();
    // Iter10: expand/collapse detail rows
    function toggleRow(btn){{
      const tr=btn.closest('tr');
      const det=tr.nextElementSibling;
      if(det && det.classList.contains('row-details')){{
        det.classList.toggle('open');
        btn.innerHTML=det.classList.contains('open')?'&#9652;':'&#9662;';
      }}
    }}
    // Iter44: expand/collapse all
    let allExpanded=false;
    function toggleAllRows(){{
      allExpanded=!allExpanded;
      document.querySelectorAll('.row-details').forEach(r=>{{
        if(allExpanded)r.classList.add('open');else r.classList.remove('open');
      }});
      document.querySelectorAll('.btn-expand').forEach(b=>{{
        if(b.closest('thead'))return;
        b.innerHTML=allExpanded?'&#9652;':'&#9662;';
      }});
    }}
    // Iter12: close export menu on outside click
    document.addEventListener('click',function(e){{
      const m=document.getElementById('exp-menu');
      if(m && !e.target.closest('.export-dd')) m.classList.remove('show');
    }});
    // Iter14: keyboard shortcuts
    document.addEventListener('keydown',function(e){{
      if(e.target.tagName==='INPUT'||e.target.tagName==='SELECT'||e.target.tagName==='TEXTAREA') return;
      if(e.key==='/'){{e.preventDefault();const q=document.querySelector('input[name="q"]');if(q)q.focus();}}
      if(e.key==='Escape'){{const q=document.querySelector('input[name="q"]');if(q&&document.activeElement===q)q.blur();}}
      if(e.key==='e'){{const m=document.getElementById('exp-menu');if(m)m.classList.toggle('show');}}
    }});
    // Iter13: save current filters to localStorage
    (function(){{
      const params=new URLSearchParams(window.location.search);
      if(params.toString()) localStorage.setItem('tog-items-filters',params.toString());
    }})();
    // Iter31: saved searches
    function saveSearch(){{
      const params=new URLSearchParams(window.location.search);
      if(!params.toString()||params.toString()==='days=30'){{showToast('フィルタを設定してから保存してください');return;}}
      const label=params.get('q')||params.get('cat')||params.get('pref')||'保存済み検索';
      let saves=JSON.parse(localStorage.getItem('tog-saved-searches')||'[]');
      saves.push({{label:label.substring(0,30),url:window.location.search}});
      if(saves.length>10) saves=saves.slice(-10);
      localStorage.setItem('tog-saved-searches',JSON.stringify(saves));
      renderSaved();showToast('検索条件を保存しました');
    }}
    function renderSaved(){{
      const el=document.getElementById('saved-list');if(!el)return;
      const saves=JSON.parse(localStorage.getItem('tog-saved-searches')||'[]');
      if(!saves.length){{el.innerHTML='<div style="padding:0.5rem 0.8rem;font-size:0.8rem;color:var(--muted)">保存済み検索なし</div>';return;}}
      el.innerHTML=saves.map((s,i)=>'<a href="/items'+s.url+'"><span>'+s.label+'</span><span class="del-saved" onclick="event.preventDefault();event.stopPropagation();delSaved('+i+')">&#10005;</span></a>').join('');
    }}
    function delSaved(i){{
      let saves=JSON.parse(localStorage.getItem('tog-saved-searches')||'[]');
      saves.splice(i,1);localStorage.setItem('tog-saved-searches',JSON.stringify(saves));
      renderSaved();showToast('削除しました');
    }}
    renderSaved();
    // close saved search on outside click
    document.addEventListener('click',function(e){{
      const m=document.getElementById('saved-list');
      if(m && !e.target.closest('.saved-searches')) m.classList.remove('show');
    }});
    // Start bid project
    async function startBid(itemId,btn){{
      btn.textContent='...';btn.disabled=true;
      const resp=await fetch('/api/v4/bid-projects',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{item_id:itemId}})}});
      if(resp.ok){{const d=await resp.json();window.location.href='/bid-workbench/'+d.project_id;}}
      else{{btn.textContent='⚡ 準備開始';btn.disabled=false;showToast('エラー');}}
    }}
    // GO/NO-GO scorecard
    async function loadScorecard(itemId,btn){{
      const el=document.getElementById('scorecard-'+itemId);
      if(el.innerHTML){{el.innerHTML='';return;}}
      btn.textContent='読込中...';
      const resp=await fetch('/api/scorecard/'+itemId);
      const d=await resp.json();
      btn.textContent='GO/NO-GO スコア';
      const color=d.pct>=70?'var(--success)':d.pct>=40?'var(--warning)':'var(--danger)';
      let html='<div style="background:var(--hover);padding:0.6rem;border-radius:6px;font-size:0.8rem">';
      html+='<div style="display:flex;justify-content:space-between;margin-bottom:0.4rem"><strong>GO/NO-GO スコア</strong><span style="font-weight:700;color:'+color+'">'+d.pct+'点</span></div>';
      html+='<div class="score-bar" style="margin-bottom:0.4rem"><div class="score-fill" style="width:'+d.pct+'%;background:'+color+'"></div></div>';
      d.factors.forEach(f=>{{
        html+='<div style="display:flex;justify-content:space-between;font-size:0.75rem;color:var(--muted)"><span>'+f.name+' ('+f.score+'/'+f.max+')</span><span>'+f.reason+'</span></div>';
      }});
      html+='</div>';
      el.innerHTML=html;
    }}
    // Similar items
    async function loadSimilar(itemId,btn){{
      const el=document.getElementById('similar-'+itemId);
      if(el.innerHTML){{el.innerHTML='';return;}}
      btn.textContent='読込中...';
      const resp=await fetch('/api/similar/'+itemId+'?limit=3');
      const d=await resp.json();
      btn.textContent='類似案件';
      if(!d.similar.length){{el.innerHTML='<div style="font-size:0.78rem;color:var(--muted);padding:0.3rem">類似案件なし</div>';return;}}
      let html='<div style="background:var(--hover);padding:0.5rem;border-radius:6px">';
      html+='<div style="font-size:0.78rem;font-weight:600;margin-bottom:0.3rem">類似案件 ('+d.similar.length+'件)</div>';
      d.similar.forEach(s=>{{
        html+='<div style="font-size:0.78rem;padding:0.2rem 0;border-bottom:1px solid var(--border)">'+s.title.substring(0,50)+'<span style="color:var(--muted);margin-left:0.5rem">'+s.detected_at.substring(0,10)+'</span></div>';
      }});
      html+='</div>';
      el.innerHTML=html;
    }}
    // Iter47: restore scroll position
    (function(){{
      const key='tog-scroll-'+window.location.pathname;
      const saved=sessionStorage.getItem(key);
      if(saved) window.scrollTo(0,parseInt(saved));
      window.addEventListener('beforeunload',()=>sessionStorage.setItem(key,window.scrollY.toString()));
    }})();
    // Iter36: live deadline countdown
    (function(){{
      document.querySelectorAll('.dl-badge').forEach(b=>{{
        const tr=b.closest('tr');if(!tr)return;
        const tdDl=tr.querySelector('td[data-sort]');
        if(!tdDl)return;
        const dl=tdDl.dataset.sort;
        if(!dl||dl==='z')return;
        const diff=Math.floor((new Date(dl)-new Date())/86400000);
        if(diff>=0 && diff<=3){{
          const h=Math.floor((new Date(dl)-new Date())/3600000);
          if(h<=72) b.title=h+'時間後に締切';
        }}
      }});
    }})();
    </script>
    """

    return layout("案件一覧", content, active='items', timing=elapsed)


@app.get("/municipalities", response_class=HTMLResponse)
async def municipalities_page(pref: str = Query(None), page: int = Query(1, ge=1)):
    t0 = time.perf_counter()
    conn = get_db()
    per_page = 50

    cached_munis = _cache_get('muni_list_stats')
    if cached_munis:
        snapshot_status, item_counts = cached_munis
    else:
        snapshot_status = {}
        for row in conn.execute("""
            SELECT muni_code, MAX(fetched_at) as last_fetch, content_hash, error
            FROM snapshots GROUP BY muni_code
        """).fetchall():
            snapshot_status[row['muni_code']] = dict(row)
        item_counts = {}
        for row in conn.execute("""
            SELECT muni_code, COUNT(*) as cnt
            FROM procurement_items WHERE detected_at > datetime('now', '-30 days')
            GROUP BY muni_code
        """).fetchall():
            item_counts[row['muni_code']] = row['cnt']
        _cache_set('muni_list_stats', (snapshot_status, item_counts))

    all_munis = sorted(MASTER.items())
    if pref:
        all_munis = [(mc, info) for mc, info in all_munis if mc.startswith(pref)]

    total = len(all_munis)
    offset = (page - 1) * per_page
    page_munis = all_munis[offset:offset + per_page]

    rows_html = ""
    for mc, info in page_munis:
        name = info.get('muni_name', mc)
        pref_name = PREF_NAMES.get(mc[:2], '')
        status = snapshot_status.get(mc)
        n_items = item_counts.get(mc, 0)
        url = info.get('procurement_url', '')

        if status:
            if status.get('error'):
                badge = '<span class="badge" style="background:#fef2f2;color:var(--danger)">Error</span>'
            else:
                badge = '<span class="badge" style="background:#f0fdf4;color:var(--success)">OK</span>'
            last = status['last_fetch'][:10] if status.get('last_fetch') else '-'
        else:
            badge = '<span style="color:var(--muted);font-size:0.8rem" title="この自治体のデータは現在収集中です">データ準備中</span>'
            last = '-'

        items_badge = f'<span style="font-weight:600;color:var(--primary)">{n_items}</span>' if n_items else '<span style="color:var(--muted)">0</span>'

        rows_html += f"""<tr>
          <td style="width:30px"><button class="fav-star" data-code="{mc}" onclick="toggleFav(this)" title="お気に入り">&#9734;</button></td>
          <td><span class="pref-tag">{esc(pref_name)}</span>
              <a href="/municipality/{mc}" style="font-weight:500">{esc(name)}</a></td>
          <td>{badge}</td>
          <td style="white-space:nowrap">{last}</td>
          <td style="text-align:center" data-sort="{n_items}">{items_badge}</td>
          <td style="max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
            <a href="{esc(url)}" target="_blank" style="font-size:0.78rem;color:var(--muted)">{esc(url[:60])}</a></td>
        </tr>"""

    pref_options = ''.join(
        f'<option value="{k}" {"selected" if pref == k else ""}>{v}</option>'
        for k, v in sorted(PREF_NAMES.items())
    )

    # Iter 24: prefecture summary grid (only when not filtered)
    pref_grid_html = ''
    if not pref:
        pref_counts = {}
        for mc in MASTER:
            pc = mc[:2]
            pref_counts.setdefault(pc, {'total': 0, 'items': 0})
            pref_counts[pc]['total'] += 1
            pref_counts[pc]['items'] += item_counts.get(mc, 0)
        max_items = max((v['items'] for v in pref_counts.values()), default=1) or 1
        grid_items = ''
        for pc in sorted(pref_counts.keys()):
            pn = PREF_NAMES.get(pc, pc)
            c = pref_counts[pc]
            pct = c['items'] / max_items * 100
            grid_items += f'''<a href="/municipalities?pref={pc}" class="pref-card">
              <div style="font-weight:700;font-size:0.85rem">{pn}</div>
              <div style="font-size:0.72rem;color:var(--muted)">{c['total']}自治体</div>
              <div style="height:3px;background:var(--border);border-radius:2px;margin:0.3rem 0">
                <div style="width:{pct:.0f}%;background:var(--primary-light);height:100%;border-radius:2px"></div>
              </div>
              <div style="font-size:0.72rem;color:var(--muted)">案件: {c['items']:,}</div>
            </a>'''
        pref_grid_html = f'<div class="pref-grid">{grid_items}</div>'

    base_url = '/municipalities?' + (f'pref={quote(pref)}' if pref else '')
    pag_html = paginate_html(total, page, per_page, base_url)

    elapsed = time.perf_counter() - t0

    content = f"""
    <div class="section">
      <div class="section-header">
        <h2>自治体一覧 ({total:,}件)</h2>
      </div>
      <div class="toolbar">
        <form method="get">
          <div class="toolbar-row">
            <select name="pref" onchange="this.form.submit()">
              <option value="">全都道府県</option>
              {pref_options}
            </select>
          </div>
        </form>
      </div>
      {pref_grid_html}
      <div class="recent-munis" id="recent-munis"></div>
      <script>(function(){{
        const recent=JSON.parse(localStorage.getItem('tog-recent-munis')||'[]');
        if(!recent.length) return;
        const el=document.getElementById('recent-munis');
        el.innerHTML='<span style="font-size:0.72rem;color:var(--muted)">最近閲覧:</span>'+
          recent.slice(0,6).map(r=>'<a href="/municipality/'+(r.code||r)+'" class="recent-chip">'+(r.name||r.code||r)+'</a>').join('');
      }})()</script>
      <div class="tbl-wrap">
        <table>
          <thead>
          <tr>
            <th style="width:30px"></th>
            <th class="sortable" data-col="1">自治体</th><th style="width:60px">状態</th>
            <th class="sortable" data-col="3" style="width:90px">最終取得</th>
            <th class="sortable" data-col="4" style="width:70px;text-align:center">案件数 &#8597;</th>
            <th>調達ページURL</th>
          </tr>
          </thead>
          <tbody id="muni-tbody">{rows_html}</tbody>
        </table>
      </div>
      {pag_html}
    </div>
    <script>
    // Iter25: favorite toggle
    function toggleFav(btn){{
      const code=btn.dataset.code;
      let favs=JSON.parse(localStorage.getItem('tog-favs')||'[]');
      const idx=favs.indexOf(code);
      if(idx>=0){{favs.splice(idx,1);btn.classList.remove('active');btn.innerHTML='&#9734;';}}
      else{{favs.push(code);btn.classList.add('active');btn.innerHTML='&#9733;';}}
      localStorage.setItem('tog-favs',JSON.stringify(favs));
    }}
    // restore favorite state
    (function(){{
      const favs=JSON.parse(localStorage.getItem('tog-favs')||'[]');
      document.querySelectorAll('.fav-star').forEach(btn=>{{
        if(favs.includes(btn.dataset.code)){{btn.classList.add('active');btn.innerHTML='&#9733;';}}
      }});
    }})();
    // sort
    document.querySelectorAll('.sortable').forEach(th => {{
      th.style.cursor = 'pointer';
      th.addEventListener('click', () => {{
        const tbody = document.getElementById('muni-tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const col = parseInt(th.dataset.col);
        const asc = th.dataset.dir !== 'asc';
        th.dataset.dir = asc ? 'asc' : 'desc';
        rows.sort((a, b) => {{
          let va = a.children[col].dataset.sort || a.children[col].textContent.trim();
          let vb = b.children[col].dataset.sort || b.children[col].textContent.trim();
          const na = parseFloat(va), nb = parseFloat(vb);
          if (!isNaN(na) && !isNaN(nb)) return asc ? na - nb : nb - na;
          return asc ? va.localeCompare(vb, 'ja') : vb.localeCompare(va, 'ja');
        }});
        rows.forEach(r => tbody.appendChild(r));
      }});
    }});
    </script>"""

    return layout("自治体一覧", content, active='munis', timing=elapsed)


@app.get("/municipality/{muni_code}", response_class=HTMLResponse)
async def municipality_detail(muni_code: str):
    """Municipality dashboard — renders skeleton instantly, loads data via AJAX."""
    info = MASTER.get(muni_code, {})
    name = info.get('muni_name', muni_code)
    pref_name = PREF_NAMES.get(muni_code[:2], '')

    # Render loading skeleton — page loads instantly
    content = f"""
    <div style="margin-bottom:1rem;display:flex;justify-content:space-between;align-items:center">
      <div>
        <a href="/municipalities" style="color:var(--primary);font-size:0.85rem">&#8592; 自治体一覧</a>
        <span style="color:var(--muted);margin:0 0.3rem">/</span>
        <a href="/items?pref={muni_code[:2]}" style="font-size:0.85rem;color:var(--muted);text-decoration:none">{esc(pref_name)}</a>
        <span style="color:var(--muted);margin:0 0.3rem">/</span>
        <span style="font-size:0.85rem;font-weight:600">{esc(name)}</span>
      </div>
      <span style="font-size:0.72rem;color:var(--muted)">コード: {muni_code}</span>
    </div>
    <div id="muni-content">
      <div style="text-align:center;padding:4rem 2rem">
        <div class="loading-spinner"></div>
        <p style="margin-top:1rem;color:var(--muted);font-size:0.9rem">
          {esc(name)} のデータを読み込み中...
        </p>
        <p style="color:var(--muted);font-size:0.8rem;margin-top:0.5rem">
          MCP + ローカルDB から取得しています
        </p>
        <div class="progress-bar" style="max-width:300px;margin:0.8rem auto 0">
          <div class="fill"></div>
        </div>
        <p id="load-timer" style="color:var(--muted);font-size:0.72rem;margin-top:0.3rem">0秒経過</p>
      </div>
    </div>
    <style>
    .loading-spinner {{
      display:inline-block; width:40px; height:40px;
      border:4px solid #e2e8f0; border-top:4px solid var(--primary);
      border-radius:50%; animation:spin 0.8s linear infinite;
    }}
    @keyframes spin {{ to {{ transform:rotate(360deg) }} }}
    </style>
    <script>
    (async () => {{
      const t0=Date.now();
      const timer=setInterval(()=>{{
        const el=document.getElementById('load-timer');
        if(el) el.textContent=Math.floor((Date.now()-t0)/1000)+'秒経過';
      }},1000);
      try {{
        const resp = await fetch('/api/muni/{muni_code}/html');
        clearInterval(timer);
        if (resp.ok) {{
          document.getElementById('muni-content').innerHTML = await resp.text();
        }} else {{
          document.getElementById('muni-content').innerHTML =
            '<div class="empty"><p>データの取得に失敗しました (' + resp.status + ')</p></div>';
        }}
      }} catch(e) {{
        clearInterval(timer);
        document.getElementById('muni-content').innerHTML =
          '<div class="empty"><p>通信エラー: ' + e.message + '</p></div>';
      }}
    }})();
    </script>
    """
    return layout(name, content)


@app.get("/api/muni/{muni_code}/html", response_class=HTMLResponse)
async def municipality_html_fragment(muni_code: str):
    """Return rendered municipality dashboard HTML fragment (called via AJAX)."""
    t0 = time.perf_counter()

    # Fetch all MCP data concurrently
    try:
        data = await fetch_muni_dashboard_data(muni_code)
    except Exception as e:
        data = {'profile': {'error': str(e)}, 'tog': {}, 'risk': {}, 'trend': {},
                'vendors': {}, 'peers': {}, 'documents': {}, 'topics': {},
                'brief': {}, 'bids': {}, 'fetch_time': 0}

    # Fetch local DB data (news + procurement items for this municipality)
    local_data = {'news': [], 'items': [], 'item_count': 0}
    try:
        conn = get_db()
        try:
            news_rows = conn.execute("""
                SELECT * FROM muni_news WHERE muni_code = ?
                ORDER BY published_at DESC LIMIT 30
            """, (muni_code,)).fetchall()
            local_data['news'] = [dict(r) for r in news_rows]
        except Exception:
            pass
        item_rows = conn.execute("""
            SELECT * FROM procurement_items WHERE muni_code = ?
            ORDER BY detected_at DESC LIMIT 20
        """, (muni_code,)).fetchall()
        local_data['items'] = [dict(r) for r in item_rows]
        local_data['item_count'] = conn.execute(
            "SELECT COUNT(*) FROM procurement_items WHERE muni_code = ?", (muni_code,)
        ).fetchone()[0]
    except Exception:
        pass

    elapsed = time.perf_counter() - t0
    return render_muni_dashboard(muni_code, data, local_data=local_data, timing=elapsed)


@app.get("/changes", response_class=HTMLResponse)
async def changes_page(days: int = Query(30), page: int = Query(1, ge=1)):
    t0 = time.perf_counter()
    conn = get_db()
    per_page = 50

    total = conn.execute(
        "SELECT COUNT(*) FROM diffs WHERE detected_at > datetime('now', ?)",
        (f'-{days} days',)
    ).fetchone()[0]

    offset = (page - 1) * per_page
    changes = conn.execute("""
        SELECT muni_code, detected_at, diff_lines, analyzed
        FROM diffs WHERE detected_at > datetime('now', ?)
        ORDER BY detected_at DESC LIMIT ? OFFSET ?
    """, (f'-{days} days', per_page, offset)).fetchall()

    # Summary stats for changes
    n_analyzed = sum(1 for ch in changes if ch['analyzed'])
    n_unanalyzed = len(changes) - n_analyzed
    unique_munis = len(set(ch['muni_code'] for ch in changes))

    rows_html = ""
    for ch in changes:
        muni = MASTER.get(ch['muni_code'], {})
        name = muni.get('muni_name', ch['muni_code'])
        pref_name = PREF_NAMES.get(ch['muni_code'][:2], '')
        analyzed = '<span class="badge" style="background:#f0fdf4;color:var(--success)">解析済</span>' if ch['analyzed'] else '<span class="badge" style="background:#fef3c7;color:#d97706">未解析</span>'
        dl = ch['diff_lines'] or 0
        if dl > 100:
            size_cls = 'color:var(--danger)'
        elif dl > 20:
            size_cls = 'color:var(--warning)'
        else:
            size_cls = 'color:var(--success)'
        rows_html += f"""<tr>
          <td style="white-space:nowrap">{ch['detected_at'][:16]}</td>
          <td><span class="pref-tag">{esc(pref_name)}</span>
              <a href="/municipality/{ch['muni_code']}">{esc(name)}</a></td>
          <td style="text-align:center"><span style="{size_cls};font-weight:700">&#9679;</span> {dl}</td>
          <td>{analyzed}</td>
        </tr>"""

    base_url = f'/changes?days={days}'
    pag_html = paginate_html(total, page, per_page, base_url)
    elapsed = time.perf_counter() - t0

    content = f"""
    <div class="section">
      <div class="section-header">
        <h2>変更検知履歴</h2>
      </div>

      <div class="kpi-row" style="margin-bottom:1rem">
        <div class="kpi kpi-accent">
          <div class="kpi-label">変更検知数</div>
          <div class="kpi-value">{total:,}</div>
          <div class="kpi-sub">過去{days}日間</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">対象自治体</div>
          <div class="kpi-value">{unique_munis}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">解析済</div>
          <div class="kpi-value" style="color:var(--success)">{n_analyzed}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">未解析</div>
          <div class="kpi-value" style="color:{('var(--warning)' if n_unanalyzed else 'var(--muted)')}">{n_unanalyzed}</div>
        </div>
      </div>

      <div class="toolbar">
        <form method="get">
          <div class="toolbar-row">
            <select name="days" onchange="this.form.submit()">
              <option value="7" {"selected" if days==7 else ""}>7日間</option>
              <option value="30" {"selected" if days==30 else ""}>30日間</option>
              <option value="90" {"selected" if days==90 else ""}>90日間</option>
            </select>
            <span style="font-size:0.75rem;color:var(--muted);margin-left:0.5rem"><span style="color:var(--success)">&#9679;</span> 小 <span style="color:var(--warning)">&#9679;</span> 中 <span style="color:var(--danger)">&#9679;</span> 大規模変更</span>
          </div>
        </form>
      </div>
      <div class="tbl-wrap">
        <table>
          <thead>
          <tr><th>検出日時</th><th>自治体</th><th style="text-align:center;width:80px">変更行数</th><th style="width:70px">解析</th></tr>
          </thead>
          <tbody>
          {rows_html if rows_html else '<tr><td colspan="4"><div class="empty"><p>変更なし</p></div></td></tr>'}
          </tbody>
        </table>
      </div>
      {pag_html}
    </div>"""

    return layout("変更履歴", content, active='changes', timing=elapsed)


# ── API Endpoints ──────────────────────────────────────────────────────────

@app.get("/api/items")
async def api_items(days: int = 365, pref: str = None, q: str = None, limit: int = 100, offset: int = 0):
    conn = get_db()
    where = "WHERE p.detected_at > datetime('now', ?)"
    params = [f'-{days} days']
    if pref:
        if pref == 'NATIONAL':
            where += " AND p.muni_code = 'NATIONAL'"
        else:
            where += " AND p.muni_code LIKE ?"
            params.append(f'{pref}%')
    if q:
        where += " AND (p.title LIKE ? OR p.method LIKE ?)"
        params.extend([f'%{q}%', f'%{q}%'])

    items = conn.execute(f"""
        SELECT p.* FROM procurement_items p {where}
        ORDER BY p.detected_at DESC LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchall()

    result = []
    for item in items:
        mc = item['muni_code']
        name, pref_name = resolve_name(mc, item['raw_json'])
        cat_key, cat_label = classify_item(item['title'])
        result.append({
            'muni_code': mc, 'muni_name': name, 'prefecture': pref_name,
            'title': item['title'], 'type': item['item_type'],
            'category': cat_label, 'deadline': item['deadline'],
            'amount': item['amount'], 'method': item['method'],
            'url': item['url'], 'detected_at': item['detected_at'],
        })
    return JSONResponse({'total': len(result), 'items': result})


@app.get("/api/stats")
async def api_stats():
    conn = get_db()
    stats = {
        'monitored': len(MASTER),
        'snapshots': conn.execute("SELECT COUNT(DISTINCT muni_code) FROM snapshots").fetchone()[0],
        'total_items': conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0],
        'national': conn.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code = 'NATIONAL'").fetchone()[0],
        'changes_30d': conn.execute("SELECT COUNT(*) FROM diffs WHERE detected_at > datetime('now', '-30 days')").fetchone()[0],
    }
    return JSONResponse(stats)




@app.get("/api/quality")
async def api_quality():
    """Quality metrics for the intelligence system."""
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    with_dl = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline IS NOT NULL AND deadline != ''").fetchone()[0]
    with_amt = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE (amount IS NOT NULL AND amount != '') OR (amount_numeric IS NOT NULL AND amount_numeric > 0)").fetchone()[0]
    with_url = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE url IS NOT NULL AND url != '' AND length(url) > 10").fetchone()[0]
    open_bids = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline >= date('now')").fetchone()[0]
    awards = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE item_type='award'").fetchone()[0]
    munis = conn.execute("SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code != 'NATIONAL'").fetchone()[0]
    munis_10 = conn.execute("SELECT COUNT(*) FROM (SELECT muni_code FROM procurement_items WHERE muni_code != 'NATIONAL' GROUP BY muni_code HAVING COUNT(*) >= 10)").fetchone()[0]
    vendors = conn.execute("SELECT COUNT(DISTINCT method) FROM procurement_items WHERE item_type='award' AND method != ''").fetchone()[0]
    return {
        'total_items': total,
        'with_deadline': with_dl,
        'with_amount': with_amt,
        'with_url': with_url,
        'open_bids': open_bids,
        'awards': awards,
        'municipalities': munis,
        'municipalities_meaningful_10plus': munis_10,
        'vendors_from_awards': vendors,
        'quality_scores': {
            'deadline_pct': round(with_dl * 100 / total, 1) if total else 0,
            'amount_pct': round(with_amt * 100 / total, 1) if total else 0,
            'url_pct': round(with_url * 100 / total, 1) if total else 0,
        }
    }


@app.get("/api/export")
async def api_export(
    days: int = Query(30), pref: str = Query(None),
    q: str = Query(None), cat: str = Query(None),
    cols: str = Query(None),
):
    """CSV export of procurement items. cols=compact for minimal columns."""
    conn = get_db()
    where = "WHERE p.detected_at > datetime('now', ?)"
    params = [f'-{days} days']
    if pref:
        if pref == 'NATIONAL':
            where += " AND p.muni_code = 'NATIONAL'"
        else:
            where += " AND p.muni_code LIKE ?"
            params.append(f'{pref}%')
    if q:
        for t in q.split():
            where += " AND (p.title LIKE ? OR p.method LIKE ?)"
            params.extend([f'%{t}%', f'%{t}%'])

    items = conn.execute(f"""
        SELECT p.* FROM procurement_items p {where}
        ORDER BY p.detected_at DESC LIMIT 10000
    """, params).fetchall()

    compact = cols == 'compact'
    output = io.StringIO()
    writer = csv.writer(output)
    if compact:
        writer.writerow(['検出日', '発注者', '案件名', '〆切'])
    else:
        writer.writerow(['検出日', '都道府県', '発注者', '案件名', 'カテゴリ', '種別', '〆切', '金額', '方式', 'URL'])
    for item in items:
        mc = item['muni_code']
        name, pref_name = resolve_name(mc, item['raw_json'])
        cat_key, cat_label = classify_item(item['title'])
        if cat and cat != 'all' and cat_key != cat:
            continue
        if compact:
            writer.writerow([
                item['detected_at'][:10], name,
                item['title'], item['deadline'] or '',
            ])
        else:
            writer.writerow([
                item['detected_at'][:10], pref_name, name,
                item['title'], cat_label, TYPE_LABELS.get(item['item_type'], item['item_type'] or ''),
                item['deadline'] or '', item['amount'] or '',
                item['method'] or '', item['url'] or '',
            ])

    content = output.getvalue()
    # BOM for Excel compatibility
    return Response(
        content='\ufeff' + content,
        media_type='text/csv; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename="procurement_{days}d.csv"'}
    )


# ── Analytics Page ─────────────────────────────────────────────────────────

@app.get("/analytics", response_class=HTMLResponse)
async def analytics_page():
    t0 = time.perf_counter()
    conn = get_db()

    # Total items by category (use pre-computed column, not Python loop)
    cat_data = {}
    for row in conn.execute("SELECT COALESCE(category, 'other') as cat, COUNT(*) as cnt FROM procurement_items GROUP BY cat").fetchall():
        cat_data[row['cat']] = row['cnt']

    # Monthly trend
    monthly = conn.execute("""
        SELECT strftime('%Y-%m', detected_at) as month, COUNT(*) as cnt
        FROM procurement_items
        GROUP BY month ORDER BY month
    """).fetchall()

    # Top municipalities by item count
    top_munis = conn.execute("""
        SELECT muni_code, COUNT(*) as cnt
        FROM procurement_items WHERE muni_code != 'NATIONAL'
        GROUP BY muni_code ORDER BY cnt DESC LIMIT 20
    """).fetchall()

    # Prefecture breakdown
    pref_data = conn.execute("""
        SELECT SUBSTR(muni_code, 1, 2) as pref, COUNT(*) as cnt
        FROM procurement_items WHERE muni_code != 'NATIONAL'
        GROUP BY pref ORDER BY cnt DESC
    """).fetchall()

    # Upcoming deadlines (next 30 days)
    upcoming = conn.execute("""
        SELECT deadline, COUNT(*) as cnt
        FROM procurement_items
        WHERE deadline IS NOT NULL AND deadline >= date('now') AND deadline <= date('now', '+30 days')
        GROUP BY deadline ORDER BY deadline
    """).fetchall()

    # Department breakdown
    dept_data = conn.execute("""
        SELECT department, COUNT(*) as cnt
        FROM procurement_items WHERE department IS NOT NULL AND department != ''
        GROUP BY department ORDER BY cnt DESC LIMIT 15
    """).fetchall()

    # News stats
    news_total = 0
    news_today = 0
    try:
        news_total = conn.execute("SELECT COUNT(*) FROM muni_news").fetchone()[0]
        news_today = conn.execute(
            "SELECT COUNT(*) FROM muni_news WHERE fetched_at >= date('now')"
        ).fetchone()[0]
    except Exception:
        pass

    # Build Chart.js data
    months_labels = json.dumps([r['month'] for r in monthly])
    months_values = json.dumps([r['cnt'] for r in monthly])

    cat_labels = json.dumps([lbl for _, lbl, _ in CATEGORY_RULES] + ['その他'])
    cat_values = json.dumps([cat_data.get(k, 0) for k, _, _ in CATEGORY_RULES] + [cat_data.get('other', 0)])
    cat_colors = json.dumps([CATEGORY_COLORS.get(k, '#6b7280') for k, _, _ in CATEGORY_RULES] + ['#6b7280'])

    # Top municipalities table
    top_munis_html = ''
    for i, row in enumerate(top_munis):
        mc = row['muni_code']
        name, pref = resolve_name(mc)
        top_munis_html += f"""<tr>
          <td style="text-align:center;color:var(--muted)">{i+1}</td>
          <td><span class="pref-tag">{esc(pref)}</span>
              <a href="/municipality/{mc}">{esc(name)}</a></td>
          <td style="text-align:right;font-weight:600">{row['cnt']:,}</td>
        </tr>"""

    # Prefecture heatmap table
    pref_html = ''
    max_pref = pref_data[0]['cnt'] if pref_data else 1
    for row in pref_data:
        pname = PREF_NAMES.get(row['pref'], row['pref'])
        pct = row['cnt'] / max_pref * 100
        # Iter48: heatmap coloring
        if pct > 66:
            bar_color = '#1e40af'
        elif pct > 33:
            bar_color = '#3b82f6'
        else:
            bar_color = '#93c5fd'
        pref_html += f"""<a href="/items?pref={row['pref']}&days=365" style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.25rem;text-decoration:none;color:inherit;border-radius:4px;padding:1px 0;transition:background 0.15s" onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='transparent'">
          <span style="width:60px;font-size:0.8rem;text-align:right">{pname}</span>
          <div style="flex:1;background:#f1f5f9;border-radius:4px;height:18px;overflow:hidden">
            <div style="width:{pct:.0f}%;background:{bar_color};height:100%;border-radius:4px"></div>
          </div>
          <span style="width:50px;font-size:0.8rem;font-weight:600">{row['cnt']:,}</span>
        </a>"""

    # Deadline calendar
    deadline_html = ''
    for row in upcoming[:15]:
        deadline_html += f"""<div style="display:flex;justify-content:space-between;padding:0.2rem 0;
                              border-bottom:1px solid #f1f5f9;font-size:0.85rem">
          <span>{row['deadline']}</span><span style="font-weight:600">{row['cnt']}件</span>
        </div>"""

    # Department breakdown
    dept_html = ''
    for row in dept_data:
        dept_html += f"""<div style="display:flex;justify-content:space-between;padding:0.2rem 0;
                          border-bottom:1px solid #f1f5f9;font-size:0.85rem">
          <span>{esc(row['department'])}</span><span style="font-weight:600">{row['cnt']:,}</span>
        </div>"""

    elapsed = time.perf_counter() - t0

    content = f"""
    <div class="section">
      <div class="section-header">
        <h2>分析ダッシュボード</h2>
      </div>

      <div class="kpi-row">
        <div class="kpi kpi-accent">
          <div class="kpi-label">総案件数</div>
          <div class="kpi-value">{sum(cat_data.values()):,}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">カテゴリ数</div>
          <div class="kpi-value">{len([v for v in cat_data.values() if v > 0])}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">ニュース記事</div>
          <div class="kpi-value">{news_total:,}</div>
          <div class="kpi-sub">本日: {news_today:,}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">30日内〆切</div>
          <div class="kpi-value">{sum(r['cnt'] for r in upcoming):,}</div>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem">
        <div class="tbl-wrap" style="padding:1rem">
          <div style="font-weight:700;margin-bottom:0.8rem">月別案件推移</div>
          <canvas id="trendChart" height="200"></canvas>
        </div>
        <div class="tbl-wrap" style="padding:1rem">
          <div style="font-weight:700;margin-bottom:0.8rem">カテゴリ別分布</div>
          <canvas id="catChart" height="200"></canvas>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem">
        <div class="tbl-wrap" style="padding:1rem">
          <div style="font-weight:700;margin-bottom:0.8rem">アクティブ自治体 TOP 20</div>
          <table>
            <thead><tr><th style="width:40px">#</th><th>自治体</th><th style="text-align:right">件数</th></tr></thead>
            <tbody>{top_munis_html}</tbody>
          </table>
        </div>
        <div class="tbl-wrap" style="padding:1rem">
          <div style="font-weight:700;margin-bottom:0.8rem">都道府県別件数</div>
          <div style="max-height:400px;overflow-y:auto">{pref_html}</div>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem">
        <div class="tbl-wrap" style="padding:1rem">
          <div style="font-weight:700;margin-bottom:0.8rem">〆切カレンダー (30日)</div>
          {deadline_html if deadline_html else '<div class="empty"><p>今後30日以内の〆切なし</p></div>'}
        </div>
        <div class="tbl-wrap" style="padding:1rem">
          <div style="font-weight:700;margin-bottom:0.8rem">発注部署別 TOP 15</div>
          {dept_html if dept_html else '<div class="empty"><p>部署データなし</p></div>'}
        </div>
      </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
    <script>
    new Chart(document.getElementById('trendChart'), {{
      type: 'line',
      data: {{
        labels: {months_labels},
        datasets: [{{
          label: '案件数',
          data: {months_values},
          borderColor: '#3b82f6',
          backgroundColor: 'rgba(59,130,246,0.1)',
          fill: true, tension: 0.3, pointRadius: 2
        }}]
      }},
      options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
                 scales: {{ y: {{ beginAtZero: true }} }} }}
    }});
    // Iter17: clickable category chart
    const catKeys = {json.dumps([k for k, _, _ in CATEGORY_RULES] + ['other'])};
    const catChart = new Chart(document.getElementById('catChart'), {{
      type: 'doughnut',
      data: {{
        labels: {cat_labels},
        datasets: [{{ data: {cat_values}, backgroundColor: {cat_colors} }}]
      }},
      options: {{
        responsive: true,
        plugins: {{ legend: {{ position: 'right', labels: {{ font: {{ size: 11 }} }} }} }},
        onClick: function(e, elements) {{
          if(elements.length>0) {{
            const idx = elements[0].index;
            window.location.href = '/items?cat=' + catKeys[idx] + '&days=365';
          }}
        }}
      }}
    }});
    document.getElementById('catChart').style.cursor = 'pointer';
    </script>
    """

    return layout("分析", content, active='analytics', timing=elapsed)


# ── New API Endpoints ──────────────────────────────────────────────────────

@app.get("/api/health")
async def api_health():
    """Health check endpoint."""
    try:
        conn = get_db()
        n = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
        return JSONResponse({
            'status': 'ok',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'items': n,
            'municipalities': len(MASTER),
        })
    except Exception as e:
        return JSONResponse({'status': 'error', 'error': str(e)}, status_code=500)


@app.get("/api/scheduler/status")
async def api_scheduler_status():
    """Return scheduler job status."""
    return JSONResponse({
        'scheduler_enabled': HAS_SCHEDULER,
        'jobs': _scheduler_jobs,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    })


@app.get("/api/news/{muni_code}")
async def api_news(muni_code: str, limit: int = Query(20, ge=1, le=100)):
    """Get news articles for a municipality."""
    conn = get_db()
    try:
        articles = conn.execute("""
            SELECT * FROM muni_news
            WHERE muni_code = ?
            ORDER BY published_at DESC LIMIT ?
        """, (muni_code, limit)).fetchall()
        result = [dict(r) for r in articles]
        return JSONResponse({'muni_code': muni_code, 'total': len(result), 'articles': result})
    except Exception:
        return JSONResponse({'muni_code': muni_code, 'total': 0, 'articles': []})


@app.get("/api/v2/items")
async def api_v2_items(
    days: int = 30, pref: str = None, q: str = None,
    cat: str = None, dept: str = None, limit: int = 100, offset: int = 0
):
    """Enhanced items API with department and category fields."""
    conn = get_db()
    where = "WHERE p.detected_at > datetime('now', ?)"
    params = [f'-{days} days']
    if pref:
        if pref == 'NATIONAL':
            where += " AND p.muni_code = 'NATIONAL'"
        else:
            where += " AND p.muni_code LIKE ?"
            params.append(f'{pref}%')
    if q:
        for t in q.split():
            where += " AND (p.title LIKE ? OR p.method LIKE ?)"
            params.extend([f'%{t}%', f'%{t}%'])
    if cat:
        where += " AND p.category = ?"
        params.append(cat)
    if dept:
        where += " AND p.department LIKE ?"
        params.append(f'%{dept}%')

    items = conn.execute(f"""
        SELECT p.* FROM procurement_items p {where}
        ORDER BY p.detected_at DESC LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchall()

    result = []
    for item in items:
        mc = item['muni_code']
        name, pref_name = resolve_name(mc, item['raw_json'])
        cat_key, cat_label = classify_item(item['title'])
        result.append({
            'muni_code': mc, 'muni_name': name, 'prefecture': pref_name,
            'title': item['title'], 'type': item['item_type'],
            'category': item['category'] or cat_label,
            'department': item['department'],
            'division': item['division'],
            'budget_range': item['budget_range'],
            'deadline': item['deadline'],
            'amount': item['amount'], 'method': item['method'],
            'url': item['url'], 'detected_at': item['detected_at'],
        })
    return JSONResponse({'total': len(result), 'items': result})


@app.get("/api/analytics/summary")
async def api_analytics_summary():
    """Analytics summary API."""
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    national = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code = 'NATIONAL'").fetchone()[0]
    munis_with_items = conn.execute(
        "SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code != 'NATIONAL'"
    ).fetchone()[0]

    cats = {}
    for row in conn.execute("SELECT title FROM procurement_items").fetchall():
        k, _ = classify_item(row['title'])
        cats[k] = cats.get(k, 0) + 1

    return JSONResponse({
        'total_items': total, 'national': national,
        'municipal': total - national, 'municipalities_with_items': munis_with_items,
        'categories': cats,
    })


@app.get("/api/analytics/trends")
async def api_analytics_trends():
    """Monthly trend data API."""
    conn = get_db()
    rows = conn.execute("""
        SELECT strftime('%Y-%m', detected_at) as month, COUNT(*) as cnt
        FROM procurement_items GROUP BY month ORDER BY month
    """).fetchall()
    return JSONResponse({'months': [{'month': r['month'], 'count': r['cnt']} for r in rows]})


# ── Pipeline Management (Iter 1-3) ────────────────────────────────────────

PIPELINE_STATUSES = [
    ('discovered', '発見', '#6b7280', '新着案件'),
    ('reviewing', '検討中', '#2563eb', '入札可否を検討中'),
    ('preparing', '準備中', '#d97706', '入札書類を準備中'),
    ('submitted', '提出済', '#7c3aed', '入札書類を提出済み'),
    ('waiting', '結果待ち', '#0891b2', '結果発表を待機中'),
    ('won', '落札', '#16a34a', '落札しました'),
    ('lost', '不落札', '#dc2626', '不落札でした'),
    ('passed', '見送り', '#9ca3af', '入札を見送りました'),
]
PIPELINE_STATUS_MAP = {s[0]: s for s in PIPELINE_STATUSES}
PRIORITY_LABELS = {'low': '低', 'normal': '通常', 'high': '高', 'critical': '緊急'}


@app.get("/pipeline", response_class=HTMLResponse)
async def pipeline_page():
    """Kanban-style pipeline management page — 11-status bid_projects based."""
    t0 = time.perf_counter()
    conn = get_db()

    # Get bid projects grouped by status
    pipeline_items = {}
    for status, label, color, desc in bid_engine.BID_STATUSES:
        if status in ('archived', 'abandoned'):
            continue
        rows = conn.execute("""
            SELECT bp.*, pi.title, pi.muni_code, pi.deadline, pi.category, pi.raw_json,
                   bp.adjusted_total_ev, bp.win_probability
            FROM bid_projects bp
            JOIN procurement_items pi ON bp.item_id = pi.item_id
            WHERE bp.status = ?
            ORDER BY
                CASE bp.priority
                    WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                    WHEN 'normal' THEN 2 ELSE 3 END,
                pi.deadline ASC NULLS LAST
            LIMIT 50
        """, (status,)).fetchall()
        pipeline_items[status] = [dict(r) for r in rows]

    # Archive counts
    archive_counts = {}
    for row in conn.execute("""
        SELECT status, COUNT(*) as cnt FROM bid_projects
        WHERE status IN ('archived', 'abandoned')
        GROUP BY status
    """).fetchall():
        archive_counts[row['status']] = row['cnt']

    total_in_pipeline = sum(len(v) for v in pipeline_items.values())

    # Build kanban columns
    columns_html = ''
    for status, label, color, desc in bid_engine.BID_STATUSES:
        if status in ('archived', 'abandoned'):
            continue
        items = pipeline_items.get(status, [])
        cards = ''
        for item in items:
            name, pref = resolve_name(item['muni_code'], item.get('raw_json'))
            dl = deadline_info(item.get('deadline'))
            dl_badge = f'<span class="dl-badge {dl["cls"]}" style="font-size:0.65rem">{dl["label"]}</span>' if dl['label'] else ''
            pri_cls = f'border-left:3px solid {"#dc2626" if item.get("priority") == "critical" else "#d97706" if item.get("priority") == "high" else "transparent"}'
            cat_key = item.get('category') or classify_item(item.get('title', ''))[0]
            cat_label = dict((k, l) for k, l, _ in CATEGORY_RULES).get(cat_key, 'その他')
            go_badge = ''
            if item.get('go_nogo') == 'go':
                go_badge = '<span style="color:var(--success);font-weight:700;font-size:0.7rem">GO</span>'
            elif item.get('go_nogo') == 'nogo':
                go_badge = '<span style="color:var(--danger);font-weight:700;font-size:0.7rem">NO-GO</span>'

            cards += f"""<div class="pipe-card" style="{pri_cls}">
              <div style="display:flex;justify-content:space-between;align-items:start;margin-bottom:0.3rem">
                <span class="pref-tag" style="font-size:0.6rem">{esc(pref)}</span>
                <div style="display:flex;gap:0.3rem;align-items:center">{go_badge}{dl_badge}</div>
              </div>
              <div style="font-weight:600;font-size:0.82rem;margin-bottom:0.2rem;line-height:1.3">
                <a href="/bid-workbench/{item['project_id']}" style="color:inherit;text-decoration:none">{esc((item.get('title') or '(無題)')[:60])}</a>
              </div>
              <div style="display:flex;justify-content:space-between;align-items:center;font-size:0.72rem;color:var(--muted)">
                <span>{esc(name[:15])}</span>
                <span class="badge cat-{cat_key}" style="font-size:0.6rem">{cat_label}</span>
              </div>
              {f'<div style="font-size:0.7rem;color:var(--muted);margin-top:0.2rem">担当: {esc(item["assignee"])}</div>' if item.get('assignee') else ''}
              {f'<div style="font-size:0.68rem;margin-top:0.3rem;color:#1e40af;font-weight:600">EV: \u00a5{item["adjusted_total_ev"]:,.0f}</div>' if item.get('adjusted_total_ev') else ''}
              <div style="margin-top:0.4rem">
                <a href="/bid-workbench/{item['project_id']}" class="btn btn-outline btn-sm" style="font-size:0.65rem;padding:0.1rem 0.4rem">ワークベンチ</a>
              </div>
            </div>"""

        columns_html += f"""<div class="pipe-col">
          <div class="pipe-col-header" style="border-top:3px solid {color}">
            <span style="font-weight:700;font-size:0.85rem">{label}</span>
            <span class="pipe-count">{len(items)}</span>
          </div>
          <div class="pipe-cards">{cards if cards else '<div style="text-align:center;padding:1rem;color:var(--muted);font-size:0.8rem">案件なし</div>'}</div>
        </div>"""

    archive_html = ''
    for status in ('archived', 'abandoned'):
        info = bid_engine.BID_STATUS_MAP.get(status, ('','','#9ca3af',''))
        cnt = archive_counts.get(status, 0)
        archive_html += f'<span style="display:inline-flex;align-items:center;gap:0.3rem;font-size:0.85rem;margin-right:1rem"><span style="width:10px;height:10px;background:{info[2]};border-radius:50%;display:inline-block"></span>{info[1]}: <strong>{cnt}</strong></span>'

    elapsed = time.perf_counter() - t0

    content = f"""
    <div class="section">
      <div class="section-header">
        <h2>入札パイプライン</h2>
        <div class="actions">
          <span style="font-size:0.8rem;color:var(--muted)">管理中: {total_in_pipeline}件</span>
          <a href="/items" class="btn btn-outline btn-sm">+ 案件追加</a>
        </div>
      </div>

      <div class="kpi-row" style="margin-bottom:1rem">
        <div class="kpi kpi-accent">
          <div class="kpi-label">パイプライン合計</div>
          <div class="kpi-value">{total_in_pipeline}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">質問準備</div>
          <div class="kpi-value" style="color:#2563eb">{len(pipeline_items.get('questions_draft', [])) + len(pipeline_items.get('questions_sent', []))}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">提案書作成</div>
          <div class="kpi-value" style="color:#d97706">{len(pipeline_items.get('proposal_draft', [])) + len(pipeline_items.get('proposal_review', []))}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">提出済</div>
          <div class="kpi-value" style="color:#7c3aed">{len(pipeline_items.get('submitted', []))}</div>
        </div>
      </div>

      <div class="pipe-board" style="overflow-x:auto">{columns_html}</div>

      <div style="margin-top:1rem;display:flex;gap:1.5rem;align-items:center">
        <span style="font-size:0.8rem;color:var(--muted);font-weight:600">アーカイブ:</span>
        {archive_html}
      </div>
    </div>
    """
    return layout("パイプライン", content, active='pipeline', timing=elapsed)


# ── Pipeline API (Iter 2) ────────────────────────────────────────────────

@app.get("/api/pipeline/{item_id}")
async def api_pipeline_get(item_id: int):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM item_pipeline WHERE item_id = ?", (item_id,)).fetchone()
        if row:
            return JSONResponse(dict(row))
        return JSONResponse({'status': 'discovered', 'priority': 'normal', 'go_nogo': 'pending'})
    except Exception:
        return JSONResponse({'status': 'discovered', 'priority': 'normal', 'go_nogo': 'pending'})


@app.put("/api/pipeline/{item_id}")
async def api_pipeline_update(item_id: int, request: Request):
    conn = get_db()
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({'error': 'Invalid JSON'}, status_code=400)

    # Ensure table exists
    try:
        conn.execute("SELECT 1 FROM item_pipeline LIMIT 1")
    except Exception:
        from monitor import init_db
        init_db(conn)

    existing = conn.execute("SELECT status FROM item_pipeline WHERE item_id = ?", (item_id,)).fetchone()
    old_status = existing['status'] if existing else None

    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    conn.execute("""
        INSERT INTO item_pipeline (item_id, status, priority, assignee, notes, go_nogo, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(item_id) DO UPDATE SET
            status=excluded.status, priority=excluded.priority,
            assignee=excluded.assignee, notes=excluded.notes,
            go_nogo=excluded.go_nogo, updated_at=excluded.updated_at
    """, (item_id, data.get('status', 'discovered'), data.get('priority', 'normal'),
          data.get('assignee', ''), data.get('notes', ''),
          data.get('go_nogo', 'pending'), now))

    # Record history
    new_status = data.get('status', 'discovered')
    if old_status != new_status:
        conn.execute("""
            INSERT INTO pipeline_history (item_id, old_status, new_status, note)
            VALUES (?, ?, ?, ?)
        """, (item_id, old_status, new_status, data.get('notes', '')))

    conn.commit()
    return JSONResponse({'ok': True})


@app.post("/api/pipeline/add/{item_id}")
async def api_pipeline_add(item_id: int):
    """Add item to pipeline (from items page)."""
    conn = get_db()
    try:
        conn.execute("SELECT 1 FROM item_pipeline LIMIT 1")
    except Exception:
        from monitor import init_db
        init_db(conn)

    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    try:
        conn.execute("""
            INSERT OR IGNORE INTO item_pipeline (item_id, status, priority, updated_at, created_at)
            VALUES (?, 'discovered', 'normal', ?, ?)
        """, (item_id, now, now))
        conn.commit()
        return JSONResponse({'ok': True, 'status': 'discovered'})
    except Exception as e:
        return JSONResponse({'error': str(e)}, status_code=500)


# ── Alert Rules API (Iter 4-5) ───────────────────────────────────────────

@app.get("/alerts", response_class=HTMLResponse)
async def alerts_page():
    """Alert rules management page."""
    t0 = time.perf_counter()
    conn = get_db()

    try:
        rules = conn.execute("SELECT * FROM alert_rules ORDER BY created_at DESC").fetchall()
        rules = [dict(r) for r in rules]
    except Exception:
        rules = []

    # Get unread alert matches
    try:
        unread = conn.execute("SELECT COUNT(*) FROM alert_matches WHERE seen = 0").fetchone()[0]
    except Exception:
        unread = 0

    rules_html = ''
    for rule in rules:
        conditions = json.loads(rule.get('conditions', '{}'))
        cond_parts = []
        if conditions.get('pref'):
            prefs = [PREF_NAMES.get(p, p) for p in conditions['pref']]
            cond_parts.append(f"地域: {', '.join(prefs)}")
        if conditions.get('cat'):
            cats = [dict((k, l) for k, l, _ in CATEGORY_RULES).get(c, c) for c in conditions['cat']]
            cond_parts.append(f"カテゴリ: {', '.join(cats)}")
        if conditions.get('keywords'):
            cond_parts.append(f"キーワード: {conditions['keywords']}")
        if conditions.get('min_amount'):
            cond_parts.append(f"金額下限: {conditions['min_amount']}")

        active_badge = '<span style="color:var(--success);font-size:0.7rem;font-weight:700">ON</span>' if rule['active'] else '<span style="color:var(--muted);font-size:0.7rem">OFF</span>'

        rules_html += f"""<div style="display:flex;justify-content:space-between;align-items:center;padding:0.6rem;border-bottom:1px solid var(--border)">
          <div>
            <div style="font-weight:600;font-size:0.9rem">{esc(rule['name'])} {active_badge}</div>
            <div style="font-size:0.8rem;color:var(--muted)">{' / '.join(cond_parts) if cond_parts else '条件なし'}</div>
          </div>
          <div style="display:flex;gap:0.3rem">
            <button class="btn btn-outline btn-sm" onclick="toggleAlertRule({rule['rule_id']},{1 if not rule['active'] else 0})">{('無効化' if rule['active'] else '有効化')}</button>
            <button class="btn btn-outline btn-sm" style="color:var(--danger)" onclick="deleteAlertRule({rule['rule_id']})">削除</button>
          </div>
        </div>"""

    elapsed = time.perf_counter() - t0

    pref_checkboxes = ''.join(
        f'<label style="display:inline-flex;align-items:center;gap:0.2rem;font-size:0.78rem;cursor:pointer"><input type="checkbox" name="pref" value="{k}" style="width:14px;height:14px">{v}</label>'
        for k, v in sorted(PREF_NAMES.items())
    )
    cat_checkboxes = ''.join(
        f'<label style="display:inline-flex;align-items:center;gap:0.2rem;font-size:0.78rem;cursor:pointer"><input type="checkbox" name="cat" value="{k}" style="width:14px;height:14px">{l}</label>'
        for k, l, _ in CATEGORY_RULES
    )

    content = f"""
    <div class="section">
      <div class="section-header">
        <h2>アラートルール</h2>
        <div class="actions">
          <span style="font-size:0.85rem;color:var(--muted)">未読: <strong style="color:var(--danger)">{unread}</strong></span>
          <button class="btn btn-primary btn-sm" onclick="document.getElementById('new-alert-form').style.display='block'">+ ルール追加</button>
        </div>
      </div>

      <div id="new-alert-form" class="tbl-wrap" style="display:none;padding:1rem;margin-bottom:1rem">
        <div style="font-weight:700;margin-bottom:0.5rem">新しいアラートルール</div>
        <form onsubmit="createAlertRule(event)">
          <div style="display:grid;gap:0.5rem">
            <div>
              <label style="font-size:0.78rem;font-weight:600">ルール名</label>
              <input name="name" type="text" required placeholder="例: 東京IT案件" style="width:100%;padding:0.4rem;border:1px solid var(--border);border-radius:6px">
            </div>
            <div>
              <label style="font-size:0.78rem;font-weight:600">都道府県</label>
              <div style="display:flex;gap:0.3rem;flex-wrap:wrap;max-height:120px;overflow-y:auto;border:1px solid var(--border);border-radius:6px;padding:0.4rem">{pref_checkboxes}</div>
            </div>
            <div>
              <label style="font-size:0.78rem;font-weight:600">カテゴリ</label>
              <div style="display:flex;gap:0.3rem;flex-wrap:wrap">{cat_checkboxes}</div>
            </div>
            <div>
              <label style="font-size:0.78rem;font-weight:600">キーワード</label>
              <input name="keywords" type="text" placeholder="スペース区切りで複数指定" style="width:100%;padding:0.4rem;border:1px solid var(--border);border-radius:6px">
            </div>
            <button type="submit" class="btn btn-primary">作成</button>
          </div>
        </form>
      </div>

      <div class="tbl-wrap">
        {rules_html if rules_html else '<div class="empty" style="padding:2rem"><p>アラートルールがありません</p><p style="font-size:0.8rem">「+ ルール追加」から条件を設定すると、新着案件がマッチした時に通知されます</p></div>'}
      </div>
    </div>

    <script>
    async function createAlertRule(e){{
      e.preventDefault();
      const f=e.target;
      const prefs=Array.from(f.querySelectorAll('input[name=pref]:checked')).map(i=>i.value);
      const cats=Array.from(f.querySelectorAll('input[name=cat]:checked')).map(i=>i.value);
      const resp=await fetch('/api/alerts',{{
        method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{
          name:f.querySelector('input[name=name]').value,
          conditions:{{pref:prefs,cat:cats,keywords:f.querySelector('input[name=keywords]').value}}
        }})
      }});
      if(resp.ok){{showToast('ルール作成しました');location.reload();}}
    }}
    async function toggleAlertRule(id,active){{
      await fetch('/api/alerts/'+id,{{method:'PUT',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{active:active}})}});
      location.reload();
    }}
    async function deleteAlertRule(id){{
      if(!confirm('このルールを削除しますか？'))return;
      await fetch('/api/alerts/'+id,{{method:'DELETE'}});
      location.reload();
    }}
    </script>
    """
    return layout("アラート", content, active='alerts', timing=elapsed)


@app.post("/api/alerts")
async def api_create_alert(request: Request):
    conn = get_db()
    try:
        conn.execute("SELECT 1 FROM alert_rules LIMIT 1")
    except Exception:
        from monitor import init_db
        init_db(conn)
    data = await request.json()
    conn.execute("""
        INSERT INTO alert_rules (user_id, name, conditions)
        VALUES ('default', ?, ?)
    """, (data.get('name', 'Unnamed'), json.dumps(data.get('conditions', {}), ensure_ascii=False)))
    conn.commit()
    return JSONResponse({'ok': True})


@app.put("/api/alerts/{rule_id}")
async def api_update_alert(rule_id: int, request: Request):
    conn = get_db()
    data = await request.json()
    conn.execute("UPDATE alert_rules SET active = ? WHERE rule_id = ?", (data.get('active', 1), rule_id))
    conn.commit()
    return JSONResponse({'ok': True})


@app.delete("/api/alerts/{rule_id}")
async def api_delete_alert(rule_id: int):
    conn = get_db()
    conn.execute("DELETE FROM alert_rules WHERE rule_id = ?", (rule_id,))
    conn.execute("DELETE FROM alert_matches WHERE rule_id = ?", (rule_id,))
    conn.commit()
    return JSONResponse({'ok': True})


# ── Similar Items API (Iter 6) ───────────────────────────────────────────

def find_similar_items(conn, item_id: int, limit: int = 5) -> list:
    """Find similar procurement items based on title keywords and category."""
    item = conn.execute("SELECT * FROM procurement_items WHERE item_id = ?", (item_id,)).fetchone()
    if not item:
        return []

    title = item['title'] or ''
    cat = item['category'] or classify_item(title)[0]
    muni = item['muni_code']

    # Extract key terms (remove common words)
    stop_words = {'の', 'に', 'は', 'を', 'と', 'が', 'で', 'て', 'な', 'し', 'れ', 'さ', 'ある', 'いる',
                  'する', 'こと', 'これ', 'それ', 'もの', 'ため', 'から', 'まで', 'より', 'について',
                  '及び', '等', '年度', '令和', '業務', '委託', '事業', '契約'}
    terms = [t for t in re.findall(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]{2,}', title) if t not in stop_words]

    if not terms:
        return []

    # Search by category + keyword overlap
    like_clauses = ' OR '.join(['title LIKE ?'] * min(len(terms), 5))
    params = [f'%{t}%' for t in terms[:5]]

    results = conn.execute(f"""
        SELECT item_id, muni_code, title, category, deadline, detected_at, amount
        FROM procurement_items
        WHERE item_id != ? AND ({like_clauses})
        ORDER BY detected_at DESC LIMIT ?
    """, [item_id] + params + [limit * 3]).fetchall()

    # Score results
    scored = []
    for r in results:
        score = 0
        r_title = r['title'] or ''
        for t in terms:
            if t in r_title:
                score += 1
        r_cat = r['category'] or classify_item(r_title)[0]
        if r_cat == cat:
            score += 2
        if r['muni_code'] == muni:
            score += 1
        if score > 0:
            scored.append({**dict(r), 'score': score})

    scored.sort(key=lambda x: x['score'], reverse=True)
    return scored[:limit]


@app.get("/api/similar/{item_id}")
async def api_similar_items(item_id: int, limit: int = Query(5)):
    conn = get_db()
    similar = find_similar_items(conn, item_id, limit)
    return JSONResponse({'item_id': item_id, 'similar': similar})


# ── GO/NO-GO Scorecard (Iter 7) ──────────────────────────────────────────

def compute_gonogo_score(conn, item_id: int) -> dict:
    """Compute GO/NO-GO decision support scorecard."""
    row = conn.execute("SELECT * FROM procurement_items WHERE item_id = ?", (item_id,)).fetchone()
    if not row:
        return {'total': 0, 'factors': [], 'pct': 0}
    item = dict(row)

    factors = []
    title = item.get('title') or ''
    cat_key = item.get('category') or classify_item(title)[0]
    dl = deadline_info(item.get('deadline'))

    # Factor 1: Deadline feasibility
    if dl['days'] is not None:
        if dl['days'] < 0:
            factors.append({'name': '締切', 'score': 0, 'max': 20, 'reason': '締切超過'})
        elif dl['days'] <= 3:
            factors.append({'name': '締切', 'score': 5, 'max': 20, 'reason': f'残{dl["days"]}日 — 準備困難'})
        elif dl['days'] <= 14:
            factors.append({'name': '締切', 'score': 15, 'max': 20, 'reason': f'残{dl["days"]}日 — 急いで準備'})
        else:
            factors.append({'name': '締切', 'score': 20, 'max': 20, 'reason': f'残{dl["days"]}日 — 十分な準備期間'})
    else:
        factors.append({'name': '締切', 'score': 10, 'max': 20, 'reason': '締切未定'})

    # Factor 2: Past experience (same category in same municipality)
    same_cat_count = conn.execute("""
        SELECT COUNT(*) FROM procurement_items
        WHERE muni_code = ? AND category = ? AND item_id != ?
    """, (item.get('muni_code'), cat_key, item_id)).fetchone()[0]
    if same_cat_count >= 5:
        factors.append({'name': '実績', 'score': 20, 'max': 20, 'reason': f'同自治体で同カテゴリ{same_cat_count}件の実績'})
    elif same_cat_count >= 1:
        factors.append({'name': '実績', 'score': 15, 'max': 20, 'reason': f'同自治体で同カテゴリ{same_cat_count}件'})
    else:
        factors.append({'name': '実績', 'score': 5, 'max': 20, 'reason': '同自治体での実績なし'})

    # Factor 3: Competition level
    total_items = conn.execute("""
        SELECT COUNT(*) FROM procurement_items
        WHERE muni_code = ? AND detected_at > datetime('now', '-90 days')
    """, (item.get('muni_code'),)).fetchone()[0]
    if total_items <= 5:
        factors.append({'name': '競争度', 'score': 18, 'max': 20, 'reason': '発注少 = 低競争'})
    elif total_items <= 20:
        factors.append({'name': '競争度', 'score': 12, 'max': 20, 'reason': f'90日間で{total_items}件'})
    else:
        factors.append({'name': '競争度', 'score': 7, 'max': 20, 'reason': f'90日間で{total_items}件 = 高競争'})

    # Factor 4: Method type
    method = item.get('method') or ''
    if 'プロポーザル' in method or '総合評価' in method:
        factors.append({'name': '契約方式', 'score': 18, 'max': 20, 'reason': '技術評価重視 = 品質で勝負可能'})
    elif '一般競争' in method:
        factors.append({'name': '契約方式', 'score': 12, 'max': 20, 'reason': '一般競争 = 価格重視'})
    elif '指名' in method:
        factors.append({'name': '契約方式', 'score': 8, 'max': 20, 'reason': '指名競争 = 参加要指名'})
    else:
        factors.append({'name': '契約方式', 'score': 10, 'max': 20, 'reason': '方式不明'})

    # Factor 5: Strategic value
    factors.append({'name': '戦略性', 'score': 10, 'max': 20, 'reason': '手動評価を推奨'})

    total = sum(f['score'] for f in factors)
    max_total = sum(f['max'] for f in factors)

    return {'total': total, 'max': max_total, 'pct': round(total / max_total * 100) if max_total else 0, 'factors': factors}


@app.get("/api/scorecard/{item_id}")
async def api_scorecard(item_id: int):
    conn = get_db()
    score = compute_gonogo_score(conn, item_id)
    return JSONResponse(score)


# ── Market Intelligence (Iter 8-9) ───────────────────────────────────────

@app.get("/market", response_class=HTMLResponse)
async def market_page():
    """Market intelligence dashboard."""
    t0 = time.perf_counter()
    conn = get_db()

    # Market size by category × prefecture
    cat_pref_data = conn.execute("""
        SELECT COALESCE(category, 'other') as cat,
               SUBSTR(muni_code, 1, 2) as pref,
               COUNT(*) as cnt
        FROM procurement_items WHERE muni_code != 'NATIONAL'
        GROUP BY cat, pref
    """).fetchall()

    # Build category × prefecture matrix
    categories = [k for k, _, _ in CATEGORY_RULES] + ['other']
    prefectures = sorted(PREF_NAMES.keys())
    matrix = {c: {p: 0 for p in prefectures} for c in categories}
    for row in cat_pref_data:
        if row['cat'] in matrix and row['pref'] in prefectures:
            matrix[row['cat']][row['pref']] = row['cnt']

    # Seasonal analysis (month-over-month)
    seasonal = conn.execute("""
        SELECT strftime('%m', detected_at) as month, COUNT(*) as cnt
        FROM procurement_items
        GROUP BY month ORDER BY month
    """).fetchall()
    seasonal_data = {r['month']: r['cnt'] for r in seasonal}

    # Category growth (this quarter vs last quarter)
    growth_data = []
    for key, label, _ in CATEGORY_RULES:
        current = conn.execute("""
            SELECT COUNT(*) FROM procurement_items
            WHERE category = ? AND detected_at > datetime('now', '-90 days')
        """, (key,)).fetchone()[0]
        previous = conn.execute("""
            SELECT COUNT(*) FROM procurement_items
            WHERE category = ? AND detected_at > datetime('now', '-180 days')
                AND detected_at <= datetime('now', '-90 days')
        """, (key,)).fetchone()[0]
        growth = ((current - previous) / previous * 100) if previous > 0 else 0
        growth_data.append({'key': key, 'label': label, 'current': current, 'previous': previous, 'growth': growth})

    # Top growing categories
    growth_data.sort(key=lambda x: x['growth'], reverse=True)

    # Build heatmap HTML (category × top 10 prefectures)
    top_prefs = sorted(prefectures, key=lambda p: sum(matrix[c][p] for c in categories), reverse=True)[:15]
    heatmap_html = '<table style="font-size:0.75rem;width:100%"><thead><tr><th></th>'
    for p in top_prefs:
        heatmap_html += f'<th style="text-align:center;writing-mode:vertical-rl;height:50px;font-size:0.7rem">{PREF_NAMES.get(p, p)}</th>'
    heatmap_html += '</tr></thead><tbody>'
    global_max = max((matrix[c][p] for c in categories for p in top_prefs), default=1) or 1
    for key, label, _ in CATEGORY_RULES:
        heatmap_html += f'<tr><td style="font-weight:600;white-space:nowrap">{label}</td>'
        for p in top_prefs:
            v = matrix[key][p]
            intensity = min(v / global_max, 1)
            bg = f'rgba(30,64,175,{intensity:.2f})' if v > 0 else 'transparent'
            color = 'white' if intensity > 0.5 else 'var(--text)'
            heatmap_html += f'<td style="text-align:center;background:{bg};color:{color};padding:0.2rem;min-width:35px">{v if v else ""}</td>'
        heatmap_html += '</tr>'
    heatmap_html += '</tbody></table>'

    # Growth table
    growth_html = ''
    for g in growth_data:
        arrow = f'<span style="color:var(--success)">&#9650; {g["growth"]:.0f}%</span>' if g['growth'] > 5 else \
                f'<span style="color:var(--danger)">&#9660; {abs(g["growth"]):.0f}%</span>' if g['growth'] < -5 else \
                '<span style="color:var(--muted)">&#9644;</span>'
        growth_html += f"""<div style="display:flex;justify-content:space-between;align-items:center;padding:0.3rem 0;border-bottom:1px solid var(--border);font-size:0.85rem">
          <span class="badge cat-{g['key']}" style="min-width:80px;text-align:center">{g['label']}</span>
          <span>{g['current']:,}件</span>
          <span>{arrow}</span>
        </div>"""

    # Seasonal chart data
    months_labels = json.dumps([f'{int(m)}月' for m in sorted(seasonal_data.keys())])
    months_values = json.dumps([seasonal_data.get(m, 0) for m in sorted(seasonal_data.keys())])

    elapsed = time.perf_counter() - t0

    content = f"""
    <div class="section">
      <div class="section-header">
        <h2>マーケットインテリジェンス</h2>
      </div>

      <div class="kpi-row">
        <div class="kpi kpi-accent">
          <div class="kpi-label">総市場規模 (案件数)</div>
          <div class="kpi-value">{sum(sum(matrix[c].values()) for c in categories):,}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">成長カテゴリ</div>
          <div class="kpi-value" style="font-size:1.2rem;color:var(--success)">{growth_data[0]['label'] if growth_data else '-'}</div>
          <div class="kpi-sub">{f'+{growth_data[0]["growth"]:.0f}% (90日)' if growth_data and growth_data[0]['growth'] > 0 else ''}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">最活発な地域</div>
          <div class="kpi-value" style="font-size:1.2rem">{PREF_NAMES.get(top_prefs[0], '') if top_prefs else '-'}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">カバー地域</div>
          <div class="kpi-value">{len([p for p in prefectures if any(matrix[c][p] > 0 for c in categories)])}/47</div>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:2fr 1fr;gap:1rem;margin-bottom:1.5rem">
        <div class="tbl-wrap" style="padding:1rem;overflow-x:auto">
          <div style="font-weight:700;margin-bottom:0.8rem">カテゴリ × 都道府県 ヒートマップ</div>
          {heatmap_html}
        </div>
        <div class="tbl-wrap" style="padding:1rem">
          <div style="font-weight:700;margin-bottom:0.8rem">カテゴリ成長率 (90日前比)</div>
          {growth_html}
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem">
        <div class="tbl-wrap" style="padding:1rem">
          <div style="font-weight:700;margin-bottom:0.8rem">月別発注パターン (季節性)</div>
          <canvas id="seasonChart" height="200"></canvas>
        </div>
        <div class="tbl-wrap" style="padding:1rem">
          <div style="font-weight:700;margin-bottom:0.8rem">市場概況</div>
          <div style="font-size:0.85rem;line-height:1.8;color:var(--text)">
            <p>公共調達市場は<strong>{sum(sum(matrix[c].values()) for c in categories):,}件</strong>の案件を保有。</p>
            <p>最も活発なカテゴリは「<strong>{growth_data[0]['label'] if growth_data else '-'}</strong>」で、
               直近90日間で<strong>{growth_data[0]['current']:,}件</strong>の案件が公開されています。</p>
            <p>地域別では「<strong>{PREF_NAMES.get(top_prefs[0], '')}</strong>」が最多。
               上位5地域で全体の{f'{sum(sum(matrix[c][p] for c in categories) for p in top_prefs[:5]) / max(sum(sum(matrix[c].values()) for c in categories), 1) * 100:.0f}'}%を占めます。</p>
          </div>
        </div>
      </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
    <script>
    new Chart(document.getElementById('seasonChart'), {{
      type: 'bar',
      data: {{
        labels: {months_labels},
        datasets: [{{ label: '案件数', data: {months_values},
          backgroundColor: 'rgba(59,130,246,0.6)', borderColor: '#3b82f6', borderWidth: 1 }}]
      }},
      options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
                 scales: {{ y: {{ beginAtZero: true }} }} }}
    }});
    </script>
    """
    return layout("マーケット", content, active='market', timing=elapsed)


# ══════════════════════════════════════════════════════════════════════════
# Phase 2: Intelligence Layer (Iter 11-20)
# ══════════════════════════════════════════════════════════════════════════

# ── Iter 11: Vendor Intelligence ──────────────────────────────────────────

def get_vendor_landscape(conn, muni_code: str = None, category: str = None) -> dict:
    """Analyze vendor concentration and competitive landscape."""
    where = "WHERE 1=1"
    params = []
    if muni_code:
        where += " AND p.muni_code = ?"
        params.append(muni_code)
    if category:
        where += " AND p.category = ?"
        params.append(category)

    # Top vendors by mention count
    rows = conn.execute(f"""
        SELECT p.muni_code, p.title, p.method, p.amount
        FROM procurement_items p {where}
        ORDER BY p.detected_at DESC LIMIT 500
    """, params).fetchall()

    vendor_counts = {}
    for r in rows:
        method = r['method'] or ''
        # Extract vendor patterns from method text
        for pat in re.findall(r'(?:株式会社|有限会社|合同会社)[\u4e00-\u9fff\u30a0-\u30ff]+', method):
            vendor_counts[pat] = vendor_counts.get(pat, 0) + 1

    sorted_vendors = sorted(vendor_counts.items(), key=lambda x: -x[1])[:20]

    # HHI calculation
    total = sum(c for _, c in sorted_vendors) or 1
    hhi = sum((c / total) ** 2 for _, c in sorted_vendors)

    return {
        'top_vendors': [{'name': n, 'count': c, 'share': round(c / total * 100, 1)} for n, c in sorted_vendors],
        'hhi': round(hhi, 4),
        'concentration': 'high' if hhi > 0.25 else 'medium' if hhi > 0.15 else 'low',
        'total_mentions': total,
    }


@app.get("/api/vendors")
async def api_vendor_landscape(muni_code: str = None, category: str = None):
    conn = get_db()
    data = get_vendor_landscape(conn, muni_code, category)
    return JSONResponse(data)


# ── Iter 12: Procurement Cycle Detection ──────────────────────────────────

def detect_procurement_cycles(conn, muni_code: str = None, category: str = None) -> dict:
    """Detect seasonal procurement patterns."""
    where = "WHERE detected_at IS NOT NULL"
    params = []
    if muni_code:
        where += " AND muni_code = ?"
        params.append(muni_code)
    if category:
        where += " AND category = ?"
        params.append(category)

    rows = conn.execute(f"""
        SELECT strftime('%m', detected_at) as month, COUNT(*) as cnt
        FROM procurement_items {where}
        GROUP BY month ORDER BY month
    """, params).fetchall()

    monthly = {int(r['month']): r['cnt'] for r in rows}
    total = sum(monthly.values()) or 1
    avg = total / 12

    # Detect peak months (>1.3x average)
    peaks = [m for m, c in monthly.items() if c > avg * 1.3]
    troughs = [m for m, c in monthly.items() if c < avg * 0.7]

    # Seasonality coefficient of variation
    import math
    variance = sum((monthly.get(m, 0) - avg) ** 2 for m in range(1, 13)) / 12
    cv = math.sqrt(variance) / avg if avg > 0 else 0

    return {
        'monthly': {m: monthly.get(m, 0) for m in range(1, 13)},
        'peaks': peaks,
        'troughs': troughs,
        'average': round(avg, 1),
        'seasonality_cv': round(cv, 3),
        'pattern': 'strong_seasonal' if cv > 0.5 else 'mild_seasonal' if cv > 0.2 else 'stable',
    }


@app.get("/api/cycles")
async def api_procurement_cycles(muni_code: str = None, category: str = None):
    conn = get_db()
    data = detect_procurement_cycles(conn, muni_code, category)
    return JSONResponse(data)


# ── Iter 13: Price Range Estimation ───────────────────────────────────────

def estimate_price_range(conn, category: str, muni_code: str = None) -> dict:
    """Estimate typical price ranges for a category."""
    where = "WHERE amount IS NOT NULL AND amount != '' AND category = ?"
    params = [category]
    if muni_code:
        where += " AND muni_code = ?"
        params.append(muni_code)

    rows = conn.execute(f"""
        SELECT amount FROM procurement_items {where}
        ORDER BY detected_at DESC LIMIT 200
    """, params).fetchall()

    amounts = []
    for r in rows:
        amt_str = str(r['amount'] or '')
        # Parse Japanese yen amounts with unit support
        val = None
        s = amt_str.replace(',', '').replace(' ', '').replace('　', '')
        # 億万パターン
        m_oku = re.search(r'([\d.]+)\s*億\s*([\d.]*)\s*万?', s)
        if m_oku:
            val = int(float(m_oku.group(1)) * 100_000_000 + (float(m_oku.group(2)) * 10_000 if m_oku.group(2) else 0))
        if val is None:
            m_man = re.search(r'([\d.]+)\s*万', s)
            if m_man:
                val = int(float(m_man.group(1)) * 10_000)
        if val is None:
            m_sen = re.search(r'([\d.]+)\s*千', s)
            if m_sen:
                val = int(float(m_sen.group(1)) * 1_000)
        if val is None:
            nums = re.findall(r'[\d]+', s)
            for n in nums:
                try:
                    v = int(n)
                    if v > 10000:
                        val = v
                        break
                except ValueError:
                    pass
        if val and val > 10000:
            amounts.append(val)

    if not amounts:
        return {'available': False, 'message': 'データ不足'}

    amounts.sort()
    n = len(amounts)
    return {
        'available': True,
        'count': n,
        'min': amounts[0],
        'max': amounts[-1],
        'median': amounts[n // 2],
        'p25': amounts[n // 4] if n >= 4 else amounts[0],
        'p75': amounts[3 * n // 4] if n >= 4 else amounts[-1],
        'avg': round(sum(amounts) / n),
        'display': f'{amounts[n // 4] / 10000:.0f}万〜{amounts[3 * n // 4] / 10000:.0f}万円' if n >= 4 else f'平均 {sum(amounts) / n / 10000:.0f}万円',
    }


@app.get("/api/price-range")
async def api_price_range(category: str, muni_code: str = None):
    conn = get_db()
    data = estimate_price_range(conn, category, muni_code)
    return JSONResponse(data)


# ── Iter 14: Bid Preparation Checklist ────────────────────────────────────

BID_CHECKLISTS = {
    'construction': [
        ('書類準備', ['入札参加資格審査申請書', '経営事項審査結果通知書', '建設業許可証の写し', '納税証明書', '技術者名簿']),
        ('技術提案', ['施工計画書', '安全衛生計画', '品質管理計画', '工程表', '配置技術者の資格証明']),
        ('見積・積算', ['数量調書の確認', '単価表の作成', '直接工事費の算出', '共通仮設費', '現場管理費', '一般管理費']),
    ],
    'it': [
        ('書類準備', ['入札参加資格確認書', '会社概要書', '財務諸表', '納税証明書', 'ISO/ISMS認証の写し']),
        ('技術提案', ['システム構成図', '開発体制図', 'プロジェクト計画書', '運用保守計画', 'セキュリティ対策書']),
        ('見積・積算', ['工数見積の根拠', 'ライセンス費用', 'ハードウェア費用', '保守費用', '教育研修費用']),
    ],
    'service': [
        ('書類準備', ['入札参加資格確認書', '会社概要書', '業務実績証明書', '納税証明書']),
        ('技術提案', ['業務実施体制', '業務スケジュール', '類似業務実績', '管理技術者の経歴書']),
        ('見積・積算', ['人件費単価の根拠', '直接経費', '一般管理費', '技術経費']),
    ],
    'goods': [
        ('書類準備', ['入札参加資格確認書', '会社概要書', '納税証明書', '製品カタログ']),
        ('提案', ['納品仕様書', '品質保証書', '保守・サポート体制', '納品スケジュール']),
        ('見積', ['単価明細', '数量確認', '運送費', '設置費用', '保証費用']),
    ],
}


@app.get("/api/checklist/{category}")
async def api_bid_checklist(category: str, item_id: int = None):
    """Generate bid preparation checklist for a category."""
    checklist = BID_CHECKLISTS.get(category, BID_CHECKLISTS.get('service', []))
    result = {'category': category, 'sections': []}
    for section_name, items in checklist:
        result['sections'].append({
            'name': section_name,
            'items': [{'text': item, 'checked': False} for item in items],
        })

    # If item_id provided, add item-specific info
    if item_id:
        conn = get_db()
        row = conn.execute("SELECT * FROM procurement_items WHERE item_id = ?", (item_id,)).fetchone()
        if row:
            item = dict(row)
            dl = deadline_info(item.get('deadline'))
            result['item_title'] = item.get('title', '')
            result['deadline'] = item.get('deadline', '')
            result['days_left'] = dl.get('days')
            # Timeline suggestions
            if dl.get('days') and dl['days'] > 0:
                days = dl['days']
                result['timeline'] = [
                    {'label': '仕様書確認・質問作成', 'by_day': max(1, days - max(days // 2, 7)), 'done': False},
                    {'label': '質問書提出', 'by_day': max(1, days - max(days // 3, 5)), 'done': False},
                    {'label': '見積書作成', 'by_day': max(1, days - 3), 'done': False},
                    {'label': '社内決裁', 'by_day': max(1, days - 2), 'done': False},
                    {'label': '入札書提出', 'by_day': max(1, days - 1), 'done': False},
                ]
    return JSONResponse(result)


# ── Iter 15: Enhanced Scorecard with Price & Vendor Data ──────────────────

def compute_enhanced_scorecard(conn, item_id: int) -> dict:
    """Enhanced GO/NO-GO scorecard with price range and vendor analysis."""
    base = compute_gonogo_score(conn, item_id)
    if base['total'] == 0:
        return base

    row = conn.execute("SELECT * FROM procurement_items WHERE item_id = ?", (item_id,)).fetchone()
    if not row:
        return base
    item = dict(row)
    cat_key = item.get('category') or classify_item(item.get('title', ''))[0]

    # Add price range factor
    price_data = estimate_price_range(conn, cat_key, item.get('muni_code'))
    if price_data.get('available'):
        base['price_range'] = price_data
        base['factors'].append({
            'name': '価格情報',
            'score': 15 if price_data['count'] >= 5 else 8,
            'max': 20,
            'reason': f'類似案件{price_data["count"]}件 ({price_data["display"]})',
        })
    else:
        base['factors'].append({
            'name': '価格情報',
            'score': 5,
            'max': 20,
            'reason': '価格データ不足',
        })

    # Add vendor concentration factor
    vendor_data = get_vendor_landscape(conn, item.get('muni_code'), cat_key)
    if vendor_data['top_vendors']:
        conc = vendor_data['concentration']
        score = 18 if conc == 'low' else 12 if conc == 'medium' else 6
        base['factors'].append({
            'name': 'ベンダー集中',
            'score': score,
            'max': 20,
            'reason': f'HHI={vendor_data["hhi"]:.3f} ({conc})',
        })
        base['vendor_landscape'] = vendor_data
    else:
        base['factors'].append({
            'name': 'ベンダー集中',
            'score': 10,
            'max': 20,
            'reason': 'ベンダーデータなし',
        })

    # Recalculate totals with new factors
    base['total'] = sum(f['score'] for f in base['factors'])
    base['max'] = sum(f['max'] for f in base['factors'])
    base['pct'] = round(base['total'] / base['max'] * 100) if base['max'] else 0

    # GO/NO-GO recommendation
    pct = base['pct']
    if pct >= 70:
        base['recommendation'] = 'GO'
        base['rec_color'] = '#16a34a'
        base['rec_label'] = '積極的に検討'
    elif pct >= 50:
        base['recommendation'] = 'CONSIDER'
        base['rec_color'] = '#d97706'
        base['rec_label'] = '条件次第で検討'
    else:
        base['recommendation'] = 'NO-GO'
        base['rec_color'] = '#dc2626'
        base['rec_label'] = '見送り推奨'

    return base


@app.get("/api/scorecard-enhanced/{item_id}")
async def api_enhanced_scorecard(item_id: int):
    conn = get_db()
    score = compute_enhanced_scorecard(conn, item_id)
    return JSONResponse(score)


# ── Iter 16: Competitive Landscape Page ───────────────────────────────────

@app.get("/api/competitive/{muni_code}")
async def api_competitive_landscape(muni_code: str):
    """Competitive landscape for a specific municipality."""
    conn = get_db()

    # Category distribution
    cats = {}
    for row in conn.execute("""
        SELECT title, category FROM procurement_items
        WHERE muni_code = ? AND detected_at > datetime('now', '-365 days')
    """, (muni_code,)).fetchall():
        cat = row['category'] or classify_item(row['title'])[0]
        cats[cat] = cats.get(cat, 0) + 1

    # Method distribution
    methods = {}
    for row in conn.execute("""
        SELECT method, COUNT(*) as cnt FROM procurement_items
        WHERE muni_code = ? AND method IS NOT NULL AND detected_at > datetime('now', '-365 days')
        GROUP BY method ORDER BY cnt DESC LIMIT 10
    """, (muni_code,)).fetchall():
        methods[row['method']] = row['cnt']

    # Monthly trend
    monthly = {}
    for row in conn.execute("""
        SELECT strftime('%Y-%m', detected_at) as month, COUNT(*) as cnt
        FROM procurement_items WHERE muni_code = ?
        GROUP BY month ORDER BY month DESC LIMIT 24
    """, (muni_code,)).fetchall():
        monthly[row['month']] = row['cnt']

    return JSONResponse({
        'muni_code': muni_code,
        'categories': cats,
        'methods': methods,
        'monthly_trend': monthly,
        'total_items': sum(cats.values()),
    })


# ── Iter 17-18: Bid Timeline & Reverse Countdown ─────────────────────────

@app.get("/api/timeline/{item_id}")
async def api_bid_timeline(item_id: int):
    """Generate reverse countdown timeline for bid preparation."""
    conn = get_db()
    row = conn.execute("SELECT * FROM procurement_items WHERE item_id = ?", (item_id,)).fetchone()
    if not row:
        return JSONResponse({'error': 'Item not found'}, status_code=404)
    item = dict(row)
    dl = deadline_info(item.get('deadline'))

    today = date.today()
    events = []

    if dl.get('days') is not None and dl['days'] > 0:
        deadline_date = datetime.strptime(item['deadline'][:10], '%Y-%m-%d').date()
        days = dl['days']

        milestones = [
            (max(0, days - 14), '仕様書入手・精読', '入手した仕様書を精読し、不明点を洗い出す'),
            (max(0, days - 10), '質問書作成・提出', '仕様書の不明点について質問書を作成'),
            (max(0, days - 7), '見積・積算開始', '数量調書の確認と単価設定を開始'),
            (max(0, days - 5), '技術提案書作成', '技術提案書・施工計画書等を作成'),
            (max(0, days - 3), '社内レビュー', '見積書・提案書の社内レビュー'),
            (max(0, days - 2), '最終決裁', '入札参加の最終決裁を取得'),
            (max(0, days - 1), '入札書類最終確認', '全書類の最終確認・封緘'),
            (0, '入札書提出', '入札書を提出（電子入札 or 持参）'),
        ]

        for days_before, label, desc in milestones:
            target_date = deadline_date - timedelta(days=days - days_before) if days_before > 0 else deadline_date
            is_past = target_date <= today
            events.append({
                'date': target_date.isoformat(),
                'label': label,
                'description': desc,
                'days_from_now': (target_date - today).days,
                'status': 'done' if is_past else 'upcoming',
            })

    return JSONResponse({
        'item_id': item_id,
        'title': item.get('title', ''),
        'deadline': item.get('deadline', ''),
        'days_left': dl.get('days'),
        'events': events,
    })


# ── Iter 19: Municipality Procurement Summary ─────────────────────────────

@app.get("/api/muni-summary/{muni_code}")
async def api_muni_procurement_summary(muni_code: str):
    """Procurement summary for a municipality."""
    conn = get_db()

    total = conn.execute(
        "SELECT COUNT(*) FROM procurement_items WHERE muni_code = ?", (muni_code,)
    ).fetchone()[0]

    recent_30d = conn.execute("""
        SELECT COUNT(*) FROM procurement_items
        WHERE muni_code = ? AND detected_at > datetime('now', '-30 days')
    """, (muni_code,)).fetchone()[0]

    # Category breakdown
    cats = {}
    for row in conn.execute("""
        SELECT title, category FROM procurement_items WHERE muni_code = ?
    """, (muni_code,)).fetchall():
        cat = row['category'] or classify_item(row['title'])[0]
        cats[cat] = cats.get(cat, 0) + 1

    # Active deadlines
    active = conn.execute("""
        SELECT COUNT(*) FROM procurement_items
        WHERE muni_code = ? AND deadline > date('now') AND deadline != 'z'
    """, (muni_code,)).fetchone()[0]

    # Procurement frequency (items per month, last 12m)
    monthly_rows = conn.execute("""
        SELECT strftime('%Y-%m', detected_at) as month, COUNT(*) as cnt
        FROM procurement_items WHERE muni_code = ? AND detected_at > datetime('now', '-365 days')
        GROUP BY month
    """, (muni_code,)).fetchall()
    avg_monthly = sum(r['cnt'] for r in monthly_rows) / max(len(monthly_rows), 1)

    name, pref = resolve_name(muni_code)

    return JSONResponse({
        'muni_code': muni_code,
        'name': name,
        'prefecture': pref,
        'total_items': total,
        'recent_30d': recent_30d,
        'active_deadlines': active,
        'avg_monthly': round(avg_monthly, 1),
        'categories': cats,
        'top_category': max(cats, key=cats.get) if cats else None,
    })


# ── Iter 20: Auto-Category Backfill API ───────────────────────────────────

@app.post("/api/admin/backfill-categories")
async def api_backfill_categories():
    """Backfill category column for items that have NULL category."""
    conn = get_db()
    rows = conn.execute(
        "SELECT item_id, title FROM procurement_items WHERE category IS NULL OR category = ''"
    ).fetchall()

    updated = 0
    for r in rows:
        cat_key, _ = classify_item(r['title'])
        conn.execute("UPDATE procurement_items SET category = ? WHERE item_id = ?", (cat_key, r['item_id']))
        updated += 1
        if updated % 100 == 0:
            conn.commit()
    conn.commit()

    return JSONResponse({'backfilled': updated, 'total_null_before': len(rows)})


# ══════════════════════════════════════════════════════════════════════════
# Phase 3: Decision Support (Iter 21-30)
# ══════════════════════════════════════════════════════════════════════════

# ── Iter 21: Win Probability Estimation ───────────────────────────────────

def estimate_win_probability(conn, item_id: int) -> dict:
    """Estimate win probability based on historical patterns."""
    row = conn.execute("SELECT * FROM procurement_items WHERE item_id = ?", (item_id,)).fetchone()
    if not row:
        return {'probability': 0, 'confidence': 'low'}
    item = dict(row)
    cat_key = item.get('category') or classify_item(item.get('title', ''))[0]
    muni = item.get('muni_code')

    # Factor 1: Competition level (fewer items in same muni+cat = less competition)
    same_items = conn.execute("""
        SELECT COUNT(*) FROM procurement_items
        WHERE muni_code = ? AND category = ? AND detected_at > datetime('now', '-90 days')
    """, (muni, cat_key)).fetchone()[0]

    competition_factor = max(0.1, 1.0 / (1.0 + (same_items / 10)))

    # Factor 2: Method type advantage
    method = item.get('method') or ''
    method_factor = 0.3  # default
    if 'プロポーザル' in method or '総合評価' in method:
        method_factor = 0.45  # quality-based = higher chance for prepared bidder
    elif '一般競争' in method:
        method_factor = 0.25  # price-based
    elif '指名' in method:
        method_factor = 0.35  # need nomination

    # Factor 3: Category familiarity (how many items in this category overall)
    cat_total = conn.execute(
        "SELECT COUNT(*) FROM procurement_items WHERE category = ?", (cat_key,)
    ).fetchone()[0]
    familiarity_factor = min(0.5, cat_total / 1000)

    # Combined probability
    base_prob = (competition_factor * 0.4 + method_factor * 0.35 + familiarity_factor * 0.25)
    prob = min(0.85, max(0.05, base_prob))

    return {
        'probability': round(prob, 3),
        'probability_pct': round(prob * 100, 1),
        'factors': {
            'competition': round(competition_factor, 3),
            'method_advantage': round(method_factor, 3),
            'category_familiarity': round(familiarity_factor, 3),
        },
        'confidence': 'high' if cat_total > 100 else 'medium' if cat_total > 20 else 'low',
        'recommendation': '有利' if prob > 0.4 else '標準' if prob > 0.2 else '厳しい',
    }


@app.get("/api/win-probability/{item_id}")
async def api_win_probability(item_id: int):
    conn = get_db()
    data = estimate_win_probability(conn, item_id)
    return JSONResponse(data)


# ── Iter 22: Budget Forecast ──────────────────────────────────────────────

@app.get("/api/budget-forecast/{muni_code}")
async def api_budget_forecast(muni_code: str, months: int = Query(6, ge=1, le=24)):
    """Forecast procurement volume for a municipality."""
    conn = get_db()

    # Get historical monthly counts
    rows = conn.execute("""
        SELECT strftime('%Y-%m', detected_at) as month, COUNT(*) as cnt
        FROM procurement_items WHERE muni_code = ?
        GROUP BY month ORDER BY month
    """, (muni_code,)).fetchall()

    if len(rows) < 3:
        return JSONResponse({'available': False, 'message': 'データ不足（3ヶ月以上必要）'})

    history = [(r['month'], r['cnt']) for r in rows]
    counts = [c for _, c in history]

    # Simple moving average + trend
    recent_avg = sum(counts[-3:]) / 3
    older_avg = sum(counts[-6:-3]) / 3 if len(counts) >= 6 else recent_avg
    trend = (recent_avg - older_avg) / max(older_avg, 1)

    # Seasonal pattern
    monthly_pattern = {}
    for month_str, cnt in history:
        m = int(month_str.split('-')[1])
        monthly_pattern.setdefault(m, []).append(cnt)

    seasonal = {m: sum(v) / len(v) for m, v in monthly_pattern.items()}
    overall_avg = sum(seasonal.values()) / max(len(seasonal), 1)

    # Generate forecast
    forecast = []
    last_month = history[-1][0]
    year, month = int(last_month[:4]), int(last_month[5:7])

    for i in range(1, months + 1):
        month += 1
        if month > 12:
            month = 1
            year += 1
        seasonal_factor = seasonal.get(month, overall_avg) / max(overall_avg, 1)
        predicted = recent_avg * (1 + trend * 0.5) * seasonal_factor
        forecast.append({
            'month': f'{year:04d}-{month:02d}',
            'predicted': max(0, round(predicted, 1)),
            'low': max(0, round(predicted * 0.7, 1)),
            'high': round(predicted * 1.3, 1),
        })

    return JSONResponse({
        'available': True,
        'muni_code': muni_code,
        'history': history[-12:],
        'forecast': forecast,
        'trend': round(trend, 3),
        'trend_label': '増加傾向' if trend > 0.1 else '減少傾向' if trend < -0.1 else '横ばい',
    })


# ── Iter 23: Auto Brief Generation ───────────────────────────────────────

@app.get("/api/brief/{muni_code}")
async def api_auto_brief(muni_code: str):
    """Generate a sales brief for a municipality."""
    conn = get_db()
    name, pref = resolve_name(muni_code)

    # Gather data
    total = conn.execute(
        "SELECT COUNT(*) FROM procurement_items WHERE muni_code = ?", (muni_code,)
    ).fetchone()[0]

    recent = conn.execute("""
        SELECT COUNT(*) FROM procurement_items
        WHERE muni_code = ? AND detected_at > datetime('now', '-30 days')
    """, (muni_code,)).fetchone()[0]

    # Category breakdown
    cats = {}
    for row in conn.execute("SELECT title, category FROM procurement_items WHERE muni_code = ?", (muni_code,)).fetchall():
        c = row['category'] or classify_item(row['title'])[0]
        cats[c] = cats.get(c, 0) + 1

    top_cat = max(cats, key=cats.get) if cats else 'other'
    cat_labels = CATEGORIES

    # Active deadlines
    active_dl = conn.execute("""
        SELECT title, deadline FROM procurement_items
        WHERE muni_code = ? AND deadline > date('now') AND deadline != 'z'
        ORDER BY deadline LIMIT 5
    """, (muni_code,)).fetchall()

    # Build brief
    brief_lines = []
    brief_lines.append(f"## {pref} {name} 営業ブリーフ")
    brief_lines.append(f"")
    brief_lines.append(f"### 基本統計")
    brief_lines.append(f"- 総案件数: **{total}件**")
    brief_lines.append(f"- 直近30日: **{recent}件**")
    brief_lines.append(f"- 最多カテゴリ: **{CATEGORIES.get(top_cat, top_cat)}** ({cats.get(top_cat, 0)}件)")
    brief_lines.append(f"")

    if active_dl:
        brief_lines.append(f"### 進行中の案件（締切順）")
        for r in active_dl:
            dl_info = deadline_info(r['deadline'])
            brief_lines.append(f"- [{dl_info['label']}] {r['title'][:60]}")
        brief_lines.append(f"")

    brief_lines.append(f"### 推奨アクション")
    if recent > 5:
        brief_lines.append(f"- 活発な発注が続いています。営業訪問のタイミングです。")
    elif recent > 0:
        brief_lines.append(f"- 通常ペースの発注です。定期的なフォローを継続してください。")
    else:
        brief_lines.append(f"- 直近の発注がありません。次期予算期（4月/10月）に注目してください。")

    return JSONResponse({
        'muni_code': muni_code,
        'name': name,
        'prefecture': pref,
        'brief_markdown': '\n'.join(brief_lines),
        'stats': {
            'total': total, 'recent_30d': recent,
            'categories': cats, 'active_deadlines': len(active_dl),
        },
    })


# ── Iter 24: Comparative Analysis ─────────────────────────────────────────

@app.get("/api/compare")
async def api_compare_municipalities(codes: str = Query(..., description="Comma-separated muni codes")):
    """Compare procurement patterns across municipalities."""
    conn = get_db()
    muni_list = [c.strip() for c in codes.split(',') if c.strip()][:10]

    results = []
    for mc in muni_list:
        name, pref = resolve_name(mc)
        total = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code = ?", (mc,)).fetchone()[0]
        recent = conn.execute("""
            SELECT COUNT(*) FROM procurement_items
            WHERE muni_code = ? AND detected_at > datetime('now', '-30 days')
        """, (mc,)).fetchone()[0]

        cats = {}
        for row in conn.execute("SELECT title, category FROM procurement_items WHERE muni_code = ?", (mc,)).fetchall():
            c = row['category'] or classify_item(row['title'])[0]
            cats[c] = cats.get(c, 0) + 1

        results.append({
            'muni_code': mc,
            'name': name,
            'prefecture': pref,
            'total_items': total,
            'recent_30d': recent,
            'categories': cats,
            'top_category': max(cats, key=cats.get) if cats else None,
        })

    return JSONResponse({'municipalities': results, 'count': len(results)})


# ── Iter 25-26: Market Trend Analysis ─────────────────────────────────────

@app.get("/api/trends")
async def api_market_trends(days: int = Query(365), category: str = None, pref: str = None):
    """Market trend analysis with growth rates and forecasting."""
    conn = get_db()

    where = "WHERE detected_at > datetime('now', ?)"
    params = [f'-{days} days']
    if category:
        where += " AND category = ?"
        params.append(category)
    if pref:
        where += " AND muni_code LIKE ?"
        params.append(f'{pref}%')

    # Monthly totals
    monthly = conn.execute(f"""
        SELECT strftime('%Y-%m', detected_at) as month, COUNT(*) as cnt
        FROM procurement_items {where}
        GROUP BY month ORDER BY month
    """, params).fetchall()

    monthly_data = [{'month': r['month'], 'count': r['cnt']} for r in monthly]

    # Category breakdown over time
    cat_monthly = {}
    for row in conn.execute(f"""
        SELECT strftime('%Y-%m', detected_at) as month, title, category
        FROM procurement_items {where}
    """, params).fetchall():
        m = row['month']
        c = row['category'] or classify_item(row['title'])[0]
        cat_monthly.setdefault(c, {}).setdefault(m, 0)
        cat_monthly[c][m] += 1

    # Growth rate calculation
    if len(monthly_data) >= 2:
        recent = sum(r['count'] for r in monthly_data[-3:])
        older = sum(r['count'] for r in monthly_data[-6:-3]) if len(monthly_data) >= 6 else recent
        growth = (recent - older) / max(older, 1) * 100
    else:
        growth = 0

    return JSONResponse({
        'monthly': monthly_data,
        'category_monthly': {c: dict(sorted(m.items())) for c, m in cat_monthly.items()},
        'growth_rate': round(growth, 1),
        'growth_label': '成長' if growth > 10 else '安定' if growth > -10 else '縮小',
        'total': sum(r['count'] for r in monthly_data),
    })


# ── Iter 27: Top Municipalities Ranking ───────────────────────────────────

@app.get("/api/ranking")
async def api_municipality_ranking(
    metric: str = Query('activity', description='activity|growth|diversity'),
    days: int = Query(90),
    limit: int = Query(50, ge=1, le=200),
):
    """Rank municipalities by various metrics."""
    conn = get_db()

    if metric == 'activity':
        rows = conn.execute("""
            SELECT muni_code, COUNT(*) as cnt
            FROM procurement_items
            WHERE detected_at > datetime('now', ?) AND muni_code != 'NATIONAL'
            GROUP BY muni_code ORDER BY cnt DESC LIMIT ?
        """, (f'-{days} days', limit)).fetchall()

        result = []
        for i, r in enumerate(rows, 1):
            name, pref = resolve_name(r['muni_code'])
            result.append({
                'rank': i,
                'muni_code': r['muni_code'],
                'name': name,
                'prefecture': pref,
                'value': r['cnt'],
                'label': f'{r["cnt"]}件',
            })

    elif metric == 'growth':
        # Growth = recent 30d vs previous 30d
        recent = {}
        for row in conn.execute("""
            SELECT muni_code, COUNT(*) as cnt FROM procurement_items
            WHERE detected_at > datetime('now', '-30 days') AND muni_code != 'NATIONAL'
            GROUP BY muni_code
        """).fetchall():
            recent[row['muni_code']] = row['cnt']

        older = {}
        for row in conn.execute("""
            SELECT muni_code, COUNT(*) as cnt FROM procurement_items
            WHERE detected_at BETWEEN datetime('now', '-60 days') AND datetime('now', '-30 days')
                AND muni_code != 'NATIONAL'
            GROUP BY muni_code
        """).fetchall():
            older[row['muni_code']] = row['cnt']

        growth_data = []
        for mc in set(list(recent.keys()) + list(older.keys())):
            r, o = recent.get(mc, 0), older.get(mc, 0)
            if o > 0:
                growth = (r - o) / o * 100
            elif r > 0:
                growth = 100
            else:
                continue
            growth_data.append((mc, growth, r))

        growth_data.sort(key=lambda x: -x[1])
        result = []
        for i, (mc, g, cnt) in enumerate(growth_data[:limit], 1):
            name, pref = resolve_name(mc)
            result.append({
                'rank': i,
                'muni_code': mc,
                'name': name,
                'prefecture': pref,
                'value': round(g, 1),
                'label': f'+{g:.0f}%' if g > 0 else f'{g:.0f}%',
                'recent_count': cnt,
            })

    elif metric == 'diversity':
        # Category diversity (unique categories)
        rows = conn.execute("""
            SELECT muni_code, title, category FROM procurement_items
            WHERE detected_at > datetime('now', ?) AND muni_code != 'NATIONAL'
        """, (f'-{days} days',)).fetchall()

        muni_cats = {}
        for r in rows:
            mc = r['muni_code']
            c = r['category'] or classify_item(r['title'])[0]
            muni_cats.setdefault(mc, set()).add(c)

        div_data = sorted(
            [(mc, len(cats)) for mc, cats in muni_cats.items()],
            key=lambda x: -x[1]
        )

        result = []
        for i, (mc, ncats) in enumerate(div_data[:limit], 1):
            name, pref = resolve_name(mc)
            result.append({
                'rank': i,
                'muni_code': mc,
                'name': name,
                'prefecture': pref,
                'value': ncats,
                'label': f'{ncats}カテゴリ',
            })
    else:
        result = []

    return JSONResponse({'metric': metric, 'days': days, 'ranking': result})


# ── Iter 28: Deadline Calendar ────────────────────────────────────────────

@app.get("/api/calendar")
async def api_deadline_calendar(days: int = Query(30)):
    """Get upcoming deadlines as calendar events."""
    conn = get_db()
    rows = conn.execute("""
        SELECT item_id, muni_code, title, category, deadline, method, amount
        FROM procurement_items
        WHERE deadline > date('now') AND deadline < date('now', ?) AND deadline != 'z'
        ORDER BY deadline
    """, (f'+{days} days',)).fetchall()

    events = []
    for r in rows:
        name, pref = resolve_name(r['muni_code'])
        cat_key = r['category'] or classify_item(r['title'])[0]
        events.append({
            'item_id': r['item_id'],
            'date': r['deadline'][:10] if r['deadline'] else None,
            'title': r['title'],
            'muni_code': r['muni_code'],
            'muni_name': name,
            'category': cat_key,
            'method': r['method'],
            'amount': r['amount'],
        })

    # Group by date
    by_date = {}
    for e in events:
        d = e['date']
        by_date.setdefault(d, []).append(e)

    return JSONResponse({
        'events': events,
        'by_date': {d: evts for d, evts in sorted(by_date.items())},
        'total': len(events),
    })


# ── Iter 29: Category Performance Dashboard ──────────────────────────────

@app.get("/api/category-performance")
async def api_category_performance():
    """Performance metrics by category."""
    conn = get_db()

    results = {}
    for cat_key, cat_label, _ in CATEGORY_RULES + [('other', 'その他', [])]:
        total = conn.execute(
            "SELECT COUNT(*) FROM procurement_items WHERE category = ?", (cat_key,)
        ).fetchone()[0]

        recent = conn.execute("""
            SELECT COUNT(*) FROM procurement_items
            WHERE category = ? AND detected_at > datetime('now', '-30 days')
        """, (cat_key,)).fetchone()[0]

        older = conn.execute("""
            SELECT COUNT(*) FROM procurement_items
            WHERE category = ? AND detected_at BETWEEN datetime('now', '-60 days') AND datetime('now', '-30 days')
        """, (cat_key,)).fetchone()[0]

        growth = ((recent - older) / max(older, 1) * 100) if older > 0 else 0

        # Active deadlines
        active = conn.execute("""
            SELECT COUNT(*) FROM procurement_items
            WHERE category = ? AND deadline > date('now') AND deadline != 'z'
        """, (cat_key,)).fetchone()[0]

        # Unique municipalities
        munis = conn.execute("""
            SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE category = ?
        """, (cat_key,)).fetchone()[0]

        if total > 0:
            results[cat_key] = {
                'label': cat_label,
                'total': total,
                'recent_30d': recent,
                'growth_pct': round(growth, 1),
                'active_deadlines': active,
                'municipalities': munis,
            }

    return JSONResponse({'categories': results})


# ── Iter 30: Prefecture Analytics ─────────────────────────────────────────

@app.get("/api/prefecture-analytics")
async def api_prefecture_analytics():
    """Analytics breakdown by prefecture."""
    conn = get_db()

    pref_data = {}
    for row in conn.execute("""
        SELECT muni_code, COUNT(*) as cnt
        FROM procurement_items
        WHERE muni_code != 'NATIONAL' AND detected_at > datetime('now', '-365 days')
        GROUP BY muni_code
    """).fetchall():
        mc = row['muni_code']
        pref_code = mc[:2] if len(mc) >= 2 else '00'
        pref_data.setdefault(pref_code, {'total': 0, 'municipalities': 0})
        pref_data[pref_code]['total'] += row['cnt']
        pref_data[pref_code]['municipalities'] += 1

    results = []
    for pref_code, data in sorted(pref_data.items(), key=lambda x: -x[1]['total']):
        results.append({
            'pref_code': pref_code,
            'name': PREF_NAMES.get(pref_code, f'コード{pref_code}'),
            'total_items': data['total'],
            'municipalities': data['municipalities'],
            'avg_per_muni': round(data['total'] / max(data['municipalities'], 1), 1),
        })

    return JSONResponse({'prefectures': results, 'total': sum(d['total_items'] for d in results)})


# ══════════════════════════════════════════════════════════════════════════
# Phase 4: Advanced Analytics & Reports (Iter 31-40)
# ══════════════════════════════════════════════════════════════════════════

# ── Iter 31: Report Generation API ────────────────────────────────────────

@app.get("/api/report/municipality/{muni_code}")
async def api_muni_report(muni_code: str):
    """Generate comprehensive municipality report."""
    conn = get_db()
    name, pref = resolve_name(muni_code)

    # Gather all data
    total = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code = ?", (muni_code,)).fetchone()[0]
    recent = conn.execute("""
        SELECT COUNT(*) FROM procurement_items
        WHERE muni_code = ? AND detected_at > datetime('now', '-30 days')
    """, (muni_code,)).fetchone()[0]

    # Category breakdown
    cats = {}
    for row in conn.execute("SELECT title, category FROM procurement_items WHERE muni_code = ?", (muni_code,)).fetchall():
        c = row['category'] or classify_item(row['title'])[0]
        cats[c] = cats.get(c, 0) + 1

    # Monthly trend
    monthly = conn.execute("""
        SELECT strftime('%Y-%m', detected_at) as month, COUNT(*) as cnt
        FROM procurement_items WHERE muni_code = ?
        GROUP BY month ORDER BY month
    """, (muni_code,)).fetchall()

    # Vendor landscape
    vendors = get_vendor_landscape(conn, muni_code)

    # Cycles
    cycles = detect_procurement_cycles(conn, muni_code)

    return JSONResponse({
        'report_type': 'municipality',
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'municipality': {
            'code': muni_code, 'name': name, 'prefecture': pref,
        },
        'overview': {
            'total_items': total,
            'recent_30d': recent,
            'categories': cats,
            'top_category': max(cats, key=cats.get) if cats else None,
        },
        'trends': {
            'monthly': [{'month': r['month'], 'count': r['cnt']} for r in monthly],
        },
        'vendors': vendors,
        'cycles': cycles,
    })


# ── Iter 32: Market Report ────────────────────────────────────────────────

@app.get("/api/report/market")
async def api_market_report(category: str = None, pref: str = None):
    """Generate market overview report."""
    conn = get_db()

    where = "WHERE detected_at > datetime('now', '-365 days')"
    params = []
    if category:
        where += " AND category = ?"
        params.append(category)
    if pref:
        where += " AND muni_code LIKE ?"
        params.append(f'{pref}%')

    total = conn.execute(f"SELECT COUNT(*) FROM procurement_items {where}", params).fetchone()[0]

    # Monthly trend
    monthly = conn.execute(f"""
        SELECT strftime('%Y-%m', detected_at) as month, COUNT(*) as cnt
        FROM procurement_items {where}
        GROUP BY month ORDER BY month
    """, params).fetchall()

    # Category distribution
    cats = {}
    for row in conn.execute(f"SELECT title, category FROM procurement_items {where}", params).fetchall():
        c = row['category'] or classify_item(row['title'])[0]
        cats[c] = cats.get(c, 0) + 1

    # Top municipalities
    top_munis = conn.execute(f"""
        SELECT muni_code, COUNT(*) as cnt FROM procurement_items {where}
        AND muni_code != 'NATIONAL'
        GROUP BY muni_code ORDER BY cnt DESC LIMIT 20
    """, params).fetchall()

    top_muni_list = []
    for r in top_munis:
        name, pref_name = resolve_name(r['muni_code'])
        top_muni_list.append({'muni_code': r['muni_code'], 'name': name, 'prefecture': pref_name, 'count': r['cnt']})

    return JSONResponse({
        'report_type': 'market',
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'filters': {'category': category, 'prefecture': pref},
        'overview': {'total_items': total, 'categories': cats},
        'trends': {'monthly': [{'month': r['month'], 'count': r['cnt']} for r in monthly]},
        'top_municipalities': top_muni_list,
    })


# ── Iter 33: Cross-category Analysis ─────────────────────────────────────

@app.get("/api/cross-analysis")
async def api_cross_analysis():
    """Cross-category correlation analysis."""
    conn = get_db()

    # Which municipalities are active across multiple categories?
    rows = conn.execute("""
        SELECT muni_code, title, category
        FROM procurement_items
        WHERE detected_at > datetime('now', '-180 days') AND muni_code != 'NATIONAL'
    """).fetchall()

    muni_cats = {}
    for r in rows:
        mc = r['muni_code']
        c = r['category'] or classify_item(r['title'])[0]
        muni_cats.setdefault(mc, {}).setdefault(c, 0)
        muni_cats[mc][c] += 1

    # Category co-occurrence matrix
    cat_keys = list(CATEGORIES.keys())
    cooccurrence = {c1: {c2: 0 for c2 in cat_keys} for c1 in cat_keys}
    for mc, cats in muni_cats.items():
        active_cats = [c for c, count in cats.items() if count > 0]
        for c1 in active_cats:
            for c2 in active_cats:
                if c1 in cooccurrence and c2 in cooccurrence.get(c1, {}):
                    cooccurrence[c1][c2] += 1

    # Multi-category municipalities (diverse procurement)
    diverse = sorted(
        [(mc, len(cats)) for mc, cats in muni_cats.items() if len(cats) >= 3],
        key=lambda x: -x[1]
    )[:20]

    diverse_list = []
    for mc, n in diverse:
        name, pref = resolve_name(mc)
        diverse_list.append({'muni_code': mc, 'name': name, 'prefecture': pref, 'num_categories': n})

    return JSONResponse({
        'cooccurrence': cooccurrence,
        'diverse_municipalities': diverse_list,
        'category_labels': CATEGORIES,
    })


# ── Iter 34: Anomaly Detection ────────────────────────────────────────────

@app.get("/api/anomalies")
async def api_detect_anomalies(days: int = Query(30)):
    """Detect unusual procurement patterns."""
    conn = get_db()

    anomalies = []

    # 1. Municipalities with sudden spikes (>3x their average)
    for row in conn.execute("""
        SELECT muni_code,
            COUNT(CASE WHEN detected_at > datetime('now', ?) THEN 1 END) as recent,
            COUNT(CASE WHEN detected_at BETWEEN datetime('now', ?) AND datetime('now', ?) THEN 1 END) as older
        FROM procurement_items
        WHERE muni_code != 'NATIONAL'
        GROUP BY muni_code
        HAVING recent > 3 AND recent > older * 3
    """, (f'-{days} days', f'-{days * 2} days', f'-{days} days')).fetchall():
        name, pref = resolve_name(row['muni_code'])
        anomalies.append({
            'type': 'volume_spike',
            'severity': 'high',
            'muni_code': row['muni_code'],
            'name': name,
            'description': f'{name}の案件数が急増（{row["older"]}件→{row["recent"]}件, {days}日間）',
            'recent': row['recent'],
            'previous': row['older'],
        })

    # 2. Categories with unusual growth
    for cat_key, cat_label, _ in CATEGORY_RULES:
        recent_cnt = conn.execute("""
            SELECT COUNT(*) FROM procurement_items WHERE category = ? AND detected_at > datetime('now', ?)
        """, (cat_key, f'-{days} days')).fetchone()[0]
        older_cnt = conn.execute("""
            SELECT COUNT(*) FROM procurement_items
            WHERE category = ? AND detected_at BETWEEN datetime('now', ?) AND datetime('now', ?)
        """, (cat_key, f'-{days * 2} days', f'-{days} days')).fetchone()[0]

        if older_cnt > 5 and recent_cnt > older_cnt * 2:
            anomalies.append({
                'type': 'category_surge',
                'severity': 'medium',
                'category': cat_key,
                'description': f'{cat_label}カテゴリが急増（{older_cnt}件→{recent_cnt}件）',
                'recent': recent_cnt,
                'previous': older_cnt,
            })

    # 3. Deadlines clustering (many deadlines on same day)
    deadline_rows = conn.execute("""
        SELECT deadline, COUNT(*) as cnt FROM procurement_items
        WHERE deadline > date('now') AND deadline < date('now', '+30 days') AND deadline != 'z'
        GROUP BY deadline HAVING cnt >= 5
        ORDER BY cnt DESC LIMIT 5
    """).fetchall()

    for r in deadline_rows:
        anomalies.append({
            'type': 'deadline_cluster',
            'severity': 'info',
            'date': r['deadline'],
            'description': f'{r["deadline"]}に{r["cnt"]}件の締切が集中',
            'count': r['cnt'],
        })

    return JSONResponse({
        'anomalies': sorted(anomalies, key=lambda x: {'high': 0, 'medium': 1, 'info': 2}.get(x['severity'], 3)),
        'total': len(anomalies),
        'analysis_period_days': days,
    })


# ── Iter 35: Saved Reports ───────────────────────────────────────────────

@app.post("/api/reports/save")
async def api_save_report(request: Request):
    """Save a report configuration for recurring generation."""
    conn = get_db()
    data = await request.json()

    # Use procurement_analytics table for storing report configs
    conn.execute("""
        INSERT OR REPLACE INTO procurement_analytics (muni_code, metric, period, value, detail_json, updated_at)
        VALUES (?, 'saved_report', ?, 0, ?, datetime('now'))
    """, (
        data.get('muni_code', 'ALL'),
        data.get('name', 'Untitled'),
        json.dumps(data, ensure_ascii=False),
    ))
    conn.commit()
    return JSONResponse({'ok': True})


@app.get("/api/reports/saved")
async def api_list_saved_reports():
    """List saved report configurations."""
    conn = get_db()
    rows = conn.execute("""
        SELECT muni_code, period as name, detail_json, updated_at
        FROM procurement_analytics WHERE metric = 'saved_report'
        ORDER BY updated_at DESC
    """).fetchall()
    return JSONResponse({
        'reports': [{'muni_code': r['muni_code'], 'name': r['name'],
                     'config': json.loads(r['detail_json']), 'updated_at': r['updated_at']} for r in rows]
    })


# ── Iter 36-37: Dashboard Widgets API ─────────────────────────────────────

@app.get("/api/widgets/kpi-summary")
async def api_widget_kpi_summary():
    """KPI summary widget data."""
    conn = get_db()

    total = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    today_count = conn.execute("""
        SELECT COUNT(*) FROM procurement_items WHERE detected_at > datetime('now', '-1 day')
    """).fetchone()[0]
    active_dl = conn.execute("""
        SELECT COUNT(*) FROM procurement_items WHERE deadline > date('now') AND deadline != 'z'
    """).fetchone()[0]
    munis = conn.execute(
        "SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code != 'NATIONAL'"
    ).fetchone()[0]
    pipeline_count = conn.execute(
        "SELECT COUNT(*) FROM item_pipeline WHERE status NOT IN ('won','lost','passed')"
    ).fetchone()[0]

    return JSONResponse({
        'total_items': total,
        'new_today': today_count,
        'active_deadlines': active_dl,
        'municipalities': munis,
        'pipeline_active': pipeline_count,
    })


@app.get("/api/widgets/deadline-urgent")
async def api_widget_urgent_deadlines():
    """Urgent deadlines widget (next 7 days)."""
    conn = get_db()
    rows = conn.execute("""
        SELECT item_id, muni_code, title, category, deadline, method
        FROM procurement_items
        WHERE deadline BETWEEN date('now') AND date('now', '+7 days') AND deadline != 'z'
        ORDER BY deadline LIMIT 20
    """).fetchall()

    items = []
    for r in rows:
        name, pref = resolve_name(r['muni_code'])
        dl = deadline_info(r['deadline'])
        items.append({
            'item_id': r['item_id'],
            'title': r['title'],
            'muni_name': name,
            'deadline': r['deadline'],
            'days_left': dl.get('days'),
            'urgency': dl.get('cls'),
        })

    return JSONResponse({'items': items, 'total': len(items)})


@app.get("/api/widgets/new-items")
async def api_widget_new_items(hours: int = Query(24)):
    """Recently detected items widget."""
    conn = get_db()
    rows = conn.execute("""
        SELECT item_id, muni_code, title, category, deadline, detected_at
        FROM procurement_items WHERE detected_at > datetime('now', ?)
        ORDER BY detected_at DESC LIMIT 20
    """, (f'-{hours} hours',)).fetchall()

    items = []
    for r in rows:
        name, pref = resolve_name(r['muni_code'])
        cat_key = r['category'] or classify_item(r['title'])[0]
        items.append({
            'item_id': r['item_id'],
            'title': r['title'],
            'muni_name': name,
            'category': cat_key,
            'deadline': r['deadline'],
            'detected_at': r['detected_at'],
        })

    return JSONResponse({'items': items, 'total': len(items)})


# ── Iter 38: Pipeline Analytics ───────────────────────────────────────────

@app.get("/api/pipeline-analytics")
async def api_pipeline_analytics():
    """Pipeline funnel analytics."""
    conn = get_db()

    # Status distribution
    status_counts = {}
    for row in conn.execute("SELECT status, COUNT(*) as cnt FROM item_pipeline GROUP BY status").fetchall():
        status_counts[row['status']] = row['cnt']

    # Priority distribution
    priority_counts = {}
    for row in conn.execute("SELECT priority, COUNT(*) as cnt FROM item_pipeline GROUP BY priority").fetchall():
        priority_counts[row['priority']] = row['cnt']

    # Win/loss rate
    won = status_counts.get('won', 0)
    lost = status_counts.get('lost', 0)
    decided = won + lost
    win_rate = round(won / decided * 100, 1) if decided > 0 else 0

    # Average time in pipeline (from created to decided)
    avg_time = conn.execute("""
        SELECT AVG(julianday(updated_at) - julianday(created_at)) as avg_days
        FROM item_pipeline WHERE status IN ('won', 'lost')
    """).fetchone()
    avg_days = round(avg_time['avg_days'], 1) if avg_time and avg_time['avg_days'] else 0

    return JSONResponse({
        'status_distribution': status_counts,
        'priority_distribution': priority_counts,
        'funnel': {
            'total': sum(status_counts.values()),
            'active': sum(v for k, v in status_counts.items() if k not in ('won', 'lost', 'passed')),
            'decided': decided,
            'won': won,
            'lost': lost,
            'win_rate': win_rate,
        },
        'avg_days_to_decision': avg_days,
    })


# ── Iter 39: Alert Matching Engine ────────────────────────────────────────

@app.get("/api/alerts/matches")
async def api_alert_matches():
    """Find items matching active alert rules."""
    conn = get_db()

    rules = conn.execute("SELECT * FROM alert_rules WHERE active = 1").fetchall()
    all_matches = []

    for rule in rules:
        conditions = json.loads(rule['conditions'])
        where_parts = ["detected_at > datetime('now', '-7 days')"]
        params = []

        if conditions.get('prefecture'):
            where_parts.append("muni_code LIKE ?")
            params.append(f'{conditions["prefecture"]}%')
        if conditions.get('category'):
            where_parts.append("category = ?")
            params.append(conditions['category'])
        if conditions.get('keywords'):
            for kw in conditions['keywords'].split():
                where_parts.append("title LIKE ?")
                params.append(f'%{kw}%')
        if conditions.get('min_amount'):
            where_parts.append("CAST(REPLACE(amount, ',', '') AS INTEGER) >= ?")
            params.append(int(conditions['min_amount']))

        where = ' AND '.join(where_parts)
        matched = conn.execute(f"""
            SELECT item_id, muni_code, title, category, deadline, detected_at
            FROM procurement_items WHERE {where}
            ORDER BY detected_at DESC LIMIT 20
        """, params).fetchall()

        for m in matched:
            name, pref = resolve_name(m['muni_code'])
            all_matches.append({
                'rule_id': rule['rule_id'],
                'rule_name': rule['name'],
                'item_id': m['item_id'],
                'title': m['title'],
                'muni_name': name,
                'category': m['category'],
                'deadline': m['deadline'],
                'detected_at': m['detected_at'],
            })

    return JSONResponse({
        'matches': all_matches,
        'total': len(all_matches),
        'rules_checked': len(rules),
    })


# ── Iter 40: Data Quality Dashboard ──────────────────────────────────────

@app.get("/api/data-quality")
async def api_data_quality():
    """Data quality metrics and completeness."""
    conn = get_db()

    total = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]

    # Completeness checks
    has_title = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE title IS NOT NULL AND title != ''").fetchone()[0]
    has_deadline = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline IS NOT NULL AND deadline != '' AND deadline != 'z'").fetchone()[0]
    has_amount = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE amount IS NOT NULL AND amount != ''").fetchone()[0]
    has_method = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE method IS NOT NULL AND method != ''").fetchone()[0]
    has_category = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE category IS NOT NULL AND category != ''").fetchone()[0]
    has_url = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE url IS NOT NULL AND url != ''").fetchone()[0]

    # Latest data timestamp
    latest = conn.execute("SELECT MAX(detected_at) as latest FROM procurement_items").fetchone()

    # Items per day (last 7 days)
    daily = conn.execute("""
        SELECT strftime('%Y-%m-%d', detected_at) as day, COUNT(*) as cnt
        FROM procurement_items WHERE detected_at > datetime('now', '-7 days')
        GROUP BY day ORDER BY day
    """).fetchall()

    return JSONResponse({
        'total_items': total,
        'completeness': {
            'title': {'count': has_title, 'pct': round(has_title / max(total, 1) * 100, 1)},
            'deadline': {'count': has_deadline, 'pct': round(has_deadline / max(total, 1) * 100, 1)},
            'amount': {'count': has_amount, 'pct': round(has_amount / max(total, 1) * 100, 1)},
            'method': {'count': has_method, 'pct': round(has_method / max(total, 1) * 100, 1)},
            'category': {'count': has_category, 'pct': round(has_category / max(total, 1) * 100, 1)},
            'url': {'count': has_url, 'pct': round(has_url / max(total, 1) * 100, 1)},
        },
        'latest_data': latest['latest'] if latest else None,
        'daily_intake': [{'date': r['day'], 'count': r['cnt']} for r in daily],
    })


# ══════════════════════════════════════════════════════════════════════════
# Phase 5: Platform & API (Iter 41-50)
# ══════════════════════════════════════════════════════════════════════════

# ── Iter 41-42: API v3 Endpoints ──────────────────────────────────────────

@app.get("/api/v3/items")
async def api_v3_items(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    pref: str = None, category: str = None, q: str = None,
    days: int = 365, sort: str = 'detected_at', order: str = 'desc',
):
    """API v3 items endpoint with pagination and full filtering."""
    conn = get_db()
    where = "WHERE detected_at > datetime('now', ?)"
    params = [f'-{days} days']

    if pref:
        if pref == 'NATIONAL':
            where += " AND muni_code = 'NATIONAL'"
        else:
            where += " AND muni_code LIKE ?"
            params.append(f'{pref}%')
    if category:
        where += " AND category = ?"
        params.append(category)
    if q:
        for term in q.split()[:5]:
            where += " AND title LIKE ?"
            params.append(f'%{term}%')

    # Count total
    total = conn.execute(f"SELECT COUNT(*) FROM procurement_items {where}", params).fetchone()[0]

    # Sort
    valid_sorts = {'detected_at', 'deadline', 'title', 'muni_code', 'category'}
    sort_col = sort if sort in valid_sorts else 'detected_at'
    sort_dir = 'DESC' if order == 'desc' else 'ASC'

    # Paginate
    offset = (page - 1) * per_page
    rows = conn.execute(f"""
        SELECT * FROM procurement_items {where}
        ORDER BY {sort_col} {sort_dir} LIMIT ? OFFSET ?
    """, params + [per_page, offset]).fetchall()

    items = []
    for r in rows:
        d = dict(r)
        name, pref_name = resolve_name(d['muni_code'], d.get('raw_json'))
        cat_key = d.get('category') or classify_item(d.get('title', ''))[0]
        items.append({
            'item_id': d.get('item_id'),
            'muni_code': d.get('muni_code'),
            'muni_name': name,
            'prefecture': pref_name,
            'title': d.get('title'),
            'category': cat_key,
            'category_label': CATEGORIES.get(cat_key, cat_key),
            'deadline': d.get('deadline'),
            'amount': d.get('amount'),
            'method': d.get('method'),
            'url': d.get('url'),
            'detected_at': d.get('detected_at'),
            'department': d.get('department'),
            'budget_range': d.get('budget_range'),
        })

    total_pages = (total + per_page - 1) // per_page

    return JSONResponse({
        'items': items,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'total_pages': total_pages,
            'has_next': page < total_pages,
            'has_prev': page > 1,
        },
    })


# ── Iter 43: Webhook Registration ─────────────────────────────────────────

@app.post("/api/webhooks")
async def api_register_webhook(request: Request):
    """Register a webhook for event notifications."""
    conn = get_db()
    data = await request.json()

    # Store webhook config in alert_rules with special type
    conn.execute("""
        INSERT INTO alert_rules (user_id, name, conditions, channels, active, created_at)
        VALUES (?, ?, ?, ?, 1, datetime('now'))
    """, (
        'webhook',
        data.get('name', 'Webhook'),
        json.dumps({'event': data.get('event', 'new_item'), **data.get('filters', {})}),
        json.dumps([{'type': 'webhook', 'url': data.get('url'), 'secret': data.get('secret', '')}]),
    ))
    conn.commit()
    rule_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    return JSONResponse({'ok': True, 'webhook_id': rule_id})


@app.get("/api/webhooks")
async def api_list_webhooks():
    """List registered webhooks."""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM alert_rules WHERE user_id = 'webhook' ORDER BY created_at DESC"
    ).fetchall()
    webhooks = []
    for r in rows:
        channels = json.loads(r['channels'])
        webhooks.append({
            'webhook_id': r['rule_id'],
            'name': r['name'],
            'conditions': json.loads(r['conditions']),
            'url': channels[0].get('url') if channels else None,
            'active': bool(r['active']),
            'created_at': r['created_at'],
        })
    return JSONResponse({'webhooks': webhooks})


# ── Iter 44: Admin Monitoring ─────────────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
async def admin_page():
    """Admin monitoring dashboard."""
    t0 = time.perf_counter()
    conn = get_db()

    # System stats (handle missing tables gracefully)
    def safe_count(table):
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except Exception:
            return 0

    total_items = safe_count("procurement_items")
    total_snapshots = safe_count("snapshots")
    total_changes = safe_count("changes")
    total_pipeline = safe_count("item_pipeline")
    total_alerts = safe_count("alert_rules")

    latest_item = conn.execute("SELECT MAX(detected_at) FROM procurement_items").fetchone()[0] or 'N/A'

    # DB size
    db_size_bytes = os.path.getsize(str(DB_PATH)) if DB_PATH.exists() else 0
    db_size_mb = round(db_size_bytes / (1024 * 1024), 1)

    # Items per day (last 7 days)
    daily_rows = conn.execute("""
        SELECT strftime('%Y-%m-%d', detected_at) as day, COUNT(*) as cnt
        FROM procurement_items WHERE detected_at > datetime('now', '-7 days')
        GROUP BY day ORDER BY day
    """).fetchall()
    daily_html = ''.join(
        f'<div style="display:flex;justify-content:space-between;padding:0.3rem 0;border-bottom:1px solid var(--border)">'
        f'<span>{r["day"]}</span><strong>{r["cnt"]:,}件</strong></div>'
        for r in daily_rows
    ) or '<div style="color:var(--muted)">データなし</div>'

    # Scheduler status
    sched_html = ''
    for job_name, info in _scheduler_jobs.items():
        sched_html += f'''
        <div style="display:flex;justify-content:space-between;padding:0.3rem 0;border-bottom:1px solid var(--border)">
          <span>{job_name}</span>
          <span style="font-size:0.8rem;color:var(--muted)">{info.get("last_run", "未実行")[:19]}</span>
          <span>{info.get("result", "-")}</span>
        </div>'''
    if not sched_html:
        sched_html = '<div style="color:var(--muted)">スケジューラ未実行</div>'

    elapsed = time.perf_counter() - t0

    content = f"""
    <h2>管理者ダッシュボード</h2>

    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:1rem;margin-bottom:1.5rem">
      <div class="kpi"><div class="kpi-label">総案件数</div><div class="kpi-value">{total_items:,}</div></div>
      <div class="kpi"><div class="kpi-label">スナップショット</div><div class="kpi-value">{total_snapshots:,}</div></div>
      <div class="kpi"><div class="kpi-label">変更検出</div><div class="kpi-value">{total_changes:,}</div></div>
      <div class="kpi"><div class="kpi-label">パイプライン</div><div class="kpi-value">{total_pipeline:,}</div></div>
      <div class="kpi"><div class="kpi-label">アラートルール</div><div class="kpi-value">{total_alerts}</div></div>
      <div class="kpi"><div class="kpi-label">DB サイズ</div><div class="kpi-value">{db_size_mb} MB</div></div>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem">
      <div class="tbl-wrap" style="padding:1rem">
        <div style="font-weight:700;margin-bottom:0.8rem">日別取り込み (7日間)</div>
        {daily_html}
      </div>
      <div class="tbl-wrap" style="padding:1rem">
        <div style="font-weight:700;margin-bottom:0.8rem">スケジューラ状況</div>
        {sched_html}
      </div>
    </div>

    <div class="tbl-wrap" style="padding:1rem;margin-bottom:1.5rem">
      <div style="font-weight:700;margin-bottom:0.8rem">システム情報</div>
      <div style="font-size:0.85rem;line-height:2">
        <div>最新データ: <strong>{latest_item}</strong></div>
        <div>DB パス: <code>{DB_PATH}</code></div>
        <div>API エンドポイント: 30+</div>
        <div>Python: 3.12</div>
      </div>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:0.8rem;margin-bottom:1.5rem">
      <a href="/api/health" class="kpi" style="text-decoration:none;text-align:center">
        <div class="kpi-label">ヘルスチェック</div>
        <div style="color:#16a34a;font-weight:700">GET /api/health</div>
      </a>
      <a href="/api/data-quality" class="kpi" style="text-decoration:none;text-align:center">
        <div class="kpi-label">データ品質</div>
        <div style="color:#2563eb;font-weight:700">GET /api/data-quality</div>
      </a>
      <a href="/api/scheduler/status" class="kpi" style="text-decoration:none;text-align:center">
        <div class="kpi-label">スケジューラ</div>
        <div style="color:#7c3aed;font-weight:700">GET /api/scheduler/status</div>
      </a>
    </div>
    """
    return layout("管理", content, timing=elapsed)


# ── Iter 45: Enhanced Health Check ────────────────────────────────────────

@app.get("/api/v3/health")
async def api_v3_health():
    """Enhanced health check with system details."""
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    latest = conn.execute("SELECT MAX(detected_at) FROM procurement_items").fetchone()[0]
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()

    return JSONResponse({
        'status': 'healthy',
        'version': '3.0',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'database': {
            'total_items': total,
            'latest_item': latest,
            'tables': [t['name'] for t in tables],
            'size_mb': round(os.path.getsize(str(DB_PATH)) / (1024 * 1024), 1) if DB_PATH.exists() else 0,
        },
        'scheduler': _scheduler_jobs,
        'endpoints': {
            'pages': ['/', '/items', '/pipeline', '/municipalities', '/market', '/analytics', '/alerts', '/changes', '/admin'],
            'api_v3': ['/api/v3/items', '/api/v3/health'],
            'intelligence': ['/api/vendors', '/api/cycles', '/api/price-range', '/api/win-probability/{id}',
                           '/api/budget-forecast/{code}', '/api/brief/{code}', '/api/trends'],
            'decision': ['/api/scorecard-enhanced/{id}', '/api/checklist/{cat}', '/api/timeline/{id}',
                        '/api/calendar', '/api/anomalies'],
            'reports': ['/api/report/municipality/{code}', '/api/report/market', '/api/cross-analysis'],
        },
    })


# ── Iter 46: Notification Center ──────────────────────────────────────────

@app.get("/api/notifications")
async def api_notifications(limit: int = Query(20)):
    """Get notification feed for current user."""
    conn = get_db()
    notifications = []

    # Recent alert matches
    try:
        matches = conn.execute("""
            SELECT am.*, ar.name as rule_name
            FROM alert_matches am
            JOIN alert_rules ar ON am.rule_id = ar.rule_id
            ORDER BY am.matched_at DESC LIMIT ?
        """, (limit,)).fetchall()
        for m in matches:
            notifications.append({
                'type': 'alert_match',
                'rule_name': m['rule_name'],
                'item_id': m['item_id'],
                'matched_at': m['matched_at'],
            })
    except Exception:
        pass

    # Recent pipeline changes
    try:
        pipeline_changes = conn.execute("""
            SELECT ph.*, p.item_id
            FROM pipeline_history ph
            JOIN item_pipeline p ON ph.pipeline_id = p.item_id
            ORDER BY ph.changed_at DESC LIMIT ?
        """, (limit,)).fetchall()
        for p in pipeline_changes:
            notifications.append({
                'type': 'pipeline_change',
                'item_id': p['item_id'],
                'from_status': p['from_status'],
                'to_status': p['to_status'],
                'changed_at': p['changed_at'],
            })
    except Exception:
        pass

    # Sort all by time
    notifications.sort(key=lambda x: x.get('matched_at') or x.get('changed_at') or '', reverse=True)

    return JSONResponse({'notifications': notifications[:limit], 'total': len(notifications)})


# ── Iter 47-48: Search Enhancement ────────────────────────────────────────

@app.get("/api/search")
async def api_search(
    q: str = Query(..., min_length=1),
    scope: str = Query('items', description='items|municipalities|all'),
    limit: int = Query(20, ge=1, le=100),
):
    """Global search across items and municipalities."""
    conn = get_db()
    results = []

    if scope in ('items', 'all'):
        terms = q.split()[:5]
        where_parts = []
        params = []
        for t in terms:
            where_parts.append("(title LIKE ? OR method LIKE ?)")
            params.extend([f'%{t}%', f'%{t}%'])

        where = ' AND '.join(where_parts)
        rows = conn.execute(f"""
            SELECT item_id, muni_code, title, category, deadline, detected_at
            FROM procurement_items WHERE {where}
            ORDER BY detected_at DESC LIMIT ?
        """, params + [limit]).fetchall()

        for r in rows:
            name, pref = resolve_name(r['muni_code'])
            results.append({
                'type': 'item',
                'item_id': r['item_id'],
                'title': r['title'],
                'muni_code': r['muni_code'],
                'muni_name': name,
                'category': r['category'],
                'deadline': r['deadline'],
                'detected_at': r['detected_at'],
            })

    if scope in ('municipalities', 'all'):
        for mc, info in MASTER.items():
            name = info.get('name', '')
            if q in name or q in mc:
                _, pref = resolve_name(mc)
                count = conn.execute(
                    "SELECT COUNT(*) FROM procurement_items WHERE muni_code = ?", (mc,)
                ).fetchone()[0]
                results.append({
                    'type': 'municipality',
                    'muni_code': mc,
                    'name': name,
                    'prefecture': pref,
                    'item_count': count,
                })
                if len([r for r in results if r['type'] == 'municipality']) >= limit:
                    break

    return JSONResponse({'query': q, 'scope': scope, 'results': results[:limit], 'total': len(results)})


# ── Iter 49: Batch Operations ─────────────────────────────────────────────

@app.post("/api/pipeline/batch")
async def api_pipeline_batch(request: Request):
    """Batch add/update pipeline items."""
    conn = get_db()
    data = await request.json()
    action = data.get('action', 'add')
    item_ids = data.get('item_ids', [])

    if not item_ids:
        return JSONResponse({'error': 'No item_ids provided'}, status_code=400)

    count = 0
    now = datetime.now(timezone.utc).isoformat()

    if action == 'add':
        for iid in item_ids[:50]:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO item_pipeline (item_id, status, priority, created_at, updated_at)
                    VALUES (?, 'discovered', 'normal', ?, ?)
                """, (iid, now, now))
                count += 1
            except Exception:
                pass
    elif action == 'update_status':
        new_status = data.get('status', 'reviewing')
        for iid in item_ids[:50]:
            conn.execute(
                "UPDATE item_pipeline SET status = ?, updated_at = ? WHERE item_id = ?",
                (new_status, now, iid)
            )
            count += 1
    elif action == 'remove':
        for iid in item_ids[:50]:
            conn.execute("DELETE FROM item_pipeline WHERE item_id = ?", (iid,))
            count += 1

    conn.commit()
    return JSONResponse({'ok': True, 'action': action, 'count': count})


# ── Iter 50: API Documentation Page ──────────────────────────────────────

@app.get("/api-docs", response_class=HTMLResponse)
async def api_docs_page():
    """API documentation page."""
    endpoints = [
        ('Core', [
            ('GET', '/api/v3/items', '案件一覧（ページング・フィルタ対応）'),
            ('GET', '/api/v3/health', 'ヘルスチェック（詳細版）'),
            ('GET', '/api/search', 'グローバル検索'),
            ('GET', '/api/export', 'CSV/簡易CSVエクスポート'),
        ]),
        ('Intelligence', [
            ('GET', '/api/vendors', 'ベンダー分析'),
            ('GET', '/api/cycles', '調達サイクル検出'),
            ('GET', '/api/price-range', '価格帯推定'),
            ('GET', '/api/win-probability/{id}', '落札確率推定'),
            ('GET', '/api/budget-forecast/{code}', '予算予測'),
            ('GET', '/api/brief/{code}', '営業ブリーフ自動生成'),
            ('GET', '/api/trends', '市場トレンド'),
            ('GET', '/api/ranking', '自治体ランキング'),
        ]),
        ('Decision Support', [
            ('GET', '/api/scorecard/{id}', 'GO/NO-GOスコアカード'),
            ('GET', '/api/scorecard-enhanced/{id}', '拡張スコアカード'),
            ('GET', '/api/checklist/{cat}', '入札チェックリスト'),
            ('GET', '/api/timeline/{id}', '入札準備タイムライン'),
            ('GET', '/api/calendar', '締切カレンダー'),
            ('GET', '/api/anomalies', '異常検知'),
            ('GET', '/api/similar/{id}', '類似案件'),
        ]),
        ('Pipeline', [
            ('GET', '/api/pipeline/{id}', 'パイプライン取得'),
            ('PUT', '/api/pipeline/{id}', 'パイプライン更新'),
            ('POST', '/api/pipeline/add/{id}', 'パイプライン追加'),
            ('POST', '/api/pipeline/batch', 'バッチ操作'),
            ('GET', '/api/pipeline-analytics', 'パイプライン分析'),
        ]),
        ('Alerts', [
            ('POST', '/api/alerts', 'アラートルール作成'),
            ('PUT', '/api/alerts/{id}', 'アラートルール更新'),
            ('DELETE', '/api/alerts/{id}', 'アラートルール削除'),
            ('GET', '/api/alerts/matches', 'マッチ案件取得'),
        ]),
        ('Reports', [
            ('GET', '/api/report/municipality/{code}', '自治体レポート'),
            ('GET', '/api/report/market', '市場レポート'),
            ('GET', '/api/cross-analysis', 'クロスカテゴリ分析'),
            ('GET', '/api/category-performance', 'カテゴリパフォーマンス'),
            ('GET', '/api/prefecture-analytics', '都道府県分析'),
            ('GET', '/api/muni-summary/{code}', '自治体サマリ'),
        ]),
        ('Widgets', [
            ('GET', '/api/widgets/kpi-summary', 'KPIサマリ'),
            ('GET', '/api/widgets/deadline-urgent', '緊急締切'),
            ('GET', '/api/widgets/new-items', '新着案件'),
        ]),
        ('System', [
            ('GET', '/api/health', 'ヘルスチェック'),
            ('GET', '/api/scheduler/status', 'スケジューラ状態'),
            ('GET', '/api/data-quality', 'データ品質'),
            ('GET', '/api/notifications', '通知一覧'),
            ('POST', '/api/webhooks', 'Webhook登録'),
            ('GET', '/api/webhooks', 'Webhook一覧'),
        ]),
    ]

    sections_html = ''
    total_endpoints = 0
    for section_name, eps in endpoints:
        total_endpoints += len(eps)
        rows = ''.join(
            f'<tr><td><span style="background:{"#16a34a" if m=="GET" else "#d97706" if m=="POST" else "#2563eb" if m=="PUT" else "#dc2626"};'
            f'color:white;padding:0.15rem 0.4rem;border-radius:3px;font-size:0.75rem;font-weight:700">{m}</span></td>'
            f'<td><code style="font-size:0.85rem">{path}</code></td>'
            f'<td style="color:var(--muted);font-size:0.85rem">{desc}</td></tr>'
            for m, path, desc in eps
        )
        sections_html += f'''
        <div class="tbl-wrap" style="padding:1rem;margin-bottom:1rem">
          <div style="font-weight:700;margin-bottom:0.8rem;font-size:1rem">{section_name}</div>
          <table style="width:100%;border-collapse:collapse">
            <thead><tr>
              <th style="text-align:left;padding:0.4rem;border-bottom:2px solid var(--border);width:80px">Method</th>
              <th style="text-align:left;padding:0.4rem;border-bottom:2px solid var(--border)">Endpoint</th>
              <th style="text-align:left;padding:0.4rem;border-bottom:2px solid var(--border)">Description</th>
            </tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </div>'''

    content = f"""
    <h2>API Documentation</h2>
    <div style="display:flex;gap:1rem;margin-bottom:1.5rem">
      <div class="kpi"><div class="kpi-label">エンドポイント数</div><div class="kpi-value">{total_endpoints}</div></div>
      <div class="kpi"><div class="kpi-label">バージョン</div><div class="kpi-value">v3.0</div></div>
      <div class="kpi"><div class="kpi-label">フォーマット</div><div class="kpi-value">JSON</div></div>
    </div>
    <div style="background:var(--card);border:1px solid var(--border);border-radius:8px;padding:1rem;margin-bottom:1.5rem;font-size:0.85rem">
      <strong>Base URL:</strong> <code>http://78.46.57.151:8009</code><br>
      <strong>Format:</strong> All responses are JSON.<br>
      <strong>Pagination:</strong> Use <code>page</code> and <code>per_page</code> parameters on v3 endpoints.
    </div>
    {sections_html}
    """
    return layout("API Docs", content)


# ══════════════════════════════════════════════════════════════════════════════
# Auto Bidding System — Pages + /api/v4/ routes
# ══════════════════════════════════════════════════════════════════════════════

# ── Bid Workbench page ──────────────────────────────────────────────────────

@app.get("/bid-workbench/{project_id}", response_class=HTMLResponse)
async def bid_workbench_page(project_id: int, tab: str = Query("overview")):
    t0 = time.perf_counter()
    conn = get_db()
    project = bid_engine.get_project(conn, project_id)
    if not project:
        return HTMLResponse("<h1>Project not found</h1>", status_code=404)

    item = project.get("item") or {}
    context = json.loads(project.get("context_json") or "{}")
    status_info = bid_engine.BID_STATUS_MAP.get(project["status"], ("","","#6b7280",""))
    name, pref = resolve_name(item.get("muni_code", ""), item.get("raw_json"))

    questions = bid_engine.list_questions(conn, project_id)
    drafts = bid_engine.list_drafts(conn, project_id)
    artifacts = bid_engine.list_artifacts(conn, project_id)
    templates = bid_engine.list_templates(conn)

    # Tabs
    tabs = [("overview", "概要"), ("questions", "質問"), ("proposal", "提案書"), ("artifacts", "提出物")]
    tab_html = ''.join(
        f'<a href="/bid-workbench/{project_id}?tab={t}" class="{"active" if t == tab else ""}" '
        f'style="padding:0.5rem 1rem;border-bottom:{"2px solid var(--primary)" if t == tab else "2px solid transparent"};'
        f'text-decoration:none;font-size:0.85rem;font-weight:{"700" if t == tab else "400"};color:{"var(--primary)" if t == tab else "var(--muted)"}">{l}</a>'
        for t, l in tabs
    )

    # Overview tab
    if tab == "overview":
        tab_content = f"""
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:1.5rem">
          <div>
            <h3 style="font-size:1rem;margin-bottom:0.8rem">案件情報</h3>
            <div style="display:grid;grid-template-columns:100px 1fr;gap:0.4rem;font-size:0.85rem">
              <span style="color:var(--muted)">案件名:</span><span style="font-weight:600">{esc(item.get('title',''))}</span>
              <span style="color:var(--muted)">発注者:</span><span>{esc(pref)} {esc(name)}</span>
              <span style="color:var(--muted)">カテゴリ:</span><span>{esc(context.get('category',''))}</span>
              <span style="color:var(--muted)">方式:</span><span>{esc(context.get('method',''))}</span>
              <span style="color:var(--muted)">締切:</span><span>{esc(context.get('deadline','未定'))}</span>
              <span style="color:var(--muted)">金額:</span><span>{esc(context.get('amount','未定'))}</span>
              <span style="color:var(--muted)">部署:</span><span>{esc(context.get('department',''))}</span>
              <span style="color:var(--muted)">URL:</span><span>{'<a href="' + esc(context.get("url","")) + '" target="_blank">' + esc(context.get("url","")[:60]) + '</a>' if context.get("url") else '-'}</span>
            </div>
          </div>
          <div>
            <h3 style="font-size:1rem;margin-bottom:0.8rem">プロジェクト状態</h3>
            <div style="display:grid;grid-template-columns:100px 1fr;gap:0.4rem;font-size:0.85rem">
              <span style="color:var(--muted)">ステータス:</span><span class="badge" style="background:{status_info[2]};color:white;font-size:0.75rem;width:fit-content">{status_info[1]}</span>
              <span style="color:var(--muted)">種別:</span><span>{esc(project.get('project_type',''))}</span>
              <span style="color:var(--muted)">構造優先:</span><span>{esc(project.get('structural_priority','—'))}</span>
              <span style="color:var(--muted)">自動化:</span><span>{str(int(project.get('automation_readiness',0))) + '%' if project.get('automation_readiness') else '—'}</span>
              <span style="color:var(--muted)">優先度:</span><span>{esc(project.get('priority',''))}</span>
              <span style="color:var(--muted)">GO/NO-GO:</span><span>{esc(project.get('go_nogo','pending'))}</span>
              <span style="color:var(--muted)">質問数:</span><span>{project.get('question_count',0)}</span>
              <span style="color:var(--muted)">提案書:</span><span>{project.get('draft_count',0)}版</span>
              <span style="color:var(--muted)">成果物:</span><span>{project.get('artifact_count',0)}件</span>
            </div>
            <div style="margin-top:1rem">
              <div style="font-size:0.78rem;color:var(--muted);margin-bottom:0.3rem">遷移可能なステータス:</div>
              <div style="display:flex;gap:0.5rem;flex-wrap:wrap">
                {''.join(f'<button class="btn {"btn-primary" if s!="abandoned" else "btn-outline"} btn-sm" style="{"color:var(--danger);border-color:var(--danger)" if s=="abandoned" else ""}" onclick="advanceProject({project_id},&quot;{s}&quot;)">{lbl}</button>' for s,lbl in project.get("allowed_transitions",[])) or '<span style="font-size:0.8rem;color:var(--muted)">遷移先なし（終了状態）</span>'}
              </div>
            </div>
            {f'<div style="margin-top:0.8rem;padding:0.5rem;background:#fef3c7;border-radius:6px;font-size:0.8rem"><strong>[要確認]</strong> 提案書に未解決項目が <strong>{project.get("unresolved_count",0)}件</strong> あります</div>' if project.get("unresolved_count",0)>0 else ''}
          </div>
        </div>
        <div style="margin-top:1.5rem">
          <h3 style="font-size:1rem;margin-bottom:0.5rem">メモ</h3>
          <textarea id="proj-notes" rows="3" style="width:100%;padding:0.5rem;border:1px solid var(--border);border-radius:6px;font-size:0.85rem;resize:vertical">{esc(project.get('notes',''))}</textarea>
          <button class="btn btn-outline btn-sm" style="margin-top:0.3rem" onclick="saveNotes({project_id})">メモ保存</button>
        </div>
        <div style="margin-top:1.5rem;padding:1rem;background:#fef2f2;border:1px solid #fca5a5;border-radius:8px">
          <h3 style="font-size:1rem;margin-bottom:0.8rem;color:#991b1b">監査スコア</h3>
          <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:0.6rem;font-size:0.83rem">
            <div style="padding:0.5rem;background:white;border-radius:6px;text-align:center">
              <div style="font-size:0.7rem;color:#991b1b;margin-bottom:0.2rem">Hard Fail Risk</div>
              <div style="font-size:1.2rem;font-weight:700;color:{'#dc2626' if (project.get('hard_fail_risk_score') or 0) > 50 else '#f59e0b' if (project.get('hard_fail_risk_score') or 0) > 0 else '#22c55e'}">{project.get('hard_fail_risk_score') or 0}</div>
            </div>
            <div style="padding:0.5rem;background:white;border-radius:6px;text-align:center">
              <div style="font-size:0.7rem;color:#991b1b;margin-bottom:0.2rem">Adjusted EV</div>
              <div style="font-size:1.1rem;font-weight:700;color:#1e40af">{("\u00a5" + "{:,.0f}".format(project.get('adjusted_total_ev') or 0)) if project.get('adjusted_total_ev') else '\u2014'}</div>
            </div>
            <div style="padding:0.5rem;background:white;border-radius:6px;text-align:center">
              <div style="font-size:0.7rem;color:#991b1b;margin-bottom:0.2rem">Reality Gap</div>
              <div style="font-size:1.2rem;font-weight:700;color:{'#dc2626' if project.get('reality_gap_flag') else '#22c55e'}">{'\u26a0\ufe0f GAP' if project.get('reality_gap_flag') else '\u2714\ufe0f OK'}</div>
            </div>
            <div style="padding:0.5rem;background:white;border-radius:6px;text-align:center">
              <div style="font-size:0.7rem;color:#991b1b;margin-bottom:0.2rem">PDF Required</div>
              <div style="font-size:0.9rem;font-weight:600;color:{'#d97706' if project.get('pdf_required_flag') else '#6b7280'}">{'\U0001f4c4 YES' if project.get('pdf_required_flag') else 'NO'}</div>
            </div>
            <div style="padding:0.5rem;background:white;border-radius:6px;text-align:center">
              <div style="font-size:0.7rem;color:#991b1b;margin-bottom:0.2rem">Attachment</div>
              <div style="font-size:0.9rem;font-weight:600;color:{'#d97706' if project.get('attachment_priority_flag') else '#6b7280'}">{'\U0001f4ce YES' if project.get('attachment_priority_flag') else 'NO'}</div>
            </div>
            <div style="padding:0.5rem;background:white;border-radius:6px;text-align:center">
              <div style="font-size:0.7rem;color:#991b1b;margin-bottom:0.2rem">Submission</div>
              <div style="font-size:0.9rem;font-weight:600;color:{'#22c55e' if project.get('submission_ready') else '#dc2626'}">{'READY' if project.get('submission_ready') else 'NOT READY'}</div>
            </div>
          </div>
        </div>
        <div style="margin-top:1rem">
          <h3 style="font-size:1rem;margin-bottom:0.5rem">仕様解析 (SpecParser)</h3>
          <button class="btn btn-outline btn-sm" onclick="runSpecParse({project.get('item_id', 0)})">仕様解析実行</button>
          <div id="spec-parse-result" style="margin-top:0.5rem;font-size:0.8rem;color:var(--muted)"></div>
        </div>"""

    elif tab == "questions":
        def _q_ig_bar(score):
            if score is None: return ''
            pct = int(score * 100)
            color = '#22c55e' if score >= 0.7 else '#f59e0b' if score >= 0.4 else '#9ca3af'
            return f'<div style="display:flex;align-items:center;gap:0.3rem"><div style="width:50px;height:6px;background:var(--border);border-radius:3px;overflow:hidden"><div style="width:{pct}%;height:100%;background:{color}"></div></div><span style="font-size:0.7rem;color:{color}">{score:.2f}</span></div>'

        def _q_sub_scores(json_str):
            if not json_str: return ''
            try:
                ss = json.loads(json_str) if isinstance(json_str, str) else json_str
                return f'<div style="font-size:0.65rem;color:var(--muted);margin-top:0.2rem">曖昧={ss.get("ambiguity",0):.1f} 影響={ss.get("proposal_impact",0):.1f} 評価={ss.get("evaluation_exposure",0):.1f}</div>'
            except Exception: return ''

        cat_colors = {"evaluation":"#7c3aed","scope":"#2563eb","deliverables":"#0ea5e9","eligibility":"#d97706","schedule":"#0891b2","pricing":"#22c55e"}

        def _q_cat_badge(cat):
            if not cat: return ''
            c = cat_colors.get(cat, "#9ca3af")
            return '<span class="badge" style="font-size:0.65rem;background:' + c + ';color:white">' + esc(cat) + '</span>'

        def _q_effectiveness(q):
            us = q.get("usefulness_score")
            used = q.get("was_used_in_proposal")
            if us is None and used is None:
                return ''
            parts = []
            if used:
                parts.append('<span class="badge" style="font-size:0.6rem;background:#22c55e;color:white">提案反映</span>')
            if us is not None:
                us_color = '#22c55e' if us >= 0.6 else '#f59e0b' if us >= 0.3 else '#9ca3af'
                parts.append(f'<span style="font-size:0.6rem;color:{us_color}">有効度{us:.2f}</span>')
            return '<div style="margin-top:0.2rem">' + ' '.join(parts) + '</div>' if parts else ''

        q_rows = ''.join(
            f'<tr><td style="font-size:0.83rem">{esc(q["question_text"])}'
            f'{_q_sub_scores(q.get("sub_scores_json",""))}'
            f'{_q_effectiveness(q)}</td>'
            f'<td style="text-align:center">{_q_ig_bar(q.get("info_gain_score"))}</td>'
            f'<td style="text-align:center">{_q_cat_badge(q.get("question_category",""))}</td>'
            f'<td style="font-size:0.78rem;color:var(--muted)">{esc(q.get("rationale","")[:80])}</td>'
            f'<td><textarea class="q-ans" data-qid="{q["question_id"]}" rows="2" style="width:100%;font-size:0.8rem;padding:0.3rem;border:1px solid var(--border);border-radius:4px">{esc(q.get("answer","") or "")}</textarea></td>'
            f'<td><button class="btn btn-outline btn-sm" style="font-size:0.7rem" onclick="saveAnswer({q["question_id"]})">保存</button>'
            f' <button class="btn btn-outline btn-sm" style="font-size:0.7rem;color:var(--danger)" onclick="deleteQuestion({q["question_id"]},{project_id})">削</button></td></tr>'
            for q in questions
        ) if questions else '<tr><td colspan="7" style="text-align:center;color:var(--muted);padding:1rem">質問なし。「自動生成」ボタンで生成できます。</td></tr>'

        avg_ig = round(sum(q.get('info_gain_score',0) or 0 for q in questions) / max(len(questions),1), 2) if questions else 0
        high_ig = sum(1 for q in questions if (q.get('info_gain_score') or 0) >= 0.7)

        tab_content = f"""
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem">
          <div>
            <h3 style="font-size:1rem;display:inline">質問事項 ({len(questions)}件)</h3>
            <span style="font-size:0.78rem;color:var(--muted);margin-left:0.8rem">平均IG: {avg_ig:.2f} &middot; 高IG(≥0.7): {high_ig}件</span>
          </div>
          <div style="display:flex;gap:0.5rem">
            <button class="btn btn-primary btn-sm" onclick="generateQuestions({project_id})">AI自動生成</button>
            <button class="btn btn-outline btn-sm" onclick="document.getElementById('add-q-form').style.display='block'">手動追加</button>
          </div>
        </div>
        <div id="add-q-form" style="display:none;margin-bottom:1rem;padding:1rem;background:var(--hover);border-radius:8px">
          <input id="new-q-text" type="text" placeholder="質問文" style="width:100%;padding:0.4rem;border:1px solid var(--border);border-radius:6px;margin-bottom:0.5rem">
          <button class="btn btn-primary btn-sm" onclick="addQuestion({project_id})">追加</button>
        </div>
        <div class="tbl-wrap">
          <table style="width:100%">
            <thead><tr>
              <th>質問</th><th style="width:70px">IG Score</th><th style="width:70px">カテゴリ</th><th style="width:120px">意図</th><th style="width:180px">回答</th><th style="width:80px">操作</th>
            </tr></thead>
            <tbody>{q_rows}</tbody>
          </table>
        </div>"""

    elif tab == "proposal":
        tmpl_options = ''.join(
            f'<option value="{t["template_id"]}" {"selected" if t["template_id"]==project.get("template_id") else ""}>{esc(t["name"])}</option>'
            for t in templates
        )
        latest = drafts[0] if drafts else None
        draft_preview = f'<div style="background:var(--hover);padding:1rem;border-radius:8px;font-size:0.85rem;white-space:pre-wrap;max-height:500px;overflow-y:auto">{esc(latest["content_markdown"])}</div>' if latest else '<p style="color:var(--muted)">提案書未作成。テンプレートを選んで「AI生成」してください。</p>'
        versions_html = ''.join(
            f'<span class="badge" style="font-size:0.7rem;margin-right:0.3rem;{"background:var(--primary);color:white" if d==latest else ""}">v{d["version"]} {"[確定]" if d.get("status")=="frozen" else ""}</span>'
            for d in drafts
        )

        # Variable map display
        vmap_html = ''
        if latest and latest.get("variable_map_json"):
            try:
                vmap = json.loads(latest["variable_map_json"])
                if vmap:
                    src_colors = {"spec":"#22c55e","municipality_context":"#2563eb","inferred":"#d97706","question_answer":"#0ea5e9","placeholder":"#ef4444"}
                    src_labels = {"spec":"仕様書","municipality_context":"自治体情報","inferred":"推論","question_answer":"Q&A回答","placeholder":"[要確認]"}
                    vmap_rows = ''.join(
                        f'<tr><td style="font-size:0.78rem;font-weight:600">{esc(k)}</td>'
                        f'<td style="font-size:0.78rem">{esc(str(v.get("value",""))[:80]) if isinstance(v,dict) else esc(str(v)[:80])}</td>'
                        f'<td><span class="badge" style="font-size:0.65rem;background:{src_colors.get(v.get("source","") if isinstance(v,dict) else "","#9ca3af")};color:white">'
                        f'{src_labels.get(v.get("source","") if isinstance(v,dict) else "","不明")}</span></td></tr>'
                        for k,v in vmap.items()
                    )
                    unresolved = sum(1 for v in vmap.values() if isinstance(v,dict) and v.get("source")=="placeholder")
                    vmap_html = f"""
                    <div style="margin-top:1rem">
                      <h4 style="font-size:0.9rem;margin-bottom:0.5rem">変数マップ ({len(vmap)}変数, {f'<span style="color:#ef4444">{unresolved}件 要確認</span>' if unresolved else '<span style="color:#22c55e">全て充足</span>'})</h4>
                      <table style="width:100%;font-size:0.8rem"><thead><tr><th>変数</th><th>値</th><th>ソース</th></tr></thead><tbody>{vmap_rows}</tbody></table>
                    </div>"""
            except (json.JSONDecodeError, TypeError):
                pass

        # Applied patterns display
        patterns_html = ''
        if latest and latest.get("applied_patterns_json"):
            try:
                ap = json.loads(latest["applied_patterns_json"])
                if ap:
                    patterns_html = '<div style="margin-top:0.8rem;font-size:0.78rem"><strong>適用パターン:</strong> ' + ', '.join(
                        f'<span class="badge" style="font-size:0.65rem;background:#7c3aed;color:white">{esc(p.get("type",""))}:{esc(p.get("key",""))}</span>'
                        for p in ap
                    ) + '</div>'
            except (json.JSONDecodeError, TypeError):
                pass

        # Practicality score display
        pract_html = ''
        if latest and latest.get("practicality_score") is not None:
            ps = latest["practicality_score"]
            ps_color = '#22c55e' if ps >= 70 else '#f59e0b' if ps >= 40 else '#ef4444'
            pbd = {}
            try:
                pbd = json.loads(latest.get("practicality_breakdown_json") or "{}")
            except (json.JSONDecodeError, TypeError):
                pass
            axis_labels = {"unresolved_penalty":"未解決","template_fit":"テンプレ適合","specificity":"固有性","answer_reflection":"回答反映","requirement_coverage":"要件カバー"}
            pbd_bars = ''.join(
                f'<div style="display:flex;align-items:center;gap:0.3rem;margin-bottom:2px">'
                f'<span style="font-size:0.65rem;width:65px;text-align:right;color:var(--muted)">{axis_labels.get(k,k)}</span>'
                f'<div style="width:60px;height:5px;background:var(--border);border-radius:2px;overflow:hidden"><div style="width:{v}%;height:100%;background:{ps_color}"></div></div>'
                f'<span style="font-size:0.6rem;color:var(--muted)">{v}</span></div>'
                for k,v in pbd.items() if k not in ("unresolved_count","total_vars")
            )
            pract_html = f'<div style="display:inline-flex;align-items:center;gap:0.5rem;margin-left:0.8rem;padding:0.3rem 0.6rem;background:var(--hover);border-radius:6px;border:1px solid {ps_color}"><span style="font-size:1.1rem;font-weight:700;color:{ps_color}">{ps}</span><span style="font-size:0.7rem;color:var(--muted)">/100</span><div style="margin-left:0.3rem">{pbd_bars}</div></div>'

        tab_content = f"""
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem">
          <div style="display:flex;align-items:center">
            <h3 style="font-size:1rem">提案書 ({len(drafts)}版)</h3>
            {pract_html}
          </div>
          <div style="display:flex;gap:0.5rem;align-items:center">
            <select id="tmpl-sel" style="font-size:0.8rem;padding:0.3rem;border:1px solid var(--border);border-radius:6px">{tmpl_options}</select>
            <button class="btn btn-primary btn-sm" onclick="generateProposal({project_id})">AI生成</button>
            {f'<button class="btn btn-outline btn-sm" onclick="scoreProposal({latest["draft_id"]})">スコア算出</button>' if latest else ""}
            {f'<button class="btn btn-outline btn-sm" onclick="freezeDraft({latest["draft_id"]},{project_id})">確定</button>' if latest and latest.get("status")!="frozen" else ""}
          </div>
        </div>
        <div style="margin-bottom:0.5rem">{versions_html}</div>
        {patterns_html}
        {draft_preview}
        {vmap_html}"""

    else:  # artifacts
        art_rows = ''.join(
            f'<tr><td>{esc(a["artifact_type"])}</td><td>{esc(a["file_name"])}</td>'
            f'<td style="font-size:0.78rem">{esc((a.get("content","") or "")[:100])}</td>'
            f'<td><a href="/api/v4/artifacts/download/{a["artifact_id"]}" class="btn btn-outline btn-sm" style="font-size:0.7rem">DL</a></td></tr>'
            for a in artifacts
        ) if artifacts else '<tr><td colspan="4" style="text-align:center;color:var(--muted);padding:1rem">成果物なし</td></tr>'

        tab_content = f"""
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem">
          <h3 style="font-size:1rem">提出物 ({len(artifacts)}件)</h3>
          <div style="display:flex;gap:0.5rem;flex-wrap:wrap">
            <button class="btn btn-primary btn-sm" onclick="genChecklist({project_id})">チェックリスト生成</button>
            <button class="btn btn-outline btn-sm" onclick="genCover({project_id})">送付状生成</button>
            <button class="btn btn-outline btn-sm" onclick="genSummary({project_id})">サマリー生成</button>
            <button class="btn btn-outline btn-sm" style="border-color:#7c3aed;color:#7c3aed" onclick="genCompliance({project_id})">適合マトリクス</button>
            <button class="btn btn-outline btn-sm" style="border-color:#0891b2;color:#0891b2" onclick="genSubmissionNotes({project_id})">提出前メモ</button>
          </div>
        </div>
        <div class="tbl-wrap">
          <table style="width:100%">
            <thead><tr><th>種別</th><th>ファイル名</th><th>内容</th><th>操作</th></tr></thead>
            <tbody>{art_rows}</tbody>
          </table>
        </div>"""

    elapsed = time.perf_counter() - t0
    content = f"""
    <div class="section">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem">
        <div>
          <a href="/pipeline" style="font-size:0.78rem;color:var(--muted);text-decoration:none">&larr; パイプライン</a>
          <h2 style="margin:0.3rem 0">{esc((item.get('title','案件') or '案件')[:50])}</h2>
          <span style="font-size:0.8rem;color:var(--muted)">{esc(pref)} {esc(name)} &middot; PJ#{project_id}</span>
        </div>
        <span class="badge" style="background:{status_info[2]};color:white;font-size:0.85rem;padding:0.3rem 0.8rem">{status_info[1]}</span>
      </div>
      <div style="display:flex;border-bottom:1px solid var(--border);margin-bottom:1.5rem">{tab_html}</div>
      {tab_content}
    </div>
    <script>
    async function advanceProject(pid,targetStatus){{
      if(targetStatus==='abandoned'){{if(!confirm('この案件を見送りますか？'))return;}}
      const r=await fetch('/api/v4/bid-projects/'+pid+'/advance',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{target_status:targetStatus}})}});
      if(r.ok)location.reload();else{{const d=await r.json();showToast(d.detail||'遷移エラー');}}
    }}
    async function saveNotes(pid){{
      const notes=document.getElementById('proj-notes').value;
      const r=await fetch('/api/v4/bid-projects/'+pid,{{method:'PUT',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{notes}})}});
      if(r.ok)showToast('保存しました');else showToast('エラー');
    }}
    async function generateQuestions(pid){{
      showToast('質問を生成中...');
      const r=await fetch('/api/v4/questions/generate/'+pid,{{method:'POST'}});
      if(r.ok)location.reload();else showToast('生成エラー');
    }}
    async function saveAnswer(qid){{
      const ans=document.querySelector('[data-qid="'+qid+'"]').value;
      const r=await fetch('/api/v4/questions/'+qid,{{method:'PUT',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{answer:ans}})}});
      if(r.ok)showToast('保存しました');else showToast('エラー');
    }}
    async function deleteQuestion(qid,pid){{
      const r=await fetch('/api/v4/questions/'+qid,{{method:'DELETE'}});
      if(r.ok)location.reload();else showToast('エラー');
    }}
    async function addQuestion(pid){{
      const text=document.getElementById('new-q-text').value;
      if(!text)return;
      const r=await fetch('/api/v4/questions',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{project_id:pid,question_text:text}})}});
      if(r.ok)location.reload();else showToast('エラー');
    }}
    async function generateProposal(pid){{
      const tid=document.getElementById('tmpl-sel')?.value;
      if(tid)await fetch('/api/v4/bid-projects/'+pid,{{method:'PUT',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{template_id:parseInt(tid)}})}});
      showToast('提案書を生成中...');
      const r=await fetch('/api/v4/proposals/generate/'+pid,{{method:'POST'}});
      if(r.ok)location.reload();else showToast('生成エラー');
    }}
    async function freezeDraft(did,pid){{
      const r=await fetch('/api/v4/proposals/'+did+'/freeze',{{method:'POST'}});
      if(r.ok)location.reload();else showToast('エラー');
    }}
    async function genChecklist(pid){{
      showToast('チェックリスト生成中...');
      const r=await fetch('/api/v4/artifacts/generate-checklist/'+pid,{{method:'POST'}});
      if(r.ok)location.reload();else showToast('生成エラー');
    }}
    async function genCover(pid){{
      showToast('送付状生成中...');
      const r=await fetch('/api/v4/artifacts/generate-cover/'+pid,{{method:'POST'}});
      if(r.ok)location.reload();else showToast('生成エラー');
    }}
    async function genSummary(pid){{
      showToast('サマリー生成中...');
      const r=await fetch('/api/v4/artifacts/generate-summary/'+pid,{{method:'POST'}});
      if(r.ok)location.reload();else showToast('生成エラー');
    }}
    async function genCompliance(pid){{
      showToast('適合マトリクス生成中...');
      const r=await fetch('/api/v4/artifacts/generate-compliance/'+pid,{{method:'POST'}});
      if(r.ok)location.reload();else showToast('生成エラー');
    }}
    async function genSubmissionNotes(pid){{
      showToast('提出前メモ生成中...');
      const r=await fetch('/api/v4/artifacts/generate-submission-notes/'+pid,{{method:'POST'}});
      if(r.ok)location.reload();else showToast('生成エラー');
    }}
    async function scoreProposal(did){{
      showToast('実務スコア算出中...');
      const r=await fetch('/api/v4/proposals/'+did+'/score',{{method:'POST'}});
      if(r.ok){{const d=await r.json();showToast('スコア: '+(d.score||0)+'/100');setTimeout(()=>location.reload(),800);}}
      else showToast('エラー');
    }}
    async function runSpecParse(itemId){{
      showToast('仕様解析中...');
      const r=await fetch('/api/v4/spec-parse/'+itemId,{{method:'POST'}});
      if(r.ok){{const d=await r.json();const el=document.getElementById('spec-parse-result');
        el.innerHTML='<strong>解析完了</strong> coverage='+((d.source_coverage_score||0)*100).toFixed(0)+'% confidence='+((d.parser_confidence_score||0)*100).toFixed(0)+'%';
        showToast('仕様解析完了');}}
      else showToast('解析エラー');
    }}
    </script>
    """
    return layout("ワークベンチ", content, active='pipeline', timing=elapsed)


# ── Templates page ──────────────────────────────────────────────────────────

@app.get("/templates", response_class=HTMLResponse)
async def templates_page():
    t0 = time.perf_counter()
    conn = get_db()
    templates = bid_engine.list_templates(conn)

    rows_html = ''.join(
        f'<tr><td>{esc(t["name"])}</td><td>{esc(t["project_type"])}</td>'
        f'<td style="font-size:0.8rem">{esc(t.get("description",""))}</td>'
        f'<td>{"有効" if t.get("active") else "無効"}</td>'
        f'<td style="font-size:0.78rem">{esc((t.get("created_at",""))[:10])}</td>'
        f'<td><button class="btn btn-outline btn-sm" style="font-size:0.7rem" onclick="editTemplate({t["template_id"]})">編集</button></td></tr>'
        for t in templates
    ) if templates else '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:1rem">テンプレートなし</td></tr>'

    elapsed = time.perf_counter() - t0
    content = f"""
    <div class="section">
      <div class="section-header">
        <h2>提案書テンプレート</h2>
        <button class="btn btn-primary btn-sm" onclick="document.getElementById('tmpl-modal').classList.add('show')">+ 新規作成</button>
      </div>
      <div class="tbl-wrap">
        <table style="width:100%">
          <thead><tr><th>名前</th><th>種別</th><th>説明</th><th>状態</th><th>作成日</th><th>操作</th></tr></thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>
    </div>

    <div class="modal-overlay" id="tmpl-modal" onclick="if(event.target===this)this.classList.remove('show')">
      <div class="modal" style="max-width:700px">
        <button class="close-modal" onclick="this.closest('.modal-overlay').classList.remove('show')">&#10005;</button>
        <h3 id="tmpl-modal-title">テンプレート作成</h3>
        <form id="tmpl-form" onsubmit="saveTemplate(event)">
          <input type="hidden" id="tf-id" value="">
          <div style="display:grid;gap:0.6rem">
            <input id="tf-name" placeholder="テンプレート名" style="padding:0.4rem;border:1px solid var(--border);border-radius:6px" required>
            <select id="tf-type" style="padding:0.4rem;border:1px solid var(--border);border-radius:6px">
              <option value="service_general">サービス一般</option>
              <option value="service_plan">企画提案</option>
              <option value="service_research">調査研究</option>
              <option value="goods_standard">物品</option>
              <option value="construction">建設工事</option>
              <option value="it_system">ITシステム</option>
            </select>
            <input id="tf-desc" placeholder="説明" style="padding:0.4rem;border:1px solid var(--border);border-radius:6px">
            <textarea id="tf-markdown" rows="12" placeholder="テンプレートMarkdown ({{{{変数名}}}}で変数指定)" style="padding:0.4rem;border:1px solid var(--border);border-radius:6px;font-family:monospace;font-size:0.85rem" required></textarea>
            <input id="tf-vars" placeholder="変数 (カンマ区切り: title,approach,team)" style="padding:0.4rem;border:1px solid var(--border);border-radius:6px">
            <button type="submit" class="btn btn-primary">保存</button>
          </div>
        </form>
      </div>
    </div>

    <script>
    async function editTemplate(tid){{
      const r=await fetch('/api/v4/templates/'+tid);
      if(!r.ok)return;
      const d=await r.json();
      document.getElementById('tf-id').value=d.template_id;
      document.getElementById('tf-name').value=d.name;
      document.getElementById('tf-type').value=d.project_type;
      document.getElementById('tf-desc').value=d.description||'';
      document.getElementById('tf-markdown').value=d.template_markdown;
      document.getElementById('tf-vars').value=(JSON.parse(d.variables_json||'[]')).join(',');
      document.getElementById('tmpl-modal-title').textContent='テンプレート編集';
      document.getElementById('tmpl-modal').classList.add('show');
    }}
    async function saveTemplate(e){{
      e.preventDefault();
      const id=document.getElementById('tf-id').value;
      const data={{
        name:document.getElementById('tf-name').value,
        project_type:document.getElementById('tf-type').value,
        description:document.getElementById('tf-desc').value,
        template_markdown:document.getElementById('tf-markdown').value,
        variables_json:JSON.stringify(document.getElementById('tf-vars').value.split(',').map(s=>s.trim()).filter(Boolean)),
      }};
      const url=id?'/api/v4/templates/'+id:'/api/v4/templates';
      const method=id?'PUT':'POST';
      const r=await fetch(url,{{method,headers:{{'Content-Type':'application/json'}},body:JSON.stringify(data)}});
      if(r.ok){{showToast('保存しました');location.reload();}}
      else showToast('エラー');
    }}
    </script>
    """
    return layout("テンプレート", content, active='templates', timing=elapsed)


# ── Learning page ───────────────────────────────────────────────────────────

@app.get("/learning", response_class=HTMLResponse)
async def learning_page():
    t0 = time.perf_counter()
    conn = get_db()
    summary = bid_engine.get_learning_summary_with_audit(conn) if hasattr(bid_engine, 'get_learning_summary_with_audit') else bid_engine.get_learning_summary(conn)

    recent_html = ''.join(
        f'<tr><td>{esc((r.get("title") or "")[:40])}</td><td>{esc(r.get("muni_code") or "")}</td>'
        f'<td><span class="badge" style="background:{"var(--success)" if r.get("outcome")=="won" else "var(--danger)" if r.get("outcome")=="lost" else "var(--muted)"};color:white;font-size:0.7rem">{esc(r.get("outcome") or "")}</span></td>'
        f'<td style="font-size:0.8rem">{esc((r.get("feedback_text") or "")[:60])}</td>'
        f'<td style="font-size:0.78rem">{esc((r.get("received_at") or "")[:10])}</td></tr>'
        for r in summary.get("recent", [])
    ) if summary.get("recent") else '<tr><td colspan="5" style="text-align:center;color:var(--muted);padding:1rem">フィードバックデータなし</td></tr>'

    patterns_html = ''.join(
        f'<tr><td>{esc(str(p.get("muni_code","")))}</td><td>{esc(p.get("pattern_type",""))}</td>'
        f'<td>{esc(p.get("pattern_key",""))}</td><td style="font-size:0.8rem">{esc(str(p.get("pattern_value",""))[:50])}</td>'
        f'<td>{p.get("confidence",0):.0%}</td></tr>'
        for p in summary.get("patterns", [])[:20]
    ) if summary.get("patterns") else '<tr><td colspan="5" style="text-align:center;color:var(--muted);padding:1rem">学習パターンなし</td></tr>'

    elapsed = time.perf_counter() - t0
    content = f"""
    <div class="section">
      <h2>学習ダッシュボード</h2>
      <div class="kpi-row" style="margin-bottom:1.5rem">
        <div class="kpi kpi-accent"><div class="kpi-label">総入札数</div><div class="kpi-value">{summary.get('total',0)}</div></div>
        <div class="kpi"><div class="kpi-label">勝率</div><div class="kpi-value" style="color:var(--success)">{summary.get('win_rate',0)}%</div></div>
        <div class="kpi"><div class="kpi-label">落札</div><div class="kpi-value" style="color:var(--success)">{summary.get('wins',0)}</div></div>
        <div class="kpi"><div class="kpi-label">不落札</div><div class="kpi-value" style="color:var(--danger)">{summary.get('losses',0)}</div></div>
        <div class="kpi"><div class="kpi-label">見送り</div><div class="kpi-value">{summary.get('passed',0)}</div></div>
        <div class="kpi"><div class="kpi-label">提出準備率</div><div class="kpi-value" style="color:#7c3aed">{summary.get('submission_ready_rate','0%')}</div></div>
      </div>

      <h3 style="font-size:1rem;margin-bottom:0.8rem">学習済みパターン</h3>
      <div class="tbl-wrap" style="margin-bottom:1.5rem">
        <table style="width:100%">
          <thead><tr><th>自治体</th><th>パターン種別</th><th>キー</th><th>値</th><th>信頼度</th></tr></thead>
          <tbody>{patterns_html}</tbody>
        </table>
      </div>

      <h3 style="font-size:1rem;margin-bottom:0.8rem">直近のフィードバック</h3>
      <div class="tbl-wrap">
        <table style="width:100%">
          <thead><tr><th>案件名</th><th>自治体</th><th>結果</th><th>フィードバック</th><th>日付</th></tr></thead>
          <tbody>{recent_html}</tbody>
        </table>
      </div>

      <div style="margin-top:1.5rem;padding:1rem;background:var(--hover);border-radius:8px">
        <h3 style="font-size:1rem;margin-bottom:0.5rem">フィードバック登録</h3>
        <form onsubmit="submitFeedback(event)" style="display:grid;grid-template-columns:1fr 1fr;gap:0.5rem">
          <input id="fb-pid" type="number" placeholder="プロジェクトID" required style="padding:0.4rem;border:1px solid var(--border);border-radius:6px">
          <select id="fb-outcome" style="padding:0.4rem;border:1px solid var(--border);border-radius:6px">
            <option value="won">落札</option><option value="lost">不落札</option><option value="passed">見送り</option>
          </select>
          <input id="fb-amount" placeholder="落札金額 (任意)" style="padding:0.4rem;border:1px solid var(--border);border-radius:6px">
          <input id="fb-vendor" placeholder="落札者名 (任意)" style="padding:0.4rem;border:1px solid var(--border);border-radius:6px">
          <textarea id="fb-text" placeholder="フィードバック・反省点" rows="2" style="grid-column:1/-1;padding:0.4rem;border:1px solid var(--border);border-radius:6px"></textarea>
          <button type="submit" class="btn btn-primary" style="grid-column:1/-1">登録</button>
        </form>
      </div>
    </div>
    <script>
    async function submitFeedback(e){{
      e.preventDefault();
      const data={{
        project_id:parseInt(document.getElementById('fb-pid').value),
        outcome:document.getElementById('fb-outcome').value,
        actual_amount:document.getElementById('fb-amount').value||null,
        winning_vendor:document.getElementById('fb-vendor').value||null,
        feedback_text:document.getElementById('fb-text').value||null,
      }};
      const r=await fetch('/api/v4/feedback',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(data)}});
      if(r.ok){{showToast('登録しました');location.reload();}}
      else showToast('エラー');
    }}
    </script>
    """
    return layout("学習", content, active='learning', timing=elapsed)


# ══════════════════════════════════════════════════════════════════════════════
# /api/v4/ — Auto Bidding System APIs
# ══════════════════════════════════════════════════════════════════════════════

# ── Projects ────────────────────────────────────────────────────────────────

@app.get("/api/v4/bid-projects")
async def api_list_projects(
    status: str = Query(None), priority: str = Query(None),
    assignee: str = Query(None), limit: int = Query(100), offset: int = Query(0)
):
    conn = get_db()
    return bid_engine.list_projects(conn, status=status, priority=priority,
                                    assignee=assignee, limit=limit, offset=offset)

@app.post("/api/v4/bid-projects")
async def api_create_project(request: Request):
    data = await request.json()
    item_id = data.get("item_id")
    if not item_id:
        return JSONResponse({"detail": "item_id required"}, status_code=400)
    conn = get_db()
    try:
        project = bid_engine.create_project_v3(conn, int(item_id))
        return project
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=404)

@app.get("/api/v4/bid-projects/{project_id}")
async def api_get_project(project_id: int):
    conn = get_db()
    project = bid_engine.get_project(conn, project_id)
    if not project:
        return JSONResponse({"detail": "not found"}, status_code=404)
    return project

@app.put("/api/v4/bid-projects/{project_id}")
async def api_update_project(project_id: int, request: Request):
    data = await request.json()
    conn = get_db()
    return bid_engine.update_project(conn, project_id, **data)

@app.post("/api/v4/bid-projects/{project_id}/advance")
async def api_advance_project(project_id: int, request: Request):
    conn = get_db()
    try:
        body = await request.body()
        data = json.loads(body) if body else {}
    except Exception:
        data = {}
    try:
        return bid_engine.advance_project(conn, project_id, target_status=data.get("target_status"))
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.delete("/api/v4/bid-projects/{project_id}")
async def api_delete_project(project_id: int):
    conn = get_db()
    bid_engine.delete_project(conn, project_id)
    return {"ok": True}


# ── Questions ───────────────────────────────────────────────────────────────

@app.post("/api/v4/questions/generate/{project_id}")
async def api_generate_questions(project_id: int):
    conn = get_db()
    try:
        questions = bid_engine.generate_questions(conn, project_id)
        return {"questions": questions, "count": len(questions)}
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=404)

@app.get("/api/v4/questions/{project_id}")
async def api_list_questions(project_id: int):
    conn = get_db()
    return bid_engine.list_questions(conn, project_id)

@app.post("/api/v4/questions")
async def api_add_question(request: Request):
    data = await request.json()
    conn = get_db()
    return bid_engine.add_question(
        conn, data["project_id"], data["question_text"],
        priority=data.get("priority", "normal"),
        rationale=data.get("rationale")
    )

@app.put("/api/v4/questions/{question_id}")
async def api_update_question(question_id: int, request: Request):
    data = await request.json()
    conn = get_db()
    return bid_engine.update_question(conn, question_id, **data)

@app.delete("/api/v4/questions/{question_id}")
async def api_delete_question(question_id: int):
    conn = get_db()
    bid_engine.delete_question(conn, question_id)
    return {"ok": True}


# ── Proposals ───────────────────────────────────────────────────────────────

@app.post("/api/v4/proposals/generate/{project_id}")
async def api_generate_proposal(project_id: int):
    conn = get_db()
    try:
        draft = bid_engine.generate_optimized_proposal(conn, project_id)
        return draft
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=404)

@app.get("/api/v4/proposals/{project_id}")
async def api_list_proposals(project_id: int):
    conn = get_db()
    return bid_engine.list_drafts(conn, project_id)

@app.put("/api/v4/proposals/{draft_id}")
async def api_update_proposal(draft_id: int, request: Request):
    data = await request.json()
    conn = get_db()
    return bid_engine.update_draft(conn, draft_id, data.get("content_markdown", ""))

@app.post("/api/v4/proposals/{draft_id}/freeze")
async def api_freeze_proposal(draft_id: int):
    conn = get_db()
    return bid_engine.freeze_draft(conn, draft_id)


# ── Templates ───────────────────────────────────────────────────────────────

@app.get("/api/v4/templates")
async def api_list_templates():
    conn = get_db()
    return bid_engine.list_templates(conn)

@app.get("/api/v4/templates/{template_id}")
async def api_get_template(template_id: int):
    conn = get_db()
    t = bid_engine.get_template(conn, template_id)
    if not t:
        return JSONResponse({"detail": "not found"}, status_code=404)
    return t

@app.post("/api/v4/templates")
async def api_create_template(request: Request):
    data = await request.json()
    conn = get_db()
    return bid_engine.create_template(
        conn, data["name"], data["project_type"],
        data["template_markdown"], data.get("variables_json", "[]"),
        description=data.get("description")
    )

@app.put("/api/v4/templates/{template_id}")
async def api_update_template(template_id: int, request: Request):
    data = await request.json()
    conn = get_db()
    return bid_engine.update_template(conn, template_id, **data)

@app.delete("/api/v4/templates/{template_id}")
async def api_delete_template(template_id: int):
    conn = get_db()
    bid_engine.delete_template(conn, template_id)
    return {"ok": True}


# ── Artifacts ───────────────────────────────────────────────────────────────

@app.post("/api/v4/artifacts/generate-checklist/{project_id}")
async def api_gen_checklist(project_id: int):
    conn = get_db()
    try:
        return bid_engine.generate_checklist(conn, project_id)
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=404)

@app.post("/api/v4/artifacts/generate-cover/{project_id}")
async def api_gen_cover(project_id: int):
    conn = get_db()
    try:
        return bid_engine.generate_cover_letter(conn, project_id)
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=404)

@app.post("/api/v4/artifacts/generate-summary/{project_id}")
async def api_gen_summary(project_id: int):
    conn = get_db()
    try:
        return bid_engine.generate_summary(conn, project_id)
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=404)

@app.get("/api/v4/artifacts/{project_id}")
async def api_list_artifacts(project_id: int):
    conn = get_db()
    return bid_engine.list_artifacts(conn, project_id)

@app.get("/api/v4/artifacts/download/{artifact_id}")
async def api_download_artifact(artifact_id: int):
    conn = get_db()
    a = bid_engine.get_artifact(conn, artifact_id)
    if not a:
        return JSONResponse({"detail": "not found"}, status_code=404)
    content = a.get("content") or ""
    filename = a.get("file_name", "artifact.txt")
    return Response(
        content=content.encode("utf-8"),
        media_type=a.get("mime_type", "text/plain"),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# ── Feedback & Learning ────────────────────────────────────────────────────

@app.post("/api/v4/feedback")
async def api_submit_feedback(request: Request):
    data = await request.json()
    conn = get_db()
    return bid_engine.submit_feedback(
        conn, data["project_id"], data["outcome"],
        actual_amount=data.get("actual_amount"),
        winning_vendor=data.get("winning_vendor"),
        feedback_text=data.get("feedback_text")
    )

@app.get("/api/v4/feedback/stats")
async def api_feedback_stats():
    conn = get_db()
    return bid_engine.get_feedback_stats(conn)

@app.get("/api/v4/learning/patterns/{muni_code}")
async def api_muni_patterns(muni_code: str):
    conn = get_db()
    return bid_engine.get_muni_patterns(conn, muni_code)

@app.get("/api/v4/learning/summary")
async def api_learning_summary():
    conn = get_db()
    return bid_engine.get_learning_summary(conn)

# Night 2 additions
@app.post("/api/v4/proposals/{draft_id}/score")
async def api_score_proposal(draft_id: int):
    conn = get_db()
    return bid_engine.score_proposal(conn, draft_id)

@app.post("/api/v4/artifacts/generate-compliance/{project_id}")
async def api_gen_compliance(project_id: int):
    conn = get_db()
    try:
        return bid_engine.generate_compliance_matrix(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.post("/api/v4/artifacts/generate-submission-notes/{project_id}")
async def api_gen_submission_notes(project_id: int):
    conn = get_db()
    try:
        return bid_engine.generate_submission_notes(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


# Night 3 additions — SpecParser, pattern competition, data quality
@app.post("/api/v4/spec-parse/{item_id}")
async def api_parse_spec(item_id: int):
    conn = get_db()
    try:
        return bid_engine.parse_spec(conn, item_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/v4/spec-parse/{item_id}")
async def api_get_spec_parse(item_id: int):
    conn = get_db()
    result = bid_engine.get_spec_parse(conn, item_id)
    if not result:
        return JSONResponse({"detail": "no spec parse found"}, status_code=404)
    return result

@app.post("/api/v4/pattern-competition/{project_id}")
async def api_pattern_competition(project_id: int):
    conn = get_db()
    try:
        return bid_engine.run_pattern_competition(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/v4/pattern-competition/stats")
async def api_pattern_competition_stats():
    conn = get_db()
    return bid_engine.get_pattern_competition_stats(conn)

@app.get("/api/v4/data-quality")
async def api_data_quality():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    with_desc = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE raw_json LIKE '%ProjectDescription%'").fetchone()[0]
    no_deadline = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline IS NULL").fetchone()[0]
    no_amount = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE amount IS NULL OR amount = ''").fetchone()[0]
    distinct_munis = conn.execute("SELECT COUNT(DISTINCT muni_code) FROM procurement_items WHERE muni_code IS NOT NULL AND muni_code != ''").fetchone()[0]
    dup_groups = conn.execute("SELECT COUNT(*) FROM (SELECT title, muni_code, COUNT(*) as c FROM procurement_items GROUP BY title, muni_code HAVING c > 1)").fetchone()[0]
    return {
        "total_items": total,
        "with_description": with_desc,
        "with_description_pct": round(with_desc / total * 100, 1) if total else 0,
        "missing_deadline": no_deadline,
        "missing_deadline_pct": round(no_deadline / total * 100, 1) if total else 0,
        "missing_amount": no_amount,
        "missing_amount_pct": round(no_amount / total * 100, 1) if total else 0,
        "distinct_municipalities": distinct_munis,
        "duplicate_title_muni_groups": dup_groups,
    }


# ══════════════════════════════════════════════════════════════════════════════
# NIGHT 4 APIs
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/v4/scoring-model/{project_id}")
async def api_scoring_model(project_id: int):
    conn = get_db()
    try:
        return bid_engine.estimate_scoring_model(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v4/scoring-model/{project_id}")
async def api_get_scoring_model(project_id: int):
    conn = get_db()
    row = conn.execute("SELECT scoring_model_json FROM bid_projects WHERE project_id = ?", (project_id,)).fetchone()
    if not row or not row[0]:
        return JSONResponse({"detail": "No scoring model. POST first."}, status_code=404)
    return json.loads(row[0])


@app.post("/api/v4/competition/{project_id}")
async def api_competition(project_id: int):
    conn = get_db()
    try:
        return bid_engine.estimate_competition(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v4/competition/{project_id}")
async def api_get_competition(project_id: int):
    conn = get_db()
    row = conn.execute("SELECT competition_json FROM bid_projects WHERE project_id = ?", (project_id,)).fetchone()
    if not row or not row[0]:
        return JSONResponse({"detail": "No competition data. POST first."}, status_code=404)
    return json.loads(row[0])


@app.post("/api/v4/price-strategy/{project_id}")
async def api_price_strategy(project_id: int):
    conn = get_db()
    try:
        return bid_engine.estimate_price_strategy(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v4/price-strategy/{project_id}")
async def api_get_price_strategy(project_id: int):
    conn = get_db()
    row = conn.execute("SELECT price_strategy_json FROM bid_projects WHERE project_id = ?", (project_id,)).fetchone()
    if not row or not row[0]:
        return JSONResponse({"detail": "No price strategy. POST first."}, status_code=404)
    return json.loads(row[0])


@app.post("/api/v4/win-probability/{project_id}")
async def api_win_probability(project_id: int):
    conn = get_db()
    try:
        return bid_engine.compute_win_probability(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v4/proposals/generate-optimized/{project_id}")
async def api_generate_optimized_proposal(project_id: int):
    conn = get_db()
    try:
        result = bid_engine.generate_optimized_proposal(conn, project_id)
        return {"draft_id": result["draft_id"], "version": result["version"],
                "practicality_score": result.get("practicality_score"),
                "scoring_alignment_score": result.get("scoring_alignment_score")}
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v4/night4-pipeline/{project_id}")
async def api_night4_pipeline(project_id: int):
    conn = get_db()
    try:
        return bid_engine.run_night4_pipeline(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


# ══════════════════════════════════════════════════════════════════════════════
# NIGHT 5 APIs
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/v5/strategy/{project_id}")
async def api_assign_strategy(project_id: int):
    conn = get_db()
    try:
        return bid_engine.assign_strategy(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v5/strategy/{project_id}")
async def api_get_strategy(project_id: int):
    conn = get_db()
    row = conn.execute(
        "SELECT strategy_type, strategy_reason, effort_budget_hours, expected_value, expected_information_value FROM bid_projects WHERE project_id = ?",
        (project_id,)
    ).fetchone()
    if not row:
        return JSONResponse({"detail": "Project not found"}, status_code=404)
    d = dict(row)
    if not d.get("strategy_type"):
        return JSONResponse({"detail": "No strategy assigned. POST first."}, status_code=404)
    return d


@app.post("/api/v5/ev/{project_id}")
async def api_calculate_ev(project_id: int):
    conn = get_db()
    try:
        return bid_engine.calculate_ev(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v5/portfolio")
async def api_allocate_portfolio(total_hours: float = 80.0):
    conn = get_db()
    try:
        return bid_engine.allocate_portfolio(conn, total_hours)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v5/portfolio")
async def api_get_portfolio(total_hours: float = 80.0):
    conn = get_db()
    try:
        return bid_engine.allocate_portfolio(conn, total_hours)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v5/night5-pipeline/{project_id}")
async def api_night5_pipeline(project_id: int):
    conn = get_db()
    try:
        return bid_engine.run_night5_pipeline(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v5/night5-pipeline-all")
async def api_night5_pipeline_all():
    """Run Night 5 pipeline on all active projects."""
    conn = get_db()
    projects = conn.execute(
        "SELECT project_id FROM bid_projects WHERE status NOT IN ('archived', 'abandoned') ORDER BY project_id"
    ).fetchall()
    results = {}
    for row in projects:
        pid = row[0]
        try:
            results[str(pid)] = bid_engine.run_night5_pipeline(conn, pid)
        except Exception as e:
            results[str(pid)] = {"error": str(e)[:200]}
    return {"project_count": len(results), "results": results}


@app.get("/api/v5/subsidies")
async def api_list_subsidies(keyword: str = None, area: str = None, limit: int = 20, offset: int = 0):
    conn = get_db()
    sql = "SELECT * FROM subsidy_items WHERE 1=1"
    params = []
    if keyword:
        sql += " AND title LIKE ?"
        params.append(f"%{keyword}%")
    if area:
        sql += " AND target_area LIKE ?"
        params.append(f"%{area}%")
    sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    rows = conn.execute(sql, params).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM subsidy_items").fetchone()[0]
    return {"total": total, "items": [dict(r) for r in rows]}


@app.get("/api/v5/subsidies/{subsidy_id}")
async def api_get_subsidy(subsidy_id: str):
    conn = get_db()
    row = conn.execute("SELECT * FROM subsidy_items WHERE subsidy_id = ?", (subsidy_id,)).fetchone()
    if not row:
        return JSONResponse({"detail": "Subsidy not found"}, status_code=404)
    return dict(row)


# ══════════════════════════════════════════════════════════════════════════════
# NIGHT 6 APIs
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/v6/explore-classify/{project_id}")
async def api_classify_explore(project_id: int):
    conn = get_db()
    try:
        return bid_engine.classify_explore_subtype(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v6/snapshot")
async def api_snapshot(period_type: str = "weekly", total_hours: float = 80.0, notes: str = None):
    conn = get_db()
    try:
        return bid_engine.snapshot_allocation(conn, period_type=period_type, total_hours=total_hours, notes=notes)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v6/snapshots")
async def api_list_snapshots(limit: int = 20):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM allocation_snapshots ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    return {"count": len(rows), "snapshots": [dict(r) for r in rows]}


@app.post("/api/v6/learn")
async def api_learn_weights():
    conn = get_db()
    try:
        return bid_engine.learn_allocation_weights(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v6/weights")
async def api_get_weights():
    conn = get_db()
    rows = conn.execute("SELECT * FROM allocation_weights ORDER BY weight_value DESC").fetchall()
    return {"weights": [dict(r) for r in rows]}


@app.post("/api/v6/adaptive-portfolio")
async def api_adaptive_portfolio(total_hours: float = 80.0):
    conn = get_db()
    try:
        return bid_engine.adaptive_allocate(conn, total_hours)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v6/subsidy-score/{subsidy_id}")
async def api_score_subsidy(subsidy_id: str):
    conn = get_db()
    try:
        return bid_engine.score_subsidy(conn, subsidy_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v6/cross-market")
async def api_cross_market(total_hours: float = 80.0):
    conn = get_db()
    try:
        return bid_engine.cross_market_portfolio(conn, total_hours)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v6/time-series")
async def api_time_series(limit: int = 20):
    conn = get_db()
    try:
        return bid_engine.get_time_series(conn, limit)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v6/night6-pipeline")
async def api_night6_pipeline(total_hours: float = 80.0):
    conn = get_db()
    try:
        return bid_engine.run_night6_pipeline(conn, total_hours)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


# ══════════════════════════════════════════════════════════════════════════════
# NIGHT 6+ APIs — Competitive Dominance
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/v6/question-strategy/{project_id}")
async def api_question_strategy(project_id: int):
    conn = get_db()
    try:
        return bid_engine.strategize_questions(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v6/reviewer-score/{project_id}")
async def api_reviewer_score(project_id: int):
    conn = get_db()
    try:
        return bid_engine.score_reviewer_perspective(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v6/differentiate/{project_id}")
async def api_differentiate(project_id: int):
    conn = get_db()
    try:
        return bid_engine.differentiate_proposal(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v6/competitive/{project_id}")
async def api_competitive(project_id: int):
    conn = get_db()
    try:
        return bid_engine.optimize_competitive(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v6/grant-project/{subsidy_id}")
async def api_create_grant(subsidy_id: str):
    conn = get_db()
    try:
        return bid_engine.create_grant_project(conn, subsidy_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v6/grant-outline/{grant_id}")
async def api_grant_outline(grant_id: int):
    conn = get_db()
    try:
        return bid_engine.generate_grant_outline(conn, grant_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v6/grant-projects")
async def api_list_grants():
    conn = get_db()
    rows = conn.execute("SELECT * FROM grant_projects ORDER BY grant_id DESC").fetchall()
    return {"count": len(rows), "grants": [dict(r) for r in rows]}


@app.post("/api/v6/vendor-profile/{vendor_name}")
async def api_vendor_profile(vendor_name: str):
    conn = get_db()
    try:
        return bid_engine.learn_vendor_profile(conn, vendor_name)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v6/muni-behavior/{muni_code}")
async def api_muni_behavior(muni_code: str):
    conn = get_db()
    try:
        return bid_engine.learn_muni_behavior(conn, muni_code)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v6/strategy-evolution")
async def api_strategy_evolution():
    conn = get_db()
    try:
        return bid_engine.evolve_strategy(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v6/night6plus-pipeline/{project_id}")
async def api_night6plus_pipeline(project_id: int):
    conn = get_db()
    try:
        return bid_engine.run_night6plus_pipeline(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)




# ══════════════════════════════════════════════════════════════════════════════
# NIGHT 7 APIs — Revenue Closure
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/v7/submission-package/{project_id}")
async def api_build_submission(project_id: int):
    conn = get_db()
    try:
        return bid_engine.build_submission_package(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v7/submission-package/{project_id}")
async def api_get_submission(project_id: int):
    conn = get_db()
    return bid_engine.get_submission_package(conn, project_id)


@app.get("/api/v7/download/{project_id}/{doc_type}")
async def api_download_artifact(project_id: int, doc_type: str):
    conn = get_db()
    return bid_engine.download_artifact_content(conn, project_id, doc_type)


@app.post("/api/v7/compliance-check/{project_id}")
async def api_compliance_check(project_id: int):
    conn = get_db()
    try:
        return bid_engine.run_compliance_check(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v7/compliance-fix/{project_id}")
async def api_compliance_fix(project_id: int):
    conn = get_db()
    try:
        return bid_engine.auto_fix_compliance(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v7/human-optimization/{project_id}")
async def api_human_optimization(project_id: int):
    conn = get_db()
    try:
        return bid_engine.optimize_human_intervention(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v7/human-summary")
async def api_human_summary():
    conn = get_db()
    try:
        return bid_engine.portfolio_human_summary(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v7/grant-execute/{grant_id}")
async def api_grant_execute(grant_id: int):
    conn = get_db()
    try:
        return bid_engine.prepare_grant_execution(conn, grant_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v7/grants-executable")
async def api_grants_executable():
    conn = get_db()
    return bid_engine.list_executable_grants(conn)


@app.post("/api/v7/cost-minimize")
async def api_cost_minimize():
    conn = get_db()
    try:
        return bid_engine.minimize_explore_cost(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v7/revenue-metrics")
async def api_revenue_metrics():
    conn = get_db()
    try:
        return bid_engine.calculate_revenue_metrics(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v7/night7-pipeline/{project_id}")
async def api_night7_pipeline(project_id: int):
    conn = get_db()
    try:
        return bid_engine.run_night7_pipeline(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v7/night7-portfolio")
async def api_night7_portfolio():
    conn = get_db()
    try:
        return bid_engine.run_night7_portfolio(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)



# ══════════════════════════════════════════════════════════════════════════════
# AUDIT NIGHT APIs — Reality Checks
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/audit/capacity/{project_id}")
async def api_audit_capacity(project_id: int):
    conn = get_db()
    try:
        return bid_engine.assess_delivery_capacity(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/audit/hard-fail/{project_id}")
async def api_audit_hard_fail(project_id: int):
    conn = get_db()
    try:
        return bid_engine.submission_hard_fail_check(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/audit/dry-run/{project_id}")
async def api_audit_dry_run(project_id: int, action: str = "submit"):
    conn = get_db()
    try:
        return bid_engine.dry_run_guard(conn, project_id, action)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/audit/summary")
async def api_audit_summary():
    conn = get_db()
    try:
        return bid_engine.get_audit_summary(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/audit/decisions/{project_id}")
async def api_audit_decisions(project_id: int):
    conn = get_db()
    rows = conn.execute("SELECT * FROM decision_audit_log WHERE bid_project_id = ? ORDER BY id DESC", (project_id,)).fetchall()
    return {"project_id": project_id, "decisions": [dict(r) for r in rows]}


@app.get("/api/audit/decisions")
async def api_audit_all_decisions():
    conn = get_db()
    rows = conn.execute("SELECT * FROM decision_audit_log ORDER BY id DESC LIMIT 100").fetchall()
    return {"count": len(rows), "decisions": [dict(r) for r in rows]}


# ── Night 8 API Routes (Steps 3-4) ──────────────────────────────────────────

@app.post("/api/v8/readiness/{project_id}")
async def api_readiness_eval(project_id: int):
    conn = get_db()
    result = bid_engine.evaluate_readiness(conn, project_id)
    if not result:
        return JSONResponse({"error": "Project not found"}, status_code=404)
    return JSONResponse(result)

@app.get("/api/v8/readiness-all")
async def api_readiness_all():
    conn = get_db()
    return JSONResponse(bid_engine.batch_evaluate_readiness(conn))

@app.post("/api/v8/calibrate/{project_id}")
async def api_calibrate_ev(project_id: int):
    conn = get_db()
    result = bid_engine.calibrate_ev(conn, project_id)
    if not result:
        return JSONResponse({"error": "Project not found"}, status_code=404)
    return JSONResponse(result)

@app.get("/api/v8/calibrate-all")
async def api_calibrate_all():
    conn = get_db()
    return JSONResponse(bid_engine.batch_calibrate_ev(conn))

@app.post("/api/v8/enforce-freeze")
async def api_enforce_freeze():
    conn = get_db()
    return JSONResponse(bid_engine.enforce_goods_freeze(conn))

@app.get("/api/v8/freeze-status")
async def api_freeze_status():
    return JSONResponse(bid_engine.FROZEN_TYPES)

@app.post("/api/v8/human-review-assign")
async def api_human_review_assign():
    conn = get_db()
    return JSONResponse(bid_engine.assign_human_review_priority(conn))

@app.post("/api/v8/subsidy-stopgap")
async def api_subsidy_stopgap():
    conn = get_db()
    return JSONResponse(bid_engine.subsidy_stopgap_audit(conn))

@app.post("/api/v8/night8-pipeline")
async def api_night8_pipeline():
    conn = get_db()
    return JSONResponse(bid_engine.run_night8_pipeline(conn))



# ══════════════════════════════════════════════════════════════════════════════
# NIGHT 8 — SECURITY ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/security/status")
async def api_security_status():
    """Security status overview (public - no secrets revealed)."""
    conn = get_db()
    bid_engine._init_security_tables(conn)
    total_blocked = conn.execute(
        "SELECT COUNT(*) FROM write_audit_log WHERE blocked = 1"
    ).fetchone()[0]
    total_writes = conn.execute(
        "SELECT COUNT(*) FROM write_audit_log WHERE action_type != 'read'"
    ).fetchone()[0]
    recent = conn.execute(
        "SELECT endpoint, method, action_type, blocked, block_reason, created_at "
        "FROM write_audit_log ORDER BY id DESC LIMIT 20"
    ).fetchall()
    return {
        "auth_enabled": True,
        "total_write_attempts": total_writes,
        "total_blocked": total_blocked,
        "freeze_policies": bid_engine.get_freeze_status(),
        "recent_audit": [dict(r) for r in recent],
    }


@app.get("/api/security/audit-log")
async def api_audit_log(limit: int = 100, blocked_only: bool = False):
    """View write audit log."""
    conn = get_db()
    bid_engine._init_security_tables(conn)
    if blocked_only:
        rows = conn.execute(
            "SELECT * FROM write_audit_log WHERE blocked = 1 ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM write_audit_log ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
    return {"count": len(rows), "log": [dict(r) for r in rows]}


@app.get("/api/security/freeze-status")
async def api_freeze_status():
    """View freeze policy status."""
    return bid_engine.get_freeze_status()




# ══════════════════════════════════════════════════════════════════════════════
# NIGHT 8 Step 2 APIs — Attachment Pipeline + Important Items V2
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/v8/attachment-stats")
async def api_attachment_stats():
    conn = get_db()
    try:
        return bid_engine.get_attachment_stats(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v8/attachment-queue")
async def api_attachment_queue(limit: int = 50):
    conn = get_db()
    try:
        return {"queue": bid_engine.get_attachment_queue(conn, limit)}
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v8/attachment-inventory")
async def api_attachment_inventory():
    conn = get_db()
    try:
        return bid_engine.populate_attachment_inventory(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.post("/api/v8/important-rescore")
async def api_important_rescore(limit: int = 1000):
    conn = get_db()
    try:
        return bid_engine.batch_score_important_v2(conn, limit)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)


@app.get("/api/v8/important-items")
async def api_important_items(limit: int = 50):
    conn = get_db()
    try:
        return {"items": bid_engine.get_important_items_v2(conn, limit)}
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)




# ── Night 9 API Routes ──────────────────────────────────────────────────────

@app.post('/api/v9/parse-attachment/{asset_id}')
async def api_parse_attachment(asset_id: int):
    conn = get_db()
    try:
        bid_engine.init_night9_tables(conn)
        return bid_engine.parse_attachment(conn, asset_id)
    except Exception as e:
        return JSONResponse({'detail': str(e)}, status_code=400)

@app.post('/api/v9/batch-parse')
async def api_batch_parse(limit: int = 25):
    conn = get_db()
    try:
        bid_engine.init_night9_tables(conn)
        return bid_engine.batch_parse_attachments(conn, limit=limit)
    except Exception as e:
        return JSONResponse({'detail': str(e)}, status_code=400)

@app.post('/api/v9/deep-parse-benchmark')
async def api_deep_parse_benchmark(limit: int = 25):
    conn = get_db()
    try:
        return bid_engine.run_deep_parse_benchmark(conn, limit=limit)
    except Exception as e:
        return JSONResponse({'detail': str(e)}, status_code=400)

@app.get('/api/v9/parse-stats')
async def api_parse_stats():
    conn = get_db()
    try:
        bid_engine.init_night9_tables(conn)
        return bid_engine.get_parse_stats(conn)
    except Exception as e:
        return JSONResponse({'detail': str(e)}, status_code=400)

@app.get('/api/v9/parsed-attachment/{asset_id}')
async def api_parsed_attachment(asset_id: int):
    conn = get_db()
    try:
        return bid_engine.get_parsed_attachment(conn, asset_id)
    except Exception as e:
        return JSONResponse({'detail': str(e)}, status_code=400)

@app.post('/api/v9/parse-spec-enhanced/{item_id}')
async def api_parse_spec_enhanced(item_id: int):
    conn = get_db()
    try:
        bid_engine.init_night9_tables(conn)
        return bid_engine.parse_spec_with_attachments(conn, item_id)
    except Exception as e:
        return JSONResponse({'detail': str(e)}, status_code=400)

@app.post('/api/v9/quality-depth/{project_id}')
async def api_quality_depth(project_id: int):
    conn = get_db()
    try:
        bid_engine.init_night9_tables(conn)
        return bid_engine.compute_quality_depth(conn, project_id)
    except Exception as e:
        return JSONResponse({'detail': str(e)}, status_code=400)

@app.get('/api/v9/quality-depth-all')
async def api_quality_depth_all():
    conn = get_db()
    try:
        bid_engine.init_night9_tables(conn)
        return bid_engine.batch_compute_quality_depth(conn)
    except Exception as e:
        return JSONResponse({'detail': str(e)}, status_code=400)

@app.post('/api/v9/night9-pipeline')
async def api_night9_pipeline():
    conn = get_db()
    try:
        return bid_engine.run_night9_pipeline(conn)
    except Exception as e:
        return JSONResponse({'detail': str(e)}, status_code=400)



# -- Night 10 API Routes --

@app.post("/api/v10/build-bundle/{project_id}")
async def api_build_bundle(project_id: int):
    conn = get_db()
    try:
        return bid_engine.build_submission_bundle(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/v10/bundle-readiness/{project_id}")
async def api_bundle_readiness(project_id: int):
    conn = get_db()
    try:
        return bid_engine.evaluate_bundle_readiness(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/v10/bundle-readiness-all")
async def api_bundle_readiness_all():
    conn = get_db()
    try:
        bid_engine.init_night10_tables(conn)
        projects = conn.execute(
            "SELECT project_id FROM bid_projects WHERE status NOT IN (archived,abandoned)"
        ).fetchall()
        return {"results": [bid_engine.evaluate_bundle_readiness(conn, p[0]) for p in projects]}
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/v10/package-integrity/{project_id}")
async def api_package_integrity(project_id: int):
    conn = get_db()
    try:
        return bid_engine.check_package_integrity(conn, project_id)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/v10/required-docs/{project_id}")
async def api_required_docs(project_id: int):
    conn = get_db()
    try:
        return {"matrix": bid_engine.build_required_doc_matrix(conn, project_id)}
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.post("/api/v10/placeholder-check/{project_id}")
async def api_placeholder_check(project_id: int):
    conn = get_db()
    try:
        proposal = conn.execute(
            "SELECT content_markdown FROM proposal_drafts WHERE project_id = ? ORDER BY version DESC LIMIT 1",
            (project_id,)
        ).fetchone()
        text = proposal[0] if proposal else ""
        arts = conn.execute("SELECT content FROM submission_artifacts WHERE project_id = ?", (project_id,)).fetchall()
        for a in arts:
            text += chr(10) + (a[0] or "")
        return bid_engine.detect_placeholder_leaks(text, "bundle")
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.post("/api/v10/exploit-benchmark")
async def api_exploit_benchmark():
    conn = get_db()
    try:
        return bid_engine.run_exploit_dryrun_benchmark(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/v10/exploit-review-queue")
async def api_exploit_review_queue():
    conn = get_db()
    try:
        return bid_engine.assign_exploit_review_queue(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.post("/api/v10/subsidy-bundle-trial")
async def api_subsidy_bundle_trial(limit: int = 5):
    conn = get_db()
    try:
        return bid_engine.build_subsidy_bundle_trial(conn, limit=limit)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.post("/api/v10/night10-pipeline")
async def api_night10_pipeline():
    conn = get_db()
    try:
        return bid_engine.run_night10_pipeline(conn)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/v10/bundle/{project_id}")
async def api_get_bundle(project_id: int):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM submission_bundles WHERE bid_project_id = ? ORDER BY package_version DESC LIMIT 1",
            (project_id,)
        ).fetchone()
        if not row:
            return JSONResponse({"detail": "No bundle found"}, status_code=404)
        return dict(row)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)




# ============================================================
# PRE-NIGHT11 API Routes
# ============================================================

@app.post("/api/v11/placeholder-taxonomy")
async def api_placeholder_taxonomy():
    conn = get_db()
    try:
        result = bid_engine.placeholder_taxonomy(conn)
        total = len(result)
        auto = len([r for r in result if r["classification"] == "auto_fixable"])
        inf = len([r for r in result if r["classification"] == "inferable_but_risky"])
        hr = len([r for r in result if r["classification"] == "human_required"])
        return {"total": total, "auto_fixable": auto, "inferable_but_risky": inf, "human_required": hr, "items": result}
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.post("/api/v11/placeholder-autofix/{project_id}")
async def api_placeholder_autofix(project_id: int):
    conn = get_db()
    try:
        result = bid_engine.placeholder_autofix(conn, project_id)
        return result
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/v11/company-doc-skeleton/{project_id}/{doc_type}")
async def api_company_doc_skeleton(project_id: int, doc_type: str):
    conn = get_db()
    try:
        result = bid_engine.generate_company_doc_skeleton(conn, project_id, doc_type)
        return result
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/v11/company-doc-skeletons/{project_id}")
async def api_company_doc_skeletons(project_id: int):
    conn = get_db()
    try:
        result = bid_engine.generate_all_company_doc_skeletons(conn, project_id)
        return result
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.get("/api/v11/dryrun-readiness/{project_id}")
async def api_dryrun_readiness(project_id: int):
    conn = get_db()
    try:
        result = bid_engine.evaluate_dryrun_readiness(conn, project_id)
        return result
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=400)

@app.post("/api/v11/pre-night11-pipeline")
async def api_pre_night11_pipeline():
    conn = get_db()
    try:
        result = bid_engine.run_pre_night11_pipeline(conn)
        return result
    except Exception as e:
        import traceback
        return JSONResponse({"detail": str(e), "traceback": traceback.format_exc()}, status_code=400)


# Night 12 API routes
@app.post("/api/v12/deadline-extract/{project_id}")
async def api_deadline_extract(project_id: int, request: Request):
    """Extract deadlines for a project."""
    from n12_stabilization import extract_deadlines
    conn = get_db()
    result = extract_deadlines(conn, project_id)
    conn.close()
    return result

@app.post("/api/v12/deadline-extract-all")
async def api_deadline_extract_all(request: Request):
    """Extract deadlines for all active projects."""
    from n12_stabilization import run_deadline_extraction
    conn = get_db()
    results = run_deadline_extraction(conn)
    conn.close()
    return {"results": results, "count": len(results), "with_deadline": sum(1 for r in results if r['deadlines'].get('deadline_main'))}

@app.post("/api/v12/method-classify/{project_id}")
async def api_method_classify(project_id: int, request: Request):
    """Classify procurement method for a project."""
    from n12_stabilization import classify_procurement_method
    conn = get_db()
    result = classify_procurement_method(conn, project_id)
    conn.close()
    return result

@app.post("/api/v12/method-classify-all")
async def api_method_classify_all(request: Request):
    """Classify method for all active projects."""
    from n12_stabilization import run_method_classification
    conn = get_db()
    results = run_method_classification(conn)
    conn.close()
    return {"results": results, "known": sum(1 for r in results if r['method'] != 'unknown')}

@app.post("/api/v12/wp-calibrate-all")
async def api_wp_calibrate_all(request: Request):
    """Run full WP calibration pipeline."""
    from n12_stabilization import run_deadline_extraction, run_method_classification, run_wp_calibration
    conn = get_db()
    dl = run_deadline_extraction(conn)
    mt = run_method_classification(conn)
    wp = run_wp_calibration(conn, dl, mt)
    conn.close()
    return {"results": wp, "mean_wp": sum(r['calibrated_wp'] for r in wp)/len(wp) if wp else 0}

@app.post("/api/v12/strategy-rebalance-all")
async def api_strategy_rebalance_all(request: Request):
    """Run full stabilization pipeline."""
    from n12_stabilization import run_night12_pipeline
    result = run_night12_pipeline()
    return {
        "deadline_coverage": sum(1 for r in result['deadline_results'] if r['deadlines'].get('deadline_main')),
        "method_known": sum(1 for r in result['method_results'] if r['method'] != 'unknown'),
        "mean_wp": sum(r['calibrated_wp'] for r in result['wp_results'])/len(result['wp_results']) if result['wp_results'] else 0,
        "strategy_changes": sum(1 for r in result['strategy_results'] if r['old_strategy'] != r['new_strategy']),
        "tests_passed": sum(1 for r in result['test_results'] if r['passed']),
    }

@app.get("/api/v12/stabilization-status")
async def api_stabilization_status(request: Request):
    """Get current stabilization metrics."""
    conn = get_db()
    projects = conn.execute("""
        SELECT bp.project_id, bp.strategy_type, bp.win_probability, bp.wp_confidence,
               pi.deadline, pi.method
        FROM bid_projects bp
        JOIN procurement_items pi ON bp.item_id = pi.item_id
        WHERE bp.status NOT IN ('archived','abandoned')
    """).fetchall()
    conn.close()
    total = len(projects)
    with_dl = sum(1 for p in projects if p[4] and str(p[4]) not in ('None',''))
    with_mt = sum(1 for p in projects if p[5] and str(p[5]) not in ('None','','unknown'))
    wps = [p[2] for p in projects if p[2] is not None]
    from collections import Counter
    strats = dict(Counter(p[1] for p in projects))
    return {
        "total_projects": total,
        "deadline_coverage": f"{with_dl}/{total} ({with_dl/total*100:.1f}%)",
        "method_coverage": f"{with_mt}/{total} ({with_mt/total*100:.1f}%)",
        "mean_wp": round(sum(wps)/len(wps), 3) if wps else 0,
        "strategy_distribution": strats,
    }


# === Night 14: Zero-Miss Pipeline API Routes ===

@app.get("/api/v14/coverage-audit")
async def api_coverage_audit():
    """Coverage audit v3 results."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        # Score distribution
        c.execute("""SELECT
            SUM(CASE WHEN important_v3_score >= 50 THEN 1 ELSE 0 END) as critical,
            SUM(CASE WHEN important_v3_score >= 30 AND important_v3_score < 50 THEN 1 ELSE 0 END) as high,
            SUM(CASE WHEN important_v3_score >= 15 AND important_v3_score < 30 THEN 1 ELSE 0 END) as medium,
            SUM(CASE WHEN important_v3_score < 15 THEN 1 ELSE 0 END) as low,
            COUNT(*) as total
            FROM procurement_items""")
        row = c.fetchone()
        dist = {"critical": row[0], "high": row[1], "medium": row[2], "low": row[3], "total": row[4]}

        # Untracked critical
        c.execute("""SELECT COUNT(*) FROM procurement_items
                     WHERE important_v3_score >= 30
                     AND item_id NOT IN (SELECT item_id FROM bid_projects)""")
        untracked = c.fetchone()[0]

        # Tracked
        c.execute("SELECT COUNT(*) FROM bid_projects")
        tracked = c.fetchone()[0]

        # Top untracked
        c.execute("""SELECT item_id, title, important_v3_score, item_type, muni_code
                     FROM procurement_items
                     WHERE important_v3_score >= 30
                     AND item_id NOT IN (SELECT item_id FROM bid_projects)
                     ORDER BY important_v3_score DESC LIMIT 20""")
        top_untracked = [{"item_id": r[0], "title": r[1], "score": r[2], "type": r[3], "muni": r[4]}
                         for r in c.fetchall()]

        return {"score_distribution": dist, "untracked_critical": untracked,
                "tracked": tracked, "top_untracked": top_untracked}
    finally:
        conn.close()


@app.get("/api/v14/attachment-status")
async def api_attachment_status():
    """Attachment parse status overview."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("SELECT parse_status, COUNT(*) FROM attachment_assets GROUP BY parse_status")
        status_dist = {r[0]: r[1] for r in c.fetchall()}

        c.execute("SELECT asset_type, COUNT(*) FROM attachment_assets GROUP BY asset_type ORDER BY COUNT(*) DESC")
        type_dist = {r[0]: r[1] for r in c.fetchall()}

        # Per-project
        c.execute("""SELECT bp.project_id, bp.strategy_type,
            (SELECT COUNT(*) FROM attachment_assets aa WHERE aa.procurement_item_id = bp.item_id) as total,
            (SELECT COUNT(*) FROM attachment_assets aa WHERE aa.procurement_item_id = bp.item_id AND aa.parse_status = 'parsed') as parsed
            FROM bid_projects bp ORDER BY bp.project_id""")
        projects = [{"project_id": r[0], "strategy": r[1], "total": r[2], "parsed": r[3]} for r in c.fetchall()]

        return {"status_distribution": status_dist, "type_distribution": type_dist, "projects": projects}
    finally:
        conn.close()


@app.post("/api/v14/resolve-attachments")
async def api_resolve_attachments():
    """Resolve pending attachments for priority items (max 20 per call)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("resolver", "/app/n14_attachment_resolver.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        results = mod.resolve_priority_attachments(conn, max_items=20)
        return {"processed": results["processed"], "parsed": results["parsed"],
                "errors": results["errors"], "skipped": results["skipped"]}
    finally:
        conn.close()


@app.get("/api/v14/pipeline-v2")
async def api_pipeline_v2():
    """Pipeline V2 evaluation for all projects."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("n14", "/app/n14_zero_miss.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    conn = sqlite3.connect(DB_PATH)
    try:
        results = mod.run_pipeline_evaluation(conn)
        return results
    finally:
        conn.close()


@app.get("/api/v14/pipeline-v2/{project_id}")
async def api_pipeline_v2_project(project_id: int):
    """Pipeline V2 evaluation for a single project."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("n14", "/app/n14_zero_miss.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    conn = sqlite3.connect(DB_PATH)
    try:
        result = mod.evaluate_pipeline_stage(conn, project_id)
        return result
    finally:
        conn.close()


@app.get("/api/v14/data-quality-v2")
async def api_data_quality_v2():
    """Data quality V2 KPIs (depth-focused)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("n14", "/app/n14_zero_miss.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    conn = sqlite3.connect(DB_PATH)
    try:
        results = mod.compute_data_quality_v2(conn)
        return results
    finally:
        conn.close()


@app.get("/api/v14/context-audit")
async def api_context_audit():
    """Context integration audit for proposals."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("n14", "/app/n14_zero_miss.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    conn = sqlite3.connect(DB_PATH)
    try:
        results = mod.audit_context_integration(conn)
        return results
    finally:
        conn.close()


@app.get("/api/v14/important-items")
async def api_important_items(min_score: int = 30, limit: int = 50):
    """List important items by v3 score."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("""SELECT item_id, title, important_v3_score, item_type, category,
                     muni_code, deadline, method
                     FROM procurement_items
                     WHERE important_v3_score >= ?
                     ORDER BY important_v3_score DESC
                     LIMIT ?""", (min_score, limit))
        items = [{"item_id": r[0], "title": r[1], "score": r[2], "type": r[3],
                  "category": r[4], "muni": r[5], "deadline": r[6], "method": r[7]}
                 for r in c.fetchall()]
        return {"items": items, "count": len(items)}
    finally:
        conn.close()


# === End Night 14 Routes ===


# === Day 1 Hardening Sprint: Workbench API Routes ===

@app.get("/api/day1/backlog-stats")
async def api_backlog_stats():
    """Backlog queue statistics."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) FROM backlog_queue")
        total = c.fetchone()[0]
        c.execute("SELECT backlog_type, COUNT(*) FROM backlog_queue GROUP BY backlog_type ORDER BY COUNT(*) DESC")
        by_type = {r[0]: r[1] for r in c.fetchall()}
        c.execute("SELECT auto_action_status, COUNT(*) FROM backlog_queue GROUP BY auto_action_status")
        by_status = {r[0]: r[1] for r in c.fetchall()}
        return {"total": total, "by_type": by_type, "by_status": by_status}
    finally:
        conn.close()


@app.get("/api/day1/backlog-items")
async def api_backlog_items(backlog_type: str = None, limit: int = 50, offset: int = 0):
    """Get backlog items with details."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        query = """SELECT bq.id, bq.procurement_item_id, bq.important_v3_score,
                          bq.backlog_type, bq.priority_rank, bq.auto_action_status,
                          pi.title, pi.category, pi.muni_code, pi.deadline
                   FROM backlog_queue bq
                   JOIN procurement_items pi ON pi.item_id = bq.procurement_item_id"""
        params = []
        if backlog_type:
            query += " WHERE bq.backlog_type = ?"
            params.append(backlog_type)
        query += " ORDER BY bq.priority_rank ASC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        c.execute(query, params)
        items = [{"id": r[0], "item_id": r[1], "score": r[2], "backlog_type": r[3],
                  "priority_rank": r[4], "status": r[5], "title": r[6],
                  "category": r[7], "muni_code": r[8], "deadline": r[9]}
                 for r in c.fetchall()]
        return {"items": items, "count": len(items)}
    finally:
        conn.close()


@app.get("/api/day1/context-stats")
async def api_context_stats():
    """Context enrichment statistics."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) FROM municipality_context_packs")
        packs = c.fetchone()[0]
        c.execute("SELECT AVG(quality_score) FROM municipality_context_packs")
        avg_quality = c.fetchone()[0] or 0
        c.execute("SELECT COUNT(*) FROM proposal_drafts WHERE status = 'draft'")
        drafts = c.fetchone()[0]
        return {"context_packs": packs, "avg_pack_quality": round(avg_quality, 1),
                "total_proposals": drafts}
    finally:
        conn.close()


@app.get("/api/day1/pipeline-v2-stages")
async def api_pipeline_v2_stages():
    """Pipeline V2 stage distribution."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("SELECT current_stage, COUNT(*) FROM pipeline_v2_stages GROUP BY current_stage")
        dist = {r[0]: r[1] for r in c.fetchall()}
        stages = ['discovered', 'captured', 'detail_fetched', 'attachment_fetched',
                  'parsed', 'context_enriched', 'proposal_ready', 'bundle_ready',
                  'dryrun_ready', 'submission_ready']
        for s in stages:
            if s not in dist:
                dist[s] = 0
        return {"stages": dist, "total": sum(dist.values())}
    finally:
        conn.close()


@app.get("/api/day1/pipeline-v2-bottlenecks")
async def api_pipeline_bottlenecks():
    """Pipeline V2 bottleneck analysis."""
    import json as _json
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("SELECT current_stage, COUNT(*) FROM pipeline_v2_stages GROUP BY current_stage")
        dist = dict(c.fetchall())
        bottlenecks = []
        for stage, count in sorted(dist.items(), key=lambda x: -x[1]):
            if count >= 3:
                c.execute("SELECT gate_checks_json FROM pipeline_v2_stages WHERE current_stage = ? LIMIT 5", (stage,))
                checks = [_json.loads(r[0]) for r in c.fetchall() if r[0]]
                missing = set()
                for ch in checks:
                    if not ch.get('has_attachments'): missing.add('attachments')
                    if ch.get('parsed_count', 0) == 0: missing.add('parsed_text')
                    if not ch.get('has_proposal'): missing.add('proposal')
                    if not ch.get('has_context'): missing.add('context_pack')
                bottlenecks.append({"stage": stage, "count": count, "common_missing": list(missing)})
        return {"bottlenecks": bottlenecks}
    finally:
        conn.close()


@app.get("/api/day1/attachment-failures")
async def api_attachment_failures():
    """Attachment failure taxonomy."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("SELECT failure_type, COUNT(*) FROM attachment_failures GROUP BY failure_type ORDER BY COUNT(*) DESC")
        failures = [{"type": r[0], "count": r[1]} for r in c.fetchall()]
        c.execute("SELECT COUNT(*) FROM attachment_failures")
        total = c.fetchone()[0]
        return {"total_failures": total, "by_type": failures}
    finally:
        conn.close()


@app.get("/api/day1/dead-links")
async def api_dead_links():
    """Dead link analysis by domain."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("""SELECT aa.url FROM attachment_failures af
                     JOIN attachment_assets aa ON aa.id = af.attachment_asset_id
                     WHERE af.failure_type = 'http_404'""")
        urls = [r[0] for r in c.fetchall() if r[0]]
        from urllib.parse import urlparse
        domains = {}
        for url in urls:
            try:
                d = urlparse(url).netloc
                domains[d] = domains.get(d, 0) + 1
            except: pass
        sorted_domains = sorted(domains.items(), key=lambda x: -x[1])
        return {"total_dead": len(urls), "by_domain": [{"domain": d, "count": c2} for d, c2 in sorted_domains]}
    finally:
        conn.close()


@app.get("/api/day1/municipality-completeness/{muni_code}")
async def api_muni_completeness(muni_code: str):
    """Municipality data completeness."""
    import json as _json
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        checks = {}
        c.execute("SELECT COUNT(*) FROM procurement_items WHERE muni_code = ?", (muni_code,))
        checks['items'] = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM muni_news WHERE muni_code = ?", (muni_code,))
        checks['news'] = c.fetchone()[0]
        c.execute("SELECT quality_score FROM municipality_context_packs WHERE muni_code = ?", (muni_code,))
        ctx = c.fetchone()
        checks['context_quality'] = ctx[0] if ctx else 0
        c.execute("SELECT COUNT(*) FROM bid_projects bp JOIN procurement_items pi ON pi.item_id = bp.item_id WHERE pi.muni_code = ?", (muni_code,))
        checks['projects'] = c.fetchone()[0]

        score = 0
        if checks['items'] > 0: score += 25
        if checks['news'] > 0: score += 20
        if checks['context_quality'] >= 30: score += 20
        if checks['projects'] > 0: score += 15
        checks['completeness_score'] = score

        return checks
    finally:
        conn.close()


@app.get("/api/day1/burndown-summary")
async def api_burndown_summary():
    """Overall burn-down sprint summary."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) FROM bid_projects")
        total_projects = c.fetchone()[0]
        c.execute("SELECT parse_status, COUNT(*) FROM attachment_assets GROUP BY parse_status")
        att_status = {r[0]: r[1] for r in c.fetchall()}
        c.execute("SELECT COUNT(*) FROM backlog_queue WHERE auto_action_status = 'pending'")
        pending_backlog = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM municipality_context_packs")
        context_packs = c.fetchone()[0]
        c.execute("SELECT current_stage, COUNT(*) FROM pipeline_v2_stages GROUP BY current_stage")
        pipeline = {r[0]: r[1] for r in c.fetchall()}

        return {
            "bid_projects": total_projects,
            "attachments_parsed": att_status.get('parsed', 0),
            "attachments_pending": att_status.get('pending', 0),
            "attachments_error": att_status.get('error', 0),
            "backlog_pending": pending_backlog,
            "context_packs": context_packs,
            "pipeline_stages": pipeline
        }
    finally:
        conn.close()


# === End Day 1 Routes ===

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8007)
