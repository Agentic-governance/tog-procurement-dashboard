"""
Municipality Dashboard — Rich detail page powered by MCP tools.

Data sources:
  - Municipality MCP (port 8004): profile, ToG score, risk, trend, vendors, peers, documents, topics
  - Government Procurement MCP (port 8003): bid search, corporation search
  - J-SHIS (bosai.go.jp): earthquake hazard data (no API key required)
  - Nominatim (OpenStreetMap): geocoding for lat/lon (no API key required)
"""

import asyncio
import json
import os
import time
import re
from datetime import datetime
from typing import Optional

from urllib.parse import quote

import httpx

try:
    import redis.asyncio as aioredis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

# ── MCP Config ─────────────────────────────────────────────────────────────

MUNI_MCP_URL = os.environ.get('MUNI_MCP_URL', 'http://78.46.57.151:8004/mcp/')
GOVT_MCP_URL = os.environ.get('GOVT_MCP_URL', 'http://78.46.57.151:8003/mcp/')
REDIS_URL = os.environ.get('REDIS_URL', '')
MCP_TIMEOUT = 25.0  # seconds per call

CACHE_TTL = 3600  # 1 hour

# Redis connection (lazy init)
_redis_pool = None

# Fallback in-memory cache (used when Redis unavailable)
_mem_cache = {}


async def _get_redis():
    global _redis_pool
    if not HAS_REDIS or not REDIS_URL:
        return None
    if _redis_pool is None:
        try:
            _redis_pool = aioredis.from_url(REDIS_URL, decode_responses=True)
            await _redis_pool.ping()
        except Exception:
            _redis_pool = None
            return None
    return _redis_pool


def _cache_get(key):
    """Synchronous in-memory cache get (fallback)."""
    if key in _mem_cache:
        ts, data = _mem_cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
        del _mem_cache[key]
    return None


def _cache_set(key, data):
    """Synchronous in-memory cache set (fallback)."""
    _mem_cache[key] = (time.time(), data)
    if len(_mem_cache) > 500:
        cutoff = time.time() - CACHE_TTL
        to_del = [k for k, (ts, _) in _mem_cache.items() if ts < cutoff]
        for k in to_del:
            del _mem_cache[k]


async def cache_get_async(key):
    """Async cache get: try Redis first, fallback to in-memory."""
    r = await _get_redis()
    if r:
        try:
            val = await r.get(f"muni:{key}")
            if val:
                return json.loads(val)
        except Exception:
            pass
    return _cache_get(key)


async def cache_set_async(key, data, ttl=CACHE_TTL):
    """Async cache set: write to Redis + in-memory fallback."""
    _cache_set(key, data)
    r = await _get_redis()
    if r:
        try:
            await r.setex(f"muni:{key}", ttl, json.dumps(data, ensure_ascii=False, default=str))
        except Exception:
            pass


# ── MCP Client ─────────────────────────────────────────────────────────────

def _parse_sse(text: str) -> dict:
    """Parse SSE response to extract JSON-RPC result."""
    for line in text.split('\n'):
        if line.startswith('data: '):
            try:
                msg = json.loads(line[6:])
                if 'result' in msg:
                    content = msg['result'].get('content', [])
                    for c in content:
                        if c.get('type') == 'text':
                            return json.loads(c['text'])
                return msg.get('result', {})
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
    return {}


async def mcp_call(client: httpx.AsyncClient, base_url: str, session_id: str,
                   tool_name: str, arguments: dict) -> dict:
    """Call an MCP tool and return parsed result. Uses streaming to handle SSE."""
    cache_key = f"{base_url}:{tool_name}:{json.dumps(arguments, sort_keys=True)}"
    cached = await cache_get_async(cache_key)
    if cached is not None:
        return cached

    headers = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id

    body = {
        "jsonrpc": "2.0", "id": 2,
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments}
    }

    try:
        # Use streaming to handle SSE — read first complete message then close
        async with client.stream("POST", base_url, json=body, headers=headers,
                                  timeout=MCP_TIMEOUT) as resp:
            ct = resp.headers.get('content-type', '')
            if 'event-stream' in ct:
                # Read SSE lines until we get a complete JSON-RPC result
                result = {}
                async for line in resp.aiter_lines():
                    if line.startswith('data: '):
                        try:
                            msg = json.loads(line[6:])
                            if 'result' in msg:
                                content = msg['result'].get('content', [])
                                for c in content:
                                    if c.get('type') == 'text':
                                        result = json.loads(c['text'])
                                        break
                                if not result:
                                    result = msg.get('result', {})
                                break  # Got our result, stop reading
                        except (json.JSONDecodeError, KeyError, TypeError):
                            continue
                await cache_set_async(cache_key, result)
                return result
            else:
                # Regular JSON response
                body_bytes = await resp.aread()
                msg = json.loads(body_bytes)
                content = msg.get('result', {}).get('content', [])
                result = {}
                for c in content:
                    if c.get('type') == 'text':
                        try:
                            result = json.loads(c['text'])
                        except (json.JSONDecodeError, TypeError):
                            result = {'text': c['text']}
                        break
                if not result:
                    result = msg.get('result', {})
                await cache_set_async(cache_key, result)
                return result
    except Exception as e:
        return {'error': str(e)}


async def mcp_init_session(client: httpx.AsyncClient, base_url: str) -> str:
    """Initialize MCP session and return session ID."""
    headers = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
    body = {
        "jsonrpc": "2.0", "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "procurement-dashboard", "version": "2.0"}
        }
    }
    try:
        async with client.stream("POST", base_url, json=body, headers=headers, timeout=10) as resp:
            sid = resp.headers.get("mcp-session-id", "")
            # Consume at least one line to let the server finish
            async for line in resp.aiter_lines():
                if line.startswith('data: '):
                    break
            return sid
    except Exception:
        return ""


# ── External APIs (no API key needed) ─────────────────────────────────────

JSHIS_MESH_BASE = 'https://www.j-shis.bosai.go.jp/map/api/pshm/Y2024/AVR/TTL_MTTL/meshinfo.geojson'
JSHIS_FAULT_BASE = 'https://www.j-shis.bosai.go.jp/map/api/fltsearch'
NOMINATIM_BASE = 'https://nominatim.openstreetmap.org/search'

# Geocode cache: {muni_name: (timestamp, (lat, lon) or None)}
_geocode_cache = {}
GEOCODE_TTL_OK = 86400  # 24h for successful lookups
GEOCODE_TTL_FAIL = 300  # 5min for failures (retry soon)


def _muni_code_to_5digit(muni_code: str) -> str:
    """Convert 6-digit municipality code to 5-digit (drop check digit)."""
    return muni_code[:5] if len(muni_code) >= 5 else muni_code


async def _fetch_jshis_faults(muni_code: str) -> dict:
    """Fetch earthquake fault sources from J-SHIS by municipality code.
    Uses areacode=A{5-digit} format. No geocoding needed."""
    code5 = _muni_code_to_5digit(muni_code)
    cache_key = f"jshis_faults:{code5}"
    cached = await cache_get_async(cache_key)
    if cached is not None:
        return cached

    try:
        url = (f"{JSHIS_FAULT_BASE}?areacode=A{code5}&ecode=ALL_NT_A"
               f"&mode=C&version=Y2024&case=AVR&period=P_T30&format=json")
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10.0)
            data = resp.json()
            if data.get('status') == 'Success':
                faults = data.get('Fault', [])
                top_faults = sorted(faults, key=lambda f: float(f.get('probability', '0')),
                                    reverse=True)[:5]
                result = {
                    'total_faults': len(faults),
                    'faults': [{
                        'name': f.get('ltename', ''),
                        'probability': _safe_float(f.get('probability')),
                        'magnitude': f.get('magnitude', ''),
                    } for f in top_faults],
                }
                await cache_set_async(cache_key, result)
                return result
    except Exception:
        pass
    return {}


async def _geocode_municipality(name: str, prefecture: str) -> Optional[tuple]:
    """Geocode a Japanese municipality using Nominatim (free, no API key).
    Returns (lat, lon) or None on failure. Results are cached with TTL."""
    cache_key = f"{prefecture}{name}"
    if cache_key in _geocode_cache:
        ts, result = _geocode_cache[cache_key]
        ttl = GEOCODE_TTL_OK if result else GEOCODE_TTL_FAIL
        if time.time() - ts < ttl:
            return result

    try:
        q = quote(f"{name} {prefecture}")
        url = f"{NOMINATIM_BASE}?q={q}&format=json&limit=1&accept-language=ja&countrycodes=jp"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers={'User-Agent': 'procurement-dashboard/2.0'},
                                    timeout=8.0)
            data = resp.json()
            if data and len(data) > 0:
                lat = float(data[0]['lat'])
                lon = float(data[0]['lon'])
                _geocode_cache[cache_key] = (time.time(), (lat, lon))
                return (lat, lon)
    except Exception:
        pass
    _geocode_cache[cache_key] = (time.time(), None)
    return None


async def _fetch_jshis_mesh(lat: float, lon: float) -> dict:
    """Fetch earthquake hazard mesh data from J-SHIS for a lat/lon position.
    Returns dict with probability data or empty dict on failure."""
    cache_key = f"jshis_mesh:{lat:.3f},{lon:.3f}"
    cached = await cache_get_async(cache_key)
    if cached is not None:
        return cached

    try:
        url = f"{JSHIS_MESH_BASE}?position={lon},{lat}&epsg=4326"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10.0)
            data = resp.json()
            if data.get('status') == 'Success' and data.get('features'):
                props = data['features'][0].get('properties', {})
                result = {
                    'prob_5lower_30y': _safe_float(props.get('T30_I45_PS')),
                    'prob_5upper_30y': _safe_float(props.get('T30_I50_PS')),
                    'prob_6lower_30y': _safe_float(props.get('T30_I55_PS')),
                    'prob_6upper_30y': _safe_float(props.get('T30_I60_PS')),
                    'max_intensity_50y_02': _safe_float(props.get('T50_P02_SI')),
                    'surface_velocity': _safe_float(props.get('T30_P03_SV')),
                }
                await cache_set_async(cache_key, result)
                return result
    except Exception:
        pass
    return {}


def _safe_float(v):
    """Convert string to float, return None on failure."""
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ── Data Fetching ──────────────────────────────────────────────────────────

async def _call_with_own_session(base_url: str, tool_name: str, arguments: dict) -> dict:
    """Make an MCP call with its own dedicated client + session."""
    async with httpx.AsyncClient() as client:
        sid = await mcp_init_session(client, base_url)
        return await mcp_call(client, base_url, sid, tool_name, arguments)


POLICY_KEYWORDS = ['DX', '脱炭素', 'カーボンニュートラル', '子育て支援', '公共施設マネジメント',
                    'PPP', 'PFI', 'オーバーツーリズム', 'SDGs', 'デジタル田園都市']


async def fetch_muni_dashboard_data(muni_code: str) -> dict:
    """Fetch all dashboard data concurrently from MCPs.
    Each call gets its own session to avoid SSE serialization issues.
    """
    t0 = time.perf_counter()

    # Launch all calls in parallel, each with its own session
    tasks = [
        _call_with_own_session(MUNI_MCP_URL, "muni_profile", {"muni_code": muni_code}),
        _call_with_own_session(MUNI_MCP_URL, "muni_analyze_tog", {"muni_code": muni_code}),
        _call_with_own_session(MUNI_MCP_URL, "muni_analyze_risk", {"muni_code": muni_code}),
        _call_with_own_session(MUNI_MCP_URL, "muni_analyze_trend", {"muni_code": muni_code}),
        _call_with_own_session(MUNI_MCP_URL, "muni_vendors", {"muni_code": muni_code}),
        _call_with_own_session(MUNI_MCP_URL, "muni_analyze_peers", {"muni_code": muni_code, "n_peers": 5}),
        _call_with_own_session(MUNI_MCP_URL, "muni_documents", {"muni_code": muni_code, "limit": 15}),
        _call_with_own_session(MUNI_MCP_URL, "muni_topic_counts", {"muni_code": muni_code}),
        _call_with_own_session(MUNI_MCP_URL, "muni_generate_brief", {"muni_code": muni_code}),
        # Policy keyword adoption (lightweight — only checks this municipality)
        _call_with_own_session(MUNI_MCP_URL, "muni_detect_policy_shift", {
            "keywords": POLICY_KEYWORDS, "muni_codes": [muni_code]
        }),
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    (profile, tog, risk, trend, vendors, peers, documents, topics, brief,
     policy_shift) = [
        r if isinstance(r, dict) else {'error': str(r)} for r in results
    ]
    plans_search = {}  # Disabled: LIKE fallback too slow, returns empty for most munis

    # Prefecture name + municipality name for secondary lookups
    pref_name = ''
    muni_name = ''
    p_data = profile.get('data', profile)
    if p_data.get('identity'):
        pref_name = p_data['identity'].get('prefecture', '')
        muni_name = p_data['identity'].get('name', '')

    # Parallel: bids search + J-SHIS earthquake hazard (faults + mesh)
    bids = {}
    eq_faults = {}
    eq_mesh = {}

    async def _fetch_bids():
        if not pref_name:
            return {}
        try:
            return await _call_with_own_session(GOVT_MCP_URL, "govt_search_bids", {
                "query": pref_name, "count": 10
            })
        except Exception:
            return {}

    async def _fetch_eq_mesh():
        """Fetch mesh-based probability data (needs geocoding)."""
        if not muni_name or not pref_name:
            return {}
        coords = await _geocode_municipality(muni_name, pref_name)
        if not coords:
            return {}
        return await _fetch_jshis_mesh(coords[0], coords[1])

    bids, eq_faults, eq_mesh = await asyncio.gather(
        _fetch_bids(),
        _fetch_jshis_faults(muni_code),  # Direct — uses muni_code, no geocoding
        _fetch_eq_mesh(),
    )

    elapsed = time.perf_counter() - t0

    return {
        'profile': profile, 'tog': tog, 'risk': risk, 'trend': trend,
        'vendors': vendors, 'peers': peers, 'documents': documents,
        'topics': topics, 'brief': brief, 'bids': bids,
        'policy_shift': policy_shift, 'plans_search': plans_search,
        'eq_faults': eq_faults, 'eq_mesh': eq_mesh,
        'fetch_time': elapsed,
    }


# ── HTML Rendering ─────────────────────────────────────────────────────────

def esc(s):
    if not s:
        return ''
    return str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def fmt_num(n, default='-'):
    if n is None:
        return default
    try:
        v = float(n)
        if v == int(v):
            return f'{int(v):,}'
        return f'{v:,.1f}'
    except (ValueError, TypeError):
        return str(n)


def pct_bar(value, max_val=100, color='#3b82f6', height=8):
    pct = min(100, max(0, (value / max_val * 100) if max_val else 0))
    return f'''<div style="background:#e2e8f0;border-radius:{height//2}px;height:{height}px;overflow:hidden;flex:1">
      <div style="width:{pct:.0f}%;background:{color};height:100%;border-radius:{height//2}px"></div>
    </div>'''


def gauge_indicator(value, ranges, label=''):
    """Create a color-coded value display based on ranges.
    ranges: list of (threshold, color, label) sorted ascending
    """
    color = '#6b7280'
    for threshold, c, _ in ranges:
        if value <= threshold:
            color = c
            break
    else:
        if ranges:
            color = ranges[-1][1]
    return f'<span style="color:{color};font-weight:700;font-size:1.3rem">{fmt_num(value)}</span>'


def _safe_get(d):
    """Unwrap nested 'data' key from MCP response dict."""
    if not isinstance(d, dict):
        return {}
    return d.get('data', d)


def render_muni_dashboard(muni_code: str, data: dict, local_data: dict = None, timing: float = 0) -> str:
    """Render the full municipality dashboard HTML with 6-tab layout."""
    if local_data is None:
        local_data = {'news': [], 'items': [], 'item_count': 0}
    d = data
    prof = _safe_get(d['profile'])
    tog = _safe_get(d['tog'])
    risk = _safe_get(d['risk'])
    trend = _safe_get(d['trend'])
    vendors_data = _safe_get(d['vendors'])
    peers = _safe_get(d['peers'])
    docs = _safe_get(d['documents'])
    topics = _safe_get(d['topics'])
    brief = _safe_get(d['brief'])
    policy_shift = _safe_get(d.get('policy_shift', {}))
    plans_search = _safe_get(d.get('plans_search', {}))
    eq_faults = d.get('eq_faults', {})
    eq_mesh = d.get('eq_mesh', {})
    bids = d['bids'].get('results', d['bids'].get('data', {}).get('results', []))
    if not isinstance(bids, list):
        bids = []

    # Identity
    identity = prof.get('identity', {})
    name = identity.get('name', muni_code)
    prefecture = identity.get('prefecture', '')
    category = identity.get('category', '')
    url = identity.get('url', '')

    # Demographics
    demo = prof.get('demographics', {})
    pop = demo.get('population')
    pop_under15 = demo.get('pop_under15', 0)
    pop_15_64 = demo.get('pop_15_64', 0)
    pop_over65 = demo.get('pop_over65', 0)
    area = demo.get('area_km2')
    density = demo.get('density_per_km2')
    aging = demo.get('aging_rate_pct')
    households = demo.get('households')
    establishments = demo.get('establishments')
    dep_ratio = demo.get('dependency_ratio')

    # Fiscal
    fiscal = prof.get('fiscal', {})
    fsi = fiscal.get('fiscal_strength_index')
    cbr = fiscal.get('current_balance_ratio_pct')
    rdsr = fiscal.get('real_debt_service_ratio_pct')
    fbr = fiscal.get('future_burden_ratio_pct')
    fy = fiscal.get('fiscal_year', '')

    # ToG
    tog_score = tog.get('tog_score', 0)
    tog_rank = tog.get('national_rank', '-')
    tog_total = tog.get('national_total', 1710)
    tog_pref_rank = tog.get('pref_rank', '-')
    tog_pref_total = tog.get('pref_total', '-')
    tog_components = tog.get('components', {})

    # Risk
    risk_score = risk.get('risk_score', 0)
    risk_rank = risk.get('national_rank', '-')
    risk_components = risk.get('components', {})
    rcomp_labels = {'low_transparency': '透明性不足', 'vendor_concentration': 'ベンダー集中',
                    'declining_trend': '情報減少', 'low_activity': '調達活動低下'}

    # Vendor
    prof_vendors = prof.get('vendors', {})
    hhi = prof_vendors.get('hhi', 0)
    vendor_count = prof_vendors.get('count', 0)
    top3 = prof_vendors.get('top_3', [])

    # Brief
    urgency = brief.get('urgency', '-')
    urgency_colors = {'高': '#dc2626', '中': '#d97706', '低': '#16a34a'}
    urgency_color = urgency_colors.get(urgency, '#6b7280')

    # Category badge colors
    cat_colors = {
        '政令指定都市': '#7c3aed', '中核市': '#2563eb', '施行時特例市': '#0891b2',
        '特別区': '#be185d', '市': '#059669', '町': '#d97706', '村': '#6b7280',
    }
    cat_color = cat_colors.get(category, '#6b7280')

    # ── Build HTML sections ──

    # 1. Hero + KPIs
    hero = f'''
    <div style="display:flex;align-items:center;gap:1rem;flex-wrap:wrap;margin-bottom:1rem">
      <div>
        <span style="display:inline-block;background:#e0e7ff;color:#3730a3;padding:0.15rem 0.6rem;
              border-radius:4px;font-size:0.8rem;font-weight:600">{esc(prefecture)}</span>
        <span style="display:inline-block;background:{cat_color};color:white;padding:0.15rem 0.6rem;
              border-radius:4px;font-size:0.8rem;font-weight:600;margin-left:0.3rem">{esc(category)}</span>
      </div>
      <h1 style="font-size:1.6rem;font-weight:800">{esc(name)}</h1>
      <span style="color:#64748b;font-size:0.85rem">Code: {muni_code}</span>
      <a href="{esc(url)}" target="_blank" style="font-size:0.85rem;text-decoration:none;
         color:#2563eb">公式サイト &#8599;</a>
      <div style="margin-left:auto">
        <span style="display:inline-block;background:{urgency_color};color:white;padding:0.2rem 0.8rem;
              border-radius:6px;font-size:0.85rem;font-weight:700">営業優先度: {esc(urgency)}</span>
      </div>
    </div>

    <div class="kpi-row" style="grid-template-columns:repeat(5,1fr)">
      <div class="kpi kpi-accent">
        <div class="kpi-label">人口</div>
        <div class="kpi-value" style="font-size:1.5rem">{fmt_num(pop)}</div>
        <div class="kpi-sub">高齢化率: {fmt_num(aging)}%</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">財政力指数</div>
        <div class="kpi-value" style="font-size:1.5rem">{fmt_num(fsi)}</div>
        <div class="kpi-sub">{esc(fy)}</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">透明性スコア</div>
        <div class="kpi-value" style="font-size:1.5rem;color:#2563eb">{fmt_num(tog_score)}</div>
        <div class="kpi-sub">全国 {tog_rank}/{tog_total}位</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">調達リスク</div>
        <div class="kpi-value" style="font-size:1.5rem;color:{'#dc2626' if risk_score and risk_score > 60 else '#d97706' if risk_score and risk_score > 30 else '#16a34a'}">{fmt_num(risk_score)}</div>
        <div class="kpi-sub">全国 {risk_rank}位</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">ベンダー数</div>
        <div class="kpi-value" style="font-size:1.5rem">{fmt_num(vendor_count)}</div>
        <div class="kpi-sub">HHI: {fmt_num(hhi)}</div>
      </div>
    </div>'''

    # 1b. Sales Brief — synthesized from brief data (urgency, scores, metrics, peer comparison)
    brief_scores = brief.get('scores', {})
    brief_metrics = brief.get('key_metrics', {})
    brief_peer = brief.get('peer_comparison', {})

    brief_tog = brief_scores.get('tog', {})
    brief_risk_sc = brief_scores.get('risk', {})

    # Generate insights from structured data
    insights = []
    # ToG insight
    if brief_tog:
        tog_nat_rank = brief_tog.get('national_rank', '-')
        tog_pref_r = brief_tog.get('pref_rank', '-')
        insights.append(f'透明性: 全国{tog_nat_rank}位, 県内{tog_pref_r}位')
    # Risk insight
    if brief_risk_sc:
        risk_nat_rank = brief_risk_sc.get('national_rank', '-')
        insights.append(f'リスク: 全国{risk_nat_rank}位')
    # Vendor concentration
    bm_hhi = brief_metrics.get('hhi', 0)
    if bm_hhi and bm_hhi > 0.5:
        insights.append(f'ベンダー集中度が高い (HHI={fmt_num(bm_hhi)})')
    elif bm_hhi and bm_hhi < 0.2:
        insights.append(f'ベンダー分散 (HHI={fmt_num(bm_hhi)})')
    # Peer comparison
    peer_mean_tog = brief_peer.get('peer_mean_tog', 0)
    if tog_score and peer_mean_tog:
        if tog_score > peer_mean_tog * 1.2:
            insights.append(f'同規模自治体平均({fmt_num(peer_mean_tog)})より透明性が高い')
        elif tog_score < peer_mean_tog * 0.8:
            insights.append(f'同規模自治体平均({fmt_num(peer_mean_tog)})より透明性が低い — 改善余地あり')

    # Generate challenges/recommendations based on risk components
    challenges = []
    recommendations = []
    for rkey, rlabel in rcomp_labels.items():
        comp = risk_components.get(rkey, {})
        rscore = comp.get('score', 0)
        if rscore > 50:
            challenges.append(rlabel)
    if hhi and hhi > 0.5:
        challenges.append('ベンダー集中度が高く競争性に課題')
        recommendations.append('入札参加資格の緩和、分離発注の検討')
    if tog_score and tog_score < 40:
        challenges.append('情報公開が不十分')
        recommendations.append('調達情報・議事録のウェブ公開強化')
    if not challenges:
        recommendations.append('良好な透明性水準を維持')

    brief_challenges_html = f'''
    <div class="tbl-wrap" style="padding:1.2rem;margin-bottom:1.5rem;border-left:3px solid #f59e0b">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.8rem">
        <span style="font-weight:700;font-size:1rem;color:#1e293b">営業ブリーフ</span>
        <span style="display:inline-block;background:{urgency_color};color:white;padding:0.15rem 0.6rem;
              border-radius:6px;font-size:0.78rem;font-weight:700">優先度: {esc(urgency)}</span>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:0.4rem;margin-bottom:1rem">
        {''.join(f'<span style="display:inline-block;padding:0.2rem 0.6rem;background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;font-size:0.8rem">{esc(i)}</span>' for i in insights)}
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1.5rem">
        <div>
          <div style="font-weight:600;font-size:0.85rem;color:#dc2626;margin-bottom:0.4rem">課題・注意点</div>
          {''.join(f'<div style="display:flex;align-items:flex-start;gap:0.4rem;margin-bottom:0.3rem;font-size:0.85rem"><span style="color:#dc2626">&#9679;</span> {esc(c)}</div>' for c in challenges) if challenges else '<div style="color:#64748b;font-size:0.85rem">特記事項なし</div>'}
        </div>
        <div>
          <div style="font-weight:600;font-size:0.85rem;color:#16a34a;margin-bottom:0.4rem">推奨アプローチ</div>
          {''.join(f'<div style="display:flex;align-items:flex-start;gap:0.4rem;margin-bottom:0.3rem;font-size:0.85rem"><span style="color:#16a34a">&#10003;</span> {esc(r)}</div>' for r in recommendations)}
        </div>
      </div>
    </div>'''

    # 1c. Policy keyword adoption timeline
    policy_results = policy_shift.get('results', [])
    policy_html = ''
    if policy_results or isinstance(policy_shift, dict):
        # Build a keyword → year mapping
        kw_years = {}
        for pr in policy_results:
            # The policy_shift might return one result per keyword match
            year = pr.get('first_occurrence_year')
            doc_title = pr.get('doc_title', '')
            # Try to figure out which keyword matched
            for kw in POLICY_KEYWORDS:
                if kw.lower() in (doc_title or '').lower() or kw in str(pr):
                    kw_years[kw] = {'year': year, 'title': doc_title}
                    break
            else:
                # Couldn't match keyword, store generically
                kw_years[doc_title[:20] if doc_title else '?'] = {'year': year, 'title': doc_title}

        kw_badges = ''
        for kw in POLICY_KEYWORDS:
            info = kw_years.get(kw)
            if info and info['year']:
                kw_badges += f'''<span style="display:inline-flex;align-items:center;gap:0.3rem;
                    padding:0.25rem 0.6rem;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:6px;
                    font-size:0.78rem;margin:0.15rem" title="{esc(info.get('title',''))}">
                    <span style="font-weight:700;color:#16a34a">{esc(kw)}</span>
                    <span style="color:#64748b">{info['year']}</span></span>'''
            else:
                kw_badges += f'''<span style="display:inline-flex;align-items:center;gap:0.3rem;
                    padding:0.25rem 0.6rem;background:#f9fafb;border:1px solid #e2e8f0;border-radius:6px;
                    font-size:0.78rem;margin:0.15rem;opacity:0.5">
                    <span style="color:#94a3b8">{esc(kw)}</span>
                    <span style="color:#d1d5db">-</span></span>'''

        if kw_badges:
            policy_html = f'''
            <div class="tbl-wrap" style="padding:1rem;margin-bottom:1.5rem">
              <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.6rem;color:#1e293b">
                政策キーワード採用状況
                <span style="font-weight:400;font-size:0.75rem;color:#64748b;margin-left:0.5rem">
                  文書中に初めて登場した年</span>
              </div>
              <div style="display:flex;flex-wrap:wrap;gap:0.1rem">{kw_badges}</div>
            </div>'''

    # 1d. Key plans found via fulltext search
    plans_list = plans_search.get('results', [])
    plans_html = ''
    if plans_list:
        plan_items = ''
        for pl in plans_list[:5]:
            ptitle = pl.get('title', '(無題)')
            purl = pl.get('url', '')
            ptype = pl.get('doc_type', '')
            pyear = pl.get('year', '')
            plan_items += f'''<div style="display:flex;align-items:flex-start;gap:0.5rem;padding:0.4rem 0;
                border-bottom:1px solid #f1f5f9">
                <span style="display:inline-block;background:#dbeafe;color:#1d4ed8;padding:0.05rem 0.3rem;
                      border-radius:3px;font-size:0.68rem;font-weight:600;white-space:nowrap">{esc(ptype)}</span>
                <a href="{esc(purl)}" target="_blank" style="font-size:0.85rem;color:#1e293b;
                   text-decoration:none;font-weight:500">{esc(str(ptitle)[:80])}</a>
                <span style="color:#94a3b8;font-size:0.78rem;white-space:nowrap;margin-left:auto">{pyear}</span>
            </div>'''
        plans_html = f'''
        <div class="tbl-wrap" style="padding:1rem;margin-bottom:1.5rem">
          <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.6rem;color:#1e293b">
            主要計画文書
          </div>
          {plan_items}
        </div>'''

    # 2. Demographics + Fiscal (two columns)
    fiscal_html = ''
    fiscal_items = [
        ('財政力指数', fsi, 0, 2, [(0.5, '#dc2626', '低'), (0.8, '#d97706', '中'), (2.0, '#16a34a', '高')]),
        ('経常収支比率', cbr, 0, 100, [(90, '#16a34a', '健全'), (95, '#d97706', '注意'), (100, '#dc2626', '硬直')]),
        ('実質公債費比率', rdsr, 0, 25, [(10, '#16a34a', '健全'), (18, '#d97706', '注意'), (25, '#dc2626', '危険')]),
        ('将来負担比率', fbr, 0, 350, [(100, '#16a34a', '低'), (200, '#d97706', '中'), (350, '#dc2626', '高')]),
    ]
    for label, value, lo, hi, ranges in fiscal_items:
        if value is None:
            continue
        color = '#6b7280'
        status = ''
        for thr, c, s in ranges:
            if value <= thr:
                color = c
                status = s
                break
        else:
            color = ranges[-1][1]
            status = ranges[-1][2]
        pct = min(100, max(0, (value - lo) / (hi - lo) * 100)) if hi > lo else 0
        fiscal_html += f'''
        <div style="margin-bottom:0.7rem">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.2rem">
            <span style="font-size:0.8rem;color:#64748b">{label}</span>
            <span style="font-weight:700;color:{color}">{fmt_num(value)} <span style="font-size:0.7rem">{status}</span></span>
          </div>
          <div style="background:#e2e8f0;border-radius:4px;height:6px;overflow:hidden">
            <div style="width:{pct:.0f}%;background:{color};height:100%;border-radius:4px"></div>
          </div>
        </div>'''

    # Age structure data for donut chart
    age_data = {
        'labels': ['15歳未満', '15-64歳', '65歳以上'],
        'values': [pop_under15 or 0, pop_15_64 or 0, pop_over65 or 0],
        'colors': ['#3b82f6', '#10b981', '#f59e0b'],
    }

    # e-Stat data section (will be populated via API)
    estat_data = data.get('estat', {})
    industry = estat_data.get('industry', {})
    industry_html = ''
    if industry:
        ind_items = [
            ('建設業', industry.get('construction', 0), '#dc2626'),
            ('製造業', industry.get('manufacturing', 0), '#f59e0b'),
            ('卸売・小売', industry.get('wholesale_retail', 0), '#3b82f6'),
            ('宿泊・飲食', industry.get('accommodation_food', 0), '#8b5cf6'),
            ('医療・福祉', industry.get('medical_welfare', 0), '#10b981'),
            ('その他', industry.get('other', 0), '#6b7280'),
        ]
        max_ind = max((v for _, v, _ in ind_items), default=1) or 1
        for ilbl, ival, iclr in ind_items:
            ipct = ival / max_ind * 100
            industry_html += f'''
            <div style="display:flex;align-items:center;gap:0.4rem;margin-bottom:0.3rem">
              <span style="width:70px;font-size:0.78rem;text-align:right;color:#64748b">{ilbl}</span>
              <div style="flex:1;background:#e2e8f0;border-radius:3px;height:16px;overflow:hidden">
                <div style="width:{ipct:.0f}%;background:{iclr};height:100%;border-radius:3px;min-width:{3 if ival else 0}px"></div>
              </div>
              <span style="width:40px;font-size:0.78rem;font-weight:600">{fmt_num(ival)}</span>
            </div>'''

    demo_fiscal = f'''
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:1rem;margin-bottom:1.5rem">
      <div class="tbl-wrap" style="padding:1.2rem">
        <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.8rem;color:#1e293b">
          基礎情報</div>
        <table style="border:none">
          <tr><td style="border:none;color:#64748b;width:100px">人口</td>
              <td style="border:none;font-weight:600">{fmt_num(pop)} 人</td></tr>
          <tr><td style="border:none;color:#64748b">世帯数</td>
              <td style="border:none;font-weight:600">{fmt_num(households)}</td></tr>
          <tr><td style="border:none;color:#64748b">面積</td>
              <td style="border:none;font-weight:600">{fmt_num(area)} km&sup2;</td></tr>
          <tr><td style="border:none;color:#64748b">人口密度</td>
              <td style="border:none;font-weight:600">{fmt_num(density)} /km&sup2;</td></tr>
          <tr><td style="border:none;color:#64748b">事業所数</td>
              <td style="border:none;font-weight:600">{fmt_num(establishments)}</td></tr>
        </table>
        <div style="margin-top:0.8rem">
          <div style="font-weight:600;font-size:0.8rem;color:#64748b;margin-bottom:0.4rem">年齢構成</div>
          <canvas id="ageChart" width="200" height="140"></canvas>
        </div>
      </div>
      <div class="tbl-wrap" style="padding:1.2rem">
        <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.8rem;color:#1e293b">
          財政指標 <span style="font-weight:400;font-size:0.75rem;color:#64748b">{esc(fy)}</span></div>
        {fiscal_html if fiscal_html else '<p style="color:#64748b">データなし</p>'}
      </div>
      <div class="tbl-wrap" style="padding:1.2rem">
        <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.8rem;color:#1e293b">
          産業構造 <span style="font-weight:400;font-size:0.75rem;color:#64748b">事業所数</span></div>
        {industry_html if industry_html else f'<div style="font-size:0.85rem;color:#64748b"><p>総事業所数: {fmt_num(establishments)}</p><p style="margin-top:0.5rem;font-size:0.78rem">業種別データは e-Stat API 連携後に表示</p></div>'}
      </div>
    </div>'''

    # 2b. Earthquake hazard (J-SHIS — faults + mesh probabilities)
    earthquake_html = ''
    if eq_faults or eq_mesh:
        # Mesh-based probability bars (if geocoding succeeded)
        mesh_html = ''
        eq_level_html = ''
        if eq_mesh:
            p5l = eq_mesh.get('prob_5lower_30y')
            p5u = eq_mesh.get('prob_5upper_30y')
            p6l = eq_mesh.get('prob_6lower_30y')
            p6u = eq_mesh.get('prob_6upper_30y')

            def _hazard_bar(label, prob, warning_threshold=0.26, danger_threshold=0.60):
                if prob is None:
                    return ''
                pct_val = prob * 100
                color = '#dc2626' if prob >= danger_threshold else '#d97706' if prob >= warning_threshold else '#16a34a'
                bar_w = min(100, max(2, pct_val))
                return f'''
                <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.6rem">
                  <span style="width:90px;font-size:0.8rem;color:#64748b;text-align:right">{label}</span>
                  <div style="flex:1;background:#e2e8f0;border-radius:4px;height:22px;overflow:hidden;position:relative">
                    <div style="width:{bar_w:.1f}%;background:{color};height:100%;border-radius:4px;min-width:3px"></div>
                    <span style="position:absolute;right:6px;top:2px;font-size:0.72rem;font-weight:700;
                          color:{'white' if bar_w > 40 else '#1e293b'}">{pct_val:.1f}%</span>
                  </div>
                </div>'''

            max_prob = max((p for p in [p5l, p5u, p6l, p6u] if p is not None), default=0)
            if max_prob >= 0.90:
                eq_level, eq_color = '極めて高い', '#dc2626'
            elif max_prob >= 0.60:
                eq_level, eq_color = '高い', '#dc2626'
            elif max_prob >= 0.26:
                eq_level, eq_color = 'やや高い', '#d97706'
            elif max_prob >= 0.06:
                eq_level, eq_color = 'やや低い', '#16a34a'
            else:
                eq_level, eq_color = '低い', '#16a34a'

            eq_level_html = f'''<span style="display:inline-block;background:{eq_color};color:white;
                  padding:0.2rem 0.8rem;border-radius:6px;font-size:0.85rem;font-weight:700">{eq_level}</span>'''
            mesh_html = f'''
            <div>
              <div style="font-weight:600;font-size:0.82rem;color:#475569;margin-bottom:0.5rem">
                今後30年間の揺れの確率</div>
              {_hazard_bar('震度5弱以上', p5l)}
              {_hazard_bar('震度5強以上', p5u)}
              {_hazard_bar('震度6弱以上', p6l, 0.06, 0.26)}
              {_hazard_bar('震度6強以上', p6u, 0.03, 0.06)}
            </div>'''

        # Fault-based risk list (always available — uses muni_code directly)
        fault_html = ''
        if eq_faults and eq_faults.get('faults'):
            fault_rows = ''
            for f in eq_faults['faults']:
                fprob = f.get('probability')
                fprob_pct = fprob * 100 if fprob else 0
                fcolor = '#dc2626' if fprob and fprob > 0.26 else '#d97706' if fprob and fprob > 0.03 else '#16a34a'
                fmag = f.get('magnitude', '')
                if fmag.startswith('-'):
                    fmag = 'M' + fmag[1:]
                elif fmag:
                    fmag = 'M' + fmag
                fault_rows += f'''
                <div style="display:flex;align-items:center;gap:0.5rem;padding:0.35rem 0;
                     border-bottom:1px solid #f1f5f9">
                  <span style="flex:1;font-size:0.82rem;color:#1e293b">{esc(f.get('name', ''))}</span>
                  <span style="font-size:0.75rem;color:#64748b;white-space:nowrap">{fmag}</span>
                  <span style="font-weight:700;color:{fcolor};font-size:0.82rem;min-width:50px;text-align:right">
                    {fprob_pct:.1f}%</span>
                </div>'''
            total_faults = eq_faults.get('total_faults', 0)
            fault_html = f'''
            <div>
              <div style="font-weight:600;font-size:0.82rem;color:#475569;margin-bottom:0.5rem">
                影響断層 (上位5 / {total_faults}件)</div>
              {fault_rows}
            </div>'''

            # If no mesh data, derive level from fault probabilities
            if not eq_level_html and eq_faults['faults']:
                top_prob = eq_faults['faults'][0].get('probability', 0) or 0
                if top_prob >= 0.60:
                    eq_level, eq_color = '高い', '#dc2626'
                elif top_prob >= 0.26:
                    eq_level, eq_color = 'やや高い', '#d97706'
                else:
                    eq_level, eq_color = '低い', '#16a34a'
                eq_level_html = f'''<span style="display:inline-block;background:{eq_color};color:white;
                      padding:0.2rem 0.8rem;border-radius:6px;font-size:0.85rem;font-weight:700">{eq_level}</span>'''

        earthquake_html = f'''
        <div class="tbl-wrap" style="padding:1.2rem;margin-bottom:1.5rem">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.8rem">
            <span style="font-weight:700;font-size:0.95rem;color:#1e293b">
              地震ハザード
              <span style="font-weight:400;font-size:0.75rem;color:#64748b;margin-left:0.5rem">
                J-SHIS 2024年版</span>
            </span>
            {eq_level_html}
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:1.5rem">
            {mesh_html if mesh_html else fault_html}
            {fault_html if mesh_html else ''}
          </div>
          <div style="font-size:0.72rem;color:#94a3b8;margin-top:0.8rem;text-align:right">
            データ: 防災科学技術研究所 J-SHIS / 地震調査研究推進本部 全国地震動予測地図2024年版
          </div>
        </div>'''

    # 3. ToG score + Risk (two columns)
    tog_comp_html = ''
    comp_labels = {'variety': '文書多様性', 'volume': '文書量', 'deliberation': '議事録',
                   'procurement': '調達情報', 'trend': '改善傾向'}
    comp_colors = {'variety': '#3b82f6', 'volume': '#10b981', 'deliberation': '#8b5cf6',
                   'procurement': '#f59e0b', 'trend': '#ef4444'}
    for key, label in comp_labels.items():
        comp = tog_components.get(key, {})
        score = comp.get('score', 0)
        weight = comp.get('weight', 0)
        color = comp_colors.get(key, '#6b7280')
        tog_comp_html += f'''
        <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.5rem">
          <span style="width:80px;font-size:0.8rem;color:#64748b;text-align:right">{label}</span>
          <div style="flex:1;background:#e2e8f0;border-radius:4px;height:20px;overflow:hidden;position:relative">
            <div style="width:{min(100, score):.0f}%;background:{color};height:100%;border-radius:4px"></div>
            <span style="position:absolute;right:6px;top:1px;font-size:0.7rem;font-weight:700;
                  color:{'white' if score > 50 else '#1e293b'}">{fmt_num(score)}</span>
          </div>
          <span style="width:30px;font-size:0.7rem;color:#94a3b8">x{weight}</span>
        </div>'''

    risk_comp_html = ''
    for key, label in rcomp_labels.items():
        comp = risk_components.get(key, {})
        score = comp.get('score', 0)
        color = '#dc2626' if score > 60 else '#d97706' if score > 30 else '#16a34a'
        risk_comp_html += f'''
        <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.5rem">
          <span style="width:100px;font-size:0.8rem;color:#64748b;text-align:right">{label}</span>
          <div style="flex:1;background:#e2e8f0;border-radius:4px;height:20px;overflow:hidden;position:relative">
            <div style="width:{min(100, score):.0f}%;background:{color};height:100%;border-radius:4px"></div>
            <span style="position:absolute;right:6px;top:1px;font-size:0.7rem;font-weight:700;
                  color:{'white' if score > 50 else '#1e293b'}">{fmt_num(score)}</span>
          </div>
        </div>'''

    tog_risk = f'''
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem">
      <div class="tbl-wrap" style="padding:1.2rem">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.8rem">
          <span style="font-weight:700;font-size:0.95rem;color:#1e293b">透明性スコア (ToG)</span>
          <div style="text-align:right">
            <span style="font-size:1.8rem;font-weight:800;color:#2563eb">{fmt_num(tog_score)}</span>
            <span style="font-size:0.75rem;color:#64748b">/100</span>
          </div>
        </div>
        <div style="font-size:0.78rem;color:#64748b;margin-bottom:0.8rem">
          全国 {tog_rank}/{tog_total}位 | 県内 {tog_pref_rank}/{tog_pref_total}位
        </div>
        {tog_comp_html}
      </div>
      <div class="tbl-wrap" style="padding:1.2rem">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.8rem">
          <span style="font-weight:700;font-size:0.95rem;color:#1e293b">調達リスクスコア</span>
          <div style="text-align:right">
            <span style="font-size:1.8rem;font-weight:800;color:{'#dc2626' if risk_score and risk_score > 60 else '#d97706' if risk_score and risk_score > 30 else '#16a34a'}">{fmt_num(risk_score)}</span>
            <span style="font-size:0.75rem;color:#64748b">/100</span>
          </div>
        </div>
        <div style="font-size:0.78rem;color:#64748b;margin-bottom:0.8rem">
          全国 {risk_rank}位 (低いほど健全)
        </div>
        {risk_comp_html}
      </div>
    </div>'''

    # 4. Trend chart
    trend_series = trend.get('series', {})
    trend_years = trend.get('years', [])
    trend_chart_data = {
        'labels': trend_years,
        'total': [trend_series.get('total_artifacts', [None]*len(trend_years))[i]
                  if i < len(trend_series.get('total_artifacts', [])) else None
                  for i in range(len(trend_years))],
        'procurement': [trend_series.get('procurement', [None]*len(trend_years))[i]
                        if i < len(trend_series.get('procurement', [])) else None
                        for i in range(len(trend_years))],
        'deliberation': [trend_series.get('deliberation', [None]*len(trend_years))[i]
                         if i < len(trend_series.get('deliberation', [])) else None
                         for i in range(len(trend_years))],
    }

    # Trend metrics
    metrics = trend.get('metrics', {})
    trend_badges = ''
    metric_labels = {'total_artifacts': '総文書数', 'procurement': '調達', 'deliberation': '議事録',
                     'doc_type_variety': '文書多様性'}
    for mk, ml in metric_labels.items():
        m = metrics.get(mk, {})
        direction = m.get('direction', 'stable')
        p_val = m.get('p_value')
        if direction == 'increasing':
            icon, color = '&#9650;', '#16a34a'
        elif direction == 'decreasing':
            icon, color = '&#9660;', '#dc2626'
        else:
            icon, color = '&#9644;', '#6b7280'
        sig = '*' if p_val and p_val < 0.05 else ''
        trend_badges += f'<span style="display:inline-flex;align-items:center;gap:0.2rem;padding:0.2rem 0.6rem;background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;font-size:0.78rem;margin-right:0.3rem"><span style="color:{color}">{icon}</span> {ml}{sig}</span>'

    trend_html = f'''
    <div class="tbl-wrap" style="padding:1.2rem;margin-bottom:1.5rem">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem">
        <span style="font-weight:700;font-size:0.95rem;color:#1e293b">文書公開トレンド</span>
        <div>{trend_badges}</div>
      </div>
      <canvas id="trendChart" height="200"></canvas>
    </div>'''

    # 5. Vendors
    vendors_list = vendors_data if isinstance(vendors_data, list) else vendors_data.get('vendors', [])
    if not isinstance(vendors_list, list):
        vendors_list = []
    vendor_rows = ''
    for v in vendors_list[:10]:
        vname = v.get('vendor_name', '') if isinstance(v, dict) else str(v)
        mentions = v.get('mention_count', '-') if isinstance(v, dict) else '-'
        muni_cnt = v.get('municipality_count', '-') if isinstance(v, dict) else '-'
        vendor_rows += f'''<tr>
          <td style="font-weight:500">{esc(vname)}</td>
          <td style="text-align:center">{mentions}</td>
          <td style="text-align:center">{muni_cnt}</td>
        </tr>'''

    # 6. Peers
    peers_target = peers.get('target', {})
    peers_list = peers.get('peers', [])
    strengths = peers.get('strengths', [])
    weaknesses = peers.get('weaknesses', [])
    tier_label = peers.get('tier_label', '')

    peer_rows = ''
    for p in peers_list[:5]:
        pname = p.get('name', '')
        ppref = p.get('prefecture', '')
        ptog = p.get('tog_score', '-')
        prisk = p.get('risk_score', '-')
        parts = p.get('artifact_count', '-')
        peer_rows += f'''<tr>
          <td><span style="font-size:0.7rem;color:#64748b">{esc(ppref)}</span> {esc(pname)}</td>
          <td style="text-align:center">{fmt_num(ptog)}</td>
          <td style="text-align:center">{fmt_num(prisk)}</td>
          <td style="text-align:center">{fmt_num(parts)}</td>
        </tr>'''

    str_badges = ''.join(f'<span style="display:inline-block;background:#f0fdf4;color:#16a34a;padding:0.1rem 0.5rem;border-radius:4px;font-size:0.75rem;font-weight:600;margin:0.15rem">{esc(s)}</span>' for s in strengths)
    weak_badges = ''.join(f'<span style="display:inline-block;background:#fef2f2;color:#dc2626;padding:0.1rem 0.5rem;border-radius:4px;font-size:0.75rem;font-weight:600;margin:0.15rem">{esc(w)}</span>' for w in weaknesses)

    vendor_peer_html = f'''
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem">
      <div class="tbl-wrap" style="padding:1.2rem">
        <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.5rem;color:#1e293b">
          主要ベンダー
          <span style="font-weight:400;font-size:0.78rem;color:#64748b;margin-left:0.5rem">
            HHI: {fmt_num(hhi)} ({vendor_count}社)</span>
        </div>
        <table>
          <thead><tr><th>企業名</th><th style="text-align:center;width:60px">件数</th>
            <th style="text-align:center;width:70px">自治体数</th></tr></thead>
          <tbody>{vendor_rows if vendor_rows else '<tr><td colspan="3" style="color:#64748b">データなし</td></tr>'}</tbody>
        </table>
      </div>
      <div class="tbl-wrap" style="padding:1.2rem">
        <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.3rem;color:#1e293b">
          同規模自治体比較
          <span style="font-weight:400;font-size:0.78rem;color:#64748b;margin-left:0.5rem">{esc(tier_label)}</span>
        </div>
        {f'<div style="margin-bottom:0.4rem"><span style="font-size:0.78rem;color:#64748b">強み:</span> {str_badges}</div>' if str_badges else ''}
        {f'<div style="margin-bottom:0.6rem"><span style="font-size:0.78rem;color:#64748b">弱み:</span> {weak_badges}</div>' if weak_badges else ''}
        <table>
          <thead><tr><th>自治体</th><th style="text-align:center;width:50px">ToG</th>
            <th style="text-align:center;width:50px">Risk</th>
            <th style="text-align:center;width:60px">文書数</th></tr></thead>
          <tbody>
            <tr style="background:#eff6ff;font-weight:600">
              <td>{esc(name)} (対象)</td>
              <td style="text-align:center">{fmt_num(peers_target.get('tog_score'))}</td>
              <td style="text-align:center">{fmt_num(peers_target.get('risk_score'))}</td>
              <td style="text-align:center">{fmt_num(peers_target.get('artifact_count'))}</td>
            </tr>
            {peer_rows}
          </tbody>
        </table>
      </div>
    </div>'''

    # 7. Documents + Topics
    doc_rows = ''
    doc_list = docs.get('documents', []) if isinstance(docs, dict) else []
    doc_type_colors = {
        'procurement': '#dc2626', 'deliberation': '#8b5cf6', 'budget': '#0891b2',
        'plan': '#16a34a', 'news': '#d97706', 'award': '#be185d', 'other': '#6b7280',
    }
    doc_type_labels = {
        'procurement': '調達', 'deliberation': '議事録', 'budget': '予算',
        'plan': '計画', 'news': 'お知らせ', 'award': '落札', 'other': 'その他',
    }
    for doc in doc_list[:12]:
        dt = doc.get('doc_type', 'other')
        dtl = doc_type_labels.get(dt, dt)
        dtc = doc_type_colors.get(dt, '#6b7280')
        durl = doc.get('url', '')
        dtitle = doc.get('title', '(無題)')
        dyear = doc.get('year', '')
        doc_rows += f'''<tr>
          <td><span style="display:inline-block;background:{dtc}15;color:{dtc};padding:0.1rem 0.4rem;
              border-radius:3px;font-size:0.7rem;font-weight:600">{dtl}</span></td>
          <td><a href="{esc(durl)}" target="_blank" style="color:#1e293b;font-weight:500">{esc(dtitle[:80])}</a></td>
          <td style="color:#64748b;white-space:nowrap">{dyear}</td>
        </tr>'''

    # Topic counts
    topic_counts = topics.get('topic_counts', {}) if isinstance(topics, dict) else {}
    topic_items = [
        ('tourism', '観光', '#f59e0b'),
        ('culture', '文化', '#8b5cf6'),
        ('inbound', '国際化', '#0891b2'),
        ('sustainability', '持続可能性', '#16a34a'),
    ]
    topic_bars = ''
    max_topic = max((topic_counts.get(k, 0) for k, _, _ in topic_items), default=1) or 1
    for tkey, tlabel, tcolor in topic_items:
        tval = topic_counts.get(tkey, 0)
        tpct = tval / max_topic * 100
        topic_bars += f'''
        <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.4rem">
          <span style="width:60px;font-size:0.8rem;text-align:right;color:#64748b">{tlabel}</span>
          <div style="flex:1;background:#e2e8f0;border-radius:4px;height:20px;overflow:hidden">
            <div style="width:{tpct:.0f}%;background:{tcolor};height:100%;border-radius:4px;
                        min-width:{4 if tval else 0}px"></div>
          </div>
          <span style="width:40px;font-size:0.8rem;font-weight:600">{tval}</span>
        </div>'''

    docs_topics = f'''
    <div style="display:grid;grid-template-columns:2fr 1fr;gap:1rem;margin-bottom:1.5rem">
      <div class="tbl-wrap" style="padding:1.2rem">
        <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.5rem;color:#1e293b">
          最近の公開文書 <span style="font-weight:400;font-size:0.78rem;color:#64748b">({docs.get('total_hits', 0)}件)</span>
        </div>
        <table>
          <thead><tr><th style="width:60px">種別</th><th>タイトル</th><th style="width:50px">年度</th></tr></thead>
          <tbody>{doc_rows if doc_rows else '<tr><td colspan="3" style="color:#64748b">データなし</td></tr>'}</tbody>
        </table>
      </div>
      <div class="tbl-wrap" style="padding:1.2rem">
        <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.8rem;color:#1e293b">政策トピック分析</div>
        {topic_bars if topic_bars else '<p style="color:#64748b">データなし</p>'}
      </div>
    </div>'''

    # 8. National bids in this prefecture
    bid_rows = ''
    for b in bids[:8]:
        if not isinstance(b, dict):
            continue
        btitle = b.get('ProjectName', '')
        borg = b.get('OrganizationName', '')
        bdate = (b.get('CftIssueDate', '') or '')[:10]
        burl = b.get('ExternalDocumentURI', '')
        bcat = b.get('Category', '')
        bid_rows += f'''<tr>
          <td style="white-space:nowrap">{esc(bdate)}</td>
          <td style="font-size:0.8rem;color:#64748b">{esc(borg[:30])}</td>
          <td><a href="{esc(burl)}" target="_blank" style="color:#1e293b;font-weight:500">{esc(btitle[:70])}</a></td>
          <td><span style="font-size:0.75rem;color:#64748b">{esc(bcat)}</span></td>
        </tr>'''

    bids_html = f'''
    <div class="tbl-wrap" style="padding:1.2rem;margin-bottom:1.5rem">
      <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.5rem;color:#1e293b">
        国の入札公告 ({esc(prefecture)}関連)
      </div>
      <table>
        <thead><tr><th style="width:90px">公告日</th><th style="width:150px">発注者</th><th>案件名</th><th style="width:50px">区分</th></tr></thead>
        <tbody>{bid_rows if bid_rows else '<tr><td colspan="4" style="color:#64748b">該当データなし</td></tr>'}</tbody>
      </table>
    </div>'''

    # Chart.js script
    chart_script = f'''
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
    <script>
    document.addEventListener('DOMContentLoaded', function() {{
      // Trend line chart
      const ctx = document.getElementById('trendChart');
      if (ctx) {{
        const data = {json.dumps(trend_chart_data)};
        new Chart(ctx, {{
          type: 'line',
          data: {{
            labels: data.labels,
            datasets: [
              {{ label: '総文書数', data: data.total, borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,0.1)',
                 fill: true, tension: 0.3, pointRadius: 4 }},
              {{ label: '調達', data: data.procurement, borderColor: '#ef4444', backgroundColor: 'transparent',
                 tension: 0.3, pointRadius: 3, borderDash: [5, 3] }},
              {{ label: '議事録', data: data.deliberation, borderColor: '#8b5cf6', backgroundColor: 'transparent',
                 tension: 0.3, pointRadius: 3, borderDash: [5, 3] }},
            ]
          }},
          options: {{
            responsive: true,
            plugins: {{ legend: {{ position: 'top', labels: {{ boxWidth: 12, font: {{ size: 11 }} }} }} }},
            scales: {{
              y: {{ beginAtZero: true, grid: {{ color: '#f1f5f9' }} }},
              x: {{ grid: {{ display: false }} }}
            }}
          }}
        }});
      }}

      // Age structure donut chart
      const ageCtx = document.getElementById('ageChart');
      if (ageCtx) {{
        const ageData = {json.dumps(age_data)};
        new Chart(ageCtx, {{
          type: 'doughnut',
          data: {{
            labels: ageData.labels,
            datasets: [{{
              data: ageData.values,
              backgroundColor: ageData.colors,
              borderWidth: 2,
              borderColor: '#fff',
            }}]
          }},
          options: {{
            responsive: true,
            cutout: '55%',
            plugins: {{
              legend: {{ position: 'right', labels: {{ boxWidth: 10, font: {{ size: 10 }}, padding: 8,
                generateLabels: function(chart) {{
                  const data = chart.data;
                  const total = data.datasets[0].data.reduce((a, b) => a + b, 0);
                  return data.labels.map(function(label, i) {{
                    const val = data.datasets[0].data[i];
                    const pct = total > 0 ? (val / total * 100).toFixed(1) : 0;
                    return {{
                      text: label + ' ' + pct + '%',
                      fillStyle: data.datasets[0].backgroundColor[i],
                      index: i
                    }};
                  }});
                }}
              }} }}
            }}
          }}
        }});
      }}
    }});
    </script>'''

    # ── Tab 5: News ──
    news_list = local_data.get('news', [])
    news_cards = ''
    news_cat_colors = {
        'procurement': '#dc2626', 'budget': '#0891b2', 'policy': '#16a34a',
        'personnel': '#8b5cf6', 'disaster': '#f59e0b', 'construction': '#d97706',
        'other': '#6b7280',
    }
    news_cat_labels = {
        'procurement': '調達', 'budget': '予算', 'policy': '政策',
        'personnel': '人事', 'disaster': '災害', 'construction': '建設',
        'other': 'その他',
    }
    for n in news_list[:20]:
        ncat = n.get('category', 'other')
        ncolor = news_cat_colors.get(ncat, '#6b7280')
        nlabel = news_cat_labels.get(ncat, 'その他')
        npub = (n.get('published_at') or '')[:10]
        nsource = n.get('source', '')
        nurl = n.get('url', '')
        ntitle = n.get('title', '(無題)')
        nsummary = n.get('summary', '')[:120]
        news_cards += f'''
        <div style="padding:0.7rem 0;border-bottom:1px solid #f1f5f9">
          <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.3rem">
            <span style="display:inline-block;background:{ncolor}15;color:{ncolor};padding:0.1rem 0.4rem;
                  border-radius:3px;font-size:0.7rem;font-weight:600">{nlabel}</span>
            <span style="font-size:0.75rem;color:#94a3b8">{esc(npub)}</span>
            <span style="font-size:0.75rem;color:#94a3b8">{esc(nsource)}</span>
          </div>
          <a href="{esc(nurl)}" target="_blank" style="font-weight:600;font-size:0.9rem;color:#1e293b;
             text-decoration:none">{esc(ntitle)}</a>
          <div style="font-size:0.8rem;color:#64748b;margin-top:0.2rem">{esc(nsummary)}</div>
        </div>'''
    news_html = f'''
    <div class="tbl-wrap" style="padding:1.2rem">
      <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.8rem">
        ニュース ({len(news_list)}件)
      </div>
      {news_cards if news_cards else '<div style="text-align:center;padding:2rem;color:#94a3b8">ニュース記事がまだありません。<br>6時間ごとにバッチ取得されます。</div>'}
    </div>'''

    # ── Tab 3: Local procurement items ──
    local_items = local_data.get('items', [])
    local_item_count = local_data.get('item_count', 0)
    local_items_html = ''
    for li in local_items[:15]:
        ltitle = li.get('title', '(無題)')
        lurl = li.get('url', '')
        ldate = (li.get('detected_at') or '')[:10]
        lamount = li.get('amount', '')
        ldept = li.get('department', '')
        lmethod = li.get('method', '')
        local_items_html += f'''<tr>
          <td style="white-space:nowrap">{esc(ldate)}</td>
          <td><a href="{esc(lurl)}" target="_blank" style="color:#1e293b;font-weight:500">{esc(ltitle[:70])}</a></td>
          <td style="font-size:0.8rem;color:#64748b">{esc(ldept)}</td>
          <td style="white-space:nowrap">{esc(lamount) or '-'}</td>
        </tr>'''

    procurement_tab_items = f'''
    <div class="tbl-wrap" style="padding:1.2rem;margin-bottom:1rem">
      <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.5rem">
        検知済み案件 ({local_item_count}件)
      </div>
      <table>
        <thead><tr><th style="width:90px">検出日</th><th>案件名</th><th style="width:80px">部署</th><th style="width:80px">金額</th></tr></thead>
        <tbody>{local_items_html if local_items_html else '<tr><td colspan="4" style="color:#64748b">案件データなし</td></tr>'}</tbody>
      </table>
    </div>'''

    # ── Assemble 6 Tabs ──
    fetch_time = data.get('fetch_time', 0)

    TAB_CSS = '''
    <style>
    .tab-nav { display:flex; gap:0; border-bottom:2px solid #e2e8f0; margin-bottom:1.2rem; }
    .tab-nav a { padding:0.6rem 1.2rem; font-size:0.9rem; font-weight:600; color:#64748b;
      text-decoration:none; border-bottom:2px solid transparent; margin-bottom:-2px; transition:all 0.15s; }
    .tab-nav a:hover { color:#1e40af; }
    .tab-nav a.active { color:#1e40af; border-bottom-color:#1e40af; }
    .tab-pane { display:none; }
    .tab-pane.active { display:block; }
    </style>'''

    TAB_JS = '''
    <script>
    document.querySelectorAll('.tab-nav a').forEach(function(tab) {
      tab.addEventListener('click', function(e) {
        e.preventDefault();
        document.querySelectorAll('.tab-nav a').forEach(function(t) { t.classList.remove('active'); });
        document.querySelectorAll('.tab-pane').forEach(function(p) { p.classList.remove('active'); });
        e.target.classList.add('active');
        var target = e.target.getAttribute('data-tab');
        document.getElementById('tab-' + target).classList.add('active');
      });
    });
    </script>'''

    return f'''
    {TAB_CSS}
    {hero}

    <div class="tab-nav">
      <a href="#" data-tab="basic" class="active">基本情報</a>
      <a href="#" data-tab="issues">課題分析</a>
      <a href="#" data-tab="procurement">調達履歴</a>
      <a href="#" data-tab="council">議会・政策</a>
      <a href="#" data-tab="news">ニュース</a>
      <a href="#" data-tab="strategy">営業戦略</a>
    </div>

    <div id="tab-basic" class="tab-pane active">
      {demo_fiscal}
      {earthquake_html}
      {trend_html}
    </div>

    <div id="tab-issues" class="tab-pane">
      {tog_risk}
      {vendor_peer_html}
    </div>

    <div id="tab-procurement" class="tab-pane">
      {procurement_tab_items}
      {bids_html}
    </div>

    <div id="tab-council" class="tab-pane">
      {policy_html}
      {plans_html}
      {docs_topics}
    </div>

    <div id="tab-news" class="tab-pane">
      {news_html}
    </div>

    <div id="tab-strategy" class="tab-pane">
      {brief_challenges_html}
    </div>

    <div style="text-align:center;padding:1rem;color:#94a3b8;font-size:0.75rem">
      MCP fetch: {fetch_time:.1f}s | Render: {timing*1000:.0f}ms
      | Sources: municipality-mcp, government-procurement-mcp, J-SHIS
    </div>

    {chart_script}
    {TAB_JS}
    '''
