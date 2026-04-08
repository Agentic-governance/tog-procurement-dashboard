#!/usr/bin/env python3
"""Add /api/quality endpoint to dashboard.py"""

with open("/app/dashboard.py", "r") as f:
    code = f.read()

ENDPOINT = '''

@app.get("/api/quality")
async def api_quality():
    """Quality metrics for the intelligence system."""
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
    with_dl = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE deadline IS NOT NULL AND deadline != ''").fetchone()[0]
    with_amt = conn.execute("SELECT COUNT(*) FROM procurement_items WHERE amount IS NOT NULL AND amount != ''").fetchone()[0]
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

'''

# Insert before /api/export
if "/api/quality" not in code:
    code = code.replace('@app.get("/api/export")', ENDPOINT + '\n@app.get("/api/export")')
    with open("/app/dashboard.py", "w") as f:
        f.write(code)
    print("Quality endpoint added", flush=True)
else:
    print("Already exists", flush=True)
