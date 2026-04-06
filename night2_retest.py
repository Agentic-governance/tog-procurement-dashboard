#!/usr/bin/env python3
"""Night 2: Re-test A1 classification + learning loop test."""
import json
import time
import urllib.request

BASE = "http://localhost:8007"

def api(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"} if data else {})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())
    except Exception as e:
        return 0, {"error": str(e)}

# ── Re-test A1 classification ──
print("=== Re-test A1: 能美市地域福祉計画策定 ===")
# Delete old PJ#13 first
api("DELETE", "/api/v4/bid-projects/13")
time.sleep(0.5)

status, data = api("POST", "/api/v4/bid-projects", {"item_id": 150813})
if status == 200:
    pid = data["project_id"]
    ptype = data.get("project_type", "?")
    print(f"  Created PJ#{pid} type={ptype} {'OK' if ptype=='service_plan' else 'MISMATCH'}")

    # Full workflow
    api("POST", f"/api/v4/questions/generate/{pid}")
    _, qs = api("GET", f"/api/v4/questions/{pid}")
    print(f"  Questions: {len(qs) if isinstance(qs, list) else 0}")

    _, prop = api("POST", f"/api/v4/proposals/generate/{pid}")
    if isinstance(prop, dict):
        print(f"  Proposal: {len(prop.get('content_markdown',''))} chars, score={prop.get('practicality_score')}")
        print(f"  Vars: {len(json.loads(prop.get('variable_map_json','{}')))} vars")
else:
    print(f"  FAILED: {data}")

# ── Learning loop test ──
print("\n=== Learning Loop Test ===")

# 1. Submit feedback for PJ#14 (A2: 苫小牧市男女平等参画基本計画策定) = won
print("\n1. Submit feedback: PJ#14 WON")
s, d = api("POST", "/api/v4/feedback", {
    "project_id": 14,
    "outcome": "won",
    "actual_amount": "5,500,000",
    "winning_vendor": "テスト社",
    "feedback_text": "提案の具体性が高く評価された。特にスケジュールの詳細度と住民参加の手法が決め手。",
    "lessons_learned": "計画策定案件ではスケジュールの詳細度と住民参加手法の具体性が重要。事前の自治体条例調査も評価される。"
})
print(f"  Status: {s}, patterns: {d.get('patterns_created',0) if isinstance(d,dict) else '?'}")

# 2. Submit feedback for PJ#15 (B1: 鹿児島市アンケート調査) = lost
print("\n2. Submit feedback: PJ#15 LOST")
s, d = api("POST", "/api/v4/feedback", {
    "project_id": 15,
    "outcome": "lost",
    "actual_amount": "3,200,000",
    "winning_vendor": "競合社A",
    "feedback_text": "価格面で及ばず。技術点は僅差だったが提案の分析手法の新規性が不足と評価された。",
    "lessons_learned": "調査案件では分析手法の新規性と独自性を打ち出すべき。価格は8割以内に収める。"
})
print(f"  Status: {s}, patterns: {d.get('patterns_created',0) if isinstance(d,dict) else '?'}")

# 3. Check patterns learned
print("\n3. Verify patterns")
s, d = api("GET", "/api/v4/learning/patterns/NATIONAL")
if s == 200 and isinstance(d, list):
    print(f"  Total patterns: {len(d)}")
    for p in d[:8]:
        print(f"    [{p.get('pattern_type','')}] {p.get('pattern_key','')}: {str(p.get('pattern_value',''))[:60]}  conf={p.get('confidence','?')}")

# 4. Re-generate proposal for a service_plan case to see if patterns applied
print("\n4. Re-generate proposal for PJ#14 (should apply learned patterns)")
# First, re-generate with new patterns
s, d = api("POST", f"/api/v4/proposals/generate/14")
if s == 200:
    ap = json.loads(d.get("applied_patterns_json", "[]"))
    print(f"  Proposal v2: {len(d.get('content_markdown',''))} chars")
    print(f"  Applied patterns: {len(ap)}")
    for p in ap:
        print(f"    [{p.get('type','')}] {p.get('key','')}: {str(p.get('value',''))[:50]}")
    print(f"  Score: {d.get('practicality_score')}")

# 5. Check pattern weighting
print("\n5. Weighted patterns for NATIONAL/service_plan")
s, d = api("GET", "/api/v4/learning/patterns/NATIONAL")
if s == 200 and isinstance(d, list):
    for p in d:
        print(f"  [{p.get('scope_type','?')}] {p.get('pattern_type','')}:{p.get('pattern_key','')} w={p.get('decay_score',1.0)} usage={p.get('usage_count',0)} success={p.get('success_count',0)}")

# 6. Check learning summary
print("\n6. Learning summary")
s, d = api("GET", "/api/v4/learning/summary")
if s == 200 and isinstance(d, dict):
    print(f"  Total feedback: {d.get('total_feedback',0)}")
    print(f"  Win rate: {d.get('win_rate','?')}")
    print(f"  Total patterns: {d.get('total_patterns',0)}")

# 7. Pattern decay test
print("\n7. Pattern decay")
s, d = api("POST", "/api/v4/feedback", {
    "project_id": 17,
    "outcome": "lost",
    "actual_amount": "2,000,000",
    "winning_vendor": "地元業者",
    "feedback_text": "地域限定案件で地元業者が有利だった。",
    "lessons_learned": "維持管理案件では地域性が重視される。"
})
print(f"  Feedback PJ#17: status={s}")

# Check total
s, d = api("GET", "/api/v4/learning/summary")
print(f"  Total feedback: {d.get('total_feedback',0)}, patterns: {d.get('total_patterns',0)}")

print("\n=== DONE ===")
