#!/usr/bin/env python3
"""Night 2: 8-case E2E test across 4 project types (2 each)."""
import json
import time
import urllib.request

BASE = "http://localhost:8007"

CASES = [
    # (item_id, expected_type, label)
    (150813, "service_plan",     "A1: 能美市地域福祉計画策定"),
    (1668,   "service_plan",     "A2: 苫小牧市男女平等参画基本計画策定"),
    (1657,   "service_research", "B1: 鹿児島市アンケート調査"),
    (150952, "service_research", "B2: 白神山地ブナ林変動調査"),
    (13858,  "service_general",  "C1: 千葉公園維持管理"),
    (2516,   "service_general",  "C2: 秋田県清掃業務委託"),
    (150524, "goods_standard",   "D1: 秩父宮ラグビー場キャラクタージェネレーター"),
    (2838,   "goods_standard",   "D2: 田村市新病院備品購入"),
]

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

results = []

for item_id, expected_type, label in CASES:
    print(f"\n{'='*60}")
    print(f"CASE: {label} (item_id={item_id}, expect={expected_type})")
    print('='*60)
    r = {}
    r["label"] = label
    r["item_id"] = item_id
    r["expected_type"] = expected_type

    # Step 1: Create project
    status, data = api("POST", "/api/v4/bid-projects", {"item_id": item_id})
    if status != 200 or "project_id" not in data:
        print(f"  SKIP: create failed (status={status})")
        r["error"] = f"create failed: {data}"
        results.append(r)
        continue
    pid = data["project_id"]
    r["project_id"] = pid
    r["actual_type"] = data.get("project_type", "?")
    r["type_match"] = r["actual_type"] == expected_type
    print(f"  Created PJ#{pid} type={r['actual_type']} {'OK' if r['type_match'] else 'MISMATCH'}")

    # Step 2: Generate questions
    status, data = api("POST", f"/api/v4/questions/generate/{pid}")
    if status == 200:
        r["question_count"] = data.get("count", 0)
        print(f"  Questions: {r['question_count']}")
    else:
        r["question_count"] = 0
        print(f"  Questions: FAILED ({status})")

    # Step 3: Check questions detail
    status, q_data = api("GET", f"/api/v4/questions/{pid}")
    if status == 200 and isinstance(q_data, list):
        r["questions"] = q_data
        ig_scores = [q.get("info_gain_score", 0) or 0 for q in q_data]
        r["avg_ig"] = round(sum(ig_scores) / max(len(ig_scores), 1), 3)
        r["high_ig_count"] = sum(1 for s in ig_scores if s >= 0.7)
        cats = [q.get("question_category", "?") for q in q_data]
        r["category_dist"] = {c: cats.count(c) for c in set(cats)}
        print(f"  IG avg={r['avg_ig']:.3f}, high(>=0.7)={r['high_ig_count']}, cats={r['category_dist']}")
    else:
        r["questions"] = []
        r["avg_ig"] = 0
        r["high_ig_count"] = 0

    # Step 4: Generate proposal
    status, data = api("POST", f"/api/v4/proposals/generate/{pid}")
    if status == 200:
        r["proposal_len"] = len(data.get("content_markdown", ""))
        r["proposal_vars"] = 0
        r["proposal_unresolved"] = 0
        vmap = {}
        try:
            vmap = json.loads(data.get("variable_map_json") or "{}")
            r["proposal_vars"] = len(vmap)
            r["proposal_unresolved"] = sum(1 for v in vmap.values() if isinstance(v, dict) and v.get("source") == "placeholder")
        except:
            pass
        r["variable_map"] = vmap
        # Check applied patterns
        ap = []
        try:
            ap = json.loads(data.get("applied_patterns_json") or "[]")
        except:
            pass
        r["applied_patterns_count"] = len(ap)
        print(f"  Proposal: {r['proposal_len']} chars, {r['proposal_vars']} vars, {r['proposal_unresolved']} unresolved, {r['applied_patterns_count']} patterns applied")

        # Check if practicality score was auto-calculated
        r["practicality_score"] = data.get("practicality_score")
        r["practicality_breakdown"] = None
        if data.get("practicality_breakdown_json"):
            try:
                r["practicality_breakdown"] = json.loads(data["practicality_breakdown_json"])
            except:
                pass
        print(f"  Practicality: {r['practicality_score']}")
    else:
        r["proposal_len"] = 0
        r["proposal_vars"] = 0
        r["proposal_unresolved"] = 0
        r["practicality_score"] = None
        print(f"  Proposal: FAILED ({status})")

    # Step 5: Score proposal manually if not auto-scored
    if r.get("practicality_score") is None:
        drafts_s, drafts_d = api("GET", f"/api/v4/proposals/{pid}")
        if drafts_s == 200 and isinstance(drafts_d, list) and drafts_d:
            did = drafts_d[0].get("draft_id")
            if did:
                sc_s, sc_d = api("POST", f"/api/v4/proposals/{did}/score")
                if sc_s == 200:
                    r["practicality_score"] = sc_d.get("score")
                    r["practicality_breakdown"] = sc_d.get("breakdown")
                    print(f"  Manual score: {r['practicality_score']}")

    # Step 6: Generate compliance matrix
    status, data = api("POST", f"/api/v4/artifacts/generate-compliance/{pid}")
    if status == 200:
        r["compliance_matrix"] = True
        cm_content = data.get("content", "")
        r["compliance_items"] = cm_content.count("| ") - 2 if cm_content else 0
        print(f"  Compliance matrix: generated ({r['compliance_items']} items)")
    else:
        r["compliance_matrix"] = False
        print(f"  Compliance matrix: FAILED ({status})")

    # Step 7: Generate submission notes
    status, data = api("POST", f"/api/v4/artifacts/generate-submission-notes/{pid}")
    if status == 200:
        r["submission_notes"] = True
        sn_content = data.get("content", "")
        r["submission_notes_len"] = len(sn_content)
        print(f"  Submission notes: generated ({r['submission_notes_len']} chars)")
    else:
        r["submission_notes"] = False
        print(f"  Submission notes: FAILED ({status})")

    # Step 8: Check question effectiveness (after proposal)
    q_status, q_data2 = api("GET", f"/api/v4/questions/{pid}")
    if q_status == 200 and isinstance(q_data2, list):
        used = sum(1 for q in q_data2 if q.get("was_used_in_proposal"))
        has_usefulness = sum(1 for q in q_data2 if q.get("usefulness_score") is not None)
        r["questions_used_in_proposal"] = used
        r["questions_with_usefulness"] = has_usefulness
        print(f"  Q effectiveness: {used} used in proposal, {has_usefulness} with usefulness score")

    # Step 9: Check workbench page loads
    try:
        req = urllib.request.Request(f"{BASE}/bid-workbench/{pid}")
        with urllib.request.urlopen(req, timeout=15) as resp:
            r["workbench_status"] = resp.status
    except:
        r["workbench_status"] = 0
    print(f"  Workbench page: {r['workbench_status']}")

    results.append(r)
    time.sleep(0.5)

# ── Summary ──
print(f"\n\n{'='*70}")
print("NIGHT 2 E2E TEST SUMMARY")
print('='*70)

type_match = sum(1 for r in results if r.get("type_match"))
total = len(results)
print(f"\nType classification: {type_match}/{total}")

for r in results:
    tm = "OK" if r.get("type_match") else "MISS"
    qs = r.get("question_count", 0)
    pl = r.get("proposal_len", 0)
    pv = r.get("proposal_vars", 0)
    ur = r.get("proposal_unresolved", 0)
    ps = r.get("practicality_score", "?")
    cm = "Y" if r.get("compliance_matrix") else "N"
    sn = "Y" if r.get("submission_notes") else "N"
    wb = r.get("workbench_status", 0)
    print(f"  {r['label']:45s} type={tm} Q={qs} P={pl}c/{pv}v/{ur}u score={ps} CM={cm} SN={sn} WB={wb}")

# Save full results
with open("/app/output/night2_e2e_results.json", "w") as f:
    # Clean non-serializable items
    clean = []
    for r in results:
        c = {k: v for k, v in r.items() if k != "questions"}
        clean.append(c)
    json.dump(clean, f, ensure_ascii=False, indent=2)
print(f"\nFull results saved to /app/output/night2_e2e_results.json")
