

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
