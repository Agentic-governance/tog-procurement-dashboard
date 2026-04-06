#!/usr/bin/env python3
"""
Procurement Intelligence Agent Server — 3 specialized AI agents.

Agents:
  1. Search Agent — Find procurement items, municipalities, vendors
  2. Analysis Agent — Deep analysis of municipalities, risk, trends
  3. Document Agent — Generate briefs, reports, comparisons

Each agent uses OpenAI GPT API + MCP tools + local DB for comprehensive answers.
"""

import asyncio
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse

# ── Config ────────────────────────────────────────────────────────────────

OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
MUNI_MCP_URL = os.environ.get('MUNI_MCP_URL', 'http://78.46.57.151:8004/mcp/')
GOVT_MCP_URL = os.environ.get('GOVT_MCP_URL', 'http://78.46.57.151:8003/mcp/')
DB_PATH = os.environ.get('AGENT_DB_PATH', '/app/data/monitor.db')
GPT_MODEL = 'gpt-4o-mini'
MAX_TOKENS = 4096

app = FastAPI(title="Procurement Agent Server", version="1.0")


# ── MCP Client ────────────────────────────────────────────────────────────

async def mcp_call(tool_name: str, arguments: dict, base_url: str = None) -> dict:
    """Call an MCP tool and return parsed result."""
    if base_url is None:
        base_url = MUNI_MCP_URL

    headers = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient() as client:
        # Initialize session
        init_body = {
            "jsonrpc": "2.0", "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "agent-server", "version": "1.0"}
            }
        }
        async with client.stream("POST", base_url, json=init_body,
                                  headers=headers, timeout=10) as resp:
            sid = resp.headers.get("mcp-session-id", "")
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    break

        # Call tool
        if sid:
            headers["Mcp-Session-Id"] = sid
        body = {
            "jsonrpc": "2.0", "id": 2,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments}
        }
        async with client.stream("POST", base_url, json=body,
                                  headers=headers, timeout=30) as resp:
            async for line in resp.aiter_lines():
                if line.startswith("data: "):
                    msg = json.loads(line[6:])
                    if "result" in msg:
                        content = msg["result"].get("content", [])
                        for c in content:
                            if c.get("type") == "text":
                                try:
                                    return json.loads(c["text"])
                                except json.JSONDecodeError:
                                    return {"text": c["text"]}
                        return msg.get("result", {})
    return {"error": "No result"}


# ── Local DB Queries ──────────────────────────────────────────────────────

def db_query(sql: str, params: tuple = ()) -> list:
    """Run a read-only SQL query on monitor.db."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        result = [dict(r) for r in rows]
        conn.close()
        return result
    except Exception as e:
        return [{"error": str(e)}]


# ── Tool Definitions ──────────────────────────────────────────────────────

def _fn_tool(name: str, description: str, parameters: dict) -> dict:
    """Helper to create OpenAI function-calling tool definition."""
    return {
        "type": "function",
        "function": {"name": name, "description": description, "parameters": parameters},
    }

SEARCH_TOOLS = [
    _fn_tool("search_items",
             "Search procurement items by keyword, category, prefecture, date range. Returns matching items from the local database.",
             {"type": "object", "properties": {
                 "query": {"type": "string", "description": "Search keyword (title/department)"},
                 "category": {"type": "string", "enum": ["construction", "service", "goods", "consulting", "it", "other"]},
                 "pref_code": {"type": "string", "description": "Prefecture code (2 digits, e.g. '13' for Tokyo)"},
                 "days": {"type": "integer", "description": "Look back N days", "default": 30},
                 "limit": {"type": "integer", "default": 20},
             }}),
    _fn_tool("search_municipalities",
             "Search municipalities by name or keyword using MCP muni_search tool.",
             {"type": "object", "properties": {
                 "query": {"type": "string", "description": "Municipality name or keyword"},
             }, "required": ["query"]}),
    _fn_tool("search_bids",
             "Search national bid announcements using government procurement MCP.",
             {"type": "object", "properties": {
                 "query": {"type": "string", "description": "Search keyword"},
                 "count": {"type": "integer", "default": 20},
             }, "required": ["query"]}),
    _fn_tool("get_item_stats",
             "Get aggregate statistics: category breakdown, monthly trends, top municipalities.",
             {"type": "object", "properties": {
                 "metric": {"type": "string", "enum": ["categories", "monthly", "top_munis", "top_depts"]},
                 "days": {"type": "integer", "default": 90},
             }, "required": ["metric"]}),
]

ANALYSIS_TOOLS = [
    _fn_tool("analyze_municipality",
             "Get comprehensive municipality profile including demographics, fiscal health, transparency score, and ToG analysis.",
             {"type": "object", "properties": {
                 "muni_code": {"type": "string", "description": "6-digit municipality code"},
             }, "required": ["muni_code"]}),
    _fn_tool("analyze_risk",
             "Assess procurement risk for a municipality.",
             {"type": "object", "properties": {
                 "muni_code": {"type": "string", "description": "6-digit municipality code"},
             }, "required": ["muni_code"]}),
    _fn_tool("analyze_peers",
             "Compare a municipality with similar peers.",
             {"type": "object", "properties": {
                 "muni_code": {"type": "string", "description": "6-digit municipality code"},
                 "n_peers": {"type": "integer", "default": 5},
             }, "required": ["muni_code"]}),
    _fn_tool("analyze_trend",
             "Get year-over-year trend data for a municipality.",
             {"type": "object", "properties": {
                 "muni_code": {"type": "string", "description": "6-digit municipality code"},
             }, "required": ["muni_code"]}),
    _fn_tool("detect_policy_shift",
             "Detect policy keyword adoption trends (DX, decarbonization, etc.) for municipalities.",
             {"type": "object", "properties": {
                 "keywords": {"type": "array", "items": {"type": "string"}},
                 "muni_codes": {"type": "array", "items": {"type": "string"}},
             }, "required": ["keywords"]}),
    _fn_tool("get_news",
             "Get recent news articles for a municipality or prefecture.",
             {"type": "object", "properties": {
                 "muni_code": {"type": "string"},
                 "limit": {"type": "integer", "default": 10},
             }, "required": ["muni_code"]}),
]

DOCUMENT_TOOLS = [
    _fn_tool("generate_brief",
             "Generate a sales brief for a municipality using MCP muni_generate_brief.",
             {"type": "object", "properties": {
                 "muni_code": {"type": "string", "description": "6-digit municipality code"},
             }, "required": ["muni_code"]}),
    _fn_tool("get_documents",
             "Get municipal documents (deliberation, plans, etc.) from govcrawl data.",
             {"type": "object", "properties": {
                 "muni_code": {"type": "string"},
                 "doc_type": {"type": "string", "description": "Optional: deliberation, plan, budget, procurement"},
                 "limit": {"type": "integer", "default": 15},
             }, "required": ["muni_code"]}),
    _fn_tool("get_vendors",
             "Get vendor information for a municipality.",
             {"type": "object", "properties": {
                 "muni_code": {"type": "string"},
             }, "required": ["muni_code"]}),
    _fn_tool("compare_municipalities",
             "Compare two or more municipalities side by side.",
             {"type": "object", "properties": {
                 "muni_codes": {"type": "array", "items": {"type": "string"}, "minItems": 2},
             }, "required": ["muni_codes"]}),
]


# ── Tool Execution ────────────────────────────────────────────────────────

async def execute_tool(name: str, args: dict) -> str:
    """Execute a tool and return JSON string result."""
    try:
        if name == "search_items":
            where = ["1=1"]
            params = []
            if args.get("query"):
                where.append("(title LIKE ? OR department LIKE ?)")
                params.extend([f"%{args['query']}%", f"%{args['query']}%"])
            if args.get("category"):
                where.append("category = ?")
                params.append(args["category"])
            if args.get("pref_code"):
                where.append("muni_code LIKE ?")
                params.append(f"{args['pref_code']}%")
            days = args.get("days", 30)
            where.append(f"detected_at > datetime('now', '-{int(days)} days')")
            limit = min(args.get("limit", 20), 50)
            sql = f"SELECT item_id, muni_code, detected_at, title, category, department, deadline, amount FROM procurement_items WHERE {' AND '.join(where)} ORDER BY detected_at DESC LIMIT {limit}"
            rows = db_query(sql, tuple(params))
            return json.dumps({"items": rows, "count": len(rows)}, ensure_ascii=False)

        elif name == "search_municipalities":
            result = await mcp_call("muni_search", {"query": args["query"]})
            return json.dumps(result, ensure_ascii=False)

        elif name == "search_bids":
            result = await mcp_call("govt_search_bids",
                                     {"query": args["query"], "count": args.get("count", 20)},
                                     base_url=GOVT_MCP_URL)
            return json.dumps(result, ensure_ascii=False)

        elif name == "get_item_stats":
            metric = args["metric"]
            days = args.get("days", 90)
            if metric == "categories":
                rows = db_query(f"SELECT COALESCE(category,'other') as cat, COUNT(*) as cnt FROM procurement_items WHERE detected_at > datetime('now', '-{int(days)} days') GROUP BY cat ORDER BY cnt DESC")
                return json.dumps({"categories": rows}, ensure_ascii=False)
            elif metric == "monthly":
                rows = db_query("SELECT substr(detected_at,1,7) as month, COUNT(*) as cnt FROM procurement_items GROUP BY month ORDER BY month DESC LIMIT 24")
                return json.dumps({"monthly": rows}, ensure_ascii=False)
            elif metric == "top_munis":
                rows = db_query(f"SELECT muni_code, COUNT(*) as cnt FROM procurement_items WHERE muni_code != 'NATIONAL' AND detected_at > datetime('now', '-{int(days)} days') GROUP BY muni_code ORDER BY cnt DESC LIMIT 20")
                return json.dumps({"top_municipalities": rows}, ensure_ascii=False)
            elif metric == "top_depts":
                rows = db_query(f"SELECT department, COUNT(*) as cnt FROM procurement_items WHERE department IS NOT NULL AND detected_at > datetime('now', '-{int(days)} days') GROUP BY department ORDER BY cnt DESC LIMIT 20")
                return json.dumps({"top_departments": rows}, ensure_ascii=False)

        elif name == "analyze_municipality":
            result = await mcp_call("muni_profile", {"muni_code": args["muni_code"]})
            return json.dumps(result, ensure_ascii=False)

        elif name == "analyze_risk":
            result = await mcp_call("muni_analyze_risk", {"muni_code": args["muni_code"]})
            return json.dumps(result, ensure_ascii=False)

        elif name == "analyze_peers":
            result = await mcp_call("muni_analyze_peers", {"muni_code": args["muni_code"], "n_peers": args.get("n_peers", 5)})
            return json.dumps(result, ensure_ascii=False)

        elif name == "analyze_trend":
            result = await mcp_call("muni_analyze_trend", {"muni_code": args["muni_code"]})
            return json.dumps(result, ensure_ascii=False)

        elif name == "detect_policy_shift":
            kw = args.get("keywords", ["DX", "脱炭素"])
            mc = args.get("muni_codes", [])
            result = await mcp_call("muni_detect_policy_shift", {"keywords": kw, "muni_codes": mc})
            return json.dumps(result, ensure_ascii=False)

        elif name == "get_news":
            rows = db_query("SELECT title, source, url, published_at, summary, category FROM muni_news WHERE muni_code = ? ORDER BY published_at DESC LIMIT ?",
                            (args["muni_code"], args.get("limit", 10)))
            return json.dumps({"news": rows, "count": len(rows)}, ensure_ascii=False)

        elif name == "generate_brief":
            result = await mcp_call("muni_generate_brief", {"muni_code": args["muni_code"]})
            return json.dumps(result, ensure_ascii=False)

        elif name == "get_documents":
            doc_args = {"muni_code": args["muni_code"], "limit": args.get("limit", 15)}
            if args.get("doc_type"):
                doc_args["doc_type"] = args["doc_type"]
            result = await mcp_call("muni_documents", doc_args)
            return json.dumps(result, ensure_ascii=False)

        elif name == "get_vendors":
            result = await mcp_call("muni_vendors", {"muni_code": args["muni_code"]})
            return json.dumps(result, ensure_ascii=False)

        elif name == "compare_municipalities":
            result = await mcp_call("muni_compare", {"muni_codes": args["muni_codes"]})
            return json.dumps(result, ensure_ascii=False)

        return json.dumps({"error": f"Unknown tool: {name}"})

    except Exception as e:
        return json.dumps({"error": str(e)})


# ── Agent System Prompts ──────────────────────────────────────────────────

AGENT_PROMPTS = {
    "search": """あなたは日本の政府調達・入札情報の検索エージェントです。

利用可能なデータ:
- 144,681件の調達案件（国・都道府県・市区町村）
- 1,742市区町村のプロフィール
- 35,723件のニュース記事
- 期間: 2024年4月〜2026年3月

ユーザーの質問に対して、適切なツールを使って検索し、結果を日本語で分かりやすく回答してください。
金額や件数は具体的な数値で示し、関連する案件があればタイトルと発注者を含めてください。""",

    "analysis": """あなたは日本の自治体分析エージェントです。

利用可能な分析:
- 自治体プロフィール（人口、財政、透明性スコア）
- ToG（Transparency of Government）スコア分析
- リスク評価（ベンダー集中度、透明性不足等）
- ピア比較（類似自治体との比較）
- 政策シフト検出（DX、脱炭素等のキーワード採用率）
- トレンド分析（年次推移）

ユーザーの質問に対して、複数のツールを組み合わせて包括的な分析を提供してください。
数値は具体的に示し、リスクや機会を明確に指摘してください。""",

    "document": """あなたは日本の政府調達に関する文書生成エージェントです。

利用可能な機能:
- 営業ブリーフ生成（自治体ごとの営業戦略）
- 議会・政策文書の検索
- ベンダー分析
- 自治体比較レポート

ユーザーの要望に対して、適切なデータを収集し、実用的な文書を生成してください。
出力はMarkdown形式で、見出し・箇条書き・表を適切に使ってください。""",
}


# ── OpenAI GPT API Call ──────────────────────────────────────────────────

async def call_gpt_with_tools(agent_type: str, user_message: str, tools: list) -> str:
    """Call OpenAI GPT API with function calling, handle tool loop, return final text."""
    if not OPENAI_API_KEY:
        return "Error: OPENAI_API_KEY not set"

    system_prompt = AGENT_PROMPTS.get(agent_type, AGENT_PROMPTS["search"])
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message},
    ]

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    for turn in range(5):  # Max 5 tool-use turns
        body = {
            "model": GPT_MODEL,
            "max_tokens": MAX_TOKENS,
            "messages": messages,
            "tools": tools,
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                json=body, headers=headers, timeout=60
            )
            if resp.status_code != 200:
                return f"API Error: {resp.status_code} {resp.text[:200]}"
            result = resp.json()

        choice = result["choices"][0]
        msg = choice["message"]
        finish_reason = choice.get("finish_reason")

        if finish_reason != "tool_calls":
            return msg.get("content") or "(回答なし)"

        # Handle tool calls
        tool_calls = msg.get("tool_calls", [])
        messages.append(msg)  # Append assistant message with tool_calls

        for tc in tool_calls:
            fn = tc["function"]
            try:
                tool_input = json.loads(fn["arguments"])
            except json.JSONDecodeError:
                tool_input = {}
            tool_result = await execute_tool(fn["name"], tool_input)
            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": tool_result,
            })

    return "(最大ターン数に達しました)"


# ── Streaming Version ─────────────────────────────────────────────────────

async def stream_gpt_with_tools(agent_type: str, user_message: str, tools: list):
    """Stream OpenAI GPT API response with function calling."""
    if not OPENAI_API_KEY:
        yield f"data: {json.dumps({'type': 'error', 'text': 'OPENAI_API_KEY not set'})}\n\n"
        return

    system_prompt = AGENT_PROMPTS.get(agent_type, AGENT_PROMPTS["search"])
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message},
    ]

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    for turn in range(5):
        body = {
            "model": GPT_MODEL,
            "max_tokens": MAX_TOKENS,
            "messages": messages,
            "tools": tools,
            "stream": True,
        }

        # Accumulators for streaming
        current_text = ""
        # tool_calls_acc: {index: {"id": ..., "name": ..., "arguments": ...}}
        tool_calls_acc = {}
        finish_reason = None

        async with httpx.AsyncClient() as client:
            async with client.stream("POST", "https://api.openai.com/v1/chat/completions",
                                      json=body, headers=headers, timeout=60) as resp:
                if resp.status_code != 200:
                    error_body = await resp.aread()
                    yield f"data: {json.dumps({'type': 'error', 'text': f'API Error: {resp.status_code}'})}\n\n"
                    return

                async for line in resp.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    data_str = line[6:]
                    if data_str == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    choice = chunk.get("choices", [{}])[0]
                    delta = choice.get("delta", {})
                    finish_reason = choice.get("finish_reason") or finish_reason

                    # Text content
                    if delta.get("content"):
                        text = delta["content"]
                        current_text += text
                        yield f"data: {json.dumps({'type': 'text', 'text': text})}\n\n"

                    # Tool calls (streamed incrementally)
                    if delta.get("tool_calls"):
                        for tc_delta in delta["tool_calls"]:
                            idx = tc_delta["index"]
                            if idx not in tool_calls_acc:
                                tool_calls_acc[idx] = {
                                    "id": tc_delta.get("id", ""),
                                    "name": tc_delta.get("function", {}).get("name", ""),
                                    "arguments": "",
                                }
                                if tool_calls_acc[idx]["name"]:
                                    yield f"data: {json.dumps({'type': 'tool_start', 'tool': tool_calls_acc[idx]['name']})}\n\n"
                            fn_delta = tc_delta.get("function", {})
                            if fn_delta.get("name"):
                                tool_calls_acc[idx]["name"] = fn_delta["name"]
                            if fn_delta.get("arguments"):
                                tool_calls_acc[idx]["arguments"] += fn_delta["arguments"]

        # If no tool calls, we're done
        if not tool_calls_acc:
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
            return

        # Build assistant message for conversation history
        assistant_msg = {"role": "assistant", "content": current_text or None, "tool_calls": []}
        for idx in sorted(tool_calls_acc.keys()):
            tc = tool_calls_acc[idx]
            assistant_msg["tool_calls"].append({
                "id": tc["id"],
                "type": "function",
                "function": {"name": tc["name"], "arguments": tc["arguments"]},
            })
        messages.append(assistant_msg)

        # Execute tools and add results
        for idx in sorted(tool_calls_acc.keys()):
            tc = tool_calls_acc[idx]
            yield f"data: {json.dumps({'type': 'tool_executing', 'tool': tc['name']})}\n\n"
            try:
                tool_input = json.loads(tc["arguments"])
            except json.JSONDecodeError:
                tool_input = {}
            result = await execute_tool(tc["name"], tool_input)
            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": result,
            })
            yield f"data: {json.dumps({'type': 'tool_done', 'tool': tc['name']})}\n\n"

    yield f"data: {json.dumps({'type': 'done'})}\n\n"


# ── API Routes ────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    has_key = bool(OPENAI_API_KEY)
    try:
        conn = sqlite3.connect(DB_PATH)
        items = conn.execute("SELECT COUNT(*) FROM procurement_items").fetchone()[0]
        conn.close()
        db_ok = True
    except Exception:
        items = 0
        db_ok = False
    return {
        "status": "ok" if has_key and db_ok else "degraded",
        "openai_key": has_key,
        "db": db_ok,
        "items": items,
        "model": GPT_MODEL,
    }


@app.post("/api/agent/{agent_type}")
async def agent_query(agent_type: str, request: Request):
    """Non-streaming agent query. Returns full response."""
    if agent_type not in ("search", "analysis", "document"):
        return JSONResponse({"error": "Invalid agent type"}, status_code=400)

    body = await request.json()
    query = body.get("query", "")
    if not query:
        return JSONResponse({"error": "query required"}, status_code=400)

    tools_map = {
        "search": SEARCH_TOOLS,
        "analysis": ANALYSIS_TOOLS,
        "document": DOCUMENT_TOOLS,
    }
    tools = tools_map[agent_type]

    t0 = time.perf_counter()
    response = await call_gpt_with_tools(agent_type, query, tools)
    elapsed = time.perf_counter() - t0

    return {
        "agent": agent_type,
        "query": query,
        "response": response,
        "elapsed_seconds": round(elapsed, 2),
        "model": GPT_MODEL,
    }


@app.post("/api/agent/{agent_type}/stream")
async def agent_stream(agent_type: str, request: Request):
    """Streaming agent query. Returns SSE stream."""
    if agent_type not in ("search", "analysis", "document"):
        return JSONResponse({"error": "Invalid agent type"}, status_code=400)

    body = await request.json()
    query = body.get("query", "")
    if not query:
        return JSONResponse({"error": "query required"}, status_code=400)

    tools_map = {
        "search": SEARCH_TOOLS,
        "analysis": ANALYSIS_TOOLS,
        "document": DOCUMENT_TOOLS,
    }
    tools = tools_map[agent_type]

    return StreamingResponse(
        stream_gpt_with_tools(agent_type, query, tools),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Chat UI ───────────────────────────────────────────────────────────────

CHAT_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>調達AIエージェント</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f8fafc; color: #1e293b; }
.container { max-width: 900px; margin: 0 auto; padding: 1rem; height: 100vh; display: flex; flex-direction: column; }
header { padding: 0.8rem 0; border-bottom: 1px solid #e2e8f0; margin-bottom: 1rem; }
header h1 { font-size: 1.2rem; font-weight: 700; }
header p { font-size: 0.8rem; color: #64748b; }
.agent-tabs { display: flex; gap: 0.5rem; margin-bottom: 1rem; }
.agent-tab { padding: 0.5rem 1rem; border: 2px solid #e2e8f0; border-radius: 8px; cursor: pointer;
             font-size: 0.85rem; font-weight: 600; background: white; transition: all 0.2s; }
.agent-tab:hover { border-color: #94a3b8; }
.agent-tab.active { border-color: #3b82f6; background: #eff6ff; color: #1d4ed8; }
.agent-tab .icon { font-size: 1.1rem; margin-right: 0.3rem; }
.chat-area { flex: 1; overflow-y: auto; padding: 0.5rem 0; }
.msg { margin-bottom: 1rem; display: flex; gap: 0.7rem; }
.msg.user { flex-direction: row-reverse; }
.msg-bubble { max-width: 80%; padding: 0.8rem 1rem; border-radius: 12px; font-size: 0.9rem; line-height: 1.5; }
.msg.user .msg-bubble { background: #3b82f6; color: white; border-bottom-right-radius: 4px; }
.msg.assistant .msg-bubble { background: white; border: 1px solid #e2e8f0; border-bottom-left-radius: 4px; }
.msg.assistant .msg-bubble pre { background: #f1f5f9; padding: 0.5rem; border-radius: 6px; overflow-x: auto; font-size: 0.82rem; }
.msg.assistant .msg-bubble code { font-size: 0.82rem; }
.msg.assistant .msg-bubble table { border-collapse: collapse; margin: 0.5rem 0; width: 100%; font-size: 0.82rem; }
.msg.assistant .msg-bubble th, .msg.assistant .msg-bubble td { border: 1px solid #e2e8f0; padding: 0.3rem 0.5rem; text-align: left; }
.msg.assistant .msg-bubble th { background: #f8fafc; font-weight: 600; }
.tool-badge { display: inline-block; background: #fef3c7; color: #92400e; padding: 0.15rem 0.5rem;
              border-radius: 4px; font-size: 0.75rem; margin: 0.2rem 0; }
.input-area { padding: 0.8rem 0; border-top: 1px solid #e2e8f0; }
.input-row { display: flex; gap: 0.5rem; }
.input-row input { flex: 1; padding: 0.7rem 1rem; border: 2px solid #e2e8f0; border-radius: 10px;
                   font-size: 0.9rem; outline: none; }
.input-row input:focus { border-color: #3b82f6; }
.input-row button { padding: 0.7rem 1.5rem; background: #3b82f6; color: white; border: none;
                    border-radius: 10px; font-weight: 600; cursor: pointer; font-size: 0.9rem; }
.input-row button:hover { background: #2563eb; }
.input-row button:disabled { background: #94a3b8; cursor: not-allowed; }
.examples { margin-top: 0.5rem; display: flex; gap: 0.4rem; flex-wrap: wrap; }
.example-btn { padding: 0.3rem 0.7rem; background: #f1f5f9; border: 1px solid #e2e8f0;
               border-radius: 6px; font-size: 0.78rem; cursor: pointer; color: #475569; }
.example-btn:hover { background: #e2e8f0; }
.typing { display: inline-block; }
.typing span { display: inline-block; width: 6px; height: 6px; background: #94a3b8; border-radius: 50%;
               margin: 0 1px; animation: bounce 1.4s infinite; }
.typing span:nth-child(2) { animation-delay: 0.2s; }
.typing span:nth-child(3) { animation-delay: 0.4s; }
@keyframes bounce { 0%, 80%, 100% { transform: translateY(0); } 40% { transform: translateY(-6px); } }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>調達AIエージェント</h1>
    <p>144,681件の調達データ + 1,742自治体 + 35,723ニュース記事を活用</p>
  </header>
  <div class="agent-tabs">
    <div class="agent-tab active" data-agent="search">
      <span class="icon">&#128269;</span>検索
    </div>
    <div class="agent-tab" data-agent="analysis">
      <span class="icon">&#128200;</span>分析
    </div>
    <div class="agent-tab" data-agent="document">
      <span class="icon">&#128196;</span>文書生成
    </div>
  </div>
  <div class="chat-area" id="chat"></div>
  <div class="input-area">
    <div class="input-row">
      <input id="query" type="text" placeholder="質問を入力..." autofocus>
      <button id="send" onclick="sendMessage()">送信</button>
    </div>
    <div class="examples" id="examples"></div>
  </div>
</div>
<script>
const EXAMPLES = {
  search: [
    "東京都のIT案件を検索",
    "今月締め切りの工事案件",
    "カテゴリ別の件数分布",
    "北海道の最新入札情報",
  ],
  analysis: [
    "札幌市の調達リスクを分析して",
    "渋谷区と新宿区を比較して",
    "横浜市のDX政策の進捗は？",
    "財政力が弱い自治体のリスク傾向",
  ],
  document: [
    "札幌市への営業ブリーフを作成して",
    "渋谷区のベンダー構成を教えて",
    "大阪市と名古屋市の比較レポート",
    "福岡市の議会文書を検索",
  ],
};

let currentAgent = 'search';
const chat = document.getElementById('chat');
const queryInput = document.getElementById('query');
const sendBtn = document.getElementById('send');

function setAgent(agent) {
  currentAgent = agent;
  document.querySelectorAll('.agent-tab').forEach(t => t.classList.remove('active'));
  document.querySelector(`[data-agent="${agent}"]`).classList.add('active');
  showExamples(agent);
}

function showExamples(agent) {
  const el = document.getElementById('examples');
  el.innerHTML = EXAMPLES[agent].map(e =>
    `<button class="example-btn" onclick="queryInput.value='${e}';sendMessage()">${e}</button>`
  ).join('');
}

document.querySelectorAll('.agent-tab').forEach(t => {
  t.addEventListener('click', () => setAgent(t.dataset.agent));
});

function addMessage(role, html) {
  const div = document.createElement('div');
  div.className = `msg ${role}`;
  div.innerHTML = `<div class="msg-bubble">${html}</div>`;
  chat.appendChild(div);
  chat.scrollTop = chat.scrollHeight;
  return div;
}

function renderMarkdown(text) {
  // Simple markdown rendering
  let html = text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/^### (.+)$/gm, '<h4 style="margin:0.5rem 0 0.3rem">$1</h4>')
    .replace(/^## (.+)$/gm, '<h3 style="margin:0.7rem 0 0.3rem">$1</h3>')
    .replace(/^# (.+)$/gm, '<h2 style="margin:0.8rem 0 0.3rem">$1</h2>')
    .replace(/^- (.+)$/gm, '<li>$1</li>')
    .replace(/^(\d+)\. (.+)$/gm, '<li>$2</li>')
    .replace(/\n\n/g, '<br><br>')
    .replace(/\n/g, '<br>');
  // Wrap consecutive <li> in <ul>
  html = html.replace(/(<li>.*?<\/li>(?:<br>)?)+/g, '<ul>$&</ul>');
  return html;
}

async function sendMessage() {
  const query = queryInput.value.trim();
  if (!query) return;

  queryInput.value = '';
  sendBtn.disabled = true;
  addMessage('user', query.replace(/&/g,'&amp;').replace(/</g,'&lt;'));

  const typingDiv = addMessage('assistant', '<div class="typing"><span></span><span></span><span></span></div>');
  const bubble = typingDiv.querySelector('.msg-bubble');
  let fullText = '';

  try {
    const resp = await fetch(`/api/agent/${currentAgent}/stream`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({query}),
    });

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, {stream: true});
      const lines = buffer.split('\\n');
      buffer = lines.pop();

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const event = JSON.parse(line.slice(6));
          if (event.type === 'text') {
            fullText += event.text;
            bubble.innerHTML = renderMarkdown(fullText);
            chat.scrollTop = chat.scrollHeight;
          } else if (event.type === 'tool_start' || event.type === 'tool_executing') {
            bubble.innerHTML = renderMarkdown(fullText) +
              `<div class="tool-badge">&#9881; ${event.tool} 実行中...</div>`;
          } else if (event.type === 'tool_done') {
            // Tool completed, waiting for Claude's response
          } else if (event.type === 'done') {
            bubble.innerHTML = renderMarkdown(fullText);
          } else if (event.type === 'error') {
            bubble.innerHTML = `<span style="color:#dc2626">${event.text}</span>`;
          }
        } catch(e) {}
      }
    }

    if (fullText) {
      bubble.innerHTML = renderMarkdown(fullText);
    }
  } catch(e) {
    bubble.innerHTML = `<span style="color:#dc2626">エラー: ${e.message}</span>`;
  }

  sendBtn.disabled = false;
  queryInput.focus();
  chat.scrollTop = chat.scrollHeight;
}

queryInput.addEventListener('keydown', e => { if (e.key === 'Enter') sendMessage(); });
showExamples('search');
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def chat_ui():
    return CHAT_HTML


# ── Main ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8010"))
    uvicorn.run(app, host="0.0.0.0", port=port)
