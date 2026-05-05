"""
StreamFeature AI Monitoring Dashboard
FastAPI + OpenAI GPT-4o-mini powered real-time feature store monitor
"""

import os
import asyncio
import json
from datetime import datetime
from typing import Optional
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from openai import AsyncOpenAI
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="StreamFeature AI Dashboard", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# ── Prometheus Query Helper ───────────────────────────────────────

async def query_prometheus(promql: str) -> dict:
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            r = await client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": promql})
            return r.json()
        except Exception as e:
            return {"status": "error", "error": str(e)}

async def get_metrics_snapshot() -> dict:
    queries = {
        "serving_p99_ms": 'histogram_quantile(0.99, rate(featurestore_serving_latency_seconds_bucket[5m])) * 1000',
        "serving_p50_ms": 'histogram_quantile(0.50, rate(featurestore_serving_latency_seconds_bucket[5m])) * 1000',
        "rps": 'rate(featurestore_serving_requests_total[1m])',
        "error_rate": 'rate(featurestore_serving_requests_total{status="error"}[5m]) / rate(featurestore_serving_requests_total[5m])',
        "cache_hit_rate": 'rate(featurestore_cache_hits_total[5m]) / (rate(featurestore_cache_hits_total[5m]) + rate(featurestore_cache_misses_total[5m]))',
        "redis_p99_ms": 'histogram_quantile(0.99, rate(featurestore_redis_latency_seconds_bucket[5m])) * 1000',
        "circuit_breaker": 'featurestore_circuit_breaker_state{name="redis"}',
        "feature_freshness_max": 'max(featurestore_feature_freshness_seconds)',
    }
    results = {}
    for name, query in queries.items():
        data = await query_prometheus(query)
        try:
            val = data["data"]["result"]
            results[name] = float(val[0]["value"][1]) if val else None
        except (KeyError, IndexError, ValueError):
            results[name] = None
    return results

# ── AI Analysis ───────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    context: Optional[dict] = None

async def analyze_metrics_with_ai(metrics: dict, user_question: str = None) -> str:
    if not OPENAI_API_KEY:
        return "⚠️ OpenAI API key not configured. Set OPENAI_API_KEY env variable."

    sla_status = []
    if metrics.get("serving_p99_ms") and metrics["serving_p99_ms"] > 10:
        sla_status.append(f"❌ p99 latency {metrics['serving_p99_ms']:.1f}ms > 10ms SLA")
    if metrics.get("error_rate") and metrics["error_rate"] > 0.0001:
        sla_status.append(f"❌ Error rate {metrics['error_rate']*100:.3f}% > 0.01% SLA")
    if metrics.get("feature_freshness_max") and metrics["feature_freshness_max"] > 5:
        sla_status.append(f"❌ Feature freshness {metrics['feature_freshness_max']:.1f}s > 5s SLA")
    if metrics.get("circuit_breaker") and metrics["circuit_breaker"] == 2:
        sla_status.append("❌ Redis circuit breaker OPEN")

    system_prompt = """You are an expert SRE for a real-time ML feature store engine.
You monitor Kafka streams, Flink jobs, Redis clusters, and gRPC serving layers.
Analyze metrics and provide concise, actionable insights. Be specific about root causes and fixes.
Format responses with emojis and clear sections. Keep responses under 300 words."""

    metrics_str = json.dumps({k: (round(v, 3) if v else "N/A") for k, v in metrics.items()}, indent=2)
    sla_str = "\n".join(sla_status) if sla_status else "✅ All SLAs met"

    user_content = f"""Current metrics snapshot:
{metrics_str}

SLA Status:
{sla_str}

{f"User question: {user_question}" if user_question else "Provide a brief system health summary with any recommendations."}"""

    try:
        response = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            max_tokens=400,
            temperature=0.3,
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"AI analysis unavailable: {e}"

# ── HTML Dashboard ────────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>StreamFeature AI Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0a0e1a; --surface: #111827; --surface2: #1a2235;
    --border: #1e2d45; --accent: #3b82f6; --accent2: #8b5cf6;
    --green: #10b981; --red: #ef4444; --yellow: #f59e0b;
    --text: #e2e8f0; --muted: #64748b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Inter', sans-serif; min-height: 100vh; }
  .header { background: linear-gradient(135deg, #1a2235 0%, #0f172a 100%); border-bottom: 1px solid var(--border); padding: 16px 32px; display: flex; align-items: center; justify-content: space-between; }
  .logo { font-size: 20px; font-weight: 700; background: linear-gradient(135deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
  .status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); box-shadow: 0 0 8px var(--green); animation: pulse 2s infinite; display: inline-block; margin-right: 8px; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
  .grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; padding: 24px 32px; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 20px; transition: border-color .2s; }
  .card:hover { border-color: var(--accent); }
  .card-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; color: var(--muted); margin-bottom: 8px; }
  .card-value { font-family: 'JetBrains Mono', monospace; font-size: 28px; font-weight: 700; line-height: 1; }
  .card-unit { font-size: 13px; color: var(--muted); margin-top: 4px; }
  .ok { color: var(--green); } .warn { color: var(--yellow); } .err { color: var(--red); }
  .main { display: grid; grid-template-columns: 1fr 400px; gap: 16px; padding: 0 32px 32px; }
  .ai-panel { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; display: flex; flex-direction: column; height: 500px; }
  .ai-header { padding: 16px 20px; border-bottom: 1px solid var(--border); font-weight: 600; display: flex; align-items: center; gap: 8px; }
  .ai-messages { flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 12px; }
  .msg { padding: 12px 16px; border-radius: 10px; font-size: 13px; line-height: 1.6; max-width: 90%; white-space: pre-wrap; }
  .msg-ai { background: var(--surface2); border: 1px solid var(--border); align-self: flex-start; }
  .msg-user { background: linear-gradient(135deg, #1d4ed8, #7c3aed); align-self: flex-end; color: white; }
  .ai-input { padding: 16px; border-top: 1px solid var(--border); display: flex; gap: 8px; }
  .ai-input input { flex: 1; background: var(--surface2); border: 1px solid var(--border); border-radius: 8px; padding: 10px 14px; color: var(--text); font-family: inherit; font-size: 13px; outline: none; }
  .ai-input input:focus { border-color: var(--accent); }
  .ai-input button { background: linear-gradient(135deg, var(--accent), var(--accent2)); border: none; border-radius: 8px; padding: 10px 16px; color: white; cursor: pointer; font-weight: 600; font-size: 13px; }
  .logs-panel { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 0; overflow: hidden; }
  .logs-header { padding: 16px 20px; border-bottom: 1px solid var(--border); font-weight: 600; font-size: 13px; }
  .logs-body { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: #94a3b8; padding: 12px 16px; height: 420px; overflow-y: auto; line-height: 1.8; }
  .log-ok { color: var(--green); } .log-warn { color: var(--yellow); } .log-err { color: var(--red); }
  .sla-bar { display: flex; gap: 12px; padding: 0 32px 16px; flex-wrap: wrap; }
  .sla-badge { padding: 6px 14px; border-radius: 20px; font-size: 11px; font-weight: 600; letter-spacing: .5px; }
  .sla-ok { background: rgba(16,185,129,.15); border: 1px solid var(--green); color: var(--green); }
  .sla-fail { background: rgba(239,68,68,.15); border: 1px solid var(--red); color: var(--red); }
</style>
</head>
<body>
<div class="header">
  <div class="logo">⚡ StreamFeature AI Dashboard</div>
  <div style="display:flex;align-items:center;gap:24px;font-size:13px;color:var(--muted)">
    <span><span class="status-dot"></span>Live</span>
    <span id="last-update">--</span>
  </div>
</div>

<div class="grid" id="metrics-grid">
  <div class="card"><div class="card-label">Serving p99 Latency</div><div class="card-value ok" id="p99">--</div><div class="card-unit">ms · SLA &lt; 10ms</div></div>
  <div class="card"><div class="card-label">Serving p50 Latency</div><div class="card-value ok" id="p50">--</div><div class="card-unit">ms · SLA &lt; 2ms</div></div>
  <div class="card"><div class="card-label">Requests / sec</div><div class="card-value" id="rps">--</div><div class="card-unit">target &gt; 10k RPS</div></div>
  <div class="card"><div class="card-label">Error Rate</div><div class="card-value ok" id="errors">--</div><div class="card-unit">% · SLA &lt; 0.01%</div></div>
  <div class="card"><div class="card-label">Cache Hit Rate</div><div class="card-value ok" id="cache">--</div><div class="card-unit">% local LRU cache</div></div>
  <div class="card"><div class="card-label">Redis p99</div><div class="card-value ok" id="redis-p99">--</div><div class="card-unit">ms · target &lt; 1ms</div></div>
  <div class="card"><div class="card-label">Feature Freshness</div><div class="card-value ok" id="freshness">--</div><div class="card-unit">s · SLA &lt; 5s</div></div>
  <div class="card"><div class="card-label">Circuit Breaker</div><div class="card-value ok" id="breaker">CLOSED</div><div class="card-unit">Redis health</div></div>
</div>

<div class="sla-bar" id="sla-bar"></div>

<div class="main">
  <div class="logs-panel">
    <div class="logs-header">📋 Live Metrics Log</div>
    <div class="logs-body" id="logs"></div>
  </div>
  <div class="ai-panel">
    <div class="ai-header">🤖 AI Assistant <span style="font-size:11px;color:var(--muted);font-weight:400">GPT-4o-mini</span></div>
    <div class="ai-messages" id="ai-messages">
      <div class="msg msg-ai">👋 I'm your AI SRE assistant. I monitor your feature store in real-time. Ask me anything about your metrics, SLA breaches, or request an analysis!</div>
    </div>
    <div class="ai-input">
      <input id="chat-input" type="text" placeholder="Ask about metrics, alerts, recommendations..." onkeydown="if(event.key==='Enter')sendChat()">
      <button onclick="sendChat()">Send</button>
    </div>
  </div>
</div>

<script>
  const ws = new WebSocket(`ws://${location.host}/ws`);
  let lastMetrics = {};

  ws.onmessage = (e) => {
    const data = JSON.parse(e.data);
    if (data.type === 'metrics') updateMetrics(data.metrics);
    else if (data.type === 'ai') appendAIMessage(data.text, 'ai');
  };

  function fmt(v, dec=1) { return v !== null && v !== undefined ? v.toFixed(dec) : '--'; }

  function updateMetrics(m) {
    lastMetrics = m;
    const p99 = m.serving_p99_ms;
    document.getElementById('p99').textContent = fmt(p99);
    document.getElementById('p99').className = `card-value ${p99 > 10 ? 'err' : p99 > 5 ? 'warn' : 'ok'}`;
    document.getElementById('p50').textContent = fmt(m.serving_p50_ms);
    document.getElementById('rps').textContent = fmt(m.rps, 0);
    const err = m.error_rate ? m.error_rate * 100 : 0;
    document.getElementById('errors').textContent = fmt(err, 3);
    document.getElementById('errors').className = `card-value ${err > 0.01 ? 'err' : 'ok'}`;
    const cache = m.cache_hit_rate ? m.cache_hit_rate * 100 : 0;
    document.getElementById('cache').textContent = fmt(cache, 1);
    document.getElementById('redis-p99').textContent = fmt(m.redis_p99_ms, 2);
    const fresh = m.feature_freshness_max;
    document.getElementById('freshness').textContent = fmt(fresh, 1);
    document.getElementById('freshness').className = `card-value ${fresh > 30 ? 'err' : fresh > 5 ? 'warn' : 'ok'}`;
    const cb = m.circuit_breaker;
    const el = document.getElementById('breaker');
    el.textContent = cb === 2 ? 'OPEN' : cb === 1 ? 'HALF-OPEN' : 'CLOSED';
    el.className = `card-value ${cb === 2 ? 'err' : cb === 1 ? 'warn' : 'ok'}`;
    document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
    appendLog(m);
    updateSLABadges(m);
  }

  function appendLog(m) {
    const logs = document.getElementById('logs');
    const ts = new Date().toLocaleTimeString();
    const p99 = m.serving_p99_ms;
    const cls = p99 > 10 ? 'log-err' : p99 > 5 ? 'log-warn' : 'log-ok';
    logs.innerHTML += `<div class="${cls}">[${ts}] p99=${fmt(m.serving_p99_ms)}ms p50=${fmt(m.serving_p50_ms)}ms rps=${fmt(m.rps,0)} err=${fmt(m.error_rate?m.error_rate*100:0,3)}% fresh=${fmt(m.feature_freshness_max,1)}s</div>`;
    logs.scrollTop = logs.scrollHeight;
    if (logs.children.length > 200) logs.removeChild(logs.firstChild);
  }

  function updateSLABadges(m) {
    const slas = [
      { label: `p99 ${fmt(m.serving_p99_ms)}ms`, ok: (m.serving_p99_ms||0) <= 10 },
      { label: `p50 ${fmt(m.serving_p50_ms)}ms`, ok: (m.serving_p50_ms||0) <= 2 },
      { label: `Error ${fmt(m.error_rate?m.error_rate*100:0,3)}%`, ok: (m.error_rate||0) <= 0.0001 },
      { label: `Freshness ${fmt(m.feature_freshness_max,1)}s`, ok: (m.feature_freshness_max||0) <= 5 },
      { label: `Redis CB ${m.circuit_breaker===2?'OPEN':'CLOSED'}`, ok: m.circuit_breaker !== 2 },
    ];
    document.getElementById('sla-bar').innerHTML = slas.map(s =>
      `<span class="sla-badge ${s.ok?'sla-ok':'sla-fail'}">${s.ok?'✅':'❌'} ${s.label}</span>`
    ).join('');
  }

  async function sendChat() {
    const input = document.getElementById('chat-input');
    const msg = input.value.trim();
    if (!msg) return;
    input.value = '';
    appendAIMessage(msg, 'user');
    const resp = await fetch('/chat', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ message: msg, context: lastMetrics })
    });
    const data = await resp.json();
    appendAIMessage(data.response, 'ai');
  }

  function appendAIMessage(text, role) {
    const container = document.getElementById('ai-messages');
    const div = document.createElement('div');
    div.className = `msg msg-${role}`;
    div.textContent = text;
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
  }
</script>
</body>
</html>"""

# ── API Endpoints ─────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML

@app.post("/chat")
async def chat(req: ChatRequest):
    metrics = req.context or await get_metrics_snapshot()
    response = await analyze_metrics_with_ai(metrics, req.message)
    return {"response": response}

@app.get("/metrics/snapshot")
async def metrics_snapshot():
    return await get_metrics_snapshot()

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            metrics = await get_metrics_snapshot()
            await ws.send_json({"type": "metrics", "metrics": metrics})
            # Auto-analyze if SLA breach detected
            p99 = metrics.get("serving_p99_ms") or 0
            freshness = metrics.get("feature_freshness_max") or 0
            if p99 > 10 or freshness > 30:
                analysis = await analyze_metrics_with_ai(metrics)
                await ws.send_json({"type": "ai", "text": f"🚨 Auto-analysis:\n{analysis}"})
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        pass

if __name__ == "__main__":
    import uvicorn
    print("🚀 StreamFeature AI Dashboard → http://localhost:8888")
    uvicorn.run(app, host="0.0.0.0", port=8888)
