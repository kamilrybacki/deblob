#!/usr/bin/env python3
"""Deblob demo — dashboard (simplified: saved schema vs incoming + auto-update).

Pure-stdlib BFF: serves the single-page UI and server-side proxies the producer
and the aware amender (avoids CORS). The page shows the SAVED (registered) schema
next to the INCOMING (just-read) shape, and — when the producer drifts — how
Deblob auto-updates the fields it can prove while flagging the ambiguous one.
"""
import json
import os
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PRODUCER = os.environ.get("PRODUCER_URL", "http://demo-producer.deblob-demo.svc.cluster.local:8080")
AWARE = os.environ.get("AWARE_URL", "http://demo-aware.deblob-demo.svc.cluster.local:8080")


def _get(url):
    try:
        with urllib.request.urlopen(url, timeout=3) as r:
            return json.loads(r.read()), 200
    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "starting": True}, 200


def _post(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=b"", method="POST"), timeout=3) as r:
            return json.loads(r.read()), 200
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}, 502


HTML = r"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Deblob — Schema Auto-Amend</title>
<style>
:root{--bg:#0e1116;--card:#171b22;--ink:#e6e9ef;--muted:#8b94a3;--line:#232833;
--red:#ff5c5c;--green:#35d0b2;--greenbg:#0f2622;--amber:#ffc857;--amberbg:#2a2410;--teal:#6fd3e6}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:15px/1.55 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto}
.wrap{max-width:960px;margin:0 auto;padding:22px}
h1{font-size:21px;margin:0 0 2px}.sub{color:var(--muted);margin:0 0 18px;font-size:14px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:640px){.grid{grid-template-columns:1fr}}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px}
.card h2{font-size:12px;text-transform:uppercase;letter-spacing:.06em;margin:0 0 10px;color:var(--muted)}
.badge{display:inline-block;padding:2px 9px;border-radius:999px;font-weight:600;font-size:12px;vertical-align:middle}
.b-sync{background:#12303a;color:var(--teal)}.b-drift{background:#3a2410;color:var(--amber)}
.b-v{background:#1c3a34;color:var(--green)}
.f{display:flex;justify-content:space-between;gap:10px;padding:5px 0;border-bottom:1px solid var(--line);font-family:ui-monospace,Menlo,monospace;font-size:13px}
.f:last-child{border:0}.f .t{color:var(--muted)}
.f.id{color:var(--green)}.f.rn{color:var(--green)}.f.amb{color:var(--amber)}.f.new{color:var(--teal)}.f.gone{color:var(--muted);text-decoration:line-through;opacity:.6}
.tag{font-size:11px;color:var(--muted);margin-left:6px}
.upd{margin-top:16px}
.pill{display:inline-block;padding:3px 10px;border-radius:999px;font-size:13px;font-weight:600;margin-right:8px}
.pill.ok{background:var(--greenbg);color:var(--green)}.pill.wait{background:var(--amberbg);color:var(--amber)}
.needs{background:var(--amberbg);border:1px solid var(--amber);border-radius:8px;padding:11px 13px;margin-top:10px;display:none}
.needs.on{display:block}
button{padding:14px;font-size:16px;font-weight:700;color:#0e1116;background:var(--amber);border:0;border-radius:10px;cursor:pointer;flex:1}
button:disabled{opacity:.45;cursor:default}button:hover:not(:disabled){filter:brightness(1.08)}
.approve{margin-top:9px;padding:9px 14px;font-size:13px;width:auto;flex:0}
.note{color:var(--muted);font-size:13px}
</style></head><body><div class=wrap>
<h1>Deblob — Schema Auto-Amend <span class=badge id=state>—</span></h1>
<p class=sub>Deblob keeps the <b>saved schema</b> in sync with what's actually arriving. On a shape change it <b>auto-updates the fields it can prove</b> and asks you only about the ambiguous one. <span class=mono>events.demo.orders</span></p>

<div class=grid>
  <div class=card>
    <h2>Saved schema <span class="badge b-v" id=savedver></span></h2>
    <div class=note id=savedname style=margin-bottom:8px>…</div>
    <div id=savedfields></div>
  </div>
  <div class=card>
    <h2>Incoming — just read <span class=badge id=inbadge></span></h2>
    <div class=note id=innote style=margin-bottom:8px>…</div>
    <div id=infields></div>
  </div>
</div>

<div class="card upd">
  <h2>Auto-update</h2>
  <div id=updsummary class=note>Producer is emitting the saved shape — nothing to amend.</div>
  <div id=updauto style=margin-top:6px></div>
  <div class=needs id=needs>
    <b>⏸ Needs your approval — Deblob won't guess a unit change:</b>
    <div class=mono id=needstext style=margin-top:6px></div>
    <button class="approve" onclick=approve()>✔ Approve this update</button>
  </div>
</div>

<div style="display:flex;gap:10px;margin-top:16px">
  <button id=trig onclick=trigger()>⚡ Trigger drift (v1 → v2)</button>
  <button onclick=rollback() style="background:#2a3340;color:var(--ink);flex:0 0 150px;font-weight:600">↺ Reset</button>
</div>
<div class=note id=trignote style=margin-top:8px></div>
</div>
<script>
function esc(s){return (s+'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}
function short(p){return (p||'').replace('$.','')}
function fieldsHTML(leaves, statusMap){ // leaves={path:type}, statusMap path->{cls,tag}
  if(!leaves) return '<div class=note>—</div>';
  return Object.keys(leaves).sort().map(p=>{
    const st=(statusMap&&statusMap[p])||{};
    return '<div class="f '+(st.cls||'')+'"><span>'+esc(short(p))+(st.tag?'<span class=tag>'+st.tag+'</span>':'')+'</span><span class=t>'+esc(leaves[p])+'</span></div>';
  }).join('');
}
async function j(u){try{return await (await fetch(u)).json()}catch(e){return{}}}
async function tick(){
 const p=await j('/api/producer'), a=await j('/api/aware');
 const drift=!!a.drift_detected_at, v=p.version||'v1';
 const sb=document.getElementById('state');
 if(drift){sb.textContent='DRIFT → AUTO-AMENDED';sb.className='badge b-drift'}
 else{sb.textContent='IN SYNC';sb.className='badge b-sync'}
 // saved schema
 document.getElementById('savedver').textContent='v'+(a.blessed_family_version||1);
 document.getElementById('savedname').textContent=a.blessed_schema_name||'(learning the baseline…)';
 const saved=a.blessed_leaves;
 // build status maps from data_amend
 const da=a.data_amend||{}, inMap={}, savedMap={};
 (da.auto||[]).forEach(m=>{ if(m.kind==='identity'){inMap[m.to]={cls:'id',tag:'unchanged'};savedMap[m.from]={cls:'id'}}
   else {inMap[m.to]={cls:'rn',tag:'← renamed from '+short(m.from)};savedMap[m.from]={cls:'rn',tag:'→ '+short(m.to)}} });
 (da.approved||[]).forEach(m=>{ inMap[m.to]={cls:'rn',tag:'← '+short(m.from)+' ✔approved'};savedMap[m.from]={cls:'rn',tag:'✔ updated'} });
 (da.additive||[]).forEach(m=>{ inMap[m.path]={cls:'new',tag:'new · additive'} });
 (da.needs_approval||[]).forEach(m=>{ if(m.to)inMap[m.to]={cls:'amb',tag:'← was '+short(m.from)+' ⏸'}; if(m.from)savedMap[m.from]={cls:'amb',tag:'⏸ pending'} });
 document.getElementById('savedfields').innerHTML=fieldsHTML(saved,savedMap);
 // incoming
 const inb=document.getElementById('inbadge');
 if(drift && a.new_leaves){
   inb.textContent='v2 · DRIFTED';inb.className='badge b-drift';
   document.getElementById('innote').textContent='Shape changed — Deblob is reconciling it into the saved schema.';
   document.getElementById('infields').innerHTML=fieldsHTML(a.new_leaves,inMap);
 } else {
   inb.textContent='matches saved';inb.className='badge b-sync';
   document.getElementById('innote').textContent='Every record matches the saved schema. Nothing to amend.';
   document.getElementById('infields').innerHTML=fieldsHTML(saved,{});
 }
 // auto-update summary
 const autoN=(da.auto||[]).length+(da.additive||[]).length+(da.approved||[]).length;
 const needs=da.needs_approval||[];
 const us=document.getElementById('updsummary'), ua=document.getElementById('updauto'), nd=document.getElementById('needs');
 if(drift && (autoN||needs.length)){
   us.innerHTML='<span class="pill ok">✅ '+autoN+' fields auto-updated</span>'+(needs.length?'<span class="pill wait">⏸ '+needs.length+' needs you</span>':'<span class="pill ok">✔ fully in sync</span>');
   ua.innerHTML='<div class=note style=margin-top:6px>Auto-applied (provable): '+
     (da.auto||[]).map(m=>short(m.from)+(m.to&&m.to!==m.from?'→'+short(m.to):'')).concat((da.approved||[]).map(m=>short(m.from)+'→'+short(m.to)+'✔')).concat((da.additive||[]).map(m=>'+'+short(m.path))).join(' · ')+'</div>';
   if(needs.length){nd.classList.add('on');
     document.getElementById('needstext').innerHTML=needs.map(m=>'<b>'+short(m.from)+' → '+short(m.to||'?')+'</b> — '+m.reason).join('<br>');
   } else nd.classList.remove('on');
 } else {
   us.textContent='Producer is emitting the saved shape — nothing to amend.';ua.innerHTML='';nd.classList.remove('on');
 }
 document.getElementById('trig').disabled=(v==='v2');
}
async function trigger(){document.getElementById('trignote').textContent='drift injected — watch the saved schema auto-update…';await fetch('/api/trigger',{method:'POST'});}
async function approve(){await fetch('/api/approve',{method:'POST'});document.getElementById('trignote').textContent='approved — the saved schema now includes that field.';}
async function rollback(){await fetch('/api/reset',{method:'POST'});document.getElementById('trignote').textContent='producer back on v1.';}
setInterval(tick,1000);tick();
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            body = HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/healthz":
            return self._json(200, {"ok": True})
        if self.path == "/api/producer":
            d, c = _get(PRODUCER + "/state"); return self._json(c, d)
        if self.path == "/api/aware":
            d, c = _get(AWARE + "/status"); return self._json(c, d)
        return self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/api/trigger":
            d, c = _post(PRODUCER + "/trigger"); return self._json(c, d)
        if self.path == "/api/reset":
            d, c = _post(PRODUCER + "/reset"); return self._json(c, d)
        if self.path == "/api/approve":
            d, c = _post(AWARE + "/approve"); return self._json(c, d)
        return self._json(404, {"error": "not found"})

    def log_message(self, *a):
        pass


def main():
    print("dashboard on :8080", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
