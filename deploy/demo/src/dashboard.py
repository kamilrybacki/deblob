#!/usr/bin/env python3
"""Deblob demo — dashboard (schema-evolution NORMALIZATION story).

Pure-stdlib BFF: serves the single-page UI and server-side proxies the producer,
the normalizer, and the mock ETL (avoids CORS). The page tells one story: the
upstream producer can drift its shape BOTH ways (v1 <-> v2) and the downstream
ETL never breaks, because Deblob's normalizer reshapes every incoming record into
one stable, accreting canonical contract.
"""
import json
import os
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PRODUCER = os.environ.get("PRODUCER_URL", "http://demo-producer.deblob-demo.svc.cluster.local:8080")
NORMALIZER = os.environ.get("NORMALIZER_URL", "http://demo-normalizer.deblob-demo.svc.cluster.local:8080")
ETL = os.environ.get("ETL_URL", "http://demo-etl.deblob-demo.svc.cluster.local:8080")
CODEGEN = os.environ.get("CODEGEN_URL", "http://demo-codegen.deblob-demo.svc.cluster.local:8080")


def _get(url):
    try:
        with urllib.request.urlopen(url, timeout=3) as r:
            return json.loads(r.read()), 200
    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "starting": True}, 200


def _post(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=b"", method="POST"), timeout=6) as r:
            return json.loads(r.read()), 200
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}, 502


HTML = r"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Deblob — Live Data Contracts</title>
<style>
:root{--bg:#0e1116;--card:#171b22;--ink:#e6e9ef;--muted:#8b94a3;--line:#232833;
--red:#ff5c5c;--green:#35d0b2;--greenbg:#0f2622;--amber:#ffc857;--amberbg:#2a2410;--teal:#6fd3e6;--tealbg:#12303a}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:15px/1.55 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto}
.wrap{max-width:1040px;margin:0 auto;padding:22px}
h1{font-size:21px;margin:0 0 2px}.sub{color:var(--muted);margin:0 0 18px;font-size:14px}
.mono{font-family:ui-monospace,Menlo,monospace}
.flow{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px}
@media(max-width:640px){.flow{grid-template-columns:1fr}}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px}
.card h2{font-size:12px;text-transform:uppercase;letter-spacing:.06em;margin:0 0 10px;color:var(--muted)}
.badge{display:inline-block;padding:2px 9px;border-radius:999px;font-weight:600;font-size:12px;vertical-align:middle}
.b-v1{background:var(--tealbg);color:var(--teal)}.b-v2{background:#3a2410;color:var(--amber)}
.b-norm{background:var(--greenbg);color:var(--green)}
.f{display:flex;justify-content:space-between;gap:10px;padding:5px 0;border-bottom:1px solid var(--line);font-family:ui-monospace,Menlo,monospace;font-size:12.5px}
.f:last-child{border:0}.f .t{color:var(--muted)}
.f.id{color:var(--ink)}.f.rn{color:var(--green)}.f.amb{color:var(--amber)}.f.new{color:var(--teal)}.f.core{color:var(--green)}.f.add{color:var(--teal)}
.tag{font-size:10.5px;color:var(--muted);margin-left:6px}
.note{color:var(--muted);font-size:13px}
.arrow{text-align:center;color:var(--muted);font-size:13px;margin:2px 0 6px}
.etl{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-top:14px}
@media(max-width:640px){.etl{grid-template-columns:1fr}}
.stat{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px;text-align:center}
.stat .n{font-size:34px;font-weight:800;line-height:1.1}
.stat .l{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-top:4px}
.stat.ok .n{color:var(--green)}.stat.err .n{color:var(--red)}.stat.held .n{color:var(--amber)}
.stat.ok.on{border-color:var(--green);box-shadow:0 0 0 1px var(--green) inset}
.pa{background:var(--amberbg);border:1px solid var(--amber);border-radius:12px;padding:14px 16px;margin-top:14px;display:none}
.pa.on{display:block}
.pa b{color:var(--amber)}
button{padding:14px;font-size:16px;font-weight:700;color:#0e1116;background:var(--amber);border:0;border-radius:10px;cursor:pointer;flex:1}
button:disabled{opacity:.45;cursor:default}button:hover:not(:disabled){filter:brightness(1.08)}
.approve{margin-top:10px;padding:10px 16px;font-size:14px;width:auto;flex:0}
.controls{display:flex;gap:10px;margin-top:16px}
.reset{background:#2a3340;color:var(--ink)}
.cg{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:640px){.cg{grid-template-columns:1fr}}
.cgh{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);margin-bottom:5px}
.code{background:#0b0e13;border:1px solid var(--line);border-radius:8px;padding:11px;margin:0;
font-family:ui-monospace,Menlo,monospace;font-size:12px;line-height:1.5;color:var(--ink);
overflow:auto;max-height:280px;white-space:pre}
</style></head><body><div class=wrap>
<h1>Deblob — Live Data Contracts <span class=badge id=state>—</span></h1>
<p class=sub>Point Deblob at a drifting source and it hands your downstream systems a <b>stable, backwards-compatible data contract</b> — as <b>JSON Schema</b>, a <b>Pydantic v2 model</b>, and <b>SQL migrations</b> — generated live. Change the payload shape (either way) and watch the model + migration update <b>without breaking old consumers</b>. <span class=mono>events.demo.orders</span></p>

<div class=card>
  <h2>Your data model — generated live <span class="badge b-norm" id=cggen>—</span></h2>
  <div class=note style=margin-bottom:12px>Deblob discovers the shape, normalizes it to a canonical superset, and emits a standard <b>JSON Schema</b> → a real <b>Pydantic v2</b> model (<span class=mono>datamodel-code-generator</span>) → <b>additive SQL migrations</b>. Trigger drift → a new <span class=mono>Optional</span> field + nullable column appear. Old code keeps working.</div>
  <div class=cg>
    <div><div class=cgh>JSON Schema · draft 2020-12</div><pre class=code id=cgschema>…</pre></div>
    <div><div class=cgh>Pydantic v2 · models.py</div><pre class=code id=cgpy>…</pre></div>
  </div>
  <div class=cgh style=margin-top:12px>SQL migrations · additive, backwards-compatible</div>
  <pre class=code id=cgsql>…</pre>
</div>

<div class=controls>
  <button id=trig onclick=trigger()>⚡ Trigger drift (v1 → v2)</button>
  <button class=reset id=reset onclick=reset()>↺ Reset (v2 → v1)</button>
</div>
<div class=note style=margin-top:8px>Drift either way, any number of times — the contract only ever grows (additive), so every generated migration is non-breaking and old consumers keep working.</div>

<div class="pa" id=pa>
  <b>⏸ Needs your approval</b> — Deblob won't guess a unit change on a core field.
  <div class=mono id=patext style=margin-top:8px></div>
  <button class=approve onclick=approve()>✔ Approve conversion &amp; flush held</button>
</div>

<div class=etl>
  <div class="stat ok" id=st-proc><div class=n id=proc>0</div><div class=l>ETL rows processed</div></div>
  <div class="stat ok on" id=st-err><div class=n id=err>0</div><div class=l>ETL errors · pipeline unbroken</div></div>
  <div class="stat held" id=st-held><div class=n id=held>0</div><div class=l>held (pending approval)</div></div>
</div>
<div class=note id=etlnote style=margin-top:8px></div>

<div class=cgh style="margin:24px 0 10px;font-size:12px">Under the hood · how the contract stays stable</div>
<div class=flow>
  <div class=card>
    <h2>Incoming shape <span class=badge id=inbadge></span></h2>
    <div class=note id=innote style=margin-bottom:8px>…</div>
    <div id=infields></div>
  </div>
  <div class=card>
    <h2>Transform applied</h2>
    <div class=note id=trnote style=margin-bottom:8px>…</div>
    <div id=trfields></div>
  </div>
  <div class=card>
    <h2>Normalized → canonical <span class="badge b-norm">the contract</span></h2>
    <div class=note style=margin-bottom:8px>The stable superset every downstream consumer + the codegen build against.</div>
    <div id=canonfields></div>
  </div>
</div>
</div>
<script>
function esc(s){return (s+'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}
function short(p){return (p||'').replace('$.','')}
async function j(u,o){try{return await (await fetch(u,o)).json()}catch(e){return{}}}

function rows(items){return items.map(x=>'<div class="f '+(x.cls||'')+'"><span>'+esc(x.k)+(x.tag?'<span class=tag>'+x.tag+'</span>':'')+'</span><span class=t>'+esc(x.v==null?'':x.v)+'</span></div>').join('')||'<div class=note>—</div>'}

async function tick(){
 const p=await j('/api/producer'), n=await j('/api/normalizer'), e=await j('/api/etl');
 const cg=await j('/api/codegen');
 document.getElementById('cggen').textContent=cg.generator?('via '+cg.generator):'—';
 document.getElementById('cgschema').textContent=cg.json_schema?JSON.stringify(cg.json_schema,null,2):(cg.error||'generating…');
 document.getElementById('cgpy').textContent=cg.pydantic||'generating…';
 document.getElementById('cgsql').textContent=(cg.migrations&&cg.migrations.length)?cg.migrations.map(m=>'-- '+m.note+'\n'+m.sql).join('\n\n'):'generating…';
 const v=p.version||'v1';
 // header badge
 const sb=document.getElementById('state');
 sb.textContent=(v==='v2'?'v2 · EVOLVING':'v1 · LIVE');
 sb.className='badge '+(v==='v2'?'b-v2':'b-v1');

 // 1 · Incoming shape (from producer sample)
 const inb=document.getElementById('inbadge');
 inb.textContent=v.toUpperCase(); inb.className='badge '+(v==='v2'?'b-v2':'b-v1');
 document.getElementById('innote').textContent=(v==='v2'
   ?'Producer drifted: amount→total_cents (×100), customer_name→customer{}, +shipping{}.'
   :'Producer is on the baseline v1 shape.');
 const fields=(v==='v2'?p.v2_fields:p.v1_fields)||[];
 const sample=p.sample||{};
 document.getElementById('infields').innerHTML=rows(fields.map(f=>({k:f,v:''})));

 // find the transform for the CURRENT incoming shape (the one whose held/rename
 // matches this version). Pick the shape whose transform mentions total_cents for v2.
 const tx=n.transforms||{}; let cur=null;
 for(const sid in tx){const t=tx[sid];
   const hasCents=Object.values(t).some(m=>m.src&&/cents/.test(m.src));
   if(v==='v2'&&hasCents){cur=t;break} if(v==='v1'&&!hasCents){cur=t}}
 // 2 · Transform applied
 const trn=document.getElementById('trnote');
 if(cur){
   trn.textContent='Per-field mapping from this shape into the canonical contract.';
   const order=Object.keys(n.canonical||{});
   document.getElementById('trfields').innerHTML=rows(order.filter(f=>cur[f]).map(f=>{
     const m=cur[f]; let cls='id',tag=m.kind;
     if(m.kind==='rename'){cls='rn';tag='← '+short(m.src)}
     else if(m.kind==='additive'){cls='add';tag='additive'}
     else if(m.kind==='held'){cls='amb';tag=(m.pending?'⏸ held ':'✔ ')+'← '+short(m.src)+(m.conversion?' '+m.conversion.desc:'')}
     else {cls='id';tag='identity'}
     return {k:f,v:'',cls:cls,tag:tag};
   }));
 } else {trn.textContent='Learning the incoming shape…';document.getElementById('trfields').innerHTML='<div class=note>—</div>'}

 // 3 · Normalized output = canonical superset
 const canon=n.canonical||{};
 document.getElementById('canonfields').innerHTML=rows(Object.keys(canon).map(f=>({
   k:f,v:canon[f].ty,cls:(canon[f].kind==='core'?'core':'add'),
   tag:(canon[f].kind==='core'?'core':'additive')})));

 // ETL stats
 document.getElementById('proc').textContent=e.processed||0;
 const errs=e.errors||0; document.getElementById('err').textContent=errs;
 const es=document.getElementById('st-err');
 es.className='stat '+(errs>0?'err':'ok on');
 es.querySelector('.l').textContent=(errs>0?'ETL errors':'ETL errors · pipeline unbroken');
 document.getElementById('held').textContent=n.held||0;
 document.getElementById('etlnote').textContent=(errs>0
   ?'⚠ contract violation — should never happen; normalizer emitted an incomplete record.'
   :'✅ backwards-compatible: '+(e.processed||0)+' records validated against the original v1 contract, 0 errors.');

 // pending approval box
 const pa=document.getElementById('pa'), pend=(n.pending_approvals||[]);
 if(pend.length){pa.classList.add('on');
   document.getElementById('patext').innerHTML=pend.map(m=>'<b>'+m.canonical_field+' ← '+m.from_shape_field+(m.conversion?' · '+m.conversion.desc:'')+'</b> &nbsp;<span class=note>'+esc(m.reason||'')+'</span>').join('<br>');
 } else pa.classList.remove('on');

 document.getElementById('trig').disabled=(v==='v2');
 document.getElementById('reset').disabled=(v==='v1');
}
async function trigger(){document.getElementById('etlnote').textContent='drift injected → watch the normalizer reshape it…';await fetch('/api/trigger',{method:'POST'});}
async function reset(){document.getElementById('etlnote').textContent='producer back on v1 — ETL stays green.';await fetch('/api/reset',{method:'POST'});}
async function approve(){document.getElementById('etlnote').textContent='approved — flushing held records into the pipeline…';await fetch('/api/approve',{method:'POST'});}
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
        if self.path == "/api/normalizer":
            d, c = _get(NORMALIZER + "/status"); return self._json(c, d)
        if self.path == "/api/etl":
            d, c = _get(ETL + "/status"); return self._json(c, d)
        if self.path == "/api/codegen":
            d, c = _get(CODEGEN + "/status"); return self._json(c, d)
        return self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/api/trigger":
            d, c = _post(PRODUCER + "/trigger"); return self._json(c, d)
        if self.path == "/api/reset":
            d, c = _post(PRODUCER + "/reset"); return self._json(c, d)
        if self.path == "/api/approve":
            d, c = _post(NORMALIZER + "/approve"); return self._json(c, d)
        return self._json(404, {"error": "not found"})

    def log_message(self, *a):
        pass


def main():
    print("dashboard on :8080", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
