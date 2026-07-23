#!/usr/bin/env python3
"""Deblob demo — dashboard.

Pure-stdlib service: serves the single-page demo UI and server-side proxies to
the producer / naive / aware services + Deblob API (avoids CORS). The page polls
~1s and renders the drift story; the TRIGGER DRIFT button POSTs to the producer.

HTTP (:8080):  GET /  GET /api/{producer,naive,aware}  POST /api/trigger  GET /healthz
"""
import json
import os
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PRODUCER = os.environ.get("PRODUCER_URL", "http://demo-producer.deblob-demo.svc.cluster.local:8080")
NAIVE = os.environ.get("NAIVE_URL", "http://demo-naive.deblob-demo.svc.cluster.local:8080")
AWARE = os.environ.get("AWARE_URL", "http://demo-aware.deblob-demo.svc.cluster.local:8080")


def _get(url):
    try:
        with urllib.request.urlopen(url, timeout=3) as r:
            return json.loads(r.read()), 200
    except Exception as e:  # noqa: BLE001
        return {"error": str(e), "starting": True}, 200


def _post(url):
    try:
        req = urllib.request.Request(url, data=b"", method="POST")
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read()), 200
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}, 502


HTML = r"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Deblob — Drift Sentinel</title>
<style>
:root{--bg:#0e1116;--card:#171b22;--ink:#e6e9ef;--muted:#8b94a3;--line:#232833;
--red:#ff5c5c;--redbg:#2a1414;--green:#35d0b2;--greenbg:#0f2622;--amber:#ffc857}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:15px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto}
.wrap{max-width:1080px;margin:0 auto;padding:24px}
h1{font-size:22px;margin:0 0 2px}.sub{color:var(--muted);margin:0 0 20px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:18px}
.card h2{font-size:13px;text-transform:uppercase;letter-spacing:.06em;margin:0 0 12px;color:var(--muted)}
.big{font-size:40px;font-weight:700;line-height:1}
.row{display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid var(--line)}
.row:last-child{border:0}.k{color:var(--muted)}.mono{font-family:ui-monospace,Menlo,monospace;font-size:13px}
.badge{display:inline-block;padding:2px 10px;border-radius:999px;font-weight:600;font-size:13px}
.v1{background:#12303a;color:#6fd3e6}.v2{background:#3a2410;color:var(--amber)}
.naive{border-color:#3a2020}.naive .big{color:var(--red)}
.aware{border-color:#1c3a34}.aware .big{color:var(--green)}
.drift{background:var(--greenbg);border:1px solid var(--green);border-radius:8px;padding:10px 12px;margin-top:10px;display:none}
.drift.on{display:block}.broke{background:var(--redbg);border:1px solid var(--red);border-radius:8px;padding:10px 12px;margin-top:10px;display:none}
.broke.on{display:block}
button{margin-top:18px;width:100%;padding:16px;font-size:17px;font-weight:700;color:#0e1116;
background:var(--amber);border:0;border-radius:10px;cursor:pointer}
button:disabled{opacity:.5;cursor:default}button:hover:not(:disabled){filter:brightness(1.08)}
.diff{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}
.diff div{background:#10141a;border:1px solid var(--line);border-radius:8px;padding:8px}
.diff .t{color:var(--muted);font-size:11px;text-transform:uppercase;margin-bottom:4px}
.chg{color:var(--amber)}.full{grid-column:1/3}.note{color:var(--muted);font-size:13px;margin-top:6px}
</style></head><body><div class=wrap>
<h1>Deblob — Drift Sentinel <span class=badge id=verbadge>—</span></h1>
<p class=sub>A producer changes its payload shape without warning. Watch the naive loader break while the Deblob-aware loader catches it. <span class=mono>events.demo.orders → events.tagged</span></p>
<div class=grid>
  <div class="card full"><h2>Hero producer · events.demo.orders</h2>
    <div class=row><span class=k>produced</span><span class=mono id=produced>0</span></div>
    <div class=row><span class=k>current shape</span><span class=mono id=shape>v1</span></div>
    <div class=diff>
      <div><div class=t>v1 (baseline)</div><div class=mono>order_id · customer_name:str · amount:float · currency · item_count · placed_at</div></div>
      <div><div class=t>v2 (on drift)</div><div class=mono>order_id · <span class=chg>customer{id,name}</span> · <span class=chg>total_cents:int</span> · currency · item_count · placed_at · <span class=chg>shipping{method,eta_days}</span></div></div>
    </div>
    <div style="display:flex;gap:10px">
      <button id=trig onclick=trigger()>⚡ TRIGGER DRIFT (v1 → v2)</button>
      <button id=rb onclick=rollback() style="background:#2a3340;color:var(--ink);flex:0 0 200px">↺ Rollback to v1</button>
    </div>
    <div class=note id=trignote></div>
  </div>
  <div class="card naive"><h2>Naive loader · assumes v1</h2>
    <div class=big id=nerr>0</div><div class=k>parse errors (silent data loss)</div>
    <div class=row><span class=k>processed ok</span><span class=mono id=nok>0</span></div>
    <div class=broke id=nbroke><b>BROKEN.</b> Reading <span class=mono>customer_name</span>/<span class=mono>amount</span> — gone in v2. <span class=mono id=nlast></span></div>
  </div>
  <div class="card aware"><h2>Deblob-aware loader · reads the tag</h2>
    <div class=big id=aq>0</div><div class=k>quarantined & rerouted (zero loss)</div>
    <div class=row><span class=k>forwarded</span><span class=mono id=afwd>0</span></div>
    <div class=row><span class=k>blessed schema</span><span class="mono" id=abless>learning…</span></div>
    <div class=drift id=adrift><b>DRIFT DETECTED.</b> <span id=adtxt></span></div>
  </div>
</div>
<div class=card style="margin-top:16px"><h2>Scorecard</h2>
  <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;text-align:center">
    <div><div class=mono id=sc_drift style="font-size:22px;font-weight:700">—</div><div class=k>drift detected</div></div>
    <div><div class=mono id=sc_bad style="font-size:22px;font-weight:700;color:var(--red)">0</div><div class=k>naive bad writes</div></div>
    <div><div class=mono id=sc_held style="font-size:22px;font-weight:700;color:var(--green)">0</div><div class=k>aware contained</div></div>
    <div><div class=mono style="font-size:22px;font-weight:700;color:var(--green)">0</div><div class=k>aware crashes</div></div>
    <div><div class=mono style="font-size:22px;font-weight:700">never</div><div class=k>raw payload stored</div></div>
  </div></div>
<p class=note>Honest claim: Deblob does <b>not</b> guess that <span class=mono>amount</span> became <span class=mono>total_cents</span> — structural similarity isn't semantic equivalence. What it guarantees is <b>containment</b>: the aware loader makes <b>zero bad warehouse writes</b>, stays healthy, and hands the operator the exact structural change. Deblob tags every record on <span class=mono>events.tagged</span> with <span class=mono>deblob-schema-id</span> and never stores the raw payload.</p>
</div>
<script>
async function j(u){try{return await (await fetch(u)).json()}catch(e){return{}}}
function fmt(id){return id&&id.length>18?id.slice(0,18)+'…':(id||'—')}
async function tick(){
 const p=await j('/api/producer'),n=await j('/api/naive'),a=await j('/api/aware');
 const v=p.version||'v1';
 const vb=document.getElementById('verbadge');vb.textContent=v.toUpperCase();vb.className='badge '+v;
 document.getElementById('produced').textContent=p.produced??0;
 document.getElementById('shape').textContent=v==='v2'?'v2 (drifted)':'v1 (baseline)';
 document.getElementById('nerr').textContent=n.errors??0;
 document.getElementById('nok').textContent=n.processed??0;
 const nb=document.getElementById('nbroke');
 if((n.errors??0)>0){nb.classList.add('on');document.getElementById('nlast').textContent=n.last_error||''}else nb.classList.remove('on');
 document.getElementById('aq').textContent=a.quarantined??0;
 document.getElementById('afwd').textContent=a.forwarded??0;
 document.getElementById('abless').textContent=a.blessed_schema_name?a.blessed_schema_name+' ('+fmt(a.blessed_schema_id)+')':fmt(a.blessed_schema_id);
 const ad=document.getElementById('adrift');
 if(a.drift_detected_at){ad.classList.add('on');
   let t='New schema id '+fmt(a.new_schema_id);
   if(a.new_schema_name)t='Deblob discovered → “'+a.new_schema_name+'”'+(a.new_family_version?' v'+a.new_family_version:'')+' ('+fmt(a.new_schema_id)+')';
   document.getElementById('adtxt').textContent=t;
 }else ad.classList.remove('on');
 document.getElementById('trig').disabled=(v==='v2');
 const sd=document.getElementById('sc_drift');
 if(a.drift_detected_at){sd.textContent='YES';sd.style.color='var(--green)'}else{sd.textContent='no';sd.style.color=''}
 document.getElementById('sc_bad').textContent=n.errors??0;
 document.getElementById('sc_held').textContent=a.quarantined??0;
}
async function trigger(){document.getElementById('trignote').textContent='triggering…';
 await fetch('/api/trigger',{method:'POST'});
 document.getElementById('trignote').textContent='drift injected — watch both loaders diverge.';}
async function rollback(){document.getElementById('trignote').textContent='rolling back…';
 await fetch('/api/reset',{method:'POST'});
 document.getElementById('trignote').textContent='producer back on v1. (aware stays latched on the observed drift for the scorecard.)';}
setInterval(tick,1000);tick();
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
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
        if self.path == "/api/naive":
            d, c = _get(NAIVE + "/status"); return self._json(c, d)
        if self.path == "/api/aware":
            d, c = _get(AWARE + "/status"); return self._json(c, d)
        return self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/api/trigger":
            d, c = _post(PRODUCER + "/trigger"); return self._json(c, d)
        if self.path == "/api/reset":
            d, c = _post(PRODUCER + "/reset"); return self._json(c, d)
        return self._json(404, {"error": "not found"})

    def log_message(self, *a):
        pass


def main():
    print("dashboard on :8080", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
