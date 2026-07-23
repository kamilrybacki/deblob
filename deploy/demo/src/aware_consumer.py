#!/usr/bin/env python3
"""Deblob demo — DEBLOB-AWARE consumer.

The loader that reads Deblob's tag. It watches the deblob-schema-id header on
events.tagged for the demo source, learns the "blessed" baseline schema id, and
the instant a record arrives with a DIFFERENT id it declares drift, routes that
record to a quarantine tally (never crashes), and asks the Deblob API what the
new schema is — so downstream sees "source X changed shape -> new schema «name»"
instead of silently ingesting garbage.

HTTP (:8080):  GET /status  GET /healthz
"""
import json
import os
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Consumer

BOOTSTRAP = os.environ.get("BOOTSTRAP", "redpanda.deblob.svc.cluster.local:9092")
TAGGED_TOPIC = os.environ.get("TAGGED_TOPIC", "events.tagged")
ORIGIN_PREFIX = os.environ.get("ORIGIN_PREFIX", "events.demo.orders")
GROUP = os.environ.get("GROUP", "demo-aware-consumer")
DEBLOB_API = os.environ.get("DEBLOB_API", "http://deblob-mgmt.deblob.svc.cluster.local:9615")
DEBLOB_TOKEN = os.environ.get("DEBLOB_TOKEN", "")
BLESS_AFTER = int(os.environ.get("BLESS_AFTER", "5"))

_st = {"forwarded": 0, "quarantined": 0, "blessed_schema_id": None,
       "blessed_schema_name": None, "blessed_family_version": None,
       "drift_detected_at": None, "new_schema_id": None, "new_schema_name": None,
       "new_family_version": None, "seen_ids": {}, "started_at": time.time()}
_lock = threading.Lock()


def _hdr(headers, key):
    for k, v in (headers or []):
        if k == key and v is not None:
            return v.decode("utf-8", "replace")
    return ""


def _api(path):
    """Best-effort GET on the Deblob API. Returns dict or None."""
    if not DEBLOB_TOKEN:
        return None
    req = urllib.request.Request(DEBLOB_API + path,
                                 headers={"Authorization": f"Bearer {DEBLOB_TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=4) as r:
            return json.loads(r.read())
    except Exception:  # noqa: BLE001 — display-only, never fatal
        return None


def _resolve(schema_id):
    """schema id -> (name, family_version). unresolved/cand ids may not resolve
    yet (discovery in flight); that's fine — we still know drift happened."""
    if not schema_id or schema_id in ("unresolved", "malformed", "tombstone"):
        return (schema_id, None)
    d = _api(f"/api/v1/schemas/{schema_id}")
    if not d:
        return (None, None)
    d = d.get("data", d)  # the API wraps records in {"data": {...}}
    prov = d.get("provenance") or {}
    # Deblob stores the accepted name in provenance.label (name_meta.heuristic is
    # the raw heuristic/SLM proposal); the top-level `name` stays null until a
    # human accepts. label is the right display name.
    name = (d.get("name") or prov.get("label")
            or (prov.get("name_meta") or {}).get("heuristic"))
    fam = d.get("version") or d.get("family_version")
    return (name, fam)


def _consume_loop():
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP,
                  "auto.offset.reset": "latest", "enable.auto.commit": True})
    c.subscribe([TAGGED_TOPIC])
    print(f"aware consumer <- {TAGGED_TOPIC} origin~{ORIGIN_PREFIX}", flush=True)
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        h = msg.headers()
        if not _hdr(h, "deblob-origin").startswith(ORIGIN_PREFIX):
            continue
        sid = _hdr(h, "deblob-schema-id")
        with _lock:
            _st["seen_ids"][sid] = _st["seen_ids"].get(sid, 0) + 1
            blessed = _st["blessed_schema_id"]
            # Learn the baseline: bless only a PROMOTED schema (sch_...), never a
            # transient candidate/unresolved tag — so the one-time candidate->schema
            # promotion of the baseline shape is not mistaken for drift. Until the
            # source promotes we simply forward (warming up).
            if blessed is None:
                if sid.startswith("sch_") and _st["seen_ids"][sid] >= BLESS_AFTER:
                    _st["blessed_schema_id"] = sid
                    name, fam = _resolve(sid)
                    _st["blessed_schema_name"] = name
                    _st["blessed_family_version"] = fam
                _st["forwarded"] += 1
                continue
            if sid == blessed:
                _st["forwarded"] += 1
                continue
            # DRIFT: a schema id we've never blessed.
            _st["quarantined"] += 1
            if _st["drift_detected_at"] is None:
                _st["drift_detected_at"] = time.time()
                _st["new_schema_id"] = sid
                name, fam = _resolve(sid)
                _st["new_schema_name"] = name
                _st["new_family_version"] = fam
                print(f"DRIFT: {sid} != blessed {blessed}", flush=True)
            elif _st["new_schema_name"] is None and sid == _st["new_schema_id"]:
                # discovery may have promoted it since — retry the name resolve.
                name, fam = _resolve(sid)
                if name:
                    _st["new_schema_name"] = name
                    _st["new_family_version"] = fam


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/healthz":
            return self._send(200, {"ok": True})
        if self.path == "/status":
            with _lock:
                s = dict(_st)
                s["seen_ids"] = dict(s["seen_ids"])
            return self._send(200, s)
        return self._send(404, {"error": "not found"})

    def log_message(self, *a):
        pass


def main():
    threading.Thread(target=_consume_loop, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
