#!/usr/bin/env python3
"""Deblob demo — mock downstream ETL (the contract consumer).

Reads ONLY events.demo.orders.normalized — never the raw drifting source — and
validates the ORIGINAL v1 contract the ETL was built against:

    order_id:str  customer_name:str  amount:(int|float)  currency:str
    item_count:(int|float)  placed_at:str

Because the normalizer only ever emits COMPLETE canonical records (and holds the
incomplete ones), this service STAYS GREEN (errors == 0) through drift in BOTH
directions. That is the whole point: the pipeline downstream of Deblob is never
broken by an upstream schema change.

HTTP (:8080):  GET /status   GET /healthz
"""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Consumer

BOOTSTRAP = os.environ.get("BOOTSTRAP", "redpanda.deblob.svc.cluster.local:9092")
NORMALIZED_TOPIC = os.environ.get("NORMALIZED_TOPIC", "events.demo.orders.normalized")
GROUP = os.environ.get("GROUP", "demo-etl")

_st = {"processed": 0, "errors": 0, "last_error": None, "last_ok_at": None,
       "distinct_orders": 0, "started_at": time.time()}
_seen = set()
_lock = threading.Lock()

# The immutable contract this ETL was written for.
_CONTRACT = {
    "order_id": (str,),
    "customer_name": (str,),
    "amount": (int, float),
    "currency": (str,),
    "item_count": (int, float),
    "placed_at": (str,),
}


def _validate(rec):
    """Raise on any contract violation — missing or mistyped core field."""
    if not isinstance(rec, dict):
        raise TypeError("record is not an object")
    for field, types in _CONTRACT.items():
        if field not in rec or rec[field] is None:
            raise KeyError(f"missing core field '{field}'")
        val = rec[field]
        if isinstance(val, bool) or not isinstance(val, types):
            raise TypeError(f"'{field}' expected {'/'.join(t.__name__ for t in types)}, "
                            f"got {type(val).__name__}")
    return rec["order_id"]


def _consume_loop():
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP,
                  "auto.offset.reset": "latest", "enable.auto.commit": True})
    c.subscribe([NORMALIZED_TOPIC])
    print(f"etl <- {NORMALIZED_TOPIC}", flush=True)
    while True:
        try:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                rec = json.loads(msg.value())
                oid = _validate(rec)
                with _lock:
                    _st["processed"] += 1
                    _st["last_ok_at"] = time.time()
                    if oid not in _seen:
                        _seen.add(oid)
                        _st["distinct_orders"] = len(_seen)
            except Exception as e:  # noqa: BLE001 — a broken contract would land here
                with _lock:
                    _st["errors"] += 1
                    _st["last_error"] = f"{type(e).__name__}: {e}"
        except Exception as e:  # noqa: BLE001 — the loop must never die
            print("etl loop error:", e, flush=True)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

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
                return self._send(200, dict(_st))
        return self._send(404, {"error": "not found"})

    def log_message(self, *a):
        pass


def main():
    threading.Thread(target=_consume_loop, daemon=True).start()
    print("etl on :8080", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
