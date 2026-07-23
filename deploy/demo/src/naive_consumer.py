#!/usr/bin/env python3
"""Deblob demo — NAIVE consumer.

The "warehouse loader" nobody updated. Reads events.tagged, filters to the demo
source, and parses each payload ASSUMING the v1 shape (customer_name: str,
amount: float). When the producer drifts to v2 those fields are gone/renamed, so
parsing raises and the record is a silent loss — exactly the failure Deblob is
meant to catch. It has no idea a schema even changed.

HTTP (:8080):  GET /status  GET /healthz
"""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Consumer

BOOTSTRAP = os.environ.get("BOOTSTRAP", "redpanda.deblob.svc.cluster.local:9092")
TAGGED_TOPIC = os.environ.get("TAGGED_TOPIC", "events.tagged")
ORIGIN_PREFIX = os.environ.get("ORIGIN_PREFIX", "events.demo.orders")
GROUP = os.environ.get("GROUP", "demo-naive-consumer")

_st = {"processed": 0, "errors": 0, "last_error": None,
       "last_ok_at": None, "last_error_at": None, "started_at": time.time()}
_lock = threading.Lock()


def _origin(headers):
    for k, v in (headers or []):
        if k == "deblob-origin" and v is not None:
            return v.decode("utf-8", "replace")
    return ""


def _process(payload_bytes):
    """v1 assumptions — deliberately brittle. Raises on the v2 shape."""
    rec = json.loads(payload_bytes)
    name = rec["customer_name"]          # v2: KeyError (renamed -> customer{})
    amount = float(rec["amount"])        # v2: KeyError (renamed -> total_cents)
    if not isinstance(name, str):
        raise TypeError("customer_name not a string")
    return name, amount


def _consume_loop():
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP,
                  "auto.offset.reset": "latest", "enable.auto.commit": True})
    c.subscribe([TAGGED_TOPIC])
    print(f"naive consumer <- {TAGGED_TOPIC} origin~{ORIGIN_PREFIX}", flush=True)
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        if not _origin(msg.headers()).startswith(ORIGIN_PREFIX):
            continue
        try:
            _process(msg.value())
            with _lock:
                _st["processed"] += 1
                _st["last_ok_at"] = time.time()
        except Exception as e:  # noqa: BLE001 — the whole point: it breaks
            with _lock:
                _st["errors"] += 1
                _st["last_error"] = f"{type(e).__name__}: {e}"
                _st["last_error_at"] = time.time()


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
                return self._send(200, dict(_st))
        return self._send(404, {"error": "not found"})

    def log_message(self, *a):
        pass


def main():
    threading.Thread(target=_consume_loop, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
