#!/usr/bin/env python3
"""Deblob demo — drift producer.

Emits synthetic e-commerce "order" events to events.demo.orders on Deblob's
Redpanda. Holds a schema VERSION (v1 by default); a control endpoint flips it to
v2, injecting a breaking drift (field rename + type change + new nesting) that
Deblob discovers as a new schema and a naive consumer chokes on.

HTTP (:8080):  GET /state  POST /trigger  POST /reset  GET /healthz
Messaging:     confluent-kafka Producer -> BOOTSTRAP / TOPIC
"""
import json
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("BOOTSTRAP", "redpanda.deblob.svc.cluster.local:9092")
TOPIC = os.environ.get("TOPIC", "events.demo.orders")
RATE_HZ = float(os.environ.get("RATE_HZ", "3"))

CURRENCIES = ["EUR", "USD", "GBP", "PLN"]
NAMES = ["Ada Lovelace", "Alan Turing", "Grace Hopper", "Linus T", "Margaret H",
         "Ken Thompson", "Barbara Liskov", "Edsger D"]
METHODS = ["standard", "express", "pickup"]

_state = {"version": "v1", "produced": 0, "started_at": time.time(),
          "last_trigger_at": None}
_lock = threading.Lock()


def _order_v1(seq):
    """Baseline shape. customer_name: str, amount: float (major units)."""
    return {
        "order_id": f"ord-{seq:08d}",
        "customer_name": random.choice(NAMES),
        "amount": round(random.uniform(9.99, 499.99), 2),
        "currency": random.choice(CURRENCIES),
        "item_count": random.randint(1, 8),
        "placed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def _order_v2(seq):
    """Drifted shape. amount->total_cents (int), customer_name->customer{}
    (nesting), + shipping{} (new nested object). A clean breaking change."""
    return {
        "order_id": f"ord-{seq:08d}",
        "customer": {
            "id": f"cust-{random.randint(1000, 9999)}",
            "name": random.choice(NAMES),
        },
        "total_cents": random.randint(999, 49999),
        "currency": random.choice(CURRENCIES),
        "item_count": random.randint(1, 8),
        "placed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "shipping": {"method": random.choice(METHODS),
                     "eta_days": random.randint(1, 7)},
    }


def _produce_loop(producer):
    seq = 0
    interval = 1.0 / max(RATE_HZ, 0.1)
    while True:
        seq += 1
        with _lock:
            ver = _state["version"]
        rec = _order_v1(seq) if ver == "v1" else _order_v2(seq)
        try:
            producer.produce(TOPIC, key=rec["order_id"].encode(),
                             value=json.dumps(rec).encode())
            producer.poll(0)
            with _lock:
                _state["produced"] += 1
        except BufferError:
            producer.poll(0.5)
        except Exception as e:  # noqa: BLE001 — keep the loop alive
            print("produce error:", e, flush=True)
        time.sleep(interval)


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
        if self.path == "/state":
            with _lock:
                s = dict(_state)
            s["sample"] = (_order_v1(s["produced"]) if s["version"] == "v1"
                           else _order_v2(s["produced"]))
            s["v1_fields"] = ["order_id", "customer_name", "amount", "currency",
                              "item_count", "placed_at"]
            s["v2_fields"] = ["order_id", "customer{id,name}", "total_cents",
                              "currency", "item_count", "placed_at",
                              "shipping{method,eta_days}"]
            return self._send(200, s)
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/trigger":
            with _lock:
                _state["version"] = "v2"
                _state["last_trigger_at"] = time.time()
            print("DRIFT TRIGGERED -> v2", flush=True)
            return self._send(200, {"version": "v2"})
        if self.path == "/reset":
            with _lock:
                _state["version"] = "v1"
            print("RESET -> v1", flush=True)
            return self._send(200, {"version": "v1"})
        return self._send(404, {"error": "not found"})

    def log_message(self, *a):  # quiet
        pass


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP,
                         "linger.ms": 50, "client.id": "demo-drift-producer"})
    threading.Thread(target=_produce_loop, args=(producer,), daemon=True).start()
    print(f"producer -> {BOOTSTRAP} topic={TOPIC} rate={RATE_HZ}/s", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
