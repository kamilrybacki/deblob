#!/usr/bin/env python3
"""Deblob demo — schema-evolution NORMALIZER.

Consumes events.tagged (filtered to the demo order source), reads the
deblob-schema-id header Deblob stamped on each record, and keeps a stable
downstream contract alive across arbitrary upstream drift — BOTH directions
(v1 -> v2 -> back to v1 -> any new shape).

How it works
------------
It maintains an ACCRETING CANONICAL SUPERSET: the first blessed shape seeds the
"core" contract; any later shape that carries fields the core doesn't have grows
the canonical with ADDITIVE (nullable) fields. The core is never renamed or
removed — that is the backwards-compat guarantee.

For EVERY distinct incoming shape it computes a TRANSFORM (via _classify): for
each canonical field it records WHERE to read that field from this shape.
  * identity / provable-rename  -> read straight through
  * additive (shape-only field) -> accrete into canonical, read through
  * unit/type change (amount <-> total_cents) -> HELD-PENDING with a proposed
    conversion (e.g. total_cents/100). Emitting is BLOCKED until a human
    approves, so downstream never sees a wrong core value.

normalize() rebuilds each record into the canonical shape and PRODUCES it to
events.demo.orders.normalized. If any CORE field is held-pending (or missing) it
HOLDS the record instead of emitting a broken contract. On /approve the pending
conversion is activated and every held record is flushed.

HTTP (:8080):  GET /status   POST /approve   GET /healthz
"""
import json
import os
import re
import threading
import time
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("BOOTSTRAP", "redpanda.deblob.svc.cluster.local:9092")
TAGGED_TOPIC = os.environ.get("TAGGED_TOPIC", "events.tagged")
NORMALIZED_TOPIC = os.environ.get("NORMALIZED_TOPIC", "events.demo.orders.normalized")
ORIGIN_PREFIX = os.environ.get("ORIGIN_PREFIX", "events.demo.orders")
GROUP = os.environ.get("GROUP", "demo-normalizer")
DEBLOB_API = os.environ.get("DEBLOB_API", "http://deblob-mgmt.deblob.svc.cluster.local:9615")
DEBLOB_TOKEN = os.environ.get("DEBLOB_TOKEN", "")

# Tokens that signal a SEMANTIC change (unit/scale/encoding) — a same-type field
# carrying one of these is NOT a provable rename, it needs a human (amount->cents).
UNIT_TOKENS = {"cents", "ms", "millis", "micros", "ns", "kb", "mb", "gb", "bytes",
               "kbps", "mbps", "pct", "percent", "ratio", "bps", "epoch", "unix",
               "utc", "sec", "secs", "min", "hours", "days", "usd", "eur", "id"}

# Normalize scalar-type vocabularies so schema-derived and payload-derived leaves
# compare on a common footing (float != integer, but "i64"=="integer").
_TYPE_MAP = {
    "i8": "integer", "i16": "integer", "i32": "integer", "i64": "integer",
    "int": "integer", "integer": "integer", "long": "integer",
    "u8": "integer", "u16": "integer", "u32": "integer", "u64": "integer",
    "f32": "float", "f64": "float", "float": "float", "double": "float",
    "number": "float", "decimal": "float",
    "str": "string", "string": "string", "utf8": "string", "text": "string",
    "bool": "boolean", "boolean": "boolean", "null": "null",
}


def _norm_type(t):
    return _TYPE_MAP.get(str(t).lower(), str(t).lower())


# ---- shared state ----------------------------------------------------------
# CANONICAL: field -> {ty, kind: "core"|"additive"}  (ACCRETING superset)
CANONICAL = OrderedDict()
CANON_PATH = {}          # canonical field -> canonical reference path ("$.amount")
PATH_TO_FIELD = {}       # canonical reference path -> canonical field
SHAPES = {}              # schema_id -> {leaf path: scalar type}
TRANSFORMS = {}          # schema_id -> {canonical field: {src, kind, conversion}}
PENDING = OrderedDict()  # canonical field -> {canonical_field, from_shape_field, reason, conversion}
APPROVED = set()         # canonical fields whose conversion the operator activated
HELD = []                # [(schema_id, payload)] parked while a core field is pending

_counts = {"normalized": 0}
_per_shape = {}
_started_at = time.time()
_producer = None
_lock = threading.Lock()


# ---- Deblob API ------------------------------------------------------------
def _api(path, method="GET", body=None):
    if not DEBLOB_TOKEN:
        return None
    import urllib.request
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        DEBLOB_API + path, data=data, method=method,
        headers={"Authorization": f"Bearer {DEBLOB_TOKEN}",
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=6) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except Exception as e:  # noqa: BLE001 — degrades, never crashes
        return {"_error": str(e)}


def _schema(schema_id):
    d = _api(f"/api/v1/schemas/{schema_id}")
    return (d or {}).get("data") if d and "_error" not in d else None


# ---- canonical -> leaf {path: scalar_type} ---------------------------------
def _leaves(canonical, prefix="$"):
    """Walk Deblob's canonical {types, children, elem} tree into leaf path->type."""
    try:
        node = json.loads(canonical) if isinstance(canonical, str) else canonical
    except Exception:  # noqa: BLE001
        return {}
    out = {}

    def walk(n, path):
        if not isinstance(n, dict):
            return
        types = n.get("types") or []
        children = n.get("children")
        elem = n.get("elem")
        if isinstance(children, dict) and children:
            for k, v in children.items():
                walk(v, f"{path}.{k}")
        elif elem:
            walk(elem, f"{path}[]")
        else:
            scalar = next((t for t in types if t != "null"), types[0] if types else "any")
            out[path] = scalar

    walk(node, prefix)
    return out


def _tokens(path):
    leaf = path.rsplit(".", 1)[-1].replace("[]", "")
    parts = re.split(r"[_\.]", path.lstrip("$."))
    toks = set()
    for p in parts + [leaf]:
        for t in re.findall(r"[a-z]+|[0-9]+", re.sub(r"([a-z])([A-Z])", r"\1_\2", p).lower()):
            toks.add(t)
    return toks


# ---- the computed shape-map transform (auto-approve-safe) ------------------
def _classify(v1, v2):
    """v1/v2 = {path: type}. Returns auto / additive / needs_approval field-maps."""
    common = {p for p in v1 if p in v2 and v1[p] == v2[p]}
    removed = {p: v1[p] for p in v1 if p not in common}
    added = {p: v2[p] for p in v2 if p not in common}

    auto = [{"from": p, "to": p, "type": v1[p], "kind": "identity"} for p in sorted(common)]
    needs = []
    used_added = set()

    for rp in sorted(removed):
        rtype, rtok = removed[rp], _tokens(rp)
        # best same-type candidate among the added fields, by token overlap
        best, best_ov = None, -1.0
        for ap in added:
            if ap in used_added:
                continue
            atok = _tokens(ap)
            ov = len(rtok & atok) / max(1, len(rtok | atok))
            same_type = added[ap] == rtype
            # prefer same nesting depth so amount($.amount) matches total_cents
            # ($.total_cents), not shipping.eta_days ($.shipping.eta_days).
            depth_pen = 0.05 * abs(rp.count(".") - ap.count("."))
            score = ov + (0.15 if same_type else 0) - depth_pen
            if score > best_ov:
                best, best_ov = ap, score
        if best is None:
            needs.append({"from": rp, "to": None, "type": rtype,
                          "reason": "field removed with no matching target"})
            continue
        atok, atype = _tokens(best), added[best]
        ov = len(rtok & atok) / max(1, len(rtok | atok))
        unit_shift = (atok - rtok) & UNIT_TOKENS or (rtok - atok) & UNIT_TOKENS
        if atype == rtype and ov >= 0.5 and not unit_shift:
            auto.append({"from": rp, "to": best, "type": rtype, "kind": "rename"})
            used_added.add(best)
        else:
            reason = ("type changed %s->%s" % (rtype, atype) if atype != rtype
                      else "unit/scale change (%s)" % ",".join(sorted(unit_shift))
                      if unit_shift else "ambiguous rename (low name overlap)")
            needs.append({"from": rp, "to": best, "type": rtype,
                          "to_type": atype, "reason": reason})
            used_added.add(best)

    additive = [{"path": ap, "type": added[ap], "kind": "additive"}
                for ap in sorted(added) if ap not in used_added]
    return {"auto": auto, "additive": additive, "needs_approval": needs, "approved": []}


# ---- normalization state machine -------------------------------------------
def _flatten(path):
    """'$.customer.id' -> 'customer_id', '$.shipping.method' -> 'shipping_method'."""
    return path.lstrip("$.").replace("[]", "").replace(".", "_")


def _leaf(path):
    return (path or "").rsplit(".", 1)[-1].replace("[]", "").lstrip("$.")


def _conversion(field, src_path):
    """Propose a scale conversion when the shape field carries a *cents scale
    token the canonical field lacks: value/100. Minimal generalization."""
    if not src_path:
        return None
    leaf = _leaf(src_path).lower()
    if ("cents" in re.split(r"[_]", leaf) or leaf.endswith("cents")) and "cents" not in field.lower():
        return {"op": "div", "by": 100, "desc": "÷100"}
    return None


def _apply_conversion(conv, val):
    if conv and conv.get("op") == "div" and isinstance(val, (int, float)) and not isinstance(val, bool):
        return val / conv["by"]
    return val


def _leaves_from_payload(payload, prefix="$"):
    """Fallback when the Deblob schema API is unavailable — derive leaves from the
    actual JSON so the demo still runs without a token."""
    out = {}

    def walk(n, path):
        if isinstance(n, dict):
            for k, v in n.items():
                walk(v, f"{path}.{k}")
        elif isinstance(n, list):
            if n:
                walk(n[0], f"{path}[]")
        else:
            if isinstance(n, bool):
                out[path] = "boolean"
            elif isinstance(n, int):
                out[path] = "integer"
            elif isinstance(n, float):
                out[path] = "float"
            elif n is None:
                out[path] = "null"
            else:
                out[path] = "string"

    walk(payload if isinstance(payload, (dict, list)) else {}, prefix)
    return out


def _canon_leaves():
    return {CANON_PATH[f]: CANONICAL[f]["ty"] for f in CANONICAL}


def _seed(shape_leaves):
    """Seed the CORE canonical from the first blessed shape's leaves."""
    CANONICAL.clear()
    CANON_PATH.clear()
    PATH_TO_FIELD.clear()
    for path, ty in shape_leaves.items():
        f = _flatten(path)
        CANONICAL[f] = {"ty": _norm_type(ty), "kind": "core"}
        CANON_PATH[f] = path
        PATH_TO_FIELD[path] = f


def _build_transform(shape_leaves):
    """Compute this shape's transform against the current canonical; ACCRETE any
    additive fields into the canonical. Returns {canonical field: {src, kind,
    conversion}}."""
    shape_leaves = {p: _norm_type(t) for p, t in shape_leaves.items()}
    cls = _classify(_canon_leaves(), shape_leaves)
    t = {}
    for m in cls["auto"]:
        f = PATH_TO_FIELD.get(m["from"])
        if f:
            t[f] = {"src": m["to"], "kind": m["kind"], "conversion": None}
    for m in cls["needs_approval"]:
        f = PATH_TO_FIELD.get(m["from"])
        if not f:
            continue
        conv = _conversion(f, m.get("to"))
        t[f] = {"src": m.get("to"), "kind": "held", "conversion": conv}
        PENDING[f] = {"canonical_field": f,
                      "from_shape_field": _leaf(m.get("to")) if m.get("to") else None,
                      "reason": m.get("reason"), "conversion": conv}
    for m in cls["additive"]:
        f = _flatten(m["path"])
        if f not in CANONICAL:
            CANONICAL[f] = {"ty": _norm_type(m["type"]), "kind": "additive"}
            CANON_PATH[f] = m["path"]
            PATH_TO_FIELD[m["path"]] = f
        t[f] = {"src": m["path"], "kind": "additive", "conversion": None}
    return t


def read_path(payload, path):
    """Walk a dotted json path ('$.a.b') into payload; return value or None."""
    if not path:
        return None
    cur = payload
    for part in path.lstrip("$.").split("."):
        if "[]" in part:
            return None
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def _is_pending(field, tmeta):
    return tmeta.get("kind") == "held" and field not in APPROVED


def normalize(payload, schema_id):
    """Build the canonical record for this payload+shape, or None to HOLD it.

    HOLD (return None) if any CORE canonical field is held-pending-unapproved or
    absent — never emit a record with a missing/wrong core field."""
    t = TRANSFORMS.get(schema_id)
    if t is None:
        return None
    rec = OrderedDict()
    for field, meta in CANONICAL.items():
        core = meta["kind"] == "core"
        m = t.get(field)
        if m is None:
            if core:
                return None
            rec[field] = None
            continue
        if _is_pending(field, m):
            if core:
                return None
            rec[field] = None
            continue
        val = read_path(payload, m.get("src")) if m.get("src") else None
        if val is None:
            if core:
                return None
            rec[field] = None
            continue
        conv = m.get("conversion")
        if conv and (m.get("kind") != "held" or field in APPROVED):
            val = _apply_conversion(conv, val)
        rec[field] = val
    return rec


# ---- consume loop ----------------------------------------------------------
def _hdr(headers, key):
    for k, v in (headers or []):
        if k == key and v is not None:
            return v.decode("utf-8", "replace")
    return ""


def _shape_leaves(schema_id):
    rec = _schema(schema_id)
    return _leaves((rec or {}).get("canonical")) if rec else {}


def _produce(rec):
    p = _producer
    if p is None:
        return
    try:
        p.produce(NORMALIZED_TOPIC,
                  key=str(rec.get("order_id", "")).encode(),
                  value=json.dumps(rec).encode())
        p.poll(0)
    except BufferError:
        p.poll(0.5)
    except Exception as e:  # noqa: BLE001 — never crash the loop
        print("normalized produce error:", e, flush=True)


def _handle(sid, payload):
    """Process one record: id the shape, (accrete +) build its transform,
    normalize, then produce or hold. Best-effort; never raises."""
    rec = None
    with _lock:
        if sid not in SHAPES:
            leaves = _shape_leaves(sid) or _leaves_from_payload(payload)
            if leaves:
                SHAPES[sid] = {p: _norm_type(t) for p, t in leaves.items()}
        if not CANONICAL:
            # seed ONLY from a promoted (blessed) sch_ id; ignore cand_/unresolved.
            if sid.startswith("sch_") and SHAPES.get(sid):
                _seed(SHAPES[sid])
                TRANSFORMS[sid] = _build_transform(SHAPES[sid])
            else:
                return  # no contract yet — nothing to normalize against
        if sid not in TRANSFORMS:
            leaves = SHAPES.get(sid) or _leaves_from_payload(payload)
            TRANSFORMS[sid] = _build_transform(leaves)
        rec = normalize(payload, sid)
        if rec is not None:
            _counts["normalized"] += 1
            _per_shape[sid] = _per_shape.get(sid, 0) + 1
        else:
            HELD.append((sid, payload))
    if rec is not None:
        _produce(rec)


def _consume_loop():
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP,
                  "auto.offset.reset": "latest", "enable.auto.commit": True})
    c.subscribe([TAGGED_TOPIC])
    print(f"normalizer <- {TAGGED_TOPIC} origin~{ORIGIN_PREFIX} -> {NORMALIZED_TOPIC}", flush=True)
    while True:
        try:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            h = msg.headers()
            if not _hdr(h, "deblob-origin").startswith(ORIGIN_PREFIX):
                continue
            sid = _hdr(h, "deblob-schema-id")
            try:
                payload = json.loads(msg.value())
            except Exception:  # noqa: BLE001 — non-JSON payloads are ignored
                continue
            _handle(sid, payload)
        except Exception as e:  # noqa: BLE001 — the loop must never die
            print("normalizer loop error:", e, flush=True)


def _approve():
    """Activate the first pending conversion, un-hold that field everywhere, then
    flush the held backlog through the now-complete contract."""
    with _lock:
        pend = [f for f in PENDING if f not in APPROVED]
        if not pend:
            return {"approved": None, "flushed": 0}
        field = pend[0]
        APPROVED.add(field)
        flushed, keep = 0, []
        for sid, payload in HELD:
            r = normalize(payload, sid)
            if r is not None:
                _counts["normalized"] += 1
                _per_shape[sid] = _per_shape.get(sid, 0) + 1
                flushed += 1
                to_emit = (sid, r)
                keep.append(("__emit__", to_emit))
            # records still incomplete (other pending fields) are dropped on flush
        HELD.clear()
    # produce outside the lock
    for tag, item in keep:
        if tag == "__emit__":
            _produce(item[1])
    return {"approved": field, "flushed": flushed}


def _status():
    with _lock:
        canonical = {f: dict(m) for f, m in CANONICAL.items()}
        transforms = {}
        for sid, t in TRANSFORMS.items():
            transforms[sid] = {
                f: {"src": m.get("src"), "kind": m.get("kind"),
                    "conversion": m.get("conversion"),
                    "pending": _is_pending(f, m)}
                for f, m in t.items()}
        pending_approvals = [dict(PENDING[f]) for f in PENDING if f not in APPROVED]
        return {
            "canonical": canonical,
            "shapes_seen": list(SHAPES.keys()),
            "transforms": transforms,
            "pending_approvals": pending_approvals,
            "normalized": _counts["normalized"],
            "held": len(HELD),
            "per_shape": dict(_per_shape),
            "started_at": _started_at,
        }


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
            return self._send(200, _status())
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/approve":
            return self._send(200, _approve())
        return self._send(404, {"error": "not found"})

    def log_message(self, *a):
        pass


def main():
    global _producer
    _producer = Producer({"bootstrap.servers": BOOTSTRAP,
                          "linger.ms": 50, "client.id": "demo-normalizer"})
    threading.Thread(target=_consume_loop, daemon=True).start()
    print("normalizer on :8080", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
