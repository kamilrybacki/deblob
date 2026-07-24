#!/usr/bin/env python3
"""Deblob demo — DEBLOB-AWARE consumer + AMENDER.

Watches the deblob-schema-id header on events.tagged for the demo source, learns
the blessed baseline schema, and on drift it does two amendments:

  1. SCHEMA amendment (registry write): promotes the drifted v2 candidate into the
     blessed schema's EXISTING family, so the registry shows Orders v1 -> v2 as one
     evolving family (not a disconnected new schema).

  2. DATA amendment (computed, deterministic): diffs the v1 and v2 canonical shapes
     and classifies each field-map — identity / provable-rename / additive (all
     auto-applied) vs ambiguous type-or-unit change (parked for human approval).
     This is the auto-approve-safe policy: Deblob amends what it can PROVE and only
     asks a human about genuinely ambiguous changes (e.g. amount -> total_cents).

Never rewrites payloads on a guess. HTTP: GET /status  POST /approve  GET /healthz
"""
import json
import os
import re
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

# Tokens that signal a SEMANTIC change (unit/scale/encoding) — a same-type field
# carrying one of these is NOT a provable rename, it needs a human (amount->cents).
UNIT_TOKENS = {"cents", "ms", "millis", "micros", "ns", "kb", "mb", "gb", "bytes",
               "kbps", "mbps", "pct", "percent", "ratio", "bps", "epoch", "unix",
               "utc", "sec", "secs", "min", "hours", "days", "usd", "eur", "id"}

_st = {"forwarded": 0, "quarantined": 0, "blessed_schema_id": None,
       "blessed_schema_name": None, "blessed_family_version": None,
       "drift_detected_at": None, "new_schema_id": None, "new_schema_name": None,
       "new_family_version": None, "seen_ids": {}, "started_at": time.time(),
       "schema_amend": None, "data_amend": None,
       "blessed_leaves": None, "new_leaves": None}
_blessed = {"family_id": None, "schema_id": None, "leaves": None}
_lock = threading.Lock()
_amend_started = {"v": False}


# ---- Deblob API ------------------------------------------------------------
def _api(path, method="GET", body=None):
    if not DEBLOB_TOKEN:
        return None
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        DEBLOB_API + path, data=data, method=method,
        headers={"Authorization": f"Bearer {DEBLOB_TOKEN}",
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=6) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except Exception as e:  # noqa: BLE001 — the amend flow degrades, never crashes
        return {"_error": str(e)}


def _schema(schema_id):
    d = _api(f"/api/v1/schemas/{schema_id}")
    return (d or {}).get("data") if d and "_error" not in d else None


def _resolve_name(rec):
    prov = (rec or {}).get("provenance") or {}
    return (rec or {}).get("name") or prov.get("label") \
        or (prov.get("name_meta") or {}).get("heuristic")


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


# ---- the computed v1 -> v2 transform (auto-approve-safe) --------------------
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


# ---- amend on drift --------------------------------------------------------
def _run_amendments(v2_id):
    """One-shot on first drift: schema amendment (version into family) + computed
    data amendment. Best-effort — never raises into the consumer loop."""
    fam = _blessed["family_id"]
    v1_leaves = _blessed["leaves"] or {}

    # 1) SCHEMA amendment: promote the v2 candidate into the blessed family.
    schema_amend = {"family_id": fam, "from_version": _blessed_version(),
                    "to_version": None, "v2_schema_id": None, "ok": False, "note": ""}
    v2_rec = None
    if v2_id.startswith("cand_") and fam:
        res = _api(f"/api/v1/candidates/{v2_id}/promote", "POST",
                   {"family": {"existing": fam}, "name": "Orders",
                    "reason": "demo drift auto-amend: version into existing family"})
        rec = (res or {}).get("data") if res and "_error" not in res else None
        if rec:
            v2_rec = rec
            schema_amend.update(ok=True, v2_schema_id=rec.get("schema_id"),
                                to_version=rec.get("version"))
        else:
            schema_amend["note"] = (res or {}).get("_error", "promote failed") if res else "no api"
    elif v2_id.startswith("sch_"):
        v2_rec = _schema(v2_id)
        if v2_rec:
            schema_amend.update(ok=(v2_rec.get("family_id") == fam),
                                v2_schema_id=v2_id, to_version=v2_rec.get("version"),
                                note="already a schema")
    with _lock:
        _st["schema_amend"] = schema_amend

    # 2) DATA amendment: diff v1 vs v2 canonical, classify field-maps.
    v2_leaves = _leaves(v2_rec.get("canonical")) if v2_rec else {}
    if v1_leaves and v2_leaves:
        data_amend = _classify(v1_leaves, v2_leaves)
    else:
        data_amend = {"auto": [], "additive": [], "needs_approval": [],
                      "approved": [], "note": "canonical unavailable"}
    with _lock:
        _st["data_amend"] = data_amend
        _st["new_leaves"] = v2_leaves


def _blessed_version():
    rec = _schema(_blessed["schema_id"]) if _blessed["schema_id"] else None
    return (rec or {}).get("version", 1)


# ---- consume loop ----------------------------------------------------------
def _hdr(headers, key):
    for k, v in (headers or []):
        if k == key and v is not None:
            return v.decode("utf-8", "replace")
    return ""


def _consume_loop():
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP,
                  "auto.offset.reset": "latest", "enable.auto.commit": True})
    c.subscribe([TAGGED_TOPIC])
    print(f"aware amender <- {TAGGED_TOPIC} origin~{ORIGIN_PREFIX}", flush=True)
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
            if blessed is None:
                # bless only a PROMOTED schema (sch_), and cache its family + leaves.
                if sid.startswith("sch_") and _st["seen_ids"][sid] >= BLESS_AFTER:
                    rec = _schema(sid)
                    _st["blessed_schema_id"] = sid
                    _st["blessed_schema_name"] = _resolve_name(rec)
                    _st["blessed_family_version"] = (rec or {}).get("version")
                    _blessed["schema_id"] = sid
                    _blessed["family_id"] = (rec or {}).get("family_id")
                    _blessed["leaves"] = _leaves((rec or {}).get("canonical"))
                    _st["blessed_leaves"] = _blessed["leaves"]
                _st["forwarded"] += 1
                continue
            if sid == blessed:
                _st["forwarded"] += 1
                continue
            # DRIFT
            _st["quarantined"] += 1
            if _st["drift_detected_at"] is None:
                _st["drift_detected_at"] = time.time()
                _st["new_schema_id"] = sid
            # Amend once, on the first PROMOTABLE drift tag (cand_/sch_) — the very
            # first drift record may be "unresolved" before the candidate forms.
            fire = (not _amend_started["v"]
                    and (sid.startswith("cand_") or sid.startswith("sch_")))
            if fire:
                _amend_started["v"] = True
                _st["new_schema_id"] = sid
        # run amendments once, OUTSIDE the lock (API calls)
        if fire:
            threading.Thread(target=_run_amendments, args=(sid,), daemon=True).start()


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
                s = dict(_st)
                s["seen_ids"] = dict(s["seen_ids"])
            return self._send(200, s)
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        # human approves a parked ambiguous field-map -> moves it to approved.
        if self.path == "/approve":
            with _lock:
                da = _st.get("data_amend")
                if da and da.get("needs_approval"):
                    moved = da["needs_approval"].pop(0)
                    moved["approved_by"] = "operator"
                    da.setdefault("approved", []).append(moved)
                    return self._send(200, {"approved": moved,
                                            "remaining": len(da["needs_approval"])})
            return self._send(200, {"approved": None, "remaining": 0})
        return self._send(404, {"error": "not found"})

    def log_message(self, *a):
        pass


def main():
    threading.Thread(target=_consume_loop, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
