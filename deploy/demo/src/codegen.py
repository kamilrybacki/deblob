#!/usr/bin/env python3
"""Deblob demo — contract codegen.

Consumes the normalizer's live CANONICAL SUPERSET (its /status) and continuously
regenerates, the moment the contract changes:

  1. a JSON Schema (draft 2020-12) — the standard interchange artifact,
  2. a Pydantic v2 model — via the real `datamodel-code-generator` tool run over
     that JSON Schema (falls back to a direct generator if the tool is absent),
  3. SQL migrations — a CREATE TABLE, then one ADDITIVE `ALTER TABLE ADD COLUMN`
     per field the canonical accretes (nullable → backwards-compatible by design).

This shows the whole loop: Deblob discovers/normalizes the shape → emits a
machine-consumable contract → downstream data-model classes + DB migrations are
generated automatically, and evolve without breaking backwards compatibility.

HTTP (:8080):  GET /status  GET /schema.json  GET /models.py  GET /migrations.sql  GET /healthz
"""
import json
import os
import subprocess
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NORMALIZER = os.environ.get("NORMALIZER_URL", "http://demo-normalizer.deblob-demo.svc.cluster.local:8080")
TABLE = os.environ.get("TABLE", "orders")
MODEL = os.environ.get("MODEL", "Order")
POLL = float(os.environ.get("POLL", "3"))

# canonical scalar type -> (JSON Schema type, SQL type, python/pydantic hint)
JS = {"string": "string", "number": "number", "integer": "integer",
      "float": "number", "double": "number", "int": "integer",
      "boolean": "boolean", "bool": "boolean", "object": "object", "array": "array"}
SQL = {"string": "TEXT", "number": "DOUBLE PRECISION", "integer": "BIGINT",
       "float": "DOUBLE PRECISION", "double": "DOUBLE PRECISION", "int": "BIGINT",
       "boolean": "BOOLEAN", "bool": "BOOLEAN", "object": "JSONB", "array": "JSONB"}
PY = {"string": "str", "number": "float", "integer": "int", "float": "float",
      "double": "float", "int": "int", "boolean": "bool", "bool": "bool",
      "object": "dict", "array": "list"}

_st = {"json_schema": None, "pydantic": "# waiting for the normalizer's canonical…",
       "ddl": "", "migrations": [], "fields": [], "table": TABLE,
       "generator": "?", "updated_at": None, "error": None}
_prev_fields = {}   # field -> ty, from the last generation (for the migration diff)
_lock = threading.Lock()


def _canonical():
    try:
        with urllib.request.urlopen(NORMALIZER + "/status", timeout=4) as r:
            return json.load(r).get("canonical") or {}
    except Exception as e:  # noqa: BLE001
        return {"_error": str(e)}


def canonical_to_jsonschema(canonical):
    props, required = {}, []
    for field, meta in canonical.items():
        js = JS.get(meta.get("ty", "string"), "string")
        if meta.get("kind") == "core":
            props[field] = {"type": js}
            required.append(field)
        else:  # additive → nullable, optional (backwards-compatible)
            props[field] = {"type": [js, "null"]}
    return {"$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": MODEL, "type": "object", "additionalProperties": False,
            "properties": props, "required": required}


def gen_pydantic(schema):
    """Prefer the real datamodel-code-generator; fall back to a direct writer."""
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(schema, f)
            src = f.name
        out = src + ".py"
        subprocess.run(
            ["datamodel-codegen", "--input", src, "--input-file-type", "jsonschema",
             "--output-model-type", "pydantic_v2.BaseModel", "--class-name", MODEL,
             "--use-standard-collections", "--target-python-version", "3.11",
             "--output", out],
            check=True, capture_output=True, timeout=40)
        with open(out) as fh:
            return fh.read().strip(), "datamodel-code-generator"
    except Exception:  # noqa: BLE001 — tool missing / failed → direct writer
        return _direct_pydantic(schema), "direct (datamodel-code-generator unavailable)"


def _direct_pydantic(schema):
    props = schema["properties"]
    req = set(schema.get("required", []))
    lines = ["from __future__ import annotations", "",
             "from pydantic import BaseModel", "", "", f"class {MODEL}(BaseModel):"]
    jspy = {"string": "str", "number": "float", "integer": "int", "boolean": "bool",
            "object": "dict", "array": "list"}
    for name, spec in props.items():
        t = spec["type"]
        base = jspy.get(t[0] if isinstance(t, list) else t, "str")
        attr = name.replace(".", "_")
        if name in req:
            lines.append(f"    {attr}: {base}")
        else:  # additive → Optional, default None
            lines.append(f"    {attr}: {base} | None = None")
    return "\n".join(lines) + "\n"


def canonical_to_ddl(canonical):
    cols = []
    for field, meta in canonical.items():
        col = field.replace(".", "_")
        sql = SQL.get(meta.get("ty", "string"), "TEXT")
        cols.append(f"    {col} {sql} {'NOT NULL' if meta.get('kind') == 'core' else 'NULL'}")
    return f"CREATE TABLE {TABLE} (\n" + ",\n".join(cols) + "\n);"


def _regen():
    global _prev_fields
    canonical = _canonical()
    if not canonical or "_error" in canonical:
        with _lock:
            _st["error"] = (canonical or {}).get("_error", "no canonical yet")
        return
    cur_fields = {f: m.get("ty", "string") for f, m in canonical.items()}
    with _lock:
        changed = cur_fields != _prev_fields
    if not changed:
        return
    schema = canonical_to_jsonschema(canonical)
    pydantic, generator = gen_pydantic(schema)
    ddl = canonical_to_ddl(canonical)
    # migration diff: fields present now but not before → additive ALTER TABLEs
    new_migs = []
    if not _prev_fields:
        new_migs.append({"kind": "create", "sql": ddl,
                         "note": "initial table from the discovered contract"})
    else:
        for f, ty in cur_fields.items():
            if f not in _prev_fields:
                col = f.replace(".", "_")
                new_migs.append({
                    "kind": "add_column",
                    "sql": f"ALTER TABLE {TABLE} ADD COLUMN {col} {SQL.get(ty, 'TEXT')} NULL;",
                    "note": "additive · nullable · backwards-compatible"})
    with _lock:
        _st["json_schema"] = schema
        _st["pydantic"] = pydantic
        _st["generator"] = generator
        _st["ddl"] = ddl
        _st["fields"] = sorted(cur_fields)
        _st["migrations"].extend(new_migs)
        _st["updated_at"] = time.time()
        _st["error"] = None
        _prev_fields = cur_fields
    if new_migs:
        print(f"regenerated: +{len(new_migs)} migration(s) via {generator}", flush=True)


def _loop():
    while True:
        try:
            _regen()
        except Exception as e:  # noqa: BLE001
            print("regen error:", e, flush=True)
        time.sleep(POLL)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, code, body, ctype="application/json"):
        b = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path == "/healthz":
            return self._send(200, json.dumps({"ok": True}))
        with _lock:
            s = dict(_st)
            s["migrations"] = list(s["migrations"])
        if self.path == "/status":
            return self._send(200, json.dumps(s))
        if self.path == "/schema.json":
            return self._send(200, json.dumps(s["json_schema"] or {}, indent=2))
        if self.path == "/models.py":
            return self._send(200, s["pydantic"] or "", "text/plain; charset=utf-8")
        if self.path == "/migrations.sql":
            sql = "\n\n".join(f"-- {m['note']}\n{m['sql']}" for m in s["migrations"])
            return self._send(200, sql, "text/plain; charset=utf-8")
        return self._send(404, json.dumps({"error": "not found"}))

    def log_message(self, *a):
        pass


def main():
    threading.Thread(target=_loop, daemon=True).start()
    print(f"codegen <- {NORMALIZER} table={TABLE} model={MODEL}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
