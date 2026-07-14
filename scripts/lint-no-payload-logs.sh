#!/usr/bin/env bash
# CI lint (spec §11): no payload bytes, parsed-node contents, or
# canonicalized/raw text may ever reach a `tracing::` log field. Deliberately
# simple and conservative — a best-effort grep guard, not a Rust parser — so
# it stays low on false positives. Its job is to catch an accidental
# `debug!(?payload)`, not to prove the absence of every possible leak.
#
# Approach: scan every `tracing::{trace,debug,info,warn,error,event}!(...)`
# call site (multi-line aware — the whole macro invocation, up to its
# closing `;`, is treated as one unit) for a short deny-list of
# identifiers that name raw payload/node content. `payload_len`,
# `payload_size`, and similar derived-scalar field names are NOT flagged —
# each deny-list word is matched as a whole identifier (`\b...\b`), so a
# longer identifier that merely contains one as a substring doesn't trip it.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

DENY_WORDS='payload|raw_payload|node|record\.canonical|raw_value|raw_bytes|canonical_bytes'
MACRO_RE='tracing::(trace|debug|info|warn|error|event)!\('

fail=0
violations=""

while IFS= read -r -d '' file; do
  # -P: PCRE (needed for \b + alternation across the deny list).
  # -z: NUL-separates records instead of newlines, so `.` in the pattern
  #     below can span multiple source lines — a macro call's argument list
  #     is not always on one line.
  # -o: print only the matched text, so the violation itself is visible.
  hits=$(grep -Pzo "${MACRO_RE}[^;]{0,500}?\b(${DENY_WORDS})\b[^;]{0,500}?\)" "$file" 2>/dev/null | tr '\0' '\n' || true)
  if [[ -n "$hits" ]]; then
    fail=1
    violations+=$'\n'"-- ${file} --"$'\n'"${hits}"$'\n'
  fi
done < <(find crates -name '*.rs' -not -path '*/target/*' -not -path '*/fuzz/*' -print0)

if [[ "$fail" -ne 0 ]]; then
  echo "lint-no-payload-logs: FAIL" >&2
  echo "The following tracing:: log call(s) appear to log raw payload/node/canonical content (spec §11 forbids this):" >&2
  echo "$violations" >&2
  echo "Fix: log a bounded/derived field instead (a fate label, a reason, a byte length, a fingerprint) — never the payload/node/canonical text itself." >&2
  exit 1
fi

echo "lint-no-payload-logs: OK — no tracing:: log call references payload/node/canonical content."
