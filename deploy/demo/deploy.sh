#!/usr/bin/env bash
# Deploy the Deblob schema-normalization demo. Idempotent.
#   deblob-side: adds events.demo.orders to config (already committed) + creates
#   the raw + normalized topics + rolls deblob. demo-side: ns deblob-demo,
#   configmaps from src/, token secret, the 4 services (producer, normalizer,
#   etl, dashboard).
set -euo pipefail
cd "$(dirname "$0")"
DIR="$(pwd)"

echo "== 1. deblob-side: topics + config =="
kubectl -n deblob exec deploy/redpanda -- rpk topic create events.demo.orders -p 1 -r 1 2>/dev/null \
  || echo "  topic events.demo.orders exists (ok)"
kubectl -n deblob exec deploy/redpanda -- rpk topic create events.demo.orders.normalized -p 1 -r 1 2>/dev/null \
  || echo "  topic events.demo.orders.normalized exists (ok)"
kubectl apply -f ../console/live/33-deblob-config.yaml
kubectl -n deblob rollout restart deploy/deblob
kubectl -n deblob rollout status deploy/deblob --timeout=180s

echo "== 2. namespace =="
kubectl apply -f 00-namespace.yaml

echo "== 3. configmaps from src/ =="
kubectl -n deblob-demo create configmap demo-producer-src   --from-file=producer.py=src/producer.py     --dry-run=client -o yaml | kubectl apply -f -
kubectl -n deblob-demo create configmap demo-normalizer-src --from-file=normalizer.py=src/normalizer.py --dry-run=client -o yaml | kubectl apply -f -
kubectl -n deblob-demo create configmap demo-etl-src        --from-file=etl.py=src/etl.py               --dry-run=client -o yaml | kubectl apply -f -
kubectl -n deblob-demo create configmap demo-dashboard-src  --from-file=dashboard.py=src/dashboard.py   --dry-run=client -o yaml | kubectl apply -f -

echo "== 4. deblob API token secret (best-effort id->name resolution) =="
TOK="$(kubectl -n deblob get secret deblob-secrets -o jsonpath='{.data.api_token}' | base64 -d)"
kubectl -n deblob-demo create secret generic demo-deblob-token --from-literal=token="$TOK" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "== 5. services =="
kubectl apply -f 10-producer.yaml -f 30-normalizer.yaml -f 35-etl.yaml -f 40-dashboard.yaml

echo "== 6. wait for rollouts =="
for d in demo-producer demo-normalizer demo-etl demo-dashboard; do
  kubectl -n deblob-demo rollout status deploy/$d --timeout=180s
done

echo
echo "DONE. Dashboard: http://<any-node-ip>:30895"
echo "Redeploy code after editing src/: re-run this script (configmaps re-created), then:"
echo "  kubectl -n deblob-demo rollout restart deploy/demo-producer demo-normalizer demo-etl demo-dashboard"
