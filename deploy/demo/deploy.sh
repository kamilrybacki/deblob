#!/usr/bin/env bash
# Deploy the Deblob drift-sentinel demo. Idempotent.
#   deblob-side: adds events.demo.orders to config (already committed) + creates
#   the topic + rolls deblob. demo-side: ns deblob-demo, configmaps from src/,
#   token secret, the 4 services.
set -euo pipefail
cd "$(dirname "$0")"
DIR="$(pwd)"

echo "== 1. deblob-side: events.demo.orders topic + config =="
kubectl -n deblob exec deploy/redpanda -- rpk topic create events.demo.orders -p 1 -r 1 2>/dev/null \
  || echo "  topic exists (ok)"
kubectl apply -f ../console/live/33-deblob-config.yaml
kubectl -n deblob rollout restart deploy/deblob
kubectl -n deblob rollout status deploy/deblob --timeout=180s

echo "== 2. namespace =="
kubectl apply -f 00-namespace.yaml

echo "== 3. configmaps from src/ =="
kubectl -n deblob-demo create configmap demo-producer-src  --from-file=producer.py=src/producer.py               --dry-run=client -o yaml | kubectl apply -f -
kubectl -n deblob-demo create configmap demo-naive-src     --from-file=naive_consumer.py=src/naive_consumer.py   --dry-run=client -o yaml | kubectl apply -f -
kubectl -n deblob-demo create configmap demo-aware-src     --from-file=aware_consumer.py=src/aware_consumer.py   --dry-run=client -o yaml | kubectl apply -f -
kubectl -n deblob-demo create configmap demo-dashboard-src --from-file=dashboard.py=src/dashboard.py             --dry-run=client -o yaml | kubectl apply -f -

echo "== 4. deblob API token secret (best-effort id->name resolution) =="
TOK="$(kubectl -n deblob get secret deblob-secrets -o jsonpath='{.data.api_token}' | base64 -d)"
kubectl -n deblob-demo create secret generic demo-deblob-token --from-literal=token="$TOK" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "== 5. services =="
kubectl apply -f 10-producer.yaml -f 20-naive-consumer.yaml -f 30-aware-consumer.yaml -f 40-dashboard.yaml

echo "== 6. wait for rollouts =="
for d in demo-producer demo-naive demo-aware demo-dashboard; do
  kubectl -n deblob-demo rollout status deploy/$d --timeout=180s
done

echo
echo "DONE. Dashboard: http://<any-node-ip>:30895"
echo "Redeploy code after editing src/: re-run this script (configmaps re-created), then:"
echo "  kubectl -n deblob-demo rollout restart deploy/demo-producer demo-naive demo-aware demo-dashboard"
