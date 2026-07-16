# Deblob k3s benchmark deploy

Ephemeral benchmark stack for `docs/superpowers/specs/2026-07-16-deblob-k3s-benchmark.md`.
Everything runs on the **worker nodes** (`lw-c1`/`lw-c2`) via a required
`node-role.kubernetes.io/control-plane DoesNotExist` affinity — the
control-plane node (`lw-c3`) and the edge host (`lw-main`, not a k8s node) are
left untouched.

## Components (namespace `deblob-bench`)

| File | Resource |
|------|----------|
| `00-namespace.yaml` | the `deblob-bench` namespace |
| `10-redpanda.yaml` | single-broker Redpanda (Kafka API), headless svc |
| `15-topics-init.yaml` | Job: create topics with partitions |
| `20-redis-vault.yaml` | AOF + noeviction Redis vault |
| `30-deblob-config.yaml` | ConfigMap: `deblob.toml` (slm/http_proxy off; seeded `[semantic]` vocab) |
| `40-deblob.yaml` | Deblob Deployment + Service (mgmt :9615) |

## Deploy

```bash
# 1) namespace
kubectl apply -f deploy/bench/00-namespace.yaml
# 2) image pull secret (private ghcr) + the Deblob API token
kubectl -n deblob-bench create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io --docker-username=<gh-user> --docker-password=<ghcr-PAT>
kubectl -n deblob-bench create secret generic deblob-secrets \
  --from-literal=api_token="$(openssl rand -hex 24)"
# 3) the rest
kubectl apply -f deploy/bench/
# 4) wait
kubectl -n deblob-bench rollout status deploy/deblob
```

## Teardown

```bash
kubectl delete ns deblob-bench
```

Nothing else on the cluster is touched.
