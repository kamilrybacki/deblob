# Building the Deblob server image (off lw-main)

Every `deblob:bN` up to **b21** was hand-built with `docker build` on **lw-main**,
the edge node. Its 109 GB disk (Caddy + Vault + docker) is chronically near-full,
and each Rust + librdkafka release build peaks at ~15–20 GB of cargo target +
docker layers — so builds repeatedly drove the disk to 100 % and were cancelled
(b8, and b22's first two attempts).

**Builds now run in-cluster via Kaniko on a k3s worker** (`kaniko-deblob.yaml`),
never on the control-plane / edge node, and push straight to
`ghcr.io/kamilrybacki/deblob` (the repo-owner namespace — same owner as the
GitHub repo, so no cross-org token juggling). Deploy is then **pull-only**.

## One-time setup

1. **`ghcr-push` secret** — a GH PAT with `write:packages` (classic PAT, scope
   `write:packages`; or a fine-grained token with Packages: read+write for the
   `kamilrybacki` account). Kaniko reads it as a docker `config.json`:

   ```sh
   # PAT with write:packages; NEVER commit it or paste it into chat.
   kubectl create secret docker-registry ghcr-push \
     --namespace deblob \
     --docker-server=ghcr.io \
     --docker-username=kamilrybacki \
     --docker-password="$GHCR_PAT"
   ```

2. **`ghcr-pull` must be able to READ the new package.** The deployment already
   references `ghcr-pull`. Either the pull token has `read:packages` on
   `kamilrybacki/deblob`, or — simplest — make the package **public** in the
   GitHub UI after the first push (Packages → deblob → Package settings →
   Change visibility → Public). Then no pull secret is needed at all for it.

## Build a release

Tags are explicit (`__TAG__` placeholder), so each build is deliberate:

```sh
TAG=b22
sed "s/__TAG__/$TAG/g" deploy/build/kaniko-deblob.yaml | kubectl apply -f -
kubectl -n deblob wait --for=condition=ready pod -l tag=$TAG --timeout=60s
kubectl -n deblob logs -f job/kaniko-deblob-$TAG
```

Kaniko clones the **public** repo itself (no git creds) at `refs/heads/main`.
To build a specific ref, change `#refs/heads/main` in the Job to
`#refs/tags/<tag>` or a commit sha. First build is a cold ~15 min compile;
later builds reuse the `deblob-cache` layer cache for the stable apt/dependency
layers.

## Deploy the built image

```sh
# bump deploy/console/live/34-deblob.yaml -> image: ghcr.io/kamilrybacki/deblob:$TAG
kubectl apply -f deploy/console/live/34-deblob.yaml
kubectl rollout restart deploy/deblob -n deblob
kubectl rollout status  deploy/deblob -n deblob
```

`redis-vault` is a separate pod, so the schema vault survives the roll.

## Node disk

Kaniko builds in the pod's ephemeral storage (requests 20 Gi, limits 40 Gi).
Schedule it on a worker with that much free — check `kubectl get nodes` +
`df` per node, or add a `nodeSelector` pinning it to a known-roomy worker /
an NFS-backed scratch. It will never land on the control-plane / edge node
(the Job's `nodeAffinity` forbids it).

## Notes

- `Dockerfile.server` has **no** `RUN --mount=type=cache` — Kaniko can't parse
  BuildKit cache mounts. Cross-build caching is Kaniko's `--cache-repo` instead.
- `deploy/bench/*` and `deploy/experiment/*` still reference the old
  `kamilandrzejrybacki-inc/deblob` path (inactive one-off jobs). Repoint them
  the same way if/when they're revived.
