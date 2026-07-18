# Deblob Console — homelab deploy

Static console served by unprivileged nginx, worker-pinned, exposed on
NodePort **30890**. The Caddy edge routes `deblob.<domain>` → Authelia →
`k8s_node_ip:30890`. **Live** as of 2026-07-18.

## Apply (console workload)
```
kubectl apply -f deploy/console/00-namespace.yaml
kubectl create configmap deblob-console-html -n deblob --from-file=console.html=web/console.html --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/console/10-nginx-conf.yaml -f deploy/console/20-deploy.yaml
```
Update the console: re-run the `create configmap … | apply` line, then
`kubectl rollout restart deploy/deblob-console -n deblob`.

nginx serves `console.html` at `/` directly (no redirect) and reverse-proxies
same-origin `/api/v1/` to `deblob-mgmt.deblob.svc:9615` with request-time DNS
(`resolver 10.43.0.10`) and no URI rewrite, so the full path is forwarded
unchanged. `absolute_redirect off` — behind Caddy/Cloudflare the pod's own
`:8080` listen port must never leak into a `Location` header.

## Edge (Caddy + Authelia) — SHIPPED via ansible
Applied through the `secure-homelab-access` role
(`ansible` commit adding `subdomain_deblob`/`deblob_nodeport` +
the Caddy block cloned from the paperless pattern):
```
{{ _scheme }}{{ subdomain_deblob }}.{{ domain }} {
    import rate_limit
    import proxy_headers
{% if cloudflare_api_token and not cf_tunnel_name %}
    import cf_tls
{% endif %}
    import authelia
    reverse_proxy http://{{ k8s_node_ip }}:{{ deblob_nodeport | default(30890) }}
}
```
**No dedicated Authelia rule is needed** — the existing `*.<domain>` access-
control rules already cover `deblob.<domain>`: `docker_internal` → bypass,
`vpn_and_lan` → one_factor, WAN → default deny (redirect to the portal), the
same posture as every other admin app. Apply with a graceful `caddy reload`
(validated first with `caddy validate`); a full restart is not required. When
re-running the role headless, keep `tags: always` so the Vault-check include
runs (an empty OIDC secret is fatal to Authelia).

## Live Deblob (in-cluster) — SHIPPED
`deploy/console/live/` stands up the management API this namespace proxies to:
Redpanda (`30-redpanda.yaml`), Redis vault (`32-redis.yaml`), config
(`33-deblob-config.yaml`, `min_samples=1`/`min_age_ms=0` relaxed for homelab
seeding, mgmt on `0.0.0.0:9615`, SLM disabled), and the `deblob` Deployment +
`deblob-mgmt` Service on **:9615** (`34-deblob.yaml`). The nginx `/api/v1/`
proxy points here. Three real schemas were seeded via the relay + governed
promotion.

### Known caveat — stale `b5` image, empty catalogue list
The deployed image `ghcr.io/kamilandrzejrybacki-inc/deblob:b5` is a hand-built
tag that **predates** commit `e1bd68c`
(*"maintained `deblob:schemas` index for GET /schemas … + rebuild"*). Its
`GET /schemas` uses the old keyspace-SCAN list that returns empty pages, so the
console's **catalogue view is empty in live mode even though the 3 schemas
exist** (they are reachable individually via `GET /schemas/{id}`, and populated
in the `deblob:schemas` index set for a future fixed binary). CI builds **no
image** (`ci.yml`/`fuzz.yml` run tests only), so there is no newer tag to bump
to. Remedy: rebuild from current `main` (`docker build` per the repo
`Dockerfile`, push to ghcr, bump `34-deblob.yaml` + `deploy/bench/*`), which
carries `e1bd68c` and surfaces the catalogue correctly. Until then, DEMO mode
(no token) shows the full schema-browsing UX with bundled data.
