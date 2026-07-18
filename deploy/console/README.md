# Deblob Console — homelab deploy

Static console served by unprivileged nginx, worker-pinned, exposed on
NodePort **30890**. The Caddy edge routes `deblob.<domain>` → Authelia →
`k8s_node_ip:30890`.

## Apply
```
kubectl apply -f deploy/console/00-namespace.yaml
kubectl create configmap deblob-console-html -n deblob --from-file=console.html=web/console.html --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/console/10-nginx-conf.yaml -f deploy/console/20-deploy.yaml
```
Update the console: re-run the `create configmap … | apply` line, then
`kubectl rollout restart deploy/deblob-console -n deblob`.

## Edge (Caddy + Authelia) — ansible
Add to `secure-homelab-access` caddy vars: `subdomain_deblob: deblob`,
`deblob_nodeport: 30890`; add the block to `Caddyfile.j2`:
```
{{ _scheme }}{{ subdomain_deblob }}.{{ domain }} {
    import rate_limit
    import proxy_headers
    import cf_tls
    import authelia
    reverse_proxy http://{{ k8s_node_ip }}:{{ deblob_nodeport | default(30890) }}
}
```
Add an Authelia access-control rule for `deblob.<domain>` (same policy as the
other admin apps), then re-run the caddy role (`docker restart caddy`, not
reload) with `--tags` including the vault-check (`tags: always`).

## Live Deblob (to leave demo mode)
The console shows bundled demo data until a Deblob management API runs in this
namespace as Service `deblob-mgmt:8081`. That needs the `deblob` image (build +
push to ghcr), a Redis vault, and a Redpanda broker (the mgmt API's config
requires a `[kafka]` section to boot). The nginx `/api/v1/` proxy is already
wired to `deblob-mgmt.deblob.svc:8081`.
