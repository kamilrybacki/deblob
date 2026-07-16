# Modal Arm-C Setup — Status (autonomous session 2026-07-16/17)

What I did with your Modal token while you slept, what's verified, and the few things left for you.

## ✅ Done + verified
- **Modal CLI authenticated** — token verified against api.modal.com, workspace `kamilrybacki`, profile `kamilrybacki` (stored in `~/.modal.toml`, local only, never committed).
- **`arm-c` environment created** (isolated from `main`).
- **T4 smoke test PASSED** — a real `Tesla T4, 15360 MiB` ran and scaled back to zero. GPU access + free credit confirmed working. Run visible in your dashboard under env `arm-c`, app `deblob-t4-smoke`.
- **`ModalBackend` built + committed** (`eb99bbe`) — headless token pair from env, budget-capped before submit, no-promote (compile-time proven), Needle guarded as `needle-custom`. 117 tests green.
- **Trainer DEPLOYED** to env `arm-c` — image built (torch 2.4.1 / transformers / peft / trl), functions `train_lora` + web endpoint live at `https://kamilrybacki-arm-c--deblob-experiment-trainer-web.modal.run`, scales to zero (no idle cost). Deployment: `modal.com/apps/kamilrybacki/arm-c/deployed/deblob-experiment-trainer`.

## 🔴 Your manual TODOs (can't be done via CLI / need you)
1. **Set spend budgets in the dashboard** (Hermes flagged — CLI can't set these; the only real safety gap):
   - Workspace budget → **$29**
   - `arm-c` Environment budget → **$25**
   - Dashboard: Settings → Usage & Billing → Budgets. Without these, a runaway round could eat the $30 credit (Modal still auto-stops at $30 since you have no card, so worst case is "workloads stop," not a bill).
2. **Rotate the Modal token.** You pasted the token-secret in chat, so it's in this conversation's history. Once you're happy setup works: Modal dashboard → Settings → API Tokens → revoke `ak-3UHyKgFgcRHunvt8TgMGAW`, create a new one, re-run `modal token set …`. (It's a Starter member token, low blast radius, but rotate it.)
3. **When running on k3s:** the Modal token must become a k8s Secret in the `deblob-experiment` namespace (Vault MCP write is 403/read-only, so seed it directly at deploy — same pattern as the other homelab secrets):
   `kubectl -n deblob-experiment create secret generic experiment-secrets --from-literal=MODAL_TOKEN_ID=… --from-literal=MODAL_TOKEN_SECRET=… --from-literal=MODAL_ENVIRONMENT=arm-c`
   (values are in `~/.modal.toml`, not chat).

## Modal operating rules baked in (Hermes' gotchas)
- `min_containers=0`, `max_containers=1`, **no region pinning** (avoids 1.5–1.75× multipliers), **no auto-retries** around the training fn (non-preemptible is 3× — the Deblob hook decides retries), cache the image + base-model files (cold starts are billed), persist adapters to an external `output_uri` not the ephemeral container.
- Budget exhaustion → training FAILURE, never a promotion (separation of duties holds).

## To run a real Arm-C round (when you're back)
The flow is: feedback → `ModalBackend.submit` (T4 LoRA on `arm-c`) → adapter digests back → Deblob held-out gate → two-stage canary → promote → next round uses the improved model. Gaps to close first (disclosed in `.superpowers/sdd/experiment-task6-report.md`): the base-model→HF-repo map and dataset/base-bundle manifest resolution in `trainer.py`. Everything else (auth, env, GPU, backend, gate) is verified.

## What I did NOT do (deliberately, unattended)
- Did not run a full real training round or the whole k3s experiment stack — those consume credit + need watching (weights, disk, Cactus image). Everything is staged one command away; left for you to trigger.
- Did not set budgets (CLI can't) — item 1 above.
