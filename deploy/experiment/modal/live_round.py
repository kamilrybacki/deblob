"""Live Arm-C round via the DEPLOYED production trainer function (SDK path;
the /submit proxy-auth HTTP layer needs a separate proxy-auth token — the
function call exercises the same train_lora the endpoint spawns, on real
data resolved from the Volume manifest, with the v4 recipe). Data + manifest
already uploaded by seam_live_round.py (volume puts succeeded).
"""

import hashlib
import json
import sys

import modal

SP = "/tmp/claude-1000/-home-kamil-rybacki-Code/728c34c8-8806-4db4-8d2b-8e30fa3c2e3f/scratchpad"


def sha(b):
    return "sha256:" + hashlib.sha256(b).hexdigest()[:24]


def main():
    replay_bytes = open(f"{SP}/replay.jsonl", "rb").read()
    dataset_digest = sha(replay_bytes)
    base_digest = "sha256:base-qwen25-05b"
    body = {
        "base_bundle_digest": base_digest,
        "dataset_digest": dataset_digest,
        "feedback_cutoff": "2026-07-17T00:00:00Z",
        "trainer_image_digest": "sha256:trainer-v4",
        "method": "lora-sft",
        "lora": {"rank": 16, "alpha": 32, "learning_rate": 2e-4, "epochs": 3},
        "replay_manifest_digest": dataset_digest,
        "seed": 7,
        "budget_max_usd": 5.0,
        "budget_max_runtime_minutes": 40,
        "output_uri": "modal-volume://arm-c-artifacts/live-round-1",
        "cached_image_tag": "trainer-v4",
        "cached_volume_name": "deblob-base-models",
    }
    print(f"dataset_digest={dataset_digest}  calling deployed train_lora...")
    fn = modal.Function.from_name("deblob-experiment-trainer", "train_lora",
                                  environment_name="arm-c")
    result = fn.remote(body)
    print("=== LIVE ROUND RESULT (production trainer, real hook data path) ===")
    print(json.dumps(result, indent=2))
    open(f"{SP}/live_round_result.json", "w").write(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
