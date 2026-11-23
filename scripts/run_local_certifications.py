"""Run approved local/mock Core connector certifications and save result evidence."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx


OUTPUT = Path(__file__).resolve().parents[1] / "evidence" / "local" / "certifications"

CASES = [
    ("discourse", "http://mock-platforms:9000/discourse", {"base_url": "http://mock-platforms:9000/discourse", "api_key_env": "DISCOURSE_API_KEY"}),
    ("lemmy", "http://mock-platforms:9000/lemmy", {"base_url": "http://mock-platforms:9000/lemmy", "community_id": 1, "jwt_env": "LEMMY_JWT"}),
    ("devto", "http://mock-platforms:9000/devto", {"base_url": "http://mock-platforms:9000/devto", "api_key_env": "DEVTO_API_KEY"}),
    ("ghost", "http://mock-platforms:9000/ghost", {"base_url": "http://mock-platforms:9000/ghost", "admin_api_key_env": "GHOST_ADMIN_API_KEY"}),
    ("signed-webhook", "http://mock-platforms:9000/partner", {"publish_url": "http://mock-platforms:9000/partner/publish", "status_url_template": "http://mock-platforms:9000/partner/status/{platform_post_id}", "delete_url_template": "http://mock-platforms:9000/partner/posts/{platform_post_id}", "signing_secret_env": "PARTNER_WEBHOOK_SECRET"}),
    ("assisted", "http://mock-platforms:9000/assisted", {}),
]


def main() -> int:
    run_id = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    results = []
    with httpx.Client(base_url="http://127.0.0.1:8000", timeout=30) as client:
        for connector, url, connection in CASES:
            destination = {
                "name": f"Local certification {connector} {run_id}", "url": f"{url}/{run_id}",
                "platform_key": connector, "surface": "topic", "category": "local-test",
                "audience_tags": ["AI"], "connector_key": connector, "connection": connection,
                "rules": {"self_promo_allowed": True, "link_policy": "allowed", "disclosure_required": False,
                          "automation_allowed": connector != "assisted", "content_types": ["text"],
                          "spec": {"title_max_chars": 255, "body_max_chars": 32000}},
            }
            create = client.post("/api/v1/destinations", json=destination, headers={"Idempotency-Key": f"local-cert-destination-{connector}-{run_id}"})
            create.raise_for_status()
            destination_id = create.json()["destination_id"]
            queued = client.post(f"/api/v1/destinations/{destination_id}/certifications", headers={"Idempotency-Key": f"local-cert-{connector}-{run_id}"})
            queued.raise_for_status()
            job_id = queued.json()["job_id"]
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                job = client.get(f"/api/v1/jobs/{job_id}").json()
                if job["status"] in {"succeeded", "failed"}:
                    break
                time.sleep(0.25)
            result = {"connector": connector, "destination_id": destination_id, "job": job}
            certification_id = (job.get("result") or job.get("error", {}).get("details", {})).get("certification_id")
            if certification_id:
                result["certification"] = client.get(f"/api/v1/certifications/{certification_id}").json()
            results.append(result)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    record = {"recorded_at": datetime.now(UTC).isoformat(), "mode": "local_or_approved_sandbox", "results": results}
    (OUTPUT / "local-core-certifications.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"runs": len(results), "succeeded": sum(item["job"]["status"] == "succeeded" for item in results)}, indent=2))
    return 0 if all(item["job"]["status"] == "succeeded" for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
