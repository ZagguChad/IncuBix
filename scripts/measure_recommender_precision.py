"""Run a labelled, synthetic local Precision@10 evaluation of recommendations."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "evidence" / "local" / "metrics"


def main() -> int:
    stamp = str(int(time.time() * 1000))
    labels: dict[str, bool] = {}
    with httpx.Client(base_url="http://127.0.0.1:8000", timeout=30) as client:
        for index in range(12):
            relevant = index < 10
            payload = {
                "name": f"Labelled recommendation {stamp}-{index}",
                "url": f"http://localhost/recommendation-eval-{stamp}-{index}",
                "platform_key": "discourse", "surface": "topic", "category": "developer",
                "connector_key": "discourse", "audience_tags": ["AI", "SaaS"] if relevant else ["gardening"],
                "rules": {"self_promo_allowed": True, "link_policy": "allowed", "disclosure_required": False,
                          "automation_allowed": True, "content_types": ["text"],
                          "spec": {"title_max_chars": 255, "body_max_chars": 32000}},
            }
            response = client.post("/api/v1/destinations", json=payload, headers={"Idempotency-Key": f"precision-destination-{stamp}-{index}"})
            response.raise_for_status()
            labels[response.json()["destination_id"]] = relevant
        response = client.post("/api/v1/recommendations", json={"audience_tags": ["AI", "SaaS"], "language": "en", "limit": 10}, headers={"Idempotency-Key": f"precision-recommendation-{stamp}"})
        response.raise_for_status()
        recommended = [item["destination"]["destination_id"] for item in response.json()["items"]]
    relevant_in_top_10 = sum(labels.get(item, False) for item in recommended)
    record = {"recorded_at": datetime.now(UTC).isoformat(), "dataset": "synthetic-labelled-local-v1", "labelled_destinations": len(labels), "relevant_labels": sum(labels.values()), "top_k": 10, "recommended_ids": recommended, "relevant_in_top_10": relevant_in_top_10, "precision_at_10": relevant_in_top_10 / 10, "limitations": "Synthetic local labels validate ranking behavior only; they do not represent community relevance research."}
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / "recommender-precision-at-10.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(record, indent=2))
    return 0 if len(recommended) == 10 else 1


if __name__ == "__main__":
    raise SystemExit(main())
