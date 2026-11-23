"""Measure a bounded 1,000-row catalogue import against a running local API."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import platform
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "evidence" / "local" / "metrics"
BASE_URL = "http://127.0.0.1:8000"


def main() -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=["name", "url", "platform_key", "surface", "category", "connector_key", "audience_tags", "level"])
    writer.writeheader()
    for number in range(1, 1001):
        writer.writerow({
            "name": f"Local measurement destination {number}",
            "url": f"http://localhost/catalogue-measurement-{stamp}-{number}",
            "platform_key": "discourse",
            "surface": "topic",
            "category": "developer",
            "connector_key": "discourse",
            "audience_tags": "AI|SaaS",
            "level": "L0",
        })
    body = stream.getvalue().encode()
    started = time.perf_counter()
    response = httpx.post(
        f"{BASE_URL}/api/v1/catalogue/import",
        headers={"Idempotency-Key": f"catalogue-measurement-{stamp}"},
        files={"file": ("catalogue-1000.csv", body, "text/csv")},
        timeout=120,
    )
    elapsed = time.perf_counter() - started
    OUTPUT.mkdir(parents=True, exist_ok=True)
    record = {
        "recorded_at": datetime.now(UTC).isoformat(),
        "rows_submitted": 1000,
        "http_status": response.status_code,
        "elapsed_seconds": round(elapsed, 4),
        "response": response.json(),
        "csv_sha256": hashlib.sha256(body).hexdigest(),
        "python": platform.python_version(),
        "platform": platform.platform(),
    }
    path = OUTPUT / "catalogue-import-1000.json"
    path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: record[key] for key in ("rows_submitted", "http_status", "elapsed_seconds")}, indent=2))
    return 0 if response.status_code == 201 and record["response"].get("imported") == 1000 else 1


if __name__ == "__main__":
    raise SystemExit(main())
