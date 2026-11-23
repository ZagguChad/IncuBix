from __future__ import annotations

from pathlib import Path

import yaml

from incubrix.main import app

document = app.openapi()
if not str(document.get("openapi", "")).startswith("3.1."):
    raise SystemExit(f"Expected OpenAPI 3.1, got {document.get('openapi')!r}")

output = Path(__file__).resolve().parents[1] / "openapi.yaml"
output.write_text(yaml.safe_dump(document, sort_keys=False, allow_unicode=True), encoding="utf-8")
print(f"Wrote {output} ({document['openapi']})")
