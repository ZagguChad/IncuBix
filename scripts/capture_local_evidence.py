"""Capture reproducible, local-only assessment evidence.

This script deliberately records command failures as evidence. It makes no
network calls beyond local command execution and never contacts communities.
"""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "evidence" / "local"


def run(name: str, command: list[str]) -> dict[str, object]:
    completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    text = completed.stdout + completed.stderr
    path = OUTPUT / f"{name}.log"
    path.write_text(text, encoding="utf-8")
    return {
        "name": name,
        "command": command,
        "exit_code": completed.returncode,
        "log": path.relative_to(ROOT).as_posix(),
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
    }


def main() -> int:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    results = [
        run("pytest", [sys.executable, "-m", "pytest"]),
        run("compose-config", ["docker", "compose", "config", "--quiet"]),
    ]
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "scope": "local-only automated checks; no external platform calls",
        "environment": {"python": sys.version, "platform": platform.platform()},
        "commands": results,
    }
    (OUTPUT / "index.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0 if all(item["exit_code"] == 0 for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
