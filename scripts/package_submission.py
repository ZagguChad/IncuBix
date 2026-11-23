"""Create a clean source and evidence bundle for the PS4 assessment."""

from __future__ import annotations

import hashlib
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "dist" / "incubrix-ps4-submission.zip"
ROOT_FILES = {
    ".env.example",
    ".gitignore",
    "AI_USE.md",
    "Dockerfile",
    "README.md",
    "REPORT.md",
    "SOURCES.md",
    "docker-compose.yml",
    "openapi.yaml",
    "pyproject.toml",
    "requirements.lock",
    "requirements-test.lock",
    "security_best_practices_report.md",
}
SOURCE_DIRS = {"docs", "incubrix", "infra", "schemas", "scripts", "testpack", "tests"}
REVIEW_ARTIFACTS = {Path("submission/Assessment4_CandidateReview_Draft.xlsx")}
EVIDENCE_DIRS = {
    Path("evidence/phase3"),
    Path("evidence/phase4/batch-run-2026-11-19-complete"),
    Path("evidence/phase6"),
    Path("evidence/final-local-batch-2026-11-19"),
    Path("evidence/local-compose-batch-2026-11-23"),
    Path("evidence/local-compose-batch-2026-11-23-fixed"),
    Path("evidence/local"),
}
EXCLUDED_PARTS = {".git", ".pytest_cache", ".mypy_cache", ".ruff_cache", "__pycache__", "node_modules", "data", "dist", "tmp"}


def bundle_files() -> list[Path]:
    files = [ROOT / name for name in ROOT_FILES if (ROOT / name).is_file()]
    for directory in SOURCE_DIRS:
        base = ROOT / directory
        if base.exists():
            files.extend(path for path in base.rglob("*") if path.is_file())
    for directory in EVIDENCE_DIRS:
        base = ROOT / directory
        if base.exists():
            files.extend(path for path in base.rglob("*") if path.is_file())
    files.extend(ROOT / path for path in REVIEW_ARTIFACTS if (ROOT / path).is_file())

    unique = sorted(set(files))
    return [
        path for path in unique
        if not (set(path.relative_to(ROOT).parts) & EXCLUDED_PARTS)
        and path.name != ".env"
        and path.suffix not in {".pyc", ".pyo"}
    ]


def main() -> None:
    files = bundle_files()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(OUTPUT, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
        for path in files:
            archive.write(path, Path("incubrix-ps4-submission") / path.relative_to(ROOT))
    digest = hashlib.sha256(OUTPUT.read_bytes()).hexdigest()
    print(f"Wrote {OUTPUT} ({len(files)} files, {OUTPUT.stat().st_size} bytes, sha256 {digest})")


if __name__ == "__main__":
    main()
