# Phase 2 — Six Core connector evidence

**Timestamp:** 23 November 2026 (UTC)  
**Scope:** owned local Docker services and deterministic protocol mocks. No public platform or community account was used.

## Test command

`SIMULATION_MODE=false docker compose up --build -d --force-recreate api mock-platforms`  
`python scripts/run_local_certifications.py`  
`python -m pytest`

## Results

| Connector | Supported lifecycle evidence | Result | Evidence file |
|---|---|---|---|
| Discourse | validate, publish, live URL, status, delete | Passed | `evidence/local/certifications/discourse-certification.json` |
| Lemmy | validate, publish, live URL, status, delete | Passed against owned Lemmy protocol mock | `evidence/local/certifications/lemmy-certification.json` |
| DEV | validate, publish, live URL, status, delete | Passed | `evidence/local/certifications/devto-certification.json` |
| Ghost | validate, publish, live URL, status, delete | Passed against owned Ghost protocol mock | `evidence/local/certifications/ghost-certification.json` |
| Signed webhook | validate, publish, live URL, status, delete | Passed | `evidence/local/certifications/signed-webhook-certification.json` |
| Assisted | validate and build assist pack | Passed; human publish/delete is intentionally unsupported | `evidence/local/certifications/assisted-certification.json` |

The combined run completed 6 of 6 jobs successfully. Host regression suite: 47 passed.

## Remaining limitation

The Compose stack contains local Lemmy and Ghost application containers, but direct application-instance publish/status/delete certification still requires locally provisioned administrator/API credentials. The completed Lemmy and Ghost evidence uses deterministic, owned protocol mocks and must be described as such in submission claims.
