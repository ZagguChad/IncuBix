# Phase 1 — Docker and Integration evidence

**Timestamp:** 23 November 2026 (UTC)  
**Scope:** local Docker stack only. No public platform calls or community publishing occurred.

## Inspection

- `docker-compose.yml` defines API, PostgreSQL, local S3-compatible storage, Lemmy, Ghost, their databases, and a mock-platform service.
- API startup waits on healthy PostgreSQL, S3-compatible storage, and mock-platform services. Lemmy and Ghost use local databases.
- The running Compose status is captured in [compose-ps.log](compose-ps.log).

## Executed checks

| Test name | Command | Result | Output / evidence |
|---|---|---|---|
| Stack build and start | `docker compose up --build -d` | Passed | [compose-up.log](compose-up.log) |
| Service health/status | `docker compose ps` | Passed | [compose-ps.log](compose-ps.log) |
| API and mock health | `GET /ready`, `GET /health`, `GET mock /health` | Passed | [health.json](health.json) |
| Storage artifact round trip | `POST /api/v1/artifacts`, then presigned download | Passed, HTTP 200 download with matching byte count | [artifact-roundtrip.json](artifact-roundtrip.json) |
| Docker test suite | `docker compose --profile tools run --build --rm test` | Passed, 47 tests | [docker-test.log](docker-test.log) |
| Batch runner | `python -m incubrix.batch_runner testpack/cases/validate_supported_text.json evidence/phase1-2026-11-23/batch --base-url http://127.0.0.1:8000 --run-id phase1-2026-11-23` | Passed, 1 of 1 | [batch.log](batch.log), [batch/index.json](batch/index.json) |

## Result

Phase 1 passes. The local integration baseline is reproducible with Compose and has evidence for startup, service health, storage, container tests, and a batch-runner request.
