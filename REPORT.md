# IncuBrix Assessment 4 — implementation report

**Updated:** 23 November 2026  
**Assessment:** Distribution: Long-Tail Destination Network

## Current delivery state

IncuBrix is a local-only, simulation-first FastAPI distribution service. Docker Compose starts the API, PostgreSQL, Moto S3-compatible storage, local Lemmy, local Ghost, and mock platform services. The service provides connector manifests, destination rules, asynchronous jobs, idempotency, certification records, local artifact evidence, structured logs, OpenAPI 3.1, and a batch runner.

The service now also exposes a Destination Passport at `GET /api/v1/destinations/{id}/passport` and explainable shared policy evaluation at `POST /api/v1/policy-evaluations`. Passport output reports persisted policy, permission status, certification facts, reliability counts, risks, and eligibility. A Trust Score is deliberately `null` until its factors are measured; IncuBrix does not fabricate a score.

Publish and certification manifests carry deterministic evidence IDs and SHA-256 hashes for the content and rules snapshot used by the operation. This is local proof-carrying evidence, not a claim of immutable third-party platform evidence.

## Verified local evidence

- Host regression suite: **47 passed** on 23 November 2026. Redacted output and a SHA-256 hash are in `evidence/local/`.
- Container test suite: **47 passed** on 23 November 2026 against the running local stack.
- `docker compose config --quiet` succeeds.
- The local Compose stack was observed healthy: API, PostgreSQL, local storage, Lemmy, Ghost, and mock services were running; `/ready`, `/health`, and mock `/health` returned success.
- The local batch runner passed **6 of 6** cases. Its evidence records expected failure paths as passing assertions.
- A real local API artifact upload to Moto followed by a presigned browser-facing download completed successfully.
- `scripts/export_openapi.py` generated `openapi.yaml` as OpenAPI 3.1 after the Passport and policy routes were added.

## What is not yet claimed

- Six Core connector lifecycle certifications have not been recorded for all local instances/mocks.
- The local Lemmy/Ghost credential bootstrap flow requires completion and documented evidence.
- The 1,000-row catalogue import is measured at 1.2535 seconds in the local running stack. Labelled Precision@10, three measured connector metric flows, and full reviewed 150-destination catalogue evidence are not complete.
- No real community was contacted or posted to. No external platform certification or public deployment is claimed.

## Safety and provenance

- Simulation defaults to enabled. Connector secrets are environment references and are rejected in API payloads.
- Local storage uses the MinIO Python client against the Moto service defined in Docker Compose.
- Publishing rules enforce frequency, promotion, link, disclosure, certification, and destination eligibility constraints. Ambiguous remote outcomes are not automatically replayed.
- The project uses only local/mock test services in the default path. Rights declarations are recorded but never represented as independently verified.

## Next phases

1. Add proof-carrying publication/evaluation identifiers and evidence-ledger indexing.
2. Implement connector health states, circuit breaking, cooldown recertification, and auditable manual override.
3. Complete and evidence six Core local/mock certification runs.
4. Produce measured Advanced/Outstanding evidence: import performance, labelled recommender evaluation, metrics, recertification, and catalogue review.
5. Complete the submission workbook only from observed evidence, then create the unedited demo video and provide reviewer access to the private repository.

See `docs/LOCAL_ONLY_MIGRATION_PLAN.md` for the phase gates, test strategy, and remaining blockers.
