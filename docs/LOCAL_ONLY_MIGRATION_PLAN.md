# IncuBrix Assessment 4 — build phases and evidence plan

**Updated:** 23 November 2026  
**Scope:** Local and simulated assessment implementation. This plan records observed work and remaining evidence; it does not claim live platform certification or a public demo.

## Product direction

IncuBrix is a trust-aware, self-monitoring content distribution network that makes publishing across fragmented online communities safe, explainable, measurable, and reproducible. Its core value is deciding whether, where, and how content may be published, carrying evidence for decisions and actions, and isolating unreliable connectors before repeated failures occur.

The design uses one shared trust layer: content rights and destination policies feed explainable eligibility checks; destination intelligence and connector health inform recommendations; safe jobs publish only when hard safety checks pass; and a proof ledger records what happened. A score never overrides unverified permission, a blocking policy result, stale certification, or an open circuit breaker.

## Current implementation

The repository contains a Python/FastAPI service, SQLAlchemy job queue, PostgreSQL Compose database, local Moto S3-compatible artifact store, local Lemmy and Ghost services, and mock platform services. Compose ports bind to localhost. Simulation is enabled by default. Adapters use environment-based configuration and host allowlists.

Implemented areas include connector manifests and shared lifecycle contracts; destination rules; idempotent asynchronous jobs; certification and recertification records; recommendation, feedback, and tailoring routes; CSV validation; structured logs and health/readiness endpoints; OpenAPI 3.1; batch runner; output/evidence manifests; destination passport; shared explainable policy evaluation; and deterministic content/rule proof identifiers with SHA-256 hashes.

## What has been verified

| Area | Status | Evidence recorded |
|---|---|---|
| Python regression suite | Verified | 47 tests passed on host and in the rebuilt Docker test image after Passport changes. |
| Compose syntax and startup | Verified | Compose configuration parsed; API readiness and mock health returned successfully. |
| Local artifact path | Verified | API upload to local Moto storage and presigned download completed. |
| OpenAPI generation | Verified | `scripts/export_openapi.py` generated the OpenAPI 3.1 document after new routes were added. |
| Passport and policy evaluation | Implemented | Routes and response facts are covered by application tests; trust score remains null until measured. |
| Evidence linkage | Implemented locally | Publish/certification manifests include evidence IDs and SHA-256 digests; this is not external immutable proof. |

Evidence above reflects the latest recorded run in the repository and should be re-run before final submission.

## Phased execution plan

### Phase 1 — Runtime and integration baseline

**Goal:** Make a clean local setup reproducible and verify the required Integration Standard.

**Work:** Rebuild Compose from a clean state; confirm database/storage/mock readiness; run the documented complete test command; execute the API batch pack; verify every route against `openapi.yaml`; retain command output and environment/hardware details.

**Reasoning:** Later feature claims rely on a reproducible runtime. A passing unit suite alone does not prove that the complete service stack starts or that API contracts match.

**Exit evidence:** Clean-machine run record, readiness output, test summary, batch result, and OpenAPI consistency check.

### Phase 2 — Trust and safety operations

**Goal:** Complete the shared destination trust layer.

**Work:** Add evidence-ledger indexing; connector health states and circuit breaking; cooldown and scheduled recertification behavior; auditable manual override; alert/ticket inspection path; and tests for safety precedence and ambiguous publish outcomes.

**Reasoning:** Recommendations are only useful when they account for current permission, policy, certification, and connector reliability. Durable evidence makes each decision inspectable.

**Exit evidence:** State-transition tests, persisted sample records, API examples, and a local run demonstrating blocked and eligible cases.

### Phase 3 — Core connector certification

**Goal:** Record lifecycle evidence for all six Core connectors using local instances or owned mocks.

**Work:** Complete local Lemmy/Ghost credential bootstrap; run validate, publish, status, and delete lifecycle cases; test remaining protocol connectors against local mocks; record cleanup and retry behavior.

**Reasoning:** Adapter unit tests do not prove lifecycle behavior across the configured local services. Certification evidence must distinguish simulated results from local service results.

**Exit evidence:** Six connector records with request/response evidence, timestamps, cleanup results, and explicitly stated simulation/local-service mode.

### Phase 4 — Advanced and Outstanding measurements

**Goal:** Produce measured evidence for catalogue, recommendations, reliability, and connector metrics.

**Work:** Measure a bounded 1,000-row import with hardware, runs, and method; create a versioned labelled recommendation set and calculate Precision@10; demonstrate `fetch_metrics` on three connectors; review all 150 catalogue entries and retain provenance; measure repeat-run success and idempotency.

**Reasoning:** The submission brief requires evidence-backed claims. Code presence or a single guessed value is not a substitute for measured outcomes or reviewed data.

**Exit evidence:** Reproducible measurement scripts/results, labelled-set definition, review status per destination, and source/license provenance.

### Phase 5 — Submission and evaluation readiness

**Goal:** Deliver a complete, reproducible assessment package.

**Work:** Complete the provided workbook only from verified evidence; finish README, `openapi.yaml`, REPORT, SOURCES, AI_USE, and the required test pack; record a clean README-based run; create an unedited demo video of no more than eight minutes; prepare private repository reviewer access and an available demo endpoint if the brief requires it.

**Reasoning:** The reviewer needs to reproduce the solution, inspect evidence, and distinguish complete work from limitations. External access and live credentials must not be implied by local tests.

**Exit evidence:** Accessible package, working documented run, demo recording, and no unsupported claims.

## Remaining work and known limits

- Re-run the full Compose lifecycle and batch pack from a clean environment and save evidence.
- Complete local non-simulated lifecycle certification for six Core connectors; local Lemmy/Ghost setup still needs credential bootstrap evidence.
- Demonstrate metrics collection on three connectors and scheduled recertification in a runtime run.
- Build a labelled recommender evaluation set; no Precision@10 result is established yet.
- Produce the 1,000-row timing result and review the 150-row catalogue. Discovery rows are not proof of reachability, permission, or suitability.
- Complete submission artifacts and demo/video/access setup.
- No real communities were contacted or posted to. No public deployment or external platform certification is claimed.

## Verification commands

Run these from the repository root and capture their outputs as evidence:

```powershell
python -m pytest
docker compose config --quiet
docker compose up --build -d
```

Then check `/ready`, run the batch runner and local connector cases, and shut the Compose stack down after collecting evidence. Record any failures rather than omitting them.
