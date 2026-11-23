# Assessment execution phases

This is the delivery order for the assessment. A phase closes only when its automated tests pass and its evidence is saved. Local/mock results must always be labelled local/mock; they are not external-platform proof.

## Phase 0 — Submission control

**Goal:** prevent unsupported claims.

- Keep `README.md`, `openapi.yaml`, `REPORT.md`, `SOURCES.md`, `AI_USE.md`, and `testpack/` in the active root.
- Maintain an evidence index with the command, timestamp, environment, output hashes, and redacted logs.
- Keep the submission form blank or `No`/`Partial` until its linked evidence exists.

**Exit gate:** all required delivery files exist; no secrets or external claims are present.

## Phase 1 — Reproducible local baseline

**Goal:** make the reviewer’s first five minutes reliable.

- `docker compose up --build -d` starts API, database, local S3-compatible storage, Lemmy, Ghost, and mocks.
- `docker compose --profile tools run --build --rm test` runs the complete suite without real API keys.
- Verify `/health`, `/ready`, an artifact upload/download, and generated OpenAPI.

**Tests:** Compose config; host and container pytest; API readiness; artifact round trip.

**Exit gate:** all commands pass from a clean clone with a generated local `.env`.

## Phase 2 — Must-have connector contract

**Goal:** protect the contract before richer features.

- Verify manifests and the required connector methods against Appendix A.
- Exercise publish, live URL/status, cleanup, HMAC webhook verification, assisted-pack creation, timeout/idempotency, 429, expired token, and rejected content against local instances/mocks.
- Preserve one evidence record per Core connector.

**Tests:** schema/contract tests, mock lifecycle tests, certification batch cases.

**Exit gate:** every Must-have row has a command, test, evidence path, and truthful limitation.

## Phase 3 — Trust and explainability

**Goal:** make destination decisions inspectable.

- Destination Passport holds recorded policy, permission status, certification state, reliability facts, risk flags, and eligibility.
- Shared policy evaluation gives the blocking rule, source, remediation, and exception path.
- Publication and certification manifests include evidence IDs plus content/rules hashes.

**Tests:** Passport response, idempotent policy evaluation, rule-block reason, manifest/schema compatibility, evidence redaction.

**Exit gate:** a reviewer can reproduce a policy block and follow its evidence without trusting a screenshot.

## Phase 4 — Connector health and recertification

**Goal:** make failures safe.

- Add health windows, open/half-open/closed circuit state, cooldown retest, exclusion, and audited manual override.
- Reuse existing recertification demotion and local ticketing; do not replay ambiguous publishes.

**Tests:** repeated failure opens circuit; excluded destination is not recommended/published; successful recertification restores only safe eligibility; override is logged.

**Exit gate:** injected connector breakage yields an evidence-backed demotion and remediation path.

## Phase 5 — Advanced requirements

**Goal:** obtain measured, not assumed, Advanced evidence.

- Generate a deterministic 1,000-row CSV fixture and time the import.
- Capture row-by-row malformed CSV output, duplicates, bad URLs, and mixed-language handling.
- Preserve confirmed rules proposals, tailoring examples, and three extra connector lifecycle results.

**Tests:** import timing harness, invalid fixture, non-English fixture, extra connector contracts.

**Exit gate:** each Advanced claim has a measured output and reproduction command.

## Phase 6 — Outstanding requirements

**Goal:** complete only evidence that can be honestly measured.

- Version a labelled recommendation dataset and calculate Precision@10.
- Collect normalized metrics from three local connector/mock flows.
- Exercise pre-expiry recertification, failure demotion, ticket generation, and exclusion.
- Review each of the 150 catalogue entries for provenance, relevance, policy, permission, and connector classification. Do not equate discovery rows with researched/approved rows.

**Tests:** dataset evaluation, metric normalization, scheduler time-control cases, catalogue validation/review checks.

**Exit gate:** Outstanding metrics are dated, versioned, and reproducible.

## Phase 7 — Submission package and live review

**Goal:** ensure the form reflects observed facts.

- Populate the supplied workbook only from evidence created in Phases 1–6.
- Produce an unedited ≤8 minute demo: cold start, Passport/policy block, safe publish or simulation, evidence lookup, injected failure/demotion, tests.
- Create the private repository and grant reviewer access only when you authorize it. Keep the local demo reachable during review if an external live URL is mandatory; do not represent a loopback URL as public.

**Exit gate:** every workbook claim links to a repository file, saved output, log, or video timestamp; declarations remain a candidate-only action.
