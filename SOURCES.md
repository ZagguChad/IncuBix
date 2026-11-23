# Sources and third-party components

## Platform APIs and infrastructure

- Discourse API documentation: https://docs.discourse.org/ (API key authentication; topic creation, read, delete).
- Lemmy API documentation: https://join-lemmy.org/docs/contributors/04-api.html and https://join-lemmy.org/api/main (local instance API; test instance endpoints must be local/approved).
- Forem / DEV API documentation: https://developers.forem.com/api/v0.
- Forem current API v1 docs: https://developers.forem.com/api/v1 (the unpublish operation is a PUT and may require an admin/moderator role; the connector uses unpublish because the platform does not expose a general hard-delete operation for normal article authors).
- Ghost Admin API documentation: https://docs.ghost.org/admin-api/.
- Slack Web API `chat.postMessage`, `chat.getPermalink`, and `chat.delete`: https://docs.slack.dev/reference/methods/chat.postMessage/, https://docs.slack.dev/reference/methods/chat.getPermalink/, https://docs.slack.dev/reference/methods/chat.delete/.
- PeerTube API video import and video lifecycle: https://docs.joinpeertube.org/api-rest-reference/api/video-upload.html and https://docs.joinpeertube.org/api-rest-reference/api/video.html. Remote video imports are only enabled for explicitly approved source hosts.
- Hashnode GraphQL API and plan/access requirements: https://github.com/Hashnode/gql-skill/blob/main/skills/gql-api/references/mutations.md and https://hashnode.com/changelog/2026-05-13-graphql-api-paid-access.
- Local Lemmy Compose/configuration reference: https://join-lemmy.org/docs/administration/install_docker.html and https://join-lemmy.org/docs/administration/configuration.html.
- Ghost hosting reference: https://docs.ghost.org/hosting/.

## Libraries and runtime

The project uses Python 3.11+ and declares supported dependency ranges in `pyproject.toml`. The container build uses the exact runtime and test pins in `requirements.lock` and `requirements-test.lock`; refresh both from a clean supported build environment when dependencies change.

- FastAPI and Starlette (MIT); Uvicorn (BSD-3-Clause); Pydantic and pydantic-settings (MIT).
- SQLAlchemy (MIT); psycopg 3 (LGPL-3.0-or-later); PostgreSQL server (PostgreSQL License).
- HTTPX (BSD-3-Clause); MinIO Python client (Apache-2.0); PyYAML (MIT); PyJWT (MIT).
- pytest and pytest-asyncio (MIT); httpx2 and httpcore2 (BSD-3-Clause); jsonschema (MIT). Test pins are recorded in `requirements-test.lock`.
- Moto's S3-compatible local test server (Apache-2.0); it is used only in the local Compose test stack, not as a shipped service.
- Docker base/runtime images: `python:3.12-slim`, `postgres:16-alpine`, `mysql:8.0`, `ghost:5-alpine`, `dessalines/lemmy:1.0.0-beta.1`, and `asonix/pictrs:0.5.25`. Each upstream image has its own license; the platform containers are separate local test services and are not linked into the app.

## Media/data/models

- No external media, dataset, AI model, or generated content is included in the default service path.
- Uploaded or URL-provided files are user-supplied. Their source/license is recorded but not independently verified by the application. Do not test with content you do not own or have rights to use.
- Simulation outputs are synthetic test records and are labeled as simulated.
