from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator
from sqlalchemy import select

from incubrix import jobs, main
from incubrix.config import get_settings
from incubrix.db import Base, SessionLocal, engine, initialize_database
from incubrix.errors import ServiceError
from incubrix.models import (
    CertificationRun,
    Destination,
    IdempotencyRecord,
    Job,
    Post,
    RecertificationTicket,
    RuleProposal,
)
from incubrix.network import validate_http_url
from incubrix.schemas import DestinationCreate
from incubrix.util import canonical_json


def test_local_storage_endpoint_cannot_be_empty(monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT_URL", "")
    get_settings.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="S3_ENDPOINT_URL"):
            get_settings().validate_runtime()
    finally:
        monkeypatch.undo()
        get_settings.cache_clear()


def test_destination_passport_and_explainable_policy_are_idempotent():
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    with TestClient(main.app) as client:
        created = client.post("/api/v1/destinations", headers={"Idempotency-Key": "passport-destination"}, json={
            "name": "Policy Community", "url": "http://localhost", "platform_key": "discourse",
            "surface": "topic", "category": "developer", "audience_tags": ["AI"], "connector_key": "discourse",
            "rules_url": "http://localhost/rules",
            "rules": {"self_promo_allowed": False, "link_policy": "allowed", "disclosure_required": True,
                      "automation_allowed": True, "permission_status": "verified", "content_types": ["text"]},
        })
        assert created.status_code == 201
        destination_id = created.json()["destination_id"]

        passport = client.get(f"/api/v1/destinations/{destination_id}/passport")
        assert passport.status_code == 200
        assert passport.json()["permission"]["status"] == "verified"
        assert passport.json()["policy"]["source_url"] == "http://localhost/rules"
        assert passport.json()["trust_score"] is None

        request = {"destination_id": destination_id, "rendition": {"body": "Buy our product", "is_promotional": True}}
        first = client.post("/api/v1/policy-evaluations", headers={"Idempotency-Key": "policy-explain"}, json=request)
        second = client.post("/api/v1/policy-evaluations", headers={"Idempotency-Key": "policy-explain"}, json=request)
        assert first.status_code == second.status_code == 200
        decision = first.json()["policy_evaluation"]["decision"]
        assert first.json()["policy_evaluation"]["allowed"] is False
        assert decision["rule"] == "self_promotion"
        assert decision["policy_source_url"] == "http://localhost/rules"
        assert "Remove promotional" in decision["recommended_action"]


class FakeStorage:
    def __init__(self):
        self.objects = {}

    def ensure_bucket(self):
        return None

    def put_json(self, key, value):
        self.objects[key] = value
        return f"s3://incubrix-test/{key}"

    def get_json(self, key):
        return self.objects[key]

    def put_bytes(self, key, body, content_type, *, filename):
        return f"s3://incubrix-test/{key}"

    def uri(self, key):
        return f"s3://incubrix-test/{key}"

    def download_url(self, key):
        return f"http://localhost:5000/incubrix-test/{key}?signature=test"


def destination_payload(*, frequency_cap=None):
    rules = {
        "self_promo_allowed": True,
        "link_policy": "allowed",
        "disclosure_required": False,
        "automation_allowed": True,
        "content_types": ["text"],
        "spec": {"title_max_chars": 255, "body_max_chars": 32_000},
    }
    if frequency_cap:
        rules["frequency_cap"] = frequency_cap
    return {
        "name": "Hardened destination",
        "url": "http://localhost",
        "platform_key": "discourse",
        "surface": "topic",
        "category": "developer",
        "connector_key": "discourse",
        "rules": rules,
    }


def test_connection_references_cannot_read_arbitrary_environment_variables():
    payload = destination_payload()
    payload["connection"] = {"api_key_env": "DATABASE_URL"}

    try:
        DestinationCreate.model_validate(payload)
    except ValueError as exc:
        assert "allowed secret reference" in str(exc)
    else:
        raise AssertionError("an arbitrary environment variable reference was accepted")


def test_live_connector_hosts_are_an_explicit_allowlist():
    try:
        validate_http_url(
            "http://localhost",
            purpose="connector endpoint",
            allowed_hosts=set(),
        )
    except ServiceError as exc:
        assert exc.code == "DEST_SPEC_VIOLATION"
        assert "allowlist" in exc.message
    else:
        raise AssertionError("an unapproved connector host was accepted")


def test_remote_artifact_fetch_requires_outbound_host_allowlist(monkeypatch):
    settings = get_settings().model_copy(update={
        "artifact_source_fetch_enabled": True,
        "outbound_allowed_hosts": "",
    })
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as client:
        response = client.post(
            "/api/v1/artifacts",
            headers={"Idempotency-Key": "blocked-artifact-fetch"},
            data={"source_url": "https://untrusted.example/file.txt"},
        )
    assert response.status_code == 422
    assert "allowlist" in response.json()["error"]["message"]


def test_remote_artifact_manifest_redacts_signed_url_query(monkeypatch):
    settings = get_settings().model_copy(update={
        "artifact_source_fetch_enabled": True,
        "outbound_allowed_hosts": "files.example",
    })
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(main, "validate_http_url", lambda url, **kwargs: url)
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)

    class Response:
        is_redirect = False
        is_success = True
        status_code = 200

        def __init__(self):
            self.headers = {"content-type": "text/plain"}

        async def aiter_bytes(self):
            yield b"file content"

    class StreamContext:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def stream(self, method, url):
            return StreamContext()

    monkeypatch.setattr(main.httpx, "AsyncClient", FakeClient)
    with TestClient(main.app) as client:
        response = client.post(
            "/api/v1/artifacts",
            headers={"Idempotency-Key": "signed-artifact-source"},
            data={"source_url": "https://files.example/private/archive.zip?token=secret-query-value"},
        )
    assert response.status_code == 201
    result = response.json()
    assert result["source_url"] == "https://files.example/private/archive.zip"
    assert "secret-query-value" not in json.dumps(result)


def test_production_mode_requires_api_key(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_API_KEY", "correct-horse-battery-staple-2026")
    # This test exercises authentication, not the Compose-managed local store.
    monkeypatch.setattr(main, "get_storage", lambda: type("Storage", (), {"ensure_bucket": lambda self: None})())
    get_settings.cache_clear()
    try:
        with TestClient(main.app) as client:
            denied = client.get("/api/v1/connectors")
            allowed = client.get("/api/v1/connectors", headers={"X-API-Key": "correct-horse-battery-staple-2026"})
            health = client.get("/health")
        assert denied.status_code == 401
        assert denied.json()["error"]["code"] == "UNAUTHORIZED"
        assert allowed.status_code == 200
        assert health.status_code == 200

        production_settings = get_settings().model_copy(update={
            "app_env": "production",
            "app_api_key": "correct-horse-battery-staple-2026",
        })
        production_docs = FastAPI(**main._documentation_route_options(production_settings))
        with TestClient(production_docs) as docs_client:
            assert production_docs.openapi_url is None
            assert docs_client.get("/openapi.json").status_code == 404
            assert docs_client.get("/docs").status_code == 404
            assert docs_client.get("/redoc").status_code == 404
    finally:
        monkeypatch.undo()
        get_settings.cache_clear()


def test_completion_callback_is_allowlisted_and_hmac_signed(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    monkeypatch.setenv("CALLBACK_ALLOWED_HOSTS", "callback.example")
    monkeypatch.setenv("JOB_CALLBACK_SIGNING_SECRET", "callback-test-secret")
    get_settings.cache_clear()
    monkeypatch.setattr(jobs, "validate_http_url", lambda value, **kwargs: value)
    received = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received["url"] = str(request.url)
        received["body"] = request.content
        received["signature"] = request.headers["X-IncuBrix-Signature-256"]
        return httpx.Response(204)

    real_client = jobs.httpx.Client
    monkeypatch.setattr(
        jobs.httpx,
        "Client",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    try:
        with SessionLocal() as session:
            job = Job(
                kind="publish",
                status="succeeded",
                progress=100,
                payload_json=canonical_json({"callback_url": "https://callback.example/jobs"}),
                result_json=canonical_json({"post_id": "post-1"}),
                finished_at=datetime.now(UTC),
            )
            session.add(job)
            session.commit()
            job_id = job.id

        jobs._deliver_completion_callback(job_id)

        assert received["url"] == "https://callback.example/jobs"
        assert json.loads(received["body"])["event"] == "job.completed"
        expected = hmac.new(b"callback-test-secret", received["body"], hashlib.sha256).hexdigest()
        assert received["signature"] == f"sha256={expected}"
    finally:
        get_settings.cache_clear()


def test_frequency_cap_reserves_capacity_before_async_publish(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)

    with TestClient(main.app) as client:
        created = client.post(
            "/api/v1/destinations",
            headers={"Idempotency-Key": "cap-destination"},
            json=destination_payload(frequency_cap={"max_posts": 1, "window_seconds": 3600}),
        )
        destination_id = created.json()["destination_id"]
        payload = {"destination_id": destination_id, "rendition": {"body": "A single permitted post."}}
        first = client.post("/api/v1/publishes", headers={"Idempotency-Key": "cap-first"}, json=payload)
        second = client.post("/api/v1/publishes", headers={"Idempotency-Key": "cap-second"}, json=payload)

    assert first.status_code == 202
    assert second.status_code == 429
    assert second.json()["error"]["code"] == "DEST_RULE_FREQUENCY_CAP"


def test_expired_job_lease_is_reclaimable_but_live_lease_is_not():
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    with SessionLocal() as session:
        job = Job(kind="certification", status="queued", progress=0, payload_json="{}")
        session.add(job)
        session.commit()
        job_id = job.id

    first = jobs.claim_next_job()
    assert first is not None and first.id == job_id
    assert jobs.claim_next_job() is None

    with SessionLocal() as session:
        job = session.get(Job, job_id)
        job.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        session.commit()

    reclaimed = jobs.claim_next_job()
    assert reclaimed is not None and reclaimed.id == job_id
    assert reclaimed.attempts == 2


def test_expired_publish_lease_is_blocked_for_manual_reconciliation():
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    with SessionLocal() as session:
        destination = Destination(
            name="Ambiguous publish destination", url="https://example.test", normalized_url="https://example.test",
            platform_key="example", surface="topic", category="developer", connector_key="discourse",
        )
        session.add(destination)
        session.flush()
        job = Job(
            kind="publish", status="running", progress=25,
            lease_expires_at=datetime.now(UTC) - timedelta(seconds=1), payload_json="{}",
        )
        session.add(job)
        session.flush()
        post = Post(
            destination_id=destination.id, job_id=job.id, idempotency_key="same-key",
            request_hash="a" * 64, rendition_json='{"body":"test"}', status="publishing",
        )
        session.add(post)
        session.commit()
        job_id, post_id = job.id, post.id

    assert jobs.claim_next_job() is None
    with SessionLocal() as session:
        job = session.get(Job, job_id)
        post = session.get(Post, post_id)
        assert job.status == "failed"
        assert '"ambiguous_publish_outcome"' in job.error_json
        assert post.status == "needs_review"


def test_link_policies_detect_case_variants_and_reject_extra_canonical_links(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)

    no_links = destination_payload()
    no_links["rules"]["link_policy"] = "no_links"
    canonical_only = destination_payload()
    canonical_only["name"] = "Canonical destination"
    canonical_only["url"] = "http://localhost/canonical"
    canonical_only["rules"]["link_policy"] = "canonical_only"
    no_promotion = destination_payload()
    no_promotion["name"] = "No promotion destination"
    no_promotion["url"] = "http://localhost/no-promotion"
    no_promotion["rules"]["self_promo_allowed"] = False
    disclosure = destination_payload()
    disclosure["name"] = "Disclosure destination"
    disclosure["url"] = "http://localhost/disclosure"
    disclosure["rules"]["disclosure_required"] = True
    with TestClient(main.app) as client:
        blocked = client.post("/api/v1/destinations", headers={"Idempotency-Key": "no-links"}, json=no_links)
        assert blocked.status_code == 201
        no_link_result = client.post(
            f"/api/v1/destinations/{blocked.json()['destination_id']}/validate",
            headers={"Idempotency-Key": "no-links-validate"},
            json={"rendition": {"body": "Read HTTPS://EXAMPLE.COM/guide"}},
        )
        assert no_link_result.status_code == 422
        bare_domain_result = client.post(
            f"/api/v1/destinations/{blocked.json()['destination_id']}/validate",
            headers={"Idempotency-Key": "no-links-bare-domain"},
            json={"rendition": {"body": "Visit example.com for details"}},
        )
        assert bare_domain_result.status_code == 422

        no_promotion_created = client.post(
            "/api/v1/destinations", headers={"Idempotency-Key": "no-promo"}, json=no_promotion
        )
        promotional = client.post(
            f"/api/v1/destinations/{no_promotion_created.json()['destination_id']}/validate",
            headers={"Idempotency-Key": "no-promo-validate"},
            json={"rendition": {"body": "A post", "is_promotional": True}},
        )
        assert promotional.status_code == 422
        assert promotional.json()["error"]["details"]["rule"] == "self_promotion"

        disclosure_created = client.post(
            "/api/v1/destinations", headers={"Idempotency-Key": "disclosure"}, json=disclosure
        )
        missing_disclosure = client.post(
            f"/api/v1/destinations/{disclosure_created.json()['destination_id']}/validate",
            headers={"Idempotency-Key": "disclosure-validate"},
            json={"rendition": {"body": "AI text", "ai_generated": True}},
        )
        assert missing_disclosure.status_code == 422
        assert missing_disclosure.json()["error"]["details"]["field"] == "ai_disclosure"

        canonical = client.post("/api/v1/destinations", headers={"Idempotency-Key": "canonical-links"}, json=canonical_only)
        assert canonical.status_code == 201
        canonical_result = client.post(
            f"/api/v1/destinations/{canonical.json()['destination_id']}/validate",
            headers={"Idempotency-Key": "canonical-links-validate"},
            json={"rendition": {"body": "[good](https://example.com/article/) and https://other.example/page", "canonical_url": "https://example.com/article"}},
        )
        assert canonical_result.status_code == 422


def test_normalized_destination_identity_rejects_equivalent_urls(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    with TestClient(main.app) as client:
        first = destination_payload()
        first["url"] = "https://Example.COM:443/community/#about"
        created = client.post("/api/v1/destinations", headers={"Idempotency-Key": "normal-one"}, json=first)
        assert created.status_code == 201
        duplicate = destination_payload()
        duplicate["connector_key"] = "lemmy"
        duplicate["url"] = "https://example.com/community"
        rejected = client.post("/api/v1/destinations", headers={"Idempotency-Key": "normal-two"}, json=duplicate)
        assert rejected.status_code == 409
        assert rejected.json()["error"]["code"] == "DESTINATION_CONFLICT"


def test_recommendation_tailoring_and_confirmed_rule_proposal(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)
    with TestClient(main.app) as client:
        destination = destination_payload()
        destination["audience_tags"] = ["Python", "AI"]
        destination["rules"]["spec"] = {"title_max_chars": 5, "body_max_chars": 10}
        destination["rules"]["disclosure_required"] = True
        created = client.post("/api/v1/destinations", headers={"Idempotency-Key": "rec-destination"}, json=destination)
        assert created.status_code == 201
        destination_id = created.json()["destination_id"]

        recommendations = client.post("/api/v1/recommendations", headers={"Idempotency-Key": "rec-request"}, json={"audience_tags": ["ai"], "language": "en"})
        assert recommendations.status_code == 200
        assert recommendations.json()["items"][0]["destination"]["destination_id"] == destination_id
        feedback = client.post("/api/v1/recommendations/rec-request/feedback", headers={"Idempotency-Key": "rec-feedback"}, json={
            "destination_id": destination_id, "accepted": True,
        })
        assert feedback.status_code == 201
        metrics = client.get("/api/v1/recommendations/metrics")
        assert metrics.status_code == 200
        assert metrics.json() == {"feedback_count": 1, "accepted_count": 1, "precision": 1.0}

        rule_filtered = client.post(
            "/api/v1/recommendations",
            headers={"Idempotency-Key": "rec-rule-filtered"},
            json={
                "audience_tags": ["ai"],
                "language": "en",
                "rendition": {"body": "AI draft", "ai_generated": True},
            },
        )
        assert rule_filtered.status_code == 200
        assert rule_filtered.json()["items"] == []

        unrelated = destination_payload()
        unrelated["name"] = "Unrelated destination"
        unrelated["url"] = "http://localhost/unrelated"
        unrelated_created = client.post(
            "/api/v1/destinations",
            headers={"Idempotency-Key": "unrelated-destination"},
            json=unrelated,
        )
        assert unrelated_created.status_code == 201
        invalid_feedback = client.post(
            "/api/v1/recommendations/rec-request/feedback",
            headers={"Idempotency-Key": "invalid-rec-feedback"},
            json={"destination_id": unrelated_created.json()["destination_id"], "accepted": True},
        )
        assert invalid_feedback.status_code == 422

        tailored = client.post("/api/v1/tailor", headers={"Idempotency-Key": "tailor-request"}, json={
            "destination_id": destination_id,
            "rendition": {"title": "long title", "body": "long body beyond limit", "ai_generated": True},
        })
        assert tailored.status_code == 200
        assert tailored.json()["rendition"]["title"] == "long"
        assert tailored.json()["rendition"]["body"] == "long body"
        assert tailored.json()["rendition"]["ai_disclosure"]

        proposal = client.post("/api/v1/rule-proposals", headers={"Idempotency-Key": "proposal-request"}, json={
            "destination_id": destination_id,
            "rules_text": "No links. Manual approval is required. One post per day.",
        })
        assert proposal.status_code == 201
        assert proposal.json()["requires_human_confirmation"] is True
        confirmed = client.post(
            f"/api/v1/rule-proposals/{proposal.json()['proposal_id']}/confirm",
            headers={"Idempotency-Key": "proposal-confirm"},
        )
        assert confirmed.status_code == 200
        retrieved = client.get(f"/api/v1/destinations/{destination_id}").json()
        assert retrieved["link_policy"] == "no_links"
        assert retrieved["automation_allowed"] is False


def test_rule_proposals_default_to_blocked_and_need_explicit_allowance():
    unknown, _ = main._propose_rules("Community rules are unclear.")
    assert unknown["self_promo_allowed"] is False
    assert unknown["automation_allowed"] is False

    allowed, _ = main._propose_rules("Self-promotion is allowed. Automated posting is allowed.")
    assert allowed["self_promo_allowed"] is True
    assert allowed["automation_allowed"] is True

    conflict, _ = main._propose_rules("No self-promotion is allowed. No automated posting is allowed.")
    assert conflict["self_promo_allowed"] is False
    assert conflict["automation_allowed"] is False

    prohibited, _ = main._propose_rules(
        "Self-promotion is prohibited. Automated posting is prohibited."
    )
    assert prohibited["self_promo_allowed"] is False
    assert prohibited["automation_allowed"] is False


def test_rule_page_capture_extracts_proposals_and_redacts_url_query(monkeypatch):
    settings = get_settings().model_copy(update={
        "rule_page_fetch_enabled": True,
        "outbound_allowed_hosts": "rules.example",
    })
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    fetches = []

    def allow_test_host(url, *, allowed_hosts=None, **kwargs):
        assert allowed_hosts == {"rules.example"}
        return url

    monkeypatch.setattr(main, "validate_http_url", allow_test_host)

    class Response:
        is_redirect = False
        is_success = True
        status_code = 200
        encoding = "utf-8"

        def __init__(self):
            self.headers = {"content-type": "text/html; charset=utf-8", "content-length": "220"}

        async def aiter_bytes(self):
            yield (
                b"<html><head><script>automated posting is allowed</script></head><body>"
                b"<main><h1>Community rules</h1><p>No links are allowed.</p>"
                b"<p>Manual approval is required. Two posts per day.</p></main></body></html>"
            )

    class StreamContext:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def stream(self, method, url):
            assert method == "GET"
            assert "token=private-value" in url
            fetches.append(url)
            return StreamContext()

    monkeypatch.setattr(main.httpx, "AsyncClient", FakeClient)
    with TestClient(main.app) as client:
        payload = {"source_url": "https://rules.example/community?token=private-value"}
        response = client.post(
            "/api/v1/rule-proposals",
            headers={"Idempotency-Key": "rules-page-proposal"},
            json=payload,
        )
        replay = client.post(
            "/api/v1/rule-proposals",
            headers={"Idempotency-Key": "rules-page-proposal"},
            json=payload,
        )
    assert response.status_code == 201
    proposal = response.json()
    assert proposal["source_kind"] == "page"
    assert proposal["source_url"] == "https://rules.example/community"
    assert proposal["source_sha256"]
    assert proposal["rules"]["link_policy"] == "no_links"
    assert proposal["rules"]["automation_allowed"] is False
    assert proposal["rules"]["frequency_cap"] == {"max_posts": 2, "window_seconds": 86400}
    assert proposal["requires_human_confirmation"] is True
    assert "private-value" not in json.dumps(proposal)
    assert replay.json() == proposal
    assert len(fetches) == 1

    with SessionLocal() as session:
        saved = session.get(RuleProposal, proposal["proposal_id"])
        assert saved.source_url == "https://rules.example/community"


def test_rule_page_capture_rejects_unapproved_hosts_without_fetching(monkeypatch):
    settings = get_settings().model_copy(update={
        "rule_page_fetch_enabled": True,
        "outbound_allowed_hosts": "approved.example",
    })
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as client:
        response = client.post(
            "/api/v1/rule-proposals",
            headers={"Idempotency-Key": "blocked-rules-page"},
            json={"source_url": "https://unapproved.example/rules"},
        )
    assert response.status_code == 422
    assert "allowlist" in response.json()["error"]["message"]


def test_rule_page_capture_rejects_redirects(monkeypatch):
    settings = get_settings().model_copy(update={
        "rule_page_fetch_enabled": True,
        "outbound_allowed_hosts": "rules.example",
    })
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(main, "validate_http_url", lambda url, **kwargs: url)

    class Response:
        is_redirect = True
        is_success = False
        status_code = 302

        def __init__(self):
            self.headers = {"location": "http://127.0.0.1/private"}

    class StreamContext:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class RedirectClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def stream(self, method, url):
            return StreamContext()

    monkeypatch.setattr(main.httpx, "AsyncClient", RedirectClient)
    with TestClient(main.app) as client:
        response = client.post(
            "/api/v1/rule-proposals",
            headers={"Idempotency-Key": "redirect-rules-page"},
            json={"source_url": "https://rules.example/rules"},
        )
    assert response.status_code == 422
    assert "Redirecting" in response.json()["error"]["message"]


def test_rule_proposal_requires_exactly_one_source():
    with TestClient(main.app) as client:
        missing = client.post(
            "/api/v1/rule-proposals",
            headers={"Idempotency-Key": "missing-rule-source"},
            json={},
        )
        both = client.post(
            "/api/v1/rule-proposals",
            headers={"Idempotency-Key": "two-rule-sources"},
            json={"rules_text": "No links.", "source_url": "https://rules.example/community"},
        )
    assert missing.status_code == 422
    assert both.status_code == 422


def test_chunked_request_without_content_length_is_bounded(monkeypatch):
    settings = get_settings().model_copy(update={"max_request_bytes": 1024})
    monkeypatch.setattr(main, "get_settings", lambda: settings)
    with TestClient(main.app) as client:
        response = client.post(
            "/api/v1/rule-proposals",
            content=iter([b"x" * 700, b"y" * 400]),
            headers={"Transfer-Encoding": "chunked"},
        )
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "DEST_SPEC_VIOLATION"


def test_expired_certification_blocks_publish_and_opens_ticket(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)
    with TestClient(main.app) as client:
        created = client.post("/api/v1/destinations", headers={"Idempotency-Key": "expired-destination"}, json=destination_payload())
        destination_id = created.json()["destination_id"]
        with SessionLocal() as session:
            destination = session.get(Destination, destination_id)
            destination.level = "L3"
            destination.last_certified_at = datetime.now(UTC) - timedelta(days=31)
            session.commit()
        blocked = client.post("/api/v1/publishes", headers={"Idempotency-Key": "expired-publish"}, json={
            "destination_id": destination_id, "rendition": {"body": "Must recertify first."},
        })
        assert blocked.status_code == 409
        tickets = client.get("/api/v1/recertification-tickets").json()["items"]
        assert tickets and tickets[0]["destination_id"] == destination_id


def test_due_recertification_is_queued_once_before_expiry(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    monkeypatch.setattr(
        jobs, "get_settings",
        lambda: get_settings().model_copy(update={"simulation_mode": False}),
    )
    certified_at = datetime.now(UTC) - timedelta(days=28)
    with SessionLocal() as session:
        destination = Destination(
            name="Due certification", url="http://localhost", normalized_url="http://localhost",
            platform_key="discourse", surface="topic", category="developer", connector_key="discourse",
            level="L3", last_certified_at=certified_at,
        )
        session.add(destination)
        session.commit()
        destination_id = destination.id

    assert jobs.sweep_due_recertifications() == 1
    assert jobs.sweep_due_recertifications() == 0

    with SessionLocal() as session:
        scheduled_jobs = session.scalars(select(Job).where(Job.kind == "certification")).all()
        assert len(scheduled_jobs) == 1
        payload = json.loads(scheduled_jobs[0].payload_json)
        assert payload == {"destination_id": destination_id, "scheduled_recertification": True}
        runs = session.scalars(select(CertificationRun).where(CertificationRun.job_id == scheduled_jobs[0].id)).all()
        assert len(runs) == 1
        keys = session.scalars(select(IdempotencyRecord).where(IdempotencyRecord.scope == f"recertification:{destination_id}")).all()
        assert len(keys) == 1


def test_due_recertification_is_not_queued_in_simulation_mode(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    with SessionLocal() as session:
        session.add(Destination(
            name="Simulation must not certify", url="http://localhost", normalized_url="http://localhost",
            platform_key="discourse", surface="topic", category="developer", connector_key="discourse",
            level="L3", last_certified_at=datetime.now(UTC) - timedelta(days=28),
        ))
        session.commit()
    monkeypatch.setattr(
        jobs, "get_settings",
        lambda: get_settings().model_copy(update={"simulation_mode": True}),
    )

    assert jobs.sweep_due_recertifications() == 0


def test_simulated_certification_does_not_extend_live_certification(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "resolve_connection", lambda connector_key, stored: {})
    monkeypatch.setattr(jobs, "_certification_host_allowed", lambda destination: True)
    monkeypatch.setattr(
        jobs, "get_settings",
        lambda: get_settings().model_copy(update={"simulation_mode": True}),
    )

    class ValidConnector:
        version = "test"

        def validate(self, rendition, target, connection):
            return {"ok": True, "violations": []}

        def publish(self, rendition, target, connection, idempotency_key):
            return {"platform_post_id": "simulated-post", "live_url": "https://example.test/post"}

        def get_status(self, platform_post_id, connection):
            return {"status": "published", "live_url": "https://example.test/post"}

        def delete(self, platform_post_id, connection):
            return {"deleted": True}

    monkeypatch.setattr(jobs, "get_connector", lambda key, connection: ValidConnector())
    last_certified = datetime.now(UTC) - timedelta(days=28)
    with SessionLocal() as session:
        destination = Destination(
            name="Simulation date guard", url="http://localhost", normalized_url="http://localhost",
            platform_key="discourse", surface="topic", category="developer", connector_key="discourse",
            level="L3", last_certified_at=last_certified,
        )
        session.add(destination)
        session.flush()
        job = Job(kind="certification", status="running", payload_json=canonical_json({"destination_id": destination.id}))
        session.add(job)
        session.flush()
        run = CertificationRun(
            destination_id=destination.id, job_id=job.id, connector_version="test", status="running",
        )
        session.add(run)
        session.commit()
        destination_id, job_id = destination.id, job.id

    job = Job(id=job_id, kind="certification", payload_json=canonical_json({"destination_id": destination_id}))
    result = jobs.process_certification(job)
    assert result["status"] == "passed"

    with SessionLocal() as session:
        destination = session.get(Destination, destination_id)
        assert destination.last_certified_at.replace(tzinfo=UTC) == last_certified
        assert destination.level == "L3"


def test_failed_scheduled_recertification_opens_operator_ticket(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "resolve_connection", lambda connector_key, stored: {})
    monkeypatch.setattr(jobs, "_certification_host_allowed", lambda destination: True)
    monkeypatch.setattr(
        jobs, "get_settings",
        lambda: get_settings().model_copy(update={"simulation_mode": False}),
    )

    class InvalidConnector:
        version = "test"

        def validate(self, rendition, target, connection):
            return {"ok": False, "violations": [{"field": "body", "code": "rejected"}]}

    monkeypatch.setattr(jobs, "get_connector", lambda key, connection: InvalidConnector())
    with SessionLocal() as session:
        destination = Destination(
            name="Failed scheduled certification", url="http://localhost", normalized_url="http://localhost",
            platform_key="discourse", surface="topic", category="developer", connector_key="discourse",
            level="L3", last_certified_at=datetime.now(UTC) - timedelta(days=28),
        )
        session.add(destination)
        session.commit()
        destination_id = destination.id

    assert jobs.sweep_due_recertifications() == 1
    with SessionLocal() as session:
        job = session.scalar(select(Job).where(Job.kind == "certification"))
        job_id = job.id
        payload = job.payload_json

    failed_job = Job(id=job_id, kind="certification", payload_json=payload)
    jobs.process_job(failed_job)

    with SessionLocal() as session:
        stored_job = session.get(Job, job_id)
        assert stored_job.status == "failed"
        ticket = session.scalar(select(RecertificationTicket).where(
            RecertificationTicket.destination_id == destination_id,
            RecertificationTicket.status == "open",
        ))
        assert ticket is not None
        assert "Scheduled pre-expiry certification failed" in ticket.reason


def test_certification_cleanup_sweeper_deletes_persisted_test_post(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()

    class CleanupConnector:
        def delete(self, platform_post_id, connection):
            assert platform_post_id == "platform-test-post"
            return {"deleted": True}

    monkeypatch.setattr(jobs, "get_connector", lambda key, connection: CleanupConnector())
    with SessionLocal() as session:
        destination = Destination(
            name="Certification cleanup", url="https://cleanup.example", normalized_url="https://cleanup.example",
            platform_key="cleanup", surface="topic", category="developer", connector_key="discourse",
        )
        session.add(destination)
        session.flush()
        job = Job(kind="certification", status="failed", progress=100, payload_json="{}")
        session.add(job)
        session.flush()
        run = CertificationRun(
            destination_id=destination.id, job_id=job.id, connector_version="1.0.0", status="failed",
            platform_post_id="platform-test-post", cleanup_deadline_at=datetime.now(UTC) - timedelta(seconds=1),
        )
        session.add(run)
        session.commit()
        run_id = run.id

    assert jobs.sweep_certification_cleanup() == 1
    with SessionLocal() as session:
        run = session.get(CertificationRun, run_id)
        assert run.cleanup_confirmed is True
        assert run.cleanup_deadline_at is None
        assert "recovery_delete" in run.steps_json


def test_runtime_destination_and_job_responses_match_public_schemas(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)
    root = Path(__file__).resolve().parents[1]
    destination_schema = json.loads((root / "schemas" / "destination.schema.json").read_text())
    job_schema = json.loads((root / "schemas" / "job.schema.json").read_text())

    with TestClient(main.app) as client:
        created = client.post(
            "/api/v1/destinations",
            headers={"Idempotency-Key": "schema-destination"},
            json=destination_payload(),
        )
        Draft202012Validator(destination_schema, format_checker=Draft202012Validator.FORMAT_CHECKER).validate(created.json())
        queued = client.post(
            "/api/v1/publishes",
            headers={"Idempotency-Key": "schema-publish"},
            json={"destination_id": created.json()["destination_id"], "rendition": {"body": "Schema test."}},
        )
        response = client.get(f"/api/v1/jobs/{queued.json()['job_id']}").json()

    Draft202012Validator(job_schema, format_checker=Draft202012Validator.FORMAT_CHECKER).validate(response)


def test_destination_validation_route_is_present_and_does_not_enqueue_work(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)

    with TestClient(main.app) as client:
        created = client.post(
            "/api/v1/destinations",
            headers={"Idempotency-Key": "validate-destination"},
            json=destination_payload(),
        )
        result = client.post(
            f"/api/v1/destinations/{created.json()['destination_id']}/validate",
            headers={"Idempotency-Key": "validate-destination-request"},
            json={"rendition": {"body": "Validate without publishing."}},
        )

    assert result.status_code == 200
    assert result.json() == {"ok": True, "violations": [], "warnings": []}
    with SessionLocal() as session:
        assert session.query(Job).count() == 0
