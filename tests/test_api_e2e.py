import json
import time
from pathlib import Path

from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator

from incubrix import jobs, main
from incubrix.db import Base, engine, initialize_database


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
        self.objects[key] = body
        return f"s3://incubrix-test/{key}"

    def uri(self, key):
        return f"s3://incubrix-test/{key}"

    def download_url(self, key):
        return f"http://localhost:5000/incubrix-test/{key}?signature=test"


def _destination_payload(connector_key="discourse", *, automation_allowed=True):
    return {
        "name": f"Test {connector_key}", "url": "http://localhost", "platform_key": connector_key,
        "surface": "topic", "category": "developer", "audience_tags": ["AI"], "connector_key": connector_key,
        "rules": {"self_promo_allowed": True, "link_policy": "allowed", "disclosure_required": False,
                  "automation_allowed": automation_allowed, "content_types": ["text"],
                  "spec": {"title_max_chars": 255, "body_max_chars": 32000}},
    }


def test_publish_job_e2e_is_idempotent_and_writes_manifest(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)

    with TestClient(main.app) as client:
        created = client.post("/api/v1/destinations", headers={"Idempotency-Key": "dest-1"}, json={
            "name": "Test Discourse", "url": "http://discourse", "platform_key": "discourse", "surface": "topic",
            "category": "developer", "audience_tags": ["AI", "video"], "connector_key": "discourse",
            "rules": {"self_promo_allowed": True, "link_policy": "allowed", "disclosure_required": False,
                      "automation_allowed": True, "content_types": ["text"], "spec": {"title_max_chars": 255, "body_max_chars": 32000}},
        })
        assert created.status_code == 201
        destination_id = created.json()["destination_id"]
        request = {"destination_id": destination_id, "rendition": {"title": "Hello", "body": "Test body", "is_promotional": False}}
        first = client.post("/api/v1/publishes", headers={"Idempotency-Key": "pub-1"}, json=request)
        second = client.post("/api/v1/publishes", headers={"Idempotency-Key": "pub-1"}, json=request)
        assert first.status_code == second.status_code == 202
        assert first.json()["job_id"] == second.json()["job_id"]
        job_id = first.json()["job_id"]
        deadline = time.monotonic() + 8
        result = None
        while time.monotonic() < deadline:
            result = client.get(f"/api/v1/jobs/{job_id}").json()
            if result["status"] in {"succeeded", "failed"}:
                break
            time.sleep(0.05)
        assert result["status"] == "succeeded", result
        assert result["result"]["platform_post_id"].startswith("sim-")
        assert result["result"]["evidence_id"] == f"publish:{job_id}"
        assert result["result"]["manifest_uri"].startswith("s3://incubrix-test/manifests/")
        assert result["result"]["manifest"]["providers"][0]["key"] == "discourse"
        assert result["result"]["manifest"]["evidence"]["content_sha256"]
        assert result["result"]["manifest"]["evidence"]["rules_sha256"]


def test_same_idempotency_key_rejects_different_payload():
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    with TestClient(main.app) as client:
        one = {"name": "One", "url": "http://forum", "platform_key": "discourse", "surface": "topic", "category": "forum", "connector_key": "discourse"}
        first = client.post("/api/v1/destinations", headers={"Idempotency-Key": "same"}, json=one)
        changed = {**one, "name": "Changed"}
        second = client.post("/api/v1/destinations", headers={"Idempotency-Key": "same"}, json=changed)
        assert first.status_code == 201
        assert second.status_code == 409
        assert second.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


def test_catalogue_import_is_idempotent_and_skips_existing_destinations(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)
    csv_body = (
        "name,url,platform_key,surface,category,connector_key,audience_tags,est_audience,level\n"
        'Example Forum,https://community.example/ai,example-forum,community,developer,assisted,"AI|video",1200,L1\n'
    )

    with TestClient(main.app) as client:
        first = client.post("/api/v1/catalogue/import", headers={"Idempotency-Key": "catalogue-import-1"},
                            files={"file": ("destinations.csv", csv_body, "text/csv")})
        replay = client.post("/api/v1/catalogue/import", headers={"Idempotency-Key": "catalogue-import-1"},
                             files={"file": ("destinations.csv", csv_body, "text/csv")})
        duplicate = client.post("/api/v1/catalogue/import", headers={"Idempotency-Key": "catalogue-import-2"},
                                files={"file": ("destinations.csv", csv_body, "text/csv")})
        conflict = client.post("/api/v1/catalogue/import", headers={"Idempotency-Key": "catalogue-import-1"},
                               files={"file": ("different.csv", csv_body.replace("Example Forum", "Changed Forum"), "text/csv")})

        assert first.status_code == replay.status_code == duplicate.status_code == 201
        assert first.json() == replay.json()
        assert first.json()["imported"] == 1
        assert first.json()["items"][0]["audience_tags"] == ["AI", "video"]
        assert first.json()["items"][0]["level"] == "L1"
        assert duplicate.json()["imported"] == 0
        assert duplicate.json()["skipped_existing"] == 1
        assert duplicate.json()["skipped"][0]["destination_id"] == first.json()["items"][0]["destination_id"]
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
        assert len(client.get("/api/v1/destinations").json()["items"]) == 1


def test_catalogue_import_rejects_invalid_rows_atomically_and_disallows_credentials(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)
    csv_body = (
        "name,url,platform_key,surface,category,connector_key\n"
        "Valid Row,https://community.example/valid,example-forum,community,developer,assisted\n"
        "Invalid Row,not-a-url,example-forum,community,developer,assisted\n"
    )
    credential_csv = (
        "name,url,platform_key,surface,category,connector_key,api_key\n"
        "No Credentials,https://community.example/no-secrets,example-forum,community,developer,assisted,secret-value\n"
    )

    with TestClient(main.app) as client:
        invalid = client.post("/api/v1/catalogue/import", headers={"Idempotency-Key": "catalogue-invalid"},
                             files={"file": ("invalid.csv", csv_body, "text/csv")})
        credentials = client.post("/api/v1/catalogue/import", headers={"Idempotency-Key": "catalogue-secret"},
                                  files={"file": ("credentials.csv", credential_csv, "text/csv")})

        assert invalid.status_code == 400
        assert invalid.json()["error"]["code"] == "DEST_SPEC_VIOLATION"
        assert invalid.json()["error"]["details"]["violations"][0]["row"] == 3
        assert credentials.status_code == 400
        assert credentials.json()["error"]["details"]["unknown_columns"] == ["api_key"]
        assert client.get("/api/v1/destinations").json()["items"] == []


def test_catalogue_import_enforces_file_and_row_limits(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)
    header = "name,url,platform_key,surface,category,connector_key\n"
    rows = "".join(
        f"Destination {index},https://catalogue.example/{index},catalogue,community,developer,assisted\n"
        for index in range(main.CATALOGUE_MAX_ROWS + 1)
    )

    with TestClient(main.app) as client:
        maximum = client.post("/api/v1/catalogue/import", headers={"Idempotency-Key": "catalogue-maximum"},
                              files={"file": ("maximum.csv", header + "".join(rows.splitlines(keepends=True)[:main.CATALOGUE_MAX_ROWS]), "text/csv")})
        too_many_rows = client.post("/api/v1/catalogue/import", headers={"Idempotency-Key": "catalogue-too-many"},
                                    files={"file": ("too-many.csv", header + rows, "text/csv")})
        too_large = client.post("/api/v1/catalogue/import", headers={"Idempotency-Key": "catalogue-too-large"},
                                files={"file": ("too-large.csv", b"x" * (main.CATALOGUE_MAX_BYTES + 1), "text/csv")})

        assert maximum.status_code == 201
        assert maximum.json()["imported"] == main.CATALOGUE_MAX_ROWS
        assert too_many_rows.status_code == 413
        assert too_large.status_code == 413
        assert len(client.get("/api/v1/destinations", params={"limit": 200}).json()["items"]) == 200


def test_assisted_publish_returns_human_pack_without_automated_publish(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    storage.objects = {}
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)

    with TestClient(main.app) as client:
        created = client.post("/api/v1/destinations", headers={"Idempotency-Key": "assist-dest"},
                              json=_destination_payload("assisted", automation_allowed=False))
        destination_id = created.json()["destination_id"]
        queued = client.post("/api/v1/publishes", headers={"Idempotency-Key": "assist-publish"}, json={
            "destination_id": destination_id, "rendition": {"body": "Review this manually before posting."},
        })
        deadline = time.monotonic() + 8
        result = None
        while time.monotonic() < deadline:
            result = client.get(f"/api/v1/jobs/{queued.json()['job_id']}").json()
            if result["status"] in {"succeeded", "failed"}:
                break
            time.sleep(0.05)
        assert result["status"] == "succeeded", result
        assert result["result"]["status"] == "needs_review"
        assert "Publish manually" in result["result"]["assist_pack"]["checklist"][-1]


def test_certification_evidence_matches_public_schema(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    storage.objects = {}
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)
    schema = json.loads((Path(__file__).resolve().parents[1] / "schemas" / "certification-run.schema.json").read_text())

    with TestClient(main.app) as client:
        created = client.post("/api/v1/destinations", headers={"Idempotency-Key": "cert-dest"},
                              json=_destination_payload("discourse"))
        destination_id = created.json()["destination_id"]
        queued = client.post(f"/api/v1/destinations/{destination_id}/certifications",
                             headers={"Idempotency-Key": "cert-run"})
        deadline = time.monotonic() + 8
        result = None
        while time.monotonic() < deadline:
            result = client.get(f"/api/v1/jobs/{queued.json()['job_id']}").json()
            if result["status"] in {"succeeded", "failed"}:
                break
            time.sleep(0.05)
        assert result["status"] == "succeeded", result
        record = storage.objects[f"certifications/{result['result']['certification_id']}/evidence.json"]
        Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER).validate(record)
        fetched = client.get(f"/api/v1/certifications/{record['certification_id']}").json()
        Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER).validate(fetched)
        assert fetched["evidence_status"] == "available"

        storage.objects.pop(f"certifications/{record['certification_id']}/evidence.json")
        missing = client.get(f"/api/v1/certifications/{record['certification_id']}")
        assert missing.status_code == 200
        assert missing.json()["evidence_status"] == "missing"
        assert missing.json()["manifest"] is None


def test_failed_certification_evidence_is_retained(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    storage.objects = {}
    monkeypatch.setattr(main, "get_storage", lambda: storage)
    monkeypatch.setattr(jobs, "get_storage", lambda: storage)
    schema = json.loads((Path(__file__).resolve().parents[1] / "schemas" / "certification-run.schema.json").read_text())
    destination = _destination_payload("discourse")
    destination["rules"]["spec"]["body_max_chars"] = 5

    with TestClient(main.app) as client:
        created = client.post("/api/v1/destinations", headers={"Idempotency-Key": "failed-cert-dest"}, json=destination)
        destination_id = created.json()["destination_id"]
        queued = client.post(f"/api/v1/destinations/{destination_id}/certifications",
                             headers={"Idempotency-Key": "failed-cert-run"})
        deadline = time.monotonic() + 8
        result = None
        while time.monotonic() < deadline:
            result = client.get(f"/api/v1/jobs/{queued.json()['job_id']}").json()
            if result["status"] in {"succeeded", "failed"}:
                break
            time.sleep(0.05)

        assert result["status"] == "failed", result
        certification_id = result["error"]["details"]["certification_id"]
        record = storage.objects[f"certifications/{certification_id}/evidence.json"]
        assert record["status"] == "failed"
        assert record["cleanup_confirmed"] is False
        assert len(record["steps"]) == 1
        assert record["steps"][0]["name"] == "validate"
        assert record["steps"][0]["status"] == "failed"
        assert record["steps"][0]["error_code"] == "DEST_SPEC_VIOLATION"
        assert record["steps"][0]["evidence_ref"] == f"s3://incubrix-test/certifications/{certification_id}/details.json"
        Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER).validate(record)


def test_artifact_rights_are_disclosed_as_unverified_and_idempotent(monkeypatch):
    Base.metadata.drop_all(bind=engine)
    initialize_database()
    storage = FakeStorage()
    storage.objects = {}
    monkeypatch.setattr(main, "get_storage", lambda: storage)

    with TestClient(main.app) as client:
        headers = {"Idempotency-Key": "artifact-key"}
        first = client.post("/api/v1/artifacts", headers=headers,
                            files={"file": ("sample.txt", b"owned sample", "text/plain")},
                            data={"license_name": "Candidate owned", "license_url": "https://example.org/license"})
        assert first.status_code == 201
        assert first.json()["rights"]["license_declared"] is True
        assert first.json()["rights"]["commercial_use_verified"] is False
        changed = client.post("/api/v1/artifacts", headers=headers,
                              files={"file": ("sample.txt", b"owned sample", "text/plain")},
                              data={"license_name": "Different declaration", "license_url": "https://example.org/other"})
        assert changed.status_code == 409
        assert changed.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
