import json
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

from incubrix.batch_runner import matches_expected, redact
from incubrix.manifest import output_manifest

ROOT = Path(__file__).resolve().parents[1]


def test_json_schemas_are_valid():
    for path in (ROOT / "schemas").glob("*.schema.json"):
        schema = json.loads(path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)


def test_each_connector_manifest_is_valid():
    schema = json.loads((ROOT / "schemas" / "connector-manifest.schema.json").read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    manifests = list((ROOT / "incubrix" / "connectors" / "manifests").glob("*.yaml"))
    assert len(manifests) == 12
    for path in manifests:
        manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
        errors = list(validator.iter_errors(manifest))
        assert not errors, f"{path.name}: {[error.message for error in errors]}"


def test_output_manifest_matches_public_schema_and_does_not_claim_zero_cost():
    schema = json.loads((ROOT / "schemas" / "output-manifest.schema.json").read_text(encoding="utf-8"))
    manifest = output_manifest(
        kind="publish", connector_key="discourse", version="1.0.0",
        timings_ms={"processing": 12}, settings={"dry_run": False},
    )
    errors = list(Draft202012Validator(schema).iter_errors(manifest))
    assert not errors, [error.message for error in errors]
    assert manifest["cost_estimate_usd"] is None
    assert manifest["cost_estimate_status"] == "not_estimated"


def test_batch_expectations_match_partial_nested_results():
    actual = {"http_status": 200, "response": {"ok": False, "violations": [{"field": "media", "code": "MEDIA_TYPE_UNSUPPORTED"}]}}
    expected = {"http_status": 200, "response": {"violations": [{"code": "MEDIA_TYPE_UNSUPPORTED"}]}}
    assert matches_expected(actual, expected)
    assert not matches_expected(actual, {"response": {"ok": True}})


def test_batch_evidence_redacts_secret_fields_echoes_and_url_parameters():
    raw = {
        "connection": {
            "api-key": "api-key-value-123",
            "clientSecret": "client-secret-value-456",
            "jwt": "jwt-value-789",
            "refresh_token": "refresh-token-value-012",
        },
        "request_url": "https://example.test/publish?api_key=query-secret-345&safe=visible",
        "error_message": "Platform echoed api-key-value-123 and jwt-value-789",
    }

    result = json.dumps(redact(raw))

    for secret in ("api-key-value-123", "client-secret-value-456", "jwt-value-789", "refresh-token-value-012", "query-secret-345"):
        assert secret not in result
    assert "visible" in result
