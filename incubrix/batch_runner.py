from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx


def matches_expected(actual, expected) -> bool:
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(key in actual and matches_expected(actual[key], value) for key, value in expected.items())
    if isinstance(expected, list):
        return isinstance(actual, list) and all(any(matches_expected(candidate, item) for candidate in actual) for item in expected)
    return actual == expected


def _run_key(run_id: str, case_name: str, purpose: str) -> str:
    return hashlib.sha256(f"{run_id}\0{case_name}\0{purpose}".encode()).hexdigest()


def run_case(client: httpx.Client, case: dict, case_name: str, run_id: str | None = None) -> dict:
    kind = case.get("kind", "connector")

    def batch_key(purpose: str) -> str:
        if run_id:
            return _run_key(run_id, case_name, purpose)
        return case_name if purpose == "connector" else f"{case_name}-{purpose}"

    if kind == "connector":
        connector_key = case["connector_key"]
        method = case.get("method", "validate")
        response = client.post(f"/connector/v1/{connector_key}/{method}", json=case.get("body", {}), headers={"Idempotency-Key": case.get("idempotency_key") or batch_key("connector")})
        return {"http_status": response.status_code, "response": response.json()}
    if kind == "publish":
        stable_key = case.get("idempotency_key") or (
            _run_key(run_id, case_name, "publish") if run_id else hashlib.sha256(case_name.encode()).hexdigest()
        )
        response = client.post("/api/v1/publishes", json=case["body"], headers={"Idempotency-Key": stable_key})
        initial = {"http_status": response.status_code, "response": response.json()}
        job_id = initial["response"].get("job_id")
        if not job_id:
            return initial
        history = []
        deadline = time.monotonic() + float(case.get("timeout_seconds", 90))
        while time.monotonic() < deadline:
            poll = client.get(f"/api/v1/jobs/{job_id}")
            snapshot = poll.json()
            history.append(snapshot)
            if snapshot.get("status") in {"succeeded", "failed"}:
                return {"http_status": response.status_code, "response": initial["response"], "job_history": history, "result": snapshot}
            time.sleep(0.4)
        return {"http_status": response.status_code, "response": initial["response"], "job_history": history, "timeout": True}
    if kind == "certification":
        destination_key = case.get("destination_idempotency_key") or batch_key("destination")
        destination_response = client.post("/api/v1/destinations", json=case["destination"], headers={"Idempotency-Key": destination_key})
        destination_body = destination_response.json()
        destination_id = destination_body.get("destination_id")
        if not destination_id:
            return {"http_status": destination_response.status_code, "response": destination_body}
        certification_key = case.get("idempotency_key") or batch_key("certification")
        response = client.post(f"/api/v1/destinations/{destination_id}/certifications", headers={"Idempotency-Key": certification_key})
        initial = response.json()
        job_id = initial.get("job_id")
        if not job_id:
            return {"http_status": response.status_code, "destination_http_status": destination_response.status_code, "response": initial}
        history = []
        deadline = time.monotonic() + float(case.get("timeout_seconds", 90))
        while time.monotonic() < deadline:
            poll = client.get(f"/api/v1/jobs/{job_id}")
            snapshot = poll.json()
            history.append(snapshot)
            if snapshot.get("status") in {"succeeded", "failed"}:
                job_result = snapshot.get("result") or {}
                job_error = snapshot.get("error") or {}
                error_details = job_error.get("details") or {}
                certification_id = job_result.get("certification_id") or error_details.get("certification_id")
                record = None
                if certification_id:
                    record_response = client.get(f"/api/v1/certifications/{certification_id}")
                    if record_response.is_success:
                        record = record_response.json()
                return {"http_status": response.status_code, "destination_http_status": destination_response.status_code,
                        "response": initial, "job_history": history, "result": snapshot, "certification_record": record}
            time.sleep(0.4)
        return {"http_status": response.status_code, "destination_http_status": destination_response.status_code,
                "response": initial, "job_history": history, "timeout": True}
    raise ValueError(f"Unknown case kind {kind!r}; use connector, publish, or certification")


_SENSITIVE_KEY_PARTS = (
    "apikey", "secret", "password", "token", "jwt", "authorization",
    "credential", "cookie", "privatekey", "signature",
)
_SENSITIVE_QUERY_KEYS = {"key", "apikey", "secret", "password", "token", "jwt", "authorization", "signature"}


def _sensitive_key(key) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def collect_sensitive_values(value) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if _sensitive_key(key):
                if isinstance(item, str) and len(item) >= 3:
                    found.add(item)
                else:
                    found.update(collect_sensitive_values(item))
            else:
                found.update(collect_sensitive_values(item))
    elif isinstance(value, list):
        for item in value:
            found.update(collect_sensitive_values(item))
    return found


def _redact_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        if not parts.scheme or not parts.netloc:
            return url
        hostname = parts.hostname or ""
        if parts.port:
            hostname += f":{parts.port}"
        query = [(key, "[REDACTED]" if key.lower().replace("-", "_") in _SENSITIVE_QUERY_KEYS or _sensitive_key(key) else value)
                 for key, value in parse_qsl(parts.query, keep_blank_values=True)]
        return urlunsplit((parts.scheme, hostname, parts.path, urlencode(query), parts.fragment))
    except ValueError:
        return url


def redact(value, sensitive_values=None):
    if sensitive_values is None:
        sensitive_values = collect_sensitive_values(value)
    if isinstance(value, dict):
        sanitized = {}
        for key, item in value.items():
            if _sensitive_key(key):
                sanitized[key] = "[REDACTED]"
            else:
                sanitized[key] = redact(item, sensitive_values)
        return sanitized
    if isinstance(value, list):
        return [redact(item, sensitive_values) for item in value]
    if isinstance(value, str):
        sanitized = _redact_url(value)
        for secret in sorted(sensitive_values, key=len, reverse=True):
            sanitized = sanitized.replace(secret, "[REDACTED]")
        return sanitized
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="Run connector, publish, and certification cases; save every result, including expected failures.")
    parser.add_argument("inputs", type=Path, help="One JSON input file or a directory containing JSON case files")
    parser.add_argument("output", type=Path, help="Folder where request/response evidence is written")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Service URL")
    parser.add_argument("--run-id", help="Optional idempotency namespace for a fresh, repeatable run")
    args = parser.parse_args()
    if not args.inputs.exists():
        parser.error(f"Input path does not exist: {args.inputs}")
    files = [args.inputs] if args.inputs.is_file() else sorted(args.inputs.glob("*.json"))
    if not files:
        parser.error(f"No JSON case files found in {args.inputs}")
    args.output.mkdir(parents=True, exist_ok=True)
    index = []
    with httpx.Client(base_url=args.base_url, timeout=20.0) as client:
        for input_path in files:
            try:
                case = json.loads(input_path.read_text(encoding="utf-8"))
            except Exception as exc:  # noqa: BLE001 - each untrusted case must be isolated in evidence output.
                case = {}
                result = {"error_type": type(exc).__name__, "message": "Case input could not be parsed.", "passed": False}
            else:
                try:
                    result = run_case(client, case, input_path.name, args.run_id)
                    expectation = case.get("expect")
                    result["passed"] = matches_expected(result, expectation) if expectation is not None else None
                except Exception as exc:  # noqa: BLE001 - a single malformed case must not abort the batch.
                    result = {"error_type": type(exc).__name__, "message": str(exc), "passed": False}
            secrets = collect_sensitive_values(case)
            output_path = args.output / f"{input_path.stem}.result.json"
            output_path.write_text(json.dumps({"input_file": input_path.name, "request": redact(case, secrets), "result": redact(result, secrets)}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            job_result = result.get("result") if isinstance(result.get("result"), dict) else {}
            response = result.get("response") if isinstance(result.get("response"), dict) else {}
            error = response.get("error") if isinstance(response.get("error"), dict) else job_result.get("error")
            http_status = result.get("http_status")
            job_status = job_result.get("status")
            expected_failure = (isinstance(http_status, int) and http_status >= 400) or job_status == "failed"
            passed = result.get("passed")
            outcome = "expected_failure" if expected_failure and passed is True else "failure" if passed is False else "passed" if passed is True else "unasserted"
            index_row = {"input_file": input_path.name, "result_file": output_path.name, "http_status": http_status,
                          "job_status": job_status, "error_code": error.get("code") if isinstance(error, dict) else None,
                          "outcome": outcome, "passed": passed}
            if args.run_id:
                index_row["run_id"] = args.run_id
            index.append(index_row)
    (args.output / "index.json").write_text(json.dumps(index, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    passed = sum(item.get("passed") is True for item in index)
    summary = {"cases": len(index), "passed": passed, "failed_or_unasserted": len(index) - passed, "output": str(args.output.resolve())}
    if args.run_id:
        summary["run_id"] = args.run_id
    print(json.dumps(summary, indent=2))
    return 0 if passed == len(index) else 1


if __name__ == "__main__":
    raise SystemExit(main())
