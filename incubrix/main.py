from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import logging
import re
import secrets
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from html.parser import HTMLParser
from urllib.parse import urlsplit, urlunsplit
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import httpx
from fastapi import FastAPI, File, Form, Header, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.trustedhost import TrustedHostMiddleware

from incubrix.config import get_settings
from incubrix.connectors import (
    get_connector,
    public_connector_list,
    validate_connector_registry,
)
from incubrix.db import SessionLocal, engine, initialize_database
from incubrix.errors import ServiceError
from incubrix.jobs import destination_dict, resolve_connection, worker_loop
from incubrix.manifest import output_manifest
from incubrix.passport import destination_passport, explain_policy
from incubrix.models import (
    CertificationRun,
    Destination,
    IdempotencyRecord,
    Job,
    Post,
    RecertificationTicket,
    RecommendationFeedback,
    RuleProposal,
)
from incubrix.network import validate_http_url
from incubrix.rules import enforce_rules
from incubrix.schemas import (
    CertificationCreate,
    ConnectorAssist,
    ConnectorDelete,
    ConnectorMetrics,
    ConnectorRefreshAuth,
    ConnectorStatus,
    ConnectorValidate,
    DestinationCreate,
    DestinationValidationRequest,
    PolicyEvaluationRequest,
    PublishCreate,
    RecommendationFeedbackRequest,
    RecommendationRequest,
    RuleProposalRequest,
    TailorRequest,
    validate_connection_config,
)
from incubrix.storage import StorageUnavailable, get_storage
from incubrix.util import canonical_json, iso, normalize_destination_url, sha256_json


class JsonLogFormatter(logging.Formatter):
    def format(self, record):
        payload = {"timestamp": datetime.now(UTC).isoformat(), "level": record.levelname.lower(), "logger": record.name, "message": record.getMessage()}
        for key in ("job_id", "correlation_id", "error_type"):
            if getattr(record, key, None) is not None:
                payload[key] = getattr(record, key)
        return json.dumps(payload, ensure_ascii=False)


handler = logging.StreamHandler()
handler.setFormatter(JsonLogFormatter())
logging.basicConfig(level=getattr(logging, get_settings().log_level.upper(), logging.INFO), handlers=[handler], force=True)
log = logging.getLogger("incubrix.api")


def api_error(code: str, message: str, retryable: bool = False, details: dict | None = None) -> dict:
    return {"error": {"code": code, "message": message, "retryable": retryable, "details": details or {}}}


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_settings().validate_runtime()
    validate_connector_registry()
    initialize_database()
    # Unit tests mock storage at individual boundaries; Compose exercises the
    # real local S3-compatible endpoint. Do not make host unit tests depend on
    # a Docker network name.
    if get_settings().app_env != "test":
        await asyncio.to_thread(get_storage().ensure_bucket)
    app.state.worker_stop = asyncio.Event()
    app.state.worker_task = asyncio.create_task(worker_loop(app.state.worker_stop))
    yield
    app.state.worker_stop.set()
    await app.state.worker_task


_startup_settings = get_settings()


def _documentation_route_options(settings) -> dict:
    docs_enabled = settings.docs_enabled
    return {
        "openapi_url": "/openapi.json" if docs_enabled else None,
        "docs_url": "/docs" if docs_enabled else None,
        "redoc_url": "/redoc" if docs_enabled else None,
    }


app = FastAPI(
    title="IncuBrix Distribution Connector Service",
    version="0.1.0",
    lifespan=lifespan,
    **_documentation_route_options(_startup_settings),
)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=_startup_settings.trusted_host_list)


class RequestBodySizeLimitMiddleware:
    """Enforce the body limit even when a client omits Content-Length."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        limit = get_settings().max_request_bytes
        messages = []
        body_size = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            messages.append(message)
            body_size += len(message.get("body", b""))
            if body_size > limit:
                response = JSONResponse(
                    status_code=413,
                    content=api_error("DEST_SPEC_VIOLATION", "Request exceeds the configured size limit."),
                )
                await response(scope, receive, send)
                return
            if not message.get("more_body", False):
                break

        async def replay_receive():
            if messages:
                return messages.pop(0)
            return await receive()

        await self.app(scope, replay_receive, send)


app.add_middleware(RequestBodySizeLimitMiddleware)

CATALOGUE_MAX_BYTES = 4 * 1_048_576
CATALOGUE_MAX_ROWS = 1_000
CATALOGUE_REQUIRED_COLUMNS = {"name", "url", "platform_key", "surface", "category", "connector_key"}
CATALOGUE_OPTIONAL_COLUMNS = {"audience_tags", "language", "geo", "est_audience", "rules_url", "level"}
CATALOGUE_FILE = File(...)
ARTIFACT_FILE = File(default=None)
ARTIFACT_SOURCE_URL = Form(default=None)
ARTIFACT_LICENSE_NAME = Form(default=None)
ARTIFACT_LICENSE_URL = Form(default=None)


_CORRELATION_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_PUBLIC_PATHS = frozenset({"/health", "/ready"})


def _with_security_headers(response: JSONResponse) -> JSONResponse:
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Content-Security-Policy"] = "frame-ancestors 'none'"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Cache-Control"] = "no-store"
    return response


def _request_is_authorized(request: Request) -> bool:
    settings = get_settings()
    if not settings.auth_required:
        return True
    expected = settings.app_api_key
    if not expected:
        return False
    bearer = request.headers.get("Authorization", "")
    supplied = request.headers.get("X-API-Key")
    if bearer.lower().startswith("bearer "):
        supplied = bearer[7:]
    return bool(supplied) and secrets.compare_digest(supplied, expected)


@app.middleware("http")
async def security_and_access_log(request: Request, call_next):
    requested_id = request.headers.get("X-Correlation-Id", "")
    correlation_id = requested_id if _CORRELATION_ID.fullmatch(requested_id) else str(uuid4())
    request.state.correlation_id = correlation_id
    settings = get_settings()
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            too_large = int(content_length) > settings.max_request_bytes
        except ValueError:
            too_large = True
        if too_large:
            response = JSONResponse(
                status_code=413,
                content=api_error("DEST_SPEC_VIOLATION", "Request exceeds the configured size limit."),
            )
            response.headers["X-Correlation-Id"] = correlation_id
            return _with_security_headers(response)
    if request.url.path not in _PUBLIC_PATHS and not _request_is_authorized(request):
        response = JSONResponse(
            status_code=401,
            content=api_error("UNAUTHORIZED", "Valid API credentials are required."),
        )
        response.headers["X-Correlation-Id"] = correlation_id
        return _with_security_headers(response)
    response = await call_next(request)
    response.headers["X-Correlation-Id"] = correlation_id
    log.info("request_complete method=%s path=%s status=%s", request.method, request.url.path, response.status_code, extra={"correlation_id": correlation_id})
    return _with_security_headers(response)


@app.exception_handler(ServiceError)
async def service_error_handler(request: Request, exc: ServiceError):
    return JSONResponse(status_code=exc.status_code, content=api_error(exc.code, exc.message, exc.retryable, exc.details), headers={"X-Correlation-Id": getattr(request.state, "correlation_id", "")})


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    details = [{"field": ".".join(str(x) for x in item["loc"]), "message": item["msg"]} for item in exc.errors()]
    return JSONResponse(status_code=422, content=api_error("DEST_SPEC_VIOLATION", "Request validation failed.", False, {"violations": details}), headers={"X-Correlation-Id": getattr(request.state, "correlation_id", "")})


@app.exception_handler(StarletteHTTPException)
async def http_error_handler(request: Request, exc: StarletteHTTPException):
    status = exc.status_code
    code = {
        401: "UNAUTHORIZED",
        403: "FORBIDDEN",
        404: "DESTINATION_NOT_FOUND",
        413: "DEST_SPEC_VIOLATION",
    }.get(status, "DEST_SPEC_VIOLATION")
    return JSONResponse(status_code=status, content=api_error(code, str(exc.detail), False), headers={"X-Correlation-Id": getattr(request.state, "correlation_id", "")})


def require_idempotency(key: str | None) -> str:
    if not key or len(key) > 255:
        raise ServiceError(400, "DEST_SPEC_VIOLATION", "A valid Idempotency-Key header is required.", details={"header": "Idempotency-Key"})
    return key


def validated_callback_url(value: str | None) -> str | None:
    """Allow callbacks only with an operator-owned signing key and host allowlist."""

    if value is None:
        return None
    settings = get_settings()
    if not settings.job_callback_signing_secret or not settings.callback_allowed_host_set:
        raise ServiceError(403, "CONNECTOR_DISABLED", "Completion callbacks are not enabled in this environment.")
    return validate_http_url(
        value,
        purpose="completion callback",
        allowed_private_hosts=settings.callback_allowed_host_set,
        allowed_hosts=settings.callback_allowed_host_set,
    )


def existing_idempotency(session, scope: str, key: str, request_hash: str):
    record = session.scalar(select(IdempotencyRecord).where(IdempotencyRecord.scope == scope, IdempotencyRecord.key == key))
    if record and record.request_hash != request_hash:
        raise ServiceError(409, "IDEMPOTENCY_CONFLICT", "This Idempotency-Key was already used for a different request.")
    if record:
        if record.response_json:
            return json.loads(record.response_json), None
        if record.job_id:
            return None, session.get(Job, record.job_id)
    return None, None


def replay_after_unique_conflict(scope: str, key: str, request_hash: str):
    with SessionLocal() as session:
        prior, job = existing_idempotency(session, scope, key, request_hash)
        if prior is not None:
            return prior
        if job is not None:
            return JSONResponse(status_code=202, content=job_response(job))
    raise ServiceError(409, "IDEMPOTENCY_CONFLICT", "A request with this Idempotency-Key is already being committed; retry with the same key.", True)


def destination_response(destination: Destination) -> dict:
    result = destination_dict(destination)
    result.update({"created_at": iso(destination.created_at), "updated_at": iso(destination.updated_at)})
    return result


def job_response(job: Job) -> dict:
    result_links = []
    if job.status == "succeeded" and job.result_json:
        result = json.loads(job.result_json)
        for rel in ("manifest_url", "evidence_url"):
            if result.get(rel):
                result_links.append({"rel": rel.removesuffix("_url"), "href": result[rel], "media_type": "application/json"})
    error = None
    if job.error_json:
        error_value = json.loads(job.error_json)
        error = {"code": error_value.get("code", "INTERNAL_ERROR"), "message": error_value.get("message", "Job failed."), "retryable": error_value.get("retryable", False), "details": error_value.get("details", {})}
    return {
        "job_id": job.id, "kind": job.kind, "status": job.status, "progress": job.progress,
        "created_at": iso(job.created_at), "updated_at": iso(job.updated_at), "started_at": iso(job.started_at),
        "finished_at": iso(job.finished_at), "result_links": result_links, "result": json.loads(job.result_json) if job.result_json else None, "error": error,
    }


@app.get("/health", tags=["health"])
def health():
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
def ready():
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as exc:
        raise ServiceError(503, "PLATFORM_UNAVAILABLE", "Database is not ready.", True, {"dependency": "database"}) from exc
    return {"status": "ready", "simulation_mode": get_settings().simulation_mode}


@app.get("/api/v1/connectors", tags=["connectors"])
def list_connectors():
    return {"items": public_connector_list(), "simulation_mode": get_settings().simulation_mode}


@app.get("/api/v1/destinations", tags=["destinations"])
def list_destinations(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    with SessionLocal() as session:
        items = session.scalars(
            select(Destination)
            .order_by(Destination.created_at.desc(), Destination.id.desc())
            .offset(offset)
            .limit(limit + 1)
        ).all()
        has_more = len(items) > limit
        visible_items = items[:limit]
        return {
            "items": [destination_response(item) for item in visible_items],
            "next_offset": offset + len(visible_items) if has_more else None,
        }


def _remember_idempotent_response(session, scope: str, key: str, request_hash: str, response: dict) -> dict:
    prior, _ = existing_idempotency(session, scope, key, request_hash)
    if prior is not None:
        return prior
    session.add(IdempotencyRecord(scope=scope, key=key, request_hash=request_hash, response_json=canonical_json(response)))
    return response


@app.post("/api/v1/recommendations", tags=["recommendations"])
def recommend_destinations(body: RecommendationRequest, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    key = require_idempotency(idempotency_key)
    request_value = body.model_dump(mode="json")
    request_hash = sha256_json(request_value)
    requested_tags = set(body.audience_tags)
    with SessionLocal() as session:
        prior, _ = existing_idempotency(session, "recommendations", key, request_hash)
        if prior is not None:
            return prior
        candidates = session.scalars(select(Destination).where(Destination.enabled.is_(True))).all()
        feedback = {}
        for item in session.scalars(select(RecommendationFeedback)).all():
            accepted, reviewed = feedback.get(item.destination_id, (0, 0))
            feedback[item.destination_id] = (accepted + int(item.accepted), reviewed + 1)
        items = []
        for destination in candidates:
            tags = {tag.lower() for tag in json.loads(destination.audience_tags_json or "[]")}
            matched = sorted(tags & requested_tags)
            language_match = body.language is None or destination.language.lower() == body.language.lower()
            reasons = ([f"matched tags: {', '.join(matched)}"] if matched else []) + (
                ["matching language"] if language_match else []
            )
            if body.rendition is not None:
                try:
                    enforce_rules(
                        session,
                        destination,
                        body.rendition.model_dump(mode="json"),
                        dry_run=True,
                        check_frequency=False,
                    )
                except ServiceError as exc:
                    # A recommendation must never hide a known policy mismatch.
                    reasons.append(f"excluded by destination rule: {exc.code}")
                    continue
                reasons.append("provided rendition satisfies known destination rules")
            accepted, reviewed = feedback.get(destination.id, (0, 0))
            history_adjustment = 0
            if reviewed:
                acceptance_rate = accepted / reviewed
                if acceptance_rate >= 0.75:
                    history_adjustment = 1
                    reasons.append(f"strong historical reviewer acceptance ({accepted}/{reviewed})")
                elif acceptance_rate < 0.25:
                    history_adjustment = -1
                    reasons.append(f"weak historical reviewer acceptance ({accepted}/{reviewed})")
            score = max(1, min(5, 1 + len(matched) * 2 + int(language_match) + history_adjustment))
            items.append({
                "destination": destination_response(destination),
                "relevance_score": score,
                "reasons": reasons,
            })
        items.sort(key=lambda item: (-item["relevance_score"], item["destination"]["name"].lower(), item["destination"]["destination_id"]))
        response = {"items": items[:body.limit], "method": "deterministic_tag_and_language_v1", "requires_human_review": True}
        _remember_idempotent_response(session, "recommendations", key, request_hash, response)
        session.commit()
        return response


@app.post("/api/v1/recommendations/{recommendation_key}/feedback", status_code=201, tags=["recommendations"])
def record_recommendation_feedback(
    recommendation_key: str,
    body: RecommendationFeedbackRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    key = require_idempotency(idempotency_key)
    request_hash = sha256_json({"recommendation_key": recommendation_key, **body.model_dump(mode="json")})
    with SessionLocal() as session:
        prior, _ = existing_idempotency(session, "recommendation-feedback", key, request_hash)
        if prior is not None:
            return prior
        recommendation = session.scalar(select(IdempotencyRecord).where(
            IdempotencyRecord.scope == "recommendations", IdempotencyRecord.key == recommendation_key,
        ))
        if not recommendation:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Recommendation request was not found.")
        recommended_ids = {
            item.get("destination", {}).get("destination_id")
            for item in json.loads(recommendation.response_json or "{}").get("items", [])
        }
        if str(body.destination_id) not in recommended_ids:
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "Feedback destination was not included in this recommendation result.")
        if not session.get(Destination, str(body.destination_id)):
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination was not found.")
        existing = session.scalar(select(RecommendationFeedback).where(
            RecommendationFeedback.recommendation_key == recommendation_key,
            RecommendationFeedback.destination_id == str(body.destination_id),
        ))
        if existing:
            existing.accepted = body.accepted
            feedback = existing
        else:
            feedback = RecommendationFeedback(
                recommendation_key=recommendation_key, destination_id=str(body.destination_id), accepted=body.accepted,
            )
            session.add(feedback)
            session.flush()
        response = {"feedback_id": feedback.id, "recommendation_key": recommendation_key, "destination_id": feedback.destination_id, "accepted": feedback.accepted}
        _remember_idempotent_response(session, "recommendation-feedback", key, request_hash, response)
        session.commit()
        return response


@app.get("/api/v1/recommendations/metrics", tags=["recommendations"])
def recommendation_metrics():
    with SessionLocal() as session:
        total = session.scalar(select(func.count(RecommendationFeedback.id))) or 0
        accepted = session.scalar(select(func.count(RecommendationFeedback.id)).where(RecommendationFeedback.accepted.is_(True))) or 0
        return {"feedback_count": total, "accepted_count": accepted, "precision": accepted / total if total else None}


@app.post("/api/v1/tailor", tags=["recommendations"])
def tailor_rendition(body: TailorRequest, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    key = require_idempotency(idempotency_key)
    request_value = body.model_dump(mode="json")
    request_hash = sha256_json(request_value)
    with SessionLocal() as session:
        prior, _ = existing_idempotency(session, "tailor", key, request_hash)
        if prior is not None:
            return prior
        destination = session.get(Destination, str(body.destination_id))
        if not destination:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination was not found.")
        rendition = body.rendition.model_dump(mode="json")
        rules = json.loads(destination.rules_json or "{}")
        spec = rules.get("spec", {})
        changes = []
        title_limit = spec.get("title_max_chars")
        if title_limit and rendition.get("title") and len(rendition["title"]) > title_limit:
            rendition["title"] = _truncate_at_word_boundary(rendition["title"], title_limit)
            changes.append("trimmed title to destination limit")
        body_limit = spec.get("body_max_chars")
        if body_limit and len(rendition["body"]) > body_limit:
            rendition["body"] = _truncate_at_word_boundary(rendition["body"], body_limit)
            changes.append("trimmed body to destination limit")
        if rendition.get("ai_generated") and rules.get("disclosure_required", destination.disclosure_required) and not rendition.get("ai_disclosure"):
            rendition["ai_disclosure"] = "AI-assisted content; reviewed before submission."
            changes.append("added required AI disclosure")
        response = {
            "destination_id": destination.id,
            "rendition": rendition,
            "changes": changes,
            "method": "deterministic_destination_constraints_v1",
            "requires_human_review": True,
        }
        _remember_idempotent_response(session, "tailor", key, request_hash, response)
        session.commit()
        return response


def _truncate_at_word_boundary(value: str, limit: int) -> str:
    """Fit text to a character limit without cutting a word when possible."""

    if limit <= 0:
        return ""
    if len(value) <= limit:
        return value
    prefix = value[:limit]
    boundary = prefix.rfind(" ")
    if boundary > 0:
        return prefix[:boundary].rstrip()
    # A single token can be longer than the destination limit. A hard cut is
    # the only way to honor the platform's maximum in that case.
    return prefix.rstrip()


def _propose_rules(rules_text: str) -> tuple[dict, list[str]]:
    text_lower = rules_text.lower()
    rules = {
        "self_promo_allowed": False,
        "link_policy": "review_required",
        "disclosure_required": True,
        "automation_allowed": False,
    }
    rationale = [
        "Unknown promotion and automation rules default to blocked.",
        "Human confirmation is required before any proposed rule is used.",
    ]
    promotion_is_blocked = any(
        phrase in text_lower
        for phrase in (
            "no self promotion", "no self-promotion", "self-promotion is not allowed",
            "self promotion is not allowed", "self-promotion is prohibited", "self promotion is prohibited",
            "self-promotion is banned", "self promotion is banned", "promotional posts are not allowed",
            "promotional posts are prohibited", "no promotional posts", "no promotional content",
        )
    )
    if promotion_is_blocked:
        rules["self_promo_allowed"] = False
        rationale.append("Detected a restriction on self-promotion.")
    if "no links" in text_lower or "links are not allowed" in text_lower:
        rules["link_policy"] = "no_links"
        rationale.append("Detected a no-links rule.")
    elif "canonical link" in text_lower or "one link" in text_lower:
        rules["link_policy"] = "canonical_only"
        rationale.append("Detected a canonical or single-link rule.")
    automation_is_blocked = any(
        phrase in text_lower
        for phrase in (
            "manual approval", "do not automate", "do not use automation", "human review",
            "no automated posting", "no automation", "automated posting is not allowed",
            "automation is not allowed", "automation is prohibited", "automated posting is prohibited",
        )
    )
    if automation_is_blocked:
        rules["automation_allowed"] = False
        rationale.append("Detected a manual-review requirement.")
    elif "automation allowed" in text_lower or "automated posting is allowed" in text_lower:
        rules["automation_allowed"] = True
        rationale.append("Detected an explicit allowance for automated posting.")
    if not promotion_is_blocked and any(phrase in text_lower for phrase in ("self-promotion is allowed", "self promotion is allowed", "promotional posts are allowed")):
        rules["self_promo_allowed"] = True
        rationale.append("Detected an explicit allowance for self-promotion.")
    if "ai disclosure" in text_lower or "disclose ai" in text_lower:
        rules["disclosure_required"] = True
        rationale.append("Detected an AI-disclosure requirement.")
    match = re.search(
        r"\b(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+posts?\s+per\s+(hour|day|week|month)\b",
        text_lower,
    )
    if match:
        window = {"hour": 3600, "day": 86400, "week": 604800, "month": 2592000}[match.group(2)]
        number_words = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
                        "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10}
        count_text = match.group(1)
        count = int(count_text) if count_text.isdigit() else number_words[count_text]
        rules["frequency_cap"] = {"max_posts": count, "window_seconds": window}
        rationale.append("Detected a posting-frequency limit.")
    return rules, rationale


class _RulesPageTextExtractor(HTMLParser):
    _HIDDEN_TAGS = frozenset({"script", "style", "noscript", "svg", "template"})
    _BLOCK_TAGS = frozenset({
        "address", "article", "blockquote", "br", "dd", "div", "dl", "dt",
        "h1", "h2", "h3", "h4", "h5", "h6", "li", "main", "ol", "p",
        "section", "table", "td", "th", "tr", "ul",
    })

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hidden_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in self._HIDDEN_TAGS:
            self.hidden_depth += 1
        elif not self.hidden_depth and tag in self._BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in self._HIDDEN_TAGS and self.hidden_depth:
            self.hidden_depth -= 1
        elif not self.hidden_depth and tag in self._BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.hidden_depth:
            self.parts.append(data)


def _extract_rules_page_text(document: str) -> str:
    parser = _RulesPageTextExtractor()
    parser.feed(document)
    parser.close()
    return re.sub(r"[ \t\r\f\v]+", " ", "\n".join(parser.parts)).strip()


async def _fetch_rules_page(source_url: str) -> str:
    settings = get_settings()
    if not settings.rule_page_fetch_is_enabled:
        raise ServiceError(403, "CONNECTOR_DISABLED", "Rule-page fetching is disabled in this environment.")
    allowed_hosts = settings.outbound_allowed_host_set
    safe_url = validate_http_url(
        source_url,
        purpose="rules page",
        allowed_hosts=allowed_hosts,
    )
    max_bytes = 1_048_576
    async with httpx.AsyncClient(
        follow_redirects=False,
        timeout=httpx.Timeout(12.0, connect=4.0),
        headers={"Accept": "text/html,text/plain;q=0.9"},
    ) as client:
        try:
            async with client.stream("GET", safe_url) as response:
                if response.is_redirect:
                    raise ServiceError(422, "DEST_SPEC_VIOLATION", "Redirecting rules pages are not accepted; provide the final URL.")
                if not response.is_success:
                    raise ServiceError(422, "PLATFORM_CONTENT_REJECTED", "The rules page did not return content.", False, {"status": response.status_code})
                media_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                if media_type not in {"text/html", "application/xhtml+xml", "text/plain"}:
                    raise ServiceError(422, "PLATFORM_CONTENT_REJECTED", "The rules page must return HTML or plain text.", False, {"content_type": media_type or "unknown"})
                declared_size = response.headers.get("content-length")
                if declared_size:
                    try:
                        if int(declared_size) > max_bytes:
                            raise ServiceError(413, "DEST_SPEC_VIOLATION", "Rules page exceeds the 1 MiB fetch limit.")
                    except ValueError as exc:
                        raise ServiceError(422, "PLATFORM_CONTENT_REJECTED", "The rules page returned an invalid Content-Length header.") from exc
                chunks = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > max_bytes:
                        raise ServiceError(413, "DEST_SPEC_VIOLATION", "Rules page exceeds the 1 MiB fetch limit.")
                    chunks.append(chunk)
                document = b"".join(chunks).decode(response.encoding or "utf-8", errors="replace")
        except httpx.HTTPError as exc:
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Could not fetch the rules page.", True, {"reason": type(exc).__name__}) from exc

    rules_text = document if media_type == "text/plain" else _extract_rules_page_text(document)
    rules_text = rules_text[:50_001]
    if not rules_text.strip():
        raise ServiceError(422, "PLATFORM_CONTENT_REJECTED", "The rules page contains no readable text.")
    if len(rules_text) > 50_000:
        raise ServiceError(413, "DEST_SPEC_VIOLATION", "Extracted rules text exceeds the 50,000 character limit.")
    return rules_text


@app.post("/api/v1/rule-proposals", status_code=201, tags=["rules"])
async def create_rule_proposal(body: RuleProposalRequest, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    key = require_idempotency(idempotency_key)
    request_value = body.model_dump(mode="json")
    request_hash = sha256_json(request_value)
    with SessionLocal() as session:
        prior, _ = existing_idempotency(session, "rule-proposal", key, request_hash)
        if prior is not None:
            return prior
        if body.destination_id and not session.get(Destination, str(body.destination_id)):
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination was not found.")

    rules_text = await _fetch_rules_page(str(body.source_url)) if body.source_url else (body.rules_text or "")
    proposed_rules, rationale = _propose_rules(rules_text)
    source_url = None
    if body.source_url:
        source_parts = urlsplit(str(body.source_url))
        source_url = urlunsplit((source_parts.scheme, source_parts.netloc, source_parts.path, "", ""))
    with SessionLocal() as session:
        prior, _ = existing_idempotency(session, "rule-proposal", key, request_hash)
        if prior is not None:
            return prior
        if body.destination_id and not session.get(Destination, str(body.destination_id)):
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination was not found.")
        proposal = RuleProposal(
            destination_id=str(body.destination_id) if body.destination_id else None,
            source_url=source_url,
            source_sha256=hashlib.sha256(rules_text.encode()).hexdigest(),
            proposal_json=canonical_json({"rules": proposed_rules, "rationale": rationale, "method": "deterministic_rule_parser_v1"}),
        )
        session.add(proposal)
        session.flush()
        response = {"proposal_id": proposal.id, "status": "proposed", "rules": proposed_rules, "rationale": rationale, "source_url": source_url, "source_sha256": proposal.source_sha256, "source_kind": "page" if source_url else "text", "requires_human_confirmation": True}
        _remember_idempotent_response(session, "rule-proposal", key, request_hash, response)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            return replay_after_unique_conflict("rule-proposal", key, request_hash)
        return response


@app.post("/api/v1/rule-proposals/{proposal_id}/confirm", tags=["rules"])
def confirm_rule_proposal(proposal_id: UUID, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    key = require_idempotency(idempotency_key)
    request_hash = sha256_json({"proposal_id": str(proposal_id), "action": "confirm"})
    with SessionLocal() as session:
        prior, _ = existing_idempotency(session, "confirm-rule-proposal", key, request_hash)
        if prior is not None:
            return prior
        proposal = session.get(RuleProposal, str(proposal_id))
        if not proposal:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Rule proposal was not found.")
        if proposal.status != "proposed":
            raise ServiceError(409, "DEST_SPEC_VIOLATION", "Rule proposal has already been confirmed or rejected.")
        payload = json.loads(proposal.proposal_json)
        if proposal.destination_id:
            destination = session.get(Destination, proposal.destination_id)
            if not destination:
                raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination was not found.")
            existing_rules = json.loads(destination.rules_json or "{}")
            existing_rules.update(payload["rules"])
            destination.rules_json = canonical_json(existing_rules)
            destination.self_promo_allowed = existing_rules["self_promo_allowed"]
            destination.link_policy = existing_rules["link_policy"]
            destination.disclosure_required = existing_rules["disclosure_required"]
            destination.automation_allowed = existing_rules["automation_allowed"]
            destination.frequency_cap_json = canonical_json(existing_rules["frequency_cap"]) if existing_rules.get("frequency_cap") else None
        proposal.status = "confirmed"
        proposal.confirmed_at = datetime.now(UTC)
        response = {"proposal_id": proposal.id, "status": proposal.status, "destination_id": proposal.destination_id}
        _remember_idempotent_response(session, "confirm-rule-proposal", key, request_hash, response)
        session.commit()
        return response


@app.get("/api/v1/destinations/{destination_id}", tags=["destinations"])
def get_destination(destination_id: UUID):
    with SessionLocal() as session:
        destination = session.get(Destination, str(destination_id))
        if not destination:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination was not found.")
        return destination_response(destination)


@app.get("/api/v1/destinations/{destination_id}/passport", tags=["destinations"])
def get_destination_passport(destination_id: UUID):
    with SessionLocal() as session:
        destination = session.get(Destination, str(destination_id))
        if not destination:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination was not found.")
        return destination_passport(session, destination)


@app.post("/api/v1/policy-evaluations", tags=["rules"])
def evaluate_policy(body: PolicyEvaluationRequest, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    key = require_idempotency(idempotency_key)
    request_value = body.model_dump(mode="json")
    request_hash = sha256_json(request_value)
    with SessionLocal() as session:
        prior, _ = existing_idempotency(session, "policy-evaluation", key, request_hash)
        if prior is not None:
            return prior
        destination = session.get(Destination, str(body.destination_id))
        if not destination:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination was not found.")
        response = {
            "destination_id": destination.id,
            "policy_evaluation": explain_policy(session, destination, body.rendition.model_dump(mode="json")),
        }
        _remember_idempotent_response(session, "policy-evaluation", key, request_hash, response)
        session.commit()
        return response


@app.get("/api/v1/recertification-tickets", tags=["certifications"])
def list_recertification_tickets(limit: int = Query(default=50, ge=1, le=200)):
    with SessionLocal() as session:
        tickets = session.scalars(
            select(RecertificationTicket).where(RecertificationTicket.status == "open")
            .order_by(RecertificationTicket.created_at.asc()).limit(limit)
        ).all()
        return {"items": [{
            "ticket_id": ticket.id, "destination_id": ticket.destination_id,
            "status": ticket.status, "reason": ticket.reason,
            "created_at": iso(ticket.created_at),
        } for ticket in tickets]}


@app.post("/api/v1/destinations/{destination_id}/validate", tags=["destinations"])
def validate_destination(
    destination_id: UUID,
    body: DestinationValidationRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    require_idempotency(idempotency_key)
    with SessionLocal() as session:
        destination = session.get(Destination, str(destination_id))
        if not destination:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination was not found.")
        rendition = body.rendition.model_dump(mode="json")
        warnings = enforce_rules(
            session,
            destination,
            rendition,
            dry_run=True,
            check_frequency=False,
        )
        adapter = get_connector(destination.connector_key)
        verdict = adapter.validate(rendition, destination_dict(destination), {})
        return {**verdict, "warnings": warnings}


@app.post("/api/v1/destinations", status_code=201, tags=["destinations"])
def create_destination(body: DestinationCreate, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    key = require_idempotency(idempotency_key)
    body_value = json.loads(body.model_dump_json())
    request_hash = sha256_json(body_value)
    with SessionLocal() as session:
        prior, _ = existing_idempotency(session, "create-destination", key, request_hash)
        if prior:
            return prior
        existing = session.scalar(
            select(Destination).where(
                Destination.platform_key == body.platform_key,
                Destination.normalized_url == normalize_destination_url(str(body.url)),
            )
        )
        if existing:
            raise ServiceError(
                409,
                "DESTINATION_CONFLICT",
                "A destination with this platform and normalized URL already exists.",
                details={"destination_id": existing.id},
            )
        destination = Destination(
            name=body.name, url=str(body.url), normalized_url=normalize_destination_url(str(body.url)),
            platform_key=body.platform_key, surface=body.surface, category=body.category,
            audience_tags_json=canonical_json(body.audience_tags), language=body.language, geo=body.geo, est_audience=body.est_audience,
            rules_url=str(body.rules_url) if body.rules_url else None,
            frequency_cap_json=canonical_json(body.rules.frequency_cap.model_dump()) if body.rules.frequency_cap else None,
            self_promo_allowed=body.rules.self_promo_allowed, link_policy=body.rules.link_policy,
            disclosure_required=body.rules.disclosure_required, automation_allowed=body.rules.automation_allowed,
            connector_key=body.connector_key, relevance_score=body.relevance_score, relevance_reason=body.relevance_reason,
            level=body.level,
            rules_json=canonical_json(body.rules.model_dump(mode="json")),
            connection_json=canonical_json(body.connection),
        )
        session.add(destination)
        session.flush()
        response = destination_response(destination)
        session.add(IdempotencyRecord(scope="create-destination", key=key, request_hash=request_hash, response_json=canonical_json(response)))
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            existing = session.scalar(
                select(Destination).where(
                    Destination.platform_key == body.platform_key,
                    Destination.normalized_url == normalize_destination_url(str(body.url)),
                )
            )
            if existing:
                raise ServiceError(
                    409,
                    "DESTINATION_CONFLICT",
                    "A destination with this platform and normalized URL already exists.",
                    details={"destination_id": existing.id},
                )
            return replay_after_unique_conflict("create-destination", key, request_hash)
        return response


@app.post("/api/v1/catalogue/import", status_code=201, tags=["catalogue"])
async def import_catalogue(file: UploadFile = CATALOGUE_FILE, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    key = require_idempotency(idempotency_key)
    raw = await file.read(CATALOGUE_MAX_BYTES + 1)
    if len(raw) > CATALOGUE_MAX_BYTES:
        raise ServiceError(413, "DEST_SPEC_VIOLATION", "Catalogue CSV exceeds the 4 MiB upload limit.")
    if not raw:
        raise ServiceError(400, "DEST_SPEC_VIOLATION", "Catalogue CSV is empty.")

    try:
        reader = csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""), strict=True)
        headers = [header.strip() for header in next(reader)]
    except (UnicodeDecodeError, csv.Error, StopIteration) as exc:
        raise ServiceError(400, "DEST_SPEC_VIOLATION", "Catalogue CSV must be valid UTF-8 with a header row.") from exc
    if len(headers) != len(set(headers)):
        raise ServiceError(400, "DEST_SPEC_VIOLATION", "Catalogue CSV contains duplicate column names.")
    missing = sorted(CATALOGUE_REQUIRED_COLUMNS - set(headers))
    unknown = sorted(set(headers) - CATALOGUE_REQUIRED_COLUMNS - CATALOGUE_OPTIONAL_COLUMNS)
    if missing or unknown:
        raise ServiceError(400, "DEST_SPEC_VIOLATION", "Catalogue CSV columns do not match the supported format.", details={"missing_columns": missing, "unknown_columns": unknown})

    parsed_rows: list[tuple[int, DestinationCreate]] = []
    violations: list[dict] = []
    seen_rows: set[tuple[str, str]] = set()
    row_count = 0
    try:
        for row_number, values in enumerate(reader, start=2):
            if not values:
                continue
            row_count += 1
            if row_count > CATALOGUE_MAX_ROWS:
                raise ServiceError(413, "DEST_SPEC_VIOLATION", "Catalogue CSV exceeds the 1000 row limit.")
            if len(values) != len(headers):
                violations.append({"row": row_number, "field": "row", "message": "Column count does not match the header."})
                continue
            row = {header: value.strip() for header, value in zip(headers, values)}
            payload = {
                "name": row["name"], "url": row["url"], "platform_key": row["platform_key"],
                "surface": row["surface"], "category": row["category"], "connector_key": row["connector_key"],
                "audience_tags": [tag.strip() for tag in row.get("audience_tags", "").split("|") if tag.strip()],
                "language": row.get("language") or "en", "geo": row.get("geo") or None,
                "est_audience": row.get("est_audience") or None, "rules_url": row.get("rules_url") or None,
                "level": row.get("level") or "L0", "connection": {},
            }
            try:
                destination = DestinationCreate.model_validate(payload)
            except ValidationError as exc:
                for item in exc.errors()[:10]:
                    violations.append({"row": row_number, "field": ".".join(str(part) for part in item["loc"]), "message": item["msg"]})
                continue
            identity = (destination.platform_key, normalize_destination_url(str(destination.url)))
            if identity in seen_rows:
                violations.append({"row": row_number, "field": "url", "message": "Duplicate destination in this CSV."})
                continue
            seen_rows.add(identity)
            parsed_rows.append((row_number, destination))
    except csv.Error as exc:
        raise ServiceError(400, "DEST_SPEC_VIOLATION", "Catalogue CSV contains malformed quoting.") from exc

    if row_count == 0:
        raise ServiceError(400, "DEST_SPEC_VIOLATION", "Catalogue CSV must contain at least one destination row.")
    if violations:
        raise ServiceError(400, "DEST_SPEC_VIOLATION", "Catalogue CSV contains invalid rows; no destinations were imported.", details={"violations": violations[:50], "violation_count": len(violations)})

    request_hash = sha256_json({"content_sha256": hashlib.sha256(raw).hexdigest()})
    with SessionLocal() as session:
        prior, _ = existing_idempotency(session, "catalogue-import", key, request_hash)
        if prior is not None:
            return prior

        candidate_urls = [normalize_destination_url(str(destination.url)) for _, destination in parsed_rows]
        existing_destinations = session.scalars(select(Destination).where(Destination.normalized_url.in_(candidate_urls))).all()
        existing = {(item.platform_key, item.normalized_url): item for item in existing_destinations}
        imported: list[tuple[int, Destination]] = []
        skipped = []
        for row_number, body in parsed_rows:
            identity = (body.platform_key, normalize_destination_url(str(body.url)))
            if identity in existing:
                skipped.append({"row": row_number, "destination_id": existing[identity].id, "reason": "already_exists"})
                continue
            destination = Destination(
                name=body.name, url=str(body.url), normalized_url=normalize_destination_url(str(body.url)),
                platform_key=body.platform_key, surface=body.surface, category=body.category,
                audience_tags_json=canonical_json(body.audience_tags), language=body.language, geo=body.geo,
                est_audience=body.est_audience, rules_url=str(body.rules_url) if body.rules_url else None,
                connector_key=body.connector_key, level=body.level,
                rules_json=canonical_json(body.rules.model_dump(mode="json")), connection_json=canonical_json({}),
            )
            session.add(destination)
            imported.append((row_number, destination))
        session.flush()
        response = {
            "imported": len(imported), "skipped_existing": len(skipped),
            "items": [{"row": row_number, **destination_response(destination)} for row_number, destination in imported],
            "skipped": skipped,
        }
        session.add(IdempotencyRecord(scope="catalogue-import", key=key, request_hash=request_hash, response_json=canonical_json(response)))
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            return replay_after_unique_conflict("catalogue-import", key, request_hash)
        return response


@app.post("/api/v1/destinations/{destination_id}/certifications", status_code=202, tags=["certifications"])
def create_certification(
    destination_id: UUID,
    body: CertificationCreate | None = None,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    key = require_idempotency(idempotency_key)
    destination_id_text = str(destination_id)
    callback_url = validated_callback_url(str(body.callback_url)) if body and body.callback_url else None
    request_hash = sha256_json({
        "destination_id": destination_id_text,
        "operation": "certification",
        "callback_url": callback_url,
    })
    with SessionLocal() as session:
        prior, job = existing_idempotency(session, f"certification:{destination_id_text}", key, request_hash)
        if prior:
            return prior
        if job:
            return JSONResponse(status_code=202, content=job_response(job))
        destination = session.get(Destination, destination_id_text)
        if not destination:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination was not found.")
        payload = {"destination_id": destination_id_text}
        if callback_url:
            payload["callback_url"] = callback_url
        job = Job(kind="certification", status="queued", progress=0, payload_json=canonical_json(payload))
        session.add(job)
        session.flush()
        run = CertificationRun(destination_id=destination_id_text, job_id=job.id, connector_version="1.0.0", status="queued")
        session.add(run)
        session.add(IdempotencyRecord(scope=f"certification:{destination_id_text}", key=key, request_hash=request_hash, job_id=job.id))
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            return replay_after_unique_conflict(f"certification:{destination_id_text}", key, request_hash)
        return JSONResponse(status_code=202, content=job_response(job))


@app.post("/api/v1/publishes", status_code=202, tags=["publishes"])
def create_publish(body: PublishCreate, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    key = require_idempotency(idempotency_key)
    body_value = json.loads(body.model_dump_json())
    body_value["callback_url"] = validated_callback_url(body_value.get("callback_url"))
    request_hash = sha256_json(body_value)
    destination_id = str(body.destination_id)
    scope = f"publish:{destination_id}"
    with SessionLocal() as session:
        prior, prior_job = existing_idempotency(session, scope, key, request_hash)
        if prior:
            return JSONResponse(status_code=202, content=prior)
        if prior_job:
            return JSONResponse(status_code=202, content=job_response(prior_job))
        # Serializing publish creation by destination makes the frequency cap a
        # real reservation instead of a best-effort worker-time check.
        destination = session.scalar(
            select(Destination)
            .where(Destination.id == destination_id)
            .with_for_update()
        )
        if not destination:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination was not found.")
        rendition = body.rendition.model_dump(mode="json")
        enforce_rules(
            session,
            destination,
            rendition,
            dry_run=body.dry_run,
            check_frequency=not body.dry_run and destination.connector_key != "assisted",
        )
        job_payload = {"destination_id": destination_id, "dry_run": body.dry_run}
        if body_value["callback_url"]:
            job_payload["callback_url"] = body_value["callback_url"]
        job = Job(
            kind="publish",
            status="queued",
            progress=0,
            payload_json=canonical_json(job_payload),
        )
        session.add(job)
        session.flush()
        post = Post(
            destination_id=destination.id,
            job_id=job.id,
            idempotency_key=key,
            request_hash=request_hash,
            rendition_json=canonical_json(rendition),
        )
        session.add(post)
        session.add(IdempotencyRecord(scope=scope, key=key, request_hash=request_hash, job_id=job.id))
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            return replay_after_unique_conflict(scope, key, request_hash)
        return JSONResponse(status_code=202, content=job_response(job))


def _safe_filename(value: str, fallback: str) -> str:
    sanitized = value.replace("/", "_").replace("\\", "_").strip().replace("\x00", "")
    sanitized = re.sub(r"[\x00-\x1f\x7f]", "_", sanitized)[-180:]
    return sanitized or fallback


@app.post("/api/v1/artifacts", status_code=201, tags=["artifacts"])
async def upload_artifact(
    file: UploadFile | None = ARTIFACT_FILE,
    source_url: str | None = ARTIFACT_SOURCE_URL,
    license_name: str | None = ARTIFACT_LICENSE_NAME,
    license_url: str | None = ARTIFACT_LICENSE_URL,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    started = time.perf_counter()
    key = require_idempotency(idempotency_key)
    if bool(file) == bool(source_url):
        raise ServiceError(422, "DEST_SPEC_VIOLATION", "Provide exactly one of file or source_url.")
    content_type = "application/octet-stream"
    filename = "download.bin"
    if file:
        body = await file.read(25 * 1024 * 1024 + 1)
        if len(body) > 25 * 1024 * 1024:
            raise ServiceError(413, "DEST_SPEC_VIOLATION", "Artifact exceeds the 25 MiB upload limit.")
        content_type = file.content_type or content_type
        filename = _safe_filename(file.filename or filename, filename)
        source = "upload"
    else:
        if not get_settings().artifact_source_fetch_is_enabled:
            raise ServiceError(
                403,
                "CONNECTOR_DISABLED",
                "Remote artifact fetching is disabled in this environment.",
            )
        outbound_hosts = get_settings().outbound_allowed_host_set
        safe_url = validate_http_url(
            source_url or "",
            purpose="source_url",
            allowed_private_hosts=outbound_hosts,
            allowed_hosts=outbound_hosts,
        )
        chunks = []
        size = 0
        async with httpx.AsyncClient(follow_redirects=False, timeout=httpx.Timeout(15.0, connect=5.0)) as client:
            try:
                async with client.stream("GET", safe_url) as response:
                    if response.is_redirect:
                        raise ServiceError(422, "DEST_SPEC_VIOLATION", "Redirecting source URLs are not accepted; provide the final URL.")
                    if not response.is_success:
                        raise ServiceError(422, "PLATFORM_CONTENT_REJECTED", "The source URL did not return an artifact.", False, {"status": response.status_code})
                    declared_size = response.headers.get("content-length")
                    if declared_size:
                        try:
                            exceeds_limit = int(declared_size) > 25 * 1024 * 1024
                        except ValueError as exc:
                            raise ServiceError(
                                422,
                                "PLATFORM_CONTENT_REJECTED",
                                "The source URL returned an invalid Content-Length header.",
                            ) from exc
                        if exceeds_limit:
                            raise ServiceError(413, "DEST_SPEC_VIOLATION", "Downloaded artifact exceeds the 25 MiB limit.")
                    content_type = response.headers.get("content-type", content_type).split(";", 1)[0]
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > 25 * 1024 * 1024:
                            raise ServiceError(413, "DEST_SPEC_VIOLATION", "Downloaded artifact exceeds the 25 MiB limit.")
                        chunks.append(chunk)
            except httpx.HTTPError as exc:
                raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Could not download the input artifact.", True, {"reason": type(exc).__name__}) from exc
        body = b"".join(chunks)
        parsed_source = urlsplit(source_url or "")
        filename = _safe_filename(parsed_source.path.rsplit("/", 1)[-1], filename)
        safe_source_url = urlunsplit((parsed_source.scheme, parsed_source.netloc, parsed_source.path, "", ""))
        source = "url"
    if source == "upload":
        safe_source_url = None
    import hashlib
    digest = hashlib.sha256(body).hexdigest()
    rights_declaration = {"license_name": license_name, "license_url": license_url}
    request_hash = sha256_json({"sha256": digest, "source": source, "filename": filename, "rights": rights_declaration})
    artifact_id = str(uuid5(NAMESPACE_URL, f"incubrix:artifact:{key}:{request_hash}"))
    with SessionLocal() as session:
        prior, _ = existing_idempotency(session, "artifact", key, request_hash)
        if prior:
            return prior
        storage = get_storage()
        object_key = f"artifacts/{artifact_id}/{filename}"
        artifact_uri = storage.put_bytes(object_key, body, content_type, filename=filename)
        manifest = output_manifest(
            kind="input_artifact",
            connector_key=None,
            version=None,
            warnings=["License declarations are recorded as supplied and are not independently verified."],
            timings_ms={"processing": int((time.perf_counter() - started) * 1000)},
            settings={"source_type": source},
            rights={
                "media": "user-provided",
                "ai_generated": False,
                "commercial_use": "not_verified",
                "commercial_use_verified": False,
                "license_name": license_name,
                "license_url": license_url,
                "license_declared": bool(license_name and license_url),
                "license_verified": False,
            },
        )
        manifest.update({
            "artifact_id": artifact_id,
            "source_type": source,
            "source_url": safe_source_url,
            "filename": filename,
            "content_type": content_type,
            "size_bytes": len(body),
            "sha256": digest,
            "stored_at": datetime.now(UTC).isoformat(),
        })
        manifest_key = f"manifests/artifacts/{artifact_id}.json"
        storage.put_json(manifest_key, manifest)
        result = {"artifact_id": artifact_id, "uri": artifact_uri, "download_url": storage.download_url(object_key),
                  "manifest_uri": storage.uri(manifest_key), "manifest_url": storage.download_url(manifest_key), **manifest}
        session.add(IdempotencyRecord(scope="artifact", key=key, request_hash=request_hash, response_json=canonical_json(result)))
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            return replay_after_unique_conflict("artifact", key, request_hash)
        return result


@app.get("/api/v1/jobs/{job_id}", tags=["jobs"])
def get_job(job_id: UUID):
    with SessionLocal() as session:
        job = session.get(Job, str(job_id))
        if not job:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Job was not found.")
        return job_response(job)


@app.get("/api/v1/certifications/{certification_id}", tags=["certifications"])
def get_certification(certification_id: UUID):
    with SessionLocal() as session:
        run = session.get(CertificationRun, str(certification_id))
        if not run:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Certification run was not found.")
        destination = session.get(Destination, run.destination_id)
        manifest = None
        evidence_status = "not_available"
        if run.evidence_uri and run.status in {"passed", "failed"}:
            evidence_key = f"certifications/{run.id}/evidence.json"
            try:
                evidence = get_storage().get_json(evidence_key)
            except KeyError:
                evidence_status = "missing"
            except StorageUnavailable as exc:
                raise ServiceError(
                    503,
                    "ARTIFACT_STORAGE_UNAVAILABLE",
                    "Certification evidence storage is temporarily unavailable.",
                    True,
                ) from exc
            except (TypeError, ValueError):
                evidence_status = "corrupt"
                log.error("certification_evidence_invalid certification_id=%s", run.id)
            else:
                manifest = evidence.get("manifest") if isinstance(evidence, dict) else None
                evidence_status = "available" if manifest is not None else "manifest_missing"
        return {"certification_id": run.id, "destination_id": run.destination_id,
                "evidence_id": f"certification:{run.id}",
                "connector_key": destination.connector_key if destination else "unknown",
                "connector_version": run.connector_version, "status": run.status,
                "steps": json.loads(run.steps_json), "cleanup_confirmed": run.cleanup_confirmed,
                "created_at": iso(run.created_at), "finished_at": iso(run.finished_at), "evidence_status": evidence_status,
                "evidence_uri": run.evidence_uri, "manifest": manifest}


def _validated_connector_body(model, body: dict) -> dict:
    try:
        return model.model_validate(body).model_dump(mode="json")
    except ValidationError as exc:
        violations = [
            {"field": ".".join(str(part) for part in item["loc"]), "message": item["msg"]}
            for item in exc.errors()
        ]
        raise ServiceError(
            422,
            "DEST_SPEC_VIOLATION",
            "Connector request validation failed.",
            details={"violations": violations},
        ) from exc


@app.post("/connector/v1/{connector_key}/{method}", tags=["connector contract"])
def invoke_connector(
    connector_key: str,
    method: str,
    request: Request,
    body: dict,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    require_idempotency(idempotency_key)
    if not get_settings().connector_contract_is_enabled:
        raise ServiceError(404, "CONNECTOR_DISABLED", "The direct connector contract is disabled in this environment.")

    model_by_method = {
        "validate": ConnectorValidate,
        "publish": ConnectorValidate,
        "get_status": ConnectorStatus,
        "delete": ConnectorDelete,
        "fetch_metrics": ConnectorMetrics,
        "refresh_auth": ConnectorRefreshAuth,
        "build_assist_pack": ConnectorAssist,
    }
    model = model_by_method.get(method)
    if model is None:
        raise ServiceError(404, "CONNECTOR_DISABLED", f"Unsupported connector method: {method}.")
    validation_body = body if method != "publish" else {key: value for key, value in body.items() if key != "idempotency_key"}
    parsed = _validated_connector_body(model, validation_body)
    connection = parsed.get("connection", {})
    validate_connection_config(connector_key, connection)
    resolved_connection = resolve_connection(connector_key, connection)
    adapter = get_connector(connector_key, resolved_connection)
    if method == "validate":
        return adapter.validate(parsed["rendition"], parsed["destination"], resolved_connection)
    if method == "publish":
        key = body.get("idempotency_key") or idempotency_key
        key = require_idempotency(str(key) if key else None)
        return adapter.publish(parsed["rendition"], parsed["destination"], resolved_connection, key)
    if method == "get_status":
        return adapter.get_status(parsed["platform_post_id"], resolved_connection)
    if method == "delete":
        return adapter.delete(parsed["platform_post_id"], resolved_connection)
    if method == "fetch_metrics":
        return adapter.fetch_metrics(parsed["platform_post_id"], resolved_connection, parsed.get("since"))
    if method == "refresh_auth":
        return adapter.refresh_auth(resolved_connection)
    return adapter.build_assist_pack(parsed["rendition"], parsed["destination"])


@app.get("/api/v1/evidence/{job_id}", tags=["evidence"])
def get_evidence(job_id: UUID):
    with SessionLocal() as session:
        job = session.get(Job, str(job_id))
        if not job or not job.result_json:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Evidence for this job was not found.")
        result = json.loads(job.result_json)
        if not result.get("evidence_uri"):
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "This job has no certification evidence.")
        return {"evidence_uri": result["evidence_uri"], "evidence_url": result.get("evidence_url")}
