from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

import httpx
from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import IntegrityError

from incubrix.config import get_settings
from incubrix.connectors import get_connector
from incubrix.db import SessionLocal
from incubrix.errors import ServiceError
from incubrix.manifest import output_manifest
from incubrix.models import (
    CertificationRun,
    Destination,
    IdempotencyRecord,
    Job,
    Post,
    RecertificationTicket,
)
from incubrix.network import validate_http_url
from incubrix.rules import enforce_rules, sweep_expired_certifications
from incubrix.schemas import validate_connection_config
from incubrix.storage import get_storage
from incubrix.util import canonical_json, iso

log = logging.getLogger("incubrix.jobs")


def destination_dict(destination: Destination) -> dict:
    rules = json.loads(destination.rules_json or "{}")
    return {
        "destination_id": destination.id,
        "name": destination.name,
        "url": destination.url,
        "platform_key": destination.platform_key,
        "surface": destination.surface,
        "category": destination.category,
        "audience_tags": json.loads(destination.audience_tags_json or "[]"),
        "language": destination.language,
        "geo": destination.geo,
        "est_audience": destination.est_audience,
        "rules_url": destination.rules_url,
        "frequency_cap": rules.get("frequency_cap"),
        "self_promo_allowed": destination.self_promo_allowed,
        "link_policy": destination.link_policy,
        "disclosure_required": destination.disclosure_required,
        "automation_allowed": destination.automation_allowed,
        "connector_key": destination.connector_key,
        "relevance_score": destination.relevance_score,
        "relevance_reason": destination.relevance_reason,
        "level": destination.level,
        "last_certified_at": iso(destination.last_certified_at),
        "spec": rules.get("spec", {}),
        "content_types": rules.get("content_types", ["text", "article"]),
    }


def resolve_connection(connector_key: str, stored: dict) -> dict:
    """Resolve only connector-approved environment references at execution time."""

    validate_connection_config(connector_key, stored)
    connection = dict(stored)
    for key, value in list(connection.items()):
        if key.endswith("_env") and isinstance(value, str):
            secret_name = value
            target_key = key[:-4]
            if secret_name not in os.environ:
                raise ServiceError(401, "CONNECTION_AUTH_EXPIRED", f"Secret environment variable {secret_name} is not configured.")
            connection[target_key] = os.environ[secret_name]
            del connection[key]
    return connection


def claim_next_job() -> Job | None:
    """Atomically claim one available job, recovering only expired leases.

    ``FOR UPDATE SKIP LOCKED`` lets PostgreSQL workers take different jobs in
    parallel. The conditional update is kept as a second guard for SQLite and
    for any database that does not implement row-level locks.
    """

    with SessionLocal() as session:
        now = datetime.now(UTC)
        claimable = or_(
            and_(
                Job.status == "queued",
                or_(Job.not_before.is_(None), Job.not_before <= now),
            ),
            and_(
                Job.status == "running",
                or_(Job.lease_expires_at.is_(None), Job.lease_expires_at <= now),
            ),
        )
        job = session.scalar(
            select(Job)
            .where(claimable)
            .order_by(Job.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if not job:
            return None
        previous_status = job.status
        # A worker lease expiring cannot tell us whether the remote platform
        # accepted a publish just before that worker stopped. Replaying the
        # request could create a second post, so preserve the idempotency key
        # and require explicit reconciliation instead.
        if previous_status == "running" and job.kind == "publish":
            post = session.scalar(select(Post).where(Post.job_id == job.id))
            if post:
                post.status = "needs_review"
            job.status = "failed"
            job.progress = 100
            job.finished_at = now
            job.lease_expires_at = None
            job.error_json = canonical_json({
                "code": "PLATFORM_UNAVAILABLE",
                "message": "Publish outcome is unknown after worker lease expiry; reconcile the destination before retrying.",
                "retryable": False,
                "details": {"reason": "ambiguous_publish_outcome", "manual_review_required": True},
            })
            session.commit()
            log.warning("ambiguous_publish_blocked job_id=%s", job.id, extra={"job_id": job.id})
            return None
        if previous_status == "running" and job.kind == "certification":
            run = session.scalar(select(CertificationRun).where(CertificationRun.job_id == job.id))
            if run and run.platform_post_id and not run.cleanup_confirmed:
                job.status = "failed"
                job.progress = 100
                job.finished_at = now
                job.lease_expires_at = None
                job.error_json = canonical_json({
                    "code": "PLATFORM_UNAVAILABLE",
                    "message": "Certification worker stopped after publishing a test post; cleanup is scheduled without replaying the post.",
                    "retryable": False,
                    "details": {"reason": "certification_cleanup_pending", "certification_id": run.id},
                })
                session.commit()
                log.warning("certification_replay_blocked job_id=%s", job.id, extra={"job_id": job.id})
                return None
        expected_state = Job.status == previous_status
        if previous_status == "queued":
            expected_state = and_(expected_state, or_(Job.not_before.is_(None), Job.not_before <= now))
        else:
            expected_state = and_(
                expected_state,
                or_(Job.lease_expires_at.is_(None), Job.lease_expires_at <= now),
            )
        result = session.execute(
            update(Job)
            .where(Job.id == job.id, expected_state)
            .values(
                status="running",
                progress=5,
                attempts=Job.attempts + 1,
                not_before=None,
                started_at=now,
                lease_expires_at=now + timedelta(seconds=get_settings().job_lease_seconds),
                updated_at=now,
            )
            .execution_options(synchronize_session=False)
        )
        if result.rowcount != 1:
            session.rollback()
            return None
        session.commit()
        session.expire_all()
        return session.get(Job, job.id)


def _update_job(job_id: str, **values) -> None:
    with SessionLocal() as session:
        job = session.get(Job, job_id)
        if not job:
            return
        for key, value in values.items():
            setattr(job, key, value)
        job.updated_at = datetime.now(UTC)
        session.commit()


def _set_publish_state(job_id: str, status: str) -> None:
    """Update a reservation without ever overwriting a confirmed publish."""

    with SessionLocal() as session:
        post = session.scalar(select(Post).where(Post.job_id == job_id))
        if not post:
            return
        if post.status in {"published", "succeeded", "needs_review", "validated"}:
            return
        post.status = status
        session.commit()


def _deliver_completion_callback(job_id: str) -> None:
    """Best-effort, signed terminal-job notification.

    The callback URL is validated at job creation and is constrained by an
    operator-owned allowlist. Delivery does not alter a completed job: callers
    can always poll the job endpoint if their receiver is unavailable.
    """

    settings = get_settings()
    if not settings.job_callback_signing_secret:
        return
    with SessionLocal() as session:
        job = session.get(Job, job_id)
        if not job or job.status not in {"succeeded", "failed"}:
            return
        callback_url = json.loads(job.payload_json or "{}").get("callback_url")
        if not callback_url:
            return
        callback_url = validate_http_url(
            callback_url,
            purpose="completion callback",
            allowed_private_hosts=settings.callback_allowed_host_set,
            allowed_hosts=settings.callback_allowed_host_set,
        )
        event = {
            "event": "job.completed",
            "job_id": job.id,
            "kind": job.kind,
            "status": job.status,
            "finished_at": iso(job.finished_at),
            "result": json.loads(job.result_json) if job.result_json else None,
            "error": json.loads(job.error_json) if job.error_json else None,
        }
    body = canonical_json(event).encode()
    signature = hmac.new(
        settings.job_callback_signing_secret.encode(), body, hashlib.sha256
    ).hexdigest()
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(10.0, connect=4.0)) as client:
            response = client.post(
                callback_url,
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-IncuBrix-Event": "job.completed",
                    "X-IncuBrix-Signature-256": f"sha256={signature}",
                },
            )
            response.raise_for_status()
    except (ServiceError, httpx.HTTPError) as exc:
        log.warning("completion_callback_failed job_id=%s error_type=%s", job_id, type(exc).__name__)


def process_publish(job: Job) -> dict:
    started = time.perf_counter()
    payload = json.loads(job.payload_json)
    with SessionLocal() as session:
        destination = session.get(Destination, payload["destination_id"])
        post = session.scalar(select(Post).where(Post.job_id == job.id))
        if not destination or not post:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Destination or publish record no longer exists.")
        rendition = json.loads(post.rendition_json)
        evidence = {
            "evidence_id": f"publish:{job.id}",
            "destination_id": destination.id,
            "content_sha256": hashlib.sha256(canonical_json(rendition).encode()).hexdigest(),
            "rules_sha256": hashlib.sha256((destination.rules_json or "{}").encode()).hexdigest(),
            "connector_key": destination.connector_key,
        }
        # The frequency-cap slot was reserved transactionally when the job was
        # created. Rechecking other rules here protects against direct DB edits
        # without counting this post against its own reservation.
        enforce_rules(
            session,
            destination,
            rendition,
            dry_run=payload.get("dry_run", False),
            check_frequency=False,
        )
        connection = resolve_connection(destination.connector_key, json.loads(destination.connection_json or "{}"))
        target = destination_dict(destination)
        adapter = get_connector(destination.connector_key, connection)
        verdict = adapter.validate(rendition, target, connection)
        if not verdict.get("ok"):
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "Content does not meet the destination specification.", details={"violations": verdict.get("violations", [])})
        _update_job(job.id, progress=25)
        if destination.connector_key == "assisted":
            pack = adapter.build_assist_pack(rendition, target)
            post.status = "needs_review"
            session.commit()
            manifest = output_manifest(
                kind="assisted_publish_pack", connector_key=destination.connector_key, version=adapter.version,
                warnings=["A person must review and publish this pack manually."],
                timings_ms={"processing": int((time.perf_counter() - started) * 1000)},
                settings={"dry_run": False},
                evidence=evidence,
            )
            storage = get_storage()
            manifest_key = f"manifests/publishes/{job.id}.json"
            manifest_uri = storage.put_json(manifest_key, manifest)
            return {"post_id": post.id, "destination_id": destination.id, "status": "needs_review", "evidence_id": evidence["evidence_id"], "assist_pack": pack,
                    "manifest_uri": manifest_uri, "manifest_url": storage.download_url(manifest_key), "manifest": manifest}
        if payload.get("dry_run", False):
            post.status = "validated"
            session.commit()
            manifest = output_manifest(
                kind="publish_preview", connector_key=destination.connector_key, version=adapter.version,
                warnings=["Dry run: no platform call was made."],
                timings_ms={"processing": int((time.perf_counter() - started) * 1000)},
                settings={"dry_run": True},
                evidence=evidence,
            )
            storage = get_storage()
            manifest_key = f"manifests/publishes/{job.id}.json"
            manifest_uri = storage.put_json(manifest_key, manifest)
            return {"post_id": post.id, "destination_id": destination.id, "status": "validated", "evidence_id": evidence["evidence_id"], "manifest_uri": manifest_uri,
                    "manifest_url": storage.download_url(manifest_key), "manifest": manifest}
        # Commit the state transition before I/O. A crash is visible as a lease
        # expiry rather than a silently queued job, and a later worker retains
        # the same idempotency key for connector-side reconciliation.
        post.status = "publishing"
        session.commit()
        response = adapter.publish(rendition, target, connection, post.idempotency_key)
        post.platform_post_id = str(response["platform_post_id"])
        post.live_url = response.get("live_url")
        post.status = response.get("status", "published")
        session.commit()
        _update_job(job.id, progress=75)
        manifest = output_manifest(
            kind="publish", connector_key=destination.connector_key, version=adapter.version,
            warnings=[], timings_ms={"processing": int((time.perf_counter() - started) * 1000)},
            settings={"dry_run": False},
            evidence=evidence,
        )
        storage = get_storage()
        manifest_key = f"manifests/publishes/{job.id}.json"
        manifest_uri = storage.put_json(manifest_key, manifest)
        return {"post_id": post.id, **response, "destination_id": destination.id, "evidence_id": evidence["evidence_id"], "manifest_uri": manifest_uri, "manifest_url": storage.download_url(manifest_key), "manifest": manifest}


def _certification_host_allowed(destination: Destination) -> bool:
    settings = get_settings()
    host = (urlsplit(destination.url).hostname or "").lower().rstrip(".")
    allowed = {item.strip().lower() for item in settings.certification_allowed_hosts.split(",") if item.strip()}
    return host in allowed


def process_certification(job: Job) -> dict:
    started = time.perf_counter()
    with SessionLocal() as session:
        run = session.scalar(select(CertificationRun).where(CertificationRun.job_id == job.id))
        destination = session.get(Destination, run.destination_id) if run else None
        if not run or not destination:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Certification run or destination was not found.")
        if not _certification_host_allowed(destination):
            raise ServiceError(403, "CONNECTOR_DISABLED", "Certification is restricted to local instances or a configured approved sandbox host.")
        connection = resolve_connection(destination.connector_key, json.loads(destination.connection_json or "{}"))
        target = destination_dict(destination)
        adapter = get_connector(destination.connector_key, connection)
        storage = get_storage()
        certification_id = run.id
        now = datetime.now(UTC).isoformat()
        mode = "simulation" if get_settings().simulation_mode else "local_or_approved_sandbox"
        details_key = f"certifications/{run.id}/details.json"
        details_uri = storage.uri(details_key)
        steps: list[dict] = []
        details: dict = {"mode": mode}
        post_id = run.platform_post_id
        cleanup_confirmed = False
        rendition = {"title": f"IncuBrix connector certification {certification_id[:8]}",
                     "body": f"Automated connector certification test. Run {certification_id}. This post is deleted after verification.",
                     "is_promotional": False, "ai_generated": False}

        def step(name: str, status: str, *, platform_post_id: str | None = None, live_url: str | None = None,
                 error_code: str | None = None, evidence_ref: str | None = details_uri) -> None:
            item = {"name": name, "status": status, "started_at": now, "finished_at": datetime.now(UTC).isoformat()}
            if platform_post_id is not None:
                item["platform_post_id"] = platform_post_id
            if live_url is not None:
                item["live_url"] = live_url
            if error_code is not None:
                item["error_code"] = error_code
            if evidence_ref is not None:
                item["evidence_ref"] = evidence_ref
            steps.append(item)

        outcome = "failed"
        active_step = "validate"
        try:
            check = adapter.validate(rendition, target, connection)
            if not check.get("ok"):
                step("validate", "failed", error_code="DEST_SPEC_VIOLATION")
                details["validation_violations"] = check.get("violations", [])
                raise ServiceError(422, "DEST_SPEC_VIOLATION", "Certification post does not meet this destination's specification.", details={"violations": check.get("violations", [])})
            step("validate", "passed")
            if destination.connector_key == "assisted":
                details["assist_pack"] = adapter.build_assist_pack(rendition, target)
                step("publish", "skipped", evidence_ref=details_uri)
                outcome = "needs_review"
            else:
                active_step = "publish"
                pub = adapter.publish(rendition, target, connection, f"cert-{run.id}")
                post_id = str(pub["platform_post_id"])
                live_url = pub.get("live_url")
                # Persist the external ID before any later status or cleanup
                # call. A crashing worker can then be prevented from posting a
                # duplicate and the cleanup sweeper has a concrete target.
                run.platform_post_id = post_id
                # Run cleanup one minute before the ten-minute assessment
                # limit, leaving scheduling tolerance for the background loop.
                run.cleanup_deadline_at = datetime.now(UTC) + timedelta(minutes=9)
                session.commit()
                step("publish", "passed", platform_post_id=post_id, live_url=live_url)
                active_step = "confirm_live"
                if not live_url:
                    raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Certification publish returned no live URL.")
                step("confirm_live", "passed", live_url=live_url)
                active_step = "get_status"
                status = adapter.get_status(post_id, connection)
                if status.get("status") not in ("published", "live", "active"):
                    raise ServiceError(502, "PLATFORM_UNAVAILABLE", "The test post was not confirmed live.", True, {"status": status.get("status")})
                step("get_status", "passed", platform_post_id=post_id, live_url=status.get("live_url"))
                active_step = "delete"
                cleanup = adapter.delete(post_id, connection)
                cleanup_confirmed = bool(cleanup.get("deleted"))
                if not cleanup_confirmed:
                    raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Certification post could not be deleted; manual cleanup is required.", True, {"platform_post_id": post_id})
                step("delete", "passed", platform_post_id=post_id)
                outcome = "passed"
        except ServiceError as exc:
            if not any(item["name"] == active_step and item["status"] == "failed" for item in steps):
                step(active_step, "failed", platform_post_id=post_id, error_code=exc.code)
            details["error_code"] = exc.code
            details["error_message"] = exc.message
            if post_id and not cleanup_confirmed:
                try:
                    cleanup = adapter.delete(post_id, connection)
                    cleanup_confirmed = bool(cleanup.get("deleted"))
                    step("delete", "passed" if cleanup_confirmed else "failed", platform_post_id=post_id,
                         error_code=None if cleanup_confirmed else "PLATFORM_UNAVAILABLE")
                except ServiceError as cleanup_error:
                    step("delete", "failed", platform_post_id=post_id, error_code=cleanup_error.code)
            outcome = "failed"

        run.status = outcome
        run.cleanup_confirmed = cleanup_confirmed
        if cleanup_confirmed:
            run.cleanup_deadline_at = None
        run.steps_json = canonical_json(steps)
        run.finished_at = datetime.now(UTC)
        scheduled_recertification = json.loads(job.payload_json or "{}").get("scheduled_recertification") is True
        if outcome == "passed":
            if not get_settings().simulation_mode:
                destination.last_certified_at = run.finished_at
                destination.enabled = True
                for ticket in session.scalars(select(RecertificationTicket).where(
                    RecertificationTicket.destination_id == destination.id,
                    RecertificationTicket.status == "open",
                )).all():
                    ticket.status = "closed"
                    ticket.closed_at = run.finished_at
                destination.level = "L3"
            elif scheduled_recertification and not session.scalar(select(RecertificationTicket.id).where(
                RecertificationTicket.destination_id == destination.id,
                RecertificationTicket.status == "open",
            )):
                session.add(RecertificationTicket(
                    destination_id=destination.id,
                    reason="Scheduled retest ran in simulation mode and did not renew live certification; operator action is required.",
                ))
        elif scheduled_recertification and not session.scalar(select(RecertificationTicket.id).where(
            RecertificationTicket.destination_id == destination.id,
            RecertificationTicket.status == "open",
        )):
            session.add(RecertificationTicket(
                destination_id=destination.id,
                reason="Scheduled pre-expiry certification failed; operator review is required.",
            ))
        key = f"certifications/{run.id}/evidence.json"
        run.evidence_uri = storage.uri(key)
        manifest = output_manifest(
            kind="certification", connector_key=destination.connector_key, version=adapter.version,
            timings_ms={"processing": int((time.perf_counter() - started) * 1000)},
            settings={"certification_mode": mode},
            warnings=[] if outcome == "passed" else ["Certification did not pass."],
            evidence={
                "evidence_id": f"certification:{run.id}",
                "destination_id": destination.id,
                "content_sha256": hashlib.sha256(canonical_json(rendition).encode()).hexdigest(),
                "rules_sha256": hashlib.sha256((destination.rules_json or "{}").encode()).hexdigest(),
                "connector_key": destination.connector_key,
            },
        )
        details["manifest"] = manifest
        storage.put_json(details_key, details)
        record = {"certification_id": run.id, "destination_id": destination.id, "evidence_id": f"certification:{run.id}", "connector_key": destination.connector_key,
                  "connector_version": adapter.version, "status": outcome, "steps": steps,
                  "cleanup_confirmed": cleanup_confirmed, "created_at": iso(run.created_at),
                  "finished_at": iso(run.finished_at), "evidence_uri": run.evidence_uri, "manifest": manifest}
        storage.put_json(key, record)
        session.commit()
        if outcome == "failed":
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Certification failed. See the stored run evidence.", False,
                               {"certification_id": run.id, "evidence_uri": run.evidence_uri, "cleanup_confirmed": cleanup_confirmed})
        return {**record, "evidence_url": storage.download_url(key)}


def process_job(job: Job) -> None:
    try:
        if job.kind == "publish":
            result = process_publish(job)
        elif job.kind == "certification":
            result = process_certification(job)
        else:
            raise ServiceError(400, "DEST_SPEC_VIOLATION", f"Unsupported job type: {job.kind}.")
        _update_job(
            job.id,
            status="succeeded",
            progress=100,
            result_json=canonical_json(result),
            finished_at=datetime.now(UTC),
            lease_expires_at=None,
        )
    except ServiceError as exc:
        retryable_platform_error = exc.retryable and exc.code in {"PLATFORM_RATE_LIMIT", "PLATFORM_UNAVAILABLE"} and job.attempts < 5
        if retryable_platform_error:
            if job.kind == "publish":
                _set_publish_state(job.id, "queued")
            delay = exc.details.get("retry_after_seconds")
            if delay is None:
                delay = min(2 ** max(0, job.attempts - 1), 30)
            delay = min(max(float(delay), 1.0), 300.0)
            _update_job(
                job.id,
                status="queued",
                progress=min(job.progress, 90),
                not_before=datetime.now(UTC) + timedelta(seconds=delay),
                error_json=canonical_json(
                    {"code": exc.code, "message": exc.message, "retryable": True, "details": exc.details}
                ),
                finished_at=None,
                lease_expires_at=None,
            )
        else:
            if job.kind == "publish":
                _set_publish_state(job.id, "failed")
            _update_job(
                job.id,
                status="failed",
                progress=100,
                error_json=canonical_json(
                    {"code": exc.code, "message": exc.message, "retryable": exc.retryable, "details": exc.details}
                ),
                finished_at=datetime.now(UTC),
                lease_expires_at=None,
            )
    except Exception as exc:  # Keep public errors sanitized; logs carry correlation/job ID only.
        log.exception("job_failed job_id=%s error_type=%s", job.id, type(exc).__name__)
        if job.kind == "publish":
            _set_publish_state(job.id, "failed")
        _update_job(
            job.id,
            status="failed",
            progress=100,
            error_json=canonical_json(
                {"code": "INTERNAL_ERROR", "message": "The job failed unexpectedly.", "retryable": False, "details": {}}
            ),
            finished_at=datetime.now(UTC),
            lease_expires_at=None,
        )
    finally:
        _deliver_completion_callback(job.id)


def sweep_certification_cleanup() -> int:
    """Delete persisted certification test posts before their hard deadline."""

    now = datetime.now(UTC)
    cleaned = 0
    with SessionLocal() as session:
        runs = session.scalars(select(CertificationRun).where(
            CertificationRun.cleanup_confirmed.is_(False),
            CertificationRun.platform_post_id.is_not(None),
            CertificationRun.cleanup_deadline_at.is_not(None),
            CertificationRun.cleanup_deadline_at <= now,
        )).all()
        for run in runs:
            destination = session.get(Destination, run.destination_id)
            if not destination:
                continue
            try:
                connection = resolve_connection(destination.connector_key, json.loads(destination.connection_json or "{}"))
                adapter = get_connector(destination.connector_key, connection)
                cleanup = adapter.delete(run.platform_post_id, connection)
                if not cleanup.get("deleted"):
                    continue
                run.cleanup_confirmed = True
                run.cleanup_deadline_at = None
                run.status = "failed"
                steps = json.loads(run.steps_json or "[]")
                steps.append({
                    "name": "recovery_delete", "status": "passed", "started_at": now.isoformat(),
                    "finished_at": datetime.now(UTC).isoformat(), "platform_post_id": run.platform_post_id,
                })
                run.steps_json = canonical_json(steps)
                run.finished_at = datetime.now(UTC)
                cleaned += 1
            except ServiceError as exc:
                log.warning("certification_cleanup_retry certification_id=%s code=%s", run.id, exc.code)
        session.commit()
    return cleaned


def sweep_due_recertifications() -> int:
    """Queue one allowlist-gated recertification before each certification expires."""

    settings = get_settings()
    if settings.simulation_mode:
        return 0
    due_before = datetime.now(UTC) - timedelta(
        days=settings.recertification_max_age_days - settings.recertification_retest_lead_days
    )
    scheduled = 0
    with SessionLocal() as session:
        destinations = session.scalars(select(Destination).where(
            Destination.level.in_(["L3", "L4"]),
            Destination.enabled.is_(True),
            Destination.last_certified_at.is_not(None),
            Destination.last_certified_at <= due_before,
        ).order_by(Destination.last_certified_at, Destination.id)).all()
        active_jobs = session.scalars(select(Job).where(
            Job.kind == "certification",
            Job.status.in_(["queued", "running"]),
        )).all()
        active_destination_ids = {
            json.loads(item.payload_json or "{}").get("destination_id")
            for item in active_jobs
        }
        for destination in destinations:
            last_certified = destination.last_certified_at
            if last_certified.tzinfo is None:
                last_certified = last_certified.replace(tzinfo=UTC)
            schedule_key = f"due-{last_certified.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}"
            scope = f"recertification:{destination.id}"
            if session.scalar(select(IdempotencyRecord.id).where(
                IdempotencyRecord.scope == scope,
                IdempotencyRecord.key == schedule_key,
            )):
                continue

            # Do not race a human-started certification for this destination.
            if destination.id in active_destination_ids:
                continue

            job = Job(
                kind="certification",
                status="queued",
                progress=0,
                payload_json=canonical_json({
                    "destination_id": destination.id,
                    "scheduled_recertification": True,
                }),
            )
            session.add(job)
            session.flush()
            run = CertificationRun(
                destination_id=destination.id,
                job_id=job.id,
                connector_version="1.0.0",
                status="queued",
            )
            session.add(run)
            session.add(IdempotencyRecord(
                scope=scope,
                key=schedule_key,
                request_hash=hashlib.sha256(
                    canonical_json({"destination_id": destination.id, "last_certified_at": iso(last_certified)}).encode()
                ).hexdigest(),
                job_id=job.id,
            ))
            try:
                session.commit()
            except IntegrityError:
                # The unique idempotency key arbitrates concurrent worker replicas.
                session.rollback()
                log.info("recertification_schedule_race destination_id=%s", destination.id)
                continue
            scheduled += 1
            active_destination_ids.add(destination.id)
    return scheduled


async def worker_loop(stop: asyncio.Event) -> None:
    last_recertification_sweep = datetime.min.replace(tzinfo=UTC)
    last_certification_cleanup_sweep = datetime.min.replace(tzinfo=UTC)
    last_due_recertification_sweep = datetime.min.replace(tzinfo=UTC)
    while not stop.is_set():
        if datetime.now(UTC) - last_recertification_sweep >= timedelta(minutes=1):
            demoted = await asyncio.to_thread(sweep_expired_certifications)
            if demoted:
                log.warning("recertification_sweep_demoted count=%s", demoted)
            last_recertification_sweep = datetime.now(UTC)
        if datetime.now(UTC) - last_due_recertification_sweep >= timedelta(minutes=1):
            scheduled = await asyncio.to_thread(sweep_due_recertifications)
            if scheduled:
                log.info("recertification_sweep_scheduled count=%s", scheduled)
            last_due_recertification_sweep = datetime.now(UTC)
        if datetime.now(UTC) - last_certification_cleanup_sweep >= timedelta(seconds=15):
            cleaned = await asyncio.to_thread(sweep_certification_cleanup)
            if cleaned:
                log.warning("certification_cleanup_sweep_cleaned count=%s", cleaned)
            last_certification_cleanup_sweep = datetime.now(UTC)
        job = claim_next_job()
        if job:
            await asyncio.to_thread(process_job, job)
        else:
            try:
                await asyncio.wait_for(stop.wait(), timeout=get_settings().job_poll_seconds)
            except TimeoutError:
                pass
