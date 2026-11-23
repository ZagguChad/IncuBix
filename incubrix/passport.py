from __future__ import annotations

import json
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from incubrix.errors import ServiceError
from incubrix.models import CertificationRun, Destination, Post, RecertificationTicket
from incubrix.rules import enforce_rules
from incubrix.util import iso


_REMEDIATION = {
    "self_promotion": "Remove promotional framing or use a destination that permits self-promotion.",
    "link_policy": "Remove links, use only the canonical URL, or obtain the required human review.",
    "ai_disclosure": "Add the required AI-content disclosure before publishing.",
    "frequency_cap": "Wait until the stated retry time before publishing again.",
}


def explain_policy(session: Session, destination: Destination, rendition: dict) -> dict:
    """Evaluate the same rules used by publishing and return a reviewable verdict."""

    try:
        warnings = enforce_rules(session, destination, rendition, dry_run=True, check_frequency=False)
    except ServiceError as exc:
        details = dict(exc.details or {})
        rule = str(details.get("rule") or details.get("field") or "destination_eligibility")
        return {
            "allowed": False,
            "decision": {
                "rule": rule,
                "code": exc.code,
                "reason": exc.message,
                "policy_source_url": destination.rules_url,
                "recommended_action": _REMEDIATION.get(
                    rule, "Resolve the stated destination eligibility condition and evaluate again."
                ),
                "details": details,
                "human_exception_possible": rule in {"link_policy", "destination_eligibility"},
            },
            "warnings": [],
            "method": "shared_rules_engine_v1",
        }
    return {
        "allowed": True,
        "decision": {
            "rule": None,
            "code": "ALLOWED",
            "reason": "The rendition satisfies the destination rules currently recorded by IncuBrix.",
            "policy_source_url": destination.rules_url,
            "recommended_action": "Human review remains required before non-simulated publishing.",
            "details": {},
            "human_exception_possible": False,
        },
        "warnings": warnings,
        "method": "shared_rules_engine_v1",
    }


def destination_passport(session: Session, destination: Destination) -> dict:
    """Create a transparent profile from persisted facts; unknown facts stay unknown."""

    rules = json.loads(destination.rules_json or "{}")
    latest_certification = session.scalar(
        select(CertificationRun)
        .where(CertificationRun.destination_id == destination.id)
        .order_by(CertificationRun.created_at.desc())
        .limit(1)
    )
    published = session.scalar(
        select(func.count(Post.id)).where(Post.destination_id == destination.id, Post.status.in_(["published", "succeeded"]))
    ) or 0
    failed = session.scalar(
        select(func.count(Post.id)).where(Post.destination_id == destination.id, Post.status.in_(["failed", "needs_review"]))
    ) or 0
    ticket = session.scalar(
        select(RecertificationTicket)
        .where(RecertificationTicket.destination_id == destination.id, RecertificationTicket.status == "open")
        .order_by(RecertificationTicket.created_at.desc())
        .limit(1)
    )
    risk_flags = []
    if not destination.enabled:
        risk_flags.append("destination_disabled")
    if ticket:
        risk_flags.append("open_recertification_ticket")
    if not destination.rules_url:
        risk_flags.append("policy_source_not_recorded")
    permission_status = rules.get("permission_status", "unknown")
    if permission_status != "verified":
        risk_flags.append("publishing_permission_not_verified")
    return {
        "destination": {
            "destination_id": destination.id,
            "name": destination.name,
            "url": destination.url,
            "platform_key": destination.platform_key,
            "connector_key": destination.connector_key,
            "language": destination.language,
            "audience_tags": json.loads(destination.audience_tags_json or "[]"),
        },
        "policy": {
            "source_url": destination.rules_url,
            "rules": rules,
            "rules_version": "destination_rules_v1",
        },
        "permission": {"status": permission_status, "basis": "destination_rules.permission_status"},
        "certification": {
            "level": destination.level,
            "last_certified_at": iso(destination.last_certified_at),
            "latest_run": {
                "certification_id": latest_certification.id,
                "status": latest_certification.status,
                "finished_at": iso(latest_certification.finished_at),
                "cleanup_confirmed": latest_certification.cleanup_confirmed,
            } if latest_certification else None,
        },
        "reliability": {
            "published_count": published,
            "failed_or_review_count": failed,
            "measured_at": datetime.now(UTC).isoformat(),
        },
        "eligibility": {
            "eligible_for_automated_publish": destination.enabled and permission_status == "verified" and destination.connector_key != "assisted",
            "requires_human_review": True,
            "risk_flags": risk_flags,
            "open_ticket": {"ticket_id": ticket.id, "reason": ticket.reason} if ticket else None,
        },
        "trust_score": None,
        "trust_score_basis": "not_measured: permission verification and scored factors are incomplete",
    }
