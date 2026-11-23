import re
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from incubrix.config import get_settings
from incubrix.db import SessionLocal
from incubrix.errors import ServiceError
from incubrix.models import Destination, Post, RecertificationTicket

_LINK_RE = re.compile(r'''(?i)(?:https?://|www\.)[^\s<>"'`]+''')
_BARE_DOMAIN_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9@.-])(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,}"
    r"(?::\d+)?(?:/[^\s<>\"'`]+)?"
)


def _body_links(body: str) -> list[str]:
    """Extract HTTP links from plain text and Markdown without trusting casing."""

    links = []
    candidates = [*_LINK_RE.findall(body), *_BARE_DOMAIN_RE.findall(body)]
    for match in candidates:
        candidate = match.rstrip(".,;:!?)]}")
        if candidate.lower().startswith("www.") or not candidate.lower().startswith(("http://", "https://")):
            candidate = f"https://{candidate}"
        parsed = urlsplit(candidate)
        if parsed.scheme in {"http", "https"} and parsed.hostname and candidate not in links:
            links.append(candidate)
    return links


def _same_link(left: str, right: str) -> bool:
    def normalized(value: str) -> tuple[str, str, int | None, str, str]:
        parsed = urlsplit(value)
        scheme = parsed.scheme.lower()
        host = (parsed.hostname or "").lower().rstrip(".")
        port = parsed.port
        if (scheme, port) in {("http", 80), ("https", 443)}:
            port = None
        return scheme, host, port, parsed.path.rstrip("/"), parsed.query

    return normalized(left) == normalized(right)


def enforce_certification_freshness(session: Session, destination: Destination) -> None:
    """Block expired certified destinations and retain a local remediation ticket."""

    if destination.level not in {"L3", "L4"} or not destination.last_certified_at:
        return
    cutoff = datetime.now(UTC) - timedelta(days=get_settings().recertification_max_age_days)
    last_certified = destination.last_certified_at
    if last_certified.tzinfo is None:
        last_certified = last_certified.replace(tzinfo=UTC)
    if last_certified >= cutoff:
        return
    _block_expired_destination(session, destination)
    # This state change must survive the rejected publish request so operators
    # can see the block and its remediation ticket.
    session.commit()
    raise ServiceError(
        409,
        "CONNECTOR_DISABLED",
        "Destination certification has expired; recertify it before publishing.",
        details={"destination_id": destination.id, "last_certified_at": last_certified.isoformat()},
    )


def _block_expired_destination(session: Session, destination: Destination) -> None:
    destination.enabled = False
    destination.level = "L2"
    ticket = session.scalar(select(RecertificationTicket).where(
        RecertificationTicket.destination_id == destination.id,
        RecertificationTicket.status == "open",
    ))
    if not ticket:
        session.add(RecertificationTicket(
            destination_id=destination.id,
            reason="Certification expired before the configured recertification deadline.",
        ))


def sweep_expired_certifications() -> int:
    """Demote stale certified destinations and open a local remediation ticket."""

    cutoff = datetime.now(UTC) - timedelta(days=get_settings().recertification_max_age_days)
    with SessionLocal() as session:
        destinations = session.scalars(select(Destination).where(
            Destination.level.in_(["L3", "L4"]),
            Destination.last_certified_at.is_not(None),
            Destination.last_certified_at < cutoff,
        )).all()
        for destination in destinations:
            _block_expired_destination(session, destination)
        session.commit()
        return len(destinations)


def enforce_rules(
    session: Session,
    destination: Destination,
    rendition: dict,
    *,
    dry_run: bool = False,
    check_frequency: bool = True,
    exclude_post_id: str | None = None,
) -> list[str]:
    rules = __import__("json").loads(destination.rules_json or "{}")
    warnings: list[str] = []
    enforce_certification_freshness(session, destination)
    if not destination.enabled:
        raise ServiceError(409, "CONNECTOR_DISABLED", "Destination is disabled.")
    if not rules.get("automation_allowed", destination.automation_allowed) and destination.connector_key != "assisted":
        raise ServiceError(409, "CONNECTOR_DISABLED", "This destination requires assisted, human posting.")
    if rendition.get("is_promotional") and not rules.get("self_promo_allowed", destination.self_promo_allowed):
        raise ServiceError(422, "DEST_SPEC_VIOLATION", "Self-promotion is not allowed at this destination.", details={"rule": "self_promotion"})
    disclosure_required = rules.get("disclosure_required", destination.disclosure_required)
    if rendition.get("ai_generated") and disclosure_required and not rendition.get("ai_disclosure"):
        raise ServiceError(422, "DEST_SPEC_VIOLATION", "This destination requires an AI-content disclosure.", details={"field": "ai_disclosure"})
    policy = rules.get("link_policy", destination.link_policy)
    body_links = _body_links(rendition.get("body", ""))
    canonical_url = rendition.get("canonical_url")
    has_link = bool(canonical_url) or bool(body_links)
    if has_link and policy == "no_links":
        raise ServiceError(422, "DEST_SPEC_VIOLATION", "Links are not allowed at this destination.", details={"rule": "link_policy"})
    if has_link and policy == "review_required":
        raise ServiceError(409, "DEST_SPEC_VIOLATION", "A person must review links before posting to this destination.", details={"rule": "link_policy"})
    if has_link and policy == "canonical_only":
        if not canonical_url:
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "Only the canonical URL is allowed at this destination.", details={"rule": "link_policy"})
        noncanonical = [link for link in body_links if not _same_link(link, str(canonical_url))]
        if noncanonical:
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "Only the canonical URL is allowed at this destination.", details={"rule": "link_policy", "noncanonical_links": noncanonical[:5]})
    cap = rules.get("frequency_cap")
    if cap and not dry_run and check_frequency:
        window = int(cap.get("window_seconds", 0))
        maximum = int(cap.get("max_posts", 0))
        if window > 0:
            since = datetime.now(UTC) - timedelta(seconds=window)
            conditions = [
                Post.destination_id == destination.id,
                # A queued publish reserves capacity before it can make an
                # external call, closing the async race between two workers.
                Post.status.in_(["queued", "running", "publishing", "published", "succeeded"]),
                Post.created_at >= since,
            ]
            if exclude_post_id:
                conditions.append(Post.id != exclude_post_id)
            count = session.scalar(select(func.count(Post.id)).where(*conditions)) or 0
            if count >= maximum:
                expiry_row = session.scalar(
                    select(Post.created_at)
                    .where(*conditions)
                    .order_by(Post.created_at.asc())
                    .offset(max(0, count - maximum))
                    .limit(1)
                )
                retry_after = expiry_row + timedelta(seconds=window) if expiry_row else datetime.now(UTC)
                raise ServiceError(429, "DEST_RULE_FREQUENCY_CAP", "The destination frequency cap has been reached.", True, {"retry_after": retry_after.isoformat(), "max_posts": maximum, "window_seconds": window})
    return warnings
