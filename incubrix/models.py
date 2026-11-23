from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from incubrix.db import Base


def utcnow() -> datetime:
    return datetime.now(UTC)


class Destination(Base):
    __tablename__ = "destinations"
    __table_args__ = (
        UniqueConstraint(
            "platform_key",
            "normalized_url",
            name="uq_destination_platform_normalized_url",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    normalized_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    platform_key: Mapped[str] = mapped_column(String(80), nullable=False)
    surface: Mapped[str] = mapped_column(String(80), nullable=False)
    category: Mapped[str] = mapped_column(String(120), nullable=False)
    audience_tags_json: Mapped[str] = mapped_column(Text, default="[]", nullable=False)
    language: Mapped[str] = mapped_column(String(35), default="en", nullable=False)
    geo: Mapped[str | None] = mapped_column(String(100))
    est_audience: Mapped[int | None] = mapped_column(Integer)
    rules_url: Mapped[str | None] = mapped_column(String(2048))
    frequency_cap_json: Mapped[str | None] = mapped_column(Text)
    self_promo_allowed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    link_policy: Mapped[str] = mapped_column(String(30), default="review_required", nullable=False)
    disclosure_required: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    automation_allowed: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    connector_key: Mapped[str] = mapped_column(String(80), nullable=False)
    relevance_score: Mapped[int | None] = mapped_column(Integer)
    relevance_reason: Mapped[str | None] = mapped_column(Text)
    level: Mapped[str] = mapped_column(String(2), default="L0", nullable=False)
    last_certified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rules_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    connection_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (Index("ix_jobs_claimable", "status", "not_before", "created_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    kind: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="queued", nullable=False, index=True)
    progress: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    not_before: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    result_json: Mapped[str | None] = mapped_column(Text)
    error_json: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class IdempotencyRecord(Base):
    __tablename__ = "idempotency_records"
    __table_args__ = (UniqueConstraint("scope", "key", name="uq_idempotency_scope_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    scope: Mapped[str] = mapped_column(String(200), nullable=False)
    key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    job_id: Mapped[str | None] = mapped_column(ForeignKey("jobs.id"))
    response_json: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class Post(Base):
    __tablename__ = "posts"
    __table_args__ = (
        UniqueConstraint("job_id", name="uq_post_job"),
        UniqueConstraint("destination_id", "idempotency_key", name="uq_post_destination_idempotency"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    destination_id: Mapped[str] = mapped_column(ForeignKey("destinations.id"), nullable=False, index=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    platform_post_id: Mapped[str | None] = mapped_column(String(300))
    live_url: Mapped[str | None] = mapped_column(String(2048))
    status: Mapped[str] = mapped_column(String(30), default="queued", nullable=False)
    rendition_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class CertificationRun(Base):
    __tablename__ = "certification_runs"
    __table_args__ = (UniqueConstraint("job_id", name="uq_certification_run_job"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    destination_id: Mapped[str] = mapped_column(ForeignKey("destinations.id"), nullable=False, index=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="queued", nullable=False)
    connector_version: Mapped[str] = mapped_column(String(40), nullable=False)
    steps_json: Mapped[str] = mapped_column(Text, default="[]", nullable=False)
    cleanup_confirmed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    platform_post_id: Mapped[str | None] = mapped_column(String(300))
    cleanup_deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    evidence_uri: Mapped[str | None] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RuleProposal(Base):
    __tablename__ = "rule_proposals"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    destination_id: Mapped[str | None] = mapped_column(ForeignKey("destinations.id"), index=True)
    source_url: Mapped[str | None] = mapped_column(String(2048))
    source_sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    proposal_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="proposed", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RecertificationTicket(Base):
    __tablename__ = "recertification_tickets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    destination_id: Mapped[str] = mapped_column(ForeignKey("destinations.id"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), default="open", nullable=False, index=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RecommendationFeedback(Base):
    __tablename__ = "recommendation_feedback"
    __table_args__ = (UniqueConstraint("recommendation_key", "destination_id", name="uq_recommendation_feedback"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    recommendation_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    destination_id: Mapped[str] = mapped_column(ForeignKey("destinations.id"), nullable=False, index=True)
    accepted: Mapped[bool] = mapped_column(Boolean, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
