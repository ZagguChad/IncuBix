"""Pydantic contracts for every externally supplied API value.

These models are deliberately stricter than the connector implementations: a
connector may consume a subset of a rendition, but clients must never be able
to use an arbitrary payload as a side channel for credentials or unbounded
data.
"""

from __future__ import annotations

import re
from typing import Any, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
)

SUPPORTED_CONNECTOR_KEYS = frozenset(
    {
        "assisted", "devto", "discourse", "ghost", "hashnode", "lemmy", "matrix",
        "mastodon", "peertube", "signed-webhook", "slack", "discord-webhook",
    }
)

# A stored connection is configuration, not a secret store. Keeping this
# connector-specific prevents an attacker from using an arbitrary environment
# variable as a credential sent to an attacker-controlled endpoint.
CONNECTION_FIELDS: dict[str, frozenset[str]] = {
    "discourse": frozenset({"api_key_env", "api_username", "base_url", "category_id"}),
    "lemmy": frozenset({"base_url", "community_id", "jwt_env"}),
    "devto": frozenset({"api_key_env", "base_url"}),
    "ghost": frozenset({"admin_api_key_env", "api_version", "base_url"}),
    "signed-webhook": frozenset(
        {"delete_url_template", "publish_url", "signing_secret_env", "status_url_template"}
    ),
    "mastodon": frozenset({"access_token_env", "base_url", "visibility"}),
    "discord-webhook": frozenset({"webhook_url_env"}),
    "matrix": frozenset({"access_token_env", "base_url", "room_id"}),
    "slack": frozenset({"bot_token_env", "channel_id", "base_url"}),
    "hashnode": frozenset({"personal_access_token_env", "publication_id", "base_url"}),
    "peertube": frozenset({"access_token_env", "base_url", "channel_id"}),
    "assisted": frozenset(),
}
SECRET_ENV_FIELDS: dict[str, frozenset[str]] = {
    "discourse": frozenset({"DISCOURSE_API_KEY"}),
    "lemmy": frozenset({"LEMMY_JWT"}),
    "devto": frozenset({"DEVTO_API_KEY"}),
    "ghost": frozenset({"GHOST_ADMIN_API_KEY"}),
    "signed-webhook": frozenset({"PARTNER_WEBHOOK_SECRET"}),
    "mastodon": frozenset({"MASTODON_ACCESS_TOKEN"}),
    "discord-webhook": frozenset({"DISCORD_WEBHOOK_URL"}),
    "matrix": frozenset({"MATRIX_ACCESS_TOKEN"}),
    "slack": frozenset({"SLACK_BOT_TOKEN"}),
    "hashnode": frozenset({"HASHNODE_PERSONAL_ACCESS_TOKEN"}),
    "peertube": frozenset({"PEERTUBE_ACCESS_TOKEN"}),
    "assisted": frozenset(),
}
_ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
_SENSITIVE_KEY = re.compile(
    r"(?:api[_-]?key|auth(?:orization)?|credential|password|private[_-]?key|secret|token|jwt)",
    re.IGNORECASE,
)


def _validate_http_url(value: object, field_name: str) -> None:
    try:
        parsed = HttpUrl(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"connection.{field_name} must be a valid HTTP(S) URL") from exc
    if parsed.username or parsed.password:
        raise ValueError(f"connection.{field_name} cannot contain embedded credentials")


def validate_connection_config(connector_key: str, connection: dict[str, Any]) -> None:
    """Reject inline secrets, unknown fields, and malformed endpoint templates."""

    if connector_key not in CONNECTION_FIELDS:
        raise ValueError(f"connector_key must be one of {', '.join(sorted(SUPPORTED_CONNECTOR_KEYS))}")
    if not isinstance(connection, dict):
        raise ValueError("connection must be an object")  # noqa: TRY004 - Pydantic converts ValueError to a 422 response.

    unexpected = sorted(set(connection) - CONNECTION_FIELDS[connector_key])
    if unexpected:
        raise ValueError(
            f"connection contains unsupported fields for {connector_key}: {', '.join(unexpected)}"
        )

    for key, value in connection.items():
        if key.endswith("_env"):
            if not isinstance(value, str) or not _ENV_NAME.fullmatch(value):
                raise ValueError(f"connection.{key} must be an uppercase environment variable name")
            if value not in SECRET_ENV_FIELDS[connector_key]:
                raise ValueError(f"connection.{key} is not an allowed secret reference for {connector_key}")
        elif _SENSITIVE_KEY.search(key):
            raise ValueError(f"connection.{key} must be an approved _env reference; inline secrets are not accepted")

    for key in ("base_url", "publish_url"):
        if key in connection:
            _validate_http_url(connection[key], key)

    for key in ("status_url_template", "delete_url_template"):
        if key not in connection:
            continue
        template = connection[key]
        if not isinstance(template, str) or template.count("{platform_post_id}") != 1:
            raise ValueError(f"connection.{key} must contain exactly one {{platform_post_id}} placeholder")
        remaining = template.replace("{platform_post_id}", "")
        if "{" in remaining or "}" in remaining:
            raise ValueError(f"connection.{key} contains an unsupported template placeholder")
        _validate_http_url(template.replace("{platform_post_id}", "placeholder"), key)

    if connector_key == "slack" and (
        not isinstance(connection.get("channel_id"), str)
        or not re.fullmatch(r"[A-Z0-9]{2,100}", connection["channel_id"])
    ):
        raise ValueError("connection.channel_id must be a Slack channel or conversation ID")
    if connector_key == "hashnode" and (
        not isinstance(connection.get("publication_id"), str)
        or not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", connection["publication_id"])
    ):
        raise ValueError("connection.publication_id must be a valid Hashnode publication ID")
    if connector_key == "peertube" and (
        not isinstance(connection.get("channel_id"), str)
        or not re.fullmatch(r"[1-9][0-9]{0,17}", connection["channel_id"])
    ):
        raise ValueError("connection.channel_id must be a positive PeerTube channel ID")


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FrequencyCap(StrictModel):
    max_posts: int = Field(ge=0)
    window_seconds: int = Field(gt=0, le=31_536_000)


class DestinationSpec(StrictModel):
    title_max_chars: int | None = Field(default=None, ge=0, le=100_000)
    body_max_chars: int | None = Field(default=None, ge=0, le=1_000_000)
    links_allowed: bool = True


class DestinationRules(StrictModel):
    frequency_cap: FrequencyCap | None = None
    self_promo_allowed: bool = False
    link_policy: Literal["allowed", "canonical_only", "no_links", "review_required"] = "review_required"
    disclosure_required: bool = True
    automation_allowed: bool = True
    permission_status: Literal["unknown", "verified", "revoked"] = "unknown"
    spec: DestinationSpec = Field(default_factory=DestinationSpec)
    content_types: list[Literal["text", "image", "video_link", "audio", "article"]] = Field(
        default_factory=lambda: ["text", "article"], min_length=1, max_length=5
    )

    @field_validator("content_types")
    @classmethod
    def unique_content_types(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("content_types must not contain duplicates")
        return value


class DestinationCreate(StrictModel):
    name: str = Field(min_length=1, max_length=200)
    url: HttpUrl
    platform_key: str = Field(min_length=1, max_length=80, pattern=r"^[a-z][a-z0-9_-]{0,79}$")
    surface: str = Field(min_length=1, max_length=80, pattern=r"^[a-z][a-z0-9_-]{0,79}$")
    category: str = Field(min_length=1, max_length=120)
    audience_tags: list[str] = Field(default_factory=list, max_length=50)
    language: str = Field(default="en", min_length=2, max_length=35)
    geo: str | None = Field(default=None, max_length=100)
    est_audience: int | None = Field(default=None, ge=0)
    rules_url: HttpUrl | None = None
    connector_key: str = Field(pattern=r"^[a-z][a-z0-9_-]{1,62}$")
    relevance_score: int | None = Field(default=None, ge=1, le=5)
    relevance_reason: str | None = Field(default=None, max_length=1_000)
    level: Literal["L0", "L1", "L2", "L3", "L4"] = "L0"
    rules: DestinationRules = Field(default_factory=DestinationRules)
    connection: dict[str, Any] = Field(default_factory=dict)

    @field_validator("url", "rules_url")
    @classmethod
    def reject_embedded_url_credentials(cls, value: HttpUrl | None) -> HttpUrl | None:
        if value is not None and (value.username or value.password):
            raise ValueError("URLs cannot contain embedded credentials")
        return value

    @field_validator("audience_tags")
    @classmethod
    def normalize_audience_tags(cls, value: list[str]) -> list[str]:
        tags = [tag.strip() for tag in value]
        if any(not tag or len(tag) > 100 for tag in tags):
            raise ValueError("audience_tags must contain non-empty values of at most 100 characters")
        if len(tags) != len(set(tags)):
            raise ValueError("audience_tags must not contain duplicates")
        return tags

    @model_validator(mode="after")
    def validate_connection(self) -> DestinationCreate:
        validate_connection_config(self.connector_key, self.connection)
        return self


class MediaItem(StrictModel):
    url: HttpUrl
    media_type: Literal["image", "video", "audio"]
    alt_text: str | None = Field(default=None, max_length=2_000)

    @field_validator("url")
    @classmethod
    def reject_embedded_url_credentials(cls, value: HttpUrl) -> HttpUrl:
        if value.username or value.password:
            raise ValueError("media URLs cannot contain embedded credentials")
        return value


class Rendition(StrictModel):
    title: str | None = Field(default=None, max_length=1_000)
    body: str = Field(min_length=1, max_length=100_000)
    canonical_url: HttpUrl | None = None
    media: list[MediaItem] = Field(default_factory=list, max_length=25)
    tags: list[str] = Field(default_factory=list, max_length=4)
    is_promotional: bool = False
    ai_generated: bool = False
    ai_disclosure: str | None = Field(default=None, max_length=1_000)
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict, max_length=25)

    @field_validator("canonical_url")
    @classmethod
    def reject_embedded_url_credentials(cls, value: HttpUrl | None) -> HttpUrl | None:
        if value is not None and (value.username or value.password):
            raise ValueError("canonical_url cannot contain embedded credentials")
        return value

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, value: list[str]) -> list[str]:
        tags = [tag.strip() for tag in value]
        if any(not tag or len(tag) > 30 for tag in tags):
            raise ValueError("tags must contain non-empty values of at most 30 characters")
        if len(tags) != len(set(tags)):
            raise ValueError("tags must not contain duplicates")
        return tags

    @field_validator("metadata")
    @classmethod
    def reject_metadata_secrets(
        cls, value: dict[str, str | int | float | bool | None]
    ) -> dict[str, str | int | float | bool | None]:
        if any(_SENSITIVE_KEY.search(key) for key in value):
            raise ValueError("metadata must not contain credentials or secrets")
        return value


class PublishCreate(StrictModel):
    destination_id: UUID
    rendition: Rendition
    dry_run: bool = False
    callback_url: HttpUrl | None = None

    @field_validator("callback_url")
    @classmethod
    def reject_embedded_callback_credentials(cls, value: HttpUrl | None) -> HttpUrl | None:
        if value is not None and (value.username or value.password):
            raise ValueError("callback_url cannot contain embedded credentials")
        return value


class CertificationCreate(StrictModel):
    callback_url: HttpUrl | None = None

    @field_validator("callback_url")
    @classmethod
    def reject_embedded_callback_credentials(cls, value: HttpUrl | None) -> HttpUrl | None:
        if value is not None and (value.username or value.password):
            raise ValueError("callback_url cannot contain embedded credentials")
        return value


class DestinationValidationRequest(StrictModel):
    rendition: Rendition


class PolicyEvaluationRequest(StrictModel):
    destination_id: UUID
    rendition: Rendition


class ConnectorValidate(StrictModel):
    rendition: Rendition
    destination: dict[str, Any]
    connection: dict[str, Any] = Field(default_factory=dict)


class ConnectorPublish(ConnectorValidate):
    idempotency_key: str = Field(min_length=1, max_length=255)


class ConnectorStatus(StrictModel):
    platform_post_id: str = Field(min_length=1, max_length=300)
    connection: dict[str, Any] = Field(default_factory=dict)


class ConnectorDelete(ConnectorStatus):
    pass


class ConnectorMetrics(ConnectorStatus):
    since: str | None = Field(default=None, max_length=100)


class ConnectorRefreshAuth(StrictModel):
    connection: dict[str, Any] = Field(default_factory=dict)


class ConnectorAssist(StrictModel):
    rendition: Rendition
    destination: dict[str, Any]


class RecommendationRequest(StrictModel):
    audience_tags: list[str] = Field(default_factory=list, max_length=50)
    language: str | None = Field(default=None, min_length=2, max_length=35)
    rendition: Rendition | None = None
    limit: int = Field(default=10, ge=1, le=50)

    @field_validator("audience_tags")
    @classmethod
    def normalize_recommendation_tags(cls, value: list[str]) -> list[str]:
        tags = [tag.strip().lower() for tag in value]
        if any(not tag or len(tag) > 100 for tag in tags) or len(tags) != len(set(tags)):
            raise ValueError("audience_tags must contain unique non-empty values of at most 100 characters")
        return tags


class RuleProposalRequest(StrictModel):
    rules_text: str | None = Field(default=None, min_length=1, max_length=50_000)
    source_url: HttpUrl | None = None
    destination_id: UUID | None = None

    @field_validator("source_url")
    @classmethod
    def reject_embedded_source_credentials(cls, value: HttpUrl | None) -> HttpUrl | None:
        if value is not None and (value.username or value.password):
            raise ValueError("source_url cannot contain embedded credentials")
        return value

    @model_validator(mode="after")
    def require_exactly_one_rule_source(self) -> RuleProposalRequest:
        if bool(self.rules_text) == bool(self.source_url):
            raise ValueError("provide exactly one of rules_text or source_url")
        return self


class TailorRequest(StrictModel):
    destination_id: UUID
    rendition: Rendition


class RecommendationFeedbackRequest(StrictModel):
    destination_id: UUID
    accepted: bool
