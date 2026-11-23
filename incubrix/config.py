from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Default to the fail-closed production posture.  Local Compose and tests set
    # APP_ENV explicitly, so a copied service cannot accidentally become public.
    app_env: str = "production"
    log_level: str = "INFO"
    app_api_key: str | None = None
    trusted_hosts: str = "localhost,127.0.0.1,testserver"
    max_request_bytes: int = Field(default=28 * 1024 * 1024, ge=1_024, le=100 * 1024 * 1024)
    job_lease_seconds: int = Field(default=900, ge=30, le=86_400)
    recertification_max_age_days: int = Field(default=30, ge=1, le=365)
    recertification_retest_lead_days: int = Field(default=2, ge=1, le=30)
    connector_contract_enabled: bool | None = None
    artifact_source_fetch_enabled: bool | None = None
    rule_page_fetch_enabled: bool | None = None
    outbound_allowed_hosts: str = ""
    callback_allowed_hosts: str = ""
    job_callback_signing_secret: str | None = None
    connector_allowed_hosts: str = ""
    media_import_allowed_hosts: str = ""
    database_url: str = "sqlite:///./data/incubrix.db"
    simulation_mode: bool = True
    s3_bucket: str = "incubrix-artifacts"
    s3_endpoint_url: str = "http://s3-mock:5000"
    s3_access_key: str = "testing"
    s3_secret_key: str = "testing"
    s3_public_endpoint_url: str = "http://s3-mock:5000"
    presigned_url_ttl_seconds: int = Field(default=3600, ge=60, le=604_800)
    job_poll_seconds: float = Field(default=1.0, gt=0, le=60)
    discourse_api_key: str | None = None
    discourse_api_username: str = "system"
    lemmy_jwt: str | None = None
    devto_api_key: str | None = None
    ghost_admin_api_key: str | None = None
    ghost_admin_api_version: str = "v5.0"
    partner_webhook_secret: str | None = None
    mastodon_access_token: str | None = None
    discord_webhook_url: str | None = None
    matrix_access_token: str | None = None
    certification_allowed_hosts: str = "localhost,127.0.0.1,lemmy,ghost,discourse"

    @model_validator(mode="after")
    def validate_recertification_window(self):
        if self.recertification_retest_lead_days >= self.recertification_max_age_days:
            raise ValueError("RECERTIFICATION_RETEST_LEAD_DAYS must be less than RECERTIFICATION_MAX_AGE_DAYS.")
        return self

    @field_validator("app_env")
    @classmethod
    def normalize_app_env(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"development", "test", "production"}:
            raise ValueError("APP_ENV must be development, test, or production")
        return normalized

    @property
    def is_local(self) -> bool:
        return self.app_env in {"development", "test"}

    @property
    def auth_required(self) -> bool:
        # Supplying a key is an explicit request to protect even a local stack.
        return not self.is_local or bool(self.app_api_key)

    @property
    def docs_enabled(self) -> bool:
        return self.is_local

    @property
    def connector_contract_is_enabled(self) -> bool:
        if self.connector_contract_enabled is not None:
            return self.connector_contract_enabled
        return self.is_local

    @property
    def artifact_source_fetch_is_enabled(self) -> bool:
        if self.artifact_source_fetch_enabled is not None:
            return self.artifact_source_fetch_enabled
        return self.is_local

    @property
    def rule_page_fetch_is_enabled(self) -> bool:
        if self.rule_page_fetch_enabled is not None:
            return self.rule_page_fetch_enabled
        return self.is_local

    @property
    def trusted_host_list(self) -> list[str]:
        return [host.strip() for host in self.trusted_hosts.split(",") if host.strip()]

    @property
    def outbound_allowed_host_set(self) -> set[str]:
        return {host.strip().lower() for host in self.outbound_allowed_hosts.split(",") if host.strip()}

    @property
    def callback_allowed_host_set(self) -> set[str]:
        return {host.strip().lower() for host in self.callback_allowed_hosts.split(",") if host.strip()}

    @property
    def connector_allowed_host_set(self) -> set[str]:
        return {host.strip().lower() for host in self.connector_allowed_hosts.split(",") if host.strip()}

    @property
    def media_import_allowed_host_set(self) -> set[str]:
        return {host.strip().lower() for host in self.media_import_allowed_hosts.split(",") if host.strip()}

    def validate_runtime(self) -> None:
        if self.auth_required and (not self.app_api_key or len(self.app_api_key) < 24):
            raise RuntimeError("APP_API_KEY must be set to at least 24 characters when authentication is required.")
        if not self.trusted_host_list:
            raise RuntimeError("TRUSTED_HOSTS must contain at least one host.")
        if self.app_env == "production" and "*" in self.trusted_host_list:
            raise RuntimeError("TRUSTED_HOSTS cannot contain '*' in production.")
        if not self.s3_endpoint_url or not self.s3_public_endpoint_url:
            raise RuntimeError("S3_ENDPOINT_URL and S3_PUBLIC_ENDPOINT_URL must name local S3-compatible endpoints.")
        if not self.simulation_mode and not self.connector_allowed_host_set:
            raise RuntimeError(
                "CONNECTOR_ALLOWED_HOSTS must explicitly allow live connector endpoint hosts when SIMULATION_MODE=false."
            )
        if bool(self.callback_allowed_host_set) != bool(self.job_callback_signing_secret):
            raise RuntimeError(
                "CALLBACK_ALLOWED_HOSTS and JOB_CALLBACK_SIGNING_SECRET must be configured together."
            )


@lru_cache
def get_settings() -> Settings:
    return Settings()
