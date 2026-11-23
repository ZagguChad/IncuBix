from __future__ import annotations

from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ConnectorSurface(_StrictModel):
    key: str = Field(min_length=1, max_length=80)
    content_types: list[Literal["text", "image", "video_link", "audio", "article"]] = Field(min_length=1)
    spec: dict[str, int | bool] = Field(default_factory=dict)


class ConnectorRateLimit(_StrictModel):
    requests: int = Field(ge=1)
    per_seconds: int = Field(ge=1)


class ConnectorRateLimits(_StrictModel):
    per_connection: ConnectorRateLimit


class ConnectorManifest(_StrictModel):
    key: str = Field(pattern=r"^[a-z][a-z0-9_-]{1,62}$")
    version: str = Field(pattern=r"^[0-9]+\.[0-9]+\.[0-9]+$")
    tier: Literal["native", "protocol", "bridge", "feed", "assisted"]
    auth: Literal["oauth2", "api_key", "bot_token", "none"]
    surfaces: list[ConnectorSurface] = Field(min_length=1)
    rate_limits: ConnectorRateLimits
    capabilities: list[str] = Field(min_length=1)
    error_map: dict[str, str]
    instance_param: str = Field(min_length=1)

    @field_validator("surfaces")
    @classmethod
    def unique_surface_keys(cls, value: list[ConnectorSurface]) -> list[ConnectorSurface]:
        keys = [surface.key for surface in value]
        if len(keys) != len(set(keys)):
            raise ValueError("surface keys must be unique")
        return value

    @field_validator("capabilities")
    @classmethod
    def unique_capabilities(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("capabilities must be unique")
        return value


@lru_cache
def connector_manifests() -> dict[str, ConnectorManifest]:
    """Load the packaged manifests once and make them the public metadata source."""

    manifest_dir = Path(__file__).with_name("connectors") / "manifests"
    registry: dict[str, ConnectorManifest] = {}
    for path in sorted(manifest_dir.glob("*.yaml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        manifest = ConnectorManifest.model_validate(raw)
        if manifest.key in registry:
            raise RuntimeError(f"Duplicate connector manifest key: {manifest.key}")
        registry[manifest.key] = manifest
    if not registry:
        raise RuntimeError("No connector manifests were found.")
    return registry


def output_manifest(
    *,
    kind: str,
    connector_key: str | None,
    version: str | None,
    warnings: list[str] | None = None,
    timings_ms: dict | None = None,
    settings: dict | None = None,
    rights: dict | None = None,
    evidence: dict | None = None,
) -> dict:
    from incubrix.config import get_settings

    runtime = get_settings()
    providers = []
    if connector_key:
        providers.append({"key": connector_key, "version": version})
    return {
        "kind": kind,
        "providers": providers,
        "models": [],
        "settings": {
            "app_env": runtime.app_env,
            "simulation_mode": runtime.simulation_mode,
            **(settings or {}),
        },
        "seed": None,
        "timings_ms": timings_ms or {},
        "cost_estimate_usd": None,
        "cost_estimate_status": "not_estimated",
        "cost_estimate_note": "Provider and infrastructure billing data is not available to this service.",
        "warnings": warnings or [],
        "rights": rights or {
            "media": "user-provided or none",
            "ai_generated": False,
            "commercial_use": "only when every configured connector, input media source, and output is appropriately licensed",
            "license_verified": False,
        },
        "evidence": evidence or {},
        "generated_at": datetime.now(UTC).isoformat(),
    }
