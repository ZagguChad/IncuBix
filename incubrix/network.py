"""Outbound URL validation shared by artifact and connector code.

Application validation cannot replace network egress controls, but rejecting
non-HTTP URLs and non-public address ranges prevents the common SSRF cases
before an HTTP client is allowed to connect.
"""

from __future__ import annotations

import socket
from ipaddress import ip_address
from urllib.parse import urlsplit

from incubrix.errors import ServiceError


def validate_http_url(
    value: str,
    *,
    purpose: str,
    allowed_private_hosts: set[str] | None = None,
    allowed_hosts: set[str] | None = None,
) -> str:
    """Validate a HTTP(S) URL and reject credentials and private addresses.

    ``allowed_private_hosts`` is intentionally a hostname allowlist, not a
    blanket private-network bypass.  Production deployments should also apply
    equivalent egress rules at their network boundary to protect against DNS
    rebinding after this validation step.
    """

    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ServiceError(422, "DEST_SPEC_VIOLATION", f"{purpose} contains an invalid port.") from exc

    hostname = (parsed.hostname or "").lower().rstrip(".")
    if parsed.scheme not in {"http", "https"} or not hostname or parsed.username or parsed.password:
        raise ServiceError(
            422,
            "DEST_SPEC_VIOLATION",
            f"{purpose} must be a HTTP or HTTPS URL without embedded credentials.",
        )

    allowed = {host.lower().rstrip(".") for host in allowed_private_hosts or set()}
    permitted = {host.lower().rstrip(".") for host in allowed_hosts or set()}
    if allowed_hosts is not None and hostname not in permitted:
        raise ServiceError(422, "DEST_SPEC_VIOLATION", f"{purpose} host is not in the configured allowlist.")
    if hostname in allowed:
        return value

    try:
        addresses = {
            result[4][0]
            for result in socket.getaddrinfo(
                hostname,
                port or (443 if parsed.scheme == "https" else 80),
                type=socket.SOCK_STREAM,
            )
        }
    except OSError as exc:
        raise ServiceError(422, "DEST_SPEC_VIOLATION", f"{purpose} hostname could not be resolved.") from exc

    if not addresses:
        raise ServiceError(422, "DEST_SPEC_VIOLATION", f"{purpose} hostname could not be resolved.")
    for address in addresses:
        try:
            address_value = ip_address(address)
        except ValueError as exc:
            raise ServiceError(422, "DEST_SPEC_VIOLATION", f"{purpose} resolved to an invalid address.") from exc
        if not address_value.is_global:
            raise ServiceError(
                422,
                "DEST_SPEC_VIOLATION",
                f"{purpose} cannot target private, loopback, or link-local addresses.",
            )
    return value
