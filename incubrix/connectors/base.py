from __future__ import annotations

import hashlib
import hmac
import json
import threading
import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from html import escape
from typing import Any, ClassVar
from urllib.parse import quote

import httpx

from incubrix.config import get_settings
from incubrix.errors import ServiceError
from incubrix.manifest import connector_manifests
from incubrix.network import validate_http_url


def normalized_metrics(
    *, views=None, likes=None, comments=None, shares=None, saves=None, watch_time_s=None, clicks=None
) -> dict[str, int | None]:
    """Use the shared assessment fields; null means the provider does not expose a metric."""

    values = {
        "views": views,
        "likes": likes,
        "comments": comments,
        "shares": shares,
        "saves": saves,
        "watch_time_s": watch_time_s,
        "clicks": clicks,
    }
    return {key: max(0, int(value)) if value is not None else None for key, value in values.items()}


class Connector(ABC):
    key: str
    version = "1.0.0"
    tier = "native"
    rate_limit_requests = 60
    rate_limit_seconds = 60
    _rate_lock: ClassVar[threading.Lock] = threading.Lock()
    _rate_state: ClassVar[dict[str, list[float]]] = {}

    def __init__(self, http: httpx.Client | None = None):
        self.http = http or httpx.Client(timeout=httpx.Timeout(12.0, connect=4.0))

    def validate(self, rendition: dict, destination: dict, connection: dict) -> dict:
        spec = destination.get("spec", {})
        violations = []
        title = rendition.get("title") or ""
        body = rendition.get("body") or ""
        if spec.get("title_max_chars") and len(title) > spec["title_max_chars"]:
            violations.append({"field": "title", "code": "TITLE_TOO_LONG", "limit": spec["title_max_chars"], "actual": len(title)})
        if spec.get("body_max_chars") and len(body) > spec["body_max_chars"]:
            violations.append({"field": "body", "code": "BODY_TOO_LONG", "limit": spec["body_max_chars"], "actual": len(body)})
        if not spec.get("links_allowed", True) and (rendition.get("canonical_url") or "http://" in body or "https://" in body):
            violations.append({"field": "body", "code": "LINKS_NOT_ALLOWED"})
        allowed_types = set(destination.get("content_types", ["text", "article", "video_link"]))
        media_content_types = {"video": "video_link", "image": "image", "audio": "audio"}
        unsupported = [m.get("media_type") for m in rendition.get("media", []) if media_content_types.get(m.get("media_type"), m.get("media_type")) not in allowed_types]
        if unsupported:
            violations.append({"field": "media", "code": "MEDIA_TYPE_UNSUPPORTED", "values": unsupported})
        return {"ok": not violations, "violations": violations}

    @abstractmethod
    def publish(self, rendition: dict, destination: dict, connection: dict, idempotency_key: str) -> dict:
        raise NotImplementedError

    @abstractmethod
    def get_status(self, platform_post_id: str, connection: dict) -> dict:
        raise NotImplementedError

    @abstractmethod
    def delete(self, platform_post_id: str, connection: dict) -> dict:
        raise NotImplementedError

    def fetch_metrics(self, platform_post_id: str, connection: dict, since: str | None = None) -> dict:
        raise ServiceError(501, "CONNECTOR_DISABLED", "Metrics are not available for this connector.")

    def refresh_auth(self, connection: dict) -> dict:
        raise ServiceError(501, "CONNECTOR_DISABLED", "Token refresh is not available for this connector.")

    def build_assist_pack(self, rendition: dict, destination: dict) -> dict:
        raise ServiceError(501, "CONNECTOR_DISABLED", "Assisted posting is not available for this connector.")

    def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        settings = get_settings()
        if not settings.simulation_mode:
            approved_private_hosts = set(settings.outbound_allowed_host_set)
            if settings.is_local:
                approved_private_hosts.update(
                    host.strip().lower()
                    for host in settings.certification_allowed_hosts.split(",")
                    if host.strip()
                )
            validate_http_url(
                url,
                purpose="connector endpoint",
                allowed_private_hosts=approved_private_hosts,
                allowed_hosts=settings.connector_allowed_host_set,
            )
        self._throttle(url)
        headers = kwargs.get("headers") or {}
        header_names = {str(name).lower() for name in headers}
        retry_safe = method.upper() in {"GET", "HEAD", "PUT", "DELETE"} or "idempotency-key" in header_names
        try:
            response = self.http.request(method, url, **kwargs)
        except httpx.TimeoutException as exc:
            raise ServiceError(504, "PLATFORM_UNAVAILABLE", "The destination timed out.", retry_safe, {"reason": "timeout", "automatic_retry": retry_safe}) from exc
        except httpx.HTTPError as exc:
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "The destination could not be reached.", retry_safe, {"reason": type(exc).__name__, "automatic_retry": retry_safe}) from exc
        if response.status_code == 429:
            reset = response.headers.get("x-ratelimit-reset") or response.headers.get("retry-after")
            retry_after = None
            if reset:
                try:
                    number = float(reset)
                    if number > 1_000_000_000_000:
                        number /= 1000
                    retry_after = max(0.0, number - time.time()) if number > 1_000_000_000 else max(0.0, number)
                except ValueError:
                    try:
                        retry_after = max(0.0, (parsedate_to_datetime(reset) - datetime.now(UTC)).total_seconds())
                    except (TypeError, ValueError):
                        retry_after = None
            raise ServiceError(429, "PLATFORM_RATE_LIMIT", "The destination rate limit was reached; the job will back off and retry.", True, {"reset_at": reset, "retry_after_seconds": retry_after})
        if response.status_code == 401:
            raise ServiceError(401, "CONNECTION_AUTH_EXPIRED", "The destination rejected the connection credentials.", False)
        if response.status_code == 403:
            raise ServiceError(403, "PLATFORM_CONTENT_REJECTED", "The connection lacks permission for this action.", False, {"status": 403})
        if response.status_code in (400, 413, 422):
            raise ServiceError(response.status_code, "PLATFORM_CONTENT_REJECTED", "The destination rejected this content.", False, {"status": response.status_code})
        if response.status_code >= 500:
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "The destination returned a server error.", retry_safe, {"status": response.status_code, "automatic_retry": retry_safe})
        return response

    def _throttle(self, url: str) -> None:
        import urllib.parse

        host = urllib.parse.urlparse(url).netloc.lower()
        key = f"{self.key}:{host}"
        now = time.monotonic()
        with self._rate_lock:
            history = [value for value in self._rate_state.get(key, []) if value > now - self.rate_limit_seconds]
            if len(history) >= self.rate_limit_requests:
                delay = max(0.0, history[0] + self.rate_limit_seconds - now)
                self._rate_state[key] = history
            else:
                history.append(now)
                self._rate_state[key] = history
                delay = 0
        if delay:
            time.sleep(delay)

    @staticmethod
    def _json(response: httpx.Response) -> dict:
        try:
            return response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "The destination returned invalid JSON.", True) from exc

    @staticmethod
    def _base(connection: dict, destination: dict) -> str:
        base = connection.get("base_url") or destination.get("url")
        if not base:
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "Connector needs a base_url.")
        return str(base).rstrip("/")

    @staticmethod
    def _required(connection: dict, key: str) -> str:
        value = connection.get(key)
        if not value:
            raise ServiceError(401, "CONNECTION_AUTH_EXPIRED", f"Missing connector credential: {key}.")
        return str(value)


class SimulatedConnector(Connector):
    """Deterministic local-only connector for tests and safe preview mode."""
    tier = "native"

    def __init__(self, key: str, http=None):
        super().__init__(http)
        self.key = key

    def publish(self, rendition: dict, destination: dict, connection: dict, idempotency_key: str) -> dict:
        digest = hashlib.sha256(f"{self.key}:{idempotency_key}".encode()).hexdigest()[:12]
        post_id = f"sim-{digest}"
        url = str(destination.get("url") or "http://simulation.local").rstrip("/") + f"/posts/{post_id}"
        return {"platform_post_id": post_id, "live_url": url, "status": "published"}

    def get_status(self, platform_post_id: str, connection: dict) -> dict:
        return {"status": "published", "live_url": connection.get("simulated_live_url") or f"http://simulation.local/posts/{platform_post_id}"}

    def delete(self, platform_post_id: str, connection: dict) -> dict:
        return {"deleted": True}


class DiscourseConnector(Connector):
    key = "discourse"
    rate_limit_requests = 60
    rate_limit_seconds = 60

    def _headers(self, connection: dict) -> dict:
        return {"Api-Key": self._required(connection, "api_key"), "Api-Username": connection.get("api_username", "system")}

    def publish(self, rendition, destination, connection, idempotency_key):
        response = self._request("POST", self._base(connection, destination) + "/posts.json", headers=self._headers(connection), json={
            "title": rendition.get("title") or "Shared from IncuBrix",
            "raw": rendition.get("body", ""),
            "category": connection.get("category_id"),
            "external_id": idempotency_key,
        })
        payload = self._json(response)
        topic_id = payload.get("topic_id")
        post_id = payload.get("id")
        if not topic_id or not post_id:
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Discourse response did not include topic and post IDs.", False,
                               {"reason": "ambiguous_publish_outcome", "manual_review_required": True})
        return {"platform_post_id": str(topic_id), "live_url": self._base(connection, destination) + f"/t/{topic_id}", "status": "published"}

    def get_status(self, platform_post_id, connection):
        response = self._request("GET", self._base(connection, {}) + f"/t/{platform_post_id}.json", headers=self._headers(connection))
        payload = self._json(response)
        return {"status": "published" if payload.get("id") else "unknown", "live_url": self._base(connection, {}) + f"/t/{platform_post_id}"}

    def delete(self, platform_post_id, connection):
        response = self._request("DELETE", self._base(connection, {}) + f"/t/{platform_post_id}.json", headers=self._headers(connection))
        return {"deleted": response.is_success}

    def fetch_metrics(self, platform_post_id, connection, since=None):
        payload = self._json(self._request("GET", self._base(connection, {}) + f"/t/{platform_post_id}.json", headers=self._headers(connection)))
        return {"platform_post_id": str(platform_post_id), "metrics": normalized_metrics(
            views=payload.get("views"),
            likes=payload.get("like_count"),
            comments=max(0, int(payload.get("posts_count") or 0) - 1),
        )}


class LemmyConnector(Connector):
    key = "lemmy"
    rate_limit_requests = 30
    rate_limit_seconds = 60

    def _headers(self, connection):
        jwt = connection.get("jwt")
        return {"Authorization": f"Bearer {jwt}"} if jwt else {}

    def publish(self, rendition, destination, connection, idempotency_key):
        community_id = connection.get("community_id")
        if community_id is None:
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "Lemmy connector requires community_id.")
        body = {"community_id": int(community_id), "name": rendition.get("title") or "Shared from IncuBrix", "body": rendition.get("body", "")}
        if rendition.get("canonical_url"):
            body["url"] = rendition["canonical_url"]
        response = self._request("POST", self._base(connection, destination) + "/api/v4/post", headers=self._headers(connection), json=body)
        payload = self._json(response)
        post = payload.get("post_view", {}).get("post", payload.get("post", {}))
        post_id = post.get("id")
        if not post_id:
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Lemmy response did not include a post ID.", False,
                               {"reason": "ambiguous_publish_outcome", "manual_review_required": True})
        live_url = post.get("ap_id") or self._base(connection, destination) + f"/post/{post_id}"
        return {"platform_post_id": str(post_id), "live_url": live_url, "status": "published"}

    def get_status(self, platform_post_id, connection):
        response = self._request("GET", self._base(connection, {}) + "/api/v4/post", params={"id": int(platform_post_id)}, headers=self._headers(connection))
        payload = self._json(response)
        post = payload.get("post_view", {}).get("post", payload.get("post", {}))
        return {"status": "deleted" if post.get("deleted") else "published" if post else "unknown", "live_url": post.get("ap_id") or self._base(connection, {}) + f"/post/{platform_post_id}"}

    def delete(self, platform_post_id, connection):
        response = self._request("DELETE", self._base(connection, {}) + "/api/v4/post", headers=self._headers(connection), json={"post_id": int(platform_post_id), "deleted": True})
        return {"deleted": response.is_success}

    def fetch_metrics(self, platform_post_id, connection, since=None):
        payload = self._json(self._request("GET", self._base(connection, {}) + "/api/v4/post", params={"id": int(platform_post_id)}, headers=self._headers(connection)))
        counts = payload.get("post_view", {}).get("counts", payload.get("counts", {}))
        return {"platform_post_id": str(platform_post_id), "metrics": normalized_metrics(
            likes=counts.get("upvotes"),
            comments=counts.get("comments"),
        )}


class DevtoConnector(Connector):
    key = "devto"
    rate_limit_requests = 10
    rate_limit_seconds = 30

    def _headers(self, connection):
        return {"api-key": self._required(connection, "api_key"), "Content-Type": "application/json", "Accept": "application/vnd.forem.api-v1+json", "User-Agent": "IncuBrix-Assessment/0.1"}

    def publish(self, rendition, destination, connection, idempotency_key):
        article = {"title": rendition.get("title") or "Shared from IncuBrix", "body_markdown": rendition.get("body", ""), "published": True}
        if rendition.get("canonical_url"):
            article["canonical_url"] = rendition["canonical_url"]
        tags = rendition.get("tags")
        if tags:
            article["tags"] = tags[:4]
        response = self._request("POST", self._base(connection, destination) + "/api/articles", headers=self._headers(connection), json={"article": article})
        payload = self._json(response)
        if not payload.get("id"):
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "DEV response did not include an article ID.", False,
                               {"reason": "ambiguous_publish_outcome", "manual_review_required": True})
        return {"platform_post_id": str(payload["id"]), "live_url": payload.get("url"), "status": "published"}

    def get_status(self, platform_post_id, connection):
        response = self._request("GET", self._base(connection, {}) + f"/api/articles/{platform_post_id}", headers=self._headers(connection))
        payload = self._json(response)
        return {"status": "published" if payload.get("published", True) else "unpublished", "live_url": payload.get("url")}

    def delete(self, platform_post_id, connection):
        # Forem exposes unpublish rather than hard-delete; it may require admin/moderator role.
        response = self._request("PUT", self._base(connection, {}) + f"/api/articles/{platform_post_id}/unpublish", headers=self._headers(connection))
        return {"deleted": response.is_success}

    def fetch_metrics(self, platform_post_id, connection, since=None):
        payload = self._json(self._request("GET", self._base(connection, {}) + f"/api/articles/{platform_post_id}", headers=self._headers(connection)))
        return {"platform_post_id": str(platform_post_id), "metrics": normalized_metrics(
            views=payload.get("page_views_count"),
            likes=payload.get("positive_reactions_count"),
            comments=payload.get("comments_count"),
        )}


class GhostConnector(Connector):
    key = "ghost"
    rate_limit_requests = 60
    rate_limit_seconds = 60

    def _headers(self, connection):
        api_key = self._required(connection, "admin_api_key")
        try:
            key_id, secret = api_key.split(":", 1)
            token = __import__("jwt").encode({"iat": __import__("time").time() - 5, "exp": __import__("time").time() + 300, "aud": "/admin/"}, bytes.fromhex(secret), algorithm="HS256", headers={"kid": key_id})
        except (ValueError, TypeError) as exc:
            raise ServiceError(401, "CONNECTION_AUTH_EXPIRED", "Ghost Admin API key must be key_id:hex_secret.") from exc
        return {"Authorization": f"Ghost {token}", "Accept-Version": connection.get("api_version", "v5.0"), "Content-Type": "application/json"}

    def publish(self, rendition, destination, connection, idempotency_key):
        # Renditions are plain text/Markdown-like input. Escape it before using
        # Ghost's HTML source mode so a submission cannot become active script.
        post = {
            "title": rendition.get("title") or "Shared from IncuBrix",
            "html": escape(rendition.get("body", "")).replace("\n", "<br>"),
            "status": "published",
        }
        response = self._request("POST", self._base(connection, destination) + "/ghost/api/admin/posts/?source=html", headers=self._headers(connection), json={"posts": [post]})
        payload = self._json(response).get("posts", [{}])[0]
        if not payload.get("id"):
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Ghost response did not include a post ID.", False,
                               {"reason": "ambiguous_publish_outcome", "manual_review_required": True})
        return {"platform_post_id": str(payload["id"]), "live_url": payload.get("url"), "status": payload.get("status", "published")}

    def get_status(self, platform_post_id, connection):
        response = self._request("GET", self._base(connection, {}) + f"/ghost/api/admin/posts/{platform_post_id}/?formats=html", headers=self._headers(connection))
        payload = self._json(response).get("posts", [{}])[0]
        return {"status": payload.get("status", "unknown"), "live_url": payload.get("url")}

    def delete(self, platform_post_id, connection):
        response = self._request("DELETE", self._base(connection, {}) + f"/ghost/api/admin/posts/{platform_post_id}/", headers=self._headers(connection))
        return {"deleted": response.is_success}


class WebhookConnector(Connector):
    key = "signed-webhook"
    tier = "protocol"
    rate_limit_requests = 30
    rate_limit_seconds = 60

    def _headers(self, body: bytes, connection: dict, idempotency_key: str | None = None):
        secret = self._required(connection, "signing_secret")
        signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        headers = {"Content-Type": "application/json", "X-IncuBrix-Signature": f"sha256={signature}"}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        return headers

    def publish(self, rendition, destination, connection, idempotency_key):
        url = self._required(connection, "publish_url")
        body = json.dumps({"rendition": rendition, "destination": destination}, sort_keys=True, separators=(",", ":")).encode()
        response = self._request("POST", url, content=body, headers=self._headers(body, connection, idempotency_key))
        payload = self._json(response)
        if not payload.get("platform_post_id") or not payload.get("live_url"):
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Partner webhook response must include platform_post_id and live_url.", True)
        return {"platform_post_id": str(payload["platform_post_id"]), "live_url": payload["live_url"], "status": payload.get("status", "published")}

    def get_status(self, platform_post_id, connection):
        template = self._required(connection, "status_url_template")
        url = template.replace("{platform_post_id}", quote(str(platform_post_id), safe=""))
        body = b""
        payload = self._json(self._request("GET", url, headers=self._headers(body, connection)))
        return {"status": payload.get("status", "unknown"), "live_url": payload.get("live_url")}

    def delete(self, platform_post_id, connection):
        template = self._required(connection, "delete_url_template")
        url = template.replace("{platform_post_id}", quote(str(platform_post_id), safe=""))
        response = self._request("DELETE", url, headers=self._headers(b"", connection))
        return {"deleted": response.is_success}


class MastodonConnector(Connector):
    key = "mastodon"
    rate_limit_requests = 30
    rate_limit_seconds = 60

    def _headers(self, connection):
        return {"Authorization": f"Bearer {self._required(connection, 'access_token')}", "Content-Type": "application/json"}

    def publish(self, rendition, destination, connection, idempotency_key):
        status = rendition.get("body", "")
        if rendition.get("canonical_url") and rendition["canonical_url"] not in status:
            status = f"{status}\n\n{rendition['canonical_url']}"
        response = self._request("POST", self._base(connection, destination) + "/api/v1/statuses", headers=self._headers(connection), json={
            "status": status, "visibility": connection.get("visibility", "unlisted"),
        })
        payload = self._json(response)
        if not payload.get("id"):
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Mastodon response did not include a status ID.", False,
                               {"reason": "ambiguous_publish_outcome", "manual_review_required": True})
        return {"platform_post_id": str(payload["id"]), "live_url": payload.get("url"), "status": "published"}

    def get_status(self, platform_post_id, connection):
        payload = self._json(self._request("GET", self._base(connection, {}) + f"/api/v1/statuses/{quote(str(platform_post_id), safe='')}", headers=self._headers(connection)))
        return {"status": "published", "live_url": payload.get("url")}

    def delete(self, platform_post_id, connection):
        response = self._request("DELETE", self._base(connection, {}) + f"/api/v1/statuses/{quote(str(platform_post_id), safe='')}", headers=self._headers(connection))
        return {"deleted": response.is_success}


class DiscordWebhookConnector(Connector):
    key = "discord-webhook"
    tier = "bridge"
    rate_limit_requests = 20
    rate_limit_seconds = 60

    def _url(self, connection):
        return self._required(connection, "webhook_url").rstrip("/")

    def publish(self, rendition, destination, connection, idempotency_key):
        url = self._url(connection)
        response = self._request("POST", url + ("&" if "?" in url else "?") + "wait=true", json={"content": rendition.get("body", "")})
        payload = self._json(response)
        if not payload.get("id"):
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Discord response did not include a message ID.", False,
                               {"reason": "ambiguous_publish_outcome", "manual_review_required": True})
        return {"platform_post_id": str(payload["id"]), "live_url": payload.get("jump_url"), "status": "published"}

    def get_status(self, platform_post_id, connection):
        payload = self._json(self._request("GET", self._url(connection).split("?", 1)[0] + f"/messages/{quote(str(platform_post_id), safe='')}"))
        return {"status": "published", "live_url": payload.get("jump_url")}

    def delete(self, platform_post_id, connection):
        response = self._request("DELETE", self._url(connection).split("?", 1)[0] + f"/messages/{quote(str(platform_post_id), safe='')}")
        return {"deleted": response.is_success}


class MatrixConnector(Connector):
    key = "matrix"
    tier = "bridge"
    rate_limit_requests = 30
    rate_limit_seconds = 60

    def _headers(self, connection):
        return {"Authorization": f"Bearer {self._required(connection, 'access_token')}", "Content-Type": "application/json"}

    def _room(self, connection):
        return quote(self._required(connection, "room_id"), safe="")

    def publish(self, rendition, destination, connection, idempotency_key):
        url = self._base(connection, destination) + f"/_matrix/client/v3/rooms/{self._room(connection)}/send/m.room.message/{quote(idempotency_key, safe='')}"
        payload = self._json(self._request("PUT", url, headers=self._headers(connection), json={"msgtype": "m.text", "body": rendition.get("body", "")}))
        event_id = payload.get("event_id")
        if not event_id:
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Matrix response did not include an event ID.", False,
                               {"reason": "ambiguous_publish_outcome", "manual_review_required": True})
        room_id = self._required(connection, "room_id")
        return {"platform_post_id": str(event_id), "live_url": f"https://matrix.to/#/{room_id}/{event_id}", "status": "published"}

    def get_status(self, platform_post_id, connection):
        self._request("GET", self._base(connection, {}) + f"/_matrix/client/v3/rooms/{self._room(connection)}/event/{quote(str(platform_post_id), safe='')}", headers=self._headers(connection))
        room_id = self._required(connection, "room_id")
        return {"status": "published", "live_url": f"https://matrix.to/#/{room_id}/{platform_post_id}"}

    def delete(self, platform_post_id, connection):
        url = self._base(connection, {}) + f"/_matrix/client/v3/rooms/{self._room(connection)}/redact/{quote(str(platform_post_id), safe='')}/{quote(str(platform_post_id) + '-redact', safe='')}"
        response = self._request("PUT", url, headers=self._headers(connection), json={"reason": "IncuBrix certification cleanup"})
        return {"deleted": response.is_success}


class SlackConnector(Connector):
    """Bot-authenticated Slack messages; deletion is limited to bot-owned posts."""

    key = "slack"
    tier = "bridge"
    rate_limit_requests = 30
    rate_limit_seconds = 60

    def _base(self, connection, destination=None):
        return str(connection.get("base_url") or "https://slack.com/api").rstrip("/")

    def _headers(self, connection):
        return {"Authorization": f"Bearer {self._required(connection, 'bot_token')}", "Content-Type": "application/json"}

    def _slack_call(self, method, url, connection, *, json_body=None, params=None):
        response = self._request(method, url, headers=self._headers(connection), json=json_body, params=params)
        payload = self._json(response)
        if not payload.get("ok"):
            error = str(payload.get("error") or "unknown_error")
            if error in {"invalid_auth", "not_authed", "token_expired", "token_revoked"}:
                raise ServiceError(401, "CONNECTION_AUTH_EXPIRED", "Slack rejected the bot token.")
            if error in {"ratelimited", "rate_limited"}:
                retry_after = payload.get("retry_after") or response.headers.get("retry-after")
                try:
                    retry_after = max(0.0, float(retry_after)) if retry_after is not None else None
                except (TypeError, ValueError):
                    retry_after = None
                raise ServiceError(
                    429,
                    "PLATFORM_RATE_LIMIT",
                    "Slack rate limit was reached.",
                    True,
                    {"retry_after_seconds": retry_after},
                )
            if error in {"channel_not_found", "message_not_found", "not_in_channel", "missing_scope"}:
                raise ServiceError(403, "PLATFORM_CONTENT_REJECTED", "Slack rejected this channel or operation.", False, {"reason": error})
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Slack returned an unsuccessful API response.", False, {"reason": error})
        return payload

    def publish(self, rendition, destination, connection, idempotency_key):
        channel_id = self._required(connection, "channel_id")
        text = rendition.get("body", "")
        if rendition.get("canonical_url") and rendition["canonical_url"] not in text:
            text = f"{text}\n\n{rendition['canonical_url']}"
        payload = self._slack_call(
            "POST", self._base(connection) + "/chat.postMessage", connection,
            json_body={"channel": channel_id, "text": text},
        )
        if not payload.get("ts") or not payload.get("channel"):
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Slack did not return a message ID.", False,
                               {"reason": "ambiguous_publish_outcome", "manual_review_required": True})
        post_id = f"{payload['channel']}:{payload['ts']}"
        return {"platform_post_id": post_id, "live_url": None, "status": "published"}

    def get_status(self, platform_post_id, connection):
        channel_id, separator, timestamp = str(platform_post_id).partition(":")
        if not separator or not channel_id or not timestamp:
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "Slack post ID must contain a channel and message timestamp.")
        payload = self._slack_call(
            "GET", self._base(connection) + "/chat.getPermalink", connection,
            params={"channel": channel_id, "message_ts": timestamp},
        )
        return {"status": "published", "live_url": payload.get("permalink")}

    def delete(self, platform_post_id, connection):
        channel_id, separator, timestamp = str(platform_post_id).partition(":")
        if not separator or not channel_id or not timestamp:
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "Slack post ID must contain a channel and message timestamp.")
        self._slack_call(
            "POST", self._base(connection) + "/chat.delete", connection,
            json_body={"channel": channel_id, "ts": timestamp},
        )
        return {"deleted": True}


class HashnodeConnector(Connector):
    """Hashnode GraphQL publisher. Writes require a Pro publication."""

    key = "hashnode"
    rate_limit_requests = 20
    rate_limit_seconds = 60
    _PUBLISH = "mutation PublishPost($input: PublishPostInput!) { publishPost(input: $input) { post { id slug url } } }"
    _POST = "query PostById($id: ID!) { post(id: $id) { id slug url title } }"
    _REMOVE = "mutation RemovePost($input: RemovePostInput!) { removePost(input: $input) { post { id title slug } } }"

    def _base(self, connection, destination=None):
        return str(connection.get("base_url") or "https://gql-beta.hashnode.com").rstrip("/")

    def _graphql(self, query, variables, connection):
        headers = {
            "Authorization": f"Bearer {self._required(connection, 'personal_access_token')}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-hashnode-client": "incubrix",
        }
        payload = self._json(self._request("POST", self._base(connection) + "/", headers=headers,
                                           json={"query": query, "variables": variables}))
        errors = payload.get("errors") or []
        if errors:
            code = (errors[0].get("extensions") or {}).get("code")
            if code == "UNAUTHENTICATED":
                raise ServiceError(401, "CONNECTION_AUTH_EXPIRED", "Hashnode rejected the personal access token.")
            if code == "FORBIDDEN":
                raise ServiceError(403, "PLATFORM_CONTENT_REJECTED", "Hashnode denied this operation; the publication may need an active Pro plan.")
            raise ServiceError(422, "PLATFORM_CONTENT_REJECTED", "Hashnode rejected the GraphQL operation.", False,
                               {"reason": str(code or "graphql_error")})
        return payload.get("data") or {}

    def publish(self, rendition, destination, connection, idempotency_key):
        content = rendition.get("body", "")
        if rendition.get("ai_generated") and rendition.get("ai_disclosure"):
            content = f"{content}\n\n{rendition['ai_disclosure']}"
        post_input = {
            "publicationId": self._required(connection, "publication_id"),
            "title": rendition.get("title") or "Shared from IncuBrix",
            "contentMarkdown": content,
            "tags": [{"slug": tag.lower().replace(" ", "-")} for tag in rendition.get("tags", [])[:4]],
        }
        if rendition.get("canonical_url"):
            post_input["originalArticleURL"] = rendition["canonical_url"]
        post = self._graphql(self._PUBLISH, {"input": post_input}, connection).get("publishPost", {}).get("post", {})
        if not post.get("id"):
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Hashnode did not return a post ID.", False,
                               {"reason": "ambiguous_publish_outcome", "manual_review_required": True})
        return {"platform_post_id": str(post["id"]), "live_url": post.get("url"), "status": "published"}

    def get_status(self, platform_post_id, connection):
        post = self._graphql(self._POST, {"id": str(platform_post_id)}, connection).get("post")
        if not post:
            raise ServiceError(404, "DESTINATION_NOT_FOUND", "Hashnode post was not found.")
        return {"status": "published", "live_url": post.get("url")}

    def delete(self, platform_post_id, connection):
        removed = self._graphql(self._REMOVE, {"input": {"id": str(platform_post_id)}}, connection).get("removePost", {}).get("post")
        if not removed:
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "Hashnode did not confirm post removal.", False)
        return {"deleted": True}


class PeerTubeConnector(Connector):
    """PeerTube imports an explicitly allowed public video URL into a channel."""

    key = "peertube"
    rate_limit_requests = 10
    rate_limit_seconds = 60

    def _headers(self, connection):
        return {"Authorization": f"Bearer {self._required(connection, 'access_token')}", "Accept": "application/json"}

    def _import(self, import_id, connection):
        start = 0
        page_size = 100
        while True:
            payload = self._json(self._request(
                "GET", self._base(connection, {}) + "/api/v1/users/me/videos/imports",
                headers=self._headers(connection), params={"start": start, "count": page_size},
            ))
            items = payload.get("data", [])
            for item in items:
                if str(item.get("id")) == str(import_id):
                    return item
            try:
                total = int(payload.get("total", 0))
            except (TypeError, ValueError):
                total = 0
            if not items or len(items) < page_size or (total and start + len(items) >= total):
                break
            start += len(items)
        raise ServiceError(404, "DESTINATION_NOT_FOUND", "PeerTube import was not found for the authenticated user.")

    @staticmethod
    def _state_id(payload):
        state = payload.get("state")
        return state.get("id") if isinstance(state, dict) else state

    def publish(self, rendition, destination, connection, idempotency_key):
        settings = get_settings()
        source = next((item.get("url") for item in rendition.get("media", []) if item.get("media_type") == "video"), None)
        if not source:
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "PeerTube publishing needs a video URL in media.")
        validate_http_url(source, purpose="PeerTube video source", allowed_hosts=settings.media_import_allowed_host_set)
        response = self._request(
            "POST", self._base(connection, destination) + "/api/v1/videos/imports",
            headers={**self._headers(connection), "Content-Type": "application/json"},
            json={
                "targetUrl": source,
                "name": rendition.get("title") or "Shared from IncuBrix",
                "description": rendition.get("body", ""),
                "channelId": int(self._required(connection, "channel_id")),
                "privacy": 2,
            },
        )
        result = self._json(response)
        import_id = result.get("id")
        if import_id is None:
            raise ServiceError(502, "PLATFORM_UNAVAILABLE", "PeerTube did not return an import ID.", False,
                               {"reason": "ambiguous_publish_outcome", "manual_review_required": True})
        deadline = time.monotonic() + 300
        while True:
            current = self._import(import_id, connection)
            state_id = self._state_id(current)
            if state_id == 2:
                video = current.get("video") or {}
                if not video.get("id"):
                    raise ServiceError(502, "PLATFORM_UNAVAILABLE", "PeerTube completed the import without a video ID.", False,
                                       {"reason": "ambiguous_publish_outcome", "manual_review_required": True})
                return {
                    "platform_post_id": json.dumps({"import_id": import_id, "video_id": video["id"]}, separators=(",", ":")),
                    "live_url": video.get("url"),
                    "status": "published",
                }
            if state_id in {3, 4, 5}:
                raise ServiceError(422, "PLATFORM_CONTENT_REJECTED", "PeerTube did not accept the video import.", False,
                                   {"import_id": str(import_id), "state": state_id})
            if time.monotonic() >= deadline:
                raise ServiceError(504, "PLATFORM_UNAVAILABLE", "PeerTube import is still processing; review it before retrying.", False,
                                   {"reason": "ambiguous_publish_outcome", "import_id": str(import_id), "manual_review_required": True})
            time.sleep(2)

    def get_status(self, platform_post_id, connection):
        try:
            identity = json.loads(str(platform_post_id))
            import_id = identity["import_id"]
        except (ValueError, KeyError, TypeError) as exc:
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "PeerTube post ID is invalid.") from exc
        current = self._import(import_id, connection)
        state_id = self._state_id(current)
        video = current.get("video") or {}
        if state_id == 2:
            return {"status": "published", "live_url": video.get("url")}
        if state_id in {3, 4, 5}:
            return {"status": "failed", "live_url": None}
        return {"status": "processing", "live_url": None}

    def delete(self, platform_post_id, connection):
        try:
            identity = json.loads(str(platform_post_id))
            import_id = identity["import_id"]
            video_id = identity.get("video_id")
        except (ValueError, KeyError, TypeError) as exc:
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "PeerTube post ID is invalid.") from exc
        if video_id is None:
            response = self._request("DELETE", self._base(connection, {}) + f"/api/v1/videos/imports/{quote(str(import_id), safe='')}", headers=self._headers(connection))
        else:
            response = self._request("DELETE", self._base(connection, {}) + f"/api/v1/videos/{quote(str(video_id), safe='')}", headers=self._headers(connection))
        return {"deleted": response.is_success}


class AssistedConnector(Connector):
    key = "assisted"
    tier = "assisted"

    def publish(self, rendition, destination, connection, idempotency_key):
        raise ServiceError(409, "CONNECTOR_DISABLED", "This destination requires a person to publish the prepared pack.")

    def get_status(self, platform_post_id, connection):
        raise ServiceError(409, "CONNECTOR_DISABLED", "A human-posted pack has no automated status endpoint.")

    def delete(self, platform_post_id, connection):
        raise ServiceError(409, "CONNECTOR_DISABLED", "A human-posted pack cannot be deleted by this connector.")

    def build_assist_pack(self, rendition, destination):
        return {
            "files": [item.get("url") for item in rendition.get("media", [])],
            "caption": rendition.get("body", ""),
            "deep_link": destination.get("url"),
            "checklist": [
                "Review and follow the destination rules before posting.",
                "Confirm that links and self-promotion are permitted.",
                "Add the required AI disclosure if the content is AI-generated.",
                "Publish manually; this connector does not post on your behalf.",
            ],
        }


CONNECTOR_CLASSES = {
    "discourse": DiscourseConnector,
    "lemmy": LemmyConnector,
    "devto": DevtoConnector,
    "ghost": GhostConnector,
    "signed-webhook": WebhookConnector,
    "mastodon": MastodonConnector,
    "discord-webhook": DiscordWebhookConnector,
    "matrix": MatrixConnector,
    "slack": SlackConnector,
    "hashnode": HashnodeConnector,
    "peertube": PeerTubeConnector,
    "assisted": AssistedConnector,
}


def get_connector(key: str, connection: dict | None = None) -> Connector:
    if key not in CONNECTOR_CLASSES:
        raise ServiceError(404, "CONNECTOR_DISABLED", f"Unknown connector: {key}.")
    settings = get_settings()
    connection = connection or {}
    if key == "assisted":
        return AssistedConnector()
    if settings.simulation_mode:
        return SimulatedConnector(key)
    return CONNECTOR_CLASSES[key]()


def validate_connector_registry() -> None:
    manifests = connector_manifests()
    implementation_keys = set(CONNECTOR_CLASSES)
    manifest_keys = set(manifests)
    if implementation_keys != manifest_keys:
        missing_manifests = sorted(implementation_keys - manifest_keys)
        missing_implementations = sorted(manifest_keys - implementation_keys)
        raise RuntimeError(
            "Connector manifest registry mismatch: "
            f"missing manifests={missing_manifests}, missing implementations={missing_implementations}"
        )


def public_connector_list() -> list[dict[str, Any]]:
    validate_connector_registry()
    return [
        {
            "key": manifest.key,
            "version": manifest.version,
            "tier": manifest.tier,
            "auth": manifest.auth,
            "capabilities": ["validate", *manifest.capabilities],
        }
        for manifest in connector_manifests().values()
    ]
