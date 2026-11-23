import json

import httpx
import pytest

import incubrix.connectors.base as connector_base
from incubrix.connectors.base import (
    DevtoConnector,
    DiscordWebhookConnector,
    DiscourseConnector,
    HashnodeConnector,
    LemmyConnector,
    MastodonConnector,
    MatrixConnector,
    PeerTubeConnector,
    SlackConnector,
)
from incubrix.errors import ServiceError


def test_lemmy_connector_uses_v4_publish_status_and_delete_contract():
    seen = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, request.content))
        if request.method == "POST" and request.url.path == "/api/v4/post":
            return httpx.Response(200, json={"post_view": {"post": {"id": 42, "ap_id": "http://lemmy/post/42"}}})
        if request.method == "GET" and request.url.path == "/api/v4/post":
            return httpx.Response(200, json={"post_view": {"post": {"id": 42, "ap_id": "http://lemmy/post/42"}}})
        if request.method == "DELETE" and request.url.path == "/api/v4/post":
            return httpx.Response(200, json={"post_view": {"post": {"id": 42, "deleted": True}}})
        return httpx.Response(404)

    http = httpx.Client(transport=httpx.MockTransport(handle))
    connector = LemmyConnector(http=http)
    destination = {"url": "http://lemmy"}
    connection = {"base_url": "http://lemmy", "jwt": "test-jwt", "community_id": 7}

    published = connector.publish({"title": "Title", "body": "Body"}, destination, connection, "key-1")
    status = connector.get_status(published["platform_post_id"], connection)
    deleted = connector.delete(published["platform_post_id"], connection)

    assert published["platform_post_id"] == "42"
    assert status["status"] == "published"
    assert deleted["deleted"] is True
    method, path, body = seen[-1]
    assert (method, path) == ("DELETE", "/api/v4/post")
    assert json.loads(body) == {"post_id": 42, "deleted": True}


def test_core_connector_metrics_have_stable_normalized_fields():
    def discourse_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/t/42.json"
        return httpx.Response(200, json={"views": 12, "like_count": 3, "posts_count": 5})

    def lemmy_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v4/post"
        return httpx.Response(200, json={"post_view": {"counts": {"score": 8, "upvotes": 10, "comments": 2}}})

    def devto_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/articles/42"
        return httpx.Response(200, json={"page_views_count": 15, "positive_reactions_count": 4, "comments_count": 1})

    discourse = DiscourseConnector(http=httpx.Client(transport=httpx.MockTransport(discourse_handler)))
    lemmy = LemmyConnector(http=httpx.Client(transport=httpx.MockTransport(lemmy_handler)))
    devto = DevtoConnector(http=httpx.Client(transport=httpx.MockTransport(devto_handler)))

    expected_fields = {"views", "likes", "comments", "shares", "saves", "watch_time_s", "clicks"}
    discourse_metrics = discourse.fetch_metrics("42", {"base_url": "http://discourse", "api_key": "key"})["metrics"]
    lemmy_metrics = lemmy.fetch_metrics("42", {"base_url": "http://lemmy", "jwt": "jwt"})["metrics"]
    devto_metrics = devto.fetch_metrics("42", {"base_url": "http://devto", "api_key": "key"})["metrics"]

    unsupported = {"shares": None, "saves": None, "watch_time_s": None, "clicks": None}
    assert discourse_metrics == {"views": 12, "likes": 3, "comments": 4, **unsupported}
    assert lemmy_metrics == {"views": None, "likes": 10, "comments": 2, **unsupported}
    assert devto_metrics == {"views": 15, "likes": 4, "comments": 1, **unsupported}
    assert set(discourse_metrics) == expected_fields
    assert set(lemmy_metrics) == expected_fields
    assert set(devto_metrics) == expected_fields


def test_extra_connectors_have_publish_status_delete_lifecycles():
    def mastodon_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"id": "m1", "url": "https://mastodon.example/@account/m1"})
        if request.method == "GET":
            return httpx.Response(200, json={"id": "m1", "url": "https://mastodon.example/@account/m1"})
        return httpx.Response(204)

    def discord_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            assert request.url.params["wait"] == "true"
            return httpx.Response(200, json={"id": "d1", "jump_url": "https://discord.com/channels/a/b/d1"})
        if request.method == "GET":
            return httpx.Response(200, json={"id": "d1", "jump_url": "https://discord.com/channels/a/b/d1"})
        return httpx.Response(204)

    def matrix_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT" and "/send/" in request.url.path:
            return httpx.Response(200, json={"event_id": "$event"})
        if request.method == "GET":
            return httpx.Response(200, json={"event_id": "$event"})
        return httpx.Response(200, json={"event_id": "$redaction"})

    cases = [
        (MastodonConnector(http=httpx.Client(transport=httpx.MockTransport(mastodon_handler))), {"base_url": "https://mastodon.example", "access_token": "token"}),
        (DiscordWebhookConnector(http=httpx.Client(transport=httpx.MockTransport(discord_handler))), {"webhook_url": "https://discord.example/api/webhooks/1/token"}),
        (MatrixConnector(http=httpx.Client(transport=httpx.MockTransport(matrix_handler))), {"base_url": "https://matrix.example", "access_token": "token", "room_id": "!room:matrix.example"}),
    ]
    for connector, connection in cases:
        published = connector.publish({"body": "test"}, {"url": "https://destination.example"}, connection, "stable-key")
        status = connector.get_status(published["platform_post_id"], connection)
        deleted = connector.delete(published["platform_post_id"], connection)
        assert published["platform_post_id"]
        assert published["live_url"]
        assert status["status"] == "published"
        assert deleted["deleted"] is True


def test_allowed_list_connectors_have_publish_status_delete_lifecycles(monkeypatch):
    def slack_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        if request.url.path == "/chat.postMessage":
            assert body["channel"] == "C12345"
            return httpx.Response(200, json={"ok": True, "channel": "C12345", "ts": "1712345678.123456"})
        if request.url.path == "/chat.getPermalink":
            assert request.url.params["message_ts"] == "1712345678.123456"
            return httpx.Response(200, json={"ok": True, "permalink": "https://workspace.slack.com/archives/C12345/p1712345678123456"})
        if request.url.path == "/chat.delete":
            assert body == {"channel": "C12345", "ts": "1712345678.123456"}
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(404)

    def hashnode_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if "publishPost" in body["query"]:
            assert body["variables"]["input"]["publicationId"] == "a" * 24
            return httpx.Response(200, json={"data": {"publishPost": {"post": {"id": "post-1", "url": "https://blog.example/post-1"}}}})
        if "removePost" in body["query"]:
            return httpx.Response(200, json={"data": {"removePost": {"post": {"id": "post-1"}}}})
        return httpx.Response(200, json={"data": {"post": {"id": "post-1", "url": "https://blog.example/post-1"}}})

    def peertube_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/api/v1/videos/imports":
            body = json.loads(request.content)
            assert body["targetUrl"] == "https://media.example/video.mp4"
            assert body["privacy"] == 2
            return httpx.Response(200, json={"id": 8})
        if request.method == "GET" and request.url.path == "/api/v1/users/me/videos/imports":
            return httpx.Response(200, json={"total": 1, "data": [{"id": 8, "state": {"id": 2}, "video": {"id": 42, "url": "https://tube.example/videos/watch/video-uuid"}}]})
        if request.method == "DELETE" and request.url.path == "/api/v1/videos/42":
            return httpx.Response(204)
        return httpx.Response(404)

    def validate_import_host(url, *, allowed_hosts=None, **kwargs):
        from urllib.parse import urlsplit

        host = urlsplit(url).hostname
        if host not in (allowed_hosts or set()):
            raise ServiceError(422, "DEST_SPEC_VIOLATION", "video source host is not approved")
        return url

    monkeypatch.setattr(connector_base, "validate_http_url", validate_import_host)
    slack = SlackConnector(http=httpx.Client(transport=httpx.MockTransport(slack_handler)))
    hashnode = HashnodeConnector(http=httpx.Client(transport=httpx.MockTransport(hashnode_handler)))
    peertube = PeerTubeConnector(http=httpx.Client(transport=httpx.MockTransport(peertube_handler)))

    slack_connection = {"base_url": "http://slack", "bot_token": "bot-token", "channel_id": "C12345"}
    slack_post = slack.publish({"body": "A Slack post"}, {}, slack_connection, "slack-key")
    assert slack.get_status(slack_post["platform_post_id"], slack_connection)["live_url"].startswith("https://workspace.slack.com/")
    assert slack.delete(slack_post["platform_post_id"], slack_connection)["deleted"] is True

    hashnode_connection = {"base_url": "http://hashnode", "personal_access_token": "pat", "publication_id": "a" * 24}
    hashnode_post = hashnode.publish({"title": "Article", "body": "Markdown"}, {}, hashnode_connection, "hashnode-key")
    assert hashnode.get_status(hashnode_post["platform_post_id"], hashnode_connection)["status"] == "published"
    assert hashnode.delete(hashnode_post["platform_post_id"], hashnode_connection)["deleted"] is True

    peertube_connection = {"base_url": "http://peertube", "access_token": "token", "channel_id": "7"}
    test_settings = connector_base.get_settings().model_copy(
        update={"media_import_allowed_hosts": "media.example"}
    )
    monkeypatch.setattr(connector_base, "get_settings", lambda: test_settings)
    peertube_post = peertube.publish(
        {"title": "Video", "body": "Description", "media": [{"media_type": "video", "url": "https://media.example/video.mp4"}]},
        {}, peertube_connection, "peertube-key",
    )
    assert peertube.get_status(peertube_post["platform_post_id"], peertube_connection)["live_url"] == "https://tube.example/videos/watch/video-uuid"
    assert peertube.delete(peertube_post["platform_post_id"], peertube_connection)["deleted"] is True

    with pytest.raises(ServiceError) as exc_info:
        peertube.publish(
            {"title": "Video", "body": "Description", "media": [{"media_type": "video", "url": "https://unapproved.example/video.mp4"}]},
            {}, peertube_connection, "blocked-peertube-key",
        )
    assert exc_info.value.code == "DEST_SPEC_VIOLATION"


def test_peertube_import_lookup_paginates_past_first_hundred():
    offsets = []

    def handler(request: httpx.Request) -> httpx.Response:
        start = int(request.url.params["start"])
        offsets.append(start)
        if start == 0:
            return httpx.Response(200, json={"total": 101, "data": [{"id": item} for item in range(100)]})
        return httpx.Response(200, json={"total": 101, "data": [{"id": 100, "state": {"id": 2}}]})

    connector = PeerTubeConnector(http=httpx.Client(transport=httpx.MockTransport(handler)))
    result = connector._import(
        100,
        {"base_url": "https://video.example", "access_token": "test-token"},
    )

    assert result["id"] == 100
    assert offsets == [0, 100]
