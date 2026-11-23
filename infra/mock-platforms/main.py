from __future__ import annotations

import hashlib
import hmac
import os
from itertools import count

from fastapi import FastAPI, Header, HTTPException, Request, Response

app = FastAPI(title="Local Platform API Mocks")
ids = count(1000)
discourse_topics: dict[str, dict] = {}
devto_articles: dict[str, dict] = {}
ghost_posts: dict[str, dict] = {}
partner_posts: dict[str, dict] = {}
external_ids: dict[str, str] = {}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/discourse/posts.json")
async def discourse_create(request: Request, api_key: str | None = Header(default=None, alias="Api-Key")):
    if not api_key:
        raise HTTPException(401, "Missing API key")
    body = await request.json()
    external = body.get("external_id")
    if external and external in external_ids:
        topic = external_ids[external]
        return {"topic_id": int(topic), "id": int(topic) * 10, "post_number": 1}
    topic = str(next(ids))
    discourse_topics[topic] = {"id": int(topic), "title": body.get("title"), "raw": body.get("raw"), "deleted": False}
    if external:
        external_ids[external] = topic
    return {"topic_id": int(topic), "id": int(topic) * 10, "post_number": 1}


@app.get("/discourse/t/{topic_id}.json")
def discourse_read(topic_id: str):
    if topic_id not in discourse_topics or discourse_topics[topic_id]["deleted"]:
        raise HTTPException(404, "Topic not found")
    return {"id": int(topic_id), "title": discourse_topics[topic_id]["title"], "post_stream": {"posts": []}}


@app.delete("/discourse/t/{topic_id}.json")
def discourse_delete(topic_id: str):
    if topic_id not in discourse_topics:
        raise HTTPException(404, "Topic not found")
    discourse_topics[topic_id]["deleted"] = True
    return {"success": True}


@app.post("/devto/api/articles")
async def devto_create(request: Request, api_key: str | None = Header(default=None), user_agent: str | None = Header(default=None)):
    if not api_key:
        raise HTTPException(401, "Missing API key")
    body = await request.json()
    article = body.get("article", {})
    article_id = str(next(ids))
    devto_articles[article_id] = {"id": int(article_id), "title": article.get("title"), "url": f"http://localhost:9000/devto/{article_id}", "published": True}
    return devto_articles[article_id]


@app.get("/devto/api/articles/{article_id}")
def devto_read(article_id: str):
    if article_id not in devto_articles:
        raise HTTPException(404, "Article not found")
    return devto_articles[article_id]


@app.put("/devto/api/articles/{article_id}/unpublish", status_code=204)
def devto_unpublish(article_id: str, api_key: str | None = Header(default=None)):
    if not api_key:
        raise HTTPException(401, "Missing API key")
    if article_id not in devto_articles:
        raise HTTPException(404, "Article not found")
    devto_articles[article_id]["published"] = False
    return Response(status_code=204)


@app.post("/ghost/ghost/api/admin/posts/")
async def ghost_create(request: Request, authorization: str | None = Header(default=None)):
    if not authorization or not authorization.startswith("Ghost "):
        raise HTTPException(401, "Missing Ghost authorization")
    body = await request.json()
    post = body.get("posts", [{}])[0]
    post_id = str(next(ids))
    ghost_posts[post_id] = {"id": post_id, "title": post.get("title"), "status": "published", "url": f"http://localhost:2368/p/{post_id}/", "html": post.get("html")}
    return {"posts": [ghost_posts[post_id]]}


@app.get("/ghost/ghost/api/admin/posts/{post_id}/")
def ghost_read(post_id: str, authorization: str | None = Header(default=None)):
    if not authorization or post_id not in ghost_posts:
        raise HTTPException(404, "Post not found")
    return {"posts": [ghost_posts[post_id]]}


@app.delete("/ghost/ghost/api/admin/posts/{post_id}/")
def ghost_delete(post_id: str, authorization: str | None = Header(default=None)):
    if not authorization or post_id not in ghost_posts:
        raise HTTPException(404, "Post not found")
    del ghost_posts[post_id]
    return Response(status_code=204)


def _validate_signature(body: bytes, signature: str | None):
    secret = os.getenv("PARTNER_WEBHOOK_SECRET", "local-partner-secret")
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    if not signature or not hmac.compare_digest(signature, expected):
        raise HTTPException(401, "Invalid signature")


@app.post("/partner/publish")
async def partner_publish(request: Request, signature: str | None = Header(default=None, alias="X-IncuBrix-Signature"), idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
    body = await request.body()
    _validate_signature(body, signature)
    if idempotency_key and idempotency_key in partner_posts:
        return partner_posts[idempotency_key]
    post_id = str(next(ids))
    result = {"platform_post_id": post_id, "live_url": f"http://localhost:9000/partner/posts/{post_id}", "status": "published"}
    partner_posts[idempotency_key or post_id] = result
    return result


@app.get("/partner/status/{post_id}")
def partner_status(post_id: str, signature: str | None = Header(default=None, alias="X-IncuBrix-Signature")):
    _validate_signature(b"", signature)
    result = next((value for value in partner_posts.values() if value["platform_post_id"] == post_id), None)
    if not result:
        raise HTTPException(404, "Post not found")
    return {"status": "published", "live_url": result["live_url"]}


@app.delete("/partner/posts/{post_id}")
def partner_delete(post_id: str, signature: str | None = Header(default=None, alias="X-IncuBrix-Signature")):
    _validate_signature(b"", signature)
    for key, value in list(partner_posts.items()):
        if value["platform_post_id"] == post_id:
            del partner_posts[key]
            return {"deleted": True}
    raise HTTPException(404, "Post not found")
