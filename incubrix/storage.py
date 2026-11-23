from __future__ import annotations

import json
from datetime import timedelta
from io import BytesIO
from urllib.parse import urlsplit

from minio import Minio
from minio.error import S3Error

from incubrix.config import get_settings


class StorageUnavailable(RuntimeError):
    """A local S3-compatible endpoint could not complete a storage operation."""


def _minio_client(endpoint_url: str, access_key: str, secret_key: str) -> Minio:
    parsed = urlsplit(endpoint_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.path not in {"", "/"}:
        raise ValueError("S3 endpoint must be an HTTP(S) origin without a path.")
    # Moto and MinIO use the standard local region; specifying it prevents
    # presigning from probing the browser-facing endpoint from inside the API
    # container (where `localhost` correctly refers to the API container).
    return Minio(parsed.netloc, access_key=access_key, secret_key=secret_key, secure=parsed.scheme == "https", region="us-east-1")


class ArtifactStorage:
    """Local-only S3-compatible artifact storage backed by Moto or MinIO."""

    def __init__(self):
        self.settings = get_settings()
        self.client = _minio_client(
            self.settings.s3_endpoint_url, self.settings.s3_access_key, self.settings.s3_secret_key
        )
        self.signing_client = _minio_client(
            self.settings.s3_public_endpoint_url, self.settings.s3_access_key, self.settings.s3_secret_key
        )
        self.bucket = self.settings.s3_bucket

    def ensure_bucket(self) -> None:
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
        except S3Error as exc:
            raise StorageUnavailable("Local artifact storage is unavailable.") from exc

    def put_json(self, key: str, value: dict) -> str:
        body = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        self.client.put_object(self.bucket, key, BytesIO(body), len(body), content_type="application/json")
        return self.uri(key)

    def get_json(self, key: str) -> dict:
        try:
            response = self.client.get_object(self.bucket, key)
            try:
                return json.loads(response.read())
            finally:
                response.close()
                response.release_conn()
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket", "NotFound"}:
                raise KeyError(key) from exc
            raise StorageUnavailable("Local artifact storage is unavailable.") from exc

    def put_bytes(self, key: str, body: bytes, content_type: str, *, filename: str) -> str:
        safe_filename = filename.replace('"', "_").replace("\\", "_")
        self.client.put_object(
            self.bucket, key, BytesIO(body), len(body), content_type=content_type,
            metadata={"Content-Disposition": f'attachment; filename="{safe_filename}"'},
        )
        return self.uri(key)

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def download_url(self, key: str) -> str:
        return self.signing_client.presigned_get_object(
            self.bucket, key, expires=timedelta(seconds=self.settings.presigned_url_ttl_seconds)
        )


def get_storage() -> ArtifactStorage:
    return ArtifactStorage()
