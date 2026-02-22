from __future__ import annotations

import json
import logging
import os
from urllib.parse import parse_qsl, urlencode

from fastapi import Request

try:
    import redis
except Exception:  # pragma: no cover - optional dependency guard
    redis = None


logger = logging.getLogger(__name__)


class OpdsCache:
    def __init__(self) -> None:
        self._url = os.getenv("REDIS_URL", "").strip()
        self._prefix = os.getenv("OPDS_CACHE_PREFIX", "opds-cache")
        self._ttl_seconds = int(os.getenv("OPDS_CACHE_TTL_SECONDS", "900"))
        self._client = None

        if not self._url or redis is None:
            return

        try:
            self._client = redis.from_url(self._url, decode_responses=True)
        except Exception:
            logger.exception("Failed to initialize Redis client; OPDS cache disabled.")
            self._client = None

    def is_enabled(self) -> bool:
        return self._client is not None

    def close(self) -> None:
        if self._client is None:
            return
        try:
            self._client.close()
        except Exception:
            logger.exception("Failed to close Redis client cleanly.")

    def key_for_request(self, request: Request, namespace: str | None = None) -> str:
        path = request.url.path
        query_pairs = parse_qsl(request.url.query, keep_blank_values=True)
        normalized_query = urlencode(sorted(query_pairs))
        suffix = f"{path}?{normalized_query}" if normalized_query else path
        if namespace:
            return f"{self._prefix}:feed:{namespace}:{suffix}"
        return f"{self._prefix}:feed:{suffix}"

    def get_json(self, key: str) -> dict | None:
        if self._client is None:
            return None
        try:
            raw = self._client.get(key)
            return json.loads(raw) if raw else None
        except Exception:
            logger.exception("Redis get failed for key %s", key)
            return None

    def set_json(self, key: str, payload: dict) -> None:
        if self._client is None:
            return
        try:
            self._client.setex(key, self._ttl_seconds, json.dumps(payload))
        except Exception:
            logger.exception("Redis set failed for key %s", key)

    def invalidate_feed_keys(self, namespace: str | None = None) -> int:
        if self._client is None:
            return 0
        if namespace:
            pattern = f"{self._prefix}:feed:{namespace}:*"
        else:
            pattern = f"{self._prefix}:feed:*"
        removed = 0
        try:
            for key in self._client.scan_iter(match=pattern, count=500):
                removed += int(self._client.delete(key))
        except Exception:
            logger.exception("Redis invalidation failed for pattern %s", pattern)
            return removed
        return removed
