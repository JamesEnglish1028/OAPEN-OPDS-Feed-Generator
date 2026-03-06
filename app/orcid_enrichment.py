from __future__ import annotations

import logging
import os
import re
import threading
import time
from typing import Any

import requests

from app.models import NormalizedPublication

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.strip().split())


def _normalize_name_key(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().casefold())


def _orcid_uri(value: str) -> str:
    candidate = value.strip()
    if candidate.startswith("http://") or candidate.startswith("https://"):
        return candidate.replace("http://", "https://", 1)
    return f"https://orcid.org/{candidate}"


def _split_author_name(name: str) -> tuple[str | None, str | None]:
    candidate = _normalize_whitespace(name)
    if not candidate:
        return None, None
    if "," in candidate:
        family, given = [part.strip() for part in candidate.split(",", 1)]
        return (given or None, family or None)
    parts = candidate.split(" ")
    if len(parts) < 2:
        return (candidate, None)
    given = " ".join(parts[:-1]).strip()
    family = parts[-1].strip()
    return (given or None, family or None)


class OrcidAuthorEnricher:
    def __init__(self) -> None:
        self._enabled = _env_bool("ORCID_ENRICHMENT_ENABLED", default=False)
        self._client_id = os.getenv("ORCID_CLIENT_ID", "").strip()
        self._client_secret = os.getenv("ORCID_CLIENT_SECRET", "").strip()
        self._sandbox = _env_bool("ORCID_SANDBOX", default=False)
        self._timeout = float(os.getenv("ORCID_HTTP_TIMEOUT_SECONDS", "10").strip())
        self._max_candidates = max(1, int(os.getenv("ORCID_MAX_CANDIDATES", "5").strip()))
        self._match_threshold = max(0.0, min(1.0, float(os.getenv("ORCID_MATCH_THRESHOLD", "0.95").strip())))
        self._session = requests.Session()
        self._lock = threading.Lock()
        self._token_value: str | None = None
        self._token_expiry_epoch: float = 0.0
        self._cache: dict[str, str | None] = {}
        self._library_client: Any | None = None
        self._library_token: str | None = None
        self._init_python_orcid_client()

    @property
    def enabled(self) -> bool:
        return self._enabled and bool(self._client_id and self._client_secret)

    def _oauth_base(self) -> str:
        return "https://sandbox.orcid.org" if self._sandbox else "https://orcid.org"

    def _public_api_base(self) -> str:
        return "https://pub.sandbox.orcid.org/v3.0" if self._sandbox else "https://pub.orcid.org/v3.0"

    def _init_python_orcid_client(self) -> None:
        try:
            import orcid  # type: ignore

            self._library_client = orcid.PublicAPI(self._client_id, self._client_secret, sandbox=self._sandbox)
        except Exception:
            self._library_client = None

    def _search_token(self) -> str | None:
        now = time.time()
        with self._lock:
            if self._token_value and now < self._token_expiry_epoch:
                return self._token_value
        try:
            response = self._session.post(
                f"{self._oauth_base()}/oauth/token",
                data={
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "grant_type": "client_credentials",
                    "scope": "/read-public",
                },
                headers={"Accept": "application/json"},
                timeout=self._timeout,
            )
            response.raise_for_status()
            payload = response.json()
            token = payload.get("access_token")
            expires_in = payload.get("expires_in")
            if not isinstance(token, str) or not token.strip():
                return None
            if not isinstance(expires_in, int):
                expires_in = 300
            with self._lock:
                self._token_value = token.strip()
                self._token_expiry_epoch = now + max(expires_in - 30, 60)
                return self._token_value
        except Exception:
            logger.exception("Unable to fetch ORCID access token.")
            return None

    def _library_search(self, query: str) -> list[dict[str, Any]]:
        client = self._library_client
        if client is None:
            return []
        try:
            if not self._library_token:
                token_value = client.get_search_token_from_orcid()
                if isinstance(token_value, str) and token_value.strip():
                    self._library_token = token_value.strip()
            if not self._library_token:
                return []
            payload = client.search_public(query, self._library_token)
        except Exception:
            return []
        if isinstance(payload, dict):
            expanded = payload.get("expanded-result")
            if isinstance(expanded, list):
                return [item for item in expanded if isinstance(item, dict)]
        return []

    def _http_search(self, query: str, token: str) -> list[dict[str, Any]]:
        try:
            response = self._session.get(
                f"{self._public_api_base()}/expanded-search",
                params={"q": query},
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {token}",
                },
                timeout=self._timeout,
            )
            response.raise_for_status()
            payload = response.json()
            expanded = payload.get("expanded-result")
            if isinstance(expanded, list):
                return [item for item in expanded if isinstance(item, dict)]
        except Exception:
            logger.exception("ORCID expanded-search request failed.")
        return []

    @staticmethod
    def _clean_orcid_id(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.startswith("http://") or candidate.startswith("https://"):
            candidate = candidate.rstrip("/").split("/")[-1]
        if re.fullmatch(r"\d{4}-\d{4}-\d{4}-[\dX]{4}", candidate):
            return candidate
        return None

    @staticmethod
    def _score_candidate(item: dict[str, Any], expected_given: str | None, expected_family: str | None) -> tuple[float, str | None]:
        orcid_id = OrcidAuthorEnricher._clean_orcid_id(item.get("orcid-id"))
        if not orcid_id:
            return 0.0, None
        given = _normalize_name_key(str(item.get("given-names") or ""))
        family = _normalize_name_key(str(item.get("family-names") or ""))
        expected_given_key = _normalize_name_key(expected_given or "")
        expected_family_key = _normalize_name_key(expected_family or "")
        score = 0.0
        if expected_given_key and given == expected_given_key:
            score += 0.5
        if expected_family_key and family == expected_family_key:
            score += 0.5
        if score < 1.0:
            credit = _normalize_name_key(str(item.get("credit-name") or ""))
            if expected_given_key and expected_family_key:
                combined = _normalize_name_key(f"{expected_given_key} {expected_family_key}")
                if credit and credit == combined:
                    score = max(score, 0.9)
        return score, orcid_id

    def resolve_orcid_uri(self, author_name: str) -> str | None:
        if not self.enabled:
            return None
        key = _normalize_name_key(author_name)
        if not key:
            return None
        with self._lock:
            if key in self._cache:
                return self._cache[key]

        given, family = _split_author_name(author_name)
        if not family:
            with self._lock:
                self._cache[key] = None
            return None
        query = f'family-name:"{family}"'
        if given:
            query = f'{query} AND given-names:"{given}"'

        candidates = self._library_search(query)
        if not candidates:
            token = self._search_token()
            if token:
                candidates = self._http_search(query, token)

        best_score = 0.0
        best_orcid: str | None = None
        for item in candidates[: self._max_candidates]:
            score, orcid_id = self._score_candidate(item, given, family)
            if score > best_score and orcid_id:
                best_score = score
                best_orcid = orcid_id

        uri = _orcid_uri(best_orcid) if best_orcid and best_score >= self._match_threshold else None
        with self._lock:
            self._cache[key] = uri
        return uri

    def enrich_publication(self, publication: NormalizedPublication) -> None:
        if not publication.authors:
            publication.authors_enriched = []
            return

        existing_by_name: dict[str, dict[str, str]] = {}
        for item in publication.authors_enriched:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if not isinstance(name, str) or not name.strip():
                continue
            entry = {"name": _normalize_whitespace(name)}
            uri = item.get("uri")
            if isinstance(uri, str) and uri.strip():
                entry["uri"] = _orcid_uri(uri)
            existing_by_name[_normalize_name_key(name)] = entry

        enriched: list[dict[str, str]] = []
        for name in publication.authors:
            normalized_name = _normalize_whitespace(name)
            key = _normalize_name_key(normalized_name)
            if not normalized_name:
                continue
            if key in existing_by_name:
                enriched.append(existing_by_name[key])
                continue
            entry = {"name": normalized_name}
            uri = self.resolve_orcid_uri(normalized_name)
            if uri:
                entry["uri"] = uri
            enriched.append(entry)

        publication.authors_enriched = enriched


_ENRICHER = OrcidAuthorEnricher()


def enrich_publication_authors(publication: NormalizedPublication) -> NormalizedPublication:
    try:
        _ENRICHER.enrich_publication(publication)
    except Exception:
        logger.exception("Author ORCID enrichment failed; proceeding without URI enrichment.")
    return publication
