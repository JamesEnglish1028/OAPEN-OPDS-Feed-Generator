from __future__ import annotations

import json
import os
import tempfile
import time
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import ijson
import requests


def _first_text(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                return candidate
    return None


def _group_collection_name(group: dict[str, Any]) -> str | None:
    metadata = group.get("metadata") if isinstance(group.get("metadata"), dict) else {}
    return _first_text(
        group.get("title"),
        group.get("name"),
        group.get("label"),
        metadata.get("title"),
        metadata.get("name"),
        metadata.get("label"),
    )


def _with_group_collection(record: dict[str, Any], collection_name: str | None) -> dict[str, Any]:
    if not collection_name:
        return record
    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    belongs_to = metadata.get("belongsTo") if isinstance(metadata.get("belongsTo"), dict) else {}
    existing_collection = belongs_to.get("collection")
    if isinstance(existing_collection, str) and existing_collection.strip():
        return record
    if isinstance(existing_collection, dict):
        existing_name = existing_collection.get("name") or existing_collection.get("title")
        if isinstance(existing_name, str) and existing_name.strip():
            return record

    enriched = dict(record)
    enriched_metadata = dict(metadata)
    enriched_belongs_to = dict(belongs_to)
    enriched_belongs_to["collection"] = collection_name
    enriched_metadata["belongsTo"] = enriched_belongs_to
    enriched["metadata"] = enriched_metadata
    return enriched


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("records", "items", "results", "publications"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        groups = payload.get("groups")
        if isinstance(groups, list):
            group_records: list[dict[str, Any]] = []
            for group in groups:
                if not isinstance(group, dict):
                    continue
                collection_name = _group_collection_name(group)
                for key in ("publications", "items"):
                    value = group.get(key)
                    if isinstance(value, list):
                        group_records.extend(
                            _with_group_collection(item, collection_name)
                            for item in value
                            if isinstance(item, dict)
                        )
            if group_records:
                return group_records
    return []


def _iter_stream_records(stream) -> Iterator[dict[str, Any]]:
    for pointer in ("publications.item", "records.item", "items.item", "results.item"):
        stream.seek(0)
        found_any = False
        for item in ijson.items(stream, pointer):
            if isinstance(item, dict):
                found_any = True
                yield item
        if found_any:
            return

    stream.seek(0)
    payload = json.load(stream)
    for record in _extract_records(payload):
        yield record


def iter_json_records(path: str) -> Iterator[dict[str, Any]]:
    with Path(path).open("rb") as handle:
        yield from _iter_stream_records(handle)


def iter_json_records_from_url(url: str, timeout_seconds: int = 120) -> Iterator[dict[str, Any]]:
    # Large remote JSON feeds can fail mid-stream (e.g., IncompleteRead) in hosted environments.
    # Download to a temp file with retry/resume, then parse from disk to avoid memory spikes.
    max_attempts = 8
    backoff_seconds = 2
    temp_path: str | None = None

    try:
        with tempfile.NamedTemporaryFile(prefix="opds-json-url-", suffix=".json", delete=False) as tmp:
            temp_path = tmp.name

        downloaded_bytes = 0
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            headers: dict[str, str] = {}
            mode = "wb"
            if downloaded_bytes > 0:
                headers["Range"] = f"bytes={downloaded_bytes}-"
                mode = "ab"

            try:
                with requests.get(url, timeout=timeout_seconds, stream=True, headers=headers) as response:
                    response.raise_for_status()
                    status = response.status_code
                    if downloaded_bytes > 0 and status == 200:
                        # Server ignored Range; restart from scratch.
                        downloaded_bytes = 0
                        mode = "wb"
                    with open(temp_path, mode) as handle:
                        for chunk in response.iter_content(chunk_size=1024 * 256):
                            if not chunk:
                                continue
                            handle.write(chunk)
                            downloaded_bytes += len(chunk)
                break
            except Exception:
                if attempt >= max_attempts:
                    raise
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 20)

        yield from iter_json_records(temp_path)
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


def load_json_payload_from_url(url: str, timeout_seconds: int = 120) -> Any:
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.json()


def extract_json_records(payload: Any) -> list[dict[str, Any]]:
    return _extract_records(payload)


def load_oai_dc_records(
    base_url: str,
    metadata_prefix: str = "oai_dc",
    set_name: str | None = None,
    from_date: str | None = None,
    until_date: str | None = None,
    max_records: int | None = None,
    timeout_seconds: int = 45,
) -> list[dict[str, list[str]]]:
    ns = {
        "oai": "http://www.openarchives.org/OAI/2.0/",
        "dc": "http://purl.org/dc/elements/1.1/",
    }
    out: list[dict[str, list[str]]] = []
    token: str | None = None

    while True:
        params: dict[str, str] = {"verb": "ListRecords"}
        if token:
            params["resumptionToken"] = token
        else:
            params["metadataPrefix"] = metadata_prefix
            if set_name:
                params["set"] = set_name
            if from_date:
                params["from"] = from_date
            if until_date:
                params["until"] = until_date

        response = requests.get(base_url, params=params, timeout=timeout_seconds)
        response.raise_for_status()
        root = ET.fromstring(response.text)

        error = root.find("oai:error", ns)
        if error is not None:
            raise ValueError(f"OAI-PMH error: {error.text or 'unknown'}")

        records = root.findall(".//oai:record", ns)
        for record in records:
            header = record.find("oai:header", ns)
            metadata = record.find("oai:metadata", ns)
            if metadata is None:
                continue
            fields: dict[str, list[str]] = {}
            if header is not None:
                datestamp = header.find("oai:datestamp", ns)
                if datestamp is not None and datestamp.text and datestamp.text.strip():
                    fields["datestamp"] = [datestamp.text.strip()]
            for tag in ("title", "creator", "subject", "publisher", "date", "identifier", "language"):
                values = [node.text.strip() for node in metadata.findall(f".//dc:{tag}", ns) if node.text and node.text.strip()]
                if values:
                    fields[tag] = values
            if fields:
                out.append(fields)
            if max_records and len(out) >= max_records:
                return out

        token_node = root.find(".//oai:resumptionToken", ns)
        token = token_node.text.strip() if token_node is not None and token_node.text and token_node.text.strip() else None
        if not token:
            break

    return out
