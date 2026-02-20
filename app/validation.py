from __future__ import annotations

from typing import Any


PALACE_SUPPORTED_MEDIA_TYPES = {
    "application/epub+zip",
    "application/pdf",
    "application/audiobook+json",
    "application/vnd.readium.lcp.license.v1.0+json",
}

ACQUISITION_RELS = {
    "http://opds-spec.org/acquisition",
    "http://opds-spec.org/acquisition/open-access",
    "http://opds-spec.org/acquisition/borrow",
}


def validate_palace_opds_feed(feed: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    metadata = feed.get("metadata")
    if not isinstance(metadata, dict):
        errors.append("Feed metadata object is required.")
    elif not metadata.get("title"):
        errors.append("Feed metadata.title is required.")

    links = feed.get("links")
    if not isinstance(links, list) or not links:
        errors.append("Feed links are required.")
    else:
        has_self_link = any(link.get("rel") == "self" for link in links if isinstance(link, dict))
        if not has_self_link:
            warnings.append("Feed should include a self link for Palace clients.")

    publications = feed.get("publications")
    if not isinstance(publications, list):
        errors.append("Feed publications must be an array.")
        publications = []

    for index, publication in enumerate(publications):
        if not isinstance(publication, dict):
            errors.append(f"Publication at index {index} is not an object.")
            continue

        entry_meta = publication.get("metadata")
        if not isinstance(entry_meta, dict):
            errors.append(f"Publication {index}: metadata object is required.")
            continue
        if not entry_meta.get("title"):
            errors.append(f"Publication {index}: metadata.title is required.")
        if not entry_meta.get("identifier"):
            errors.append(f"Publication {index}: metadata.identifier is required.")

        authors = entry_meta.get("author") or []
        if authors and not all(isinstance(author, dict) and author.get("name") for author in authors):
            warnings.append(f"Publication {index}: all authors should be objects with name.")

        pub_links = publication.get("links") or []
        if not isinstance(pub_links, list) or not pub_links:
            errors.append(f"Publication {index}: at least one link is required.")
            continue

        has_acquisition_link = False
        for link in pub_links:
            if not isinstance(link, dict):
                continue
            href = link.get("href")
            if not isinstance(href, str) or not href:
                errors.append(f"Publication {index}: each link must include href.")
                continue
            rel = link.get("rel")
            media_type = link.get("type")
            if rel in ACQUISITION_RELS:
                has_acquisition_link = True
                if media_type not in PALACE_SUPPORTED_MEDIA_TYPES:
                    warnings.append(
                        f"Publication {index}: acquisition link type '{media_type}' may not be supported by Palace."
                    )
            if media_type is None:
                warnings.append(f"Publication {index}: link {href} is missing MIME type.")

        if not has_acquisition_link:
            warnings.append(f"Publication {index}: no OPDS acquisition link found.")

    return {
        "profile": "palace-opds2",
        "valid": len(errors) == 0,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
    }
