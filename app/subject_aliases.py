from __future__ import annotations

import re


_WHITESPACE_RE = re.compile(r"\s+")
_PUNCTUATION_RE = re.compile(r"[^a-z0-9]+")


def _lookup_key(value: str) -> str:
    text = value.strip().casefold()
    if not text:
        return ""
    text = text.replace("&", " and ")
    text = _PUNCTUATION_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


SUBJECT_ALIASES: dict[str, str] = {
    "ya": "Young Adult",
    "ya fiction": "Young Adult Fiction",
    "young adults": "Young Adult",
    "young adult literature": "Young Adult Literature",
    "young adulthood": "Young Adult",
    "higher education tertiary education": "Higher Education",
    "open learning distance education": "Distance Education",
    "research methods methodology": "Research Methods",
    "writing and editing guides": "Writing",
}


def canonicalize_subject_term(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        return ""
    alias = SUBJECT_ALIASES.get(_lookup_key(candidate))
    if alias:
        return alias
    return _WHITESPACE_RE.sub(" ", candidate)
