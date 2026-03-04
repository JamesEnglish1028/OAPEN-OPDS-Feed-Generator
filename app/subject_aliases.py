from __future__ import annotations

import re


_WHITESPACE_RE = re.compile(r"\s+")
_PUNCTUATION_RE = re.compile(r"[^a-z0-9]+")


def subject_lookup_key(value: str) -> str:
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
    "social research and statistics": "Research Methods",
    "writing and editing guides": "Writing",
    "creative writing and creative writing guides": "Writing",
    "society and social sciences": "Social Sciences",
    "communication studies": "Communication",
    "interpersonal communication and skills": "Communication",
    "biology life sciences": "Life Sciences",
    "business and management": "Business & Management",
    "business studies general": "Business & Management",
    "economics finance business and management": "Business & Management",
    "management leadership and motivation": "Management",
    "personnel and human resources management": "Human Resources",
    "language and linguistics": "Linguistics",
    "language teaching and learning": "Language Education",
    "language learning writing skills": "Language Education",
    "language learning grammar vocabulary and pronunciation": "Language Education",
    "language learning reading skills": "Language Education",
    "language learning speaking skills": "Language Education",
    "language acquisition": "Language Education",
    "language teaching and learning material and coursework": "Language Education",
    "language teaching and learning second or additional languages": "Language Education",
    "library and information services": "Information Studies",
    "research and information general": "Information Studies",
    "reference information and interdisciplinary subjects": "Information Studies",
    "teaching skills and techniques": "Teaching",
    "teachers classroom resources and material": "Teaching Resources",
    "early childhood care and education": "Early Childhood Education",
    "adult education continuous learning": "Adult Education",
    "artificial intelligence ai": "Artificial Intelligence",
}


def canonicalize_subject_term(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        return ""
    alias = SUBJECT_ALIASES.get(subject_lookup_key(candidate))
    if alias:
        return alias
    return _WHITESPACE_RE.sub(" ", candidate)
