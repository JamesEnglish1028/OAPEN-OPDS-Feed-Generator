from __future__ import annotations

from dataclasses import dataclass

from app.subject_aliases import canonicalize_subject_term, subject_lookup_key


@dataclass(frozen=True)
class PublicationGroupDefinition:
    slug: str
    title: str
    subjects: tuple[str, ...]


PUBLICATION_GROUP_DEFINITIONS: tuple[PublicationGroupDefinition, ...] = (
    PublicationGroupDefinition(
        slug="humanities",
        title="Humanities",
        subjects=(
            "Performing Arts",
            "Visual Arts",
            "History",
            "Languages & Literature",
            "Law",
            "Philosophy",
            "Religious Studies",
            "Divinity & Theology",
        ),
    ),
    PublicationGroupDefinition(
        slug="social-science",
        title="Social Science",
        subjects=(
            "Anthropology",
            "Archeology",
            "Archaeology",
            "Economics",
            "Geography",
            "Linguistics",
            "Political Science",
            "Psychology",
            "Sociology",
        ),
    ),
    PublicationGroupDefinition(
        slug="natural-science",
        title="Natural Science",
        subjects=(
            "Biology",
            "Chemistry",
            "Earth Science",
            "Astronomy",
            "Physics",
        ),
    ),
    PublicationGroupDefinition(
        slug="formal-science",
        title="Formal Science",
        subjects=(
            "Computer Science",
            "Mathematics",
            "Applied Mathematics",
        ),
    ),
    PublicationGroupDefinition(
        slug="applied-sciences",
        title="Applied Sciences",
        subjects=(
            "Agriculture",
            "Architecture and Design",
            "Business",
            "Education",
            "Engineering and Technology",
            "Environmental Studies and Forestry",
            "Family and Consumer Science",
            "Human physical performance and reaction",
            "Journalism, Media Studies, and Communication",
            "Law",
            "Library and Museum Studies",
            "Medicine and Health",
            "Military Science",
            "Public Administration",
            "Public Policy",
            "Social Work",
            "Transportation",
        ),
    ),
)


_GROUP_BY_SLUG: dict[str, PublicationGroupDefinition] = {item.slug: item for item in PUBLICATION_GROUP_DEFINITIONS}
_SUBJECT_KEY_TO_GROUP_SLUGS: dict[str, set[str]] = {}
for definition in PUBLICATION_GROUP_DEFINITIONS:
    for subject in definition.subjects:
        normalized_key = subject_lookup_key(canonicalize_subject_term(subject))
        if not normalized_key:
            continue
        _SUBJECT_KEY_TO_GROUP_SLUGS.setdefault(normalized_key, set()).add(definition.slug)


def list_publication_groups() -> list[PublicationGroupDefinition]:
    return list(PUBLICATION_GROUP_DEFINITIONS)


def publication_group_by_slug(group_slug: str) -> PublicationGroupDefinition | None:
    return _GROUP_BY_SLUG.get((group_slug or "").strip().casefold())


def _key_matches(pattern_key: str, subject_key: str) -> bool:
    if subject_key == pattern_key:
        return True
    if subject_key.startswith(f"{pattern_key} "):
        return True
    if subject_key.endswith(f" {pattern_key}"):
        return True
    return f" {pattern_key} " in subject_key


def group_slugs_for_subject_names(subject_names: list[str]) -> set[str]:
    matches: set[str] = set()
    for subject_name in subject_names:
        canonical = canonicalize_subject_term(subject_name)
        subject_key = subject_lookup_key(canonical)
        if not subject_key:
            continue
        for pattern_key, slugs in _SUBJECT_KEY_TO_GROUP_SLUGS.items():
            if _key_matches(pattern_key, subject_key):
                matches.update(slugs)
    return matches
