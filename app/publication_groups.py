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
_GROUP_SUBGROUP_ENTRIES: dict[str, list[dict[str, str]]] = {}
_SUBJECT_KEY_TO_GROUP_SUBGROUPS: dict[str, set[tuple[str, str]]] = {}


def _slugify(value: str) -> str:
    key = subject_lookup_key(value)
    return key.replace(" ", "-")


for definition in PUBLICATION_GROUP_DEFINITIONS:
    subgroup_entries: list[dict[str, str]] = []
    for subject in definition.subjects:
        normalized_key = subject_lookup_key(canonicalize_subject_term(subject))
        if not normalized_key:
            continue
        _SUBJECT_KEY_TO_GROUP_SLUGS.setdefault(normalized_key, set()).add(definition.slug)
        subgroup_slug = _slugify(subject)
        _SUBJECT_KEY_TO_GROUP_SUBGROUPS.setdefault(normalized_key, set()).add((definition.slug, subgroup_slug))
        subgroup_entries.append(
            {
                "slug": subgroup_slug,
                "title": subject,
            }
        )
    deduped_subgroups: list[dict[str, str]] = []
    seen_subgroup_slugs: set[str] = set()
    for entry in subgroup_entries:
        if entry["slug"] in seen_subgroup_slugs:
            continue
        seen_subgroup_slugs.add(entry["slug"])
        deduped_subgroups.append(entry)
    _GROUP_SUBGROUP_ENTRIES[definition.slug] = deduped_subgroups


def list_publication_groups() -> list[PublicationGroupDefinition]:
    return list(PUBLICATION_GROUP_DEFINITIONS)


def publication_group_by_slug(group_slug: str) -> PublicationGroupDefinition | None:
    return _GROUP_BY_SLUG.get((group_slug or "").strip().casefold())


def list_subgroups_for_group(group_slug: str) -> list[dict[str, str]]:
    definition = publication_group_by_slug(group_slug)
    if definition is None:
        return []
    return list(_GROUP_SUBGROUP_ENTRIES.get(definition.slug, []))


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
    for group_slug, _ in subgroup_memberships_for_subject_names(subject_names):
        matches.add(group_slug)
    return matches


def subgroup_memberships_for_subject_names(subject_names: list[str]) -> set[tuple[str, str]]:
    matches: set[tuple[str, str]] = set()
    for subject_name in subject_names:
        canonical = canonicalize_subject_term(subject_name)
        subject_key = subject_lookup_key(canonical)
        if not subject_key:
            continue
        for pattern_key, group_and_subgroups in _SUBJECT_KEY_TO_GROUP_SUBGROUPS.items():
            if _key_matches(pattern_key, subject_key):
                matches.update(group_and_subgroups)
    return matches
