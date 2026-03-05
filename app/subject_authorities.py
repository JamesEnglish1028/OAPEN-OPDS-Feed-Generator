from __future__ import annotations

from app.subject_aliases import subject_lookup_key
from app.subject_categories import classify_subject_category


def _lcc(term: str, code: str) -> dict[str, str]:
    return {"scheme": "LCC", "term": term, "code": code}


def _lcsh(term: str) -> dict[str, str]:
    return {"scheme": "LCSH", "term": term}


# LCC navigation-oriented mappings (hierarchical class focus).
LCC_SUBJECT_MAP: dict[str, dict[str, str]] = {
    "education": _lcc("Education", "L"),
    "higher education": _lcc("Special aspects of education", "LC"),
    "distance education": _lcc("Special aspects of education", "LC"),
    "adult education": _lcc("Special aspects of education", "LC"),
    "early childhood education": _lcc("Preschool education", "LB"),
    "teaching": _lcc("Theory and practice of education", "LB"),
    "teaching resources": _lcc("Theory and practice of education", "LB"),
    "language education": _lcc("Language and literature", "P"),
    "linguistics": _lcc("Language and literature", "P"),
    "communication": _lcc("Language and literature", "P"),
    "writing": _lcc("Language and literature", "P"),
    "information studies": _lcc("Bibliography. Library science. Information resources", "Z"),
    "research methods": _lcc("Social sciences (General)", "H"),
    "business and management": _lcc("Commerce", "HF"),
    "business & management": _lcc("Commerce", "HF"),
    "management": _lcc("Industries. Land use. Labor", "HD"),
    "human resources": _lcc("Commerce", "HF"),
    "social sciences": _lcc("Social sciences (General)", "H"),
    "psychology": _lcc("Psychology", "BF"),
    "sociology": _lcc("Sociology (General)", "HM"),
    "social work": _lcc("Social pathology. Social and public welfare", "HV"),
    "health & medicine": _lcc("Medicine", "R"),
    "medicine and nursing": _lcc("Medicine", "R"),
    "nursing": _lcc("Nursing", "RT"),
    "anatomy": _lcc("Human anatomy", "QM"),
    "stem": _lcc("Science (General)", "Q"),
    "mathematics": _lcc("Mathematics", "QA"),
    "life sciences": _lcc("Natural history. Biology", "QH"),
    "artificial intelligence": _lcc("Computer science", "QA"),
    "earth & environment": _lcc("Geology", "QE"),
    "law & policy": _lcc("Law in general. Comparative and uniform law", "K"),
    "arts & humanities": _lcc("Philosophy. Psychology. Religion", "B"),
    "history": _lcc("World History", "D"),
    "philosophy": _lcc("Philosophy (General)", "B"),
}

# LCSH search-oriented headings (graph/discovery focus).
LCSH_SUBJECT_MAP: dict[str, list[dict[str, str]]] = {
    "education": [_lcsh("Education")],
    "higher education": [_lcsh("Education, Higher")],
    "distance education": [_lcsh("Distance education")],
    "adult education": [_lcsh("Adult education")],
    "early childhood education": [_lcsh("Early childhood education")],
    "teaching": [_lcsh("Teaching")],
    "teaching resources": [_lcsh("Teaching--Aids and devices")],
    "language education": [_lcsh("Language and languages--Study and teaching")],
    "linguistics": [_lcsh("Linguistics")],
    "communication": [_lcsh("Communication")],
    "writing": [_lcsh("Authorship"), _lcsh("Report writing")],
    "information studies": [_lcsh("Information science"), _lcsh("Library science")],
    "research methods": [_lcsh("Research--Methodology")],
    "business and management": [_lcsh("Business"), _lcsh("Management")],
    "business & management": [_lcsh("Business"), _lcsh("Management")],
    "management": [_lcsh("Management")],
    "human resources": [_lcsh("Personnel management")],
    "social sciences": [_lcsh("Social sciences")],
    "psychology": [_lcsh("Psychology")],
    "sociology": [_lcsh("Sociology")],
    "social work": [_lcsh("Social service")],
    "health & medicine": [_lcsh("Medicine"), _lcsh("Public health")],
    "medicine and nursing": [_lcsh("Medicine"), _lcsh("Nursing")],
    "nursing": [_lcsh("Nursing")],
    "anatomy": [_lcsh("Anatomy")],
    "stem": [_lcsh("Science"), _lcsh("Technology"), _lcsh("Engineering"), _lcsh("Mathematics")],
    "mathematics": [_lcsh("Mathematics")],
    "life sciences": [_lcsh("Life sciences")],
    "artificial intelligence": [_lcsh("Artificial intelligence")],
    "earth & environment": [_lcsh("Earth sciences"), _lcsh("Environmental sciences")],
    "law & policy": [_lcsh("Law"), _lcsh("Public policy")],
    "arts & humanities": [_lcsh("Humanities"), _lcsh("Arts")],
    "history": [_lcsh("History")],
    "philosophy": [_lcsh("Philosophy")],
}


def _canonical_key(subject_name: str) -> str:
    return subject_lookup_key(subject_name)


def resolve_lcc(subject_name: str) -> dict[str, str] | None:
    key = _canonical_key(subject_name)
    if not key:
        return None
    direct = LCC_SUBJECT_MAP.get(key)
    if direct:
        return dict(direct)
    category = classify_subject_category(subject_name)
    if not category:
        return None
    category_key = _canonical_key(category)
    mapped = LCC_SUBJECT_MAP.get(category_key)
    return dict(mapped) if mapped else None


def resolve_lcsh(subject_name: str) -> list[dict[str, str]]:
    key = _canonical_key(subject_name)
    if not key:
        return []
    direct = LCSH_SUBJECT_MAP.get(key)
    if direct:
        return [dict(item) for item in direct]
    category = classify_subject_category(subject_name)
    if not category:
        return []
    category_key = _canonical_key(category)
    mapped = LCSH_SUBJECT_MAP.get(category_key, [])
    return [dict(item) for item in mapped]

