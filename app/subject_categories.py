from __future__ import annotations

from app.subject_aliases import subject_lookup_key


EXACT_CATEGORY_MAP: dict[str, str] = {
    "the arts": "Arts & Humanities",
    "history": "Arts & Humanities",
    "philosophy": "Arts & Humanities",
    "music": "Arts & Humanities",
    "education educational sciences pedagogy": "Education",
    "higher education": "Education",
    "distance education": "Education",
    "early childhood education": "Education",
    "adult education": "Education",
    "teaching": "Education",
    "teaching resources": "Education",
    "study and learning skills general": "Education",
    "language education": "Language & Communication",
    "linguistics": "Language & Communication",
    "communication": "Language & Communication",
    "writing": "Language & Communication",
    "information studies": "Information & Research",
    "research methods": "Information & Research",
    "business and management": "Business & Management",
    "management": "Business & Management",
    "human resources": "Business & Management",
    "social sciences": "Social Sciences",
    "sociology": "Social Sciences",
    "social work": "Social Sciences",
    "psychology": "Social Sciences",
    "indigenous peoples indigeneity": "Social Sciences",
    "medicine and nursing": "Health & Medicine",
    "nursing": "Health & Medicine",
    "anatomy": "Health & Medicine",
    "medical and health informatics": "Health & Medicine",
    "personal and public health health education": "Health & Medicine",
    "mathematics": "STEM",
    "life sciences": "STEM",
    "artificial intelligence": "STEM",
    "chemistry": "STEM",
    "probability and statistics": "STEM",
    "earth sciences": "Earth & Environment",
    "earth sciences geography environment planning": "Earth & Environment",
    "geology geomorphology and the lithosphere": "Earth & Environment",
    "law": "Law & Policy",
}


def classify_subject_category(subject_name: str) -> str | None:
    key = subject_lookup_key(subject_name)
    if not key:
        return None
    exact = EXACT_CATEGORY_MAP.get(key)
    if exact:
        return exact

    if any(token in key for token in ("education", "teaching", "learning", "classroom", "pedagogy")):
        return "Education"
    if any(token in key for token in ("language", "linguistic", "writing", "communication")):
        return "Language & Communication"
    if any(token in key for token in ("research", "information", "reference", "study skills")):
        return "Information & Research"
    if any(token in key for token in ("business", "management", "marketing", "human resources", "sales")):
        return "Business & Management"
    if any(token in key for token in ("psychology", "sociology", "social", "indigenous", "society")):
        return "Social Sciences"
    if any(token in key for token in ("medicine", "nursing", "anatomy", "health", "physiology", "medical")):
        return "Health & Medicine"
    if any(token in key for token in ("mathematics", "biology", "chemistry", "artificial intelligence", "probability", "science")):
        return "STEM"
    if any(token in key for token in ("earth", "geology", "environment", "geography")):
        return "Earth & Environment"
    if "law" in key:
        return "Law & Policy"
    if any(token in key for token in ("arts", "history", "philosophy", "music")):
        return "Arts & Humanities"
    return None
