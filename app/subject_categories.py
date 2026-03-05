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
    "politics": "Law & Policy",
    "political science": "Law & Policy",
    "public policy": "Law & Policy",
    "democracy": "Law & Policy",
    "gender studies": "Social Sciences",
    "migration": "Social Sciences",
    "media studies": "Language & Communication",
    "digital media": "Language & Communication",
    "literature": "Arts & Humanities",
    "literary studies": "Arts & Humanities",
    "literary criticism": "Arts & Humanities",
    "culture": "Arts & Humanities",
    "cultural studies": "Arts & Humanities",
    "cultural history": "Arts & Humanities",
    "film": "Arts & Humanities",
    "art": "Arts & Humanities",
    "drawing": "Arts & Humanities",
    "religion": "Arts & Humanities",
    "islam": "Arts & Humanities",
    "europe": "Social Sciences",
    "germany": "Social Sciences",
    "china": "Social Sciences",
    "africa": "Social Sciences",
    "russia": "Social Sciences",
    "united states": "Social Sciences",
    "italy": "Social Sciences",
    "european union": "Law & Policy",
    "economics": "Business & Management",
    "technology and engineering": "STEM",
    "technology engineering": "STEM",
    "agriculture": "Earth & Environment",
    "semiotics": "Language & Communication",
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
    if any(token in key for token in ("economics", "economic", "finance", "commerce")):
        return "Business & Management"
    if any(token in key for token in ("psychology", "sociology", "social", "indigenous", "society")):
        return "Social Sciences"
    if any(token in key for token in ("politic", "policy", "governance", "democracy")):
        return "Law & Policy"
    if any(token in key for token in ("medicine", "nursing", "anatomy", "health", "physiology", "medical")):
        return "Health & Medicine"
    if any(token in key for token in ("mathematics", "biology", "chemistry", "artificial intelligence", "probability", "science", "technology", "engineering")):
        return "STEM"
    if any(token in key for token in ("earth", "geology", "environment", "geography", "agriculture")):
        return "Earth & Environment"
    if "law" in key:
        return "Law & Policy"
    if any(token in key for token in ("arts", "history", "philosophy", "music", "literature", "culture", "religion", "film", "drawing")):
        return "Arts & Humanities"
    return None
