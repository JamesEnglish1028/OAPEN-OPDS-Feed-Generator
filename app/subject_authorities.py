from __future__ import annotations

from app.subject_aliases import subject_lookup_key
from app.subject_categories import classify_subject_category


THEMA_REFERENCE_URLS: dict[str, str] = {
    "en": "https://www.editeur.org/files/Thema/1.6/v1.6_en/20250410_Thema_v1.6_en.json",
    "de": "https://www.editeur.org/files/Thema/1.6/v1.6_de/20251215_Thema_v1.6_de.json",
    "da": "https://www.editeur.org/files/Thema/1.6/v1.6_da/20250415_Thema_v1.6_da.json",
    "nl": "https://www.editeur.org/files/Thema/1.6/v1.6_nl/20250512_Thema_v1.6_nl.json",
    "fr": "https://www.editeur.org/files/Thema/1.6/v1.6_fr/20251202_Thema_v1.6_fr.json",
    "it": "https://www.editeur.org/files/Thema/1.6/v1.6_it/20250529_Thema_v1.6_it.json",
}


def _lcc(term: str, code: str) -> dict[str, str]:
    return {"scheme": "http://id.loc.gov", "term": term, "code": code}


def _lcsh(term: str) -> dict[str, str]:
    return {"scheme": "http://id.loc.gov/authorities/subjects", "term": term}


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
    "social science": _lcc("Social sciences (General)", "H"),
    "society": _lcc("Social sciences (General)", "H"),
    "psychology": _lcc("Psychology", "BF"),
    "sociology": _lcc("Sociology (General)", "HM"),
    "social work": _lcc("Social pathology. Social and public welfare", "HV"),
    "politics": _lcc("Political science (General)", "JA"),
    "political science": _lcc("Political science (General)", "JA"),
    "gender": _lcc("Women. Feminism", "HQ"),
    "migration": _lcc("Emigration and immigration", "JV"),
    "anthropology": _lcc("Anthropology", "GN"),
    "europe": _lcc("History of Europe", "D"),
    "germany": _lcc("History of Germany", "DD"),
    "china": _lcc("History of China", "DS"),
    "africa": _lcc("History of Africa", "DT"),
    "russia": _lcc("History of Russia", "DK"),
    "democracy": _lcc("Political theory. State", "JC"),
    "european union": _lcc("Political institutions and public administration (Europe)", "JN"),
    "united states": _lcc("History of America", "E"),
    "italy": _lcc("History of Italy. Malta", "DG"),
    "health & medicine": _lcc("Medicine", "R"),
    "medicine and nursing": _lcc("Medicine", "R"),
    "nursing": _lcc("Nursing", "RT"),
    "anatomy": _lcc("Human anatomy", "QM"),
    "stem": _lcc("Science (General)", "Q"),
    "mathematics": _lcc("Mathematics", "QA"),
    "life sciences": _lcc("Natural history. Biology", "QH"),
    "artificial intelligence": _lcc("Computer science", "QA"),
    "earth & environment": _lcc("Geology", "QE"),
    "technology": _lcc("Technology (General)", "T"),
    "digitalization": _lcc("Technology (General)", "T"),
    "digital media": _lcc("Communication. Mass media", "P"),
    "technology & engineering": _lcc("Technology (General)", "T"),
    "law & policy": _lcc("Law in general. Comparative and uniform law", "K"),
    "law": _lcc("Law in general. Comparative and uniform law", "K"),
    "public policy": _lcc("Political institutions and public administration (General)", "JF"),
    "arts & humanities": _lcc("Philosophy. Psychology. Religion", "B"),
    "literature": _lcc("Literature (General)", "PN"),
    "literary studies": _lcc("Literature (General)", "PN"),
    "literary criticism": _lcc("Literature (General)", "PN"),
    "culture": _lcc("Civilization. Culture", "CB"),
    "cultural studies": _lcc("Civilization. Culture", "CB"),
    "cultural history": _lcc("Civilization. Culture", "CB"),
    "media": _lcc("Communication. Mass media", "P"),
    "media studies": _lcc("Communication. Mass media", "P"),
    "open access": _lcc("Libraries. Library science. Information resources", "Z"),
    "textbook": _lcc("Textbooks", "LB"),
    "history": _lcc("World History", "D"),
    "philosophy": _lcc("Philosophy (General)", "B"),
    "religion": _lcc("Religion", "BL"),
    "islam": _lcc("Islam. Bahaism. Theosophy, etc.", "BP"),
    "archaeology": _lcc("Archaeology", "CC"),
    "ethics": _lcc("Ethics", "BJ"),
    "gender studies": _lcc("Women. Feminism", "HQ"),
    "environment": _lcc("Human ecology. Anthropogeography", "GF"),
    "music": _lcc("Music", "M"),
    "middle ages": _lcc("Medieval history", "D"),
    "economics": _lcc("Economic theory. Demography", "HB"),
    "globalization": _lcc("International relations", "JZ"),
    "art": _lcc("Visual arts", "N"),
    "drawing": _lcc("Drawing. Design. Illustration", "NC"),
    "medicine": _lcc("Medicine", "R"),
    "agriculture": _lcc("Agriculture", "S"),
    "sustainability": _lcc("Economic theory. Demography", "HC"),
    "slavic linguistics": _lcc("Slavic languages. Baltic languages. Albanian language", "PG"),
    "language arts and disciplines": _lcc("Language and literature", "P"),
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
    "social science": [_lcsh("Social sciences")],
    "society": [_lcsh("Social sciences")],
    "psychology": [_lcsh("Psychology")],
    "sociology": [_lcsh("Sociology")],
    "social work": [_lcsh("Social service")],
    "politics": [_lcsh("Political science")],
    "political science": [_lcsh("Political science")],
    "gender": [_lcsh("Sex role"), _lcsh("Gender identity")],
    "migration": [_lcsh("Emigration and immigration")],
    "anthropology": [_lcsh("Anthropology")],
    "europe": [_lcsh("Europe")],
    "germany": [_lcsh("Germany")],
    "china": [_lcsh("China")],
    "africa": [_lcsh("Africa")],
    "russia": [_lcsh("Russia")],
    "democracy": [_lcsh("Democracy")],
    "european union": [_lcsh("European Union countries")],
    "united states": [_lcsh("United States")],
    "italy": [_lcsh("Italy")],
    "health & medicine": [_lcsh("Medicine"), _lcsh("Public health")],
    "medicine and nursing": [_lcsh("Medicine"), _lcsh("Nursing")],
    "nursing": [_lcsh("Nursing")],
    "anatomy": [_lcsh("Anatomy")],
    "stem": [_lcsh("Science"), _lcsh("Technology"), _lcsh("Engineering"), _lcsh("Mathematics")],
    "mathematics": [_lcsh("Mathematics")],
    "life sciences": [_lcsh("Life sciences")],
    "artificial intelligence": [_lcsh("Artificial intelligence")],
    "earth & environment": [_lcsh("Earth sciences"), _lcsh("Environmental sciences")],
    "technology": [_lcsh("Technology")],
    "digitalization": [_lcsh("Digitization")],
    "digital media": [_lcsh("Digital media")],
    "technology & engineering": [_lcsh("Technology"), _lcsh("Engineering")],
    "law & policy": [_lcsh("Law"), _lcsh("Public policy")],
    "law": [_lcsh("Law")],
    "public policy": [_lcsh("Public policy")],
    "arts & humanities": [_lcsh("Humanities"), _lcsh("Arts")],
    "literature": [_lcsh("Literature")],
    "literary studies": [_lcsh("Literature--History and criticism")],
    "literary criticism": [_lcsh("Criticism")],
    "culture": [_lcsh("Culture")],
    "cultural studies": [_lcsh("Culture")],
    "cultural history": [_lcsh("Civilization"), _lcsh("History")],
    "media": [_lcsh("Mass media")],
    "media studies": [_lcsh("Mass media")],
    "open access": [_lcsh("Open access publishing"), _lcsh("Open access journals")],
    "textbook": [_lcsh("Textbooks")],
    "history": [_lcsh("History")],
    "philosophy": [_lcsh("Philosophy")],
    "religion": [_lcsh("Religion")],
    "islam": [_lcsh("Islam")],
    "archaeology": [_lcsh("Archaeology")],
    "ethics": [_lcsh("Ethics")],
    "gender studies": [_lcsh("Women's studies"), _lcsh("Gender identity")],
    "environment": [_lcsh("Environment")],
    "music": [_lcsh("Music")],
    "middle ages": [_lcsh("Middle Ages")],
    "economics": [_lcsh("Economics")],
    "globalization": [_lcsh("Globalization")],
    "art": [_lcsh("Art")],
    "drawing": [_lcsh("Drawing")],
    "medicine": [_lcsh("Medicine")],
    "agriculture": [_lcsh("Agriculture")],
    "sustainability": [_lcsh("Sustainable development")],
    "slavic linguistics": [_lcsh("Slavic languages")],
    "language arts and disciplines": [_lcsh("Language arts")],
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
