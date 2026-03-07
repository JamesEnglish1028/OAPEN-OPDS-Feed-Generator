from __future__ import annotations

from functools import lru_cache
from typing import Any

import requests

from app.subject_aliases import canonicalize_subject_term, subject_lookup_key
from app.subject_categories import classify_subject_category


THEMA_SCHEME_URI = "https://ns.editeur.org/thema/"
THEMA_REFERENCE_URLS: dict[str, str] = {
    "en": "https://www.editeur.org/files/Thema/1.6/v1.6_en/20250410_Thema_v1.6_en.json",
    "de": "https://www.editeur.org/files/Thema/1.6/v1.6_de/20251215_Thema_v1.6_de.json",
    "da": "https://www.editeur.org/files/Thema/1.6/v1.6_da/20250415_Thema_v1.6_da.json",
    "nl": "https://www.editeur.org/files/Thema/1.6/v1.6_nl/20250512_Thema_v1.6_nl.json",
    "fr": "https://www.editeur.org/files/Thema/1.6/v1.6_fr/20251202_Thema_v1.6_fr.json",
    "it": "https://www.editeur.org/files/Thema/1.6/v1.6_it/20250529_Thema_v1.6_it.json",
}


def _thema(term: str, code: str) -> dict[str, str]:
    return {"scheme": THEMA_SCHEME_URI, "term": term, "code": code}


THEMA_STARTER_MAP: dict[str, list[dict[str, str]]] = {
    "history": [_thema("History", "NH")],
    "archaeology": [_thema("Archaeology", "NK")],
    "politics": [_thema("Politics and government", "JP")],
    "political science": [_thema("Political science and theory", "JPA")],
    "sociology": [_thema("Sociology", "JHB")],
    "anthropology": [_thema("Sociology and anthropology", "JH")],
    "education": [_thema("Education", "JN")],
    "education research": [_thema("Education", "JN")],
    "philosophy": [_thema("Philosophy", "QD")],
    "religion": [_thema("Religion and beliefs", "QR")],
    "islam": [_thema("Islam", "QRSF")],
    "law": [_thema("Law", "L")],
    "public policy": [_thema("Politics and government", "JP")],
    "democracy": [_thema("Politics and government", "JP")],
    "european union": [_thema("Politics and government", "JP")],
    "colonialism": [_thema("Colonialism and imperialism", "NHTB")],
    "racism": [_thema("Racism and racial discrimination / segregation", "JBFK2")],
    "diversity": [_thema("Diversity, equality and inclusion in society", "JBFK5")],
    "human rights": [_thema("Human rights, civil rights", "JPVH")],
    "international relations": [_thema("International relations", "JPS")],
    "public administration": [_thema("Public administration", "JPP")],
    "literature": [_thema("Literature, literary studies and rhetoric", "DS")],
    "literary studies": [_thema("Literary studies: prose, narrative and genres", "DSA")],
    "literary criticism": [_thema("Literary studies: prose, narrative and genres", "DSA")],
    "culture": [_thema("Society and culture: general", "JB")],
    "cultural studies": [_thema("Society and culture: general", "JB")],
    "media": [_thema("Media studies", "JFD")],
    "media studies": [_thema("Media studies", "JFD")],
    "communication": [_thema("Communication studies", "JFC")],
    "linguistics": [_thema("Language, linguistics and communication", "C")],
    "psychology": [_thema("Psychology", "JM")],
    "gender": [_thema("Gender studies: women and girls", "JBSF1")],
    "gender studies": [_thema("Gender studies: women and girls", "JBSF1")],
    "feminism": [_thema("Feminism and feminist theory", "JBSF1")],
    "migration": [_thema("Migration, immigration and emigration", "JBFH")],
    "science": [_thema("Science: general issues", "PD")],
    "space": [_thema("Science: general issues", "PD")],
    "mathematics": [_thema("Mathematics", "PB")],
    "artificial intelligence": [_thema("Artificial intelligence", "UB")],
    "technology": [_thema("Technology, engineering, agriculture", "T")],
    "digitalization": [_thema("Digital technology: general", "UBJ")],
    "digital media": [_thema("Digital media and online communication", "JFDU")],
    "film": [_thema("Media studies", "JFD")],
    "earth & environment": [_thema("Earth sciences, geography, environment, planning", "R")],
    "environment": [_thema("The environment", "RNK")],
    "climate change": [_thema("The environment", "RNK")],
    "medicine": [_thema("Medicine and nursing", "M")],
    "health & medicine": [_thema("Medicine and nursing", "M")],
    "nursing": [_thema("Nursing", "MQC")],
    "economics": [_thema("Economics", "KC")],
    "economy": [_thema("Economics", "KC")],
    "business and economics": [_thema("Economics, finance, business and management", "K")],
    "business and management": [_thema("Business and management", "KJ")],
    "business and management": [_thema("Business and management", "KJ")],
    "human resources": [_thema("Human resource management", "KJMB")],
    "globalization": [_thema("Globalization", "JPS")],
    "ethics": [_thema("Ethics and moral philosophy", "QDPM")],
    "aesthetics": [_thema("Aesthetics", "QDTQ")],
    "music": [_thema("Music", "AV")],
    "art": [_thema("The arts", "A")],
    "design": [_thema("Design", "AKC")],
    "poetry": [_thema("Poetry", "DCQ")],
    "translation": [_thema("Language: reference and general", "CB")],
    "biography": [_thema("Biography: general", "DNB")],
    "drawing": [_thema("Drawing and drawings", "AFC")],
    "agriculture": [_thema("Agriculture and farming", "TV")],
    "sustainability": [_thema("The environment", "RNK")],
    "sustainable development": [_thema("The environment", "RNK")],
    "ecology": [_thema("The environment", "RNK")],
    "development": [_thema("Society and Social Sciences", "J")],
    "identity": [_thema("Society and culture: general", "JB")],
    "ethnography": [_thema("Sociology and anthropology", "JH")],
    "violence": [_thema("Society and Social Sciences", "J")],
    "covid 19": [_thema("Medicine and nursing", "M")],
    "innovation": [_thema("Innovation management", "KJMV5")],
    "middle ages": [_thema("History", "NH")],
    "language arts and disciplines": [_thema("Language, linguistics and communication", "C")],
    "health and medicine": [_thema("Medicine and nursing", "M")],
    "earth and environment": [_thema("Earth sciences, geography, environment, planning", "R")],
    "semiotics": [_thema("Language, linguistics and communication", "C")],
    "slavic linguistics": [_thema("Language, linguistics and communication", "C")],
    "architecture": [_thema("The arts", "A")],
    "open access": [_thema("Library and information studies / archivistics", "GL")],
    "textbook": [_thema("Education", "JN")],
}

THEMA_CATEGORY_MAP: dict[str, list[dict[str, str]]] = {
    "arts and humanities": [_thema("The arts", "A")],
    "social sciences": [_thema("Society and Social Sciences", "J")],
    "business and management": [_thema("Economics, finance, business and management", "K")],
    "law and policy": [_thema("Law", "L")],
    "health and medicine": [_thema("Medicine and nursing", "M")],
    "earth and environment": [_thema("Earth sciences, geography, environment, planning", "R")],
    "stem": [_thema("Mathematics and Science", "P"), _thema("Technology, engineering, agriculture", "T")],
    "language and communication": [_thema("Language, linguistics and communication", "C")],
    "information and research": [_thema("Library and information studies / archivistics", "GL")],
    "education": [_thema("Education", "JN")],
}

THEMA_GEOGRAPHIC_KEYS: set[str] = {
    "europe",
    "germany",
    "china",
    "africa",
    "russia",
    "united states",
    "italy",
    "latin america",
}


def _is_geographic_subject_key(key: str) -> bool:
    if not key:
        return False
    if key in THEMA_GEOGRAPHIC_KEYS:
        return True
    tokens = key.split()
    return any(token in {"europe", "africa", "america", "asia"} for token in tokens)


def _freeze_mapping(mapping: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(mapping.items()))


def _thaw_mapping(frozen: tuple[tuple[str, str], ...]) -> dict[str, str]:
    return {key: value for key, value in frozen}


def _lcc(term: str, code: str) -> dict[str, str]:
    return {"scheme": "http://id.loc.gov", "term": term, "code": code}


def _lcsh(term: str) -> dict[str, str]:
    return {"scheme": "http://id.loc.gov/authorities/subjects", "term": term}


def _iter_thema_codes(node: Any) -> list[tuple[str, str]]:
    matches: list[tuple[str, str]] = []

    def _walk(value: Any) -> None:
        if isinstance(value, dict):
            raw_code = value.get("CodeValue")
            raw_desc = value.get("CodeDescription")
            if isinstance(raw_code, str) and isinstance(raw_desc, str):
                code = raw_code.strip()
                description = raw_desc.strip()
                if code and description:
                    matches.append((code, description))
            for item in value.values():
                _walk(item)
            return
        if isinstance(value, list):
            for item in value:
                _walk(item)

    _walk(node)
    return matches


@lru_cache(maxsize=1)
def _thema_lookup() -> dict[str, list[dict[str, str]]]:
    lookup: dict[str, list[dict[str, str]]] = {}
    for url in THEMA_REFERENCE_URLS.values():
        try:
            response = requests.get(
                url,
                timeout=20,
                headers={"Accept": "application/json", "User-Agent": "oapen-opds-feed-generator/1.0"},
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            continue

        seen_in_language: set[tuple[str, str]] = set()
        for code, description in _iter_thema_codes(payload):
            key = subject_lookup_key(description)
            if not key:
                continue
            dedupe_key = (key, code)
            if dedupe_key in seen_in_language:
                continue
            seen_in_language.add(dedupe_key)
            lookup.setdefault(key, []).append(_thema(description, code))
    return lookup


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
    "feminism": _lcc("Women. Feminism", "HQ"),
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
    "colonialism": _lcc("Colonies and colonization. Emigration and immigration. International migration", "JV"),
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
    "feminism": [_lcsh("Feminism")],
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
    "colonialism": [_lcsh("Colonization"), _lcsh("Imperialism")],
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
    canonical = canonicalize_subject_term(subject_name)
    return subject_lookup_key(canonical)


def _contains_key_term(container_key: str, candidate_key: str) -> bool:
    if not container_key or not candidate_key:
        return False
    if container_key == candidate_key:
        return True
    if container_key.startswith(f"{candidate_key} "):
        return True
    if container_key.endswith(f" {candidate_key}"):
        return True
    return f" {candidate_key} " in container_key


def _best_partial_match_key(container_key: str, available_keys: list[str]) -> str | None:
    matches = [key for key in available_keys if _contains_key_term(container_key, key)]
    if not matches:
        return None
    matches.sort(key=len, reverse=True)
    return matches[0]


@lru_cache(maxsize=32768)
def _resolve_lcc_cached(key: str) -> tuple[tuple[str, str], ...] | None:
    if not key:
        return None
    direct = LCC_SUBJECT_MAP.get(key)
    if direct:
        return _freeze_mapping(direct)
    partial_key = _best_partial_match_key(key, list(LCC_SUBJECT_MAP.keys()))
    if partial_key:
        partial = LCC_SUBJECT_MAP.get(partial_key)
        if partial:
            return _freeze_mapping(partial)
    category = classify_subject_category(key)
    if not category:
        return None
    category_key = _canonical_key(category)
    mapped = LCC_SUBJECT_MAP.get(category_key)
    return _freeze_mapping(mapped) if mapped else None


@lru_cache(maxsize=32768)
def _resolve_lcsh_cached(key: str) -> tuple[tuple[tuple[str, str], ...], ...]:
    if not key:
        return tuple()
    direct = LCSH_SUBJECT_MAP.get(key)
    if direct:
        return tuple(_freeze_mapping(item) for item in direct)
    partial_key = _best_partial_match_key(key, list(LCSH_SUBJECT_MAP.keys()))
    if partial_key:
        partial = LCSH_SUBJECT_MAP.get(partial_key, [])
        if partial:
            return tuple(_freeze_mapping(item) for item in partial)
    category = classify_subject_category(key)
    if not category:
        return tuple()
    category_key = _canonical_key(category)
    mapped = LCSH_SUBJECT_MAP.get(category_key, [])
    return tuple(_freeze_mapping(item) for item in mapped)


@lru_cache(maxsize=32768)
def _resolve_thema_cached(key: str) -> tuple[tuple[tuple[str, str], ...], ...]:
    lookup = _thema_lookup()
    mappings: list[tuple[tuple[str, str], ...]] = []
    dedupe_codes: set[str] = set()

    def _append(entries: list[dict[str, str]] | tuple[tuple[tuple[str, str], ...], ...]) -> None:
        for entry in entries:
            if isinstance(entry, tuple):
                thawed = _thaw_mapping(entry)
            else:
                thawed = entry
            code = thawed.get("code", "")
            if not code or code in dedupe_codes:
                continue
            dedupe_codes.add(code)
            mappings.append(_freeze_mapping(thawed))

    if key:
        _append(THEMA_STARTER_MAP.get(key, []))
        partial_key = _best_partial_match_key(key, list(THEMA_STARTER_MAP.keys()))
        if partial_key and partial_key != key:
            _append(THEMA_STARTER_MAP.get(partial_key, []))

    category = classify_subject_category(key)
    category_key = ""
    lcsh_keys: list[str] = []
    if category and not _is_geographic_subject_key(key):
        category_key = _canonical_key(category)
        if category_key:
            _append(THEMA_CATEGORY_MAP.get(category_key, []))

    # Reuse LCSH terms as lexical hints into THEMA labels.
    for frozen_mapping in _resolve_lcsh_cached(key):
        mapping = _thaw_mapping(frozen_mapping)
        term = mapping.get("term", "")
        term_key = _canonical_key(term)
        if term_key:
            lcsh_keys.append(term_key)
            _append(THEMA_STARTER_MAP.get(term_key, []))

    # Optional enrichment from remote THEMA label corpora.
    for candidate_key in (key, category_key, *lcsh_keys):
        if not candidate_key:
            continue
        _append(lookup.get(candidate_key, []))

    return tuple(mappings)


def resolve_lcc(subject_name: str) -> dict[str, str] | None:
    key = _canonical_key(subject_name)
    resolved = _resolve_lcc_cached(key)
    return _thaw_mapping(resolved) if resolved else None


def resolve_lcsh(subject_name: str) -> list[dict[str, str]]:
    key = _canonical_key(subject_name)
    return [_thaw_mapping(item) for item in _resolve_lcsh_cached(key)]


def resolve_thema(subject_name: str) -> list[dict[str, str]]:
    key = _canonical_key(subject_name)
    return [_thaw_mapping(item) for item in _resolve_thema_cached(key)]
