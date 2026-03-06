import os
import json
from pathlib import Path
from urllib.parse import urlparse

os.environ["DATABASE_URL"] = "sqlite:///./test_oapen_opds.db"
os.environ["SCHEDULER_ENABLED"] = "false"

import pytest
from sqlalchemy import text as sqla_text

try:
    from fastapi.testclient import TestClient
except (RuntimeError, ModuleNotFoundError) as exc:
    if "httpx" in str(exc).lower():
        pytest.skip("fastapi test client unavailable because httpx is not installed in this environment", allow_module_level=True)
    raise

import app.main as main_module
from app.main import app, store
from app.store import Base, PublicationStore


client = TestClient(app)


def _reset_store() -> None:
    Base.metadata.drop_all(store._engine)
    Base.metadata.create_all(store._engine)


def test_ingest_json_and_opds_pagination() -> None:
    _reset_store()
    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"

    response = client.post("/ingest/json", json={"path": str(sample_path)})
    assert response.status_code == 200
    payload = response.json()
    assert payload["accepted"] == 3
    assert payload["total_indexed"] == 3

    feed_page_1 = client.get("/opds?page=1&page_size=1")
    assert feed_page_1.status_code == 200
    page_1_json = feed_page_1.json()
    assert page_1_json["metadata"]["numberOfItems"] == 3
    assert page_1_json["metadata"]["itemsPerPage"] == 1
    assert len(page_1_json["publications"]) == 1
    rels = [link["rel"] for link in page_1_json["links"]]
    assert "next" in rels
    assert "groups" in page_1_json
    assert any(group["metadata"]["title"] == "Language" for group in page_1_json["groups"])
    assert any(group["metadata"]["title"] == "LCC Top-Level Subjects" for group in page_1_json["groups"])
    assert any(group["metadata"]["title"] == "Collections" for group in page_1_json["groups"])
    assert any(
        link["rel"] == "search"
        and link.get("templated") is True
        and "collection" in link.get("href", "")
        and "subject" in link.get("href", "")
        for link in page_1_json["links"]
    )

    feed_page_2 = client.get("/opds?page=2&page_size=1")
    assert feed_page_2.status_code == 200
    page_2_json = feed_page_2.json()
    assert page_2_json["metadata"]["itemsPerPage"] == 1
    rels = [link["rel"] for link in page_2_json["links"]]
    assert "previous" in rels


def test_get_single_publication() -> None:
    _reset_store()
    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"
    ingest = client.post("/ingest/json", json={"path": str(sample_path)})
    assert ingest.status_code == 200

    response = client.get("/publications/book-1")
    assert response.status_code == 200
    payload = response.json()
    assert payload["metadata"]["title"] == "Open Access Book One"
    assert payload["metadata"]["belongsTo"]["collection"]["name"] == "OAPEN Press"

    enriched = client.get("/publications/book-3")
    assert enriched.status_code == 200
    enriched_payload = enriched.json()
    assert enriched_payload["metadata"]["description"] == "A demonstration title with belongsTo and accessibility metadata."
    assert enriched_payload["metadata"]["belongsTo"]["series"]["name"] == "Demo Series"
    assert enriched_payload["metadata"]["belongsTo"]["series"]["position"] == 12
    assert enriched_payload["metadata"]["belongsTo"]["series"]["links"][0]["href"].endswith("/opds/series/demo-series")
    assert enriched_payload["metadata"]["belongsTo"]["collection"]["name"] == "SciFi Classics"
    assert enriched_payload["metadata"]["belongsTo"]["collection"]["links"][0]["href"].endswith("/opds/collections/scifi-classics")
    assert enriched_payload["metadata"]["publisher"]["links"][0]["href"].endswith("/opds/publishers/oapen-press")
    assert all(value is not None for value in enriched_payload["metadata"]["belongsTo"]["series"].values())
    assert enriched_payload["metadata"]["altIdentifier"][0] == "https://doi.org/10.1234/example-doi"
    assert "urn:isbn:9780000000001" in enriched_payload["metadata"]["altIdentifier"]
    assert enriched_payload["images"][0]["href"] == "https://example.org/book-3-cover.jpg"
    assert all(link.get("rel") != "http://opds-spec.org/image" for link in enriched_payload["links"])
    assert enriched_payload["metadata"]["accessibility"][0]["hazard"] == "unknown"


def test_persistence_across_store_instances() -> None:
    _reset_store()
    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"
    ingest = client.post("/ingest/json", json={"path": str(sample_path)})
    assert ingest.status_code == 200

    persisted = PublicationStore("sqlite:///./test_oapen_opds.db")
    persisted.initialize()
    assert persisted.count() == 3


def test_opds_feed_survives_legacy_publications_schema_and_auto_repairs_columns() -> None:
    _reset_store()
    with store._engine.begin() as conn:
        conn.execute(sqla_text("DROP TABLE publications"))
        conn.execute(
            sqla_text(
                """
                CREATE TABLE publications (
                    publication_id VARCHAR(512) PRIMARY KEY,
                    title VARCHAR(2000) NOT NULL,
                    authors_json TEXT NOT NULL DEFAULT '[]',
                    language VARCHAR(64),
                    publisher VARCHAR(512),
                    published VARCHAR(64),
                    identifier VARCHAR(1024),
                    subjects_json TEXT NOT NULL DEFAULT '[]',
                    links_json TEXT NOT NULL DEFAULT '[]',
                    source VARCHAR(64) NOT NULL DEFAULT 'unknown',
                    raw_json TEXT NOT NULL DEFAULT '{}',
                    updated_at DATETIME,
                    created_at DATETIME
                )
                """
            )
        )

    # Regression guard: initialize() should patch legacy schemas so OPDS queries
    # do not fail with missing-column 500s.
    store.initialize()

    with store._engine.connect() as conn:
        columns = {str(row[1]) for row in conn.execute(sqla_text("PRAGMA table_info(publications)")).all()}
    assert "repository_id" in columns
    assert "subject_authorities_json" in columns
    assert "publication_year" in columns

    feed = client.get("/opds?page=1&page_size=10")
    assert feed.status_code == 200


def test_incremental_checkpoint_and_palace_validation(monkeypatch) -> None:
    _reset_store()

    def fake_oai_loader(**kwargs):
        assert kwargs["from_date"] is None
        return [
            {
                "title": ["Checkpointed Book"],
                "identifier": ["checkpoint-1"],
                "creator": ["Checkpoint Author"],
                "date": ["2025-12-31"],
            }
        ]

    monkeypatch.setattr("app.main.load_oai_dc_records", fake_oai_loader)

    payload = {
        "base_url": "https://example.org/oai",
        "metadata_prefix": "oai_dc",
        "set_name": "oapen",
        "incremental": True,
    }
    first = client.post("/ingest/oai-pmh", json=payload)
    assert first.status_code == 200
    first_body = first.json()
    assert first_body["accepted"] == 1
    assert first_body["checkpoint"]["last_until_date"] == "2025-12-31"

    def fake_oai_loader_incremental(**kwargs):
        assert kwargs["from_date"] == "2025-12-31"
        return []

    monkeypatch.setattr("app.main.load_oai_dc_records", fake_oai_loader_incremental)
    second = client.post("/ingest/oai-pmh", json=payload)
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["previous_checkpoint"] is not None

    validation = client.get("/validate/palace?page=1&page_size=50")
    assert validation.status_code == 200
    val = validation.json()
    assert val["profile"] == "palace-opds2"
    assert "valid" in val


def test_manual_harvest_run_endpoint(monkeypatch) -> None:
    _reset_store()

    monkeypatch.setattr(
        "app.main.run_incremental_for_all_checkpoints",
        lambda store, max_records: {
            "run_at": "2026-02-20T00:00:00+00:00",
            "checkpoints_total": 0,
            "results": [],
            "max_records": max_records,
        },
    )
    response = client.post("/harvest/run", json={"max_records": 20})
    assert response.status_code == 200
    payload = response.json()
    assert payload["checkpoints_total"] == 0
    assert payload["max_records"] == 20


def test_manual_harvest_run_advances_opds_json_checkpoint(monkeypatch) -> None:
    _reset_store()
    create_repo = client.put(
        "/repositories/opds-remote",
        json={
            "source_type": "opds-json",
            "name": "Remote OPDS",
            "config": {},
            "is_active": True,
        },
    )
    assert create_repo.status_code == 200

    store.upsert_checkpoint(
        checkpoint_key="opds-json|opds-remote|https://example.org/feed.json",
        repository_id="opds-remote",
        source_type="opds-json",
        base_url="https://example.org/feed.json",
        metadata_prefix="opds-json",
        set_name=None,
        last_from_date=None,
        last_until_date=None,
        state={"next_url": "https://example.org/feed.json"},
    )

    payloads = {
        "https://example.org/feed.json": {
            "publications": [
                {
                    "metadata": {
                        "identifier": "scheduled-1",
                        "title": "Scheduled Remote One",
                        "author": [{"name": "Remote Author"}],
                        "publisher": {"name": "Remote Press"},
                    },
                    "links": [{"href": "https://cdn.example.org/scheduled-1.epub", "rel": "http://opds-spec.org/acquisition"}],
                }
            ],
            "links": [{"rel": "next", "href": "https://example.org/feed-2.json"}],
        },
        "https://example.org/feed-2.json": {
            "publications": [
                {
                    "metadata": {
                        "identifier": "scheduled-2",
                        "title": "Scheduled Remote Two",
                        "author": [{"name": "Remote Author Two"}],
                    }
                }
            ],
            "links": [],
        },
    }

    monkeypatch.setattr("app.harvest.load_json_payload_from_url", lambda url: payloads[url])

    response = client.post("/harvest/run", json={"max_records": 1})
    assert response.status_code == 200
    payload = response.json()
    assert payload["checkpoints_total"] == 1
    assert payload["max_records"] == 1
    assert payload["results"][0]["source_type"] == "opds-json"
    assert payload["results"][0]["accepted"] == 1

    checkpoint = store.get_checkpoint("opds-json|opds-remote|https://example.org/feed.json", repository_id="opds-remote")
    assert checkpoint is not None
    assert checkpoint.state["next_url"] == "https://example.org/feed-2.json"

    repo_feed = client.get("/repositories/opds-remote/opds?page=1&page_size=10")
    assert repo_feed.status_code == 200
    assert repo_feed.json()["metadata"]["numberOfItems"] == 1


def test_collection_and_language_subfeeds() -> None:
    _reset_store()
    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"
    ingest = client.post("/ingest/json", json={"path": str(sample_path)})
    assert ingest.status_code == 200

    language_feed = client.get("/opds/languages/en?page=1&page_size=10")
    assert language_feed.status_code == 200
    language_json = language_feed.json()
    assert language_json["metadata"]["numberOfItems"] == 3
    assert "facets" in language_json
    assert language_json["facets"][0]["metadata"]["title"] == "Language"
    assert any(link["rel"] == "start" for link in language_json["links"])
    assert any(link["rel"] == "up" for link in language_json["links"])

    year_feed = client.get("/opds/years/2026?page=1&page_size=10")
    assert year_feed.status_code == 200
    year_json = year_feed.json()
    assert year_json["metadata"]["numberOfItems"] == 1
    assert year_json["publications"][0]["metadata"]["title"] == "Open Access Book Three"
    assert "facets" in year_json
    assert year_json["facets"][0]["metadata"]["title"] == "Language"
    assert len(year_json["facets"][0]["links"]) >= 1
    assert any(link["rel"] == "start" for link in year_json["links"])
    assert any(link["rel"] == "up" for link in year_json["links"])

    publisher_feed = client.get("/opds/publishers/oapen-press?page=1&page_size=10")
    assert publisher_feed.status_code == 200
    publisher_json = publisher_feed.json()
    assert publisher_json["metadata"]["numberOfItems"] == 3

    classification_feed = client.get("/opds/classifications?page=1&page_size=10")
    assert classification_feed.status_code == 200


def test_classification_subject_navigation_links_match_non_empty_subject_feeds(tmp_path) -> None:
    _reset_store()
    payload_path = tmp_path / "classification_nav_fixture.json"
    payload_path.write_text(
        json.dumps(
            {
                "publications": [
                    {"id": "class-nav-1", "title": "Class Nav 1", "subjects": ["Political Science"]},
                    {"id": "class-nav-2", "title": "Class Nav 2", "subjects": ["Political Science"]},
                    {"id": "class-nav-3", "title": "Class Nav 3", "subjects": ["Political Science"]},
                ]
            }
        ),
        encoding="utf-8",
    )
    ingest = client.post("/ingest/json", json={"path": str(payload_path)})
    assert ingest.status_code == 200

    classifications = client.get("/opds/classifications?page=1&page_size=10")
    assert classifications.status_code == 200
    navigation = classifications.json().get("navigation", [])
    assert navigation
    classification_slug = navigation[0]["href"].split("/opds/classifications/")[-1]

    subject_index = client.get(f"/opds/classifications/{classification_slug}/subjects?page=1&page_size=25")
    assert subject_index.status_code == 200
    subject_nav = subject_index.json().get("navigation", [])
    assert subject_nav

    first_subject = subject_nav[0]
    expected_count = int(first_subject.get("properties", {}).get("numberOfItems", 0))
    subject_path = urlparse(first_subject["href"]).path
    subject_feed = client.get(subject_path)
    assert subject_feed.status_code == 200
    payload = subject_feed.json()
    assert payload["metadata"]["numberOfItems"] == expected_count
    assert len(payload["publications"]) >= 1


def test_opds_search_endpoint() -> None:
    _reset_store()
    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"
    ingest = client.post("/ingest/json", json={"path": str(sample_path)})
    assert ingest.status_code == 200

    title_search = client.get("/opds/search?title=Open%20Access%20Book%20Three&page=1&page_size=10")
    assert title_search.status_code == 200
    title_json = title_search.json()
    assert title_json["metadata"]["numberOfItems"] == 1
    assert title_json["publications"][0]["metadata"]["title"] == "Open Access Book Three"
    assert any(link["rel"] == "start" for link in title_json["links"])

    author_search = client.get("/opds/search?author=Carol&page=1&page_size=10")
    assert author_search.status_code == 200
    author_json = author_search.json()
    assert author_json["metadata"]["numberOfItems"] == 1

    publisher_search = client.get("/opds/search?publisher=OAPEN%20Press&page=1&page_size=10")
    assert publisher_search.status_code == 200
    assert publisher_search.json()["metadata"]["numberOfItems"] == 3

    series_search = client.get("/opds/search?series=Demo%20Series&page=1&page_size=10")
    assert series_search.status_code == 200
    assert series_search.json()["metadata"]["numberOfItems"] == 1

    collection_search = client.get("/opds/search?collection=SciFi%20Classics&page=1&page_size=10")
    assert collection_search.status_code == 200
    assert collection_search.json()["metadata"]["numberOfItems"] == 1

    subject_search = client.get("/opds/search?subject=Education&page=1&page_size=10")
    assert subject_search.status_code == 200
    assert subject_search.json()["metadata"]["numberOfItems"] == 1


def test_publisher_normalization_for_opds_metadata(tmp_path) -> None:
    _reset_store()
    payload_path = tmp_path / "publisher_cases.json"
    payload_path.write_text(
        json.dumps(
            {
                "publications": [
                    {"id": "pub-string", "title": "Publisher String", "publisher": "  OAPEN Press  "},
                    {"id": "pub-object", "title": "Publisher Object", "publisher": {"label": "  Label Press  "}},
                    {
                        "id": "pub-array",
                        "title": "Publisher Array",
                        "publisher": [{"name": " Alpha Press "}, {"title": " Beta Press "}, {"foo": "bar"}, "noise"],
                    },
                    {"id": "pub-invalid-object", "title": "Publisher Invalid Object", "publisher": {"foo": "bar"}},
                    {"id": "pub-empty-string", "title": "Publisher Empty String", "publisher": "   "},
                ]
            }
        ),
        encoding="utf-8",
    )

    ingest = client.post("/ingest/json", json={"path": str(payload_path)})
    assert ingest.status_code == 200
    assert ingest.json()["accepted"] == 5

    string_pub = client.get("/publications/pub-string").json()
    assert string_pub["metadata"]["publisher"]["name"] == "OAPEN Press"

    object_pub = client.get("/publications/pub-object").json()
    assert object_pub["metadata"]["publisher"]["name"] == "Label Press"

    array_pub = client.get("/publications/pub-array").json()
    assert isinstance(array_pub["metadata"]["publisher"], list)
    assert [item["name"] for item in array_pub["metadata"]["publisher"]] == ["Alpha Press", "Beta Press"]

    invalid_obj_pub = client.get("/publications/pub-invalid-object").json()
    assert "publisher" not in invalid_obj_pub["metadata"]

    empty_string_pub = client.get("/publications/pub-empty-string").json()
    assert "publisher" not in empty_string_pub["metadata"]


def test_language_normalization_and_omission(tmp_path) -> None:
    _reset_store()
    payload_path = tmp_path / "language_cases.json"
    payload_path.write_text(
        json.dumps(
            {
                "publications": [
                    {"id": "lang-two", "title": "Language Two", "language": "en"},
                    {"id": "lang-two-af", "title": "Language Two AF", "language": "af"},
                    {"id": "lang-three", "title": "Language Three", "language": "esp"},
                    {"id": "lang-three-uppercase-rus", "title": "Language Three Uppercase RUS", "language": "RUS"},
                    {"id": "lang-three-uppercase-dut", "title": "Language Three Uppercase DUT", "language": "DUT"},
                    {"id": "lang-word", "title": "Language Word", "language": "spanish"},
                    {"id": "lang-null", "title": "Language Null", "language": None},
                ]
            }
        ),
        encoding="utf-8",
    )

    ingest = client.post("/ingest/json", json={"path": str(payload_path)})
    assert ingest.status_code == 200
    assert ingest.json()["accepted"] == 7

    two_letter = client.get("/publications/lang-two").json()
    assert two_letter["metadata"]["language"] == "ENG"

    two_letter_af = client.get("/publications/lang-two-af").json()
    assert two_letter_af["metadata"]["language"] == "AFR"

    three_letter = client.get("/publications/lang-three").json()
    assert three_letter["metadata"]["language"] == "SPA"

    three_letter_upper_rus = client.get("/publications/lang-three-uppercase-rus").json()
    assert three_letter_upper_rus["metadata"]["language"] == "RUS"

    three_letter_upper_dut = client.get("/publications/lang-three-uppercase-dut").json()
    assert three_letter_upper_dut["metadata"]["language"] == "NLD"

    language_word = client.get("/publications/lang-word").json()
    assert language_word["metadata"]["language"] == "SPA"

    null_language = client.get("/publications/lang-null").json()
    assert "language" not in null_language["metadata"]

    lower_case_language_feed = client.get("/opds/languages/en?page=1&page_size=10")
    assert lower_case_language_feed.status_code == 200
    assert lower_case_language_feed.json()["metadata"]["numberOfItems"] == 1


class _FakeOpdsCache:
    def __init__(self) -> None:
        self._payloads: dict[str, dict] = {}
        self.invalidations = 0

    def key_for_request(self, request, namespace=None) -> str:
        query = f"?{request.url.query}" if request.url.query else ""
        prefix = f"{namespace}:" if namespace else ""
        return f"{prefix}{request.url.path}{query}"

    def get_json(self, key: str):
        return self._payloads.get(key)

    def set_json(self, key: str, payload: dict) -> None:
        self._payloads[key] = payload

    def invalidate_feed_keys(self) -> int:
        count = len(self._payloads)
        self.invalidations += 1
        self._payloads.clear()
        return count

    def is_enabled(self) -> bool:
        return True

    def close(self) -> None:
        return None


def test_opds_feed_cache_hit_reuses_previous_response(monkeypatch) -> None:
    _reset_store()
    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"
    ingest = client.post("/ingest/json", json={"path": str(sample_path)})
    assert ingest.status_code == 200

    fake_cache = _FakeOpdsCache()
    monkeypatch.setattr(main_module, "opds_cache", fake_cache)

    page_calls = {"count": 0}
    original_page = main_module.store.page

    def counting_page(*args, **kwargs):
        page_calls["count"] += 1
        return original_page(*args, **kwargs)

    monkeypatch.setattr(main_module.store, "page", counting_page)

    first = client.get("/opds?page=1&page_size=2")
    second = client.get("/opds?page=1&page_size=2")
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert page_calls["count"] == 1


def test_ingest_invalidates_opds_cache(monkeypatch) -> None:
    _reset_store()
    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"
    ingest = client.post("/ingest/json", json={"path": str(sample_path)})
    assert ingest.status_code == 200

    fake_cache = _FakeOpdsCache()
    monkeypatch.setattr(main_module, "opds_cache", fake_cache)

    cached = client.get("/opds?page=1&page_size=1")
    assert cached.status_code == 200
    assert len(fake_cache._payloads) == 1

    again = client.post("/ingest/json", json={"path": str(sample_path)})
    assert again.status_code == 200
    assert fake_cache.invalidations >= 1
    assert len(fake_cache._payloads) == 0


def test_ingest_progressively_invalidates_cache_when_configured(monkeypatch) -> None:
    _reset_store()
    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"

    fake_cache = _FakeOpdsCache()
    monkeypatch.setattr(main_module, "opds_cache", fake_cache)
    monkeypatch.setenv("OPDS_CACHE_INVALIDATE_EVERY_N_UPSERTS", "2")

    response = client.post("/ingest/json", json={"path": str(sample_path)})
    assert response.status_code == 200
    assert response.json()["accepted"] == 3
    # One mid-ingest invalidation at 2, one final invalidation at endpoint completion.
    assert fake_cache.invalidations == 2


def test_language_facet_titles_use_uppercase_native_names() -> None:
    _reset_store()
    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"
    ingest = client.post("/ingest/json", json={"path": str(sample_path)})
    assert ingest.status_code == 200

    root_feed = client.get("/opds?page=1&page_size=10")
    assert root_feed.status_code == 200
    language_group = next(group for group in root_feed.json()["groups"] if group["metadata"]["title"] == "Language")
    language_facet_links = language_group["navigation"]
    titles = [item["title"] for item in language_facet_links]
    assert "ENGLISH" in titles


def test_language_facet_titles_for_norwegian_variants(tmp_path) -> None:
    _reset_store()
    payload_path = tmp_path / "language_norwegian_cases.json"
    payload_path.write_text(
        json.dumps(
            {
                "publications": [
                    {"id": "lang-nor", "title": "Language NOR", "language": "NOR"},
                    {"id": "lang-nob", "title": "Language NOB", "language": "NOB"},
                    {"id": "lang-nno", "title": "Language NNO", "language": "NNO"},
                ]
            }
        ),
        encoding="utf-8",
    )
    ingest = client.post("/ingest/json", json={"path": str(payload_path)})
    assert ingest.status_code == 200

    root_feed = client.get("/opds?page=1&page_size=10")
    assert root_feed.status_code == 200
    language_group = next(group for group in root_feed.json()["groups"] if group["metadata"]["title"] == "Language")
    links = language_group["navigation"]
    title_by_href = {item["href"].split("/opds/languages/")[-1]: item["title"] for item in links}
    assert title_by_href["NOR"] == "NORSK"
    assert title_by_href["NOB"] == "NORSK BOKMAL"
    assert title_by_href["NNO"] == "NORSK NYNORSK"


def test_language_facet_titles_from_iso639_living_reference_names(tmp_path) -> None:
    _reset_store()
    payload_path = tmp_path / "language_reference_name_case.json"
    payload_path.write_text(
        json.dumps({"publications": [{"id": "lang-afr", "title": "Language AFR", "language": "af"}]}),
        encoding="utf-8",
    )
    ingest = client.post("/ingest/json", json={"path": str(payload_path)})
    assert ingest.status_code == 200

    root_feed = client.get("/opds?page=1&page_size=10")
    assert root_feed.status_code == 200
    language_group = next(group for group in root_feed.json()["groups"] if group["metadata"]["title"] == "Language")
    links = language_group["navigation"]
    title_by_href = {item["href"].split("/opds/languages/")[-1]: item["title"] for item in links}
    assert title_by_href["AFR"] == "AFRIKAANS"


def test_opds_metadata_omits_null_temporal_fields(tmp_path) -> None:
    _reset_store()
    payload_path = tmp_path / "null_time_case.json"
    payload_path.write_text(
        json.dumps({"publications": [{"id": "null-time", "title": "No Published Date"}]}),
        encoding="utf-8",
    )

    ingest = client.post("/ingest/json", json={"path": str(payload_path)})
    assert ingest.status_code == 200
    publication = client.get("/publications/null-time")
    assert publication.status_code == 200
    metadata = publication.json()["metadata"]
    assert "modified" not in metadata
    assert "published" not in metadata


def test_publication_metadata_type_maps_chapter_records(tmp_path) -> None:
    _reset_store()
    payload_path = tmp_path / "chapter_type_case.json"
    payload_path.write_text(
        json.dumps(
            {
                "publications": [
                    {
                        "id": "chapter-1",
                        "title": "A Chapter Record",
                        "doi": "10.1007/1345_2025_301",
                        "contentType": "Chapter",
                        "publicationType": "Book",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    ingest = client.post("/ingest/json", json={"path": str(payload_path)})
    assert ingest.status_code == 200

    publication = client.get("/publications/chapter-1")
    assert publication.status_code == 200
    payload = publication.json()
    assert payload["metadata"]["@type"] == "http://schema.org/Chapter"


def test_subject_authorities_backfill_emits_backward_compatible_enriched_subjects(tmp_path) -> None:
    _reset_store()
    payload_path = tmp_path / "subject_authority_case.json"
    payload_path.write_text(
        json.dumps(
            {
                "publications": [
                    {
                        "id": "subject-authority-1",
                        "title": "Subject Authority Demo",
                        "subjects": ["Political Science"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    ingest = client.post("/ingest/json", json={"path": str(payload_path)})
    assert ingest.status_code == 200

    backfill = client.post(
        "/repositories/default/backfill/subject-authorities",
        json={"batch_size": 100},
    )
    assert backfill.status_code == 200
    backfill_payload = backfill.json()
    assert backfill_payload["processed_publications"] >= 1

    publication = client.get("/publications/subject-authority-1")
    assert publication.status_code == 200
    metadata = publication.json()["metadata"]
    assert metadata["subject"] == [{"name": "Political Science"}]
    assert isinstance(metadata.get("subjects"), list)
    assert metadata["subjects"][0]["name"] == "Political Science"
    authority_schemes = {
        authority.get("scheme")
        for authority in metadata["subjects"][0].get("authorities", [])
        if isinstance(authority, dict)
    }
    assert "http://id.loc.gov" in authority_schemes or "http://id.loc.gov/authorities/subjects" in authority_schemes


def test_repository_scoped_ingest_and_feed_isolated_from_default() -> None:
    _reset_store()
    create_repo = client.put(
        "/repositories/demo-repo",
        json={
            "source_type": "json",
            "name": "Demo Repository",
            "config": {},
            "is_active": True,
        },
    )
    assert create_repo.status_code == 200

    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"
    ingest_repo = client.post("/repositories/demo-repo/ingest/json", json={"path": str(sample_path)})
    assert ingest_repo.status_code == 200
    assert ingest_repo.json()["accepted"] == 3

    default_feed = client.get("/opds?page=1&page_size=10")
    assert default_feed.status_code == 200
    assert default_feed.json()["metadata"]["numberOfItems"] == 0

    repo_feed = client.get("/repositories/demo-repo/opds?page=1&page_size=10")
    assert repo_feed.status_code == 200
    assert repo_feed.json()["metadata"]["numberOfItems"] == 3
    assert repo_feed.json()["metadata"]["repositoryId"] == "demo-repo"
    assert repo_feed.json()["metadata"]["isDefaultRepository"] is False

    default_alias = client.get("/opds/default?page=1&page_size=10")
    assert default_alias.status_code == 200
    assert default_alias.json()["metadata"]["repositoryId"] == "default"
    assert default_alias.json()["metadata"]["isDefaultRepository"] is True

    repo_alias = client.get("/opds/demo-repo?page=1&page_size=10")
    assert repo_alias.status_code == 200
    assert repo_alias.json()["metadata"]["repositoryId"] == "demo-repo"

    index = client.get("/opds/index")
    assert index.status_code == 200
    entries = index.json()["navigation"]
    assert any(item["properties"]["repositoryId"] == "default" for item in entries)
    assert any(item["properties"]["repositoryId"] == "demo-repo" for item in entries)

    repositories = client.get("/repositories")
    assert repositories.status_code == 200
    repo_entries = {item["repository_id"]: item for item in repositories.json()["repositories"]}
    assert repo_entries["default"]["isDefaultRepository"] is True
    assert repo_entries["default"]["publicationCount"] == 0
    assert repo_entries["default"]["sourceDomain"] == "oapen.org"
    assert repo_entries["demo-repo"]["isDefaultRepository"] is False
    assert repo_entries["demo-repo"]["publicationCount"] == 3
    assert repo_entries["demo-repo"]["sourceDomain"] is None
    assert repo_entries["demo-repo"]["feedHref"].endswith("/repositories/demo-repo/opds")


def test_repository_opds_json_ingest_follows_next_and_reuses_feed_features(monkeypatch) -> None:
    _reset_store()
    create_repo = client.put(
        "/repositories/opds-remote",
        json={
            "source_type": "opds-json",
            "name": "Remote OPDS",
            "config": {},
            "is_active": True,
        },
    )
    assert create_repo.status_code == 200

    payloads = {
        "https://example.org/feed.json": {
            "metadata": {"title": "Remote Feed"},
            "publications": [
                {
                    "metadata": {
                        "identifier": "remote-1",
                        "title": "Remote One",
                        "author": [{"name": "Ada Author"}],
                        "language": "eng",
                        "publisher": {"name": "Remote Press"},
                        "subject": [{"name": "Science"}],
                        "belongsTo": {
                            "collection": {"name": "Remote Collection"},
                            "series": {"name": "Remote Series", "position": 2},
                        },
                    },
                    "links": [
                        {
                            "href": "https://cdn.example.org/remote-1.epub",
                            "rel": "http://opds-spec.org/acquisition",
                            "type": "application/epub+zip",
                        }
                    ],
                }
            ],
            "links": [{"rel": "next", "href": "https://example.org/feed-2.json"}],
        },
        "https://example.org/feed-2.json": {
            "groups": [
                {
                    "metadata": {"title": "Recent"},
                    "publications": [
                        {
                            "metadata": {
                                "identifier": "remote-2",
                                "title": "Remote Two",
                                "author": [{"name": "Bea Writer"}],
                                "language": "en",
                                "publisher": {"name": "Remote Press"},
                                "subject": ["Mathematics"],
                            },
                            "links": [
                                {
                                    "href": "https://cdn.example.org/remote-2.pdf",
                                    "rel": "http://opds-spec.org/acquisition/open-access",
                                    "type": "application/pdf",
                                }
                            ],
                        }
                    ],
                }
            ],
            "links": [],
        },
    }

    def fake_loader(url: str, timeout_seconds: int = 120):
        assert timeout_seconds == 30
        return payloads[url]

    monkeypatch.setattr(main_module, "load_json_payload_from_url", fake_loader)

    ingest = client.post(
        "/repositories/opds-remote/ingest/opds-json",
        json={
            "url": "https://example.org/feed.json",
            "max_pages": 3,
            "follow_next": True,
            "timeout_seconds": 30,
        },
    )
    assert ingest.status_code == 200
    payload = ingest.json()
    assert payload["accepted"] == 2
    assert payload["rejected"] == 0
    assert payload["pages_crawled"] == 2
    assert payload["records_processed"] == 2
    assert payload["total_indexed"] == 2
    assert payload["checkpoint"]["state"]["next_url"] == "https://example.org/feed.json"

    feed = client.get("/repositories/opds-remote/opds?page=1&page_size=10")
    assert feed.status_code == 200
    feed_json = feed.json()
    assert feed_json["metadata"]["numberOfItems"] == 2
    assert feed_json["metadata"]["repositoryId"] == "opds-remote"
    assert "groups" in feed_json
    assert any(link["rel"] == "search" for link in feed_json["links"])

    first_pub = client.get("/repositories/opds-remote/publications/remote-1")
    assert first_pub.status_code == 200
    pub_json = first_pub.json()
    assert pub_json["metadata"]["author"][0]["name"] == "Ada Author"
    assert pub_json["metadata"]["belongsTo"]["series"]["name"] == "Remote Series"
    assert pub_json["metadata"]["belongsTo"]["collection"]["name"] == "Remote Collection"
    assert pub_json["metadata"]["publisher"]["name"] == "Remote Press"


def test_delete_repository_removes_data_and_config() -> None:
    _reset_store()
    create_repo = client.put(
        "/repositories/demo-repo",
        json={
            "source_type": "json",
            "name": "Demo Repository",
            "config": {},
            "is_active": True,
        },
    )
    assert create_repo.status_code == 200

    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"
    ingest_repo = client.post("/repositories/demo-repo/ingest/json", json={"path": str(sample_path)})
    assert ingest_repo.status_code == 200
    assert ingest_repo.json()["accepted"] == 3

    deleted = client.delete("/repositories/demo-repo")
    assert deleted.status_code == 200
    payload = deleted.json()
    assert payload["deleted"] is True
    assert payload["repository_id"] == "demo-repo"
    assert payload["removed_publications"] == 3

    missing_repo = client.get("/repositories/demo-repo")
    assert missing_repo.status_code == 404

    repo_feed = client.get("/repositories/demo-repo/opds?page=1&page_size=10")
    assert repo_feed.status_code == 404


def test_default_repository_cannot_be_deleted() -> None:
    _reset_store()
    response = client.delete("/repositories/default")
    assert response.status_code == 400


def test_cleanup_repository_by_domain_removes_only_matching_records(tmp_path) -> None:
    _reset_store()
    payload_path = tmp_path / "cleanup_domain_case.json"
    payload_path.write_text(
        json.dumps(
            {
                "publications": [
                    {
                        "id": "keep-1",
                        "title": "Keep Me",
                        "links": [{"href": "https://library.oapen.org/handle/20.500.12657/12345"}],
                    },
                    {
                        "id": "remove-1",
                        "title": "Remove Me",
                        "links": [{"href": "https://pressbooks.pub/example-book/download"}],
                        "metadata": {"identifier": "https://pressbooks.pub/example-book"},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    ingest = client.post("/ingest/json", json={"path": str(payload_path)})
    assert ingest.status_code == 200
    assert ingest.json()["accepted"] == 2

    dry_run = client.post("/repositories/default/cleanup/domain", json={"domain": "pressbooks.pub", "dry_run": True})
    assert dry_run.status_code == 200
    assert dry_run.json()["matched_publications"] == 1
    assert dry_run.json()["removed_publications"] == 0

    cleanup = client.post("/repositories/default/cleanup/domain", json={"domain": "pressbooks.pub"})
    assert cleanup.status_code == 200
    payload = cleanup.json()
    assert payload["removed_publications"] == 1
    assert payload["remaining_publications"] == 1

    remaining = client.get("/opds?page=1&page_size=10")
    assert remaining.status_code == 200
    assert remaining.json()["metadata"]["numberOfItems"] == 1
    assert remaining.json()["publications"][0]["metadata"]["title"] == "Keep Me"


def test_cleanup_repository_by_identifier_prefix_removes_matching_records(tmp_path) -> None:
    _reset_store()
    payload_path = tmp_path / "cleanup_prefix_case.json"
    payload_path.write_text(
        json.dumps(
            {
                "publications": [
                    {
                        "title": "Keep Prefix Me",
                        "identifier": "oapen:book:123",
                    },
                    {
                        "title": "Remove Prefix Me",
                        "identifier": "urn:pressbooks.directory:book:456",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    ingest = client.post("/ingest/json", json={"path": str(payload_path)})
    assert ingest.status_code == 200
    assert ingest.json()["accepted"] == 2

    dry_run = client.post(
        "/repositories/default/cleanup/identifier-prefix",
        json={"prefix": "urn:pressbooks.directory:book:", "dry_run": True},
    )
    assert dry_run.status_code == 200
    assert dry_run.json()["matched_publications"] == 1
    assert dry_run.json()["removed_publications"] == 0

    cleanup = client.post(
        "/repositories/default/cleanup/identifier-prefix",
        json={"prefix": "urn:pressbooks.directory:book:"},
    )
    assert cleanup.status_code == 200
    payload = cleanup.json()
    assert payload["removed_publications"] == 1
    assert payload["remaining_publications"] == 1

    remaining = client.get("/opds?page=1&page_size=10")
    assert remaining.status_code == 200
    assert remaining.json()["metadata"]["numberOfItems"] == 1
    assert remaining.json()["publications"][0]["metadata"]["title"] == "Keep Prefix Me"


def test_admin_ui_renders_repository_and_opds_json_controls() -> None:
    response = client.get("/admin")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert "Multi-Repository OPDS Admin" in body
    assert "/repositories/" in body
    assert "/ingest/opds-json" in body
    assert "Delete" in body
    assert "repo-actions" in body
