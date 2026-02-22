import os
import json
from pathlib import Path

os.environ["DATABASE_URL"] = "sqlite:///./test_oapen_opds.db"
os.environ["SCHEDULER_ENABLED"] = "false"

from fastapi.testclient import TestClient

from app.main import app, store
from app.store import PublicationStore


client = TestClient(app)


def _reset_store() -> None:
    store.initialize()
    store.clear()
    store.clear_checkpoints()


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
    assert len(page_1_json["publications"]) == 1
    rels = [link["rel"] for link in page_1_json["links"]]
    assert "next" in rels
    assert "navigation" in page_1_json
    assert "facets" in page_1_json
    assert page_1_json["facets"][0]["metadata"]["title"] == "Language"
    assert page_1_json["navigation"][0]["href"].endswith("/opds/years/2026")
    assert page_1_json["navigation"][0]["title"] == "Publication Year: 2026"

    feed_page_2 = client.get("/opds?page=2&page_size=1")
    assert feed_page_2.status_code == 200
    page_2_json = feed_page_2.json()
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
    assert "belongsTo" not in payload["metadata"]

    enriched = client.get("/publications/book-3")
    assert enriched.status_code == 200
    enriched_payload = enriched.json()
    assert enriched_payload["metadata"]["description"] == "A demonstration title with belongsTo and accessibility metadata."
    assert enriched_payload["metadata"]["belongsTo"]["series"]["name"] == "Demo Series"
    assert enriched_payload["metadata"]["belongsTo"]["series"]["position"] == 12
    assert enriched_payload["metadata"]["belongsTo"]["series"]["links"][0]["href"].endswith("/opds/series/demo-series")
    assert enriched_payload["metadata"]["belongsTo"]["collection"] == "SciFi Classics"
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


def test_collection_and_language_subfeeds() -> None:
    _reset_store()
    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"
    ingest = client.post("/ingest/json", json={"path": str(sample_path)})
    assert ingest.status_code == 200

    language_feed = client.get("/opds/languages/en?page=1&page_size=10")
    assert language_feed.status_code == 200
    language_json = language_feed.json()
    assert language_json["metadata"]["numberOfItems"] == 3

    year_feed = client.get("/opds/years/2026?page=1&page_size=10")
    assert year_feed.status_code == 200
    year_json = year_feed.json()
    assert year_json["metadata"]["numberOfItems"] == 1
    assert year_json["publications"][0]["metadata"]["title"] == "Open Access Book Three"

    publisher_feed = client.get("/opds/publishers/oapen-press?page=1&page_size=10")
    assert publisher_feed.status_code == 200
    publisher_json = publisher_feed.json()
    assert publisher_json["metadata"]["numberOfItems"] == 3


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
    assert ingest.json()["accepted"] == 6

    two_letter = client.get("/publications/lang-two").json()
    assert two_letter["metadata"]["language"] == "ENG"

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


def test_language_facet_titles_use_uppercase_native_names() -> None:
    _reset_store()
    sample_path = Path(__file__).parent / "data" / "sample_oapen.json"
    ingest = client.post("/ingest/json", json={"path": str(sample_path)})
    assert ingest.status_code == 200

    root_feed = client.get("/opds?page=1&page_size=10")
    assert root_feed.status_code == 200
    language_facet_links = root_feed.json()["facets"][0]["links"]
    titles = [item["title"] for item in language_facet_links]
    assert "ENGLISH" in titles


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
