import os
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

    enriched = client.get("/publications/book-3")
    assert enriched.status_code == 200
    enriched_payload = enriched.json()
    assert enriched_payload["metadata"]["belongsTo"][0]["name"] == "Demo Series"
    assert enriched_payload["metadata"]["belongsTo"][0]["series"] == "Demo Series"
    assert enriched_payload["metadata"]["belongsTo"][0]["seriesNumber"] == "12"
    assert enriched_payload["metadata"]["altIdentifier"][0] == "https://doi.org/10.1234/example-doi"
    assert "urn:isbn:9780000000001" in enriched_payload["metadata"]["altIdentifier"]
    assert enriched_payload["images"][0]["href"] == "https://example.org/book-3-cover.jpg"
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
