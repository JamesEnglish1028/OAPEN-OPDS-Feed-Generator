# OAPEN OPDS Feed Generator

Small service that ingests OAPEN metadata (JSON or OAI-PMH Dublin Core) and exposes a paginated OPDS 2.0 catalog for Palace-compatible harvesting.

## Why this exists

- OAPEN JSON dumps are too large for practical validation and incremental harvesting workflows.
- Palace uses OPDS 2.0 and does not natively harvest OAI-PMH.
- This service normalizes upstream metadata and provides batched OPDS pages.

## Features

- Ingest from JSON file (`POST /ingest/json`)
- Ingest from JSON URL (`POST /ingest/json-url`)
- Harvest paginated OPDS-like JSON feeds with checkpointed `next` links (`POST /ingest/opds-json`)
- Async ingest jobs for JSON URLs (`POST /ingest/json-url/jobs`, `GET /ingest/jobs/{job_id}`)
- Ingest from OAI-PMH endpoint with checkpointed incremental windows (`POST /ingest/oai-pmh`)
- Multi-repository support with repository-scoped ingest/feed endpoints (`/repositories/{repository_id}/...`)
- Built-in admin UI for repository management and OPDS-like JSON harvesting (`GET /admin`)
- Normalize/validate records into OPDS-like publication entries
- Paginated OPDS 2 feed (`GET /opds?page=1&page_size=50`)
- Single publication endpoint (`GET /publications/{id}`)
- Palace-oriented OPDS profile validation (`GET /validate/palace`)
- OAPEN metadata extensions mapped into OPDS output (`belongsTo`, `images`, `altIdentifier`, `accessibility`)
- Root OPDS navigation links (`rel: subsection`) for publication year feeds
- OPDS language facet compact collection (`facets`) with `numberOfItems`
- Additional `Collection` facet group for Collection and Series feeds
- `metadata.publisher.links` points to publisher-specific OPDS subfeeds
- `metadata.belongsTo.collection` includes `name` + `links`
- OPDS 2 search link and search endpoint (`/opds/search{?query,title,author,publisher,series,collection,subject}`)
- Persistent storage with `DATABASE_URL` (SQLite default, PostgreSQL supported)
- Harvest checkpoint visibility (`GET /harvest/checkpoints`)
- Alembic migration/versioning for schema changes
- Daily scheduled incremental harvest job via APScheduler
- Optional Redis/Valkey cache for OPDS feed responses (`/opds*`) with TTL + cache invalidation on ingest/harvest writes

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

`requirements.txt` installs `psycopg[binary]` automatically on Python `<3.14` (including Render Python 3.12).  
On Python `3.14+`, install a compatible PostgreSQL driver manually if you are not using SQLite.

Open docs: `http://127.0.0.1:8000/docs`
Admin UI: `http://127.0.0.1:8000/admin`

Set database connection (optional):

```bash
export DATABASE_URL="sqlite:///./oapen_opds.db"
# or PostgreSQL
export DATABASE_URL="postgresql+psycopg://username:password@localhost:5432/oapen_opds"
```

Scheduler settings (optional):

```bash
export SCHEDULER_ENABLED="true"
export SCHEDULER_DAILY_UTC_HOUR="2"
export SCHEDULER_DAILY_UTC_MINUTE="0"
```

OPDS cache settings (optional):

```bash
export REDIS_URL="redis://localhost:6379/0"
export OPDS_CACHE_TTL_SECONDS="900"
export OPDS_CACHE_PREFIX="opds-cache"
export OPDS_CACHE_INVALIDATE_EVERY_N_UPSERTS="0"
export OPDS_COLLECTION_FACET_LINK_LIMIT="100"
export OPDS_CLASSIFICATION_FACET_LINK_LIMIT="100"
export OPDS_SUBCLASSIFICATION_FACET_LINK_LIMIT="100"
```

`OPDS_CACHE_INVALIDATE_EVERY_N_UPSERTS` controls progressive cache refresh during long ingests.  
Use `0` to disable (default). Set a positive value (for example `500` or `1000`) to invalidate OPDS cache every N accepted upserts while ingest is running.

`OPDS_COLLECTION_FACET_LINK_LIMIT` limits the number of collection links embedded in normal OPDS facet groups.
Use `/opds/collections` (or repository-scoped `/repositories/{repository_id}/opds/collections`) for full paged collection browsing.

`OPDS_CLASSIFICATION_FACET_LINK_LIMIT` limits top-level classification links embedded in normal OPDS facet groups.
Use `/opds/classifications` (or repository-scoped equivalent) for full paged classification browsing.

`OPDS_SUBCLASSIFICATION_FACET_LINK_LIMIT` limits sub-classification links embedded on classification feeds.
Use `/opds/classifications/{classification_slug}/subjects` (or repository-scoped equivalent) for full paged sub-classification browsing.

Run migrations:

```bash
alembic upgrade head
```

## Example usage

Ingest JSON from local path:

```bash
curl -X POST http://127.0.0.1:8000/ingest/json \
  -H "content-type: application/json" \
  -d '{"path":"tests/data/sample_oapen.json"}'
```

Ingest JSON directly from URL (useful on Render):

```bash
curl -X POST http://127.0.0.1:8000/ingest/json-url \
  -H "content-type: application/json" \
  -d '{"url":"https://memo.oapen.org/file/oapen/OAPENLibrary.json"}'
```

Note: `json-url` ingestion streams records to reduce memory usage for large feeds.

Harvest an OPDS-like JSON feed and follow `next` links:

```bash
curl -X POST http://127.0.0.1:8000/ingest/opds-json \
  -H "content-type: application/json" \
  -d '{
    "url":"https://example.org/catalog.json",
    "max_pages":5,
    "max_records":250,
    "follow_next":true,
    "incremental":true
  }'
```

Run URL ingest as a background job (non-blocking):

```bash
curl -X POST http://127.0.0.1:8000/ingest/json-url/jobs \
  -H "content-type: application/json" \
  -d '{"url":"https://memo.oapen.org/file/oapen/OAPENLibrary.json"}'
```

Check job status:

```bash
curl "http://127.0.0.1:8000/ingest/jobs/<job_id>"
```

Ingest from OAI-PMH:

```bash
curl -X POST http://127.0.0.1:8000/ingest/oai-pmh \
  -H "content-type: application/json" \
  -d '{
    "base_url":"https://library.oapen.org/oai/request",
    "metadata_prefix":"oai_dc",
    "set_name":"oapen",
    "incremental":true,
    "from_date":"2025-01-01",
    "max_records":500
  }'
```

Retrieve OPDS page 1:

```bash
curl "http://127.0.0.1:8000/opds?page=1&page_size=25"
```

Collection feed:

```bash
curl "http://127.0.0.1:8000/opds/collections/scifi-classics?page=1&page_size=25"
```

Collections index:

```bash
curl "http://127.0.0.1:8000/opds/collections?page=1&page_size=100"
```

Classifications index:

```bash
curl "http://127.0.0.1:8000/opds/classifications?page=1&page_size=100"
```

Sub-classifications index for a classification:

```bash
curl "http://127.0.0.1:8000/opds/classifications/education/subjects?page=1&page_size=100"
```

Publication year feed:

```bash
curl "http://127.0.0.1:8000/opds/years/2026?page=1&page_size=25"
```

Language feed:

```bash
curl "http://127.0.0.1:8000/opds/languages/en?page=1&page_size=25"
```

Series feed:

```bash
curl "http://127.0.0.1:8000/opds/series/demo-series?page=1&page_size=25"
```

Publisher feed:

```bash
curl "http://127.0.0.1:8000/opds/publishers/oapen-press?page=1&page_size=25"
```

Search feed:

```bash
curl "http://127.0.0.1:8000/opds/search?query=education&page=1&page_size=25"
curl "http://127.0.0.1:8000/opds/search?title=Open%20Access&author=Carol&page=1&page_size=25"
curl "http://127.0.0.1:8000/opds/search?collection=SciFi%20Classics&page=1&page_size=25"
curl "http://127.0.0.1:8000/opds/search?subject=Education&page=1&page_size=25"
```

Validate current OPDS page against Palace profile:

```bash
curl "http://127.0.0.1:8000/validate/palace?page=1&page_size=25"
```

View persisted incremental checkpoints:

```bash
curl "http://127.0.0.1:8000/harvest/checkpoints"
```

Run incremental harvest immediately for all saved checkpoints (OAI-PMH and OPDS-like JSON):

```bash
curl -X POST http://127.0.0.1:8000/harvest/run \
  -H "content-type: application/json" \
  -d '{"max_records":500}'
```

## Test

```bash
pytest -q
```

Or run the local pre-push check wrapper:

```bash
./scripts/check.sh
```

## Incremental harvest behavior

- If `incremental` is `true`, the service will reuse the previous checkpoint `last_until_date` as the next `from_date` when `from_date` is omitted.
- Checkpoints are keyed by `base_url|metadata_prefix|set_name` unless `checkpoint_key` is provided.
- Checkpoints are persisted in the database and survive service restarts.
- A daily scheduler run automatically advances all saved checkpoints.

## Migrations

- Initial schema migration is in `alembic/versions/0001_initial_schema.py`.
- Apply latest schema changes with `alembic upgrade head`.
- Latest releases include subject indexing for classification facets, so production deploys should run the newest migration before expecting `Classifications` facets to populate correctly.
- Generate a new migration after model updates:

```bash
alembic revision -m "describe change"
```

## Render Deployment

This repo includes:

- `render.yaml` for Render service settings

### Render setup

1. Create a new Render Web Service connected to this GitHub repo.
2. Confirm build/start commands:
   - Build: `pip install -r requirements.txt`
   - Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
3. Set environment variables in Render:
   - `DATABASE_URL` (from Render Postgres connection string)
   - `SCHEDULER_ENABLED=false` (recommended until you configure recurring harvest policy)
   - `SCHEDULER_DAILY_UTC_HOUR=2`
   - `SCHEDULER_DAILY_UTC_MINUTE=0`
   - `OPDS_CACHE_TTL_SECONDS=900` (optional)
   - `OPDS_CACHE_INVALIDATE_EVERY_N_UPSERTS=0` (optional; set >0 for progressive OPDS refresh during ingest)
4. Ensure Auto-Deploy is enabled (it is enabled in `render.yaml`).
5. Push to `main`; Render will deploy automatically.

### Persistent Postgres mode

- `render.yaml` provisions `oapen-opds-db` and injects its connection string into `DATABASE_URL`.
- `render.yaml` provisions `oapen-opds-cache` (Render Key Value) and injects its connection string into `REDIS_URL`.
- On first deploy, migrations run automatically at startup.
- Data persists across service restarts/redeploys, so re-ingest is no longer required after each deployment.
