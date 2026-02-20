# OAPEN OPDS Feed Generator

Small service that ingests OAPEN metadata (JSON or OAI-PMH Dublin Core) and exposes a paginated OPDS 2.0 catalog for Palace-compatible harvesting.

## Why this exists

- OAPEN JSON dumps are too large for practical validation and incremental harvesting workflows.
- Palace uses OPDS 2.0 and does not natively harvest OAI-PMH.
- This service normalizes upstream metadata and provides batched OPDS pages.

## Features

- Ingest from JSON file (`POST /ingest/json`)
- Ingest from OAI-PMH endpoint with checkpointed incremental windows (`POST /ingest/oai-pmh`)
- Normalize/validate records into OPDS-like publication entries
- Paginated OPDS 2 feed (`GET /opds?page=1&page_size=50`)
- Single publication endpoint (`GET /publications/{id}`)
- Palace-oriented OPDS profile validation (`GET /validate/palace`)
- Persistent storage with `DATABASE_URL` (SQLite default, PostgreSQL supported)
- Harvest checkpoint visibility (`GET /harvest/checkpoints`)
- Alembic migration/versioning for schema changes
- Daily scheduled incremental harvest job via APScheduler

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open docs: `http://127.0.0.1:8000/docs`

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

Validate current OPDS page against Palace profile:

```bash
curl "http://127.0.0.1:8000/validate/palace?page=1&page_size=25"
```

View persisted incremental checkpoints:

```bash
curl "http://127.0.0.1:8000/harvest/checkpoints"
```

Run incremental harvest immediately for all saved checkpoints:

```bash
curl -X POST http://127.0.0.1:8000/harvest/run \
  -H "content-type: application/json" \
  -d '{"max_records":500}'
```

## Test

```bash
pytest -q
```

## Incremental harvest behavior

- If `incremental` is `true`, the service will reuse the previous checkpoint `last_until_date` as the next `from_date` when `from_date` is omitted.
- Checkpoints are keyed by `base_url|metadata_prefix|set_name` unless `checkpoint_key` is provided.
- Checkpoints are persisted in the database and survive service restarts.
- A daily scheduler run automatically advances all saved checkpoints.

## Migrations

- Initial schema migration is in `alembic/versions/0001_initial_schema.py`.
- Apply latest schema changes with `alembic upgrade head`.
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
   - `DATABASE_URL` (use Render PostgreSQL for production)
   - `SCHEDULER_ENABLED=true`
   - `SCHEDULER_DAILY_UTC_HOUR=2`
   - `SCHEDULER_DAILY_UTC_MINUTE=0`
4. Ensure Auto-Deploy is enabled (it is enabled in `render.yaml`).
5. Push to `main`; Render will deploy automatically.
