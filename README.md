# QualityLink Data Aggregator

A data integration platform that discovers, transforms, and indexes data on learning opportunities (courses, programmes, micro-credentials) from European higher education institutions. The registry of providers is seeded from DEQAR; each provider's data sources are discovered via DNS TXT records and `.well-known` manifest URLs, then fetched and projected into an RDF triplestore (Jena Fuseki) and a search index (Meilisearch).

This software implements the [technical specifications](https://quality-link.eu/technical-specs/) developed as part of the [QualityLink project](https://quality-link.eu/) as a pilot version and technology demonstrator.

The aggregator supports data sources using the following standards:

- [ELM](https://europa.eu/europass/elm-browser/index.html), version 3
- [OOAPI](https://openonderwijsapi.nl/#/), version 5
- [Edu-API](https://www.1edtech.org/standards/edu-api), version 1.0

The deployment of the pilot version for the project can be found at:

- Aggregator dashboard: <https://dashboard.app.quality-link.eu/>
- Course catalogue: <https://courses.app.quality-link.eu/> (repository see [Knowledge-Innovation-Centre/course-catalogue](https://github.com/Knowledge-Innovation-Centre/course-catalogue))

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Services](#services)
- [API Reference](#api-reference)
- [Admin CLI](#admin-cli)
- [Data Pipeline](#data-pipeline)
- [Development](#development)
- [License](#license)

## Overview

QL-Pipeline provides:
- **Provider registry** seeded from the DEQAR API and stored in PostgreSQL
- **Discovery of data source** manifests via DNS TXT records and `.well-known` URLs
- **ETL pipeline** (bronze → silver → gold) running in-process in the backend
- **RDF storage** in Jena Fuseki with three named graphs (courses, reference, vocabulary)
- **Full-text search** via Meilisearch
- **Data lake** in MinIO for raw source snapshots
- **Signing keypair** served publicly so providers can verify QL-signed payloads

## Architecture

Five Docker services orchestrated via `docker-compose.yml`, designed for [Coolify](https://coolify.io/) deployment (no ports exposed by default). Meilisearch is expected to run externally in production; use `docker-compose.override.yml` to run it localliy (see [Development](#development) below).

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Frontend (React + Vite)                     │
│                              Dashboard UI                           │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Backend (FastAPI + Typer CLI)                  │
│              REST API + in-process ETL (bronze → silver → gold)     │
└─────────────────────────────────────────────────────────────────────┘
        │                 │                │                 │
        ▼                 ▼                ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  PostgreSQL  │  │    MinIO     │  │ Jena Fuseki  │  │ Meilisearch  │
│   Registry   │  │  Data Lake   │  │  Triplestore │  │   (external) │
│  + advisory  │  │  (raw files) │  │  3 named     │  │  full-text   │
│    locks     │  │              │  │  graphs      │  │  index       │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
```

The ETL runs as a FastAPI `BackgroundTask` (or synchronously via the CLI) — there is no separate worker service currently. For periodic runs in production (discover manifests, refetch data sources, etc.), a suitable scheduler will be added.

### Directory Structure

```
quality-link-infra/
├── 00_postgres/            # Postgres Dockerfile + schema/migrations
│   ├── 00_init.sql
│   └── 0N_*.sql            # Additive migrations applied at image build
├── 02_backend/             # FastAPI app + Typer admin CLI
│   ├── app/
│   │   ├── main.py         # App factory, mounts routers + /api/v1 sub-app
│   │   ├── cli.py          # Command-line interface
│   │   ├── config.py
│   │   ├── database.py
│   │   ├── dependencies.py
│   │   ├── routers/        # HTTP adapters (thin)
│   │   ├── services/       # Business logic
│   │   │   └── course_fetch/
│   │   │       ├── bronze.py       # Raw → MinIO
│   │   │       ├── silver.py       # MinIO → RDF → Fuseki
│   │   │       ├── gold.py         # Fuseki → JSON-LD → Meilisearch
│   │   │       └── source_types/   # elm, ooapi, eduapi adapters
│   │   └── schema/frame.json       # JSON-LD frame
│   ├── requirements.in     # Source dependencies
│   └── requirements.txt    # Pinned (compiled from .in)
├── 03_frontend/            # React 18 + TypeScript + Vite + Tailwind
├── docker-compose.yml
├── docker-compose.override.yml   # Local dev overrides (ports, Meili)
├── .example.env
├── CLAUDE.md
└── README.md
```

## Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/Knowledge-Innovation-Centre/quality-link-infra.git
   cd quality-link-infra
   ```

2. **Create environment file**
   ```bash
   cp .example.env .env
   ```

3. **Configure credentials** — at minimum set `POSTGRES_PASSWORD`, `MINIO_ROOT_PASSWORD`, `FUSEKI_ADMIN_PASSWORD`, `MEILISEARCH_URL`, `MEILISEARCH_API_KEY`, `VITE_API_URL`, `VITE_RECAPTCHA_SITE_KEY`. See [Configuration](#configuration).

4. **Start the stack**
   ```bash
   docker-compose up -d
   ```

5. **Verify**
   ```bash
   docker-compose ps
   docker-compose logs -f backend
   ```

Ports are not exposed by default. For local development with exposed ports and a local Meilisearch, see [Development](#development).

## Configuration

All configuration flows via environment variables. See `.example.env` for the full template and `02_backend/app/config.py` for optional overrides.

**Required**
```bash
POSTGRES_PASSWORD=<secure_password>
MINIO_ROOT_PASSWORD=<secure_password>
FUSEKI_ADMIN_PASSWORD=<secure_password>
MEILISEARCH_URL=<meilisearch URL>
MEILISEARCH_API_KEY=<meilisearch master/admin key>
VITE_API_URL=<backend external URL>
VITE_RECAPTCHA_SITE_KEY=<reCAPTCHA site key> # either
VITE_RECAPTCHA_ENABLED=false # or
```

**Optional** (defaults shown)
```bash
MINIO_BUCKET_NAME=quality-link-storage
FUSEKI_DATASET_NAME=qualitylink
MEILISEARCH_INDEX=ql_courses
DEQAR_API_URL=https://backend.testzone.eqar.eu/connectapi/v1/providers/
```

The backend also accepts overrides for the three Fuseki graph IRIs and the default controlled-vocabulary scheme URIs; see `02_backend/app/config.py`.

### Database Schema

PostgreSQL is initialised from `00_postgres/00_init.sql` with additive migrations (`01_*.sql`, `02_*.sql`, …) applied at image build time. Tables:

- `provider` — institution registry (DEQAR / ETER / SCHAC identifiers; manifest probe log in `manifest_json`)
- `source_version` — a dated snapshot of a provider's manifest (`version_date` + `version_id`)
- `source` — individual data source within a version (type, path, last fetch state)
- `transaction` — processing log, unique per (provider, version, date)
- `ql_cred` — QL signing keypair; the active entry is served by `/api/v1/public-key`

## Services

### Backend (FastAPI)
Hosts both the REST API and the in-process ETL pipeline. Key modules:
- `routers/` — thin HTTP adapters (`health`, `providers`, `manifest`, `datalake`, `credentials`)
- `services/` — business logic (`manifest`, `providers`, `deqar`, `datalake`, `course_fetch/*`, `fuseki`, `keys`, `locks`, `vocabulary`)
- `cli.py` — Typer admin CLI (see [Admin CLI](#admin-cli))

A separate public sub-app is mounted at `/api/v1` with wildcard CORS so any provider domain can fetch the public key.

### PostgreSQL
Operational database. Schema is baked into the image from `00_postgres/*.sql`. Concurrency control (e.g. preventing overlapping manifest pulls for the same provider) uses session-scoped advisory locks via `pg_try_advisory_lock(ns, hashtext(key))`.

### MinIO
S3-compatible data lake. Raw source snapshots are organised as:

```
{bucket}/
└── courses/
    └── {provider_uuid}/
        └── {source_version_uuid}/
            └── {source_uuid}/
                └── {YYYY-MM-DD}/
                    ├── {timestamp}.{ext}        # raw source snapshot
                    └── {timestamp}_log.txt      # run log for that snapshot
```

Per-run metadata (status, bronze file path, log file path, error message, …) lives in the `transaction` table; the data-lake layout no longer duplicates a `source_manifest.json`.

### Apache Jena Fuseki
Triplestore with TDB2 backend, using three named graphs:
- **courses** — provider-ingested course data
- **reference** — DEQAR-sourced provider registry
- **vocabulary** — EU controlled vocabularies (ISCED-F, EQF levels, languages, …)

### Meilisearch
Full-text search index over the framed JSON-LD course documents. Expected to run externally in production; run it locally via `docker-compose.override.yml`.

The Meilisearch index is used by the public-facing [course catalogue](https://github.com/Knowledge-Innovation-Centre/course-catalogue).

## API Reference

### Health
```
GET  /
GET  /health/database
```

### Providers
```
GET  /get_all_providers?search_provider=…&with_data=false&page=1&page_size=10
GET  /get_provider?provider_uuid={uuid}
```

### Manifest discovery
```
POST /pull_manifest_v2?provider_uuid={uuid}
```
Runs DNS TXT + `.well-known` probes, validates the JSON/YAML manifest, and upserts `source_version` + `source` rows. Returns 423 if another pull is in-flight for the same provider.

### Data lake
```
GET  /list_datalake_dates?provider_uuid=…&source_version_uuid=…&source_uuid=…
GET  /list_datalake_files_v2?provider_uuid=…&source_version_uuid=…&source_uuid=…&date=YYYY-MM-DD
GET  /download_datalake_file?file_path=…&preview=false
POST /queue_provider_data?provider_uuid=…&source_version_uuid=…&source_uuid=…
```
`queue_provider_data` validates the request and schedules the bronze → silver → gold pipeline as a FastAPI `BackgroundTask`. Returns 423 if a manifest pull is in-flight, or 410 if the caller is holding an outdated `source_version_uuid`.

### Credentials (public sub-app at `/api/v1`)
```
GET  /api/v1/public-key        # JSON with PEM + timestamps
GET  /api/v1/public-key/pem    # PEM as text/plain
```
Wildcard CORS — any provider domain can fetch the active signing key.

## Admin CLI

The Typer CLI is the preferred way to drive provider operations manually (runs in-process, so no HTTP/BackgroundTask round-trip).

```bash
docker-compose run --rm backend python cli.py vocabulary fetch                           # fetch DEFAULT_VOCABULARIES from EU controlled vocabularies
docker-compose run --rm backend python cli.py provider refresh				 # pull registry from DEQAR
docker-compose run --rm backend python cli.py provider list [SEARCH] [--with-data]       # list/search providers
docker-compose run --rm backend python cli.py provider manifest <UUID|ETER_ID|DEQAR_ID>  # run DNS + .well-known manifest discovery
docker-compose run --rm backend python cli.py provider sources  <UUID|ETER_ID|DEQAR_ID>  # show manifest and latest version's sources
docker-compose run --rm backend python cli.py provider fetch    <UUID|ETER_ID|DEQAR_ID>  # trigger data source ftech (bronze→silver→gold)
```

Provider identifiers accept a UUID, ETER id, or DEQAR id — they're resolved via `services.providers.resolve_provider_uuid`.

## Data Pipeline

`run_course_fetch(provider, version, source, path)` is called by both the HTTP `queue_provider_data` endpoint (via a `BackgroundTask`) and the `provider fetch` CLI command. It opens its own `SessionLocal` and runs three stages:

1. **Bronze** — fetch raw data from the provider source, convert to RDF (ELM), write to MinIO at `courses/{provider_uuid}/{source_version_uuid}/{source_uuid}/{YYYY-MM-DD}/...`
2. **Silver** — validate and enrich RDF data, upload to Fuseki's courses graph
3. **Gold** — SPARQL → JSON-LD frame (`schema/frame.json`) → flat docs → Meilisearch index

Per-source-type adapters live in `services/course_fetch/source_types/` (`elm`, `ooapi`, `eduapi`). Each run is logged in the `transaction` table (unique per provider+version+date).

### Manifest Discovery Flow

```
                 Provider metadata (DEQAR)
                            │
                            ▼
          Extract SCHAC identifier + website_link
                            │
                            ▼
          Build probe list (up to 6, tried in order):
            · SCHAC domain           × {DNS, .well-known}
            · website domain         × {DNS, .well-known}
            · website w/o "www."     × {DNS, .well-known}
                            │
              For each (domain, type) probe:
            ┌───────────────┴────────────────┐
            ▼                                ▼
   ┌──────────────────┐        ┌──────────────────────────────┐
   │ DNS TXT lookup   │        │ Try in order:                │
   │ for m=<URL>      │        │  /.well-known/               │
   │                  │        │    quality-link-manifest     │
   │ → URL from the   │        │    …-manifest.json           │
   │   TXT record     │        │    …-manifest.yaml           │
   └──────────────────┘        └──────────────────────────────┘
            │                                │
            └───────────────┬────────────────┘
                            ▼
               Fetch + parse as JSON / YAML
         Stop at first manifest containing "sources"
                            │
                            ▼
         If sources differ from latest source_version:
          insert new source_version + source rows
                            │
                            ▼
    Record every probe outcome in provider.manifest_json;
           update provider.last_manifest_pull
```

## Development

### Local Docker (with exposed ports and a local Meilisearch)

Create a `docker-compose.override.yml` alongside `docker-compose.yml`:

```yaml
services:

  # production setup has no Meilisearch service, as it runs separately
  meili:
    image: getmeili/meilisearch:v1.29
    ports:
      - "7700:7700"
    volumes:
      - meili_data:/meili_data
    environment:
      MEILI_MASTER_KEY: ${MEILISEARCH_API_KEY}

  frontend:
    ports:
      - "3000:3000"

  postgres:
    ports:
      - "5432:5432"

  minio:
    ports:
      - "9001:9001"

  backend:
    volumes:
      - ./02_backend/app:/app:ro
    command: ["uvicorn", "main:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]
    ports:
      - "8000:8000"
    depends_on:
      - meili

  fuseki:
    ports:
      - "3030:3030"

volumes:
  meili_data:
```

### Backend (no containers)
```bash
cd 02_backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

`requirements.txt` is compiled from `requirements.in` — edit the `.in` file and re-pin when changing dependencies.

### Frontend
```bash
cd 03_frontend
npm install
npm run dev     # Vite dev server; proxies /api to VITE_API_URL
npm run build   # tsc && vite build
npm run lint
```

### Adding Python dependencies
Edit `02_backend/requirements.in`, re-pin to `requirements.txt`, then:
```bash
docker-compose build backend && docker-compose up -d backend
```

### Logs
```bash
docker-compose logs -f              # all services
docker-compose logs -f backend      # specific service
```

## Volumes

| Volume          | Purpose                   |
|-----------------|---------------------------|
| postgres_data   | PostgreSQL database files |
| minio_data      | Object storage data       |
| fuseki_data     | Fuseki triplestore        |
| meili_data      | Meilisearch (local dev)   |

## Stopping Services

```bash
docker-compose down        # stop all services
docker-compose down -v     # stop and remove volumes (WARNING: deletes all data)
```

## Troubleshooting

### Port conflicts (local dev)
```bash
lsof -i :5432   # PostgreSQL
lsof -i :8000   # Backend
lsof -i :3030   # Fuseki
lsof -i :7700   # Meilisearch
```

### Database not ready
Wait for `database system is ready to accept connections` in:
```bash
docker-compose logs postgres
```

### MinIO access denied
Verify credentials match between `.env` and the container:
```bash
docker-compose exec minio mc admin info local
```

### Stuck provider lock
Advisory locks are session-scoped, so connection death releases them automatically. If the backend dies mid-pull, the lock is already gone. If you need to inspect held locks:
```sql
SELECT * FROM pg_locks WHERE locktype='advisory';
```

## Security Considerations

- Change all default passwords in `.env` before deployment.
- The main app's CORS is restricted to the configured frontend origin; the `/api/v1` sub-app (public key) uses wildcard CORS — do not add other routes there.
- Place services behind a reverse proxy (Caddy, nginx, Coolify) for TLS termination.
- Do not expose the MinIO console publicly in production.
- Use separate read-only Meilisearch keys for frontend search operations; the backend needs a key with index/write permissions.

## Branching

- `main` — production (PR target)
- `development` — active development

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit changes
4. Push to branch and open a Pull Request against `main`

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
