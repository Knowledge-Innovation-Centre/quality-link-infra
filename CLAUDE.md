# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Quality Link Pipeline (QL-Pipeline) — a data integration platform that discovers, transforms, and indexes data on learning opportunities (courses, programmes, micro-credentials) from European higher education institutions.

The registry of providers is seeded from DEQAR. Each provider's data sources are discovered via DNS TXT records and `.well-known` manifest URLs, then fetched and projected into RDF (Jena Fuseki) and a search index (Meilisearch).

## Architecture

Five Docker services orchestrated via `docker-compose.yml`, designed for Coolify deployment (no ports exposed by default):

- **Frontend** (`03_frontend/`) — React 18 + TypeScript + Vite + Tailwind dashboard
- **Backend** (`02_backend/`) — FastAPI REST API + Typer admin CLI; hosts the ETL pipeline in-process
- **PostgreSQL** (`00_postgres/`) — operational database; schema baked into the image via `00_init.sql` + migration files `0N_*.sql`
- **MinIO** — S3-compatible data lake
- **Jena Fuseki** — RDF triplestore (three named graphs: courses, reference, vocabulary)

Meilisearch is expected to run externally in production; add it via `docker-compose.override.yml` for local dev (see README). There is no longer a separate MageAI or Dragonfly/Redis service — ETL runs in-process in the backend, and per-provider concurrency is guarded by Postgres advisory locks.

### Backend structure (`02_backend/app/`)

The backend is modular, not a single-file app. Layout:

- `main.py` — FastAPI app factory. Mounts `routers/` and a separate public sub-app at `/api/v1` with wildcard CORS for the `credentials` router (so provider domains can fetch the QL public key).
- `cli.py` — Typer CLI entry point. Groups: `provider` (list / manifest / sources / fetch / refresh) and `vocabulary` (fetch).
- `config.py` — env-var loading (DB, MinIO, Fuseki, Meilisearch, DEQAR, default vocabularies, graph IRIs).
- `database.py` — SQLAlchemy engine + `SessionLocal`. Use `get_db` (FastAPI dep) in routers; open `SessionLocal()` directly in CLI commands and background tasks.
- `routers/` — HTTP adapters only; delegate to services.
- `services/` — business logic:
  - `manifest.py` — DNS/`.well-known` discovery, validates manifest JSON/YAML, upserts `source_version` + `source` rows.
  - `providers.py` — provider list/detail + `resolve_provider_uuid` (accepts UUID, ETER id, DEQAR id).
  - `deqar.py` — refresh provider registry from DEQAR and push to the Fuseki reference graph.
  - `datalake.py` — `queue_provider_data`: validates a fetch request (lock state, version freshness) and schedules `run_course_fetch` via `BackgroundTasks`.
  - `course_fetch/` — the ETL pipeline: `bronze.py` downloads raw source data to MinIO, `silver.py` enriches to RDF and writes to Fuseki, `gold.py` frames JSON-LD and indexes into Meilisearch. Per-source-type adapters live in `course_fetch/source_types/` (`elm`, `ooapi`, `edu-api`).
  - `fuseki.py`, `keys.py`, `locks.py`, `vocabulary.py` — Fuseki client, `ql_cred` keypair management, advisory-lock helpers, EU controlled-vocabulary fetcher.
- `schema/frame.json` — JSON-LD frame used by the gold stage.

### Locking model

Concurrency control uses **Postgres advisory locks** (`services/locks.py`), not Redis. Locks are session-scoped (`pg_try_advisory_lock(ns, hashtext(key))`), so the manifest pull can commit mid-flight without releasing its lock. `NS_PULL_MANIFEST` is the only namespace today; add new ones as stable, never-renumbered integer constants. Always `release()` explicitly; connection death is the fallback.

### ETL flow (bronze → silver → gold)

`run_course_fetch(provider, version, source, path)` opens its own `SessionLocal` (it runs in a `BackgroundTask` after the HTTP response is sent, so it must not reuse the request session):

1. **Bronze** — fetch raw data from the provider source, write to MinIO at `{bucket}/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}/{YYYY-MM-DD}/...`.
2. **Silver** — parse into an RDF graph, enrich against the reference graph, upload to Fuseki's courses graph.
3. **Gold** — SPARQL → JSON-LD frame (`schema/frame.json`) → flat docs → Meilisearch index.
4. Log a row in `transaction` (unique per provider+version+date).

### Database schema

`00_postgres/00_init.sql` defines the baseline; `01_*`, `02_*`, `03_*.sql` are additive migrations applied at image build. Tables:

- `provider` — institution registry (DEQAR / ETER / SCHAC ids, manifest probe log in `manifest_json`).
- `source_version` — a dated snapshot of a provider's manifest (`version_date` + `version_id`).
- `source` — individual data source within a version (type, path, last fetch state).
- `transaction` — processing log, unique per (provider, version, date).
- `ql_cred` — QL signing keypair; the active one is served by `/api/v1/public-key`.

## Common Commands

### Docker (full stack)
```bash
docker-compose up -d
docker-compose logs -f backend
docker-compose build backend && docker-compose up -d backend  # after requirements change
```

For local port access and a local Meilisearch, create `docker-compose.override.yml` (see README's Development section for a template).

### Frontend
```bash
cd 03_frontend
npm install
npm run dev          # Vite dev server, proxies /api to VITE_API_URL
npm run build        # tsc && vite build
npm run lint
```

### Backend — local (no containers)
```bash
cd 02_backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

`requirements.txt` is compiled from `requirements.in` — edit the `.in` file and re-pin when changing dependencies.

### Backend — admin CLI

The Typer CLI is the preferred way to drive provider operations manually (running it in-process avoids HTTP/BackgroundTask round-trips):

```bash
cd 02_backend && source venv/bin/activate
cd app  # the CLI imports top-level modules (config, database, services/...)

python cli.py course frame <URI|UUID>                     # get framed JSON-LD for a single course
python cli.py course list  <UUID|ETER_ID|DEQAR_ID> [--limit N] [--offset N]  # list courses from Fuseki
python cli.py provider list [SEARCH] [--with-data] [--page N] [--page-size N]
python cli.py provider manifest <UUID|ETER_ID|DEQAR_ID>   # DNS + .well-known discovery
python cli.py provider sources  <UUID|ETER_ID|DEQAR_ID>   # show probes + latest version's sources
python cli.py provider fetch    <UUID|ETER_ID|DEQAR_ID> [--source SOURCE_UUID]  # bronze→silver→gold
python cli.py provider refresh  [--limit N] [--offset N]  # pull registry from DEQAR
python cli.py vocabulary fetch  [SCHEME_URI ...]          # defaults to DEFAULT_VOCABULARIES
```

Or run inside the container: `docker-compose exec backend python cli.py ...`.

## Code Conventions

### Backend
- **Routers are thin**; business logic lives in `services/`. New HTTP endpoints should call into (or extend) a service function, not inline SQL.
- **Database access** uses SQLAlchemy with raw `text()` SQL — no ORM models. Parameterize everything.
- **Sessions**: HTTP handlers use `Depends(get_db)`; CLI commands and background tasks open their own `SessionLocal()` via `with` blocks.
- **Provider identifiers**: services that take a provider accept a UUID; the CLI resolves UUID / ETER id / DEQAR id via `services.providers.resolve_provider_uuid`.
- **CORS**: the main app allows the configured frontend origin; the `/api/v1` sub-app (public key) uses wildcard CORS — don't add other routes to it.

### Frontend
- Path alias `@/` → `src/`. Pages in `src/pages/`, feature components in `src/components/features/`, reusable UI in `src/components/ui/`, API clients in `src/api/`, hooks in `src/hooks/`, types in `src/types/`.

## Environment

Copy `.example.env` to `.env`. Required: `POSTGRES_PASSWORD`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `FUSEKI_ADMIN_PASSWORD`, `MEILISEARCH_URL`, `MEILISEARCH_API_KEY`, `VITE_API_URL`, `VITE_RECAPTCHA_SITE_KEY`. Optional overrides (graph names, dataset name, DEQAR API URL, default vocabulary scheme URIs) are in `02_backend/app/config.py`.

## Branching

- `main` — production branch (PR target)
- `development` — active development branch
