# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Quality Link Pipeline (QL-Pipeline) — a data integration platform for processing and indexing data on learning opportunities (courses, programmes, micro-credentials) from European higher education institutions.

It discovers eudcation provider data from DEQAR. It discovers, transforms, and indexes course metadata from provider sources discovered using DNS TXT records and `.well-known` manifest URLs.

## Architecture

Six Docker services orchestrated via `docker-compose.yml`, designed for Coolify deployment (no ports exposed by default):

- **Frontend** (`03_frontend/`) — React 18 + TypeScript + Vite + Tailwind CSS dashboard
- **Backend** (`02_backend/`) — FastAPI REST API (single file: `app/main.py`)
- **PostgreSQL** (`00_postgres/`) — Database with schema baked into the Docker image via `init.sql`
- **MageAI** (`01_mage/`) — ETL workflow orchestration, pulls pipelines from external git repo
- **MinIO** — S3-compatible object storage for the data lake
- **Jena Fuseki** — RDF triplestore with SPARQL endpoint
- **Dragonfly** — Redis-compatible cache (used for task queuing and locking)

Frontend talks to Backend. Backend talks to PostgreSQL, Dragonfly, and MinIO. MageAI connects to all data stores.

## Common Commands

### Docker (full stack)
```bash
docker-compose up -d              # Start all services
docker-compose down               # Stop all services
docker-compose logs -f backend    # Tail logs for a service
docker-compose build backend      # Rebuild after dependency changes
```

For local port access, create `docker-compose.override.yml` (see README).

### Frontend development
```bash
cd 03_frontend
npm install
npm run dev          # Vite dev server (proxies /api to VITE_API_URL)
npm run build        # tsc && vite build
npm run lint         # ESLint (TypeScript + React)
```

### Backend development
```bash
cd 02_backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Rebuilding Python services after adding dependencies
```bash
# MageAI: edit 01_mage/requirements.txt, then:
docker-compose build mageai && docker-compose up -d mageai

# Backend: edit 02_backend/requirements.txt, then:
docker-compose build backend && docker-compose up -d backend
```

## Key Files

| Path | Purpose |
|------|---------|
| `02_backend/app/main.py` | All backend API endpoints (single large file) |
| `00_postgres/init.sql` | Database schema (provider, source_version, source, transaction, ql_cred tables) |
| `.example.env` | Environment variable template — copy to `.env` |
| `docker-compose.yml` | Service definitions and wiring |
| `03_frontend/vite.config.ts` | Vite config with `@` path alias and `/api` proxy |

## Code Conventions

### Frontend
- Path alias: `@/` maps to `src/`
- Pages in `src/pages/`, feature components in `src/components/features/`, reusable UI in `src/components/ui/`
- API layer: `src/api/` contains client classes, `src/hooks/` has React hooks (useProviders, useProvider)
- Types in `src/types/`
- ESLint config: `.eslintrc.cjs` with `@typescript-eslint` and `react-refresh` plugins

### Backend
- Single FastAPI app in `02_backend/app/main.py` — all endpoints, models, and DB access in one file
- Database: SQLAlchemy with raw SQL for complex queries
- Redis locking pattern: prevents concurrent processing of the same provider
- MinIO paths: `datalake/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}/{date}/{files}`

## Environment

Copy `.example.env` to `.env`. Required variables: `POSTGRES_PASSWORD`, `DRAGONFLY_PASSWORD`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `FUSEKI_ADMIN_PASSWORD`, `DEFAULT_OWNER_EMAIL/USERNAME/PASSWORD`, `VITE_API_URL`, `VITE_RECAPTCHA_SITE_KEY`. Commented-out values in `.example.env` show defaults set in `docker-compose.yml`.

## Branching

- `main` — production branch (PR target)
- `development` — active development branch
