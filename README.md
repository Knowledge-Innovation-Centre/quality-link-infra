# Quality Link Pipeline (QL-Pipeline)
A data integration platform for processing and indexing educational data from European higher education institutions. The system discovers, transforms, and indexes course metadata, provider information, and quality assurance data from sources like DEQAR and university APIs.
## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Services](#services)
- [API Reference](#api-reference)
- [Data Pipeline](#data-pipeline)
- [Development](#development)
- [License](#license)
## Overview
QL-Pipeline provides:
- **Provider Discovery**: Automated discovery of university data manifests via DNS TXT records and `.well-known` URLs
- **Data Transformation**: RDF/Turtle to JSON-LD conversion with SPARQL query support
- **Full-Text Search**: Meilisearch integration for searching courses and institutions
- **Workflow Orchestration**: MageAI-powered ETL pipelines
- **Data Lake Storage**: MinIO-based hierarchical storage for course data
- **Version Control**: Source versioning with compound identifiers (date + version ID)
## Architecture
```
┌─────────────────────────────────────────────────────────────────────┐
│                           Frontend (React)                          │
│                         Port 3333 - Dashboard                       │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Backend (FastAPI)                            │
│                    Port 8000 - REST API                             │
└─────────────────────────────────────────────────────────────────────┘
        │                 │                │                 │
        ▼                 ▼                ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  PostgreSQL  │  │   Dragonfly  │  │    MinIO     │  │ Jena Fuseki  │
│  Port 5432   │  │  Port 6379   │  │  Port 9000   │  │  Port 3031   │
│   Database   │  │ Redis Cache  │  │ Object Store │  │  Triplestore │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
        │                 │                │                 │
        ▼                 ▼                ▼                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          MageAI                                     │
│                    Port 6789 - ETL Workflows                        │
└─────────────────────────────────────────────────────────────────────┘
```
### Directory Structure
```
quality-link-infra/
├── 00_postgres/          # Database initialization scripts
├── 01_mage/              # MageAI Dockerfile and requirements
├── 02_backend/           # FastAPI application
│   ├── app/
│   │   └── main.py       # API endpoints
│   └── requirements.txt
├── 03_frontend/          # React dashboard
│   └── src/
├── 04_notebook/          # Jupyter notebooks and sample data
├── docker-compose.yml
├── .example.env          # Environment template
└── README.md
```
## Quick Start
1. **Clone the repository**
   ```bash
   git clone https://github.com/your-org/quality-link-infra.git
   cd quality-link-infra
   ```
2. **Create environment file**
   ```bash
   cp .example.env .env
   ```
3. **Configure environment variables**
   Edit `.env` and set your credentials:
   
   Examples:-
   ```bash
   # Required: Set secure passwords
   POSTGRES_PASSWORD=your_secure_password
   DRAGONFLY_PASSWORD=your_redis_password
   MINIO_ROOT_PASSWORD=your_minio_password
   FUSEKI_ADMIN_PASSWORD=your_fuseki_password
   
   # Required: MageAI admin credentials
   DEFAULT_OWNER_EMAIL=admin@example.com
   DEFAULT_OWNER_USERNAME=admin
   DEFAULT_OWNER_PASSWORD=your_mage_password
   ```
4. **Start the services**
   ```bash
   docker-compose up -d
   ```
5. **Verify deployment**
   ```bash
   # Check all services are running
   docker-compose ps
   
   # Test backend health
   curl http://localhost:8000/health/database
   ```
6. **Access the services**
   | Service     | URL                          | Purpose                    |
   |-------------|------------------------------|----------------------------|
   | Frontend    | http://localhost:3333        | Dashboard UI               |
   | Backend API | http://localhost:8000        | REST API                   |
   | MageAI      | http://localhost:6789        | ETL workflow management    |
   | MinIO       | http://localhost:9001        | Object storage console     |
   | Fuseki      | http://localhost:3031        | SPARQL endpoint            |
## Configuration
### Environment Variables
See `.example.env` for all available options. Key configurations:
**PostgreSQL**
```bash
POSTGRES_USER=quality_link
POSTGRES_PASSWORD=<secure_password>
POSTGRES_DB=quality_link_db
```
**Dragonfly (Redis)**
```bash
DRAGONFLY_PASSWORD=<secure_password>
DRAGONFLY_CACHE_MODE=true
DRAGONFLY_SNAPSHOT_CRON=* * * * *  # Snapshot every minute
```
**MinIO**
```bash
MINIO_ROOT_USER=minio_user
MINIO_ROOT_PASSWORD=<secure_password>
BUCKET_NAME=quality-link-storage
```
**Backend**
```bash
BACKEND_PORT=8000
BACKEND_REDIS_URL=redis://:${DRAGONFLY_PASSWORD}@${DRAGONFLY_CONTAINER_NAME}:6379/1
```
### Database Schema
The PostgreSQL database includes the following tables:
- `provider` - Higher education institution records
- `source_version` - Versioned source configurations per provider
- `source` - Individual data sources within a version
- `transaction` - Processing transaction log
## Services
### Backend API
FastAPI application providing REST endpoints for:
- Provider management (`/get_all_providers`, `/get_provider`)
- Manifest discovery (`/pull_manifest_v2`)
- Data lake operations (`/list_datalake_files_v2`, `/download_datalake_file`)
- Queue management (`/queue_provider_data`)
### MageAI
ETL workflow orchestration with:
- Custom Python environment (see `01_mage/requirements.txt`)
- PostgreSQL backend for metadata
- Redis integration for task queuing
### Apache Jena Fuseki
Triplestore available on Port 3031:
- TDB2 storage backend
- SPARQL query endpoint
- Data write and update support
### MinIO
S3-compatible storage organized as:
```
datalake/
└── courses/
    └── {provider_uuid}/
        └── {source_version_uuid}/
            └── {source_uuid}/
                ├── source_manifest.json
                └── {date}/
                    └── {files}
```
## API Reference
### Health Check
```bash
GET /health/database
```
### List Providers
```bash
GET /get_all_providers?page=1&page_size=10&search_provider=university
```
### Get Provider Details
```bash
GET /get_provider?provider_uuid={uuid}
```
### Pull Manifest
Discovers and processes a provider's data manifest:
```bash
POST /pull_manifest_v2?provider_uuid={uuid}
```
The endpoint:
1. Retrieves SCHAC identifier from provider metadata
2. Checks DNS TXT records for manifest URL
3. Falls back to `.well-known` discovery
4. Parses manifest and creates source versions
### Queue Data Processing
```bash
POST /queue_provider_data?provider_uuid={uuid}&source_version_uuid={uuid}&source_uuid={uuid}&source_path={path}
```
Implements Redis-based locking to prevent concurrent processing of the same provider.
## Data Pipeline
### RDF Processing
The `04_notebook/guide.ipynb` demonstrates the RDF to Meilisearch pipeline:
1. Load RDF/Turtle data using rdflib
2. Upload to Jena Fuseki
3. Query via SPARQL
4. Transform to flat JSON documents
5. Index in Meilisearch
### Manifest Discovery Flow
```
                                            Provider Metadata
                                                    │
                                                    ▼
                                            ┌──────────────────┐
                                            │ Extract SCHAC ID │
                                            │ and Website URL  │
                                            └──────────────────┘
                                                │            │ (If URL does not lead to a manifest file)
                                                │            ▼
                                                │        ┌──────────────────┐          
                                                │        │  DNS TXT Lookup  │          
                                                │        │  for m= record   │
                                                │        └──────────────────┘
                                                │            │  
                                                ▼            ▼
                                ┌──────────────────┐ ┌────────────────────────────────┐
                                │   Validate URL   │ │ .well-known URLs               │
                                │                  │ │                                │
                                │                  │ │ - /quality-link-manifest       │
                                │    (JSON/YAML)   │ │ - /quality-link-manifest.json  │
                                │                  │ │ - /quality-link-manifest.yaml  │
                                └──────────────────┘ └────────────────────────────────┘
                                                │         │
                                                │         │
                                                │         │  
                                                │         │
                                                ▼         ▼
                                            ┌──────────────────┐
                                            │ Create Source    │
                                            │ Version Record   │
                                            └──────────────────┘
```
## Development
### Local Development
For backend development:
```bash
cd 02_backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```
For frontend development:
```bash
cd 03_frontend
npm install
npm run dev
```
### Adding Python Dependencies
**MageAI**: Add to `01_mage/requirements.txt` and rebuild:
```bash
docker-compose build mageai
docker-compose up -d mageai
```
**Backend**: Add to `02_backend/requirements.txt` and rebuild:
```bash
docker-compose build backend
docker-compose up -d backend
```
### Running Notebooks
The `04_notebook` directory contains Jupyter notebooks for data exploration. Mount the directory or run Jupyter separately with access to the Fuseki endpoint.
### Logs
```bash
# All services
docker-compose logs -f
# Specific service
docker-compose logs -f backend
```
## Volumes
Data persistence is managed through Docker volumes:
| Volume            | Purpose                              |
|-------------------|--------------------------------------|
| postgres_data     | PostgreSQL database files            |
| dragonfly_data    | Redis snapshots                      |
| minio_data        | Object storage data                  |
| mageai_projects   | MageAI project files                 |
| mageai_data       | MageAI internal data                 |
| fuseki-data       | Fuseki triplestore                   |
| fuseki-config     | Fuseki configuration                 |
| fuseki-backups    | Fuseki backup files                  |
## Stopping Services
```bash
# Stop all services
docker-compose down
# Stop and remove volumes (WARNING: deletes all data)
docker-compose down -v
```
## Troubleshooting
### Services fail to start
Check if ports are already in use:
```bash
# Check for port conflicts
lsof -i :5432  # PostgreSQL
lsof -i :6379  # Redis
lsof -i :8000  # Backend
```
### Database connection errors
Ensure PostgreSQL is fully initialized before dependent services start:
```bash
docker-compose logs postgres
# Wait for "database system is ready to accept connections"
```
### MinIO access denied
Verify credentials match between `.env` and service configuration:
```bash
docker-compose exec minio mc admin info local
```
### Redis lock issues
If a provider appears stuck in "busy" state, clear the lock:
```bash
docker-compose exec dragonfly redis-cli -a $DRAGONFLY_PASSWORD
> KEYS pull_manifest:*
> DEL pull_manifest:{provider_uuid}:*
```
## Security Considerations
- Change all default passwords in `.env` before deployment
- The backend CORS configuration in `main.py` should be restricted in production
- Consider placing services behind a reverse proxy (Caddy, nginx) for TLS termination
- MinIO console should not be exposed publicly in production
- API keys for Meilisearch should use read-only keys for search operations
## Contributing
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/new-feature`)
5. Open a Pull Request
## License
Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.