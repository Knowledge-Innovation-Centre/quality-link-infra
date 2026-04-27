import logging
from io import BytesIO
from typing import Any, Dict, Optional
from uuid import UUID

from minio import Minio
from minio.error import S3Error
from sqlalchemy import text
from sqlalchemy.orm import Session

from config import MINIO_BUCKET_NAME

from .source_types.base import DataSourceType
from .source_types.eduapi import EduApiDataSource
from .source_types.elm import ElmDataSource
from .source_types.ooapi import OoapiDataSource

logger = logging.getLogger(__name__)

HANDLERS = {
    "elm": ElmDataSource,
    "ooapi": OoapiDataSource,
    "edu-api": EduApiDataSource,
}


def _content_type_to_format(content_type: str) -> tuple[str, Optional[str]]:
    if "application/rdf+xml" in content_type or "application/xml" in content_type or "text/xml" in content_type:
        return ".xml", "xml"
    if "text/turtle" in content_type:
        return ".ttl", "turtle"
    if "application/json" in content_type or "application/ld+json" in content_type:
        return ".json", "json-ld"
    return "", None


def _format_from_path(path: str) -> Optional[str]:
    """Infer rdflib parse format from a bronze file's extension."""
    lower = path.lower()
    if lower.endswith(".ttl"):
        return "turtle"
    if lower.endswith(".xml"):
        return "xml"
    if lower.endswith(".json") or lower.endswith(".jsonld"):
        return "json-ld"
    return None


def latest_bronze_for_source(db: Session, source_uuid: UUID) -> Optional[Dict[str, Any]]:
    """Find the most recent completed bronze file for a source via the
    transaction ledger, and build a silver-input message enriched with the
    `source_version_uuid` (which the orchestrator needs to start a new
    transaction row).

    Returns None if no bronze exists for this source.
    """
    row = db.execute(
        text("""
            SELECT provider_uuid, source_version_uuid, source_uuid, bronze_file_path
            FROM transaction
            WHERE source_uuid = :source_uuid
              AND bronze_file_path IS NOT NULL
            ORDER BY created_at_date DESC, run_number DESC
            LIMIT 1
        """),
        {"source_uuid": str(source_uuid)},
    ).fetchone()
    if not row:
        return None

    file_path = row[3]
    return {
        "provider_uuid": str(row[0]),
        "source_version_uuid": str(row[1]),
        "source_uuid": str(row[2]),
        "file_path": file_path,
        "file_format": _format_from_path(file_path),
    }


def list_sources_with_bronze(
    db: Session,
    provider_uuid: Optional[UUID] = None,
) -> list[Dict[str, Any]]:
    """Enumerate sources that have at least one bronze file in the ledger.

    Picks the single most-recent transaction row per source (so the
    returned `source_version_uuid` points at the version that produced the
    latest bronze). Optionally filters by provider.
    """
    params: Dict[str, Any] = {}
    where = ["t.bronze_file_path IS NOT NULL"]
    if provider_uuid is not None:
        where.append("t.provider_uuid = :provider_uuid")
        params["provider_uuid"] = str(provider_uuid)
    where_sql = " AND ".join(where)

    rows = db.execute(
        text(f"""
            SELECT DISTINCT ON (t.source_uuid)
                   t.source_uuid, t.provider_uuid, t.source_version_uuid,
                   s.source_name, s.source_type
            FROM transaction t
            JOIN source s ON s.source_uuid = t.source_uuid
            WHERE {where_sql}
            ORDER BY t.source_uuid, t.created_at_date DESC, t.run_number DESC
        """),
        params,
    ).fetchall()

    return [
        {
            "source_uuid": str(r[0]),
            "provider_uuid": str(r[1]),
            "source_version_uuid": str(r[2]),
            "source_name": r[3],
            "source_type": r[4],
        }
        for r in rows
    ]


def fetch_bronze(
    db: Session,
    minio_client: Minio,
    provider_uuid: UUID,
    source_version_uuid: UUID,
    source_uuid: UUID,
    file_path_stem: str,
) -> Optional[Dict[str, Any]]:
    """Fetch source data from the provider, write to MinIO, return metadata.

    The caller supplies `file_path_stem` — the MinIO object key without
    extension — so the orchestrator can co-locate related artefacts (e.g. the
    run log) under a shared timestamped name. Bronze just appends the
    extension inferred from the response content-type.

    Returns None on any failure (logged).
    """
    if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
        minio_client.make_bucket(MINIO_BUCKET_NAME)
        logger.info("Created MinIO bucket %s", MINIO_BUCKET_NAME)

    row = db.execute(
        text("""
            SELECT source_id, source_name, source_type, source_path, source_version,
                   source_refresh, source_auth, source_headers, source_parameters, source_other
            FROM source WHERE source_uuid = :source_uuid
        """),
        {"source_uuid": str(source_uuid)},
    ).fetchone()
    if not row:
        logger.error("Source %s not found in DB", source_uuid)
        return None

    source = {
        "uuid": str(source_uuid),
        "source_version_uuid": str(source_version_uuid),
        "provider_uuid": str(provider_uuid),
        "id": row[0],
        "name": row[1],
        "type": row[2],
        "path": row[3],
        "version": row[4],
        "refresh": row[5],
        "auth": row[6],
        "headers": row[7],
        "parameters": row[8],
    }
    if isinstance(row[9], dict):
        source.update(row[9])

    handler_class = HANDLERS.get((source["type"] or "").lower())
    if not handler_class or not issubclass(handler_class, DataSourceType):
        logger.error("No handler for source_type %r — skipping", source["type"])
        return None

    try:
        file_bytes, content_type = handler_class(source).fetch()
    except Exception as e:
        logger.error("Fetch error for source %s: %s", source_uuid, e)
        return None

    file_extension, file_format = _content_type_to_format(content_type or "")
    file_path = f"{file_path_stem}{file_extension}"

    try:
        minio_client.put_object(
            MINIO_BUCKET_NAME, file_path,
            BytesIO(file_bytes), length=len(file_bytes),
            content_type=content_type,
        )
    except S3Error as e:
        logger.error("MinIO write failed: %s", e)
        return None

    logger.info("Bronze: wrote %s (%s bytes)", file_path, len(file_bytes))

    return {
        "provider_uuid": str(provider_uuid),
        "source_uuid": str(source_uuid),
        "file_path": file_path,
        "file_format": file_format,
    }
