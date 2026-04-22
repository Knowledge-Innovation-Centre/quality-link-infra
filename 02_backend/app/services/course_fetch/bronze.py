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
        "source_version_uuid": str(source_version_uuid),
        "source_uuid": str(source_uuid),
        "source_type": source["type"],
        "file_path": file_path,
        "file_format": file_format,
        "content_type": content_type,
    }
