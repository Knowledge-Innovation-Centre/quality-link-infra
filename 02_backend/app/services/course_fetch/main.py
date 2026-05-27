import io
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from io import BytesIO
from typing import Iterator
from uuid import UUID

import requests
from minio import Minio

from config import MINIO_BUCKET_NAME
from database import SessionLocal
from dependencies import get_minio_client
from services.locks import NS_COURSE_FETCH, advisory_lock

from .bronze import fetch_bronze, latest_bronze_for_source
from .gold import index_gold
from .silver import enrich_silver
from .transactions import finish_transaction, start_transaction, update_transaction

logger = logging.getLogger(__name__)

_LOG_FORMATTER = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")


@contextmanager
def _capture_logs() -> Iterator[io.StringIO]:
    """Attach a handler to the services.course_fetch logger and collect
    emitted records in a StringIO buffer for the duration of the block.

    Child loggers (bronze, silver, gold, source_types.*) propagate up, so
    one handler on the parent captures everything. The parent's level is
    forced to INFO for the duration — otherwise CLI runs (which never call
    logging.basicConfig) inherit the root WARNING level and drop INFO records
    before they ever reach the handler.
    """
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(_LOG_FORMATTER)
    handler.setLevel(logging.DEBUG)
    parent = logging.getLogger("services.course_fetch")
    previous_level = parent.level
    parent.setLevel(logging.INFO)
    parent.addHandler(handler)
    try:
        yield buf
    finally:
        handler.flush()
        parent.removeHandler(handler)
        parent.setLevel(previous_level)


def _upload_log(minio_client: Minio, object_key: str, content: str) -> None:
    data = content.encode("utf-8")
    minio_client.put_object(
        MINIO_BUCKET_NAME, object_key,
        BytesIO(data), length=len(data),
        content_type="text/plain; charset=utf-8",
    )


def _build_file_path_stem(
    provider_uuid: UUID, source_version_uuid: UUID, source_uuid: UUID,
) -> str:
    """MinIO object-key stem shared by the bronze data file and the run log.

    Layout: `datalake/courses/{provider}/{version}/{source}/{YYYY-MM-DD}/{YYYYMMDD_HHMMSS}`
    Bronze appends the content-type extension; the log appends `_log.txt`.
    """
    now = datetime.now(timezone.utc)
    return (
        f"datalake/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}/"
        f"{now.strftime('%Y-%m-%d')}/{now.strftime('%Y%m%d_%H%M%S')}"
    )


def run_course_fetch(
    provider_uuid: UUID,
    source_version_uuid: UUID,
    source_uuid: UUID,
) -> None:
    """Bronze → silver → gold, bracketed by a per-source advisory lock and
    a transaction-ledger row (start → update → finish) with log capture.

    Opens its own SQLAlchemy session — must not reuse a request-scoped session
    since this runs in a FastAPI BackgroundTask after the response has been sent.
    """
    logger.info(
        "course_fetch: provider=%s source=%s version=%s",
        provider_uuid, source_uuid, source_version_uuid,
    )
    minio_client = get_minio_client()

    with SessionLocal() as db, requests.Session() as http:
        with advisory_lock(db, NS_COURSE_FETCH, str(source_uuid)) as acquired:
            if not acquired:
                logger.warning(
                    "course_fetch: source %s already being processed — skipping",
                    source_uuid,
                )
                return

            started = start_transaction(db, provider_uuid, source_version_uuid, source_uuid)
            trans_uuid = started[0] if started else None

            file_path_stem = _build_file_path_stem(
                provider_uuid, source_version_uuid, source_uuid
            )
            log_path = f"{file_path_stem}_log.txt"
            status_val = "failed"
            error_message = None

            with _capture_logs() as log_buf:
                try:
                    bronze = fetch_bronze(
                        db, minio_client, provider_uuid, source_version_uuid, source_uuid,
                        file_path_stem=file_path_stem,
                    )
                    if not bronze:
                        raise RuntimeError("bronze returned no result")
                    if trans_uuid:
                        update_transaction(db, trans_uuid, bronze_file_path=bronze["file_path"])

                    courses = enrich_silver(db, minio_client, http, bronze)
                    if courses is None:
                        raise RuntimeError("silver returned no result")
                    if trans_uuid:
                        update_transaction(
                            db, trans_uuid, course_count=len(courses),
                        )

                    index_gold(http, courses)
                    status_val = "success"
                except Exception as e:
                    error_message = f"{type(e).__name__}: {e}"
                    logger.exception("course_fetch failed: %s", e)

            try:
                _upload_log(minio_client, log_path, log_buf.getvalue())
            except Exception as e:
                logger.warning("Log upload failed: %s", e)
                log_path = None

            if trans_uuid:
                finish_transaction(
                    db, trans_uuid, status_val,
                    error_message=error_message,
                    log_file_path=log_path,
                )


def run_silver_only(source_uuid: UUID, reindex: bool = True) -> dict:
    """Re-run the silver stage for a source using its most recent bronze file.

    Mirrors `run_course_fetch`'s orchestration (advisory lock, transaction
    ledger, log capture) but skips bronze. The new transaction row's
    `bronze_file_path` is set to the reused MinIO path so the ledger tells you
    exactly which file silver ran on.

    When `reindex` is True (default), the gold stage runs after a successful
    silver, upserting the produced courses into Meilisearch.

    Returns {status, source_uuid, provider_uuid, source_version_uuid,
    bronze_file_path, course_count, reindex_uploaded, reindex_failed, error}.
    """
    logger.info(
        "course_fetch (silver-only): source=%s reindex=%s", source_uuid, reindex,
    )
    minio_client = get_minio_client()

    result: dict = {
        "status": "failed",
        "source_uuid": str(source_uuid),
        "provider_uuid": None,
        "source_version_uuid": None,
        "bronze_file_path": None,
        "course_count": 0,
        "reindex_uploaded": 0,
        "reindex_failed": 0,
        "error": None,
    }

    with SessionLocal() as db, requests.Session() as http:
        message = latest_bronze_for_source(db, source_uuid)
        if not message:
            result["error"] = "no bronze file on record for this source"
            logger.warning("course_fetch (silver-only): %s", result["error"])
            return result

        provider_uuid = UUID(message["provider_uuid"])
        source_version_uuid = UUID(message["source_version_uuid"])
        result["provider_uuid"] = message["provider_uuid"]
        result["source_version_uuid"] = message["source_version_uuid"]
        result["bronze_file_path"] = message["file_path"]

        with advisory_lock(db, NS_COURSE_FETCH, str(source_uuid)) as acquired:
            if not acquired:
                result["status"] = "busy"
                result["error"] = "source already being processed"
                logger.warning(
                    "course_fetch (silver-only): source %s busy — skipping", source_uuid,
                )
                return result

            started = start_transaction(db, provider_uuid, source_version_uuid, source_uuid)
            trans_uuid = started[0] if started else None

            file_path_stem = _build_file_path_stem(
                provider_uuid, source_version_uuid, source_uuid
            )
            log_path = f"{file_path_stem}_log.txt"
            status_val = "failed"
            error_message = None

            with _capture_logs() as log_buf:
                try:
                    if trans_uuid:
                        update_transaction(
                            db, trans_uuid, bronze_file_path=message["file_path"],
                        )

                    courses = enrich_silver(db, minio_client, http, message)
                    if courses is None:
                        raise RuntimeError("silver returned no result")

                    result["course_count"] = len(courses)
                    if trans_uuid:
                        update_transaction(db, trans_uuid, course_count=len(courses))

                    if reindex:
                        uploaded, failed = index_gold(http, courses)
                        result["reindex_uploaded"] = uploaded
                        result["reindex_failed"] = failed

                    status_val = "success"
                except Exception as e:
                    error_message = f"{type(e).__name__}: {e}"
                    logger.exception("course_fetch (silver-only) failed: %s", e)

            try:
                _upload_log(minio_client, log_path, log_buf.getvalue())
            except Exception as e:
                logger.warning("Log upload failed: %s", e)
                log_path = None

            if trans_uuid:
                finish_transaction(
                    db, trans_uuid, status_val,
                    error_message=error_message,
                    log_file_path=log_path,
                )

            result["status"] = status_val
            result["error"] = error_message

    return result
