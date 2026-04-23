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
from services.locks import NS_COURSE_FETCH, release, try_acquire

from .bronze import fetch_bronze
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
        if not try_acquire(db, NS_COURSE_FETCH, str(source_uuid)):
            logger.warning(
                "course_fetch: source %s already being processed — skipping",
                source_uuid,
            )
            return

        try:
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

                    silver = enrich_silver(db, minio_client, http, bronze)
                    if not silver:
                        raise RuntimeError("silver returned no result")
                    if trans_uuid:
                        update_transaction(
                            db, trans_uuid,
                            course_count=len(silver.get("course_uuids") or []),
                        )

                    index_gold(http, silver)
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
        finally:
            try:
                release(db, NS_COURSE_FETCH, str(source_uuid))
            except Exception:
                logger.warning(
                    "Failed to release NS_COURSE_FETCH lock for %s",
                    source_uuid, exc_info=True,
                )
