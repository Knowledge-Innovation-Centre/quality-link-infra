import logging
from uuid import UUID

import requests
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from database import SessionLocal
from dependencies import get_minio_client

from .bronze import fetch_bronze
from .gold import index_gold
from .silver import enrich_silver

logger = logging.getLogger(__name__)


def _log_transaction(db: Session, provider_uuid: str, source_version_uuid: str) -> None:
    try:
        db.execute(
            text("""
                INSERT INTO transaction (provider_uuid, source_version_uuid)
                VALUES (:p, :v)
            """),
            {"p": provider_uuid, "v": source_version_uuid},
        )
        db.commit()
    except IntegrityError:
        db.rollback()
        logger.info(
            "Transaction already logged for provider %s version %s today",
            provider_uuid, source_version_uuid,
        )
    except Exception as e:
        db.rollback()
        logger.warning("Transaction insert failed: %s", e)


def run_course_fetch(
    provider_uuid: UUID,
    source_version_uuid: UUID,
    source_uuid: UUID,
) -> None:
    """Bronze → silver → gold → transaction. Errors are logged, not raised.

    Opens its own SQLAlchemy session — must not reuse a request-scoped session
    since this runs in a FastAPI BackgroundTask after the response has been sent.
    """
    logger.info(
        "course_fetch: provider=%s source=%s version=%s",
        provider_uuid, source_uuid, source_version_uuid,
    )
    minio_client = get_minio_client()

    with SessionLocal() as db, requests.Session() as http:
        try:
            bronze = fetch_bronze(db, minio_client, provider_uuid, source_version_uuid, source_uuid)
            if not bronze:
                return

            silver = enrich_silver(db, minio_client, http, bronze)
            if not silver:
                return

            index_gold(http, silver)
            _log_transaction(db, str(provider_uuid), str(source_version_uuid))
        except Exception as e:
            logger.exception("course_fetch failed: %s", e)
