import logging
from typing import Any, Dict, Optional

import requests

from config import (
    MEILISEARCH_API_KEY,
    MEILISEARCH_INDEX,
    MEILISEARCH_URL,
)
from services import fuseki
from services.courses import (
    frame_course,
    resolve_course_uri,
)

logger = logging.getLogger(__name__)


def _meili_url() -> str:
    return f"{MEILISEARCH_URL}/indexes/{MEILISEARCH_INDEX}/documents"


def _meili_headers() -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if MEILISEARCH_API_KEY:
        headers["Authorization"] = f"Bearer {MEILISEARCH_API_KEY}"
    return headers


def index_gold(session: requests.Session, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    provider_uuid = message.get("provider_uuid")
    source_version_uuid = message.get("source_version_uuid")
    source_type = message.get("source_type", "unknown")
    course_uuids = message.get("course_uuids") or []

    if not course_uuids:
        logger.info("Gold: nothing to index for %s", provider_uuid)
        return {
            "provider_uuid": provider_uuid,
            "source_version_uuid": source_version_uuid,
            "source_type": source_type,
        }

    uploaded = 0
    failed = 0

    for course_uuid in course_uuids:

        course_uri = resolve_course_uri(course_uuid)

        try:
            framed = frame_course(course_uri)
        except Exception as e:
            logger.warning("Framing failed for %s: %s", course_uuid, e)
            failed += 1
            continue

        framed.pop("@context", None)
        framed["id"] = course_uuid

        try:
            r = session.post(_meili_url(), headers=_meili_headers(), json=framed, timeout=30)
            r.raise_for_status()
            uploaded += 1
        except Exception as e:
            logger.warning("Meilisearch upload failed for %s: %s", course_uuid, e)
            failed += 1

    logger.info("Gold: uploaded=%s failed=%s", uploaded, failed)
    return {
        "provider_uuid": provider_uuid,
        "source_version_uuid": source_version_uuid,
        "source_type": source_type,
    }
