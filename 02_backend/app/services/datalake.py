import json
from datetime import datetime
from typing import Any, Dict
from uuid import UUID

import redis
from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session


PROVIDER_DATA_QUEUE = "provider_data_queue"


def queue_provider_data(
    db: Session,
    redis_client: "redis.Redis",
    provider_uuid: UUID,
    source_version_uuid: UUID,
    source_uuid: UUID,
    source_path: str,
) -> Dict[str, Any]:
    """Queue a single source for a fetch.

    Returns {"status": "busy", ...} if the provider's manifest is currently
    being pulled, {"status": "outdated", ...} if the caller's source_version
    is not the latest, otherwise {"status": "success", ...}. Raises
    HTTPException for missing versions and infrastructure errors.
    """
    if redis_client.exists(f"pull_manifest:{provider_uuid}"):
        return {
            "status": "busy",
            "message": "Manifest is currently being pulled for this provider. Please try again later.",
            "provider_uuid": str(provider_uuid),
        }

    requested_version = db.execute(
        text("""
            SELECT version_date, version_id
            FROM source_version
            WHERE provider_uuid = :provider_uuid
              AND source_version_uuid = :source_version_uuid
        """),
        {"provider_uuid": provider_uuid, "source_version_uuid": source_version_uuid},
    ).fetchone()

    if not requested_version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Manifest file version not found for the specified provider",
        )

    latest_version = db.execute(
        text("""
            SELECT source_version_uuid, version_date, version_id
            FROM source_version
            WHERE provider_uuid = :provider_uuid
            ORDER BY version_date DESC, version_id DESC
            LIMIT 1
        """),
        {"provider_uuid": provider_uuid},
    ).fetchone()

    if not latest_version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No manifest file versions found for this provider",
        )

    if str(latest_version[0]) != str(source_version_uuid):
        return {
            "status": "outdated",
            "message": "You are using an outdated manifest file version for this provider. Please refresh your page to retrieve the latest configurations.",
            "provider_uuid": str(provider_uuid),
            "requested_version": {
                "version_date": requested_version[0].isoformat(),
                "version_id": requested_version[1],
            },
            "latest_version": {
                "version_date": latest_version[1].isoformat(),
                "version_id": latest_version[2],
            },
        }

    provider_data = {
        "provider_uuid": str(provider_uuid),
        "source_version_uuid": str(source_version_uuid),
        "source_uuid": str(source_uuid),
        "source_path": source_path,
        "queued_at": datetime.utcnow().isoformat(),
        "status": "queued",
    }

    try:
        redis_client.rpush(PROVIDER_DATA_QUEUE, json.dumps(provider_data))
    except redis.RedisError as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Redis error: {err}",
        )

    return {
        "status": "success",
        "message": "Provider data has been queued for processing",
        "queue": PROVIDER_DATA_QUEUE,
        "data": {
            "provider_uuid": provider_data["provider_uuid"],
            "source_version_uuid": provider_data["source_version_uuid"],
            "source_uuid": provider_data["source_uuid"],
            "queued_at": provider_data["queued_at"],
        },
    }
