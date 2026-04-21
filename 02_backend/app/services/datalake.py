from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID

import redis
from fastapi import BackgroundTasks, HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from services.course_fetch import run_course_fetch


def queue_provider_data(
    db: Session,
    redis_client: "redis.Redis",
    provider_uuid: UUID,
    source_version_uuid: UUID,
    source_uuid: UUID,
    source_path: str,
    *,
    background_tasks: Optional[BackgroundTasks] = None,
) -> Dict[str, Any]:
    """Validate the fetch request and schedule the course-fetch pipeline.

    Returns {"status": "busy", ...} if the provider's manifest is currently
    being pulled, {"status": "outdated", ...} if the caller's source_version
    is not the latest, otherwise {"status": "success", ...}. Raises
    HTTPException for missing versions and infrastructure errors.

    When called with `background_tasks`, the pipeline is scheduled for async
    execution (HTTP use). Without it, the caller is expected to run the
    pipeline in the foreground (CLI use).
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

    queued_at = datetime.now(timezone.utc).isoformat()

    if background_tasks is not None:
        background_tasks.add_task(
            run_course_fetch, provider_uuid, source_version_uuid, source_uuid, source_path
        )

    return {
        "status": "success",
        "message": "Provider data fetch has been dispatched",
        "data": {
            "provider_uuid": str(provider_uuid),
            "source_version_uuid": str(source_version_uuid),
            "source_uuid": str(source_uuid),
            "queued_at": queued_at,
        },
    }
