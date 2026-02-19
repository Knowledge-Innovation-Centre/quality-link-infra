import uuid as uuid_lib
from typing import Dict, Any
from uuid import UUID

import redis
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import get_db
from dependencies import redis_client
from services.manifest import pull_manifest, safe_release_lock

router = APIRouter(tags=["Providers"])


@router.post("/pull_manifest_v2")
async def pull_manifest_v2(
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    lock_key = f"pull_manifest:{provider_uuid}"
    lock_uuid = str(uuid_lib.uuid4())

    try:
        acquired = redis_client.set(lock_key, lock_uuid, ex=60, nx=True)
    except redis.RedisError as redis_err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Redis error: {str(redis_err)}",
        )

    if not acquired:
        return JSONResponse(
            status_code=423,
            content={
                "status": "busy",
                "message": "This provider is currently being processed. Please try again later.",
                "provider_uuid": str(provider_uuid),
            },
        )

    try:
        result = db.execute(
            text("SELECT metadata FROM provider WHERE provider_uuid = :provider_uuid"),
            {"provider_uuid": provider_uuid},
        ).fetchone()

        if not result:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found")

        return pull_manifest(provider_uuid, result[0], db)

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error: {str(e)}",
        )
    finally:
        background_tasks.add_task(safe_release_lock, redis_client, lock_key, lock_uuid)
