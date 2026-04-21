from typing import Any, Dict
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from database import get_db
from services.manifest import refresh_manifest_for_provider

router = APIRouter(tags=["Providers"])


@router.post("/pull_manifest_v2")
async def pull_manifest_v2(
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    result = refresh_manifest_for_provider(db, provider_uuid)
    if result.get("status") == "busy":
        return JSONResponse(status_code=423, content=result)
    return result
