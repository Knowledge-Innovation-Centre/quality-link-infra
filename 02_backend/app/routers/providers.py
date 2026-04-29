from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from database import get_db
from services.providers import (
    get_provider as get_provider_service,
    list_providers as list_providers_service,
)

router = APIRouter(tags=["Providers"])


@router.get("/get_all_providers", status_code=status.HTTP_200_OK)
async def get_all_providers(
    search_provider: Optional[str] = Query(None, title="Search Provider"),
    with_data: bool = Query(False, title="Only providers with a manifest and data sources"),
    page: int = Query(1, ge=1, title="Page Number"),
    page_size: int = Query(10, ge=1, le=100, title="Page Size"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return list_providers_service(
            db, search=search_provider, with_data=with_data, page=page, page_size=page_size
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve providers: {str(e)}",
        )


@router.get("/get_provider", status_code=status.HTTP_200_OK)
async def get_provider(
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_provider_service(db, provider_uuid)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve provider: {str(e)}",
        )
