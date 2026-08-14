from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, HTTPException, Request, status
from fastapi.responses import JSONResponse

from services.search import search as search_service

router = APIRouter(tags=["Search"])


@router.post("/search")
async def search_post(body: Optional[Dict[str, Any]] = Body(default=None)):
    """Proxy a Meilisearch search request (POST form) against the configured index."""
    try:
        status_code, payload = search_service(body=body or {})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}",
        )

    return JSONResponse(status_code=status_code, content=payload)


@router.get("/search")
async def search_get(request: Request):
    """Proxy a Meilisearch search request (GET form) against the configured index."""
    try:
        status_code, payload = search_service(params=dict(request.query_params))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}",
        )

    return JSONResponse(status_code=status_code, content=payload)
