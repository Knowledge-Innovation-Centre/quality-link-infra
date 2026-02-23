from typing import Dict, Any, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import get_db

router = APIRouter(tags=["Providers"])


@router.get("/get_all_providers", status_code=status.HTTP_200_OK)
async def get_all_providers(
    search_provider: Optional[str] = Query(None, title="Search Provider"),
    page: int = Query(1, ge=1, title="Page Number"),
    page_size: int = Query(10, ge=1, le=100, title="Page Size"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        offset = (page - 1) * page_size
        params: dict = {}
        where_clause = ""

        if search_provider:
            where_clause = "WHERE name_concat ILIKE :search_term OR eter_id LIKE :search_id OR deqar_id LIKE :search_id"
            params["search_term"] = f"%{search_provider.lower()}%"
            params["search_id"] = f"{search_provider.upper()}%"

        total_records = db.execute(text(f"SELECT COUNT(*) FROM provider {where_clause}"), params).scalar()
        total_pages = (total_records + page_size - 1) // page_size

        params["limit"] = page_size
        params["offset"] = offset

        rows = db.execute(
            text(f"""
                SELECT provider_uuid, deqar_id, eter_id, provider_name
                FROM provider
                {where_clause}
                ORDER BY eter_id
                LIMIT :limit OFFSET :offset
            """),
            params,
        ).fetchall()

        return {
            "response": [
                {
                    "provider_uuid": str(row[0]),
                    "deqar_id": row[1],
                    "eter_id": row[2],
                    "provider_name": row[3],
                }
                for row in rows
            ],
            "total": total_records,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

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
        provider_result = db.execute(
            text("""
                SELECT provider_uuid, deqar_id, eter_id, metadata, manifest_json, name_concat,
                       provider_name, last_deqar_pull, last_manifest_pull, created_at, updated_at
                FROM provider
                WHERE provider_uuid = :provider_uuid
            """),
            {"provider_uuid": provider_uuid},
        ).fetchone()

        if not provider_result:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Provider not found")

        source_version_result = db.execute(
            text("""
                SELECT source_version_uuid, provider_uuid, version_date, version_id,
                       source_json, source_uuid_json, created_at, updated_at
                FROM source_version
                WHERE provider_uuid = :provider_uuid
                ORDER BY version_date DESC, version_id DESC
                LIMIT 1
            """),
            {"provider_uuid": provider_uuid},
        ).fetchone()

        response: Dict[str, Any] = {
            "provider": {
                "provider_uuid": str(provider_result[0]),
                "deqar_id": provider_result[1],
                "eter_id": provider_result[2],
                "metadata": provider_result[3],
                "manifest_json": provider_result[4],
                "name_concat": provider_result[5],
                "provider_name": provider_result[6],
                "last_deqar_pull": provider_result[7].isoformat() if provider_result[7] else None,
                "last_manifest_pull": provider_result[8].isoformat() if provider_result[8] else None,
                "created_at": provider_result[9].isoformat() if provider_result[9] else None,
                "updated_at": provider_result[10].isoformat() if provider_result[10] else None,
            },
            "source_version": None,
            "sources": [],
        }

        if source_version_result:
            source_version_uuid = source_version_result[0]
            response["source_version"] = {
                "source_version_uuid": str(source_version_uuid),
                "provider_uuid": str(source_version_result[1]),
                "version_date": source_version_result[2].isoformat() if source_version_result[2] else None,
                "version_id": source_version_result[3],
                "source_json": source_version_result[4],
                "source_uuid_json": source_version_result[5],
                "created_at": source_version_result[6].isoformat() if source_version_result[6] else None,
                "updated_at": source_version_result[7].isoformat() if source_version_result[7] else None,
            }

            sources_result = db.execute(
                text("""
                    SELECT source_uuid, source_version_uuid, source_path, source_type,
                           source_version, created_at, updated_at, source_name
                    FROM source
                    WHERE source_version_uuid = :source_version_uuid
                """),
                {"source_version_uuid": source_version_uuid},
            ).fetchall()

            for source in sources_result:
                response["sources"].append({
                    "source_uuid": str(source[0]),
                    "source_version_uuid": str(source[1]),
                    "source_name": source[7],
                    "source_path": source[2],
                    "source_type": source[3],
                    "source_version": source[4],
                    "created_at": source[5].isoformat() if source[5] else None,
                    "updated_at": source[6].isoformat() if source[6] else None,
                })

        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve provider: {str(e)}",
        )
