from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session


def list_providers(
    db: Session,
    search: Optional[str] = None,
    with_data: bool = False,
    page: int = 1,
    page_size: int = 10,
) -> Dict[str, Any]:
    clauses = []
    params: dict = {}

    if search:
        clauses.append(
            "(name_concat ILIKE :search_term OR eter_id LIKE :search_id OR deqar_id LIKE :search_id)"
        )
        params["search_term"] = f"%{search.lower()}%"
        params["search_id"] = f"{search.upper()}%"

    if with_data:
        clauses.append(
            "EXISTS (SELECT 1 FROM source_version sv "
            "JOIN source s ON s.source_version_uuid = sv.source_version_uuid "
            "WHERE sv.provider_uuid = provider.provider_uuid)"
        )

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    total_records = db.execute(
        text(f"SELECT COUNT(*) FROM provider {where_clause}"), params
    ).scalar() or 0
    total_pages = (total_records + page_size - 1) // page_size

    params["limit"] = page_size
    params["offset"] = (page - 1) * page_size

    rows = db.execute(
        text(f"""
            SELECT provider_uuid, deqar_id, eter_id, provider_name,
                   last_manifest_pull,
                   EXISTS (SELECT 1 FROM source_version sv
                           JOIN source s ON s.source_version_uuid = sv.source_version_uuid
                           WHERE sv.provider_uuid = provider.provider_uuid) AS has_sources
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
                "last_manifest_pull": row[4].isoformat() if row[4] else None,
                "has_sources": row[5],
            }
            for row in rows
        ],
        "total": total_records,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


def get_provider(db: Session, provider_uuid: UUID) -> Dict[str, Any]:
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
                   created_at, updated_at
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
            "version_date": source_version_result[2].isoformat() if source_version_result[2] else None,
            "version_id": source_version_result[3],
            "created_at": source_version_result[4].isoformat() if source_version_result[4] else None,
            "updated_at": source_version_result[5].isoformat() if source_version_result[5] else None,
        }

        sources_result = db.execute(
            text("""
                SELECT source_uuid, source_version_uuid, source_path, source_type,
                       source_version, created_at, updated_at, source_name,
                       last_file_pushed_date, source_id
                FROM source
                WHERE source_version_uuid = :source_version_uuid
            """),
            {"source_version_uuid": source_version_uuid},
        ).fetchall()

        for source in sources_result:
            response["sources"].append({
                "source_uuid": str(source[0]),
                "source_name": source[7],
                "source_path": source[2],
                "source_type": source[3],
                "source_version": source[4],
                "created_at": source[5].isoformat() if source[5] else None,
                "updated_at": source[6].isoformat() if source[6] else None,
                "last_file_pushed_date": source[8].isoformat() if source[8] else None,
                "source_id": source[9],
            })

    return response


def resolve_provider_uuid(db: Session, value: str) -> UUID:
    """Accept a UUID string or an ETER/DEQAR id and return the provider UUID."""
    rows = db.execute(
        text("""
            SELECT provider_uuid
            FROM provider
            WHERE eter_id = :v OR deqar_id = :v OR provider_uuid::text = :v
            LIMIT 2
        """),
        {"v": value},
    ).fetchall()

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No provider found for identifier '{value}'",
        )
    if len(rows) > 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Identifier '{value}' is ambiguous — pass the provider UUID",
        )
    return rows[0][0]
