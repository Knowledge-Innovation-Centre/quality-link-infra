import io
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse, StreamingResponse
from minio.error import S3Error
from sqlalchemy import text
from sqlalchemy.orm import Session

from config import MINIO_BUCKET_NAME
from database import get_db
from dependencies import get_minio_client
from services.datalake import queue_provider_data as queue_provider_data_service

router = APIRouter(tags=["Datalake"])


@router.get("/list_datalake_files_v2", status_code=status.HTTP_200_OK)
async def list_datalake_files_v2(
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    source_version_uuid: UUID = Query(..., title="Source Version UUID"),
    source_uuid: UUID = Query(..., title="Source UUID"),
    date: Optional[str] = Query(None, title="Date in YYYY-MM-DD format", regex=r"^\d{4}-\d{2}-\d{2}$|^$"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        today_date = datetime.now().strftime("%Y-%m-%d")

        source_result = db.execute(
            text("""
                SELECT last_file_pushed, last_file_pushed_date, last_file_pushed_path
                FROM source
                WHERE source_uuid = :source_uuid
            """),
            {"source_uuid": str(source_uuid)},
        ).fetchone()

        source_info = {
            "last_file_pushed": source_result[0] if source_result else None,
            "last_file_pushed_date": source_result[1].isoformat() if source_result and source_result[1] else None,
            "last_file_pushed_path": source_result[2] if source_result else None,
        }

        date_source = "provided"

        if date:
            try:
                date_folder = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid date format. Please use YYYY-MM-DD format.",
                )
        else:
            latest_row = db.execute(
                text("""
                    SELECT MAX(created_at_date)
                    FROM transaction
                    WHERE provider_uuid = :p
                      AND source_version_uuid = :v
                      AND source_uuid = :s
                """),
                {"p": str(provider_uuid), "v": str(source_version_uuid), "s": str(source_uuid)},
            ).scalar()
            if latest_row:
                date_folder = latest_row.isoformat()
                date = date_folder
                date_source = "transaction"
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No transactions recorded for this source yet.",
                )

        params_summary = {
            "provider_uuid": str(provider_uuid),
            "source_version_uuid": str(source_version_uuid),
            "source_uuid": str(source_uuid),
            "date": date,
            "date_source": date_source,
        }

        tx_rows = db.execute(
            text("""
                SELECT trans_uuid, run_number, status, started_at, finished_at,
                       bronze_file_path, log_file_path, course_count, error_message
                FROM transaction
                WHERE provider_uuid = :p
                  AND source_version_uuid = :v
                  AND source_uuid = :s
                  AND created_at_date = :d
                ORDER BY run_number ASC
            """),
            {
                "p": str(provider_uuid),
                "v": str(source_version_uuid),
                "s": str(source_uuid),
                "d": date_folder,
            },
        ).fetchall()

        file_list = []
        for tx in tx_rows:
            full_path = tx[5]
            last_modified = tx[4] or tx[3]
            file_list.append({
                "full_path": full_path,
                "filename": full_path.split("/")[-1] if full_path else None,
                "last_modified": last_modified.isoformat() if last_modified else None,
                "push_status": False,
                "trans_uuid": str(tx[0]),
                "run_number": tx[1],
                "status": tx[2],
                "started_at": tx[3].isoformat() if tx[3] else None,
                "finished_at": tx[4].isoformat() if tx[4] else None,
                "log_file_path": tx[6],
                "course_count": tx[7],
                "error_message": tx[8],
            })

        if not file_list:
            return {
                "status": "success",
                "message": "No files found for the specified parameters",
                "params": params_summary,
                **source_info,
                "files": [],
                "count": 0,
            }

        if date_folder == today_date:
            file_list[-1]["push_status"] = True

        return {
            "status": "success",
            "message": "Files retrieved successfully",
            "params": params_summary,
            "files": file_list,
            **source_info,
            "count": len(file_list),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list datalake files: {str(e)}",
        )


@router.get("/list_datalake_dates", status_code=status.HTTP_200_OK)
async def list_datalake_dates(
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    source_version_uuid: UUID = Query(..., title="Source Version UUID"),
    source_uuid: UUID = Query(..., title="Source UUID"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        params_summary = {
            "provider_uuid": str(provider_uuid),
            "source_version_uuid": str(source_version_uuid),
            "source_uuid": str(source_uuid),
        }

        rows = db.execute(
            text("""
                SELECT DISTINCT created_at_date
                FROM transaction
                WHERE provider_uuid = :p
                  AND source_version_uuid = :v
                  AND source_uuid = :s
                ORDER BY created_at_date DESC
            """),
            {"p": str(provider_uuid), "v": str(source_version_uuid), "s": str(source_uuid)},
        ).fetchall()

        if not rows:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No data pulled for this source yet.",
            )

        sorted_dates = [r[0].isoformat() for r in rows]
        latest_date = sorted_dates[0]

        return {
            "status": "success",
            "message": "Dates retrieved successfully",
            "params": params_summary,
            "dates": sorted_dates,
            "latest_date": latest_date,
            "count": len(sorted_dates),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list datalake dates: {str(e)}",
        )


@router.get("/download_datalake_file")
async def download_datalake_file(
    file_path: str = Query(..., title="Full path of file to download"),
    preview: bool = Query(False, title="If true, preview the file instead of downloading"),
    db: Session = Depends(get_db),
) -> Any:
    try:
        try:
            minio_client = get_minio_client()
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to connect to MinIO: {str(e)}",
            )

        file_name = file_path.split("/")[-1]

        content_type_map = {
            ".rdf": "application/rdf+xml",
            ".json": "application/json",
            ".xml": "application/xml",
            ".ttl": "text/turtle",
            ".txt": "text/plain",
        }
        content_type = next(
            (ct for ext, ct in content_type_map.items() if file_name.endswith(ext)),
            "application/octet-stream",
        )

        try:
            response = minio_client.get_object(MINIO_BUCKET_NAME, file_path)
            file_stream = io.BytesIO(response.read())
            disposition = "inline" if preview else "attachment"
            return StreamingResponse(
                file_stream,
                media_type=content_type,
                headers={
                    "Content-Type": content_type,
                    "Content-Disposition": f"{disposition}; filename={file_name}",
                },
            )
        except S3Error as e:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File not found or access denied: {str(e)}",
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to download file: {str(e)}",
        )


@router.post("/queue_provider_data", status_code=status.HTTP_202_ACCEPTED)
async def queue_provider_data(
    background_tasks: BackgroundTasks,
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    source_version_uuid: UUID = Query(..., title="Source Version UUID"),
    source_uuid: UUID = Query(..., title="Source UUID"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    result = queue_provider_data_service(
        db, provider_uuid, source_version_uuid, source_uuid,
        background_tasks=background_tasks,
    )
    if result.get("status") == "busy":
        return JSONResponse(status_code=423, content=result)
    if result.get("status") == "outdated":
        return JSONResponse(status_code=410, content=result)
    return result
