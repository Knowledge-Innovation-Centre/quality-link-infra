import io
import json
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse, StreamingResponse
from minio.error import S3Error
from sqlalchemy import text
from sqlalchemy.orm import Session

from config import BUCKET_NAME
from database import get_db
from dependencies import get_minio_client, redis_client
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

        try:
            minio_client = get_minio_client()
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to connect to MinIO: {str(e)}",
            )

        date_folder = None
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
            manifest_path = f"datalake/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}/source_manifest.json"
            try:
                data = minio_client.get_object(BUCKET_NAME, manifest_path)
                manifest = json.loads(data.read().decode("utf-8"))
                if "latest_date" in manifest and manifest["latest_date"]:
                    date_folder = manifest["latest_date"]
                    date = date_folder
                    date_source = "manifest"
                else:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="No latest date found in datalake manifest file",
                    )
            except S3Error as e:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Failed to retrieve datalake manifest file: {str(e)}",
                )
            except json.JSONDecodeError as e:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to parse datalake manifest file: {str(e)}",
                )

        if not date_folder:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No date could be determined.",
            )

        prefix = f"datalake/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}/{date_folder}/"
        params_summary = {
            "provider_uuid": str(provider_uuid),
            "source_version_uuid": str(source_version_uuid),
            "source_uuid": str(source_uuid),
            "date": date,
            "date_source": date_source,
        }

        try:
            objects = minio_client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True)
            file_list = [
                {
                    "full_path": obj.object_name,
                    "filename": obj.object_name.split("/")[-1],
                    "size": obj.size,
                    "last_modified": obj.last_modified.isoformat(),
                    "push_status": False,
                }
                for obj in objects
            ]

            if not file_list:
                return {
                    "status": "success",
                    "message": "No files found for the specified parameters",
                    "params": params_summary,
                    **source_info,
                    "files": [],
                    "count": 0,
                }

            sorted_files = sorted(file_list, key=lambda x: x["last_modified"])
            if date_folder == today_date and sorted_files:
                sorted_files[-1]["push_status"] = True

            return {
                "status": "success",
                "message": "Files retrieved successfully",
                "params": params_summary,
                "files": sorted_files,
                **source_info,
                "count": len(sorted_files),
            }

        except S3Error as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"MinIO error when listing files: {str(e)}",
            )

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
        try:
            minio_client = get_minio_client()
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to connect to MinIO: {str(e)}",
            )

        manifest_path = f"datalake/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}/source_manifest.json"
        params_summary = {
            "provider_uuid": str(provider_uuid),
            "source_version_uuid": str(source_version_uuid),
            "source_uuid": str(source_uuid),
        }

        try:
            data = minio_client.get_object(BUCKET_NAME, manifest_path)
            manifest = json.loads(data.read().decode("utf-8"))

            if "dates" in manifest and isinstance(manifest["dates"], list):
                dates = manifest["dates"]
            elif "latest_date" in manifest and manifest["latest_date"]:
                dates = [manifest["latest_date"]]
            else:
                return {
                    "status": "success",
                    "message": "No dates found in manifest file",
                    "params": params_summary,
                    "dates": [],
                    "latest_date": None,
                    "count": 0,
                }

            latest_date = manifest.get("latest_date") or (sorted(dates, reverse=True)[0] if dates else None)
            sorted_dates = sorted(dates, reverse=True)

            return {
                "status": "success",
                "message": "Dates retrieved successfully",
                "params": params_summary,
                "dates": sorted_dates,
                "latest_date": latest_date,
                "count": len(sorted_dates),
            }

        except S3Error as e:
            if e.code == "NoSuchKey":
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No data pulled for this source yet.")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve list of datalake files: {str(e)}",
            )
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to parse datalake manifest file: {str(e)}",
            )

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
        }
        content_type = next(
            (ct for ext, ct in content_type_map.items() if file_name.endswith(ext)),
            "application/octet-stream",
        )

        try:
            response = minio_client.get_object(BUCKET_NAME, file_path)
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
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    source_version_uuid: UUID = Query(..., title="Source Version UUID"),
    source_uuid: UUID = Query(..., title="Source UUID"),
    source_path: str = Query(..., title="Source Path"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    result = queue_provider_data_service(
        db, redis_client, provider_uuid, source_version_uuid, source_uuid, source_path
    )
    if result.get("status") == "busy":
        return JSONResponse(status_code=423, content=result)
    if result.get("status") == "outdated":
        return JSONResponse(status_code=410, content=result)
    return result
