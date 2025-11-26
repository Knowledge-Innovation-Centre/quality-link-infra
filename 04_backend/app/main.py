from fastapi import APIRouter, Depends, HTTPException, status, Query, FastAPI, Response
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
import os
from typing import Dict, Any, Tuple, Optional, List
from uuid import UUID
import requests
from bs4 import BeautifulSoup
import json
from dotenv import load_dotenv
import uuid
import redis
from fastapi import BackgroundTasks
from fastapi.responses import JSONResponse
from datetime import date, datetime
from minio import Minio
from minio.error import S3Error
import re
import io
import dns.resolver
import yaml
from urllib.parse import urlparse

load_dotenv()

app = FastAPI(title="QL-Backend")

origins = [
    "http://localhost:3000",  
    "http://frontend:3000",   
    "https://r0ggc40go8gckosso48osksk.serverfarm.knowledgeinnovation.eu",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "backend")
BACKEND_REDIS_URL = os.getenv("BACKEND_REDIS_URL", "redis://localhost:6379/1")
MINIO_ENDPOINT = os.getenv("MINIO_HOST", "minio:9000")
MINIO_KEY = os.getenv("MINIO_KEY", "quality_link")
MINIO_SECRET = os.getenv("MINIO_SECRET", "quality_link_password")
BUCKET_NAME = os.getenv("BUCKET_NAME", "bucket_name")


DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
redis_client = redis.from_url(BACKEND_REDIS_URL)


engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/health/database")
async def check_database_connection():
    """
    Check if database connection is working properly.
    """
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            result.fetchone()
        
        return {
            "status": "success",
            "message": "Database connection is healthy",
            "database": DB_USER
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Database connection failed: {str(e)}",
            "database": DB_USER
        }


@app.get("/")
async def root():
    return {"message": "Database connection testing API is running. Use /health/database to test connection."}


@app.get("/get_all_providers", tags=["Providers"], status_code=status.HTTP_200_OK)
async def get_all_providers(
    search_provider: Optional[str] = Query(None, title="Search Provider"),
    page: int = Query(1, ge=1, title="Page Number"),
    page_size: int = Query(10, ge=1, le=100, title="Page Size"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        offset = (page - 1) * page_size

        params = {}
        where_clause = ""
        
        if search_provider:
            where_clause = "WHERE provider_name ILIKE :search_term"
            params["search_term"] = f"%{search_provider.lower()}%"
        
        count_query = text(f"SELECT COUNT(*) FROM provider {where_clause}")
        
        result = db.execute(count_query, params)
        total_records = result.scalar()
        
        total_pages = (total_records + page_size - 1) // page_size
        
        final_query = text(f"""
            SELECT provider_uuid, deqar_id, eter_id, provider_name
            FROM provider
            {where_clause}
            LIMIT :limit OFFSET :offset
        """)
        
        params["limit"] = page_size
        params["offset"] = offset
        
        rows = db.execute(final_query, params).fetchall()
        providers = []
        
        for row in rows:
            providers.append({
                "provider_uuid": str(row[0]),  
                "deqar_id": row[1],        
                "eter_id": row[2],
                "provider_name": row[3],
            })
        
        response = {
            "response": providers,
            "total": total_records,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages
        }
        
        return response
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Failed to retrieve providers: {str(e)}"
        )
    

@app.get("/get_provider", tags=["Providers"], status_code=status.HTTP_200_OK)
async def get_provider(
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        provider_query = text("""
            SELECT provider_uuid, deqar_id, eter_id, metadata, manifest_json, name_concat, 
                   provider_name, last_deqar_pull, last_manifest_pull, created_at, updated_at
            FROM provider
            WHERE provider_uuid = :provider_uuid
        """)
        
        provider_result = db.execute(provider_query, {"provider_uuid": provider_uuid}).fetchone()
        
        if not provider_result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Provider not found"
            )
        
        source_version_query = text("""
            SELECT source_version_uuid, provider_uuid, version_date, version_id, 
                   source_json, source_uuid_json, created_at, updated_at
            FROM source_version
            WHERE provider_uuid = :provider_uuid
            ORDER BY version_date DESC, version_id DESC
            LIMIT 1
        """)
        
        source_version_result = db.execute(source_version_query, {"provider_uuid": provider_uuid}).fetchone()
        
        response = {
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
                "updated_at": provider_result[10].isoformat() if provider_result[10] else None
            },
            "source_version": None,
            "sources": []
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
                "updated_at": source_version_result[7].isoformat() if source_version_result[7] else None
            }
            
            sources_query = text("""
                SELECT source_uuid, source_version_uuid, source_path, source_type, 
                       source_version, created_at, updated_at, source_name
                FROM source
                WHERE source_version_uuid = :source_version_uuid
            """)
            
            sources_result = db.execute(sources_query, {"source_version_uuid": source_version_uuid}).fetchall()
            
            for source in sources_result:
                response["sources"].append({
                    "source_uuid": str(source[0]),
                    "source_version_uuid": str(source[1]),
                    "source_name": source[7],
                    "source_path": source[2],
                    "source_type": source[3],
                    "source_version": source[4],
                    "created_at": source[5].isoformat() if source[5] else None,
                    "updated_at": source[6].isoformat() if source[6] else None
                })
        
        return response
        
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve provider: {str(e)}"
        )


@app.get("/list_datalake_files_v2", tags=["Datalake"], status_code=status.HTTP_200_OK)
async def list_datalake_files_v2(
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    source_version_uuid: UUID = Query(..., title="Source Version UUID"),
    source_uuid: UUID = Query(..., title="Source UUID"),
    date: Optional[str] = Query(None, title="Date in YYYY-MM-DD format", regex=r"^\d{4}-\d{2}-\d{2}$|^$"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        today_date = datetime.now().strftime("%Y-%m-%d")
        
        source_query = text("""
            SELECT last_file_pushed, last_file_pushed_date, last_file_pushed_path
            FROM source
            WHERE source_uuid = :source_uuid
        """)
        
        source_result = db.execute(source_query, {"source_uuid": str(source_uuid)}).fetchone()
        
        source_info = {
            "last_file_pushed": None,
            "last_file_pushed_date": None,
            "last_file_pushed_path": None
        }
        
        if source_result:
            source_info["last_file_pushed"] = source_result[0]
            source_info["last_file_pushed_date"] = source_result[1].isoformat() if source_result[1] else None
            source_info["last_file_pushed_path"] = source_result[2]
        
        minio_client = None
        try:
            minio_client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_KEY,
                secret_key=MINIO_SECRET,
                secure=False  
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to connect to MinIO: {str(e)}"
            )
        
        date_folder = None
        date_source = "provided"
        
        if date:
            try:
                parsed_date = datetime.strptime(date, "%Y-%m-%d")
                date_folder = parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid date format. Please use YYYY-MM-DD format."
                )
        else:
            manifest_path = f"datalake/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}/source_manifest.json"
            try:
                data = minio_client.get_object(BUCKET_NAME, manifest_path)
                manifest_content = data.read().decode('utf-8')
                manifest = json.loads(manifest_content)
                
                if "latest_date" in manifest and manifest["latest_date"]:
                    date_folder = manifest["latest_date"]
                    date = date_folder  
                    date_source = "manifest"
                else:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="No latest date found in manifest file"
                    )
                    
            except S3Error as e:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Failed to retrieve manifest file: {str(e)}"
                )
            except json.JSONDecodeError as e:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to parse manifest file: {str(e)}"
                )
        
        if not date_folder:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No date could be determined. Please provide a date or ensure manifest contains a latest_date."
            )
        
        is_today = (date_folder == today_date)
        
        prefix = f"datalake/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}/{date_folder}/"
        
        try:
            objects = minio_client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True)
            file_list = []
            
            for obj in objects:
                file_path = obj.object_name
                filename = file_path.split('/')[-1]
                file_list.append({
                    "full_path": file_path,
                    "filename": filename,
                    "size": obj.size,
                    "last_modified": obj.last_modified.isoformat(),
                    "push_status": False  
                })
            
            if not file_list:
                return {
                    "status": "success",
                    "message": "No files found for the specified parameters",
                    "params": {
                        "provider_uuid": str(provider_uuid),
                        "source_version_uuid": str(source_version_uuid),
                        "source_uuid": str(source_uuid),
                        "date": date,
                        "date_source": date_source
                    },
                    "last_file_pushed": source_info["last_file_pushed"],
                    "last_file_pushed_date": source_info["last_file_pushed_date"],
                    "last_file_pushed_path": source_info["last_file_pushed_path"],
                    "files": [],
                    "count": 0
                }
            
            sorted_files = sorted(file_list, key=lambda x: x["last_modified"])
            
            if is_today and sorted_files:
                sorted_files[-1]["push_status"] = True
            
            return {
                "status": "success",
                "message": "Files retrieved successfully",
                "params": {
                    "provider_uuid": str(provider_uuid),
                    "source_version_uuid": str(source_version_uuid),
                    "source_uuid": str(source_uuid),
                    "date": date,
                    "date_source": date_source
                },
                "files": sorted_files,
                "last_file_pushed": source_info["last_file_pushed"],
                "last_file_pushed_date": source_info["last_file_pushed_date"],
                "last_file_pushed_path": source_info["last_file_pushed_path"],
                "count": len(sorted_files)
            }
            
        except S3Error as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"MinIO error when listing files: {str(e)}"
            )
            
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list datalake files: {str(e)}"
        )


@app.get("/download_datalake_file", tags=["Datalake"])
async def download_datalake_file(
    file_path: str = Query(..., title="Full path of file to download"),
    preview: bool = Query(False, title="If true, preview the file instead of downloading"),
    db: Session = Depends(get_db)
) -> Any:
    try:
        minio_client = None
        try:
            minio_client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_KEY,
                secret_key=MINIO_SECRET,
                secure=False  
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to connect to MinIO: {str(e)}"
            )
        
        file_name = file_path.split("/")[-1]
        
        try:
            response = minio_client.get_object(BUCKET_NAME, file_path)
            
            file_data = response.read()
            
            content_type = "application/octet-stream"  
            if file_name.endswith(".rdf"):
                content_type = "application/rdf+xml"
            elif file_name.endswith(".json"):
                content_type = "application/json"
            elif file_name.endswith(".xml"):
                content_type = "application/xml"
            elif file_name.endswith(".ttl"):
                content_type = "text/turtle"
            
            file_stream = io.BytesIO(file_data)
            
            headers = {
                "Content-Type": content_type,
            }
            
            if not preview:
                headers["Content-Disposition"] = f"attachment; filename={file_name}"
            else:
                headers["Content-Disposition"] = f"inline; filename={file_name}"
            
            return StreamingResponse(
                file_stream, 
                media_type=content_type,
                headers=headers
            )
            
        except S3Error as e:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File not found or access denied: {str(e)}"
            )
            
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to download file: {str(e)}"
        )

        
@app.post("/queue_provider_data", tags=["Datalake"], status_code=status.HTTP_202_ACCEPTED)
async def queue_provider_data(
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    source_version_uuid: UUID = Query(..., title="Source Version UUID"),
    source_uuid: UUID = Query(..., title="Source UUID"),
    source_path: str = Query(..., title="Source Path"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    
    lock_uuid = str(uuid.uuid4())
    lock_key_pattern = f"pull_manifest:{provider_uuid}:*"
    lock_key = f"pull_manifest:{provider_uuid}:{lock_uuid}"

    try:
        existing_locks = redis_client.keys(lock_key_pattern)
        
        if existing_locks:
            return JSONResponse(
                status_code=423,
                content={
                    "status": "busy",
                    "message": "This provider is currently being processed. Please try again later.",
                    "provider_uuid": str(provider_uuid)
                }
            )
        
        redis_client.setex(lock_key, 60, "locked")

        requested_version_query = text("""
            SELECT version_date, version_id
            FROM source_version
            WHERE provider_uuid = :provider_uuid
            AND source_version_uuid = :source_version_uuid
        """)

        requested_version = db.execute(requested_version_query, {
            "provider_uuid": provider_uuid,
            "source_version_uuid": source_version_uuid
        }).fetchone()
        
        if not requested_version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Source version not found for the specified provider"
            )
        
        requested_version_date = requested_version[0]
        requested_version_id = requested_version[1]

        latest_version_query = text("""
            SELECT source_version_uuid, version_date, version_id
            FROM source_version
            WHERE provider_uuid = :provider_uuid
            ORDER BY version_date DESC, version_id DESC
            LIMIT 1
        """)

        latest_version = db.execute(latest_version_query, {
            "provider_uuid": provider_uuid
        }).fetchone()
        
        if not latest_version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No source versions found for this provider"
            )
        
        latest_version_uuid = str(latest_version[0])
        latest_version_date = latest_version[1]
        latest_version_id = latest_version[2]


        # if requested_version_date != latest_version_date or requested_version_id != latest_version_id:
        if latest_version_uuid != str(source_version_uuid):
            return JSONResponse(
                status_code=426,
                content={
                    "status": "outdated",
                    "message": "You are using an outdated version for this provider. Please refresh your page to retrieve the latest configurations.",
                    "provider_uuid": str(provider_uuid),
                    "requested_version": {
                        "version_date": requested_version_date.isoformat(),
                        "version_id": requested_version_id
                    },
                    "latest_version": {
                        "version_date": latest_version_date.isoformat(),
                        "version_id": latest_version_id
                    }
                }
            )

        provider_data = {
            "provider_uuid": str(provider_uuid),
            "source_version_uuid": str(source_version_uuid),
            "source_uuid": str(source_uuid),
            "source_path": source_path,
            "queued_at": datetime.utcnow().isoformat(),
            "status": "queued"
        }
        
        queue_name = "provider_data_queue"
        
        try:
            data_json = json.dumps(provider_data)
            
            redis_client.rpush(queue_name, data_json)
            
            return {
                "status": "success",
                "message": "Provider data has been queued for processing",
                "queue": queue_name,
                "data": {
                    "provider_uuid": provider_data["provider_uuid"],
                    "source_version_uuid": provider_data["source_version_uuid"],
                    "source_uuid": provider_data["source_uuid"],
                    "source_path": provider_data["source_path"],
                    "queued_at": provider_data["queued_at"]
                }
            }
            
        except redis.RedisError as redis_err:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Redis error: {str(redis_err)}"
            )
            
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to queue provider data: {str(e)}"
        )
    finally:
        background_tasks.add_task(redis_client.delete, lock_key)


@app.get("/list_datalake_dates", tags=["Datalake"], status_code=status.HTTP_200_OK)
async def list_datalake_dates(
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    source_version_uuid: UUID = Query(..., title="Source Version UUID"),
    source_uuid: UUID = Query(..., title="Source UUID"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        minio_client = None
        try:
            minio_client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_KEY,
                secret_key=MINIO_SECRET,
                secure=False
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to connect to MinIO: {str(e)}"
            )
        
        manifest_path = f"datalake/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}/source_manifest.json"
        
        try:
            data = minio_client.get_object(BUCKET_NAME, manifest_path)
            manifest_content = data.read().decode('utf-8')
            manifest = json.loads(manifest_content)
            
            dates = []
            latest_date = None
            
            if "dates" in manifest and isinstance(manifest["dates"], list):
                dates = manifest["dates"]
            else:
                if "latest_date" in manifest and manifest["latest_date"]:
                    dates = [manifest["latest_date"]]
                else:
                    return {
                        "status": "success",
                        "message": "No dates found in manifest file",
                        "params": {
                            "provider_uuid": str(provider_uuid),
                            "source_version_uuid": str(source_version_uuid),
                            "source_uuid": str(source_uuid)
                        },
                        "dates": [],
                        "latest_date": None,
                        "count": 0
                    }
            
            if "latest_date" in manifest and manifest["latest_date"]:
                latest_date = manifest["latest_date"]
            elif dates:
                latest_date = sorted(dates, reverse=True)[0]
            
            sorted_dates = sorted(dates, reverse=True)
            
            return {
                "status": "success",
                "message": "Dates retrieved successfully",
                "params": {
                    "provider_uuid": str(provider_uuid),
                    "source_version_uuid": str(source_version_uuid),
                    "source_uuid": str(source_uuid)
                },
                "dates": sorted_dates,
                "latest_date": latest_date,
                "count": len(sorted_dates)
            }
                
        except S3Error as e:
            if e.code == "NoSuchKey":
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Manifest file not found"
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve manifest file: {str(e)}"
            )
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to parse manifest file: {str(e)}"
            )
            
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list datalake dates: {str(e)}"
        )


def get_txt_records(domain: str) -> Optional[str]:

    if not domain:
        return None
            
    try:
        answers = dns.resolver.resolve(domain, 'TXT')
        for rdata in answers:
            txt_record = ''.join(rdata.strings[0].decode() if isinstance(rdata.strings[0], bytes) else rdata.strings[0])
            if "m=" in txt_record:
                return txt_record.split("m=")[-1].strip()
        return None
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, Exception):
        return None


def check_well_known(domain: str) -> Tuple[Optional[str], Optional[dict]]:

    if not domain:
        return None, None
        
    base_url = f"https://{domain}"
    
    well_known_paths = [
        "/.well-known/quality-link-manifest",
        "/.well-known/quality-link-manifest.json",
        "/.well-known/quality-link-manifest.yaml"
    ]
    
    for path in well_known_paths:
        full_url = f"{base_url.rstrip('/')}{path}"
        
        try:
            response = requests.get(full_url, timeout=30)
            
            if response.status_code == 200:
                content_type = response.headers.get("content-type", "")
                
                if content_type.startswith("application/json") or path.endswith(".json"):
                    try:
                        return full_url, response.json()
                    except:
                        pass
                
                if content_type.startswith("application/yaml") or content_type.startswith("application/x-yaml") or path.endswith(".yaml"):
                    try:
                        return full_url, yaml.safe_load(response.text)
                    except:
                        pass
                        
                if response.text and len(response.text.strip()) > 0:
                    return full_url, {"raw_path": True, "content_type": content_type}
        
        except Exception:
            continue
    
    return None, None


def validate_manifest_url(url: str) -> Tuple[Optional[str], Optional[dict]]:

    if not url:
        return None, None
            
    try:
        manifest_resp = requests.get(url, timeout=30)
        
        if manifest_resp.status_code == 200:
            content_type = manifest_resp.headers.get("content-type", "")
            
            if content_type.startswith("application/json") or url.endswith(".json"):
                try:
                    return url, manifest_resp.json()
                except:
                    pass
            
            if content_type.startswith("application/yaml") or content_type.startswith("application/x-yaml") or url.endswith(".yaml") or url.endswith(".yml"):
                try:
                    return url, yaml.safe_load(manifest_resp.text)
                except:
                    pass
                    
            if manifest_resp.text and len(manifest_resp.text.strip()) > 0:
                return url, {"raw_path": True, "content_type": content_type}
        
        return None, None
    except Exception:
        return None, None


def prepare_test_combinations(schac_identifier: str, website_link: Optional[str]) -> List[dict]:
    schac_domain = schac_identifier
    
    tested_combinations = set()
    test_combinations = []
    
    if schac_identifier:
        test_combinations.append({"domain": schac_identifier, "type": "DNS", "check": False, "path": None})
        tested_combinations.add((schac_identifier, "DNS"))
        
        test_combinations.append({"domain": schac_identifier, "type": ".well-known", "check": None, "path": None})
        tested_combinations.add((schac_identifier, ".well-known"))
    
    if website_link:
        parsed_url = urlparse(website_link)
        
        if not parsed_url.scheme:
            website_link = f"https://{website_link}"
            parsed_url = urlparse(website_link)
        
        root_domain = parsed_url.netloc
        
        if (root_domain, "DNS") not in tested_combinations:
            test_combinations.append({"domain": root_domain, "type": "DNS", "check": None, "path": None})
            tested_combinations.add((root_domain, "DNS"))
        
        if (root_domain, ".well-known") not in tested_combinations:
            test_combinations.append({"domain": root_domain, "type": ".well-known", "check": None, "path": None})
            tested_combinations.add((root_domain, ".well-known"))
        
        root_domain_no_www = root_domain.replace("www.", "")
        
        if (root_domain_no_www, "DNS") not in tested_combinations:
            test_combinations.append({"domain": root_domain_no_www, "type": "DNS", "check": None, "path": None})
            tested_combinations.add((root_domain_no_www, "DNS"))
        
        if (root_domain_no_www, ".well-known") not in tested_combinations:
            test_combinations.append({"domain": root_domain_no_www, "type": ".well-known", "check": None, "path": None})
            tested_combinations.add((root_domain_no_www, ".well-known"))
    
    return test_combinations


@app.post("/pull_manifest_v2", tags=["Providers"])
async def pull_manifest_v2(
    provider_uuid: UUID = Query(..., title="Provider UUID"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    lock_uuid = str(uuid.uuid4())
    lock_key_pattern = f"pull_manifest:{provider_uuid}:*"
    lock_key = f"pull_manifest:{provider_uuid}:{lock_uuid}"
    
    try:
        existing_locks = redis_client.keys(lock_key_pattern)
        
        if existing_locks:
            return JSONResponse(
                status_code=423,
                content={
                    "status": "busy",
                    "message": "This provider is currently being processed. Please try again later.",
                    "provider_uuid": str(provider_uuid)
                }
            )
        
        redis_client.setex(lock_key, 60, "locked")
        
        query = text("""
            SELECT metadata
            FROM provider
            WHERE provider_uuid = :provider_uuid
        """)
        result = db.execute(query, {"provider_uuid": provider_uuid}).fetchone()
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Provider not found"
            )
        
        metadata = result[0]
        
        identifiers = metadata.get("identifiers", [])
        schac_identifier = next(
            (item["identifier"] for item in identifiers if item.get("resource") == "SCHAC"),
            None
        )

        website_link = metadata.get("website_link")

        if not schac_identifier and not website_link:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="SCHAC identifier not found in metadata"
            )
                
        test_combinations = prepare_test_combinations(schac_identifier, website_link)
        
        manifest_found = False
        manifest_url = None
        manifest_data = None
        
        for i, test in enumerate(test_combinations):
            if test["domain"] is None or manifest_found:
                test["check"] = None
                continue
                
            if test["type"] == "DNS":
                manifest_path = get_txt_records(test["domain"])
                if manifest_path:
                    manifest_url, manifest_data = validate_manifest_url(manifest_path)
                    if manifest_url:
                        manifest_found = True
                        test["check"] = True
                        test["path"] = manifest_url
                        
                        for j in range(i+1, len(test_combinations)):
                            test_combinations[j]["check"] = None
                    else:
                        test["check"] = False
                else:
                    test["check"] = False
                    
            elif test["type"] == ".well-known":
                manifest_url, manifest_data = check_well_known(test["domain"])
                
                if manifest_url and manifest_data:
                    manifest_found = True
                    test["check"] = True
                    test["path"] = manifest_url
                    
                    for j in range(i+1, len(test_combinations)):
                        test_combinations[j]["check"] = None
                else:
                    test["check"] = False
        
        update_query = text("""
            UPDATE provider
            SET manifest_json = CAST(:manifest_json AS jsonb),
                last_manifest_pull = NOW()
            WHERE provider_uuid = :provider_uuid
        """)
        db.execute(update_query, {
            "manifest_json": json.dumps(test_combinations),
            "provider_uuid": str(provider_uuid)
        })
        
        sources_processed = False
        new_source_version_created = False
        
        if manifest_found and manifest_data and isinstance(manifest_data, dict) and "sources" in manifest_data:
            sources = manifest_data["sources"]
            
            if sources:
                sources_processed = True
                
                latest_version_query = text("""
                    SELECT source_version_uuid, version_date, version_id, source_json
                    FROM source_version
                    WHERE provider_uuid = :provider_uuid
                    ORDER BY version_date DESC, version_id DESC
                    LIMIT 1
                """)
                
                latest_version = db.execute(latest_version_query, {
                    "provider_uuid": provider_uuid
                }).fetchone()
                
                create_new_version = True
                if latest_version:
                    existing_source_json = latest_version[3]
                    
                    if json.dumps(existing_source_json, sort_keys=True) == json.dumps(sources, sort_keys=True):
                        create_new_version = False
                
                if create_new_version:
                    today = date.today().isoformat()
                    
                    version_id = 1
                    if latest_version and latest_version[1].isoformat() == today:
                        version_id = latest_version[2] + 1
                    
                    source_uuid_json = []
                    for source in sources:
                        source_with_uuid = source.copy()
                        source_with_uuid["source_uuid"] = str(uuid.uuid4())
                        source_uuid_json.append(source_with_uuid)
                    
                    insert_version_query = text("""
                        INSERT INTO source_version
                        (provider_uuid, version_date, version_id, source_json, source_uuid_json)
                        VALUES
                        (:provider_uuid, :version_date, :version_id, CAST(:source_json AS jsonb), CAST(:source_uuid_json AS jsonb))
                        RETURNING source_version_uuid
                    """)
                    
                    new_version_result = db.execute(insert_version_query, {
                        "provider_uuid": provider_uuid,
                        "version_date": today,
                        "version_id": version_id,
                        "source_json": json.dumps(sources),
                        "source_uuid_json": json.dumps(source_uuid_json)
                    }).fetchone()
                    
                    source_version_uuid = new_version_result[0]
                    
                    if source_version_uuid and source_uuid_json:
                        source_records = []
                        
                        for source_item in source_uuid_json:
                            source_uuid_val = source_item.get("source_uuid")
                            source_path_val = source_item.get("path")
                            source_type_val = source_item.get("type")
                            source_version_val = source_item.get("version")
                            source_name_val = source_item.get("name", "")
                            
                            if source_uuid_val and source_path_val and source_type_val:
                                source_records.append({
                                    "source_uuid": source_uuid_val,
                                    "source_version_uuid": source_version_uuid,
                                    "source_path": source_path_val,
                                    "source_type": source_type_val,
                                    "source_version": source_version_val,
                                    "source_name": source_name_val
                                })
                        
                        if source_records:
                            insert_sources_query = text("""
                                INSERT INTO source
                                (source_uuid, source_version_uuid, source_path, source_type, source_version, source_name)
                                VALUES
                                (:source_uuid, :source_version_uuid, :source_path, :source_type, :source_version, :source_name)
                            """)
                            
                            db.execute(insert_sources_query, source_records)
                    
                    new_source_version_created = True
        
        db.commit()
        
        response_data = {
            "status": "success",
            "provider_uuid": str(provider_uuid),
            "schac_domain": schac_identifier,
            "website_link": website_link,
            "manifest_url": manifest_url,
            "manifest_found": manifest_found,
            "manifest_json": test_combinations,
            "sources_processed": sources_processed,
            "new_source_version_created": new_source_version_created
        }
        
        return response_data
    except HTTPException as http_exc:
        db.rollback()
        raise http_exc
    except redis.RedisError as redis_err:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Redis error: {str(redis_err)}"
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error: {str(e)}"
        )
    finally:
        background_tasks.add_task(redis_client.delete, lock_key)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)