import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "backend")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

BACKEND_REDIS_URL = os.getenv("BACKEND_REDIS_URL", "redis://localhost:6379/1")

MINIO_ENDPOINT = os.getenv("MINIO_HOST", "minio:9000")
MINIO_KEY = os.getenv("MINIO_KEY", "quality_link")
MINIO_SECRET = os.getenv("MINIO_SECRET", "quality_link_password")
BUCKET_NAME = os.getenv("BUCKET_NAME", "bucket_name")

SERVICE_URL_FRONTEND = os.getenv("SERVICE_URL_FRONTEND", "https://dashboard.app.quality-link.eu")
