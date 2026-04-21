import redis
from minio import Minio

from config import (
    BACKEND_REDIS_URL,
    MINIO_HOST,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
)

redis_client = redis.from_url(BACKEND_REDIS_URL)


def get_minio_client() -> Minio:
    return Minio(MINIO_HOST, access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)
