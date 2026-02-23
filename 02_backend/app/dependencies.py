import redis
from minio import Minio

from config import BACKEND_REDIS_URL, MINIO_ENDPOINT, MINIO_KEY, MINIO_SECRET

redis_client = redis.from_url(BACKEND_REDIS_URL)


def get_minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_KEY, secret_key=MINIO_SECRET, secure=False)
