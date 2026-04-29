from minio import Minio

from config import MINIO_HOST, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD


def get_minio_client() -> Minio:
    return Minio(MINIO_HOST, access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)
