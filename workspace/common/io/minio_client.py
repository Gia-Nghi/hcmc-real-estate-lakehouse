import json
import os
from io import BytesIO
from minio import Minio
from minio.error import S3Error


def create_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket_name: str) -> None:
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)


def upload_json_bytes(
    client: Minio,
    bucket_name: str,
    object_name: str,
    payload: dict,
) -> None:
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(data),
        length=len(data),
        content_type="application/json",
    )


def upload_jsonl_lines(
    client: Minio,
    bucket_name: str,
    object_name: str,
    records: list[dict],
) -> None:
    lines = "\n".join(json.dumps(r, ensure_ascii=False) for r in records).encode("utf-8")
    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(lines),
        length=len(lines),
        content_type="application/json",
    )