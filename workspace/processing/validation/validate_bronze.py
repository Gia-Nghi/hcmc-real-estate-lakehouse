import json
import os
from minio import Minio

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")

REQUIRED_KEYS = [
    "source",
    "domain",
    "entity_type",
    "collected_at_ms",
    "record_count",
    "payload",
]


def main():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    objects = list(client.list_objects(MINIO_BUCKET, prefix="bronze/", recursive=True))

    if not objects:
        raise RuntimeError("No bronze objects found in MinIO")

    total = 0
    failed = 0

    for obj in objects:
        if obj.object_name.endswith("/"):
            continue
        
        if "_parsed_" not in obj.object_name:
            print(f"SKIP: {obj.object_name}")
            continue

        total += 1
        print(f"\nChecking: s3://{MINIO_BUCKET}/{obj.object_name}")

        try:
            resp = client.get_object(MINIO_BUCKET, obj.object_name)
            raw = resp.read()
            data = json.loads(raw)

            missing = [key for key in REQUIRED_KEYS if key not in data]
            if missing:
                raise ValueError(f"Missing keys: {missing}")

            payload = data["payload"]
            if not isinstance(payload, list):
                raise ValueError("payload must be a list")

            if len(payload) == 0:
                raise ValueError("payload is empty")

            if data["record_count"] != len(payload):
                print(
                    f"WARNING: record_count={data['record_count']} "
                    f"but len(payload)={len(payload)}"
                )

            print(f"OK: {len(payload)} records")

        except Exception as exc:
            failed += 1
            print(f"FAILED: {exc}")

    print("\n========== SUMMARY ==========")
    print(f"Total checked: {total}")
    print(f"Failed: {failed}")

    if failed > 0:
        raise RuntimeError(f"{failed} bronze files failed validation")


if __name__ == "__main__":
    main()