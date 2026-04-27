import json
import os
from typing import Any, Dict, List

from minio import Minio


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9050")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")


BRONZE_DATASETS = [
    {
        "name": "chotot_listings",
        "prefix": "bronze/market_listings/chotot/listings/",
    },
    {
        "name": "google_trends",
        "prefix": "bronze/user_interest/google_trends/trends/",
    },
    {
        "name": "osm_pois",
        "prefix": "bronze/geo_context/osm/pois/",
    },
    {
        "name": "osm_roads",
        "prefix": "bronze/geo_context/osm/roads/",
    },
    {
        "name": "osm_transit_stops",
        "prefix": "bronze/geo_context/osm/transit_stops/",
    },
    {
        "name": "osm_railways",
        "prefix": "bronze/geo_context/osm/railways/",
    },
]


REQUIRED_METADATA_FIELDS = [
    "source",
    "domain",
    "entity_type",
    "collected_at_ms",
    "record_count",
]


def create_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def read_json(client: Minio, object_name: str) -> Dict[str, Any]:
    response = client.get_object(MINIO_BUCKET, object_name)
    try:
        return json.loads(response.read().decode("utf-8"))
    finally:
        response.close()
        response.release_conn()


def find_latest_json_object(client: Minio, prefix: str) -> str | None:
    objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))

    json_objects = [
        obj.object_name
        for obj in objects
        if obj.object_name.endswith(".json")
    ]

    if not json_objects:
        return None

    parsed_objects = [
        name
        for name in json_objects
        if "_parsed_" in name or "parsed" in name
    ]

    candidates = parsed_objects if parsed_objects else json_objects
    return sorted(candidates)[-1]


def validate_json_object(data: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    for field in REQUIRED_METADATA_FIELDS:
        if field not in data:
            errors.append(f"missing metadata field: {field}")

    payload = data.get("payload")
    elements = data.get("elements")

    if payload is None and elements is None:
        errors.append("missing payload/elements")

    if payload is not None:
        if not isinstance(payload, list):
            errors.append("payload must be a list")
        elif len(payload) == 0:
            errors.append("payload is empty")

    if elements is not None:
        if not isinstance(elements, list):
            errors.append("elements must be a list")
        elif len(elements) == 0:
            errors.append("elements is empty")

    record_count = data.get("record_count")

    if record_count is not None:
        if not isinstance(record_count, int):
            errors.append("record_count must be int")
        elif record_count <= 0:
            errors.append("record_count must be > 0")

        if isinstance(payload, list) and record_count != len(payload):
            errors.append(
                f"record_count mismatch: record_count={record_count}, "
                f"payload_len={len(payload)}"
            )

        if payload is None and isinstance(elements, list) and record_count != len(elements):
            errors.append(
                f"record_count mismatch: record_count={record_count}, "
                f"elements_len={len(elements)}"
            )

    collected_at_ms = data.get("collected_at_ms")
    if collected_at_ms is not None and not isinstance(collected_at_ms, int):
        errors.append("collected_at_ms must be int")

    return errors


def main() -> None:
    client = create_client()

    print(f"Validating bucket: {MINIO_BUCKET}")

    if not client.bucket_exists(MINIO_BUCKET):
        raise RuntimeError(f"Bucket does not exist: {MINIO_BUCKET}")

    total_checked = 0
    total_errors = 0

    for dataset in BRONZE_DATASETS:
        name = dataset["name"]
        prefix = dataset["prefix"]

        print(f"\nDataset: {name}")
        print(f"Prefix: s3://{MINIO_BUCKET}/{prefix}")

        latest_object = find_latest_json_object(client, prefix)

        if latest_object is None:
            total_errors += 1
            print("FAILED: no JSON object found")
            continue

        print(f"Latest object: {latest_object}")

        try:
            data = read_json(client, latest_object)
        except Exception as exc:
            total_errors += 1
            print(f"FAILED: JSON read error: {exc}")
            continue

        errors = validate_json_object(data)
        total_checked += 1

        if errors:
            total_errors += len(errors)
            print("FAILED")
            for error in errors:
                print(f"  - {error}")
        else:
            actual_len = len(data.get("payload", data.get("elements", [])))
            print("OK")
            print(f"record_count: {data.get('record_count')}")
            print(f"actual_len: {actual_len}")

    print("\nBronze validation summary")
    print(f"checked_objects: {total_checked}")
    print(f"total_errors: {total_errors}")

    if total_errors > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()