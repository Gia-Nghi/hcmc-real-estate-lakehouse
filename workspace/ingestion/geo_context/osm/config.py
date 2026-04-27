OVERPASS_URL = "https://overpass-api.de/api/interpreter"

GRID_BBOXES = [
    (10.3, 106.3, 10.7, 106.7),
    (10.3, 106.7, 10.7, 107.1),
    (10.7, 106.3, 11.2, 106.7),
    (10.7, 106.7, 11.2, 107.1),
]

MINIO_BUCKET = "lakehouse"
OVERPASS_TIMEOUT_SECONDS = 300
OVERPASS_RETRIES = 3
OVERPASS_RETRY_BACKOFF_SECONDS = 2
OVERPASS_USER_AGENT = "hcmre-lakehouse/1.0"