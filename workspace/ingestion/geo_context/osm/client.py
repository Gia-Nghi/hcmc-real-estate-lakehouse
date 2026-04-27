import time
from typing import Any, Dict

import requests

from common.utils.logger import get_logger
from ingestion.geo_context.osm.config import (
    OVERPASS_RETRIES,
    OVERPASS_RETRY_BACKOFF_SECONDS,
    OVERPASS_TIMEOUT_SECONDS,
    OVERPASS_URL,
    OVERPASS_USER_AGENT,
)

logger = get_logger(__name__)


def fetch_overpass(query: str) -> Dict[str, Any]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "text/plain; charset=utf-8",
        "User-Agent": OVERPASS_USER_AGENT,
    }

    last_error = None

    for attempt in range(OVERPASS_RETRIES):
        try:
            resp = requests.post(
                OVERPASS_URL,
                data=query.encode("utf-8"),
                headers=headers,
                timeout=OVERPASS_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_error = exc
            logger.warning(
                "Overpass request failed at attempt=%s/%s: %s",
                attempt + 1,
                OVERPASS_RETRIES,
                exc,
            )
            if attempt < OVERPASS_RETRIES - 1:
                time.sleep(OVERPASS_RETRY_BACKOFF_SECONDS * (attempt + 1))

    raise last_error