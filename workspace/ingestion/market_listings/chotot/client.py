import random
import time
from typing import Any, Dict, List, Optional

import requests

from common.utils.logger import get_logger
from ingestion.market_listings.chotot.config import (
    CHOTOT_DETAIL_URL,
    CHOTOT_LIST_URL,
    HEADERS,
)

logger = get_logger(__name__)


def request_with_retry(
    url: str,
    headers: Dict[str, str],
    retries: int = 3,
    timeout: int = 15,
    sleep_base: float = 1.0,
) -> Optional[requests.Response]:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp
            logger.warning("Request %s failed with status=%s", url, resp.status_code)
        except Exception as e:
            logger.warning("Request error %s (attempt %s/%s)", e, attempt, retries)

        time.sleep(sleep_base * attempt + random.uniform(0, 0.5))

    return None


def get_current_ads(limit: int) -> List[Dict[str, Any]]:
    url = CHOTOT_LIST_URL.format(limit=limit)
    resp = request_with_retry(url, HEADERS)
    if not resp:
        return []

    try:
        return resp.json().get("ads", [])
    except Exception as e:
        logger.error("LIST parse error: %s", e)
        return []


def get_detail(list_id: int) -> Optional[Dict[str, Any]]:
    url = CHOTOT_DETAIL_URL.format(list_id=list_id)
    resp = request_with_retry(url, HEADERS)
    if not resp:
        return None

    try:
        return resp.json()
    except Exception as e:
        logger.error("DETAIL parse error for list_id=%s: %s", list_id, e)
        return None