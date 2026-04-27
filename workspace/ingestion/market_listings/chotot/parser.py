import time
from typing import Any, Dict


def build_raw_record(ad: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    list_id = ad.get("list_id") or detail.get("ad", {}).get("list_id")

    return {
        "source": "chotot",
        "domain": "market_listings",
        "entity_type": "listing",
        "record_id": str(list_id) if list_id is not None else None,
        "crawled_at_ms": int(time.time() * 1000),
        "payload": {
            "list": ad,
            "detail": detail,
        },
    }