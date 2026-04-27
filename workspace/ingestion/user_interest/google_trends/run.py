# from datetime import datetime, timezone

# from pytrends.request import TrendReq

# from common.io.minio_client import create_minio_client, ensure_bucket, upload_json_bytes
# from common.utils.logger import get_logger
# from ingestion.user_interest.google_trends.config import GEO, KEYWORDS, MINIO_BUCKET, TIMEFRAME

# logger = get_logger(__name__)


# def main() -> None:
#     pytrends = TrendReq(hl="vi-VN", tz=420)

#     pytrends.build_payload(
#         kw_list=KEYWORDS,
#         timeframe=TIMEFRAME,
#         geo=GEO,
#     )

#     df = pytrends.interest_over_time().reset_index()

#     # Chuyển mọi giá trị Timestamp/datetime sang string ISO để JSON serialize được
#     records = []
#     for row in df.to_dict(orient="records"):
#         normalized_row = {}
#         for k, v in row.items():
#             if hasattr(v, "isoformat"):
#                 normalized_row[k] = v.isoformat()
#             else:
#                 normalized_row[k] = v
#         records.append(normalized_row)

#     payload = {
#         "source": "google_trends",
#         "domain": "user_interest",
#         "entity_type": "search_interest_timeseries",
#         "target_region": "Ho Chi Minh City",
#         "geo": GEO,
#         "timeframe": TIMEFRAME,
#         "keywords": KEYWORDS,
#         "collected_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
#         "payload": records,
#     }

#     client = create_minio_client()
#     ensure_bucket(client, MINIO_BUCKET)

#     now = datetime.now(timezone.utc)
#     dt = now.strftime("%Y-%m-%d")
#     ts = now.strftime("%Y%m%dT%H%M%SZ")
#     object_name = (
#         f"bronze/user_interest/google_trends/"
#         f"dt={dt}/snapshot_{ts}.json"
#     )

#     upload_json_bytes(client, MINIO_BUCKET, object_name, payload)
#     logger.info("Uploaded Google Trends snapshot to s3://%s/%s", MINIO_BUCKET, object_name)


# if __name__ == "__main__":
#     main()

from datetime import datetime, timezone, timedelta

import pandas as pd
from pytrends.request import TrendReq

from common.io.minio_client import create_minio_client, ensure_bucket, upload_json_bytes
from common.utils.logger import get_logger
from ingestion.user_interest.google_trends.config import (
    GEO,
    KEYWORDS,
    MINIO_BUCKET,
    TIMEFRAME,
    DAILY_MONTHS_BACK,
    DAILY_WINDOW_DAYS,
)

logger = get_logger(__name__)


def normalize_records(df: pd.DataFrame) -> list[dict]:
    records = []
    for row in df.to_dict(orient="records"):
        normalized_row = {}
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                normalized_row[k] = v.isoformat()
            else:
                normalized_row[k] = v
        records.append(normalized_row)
    return records


def collect_snapshot(pytrends: TrendReq) -> dict:
    pytrends.build_payload(
        kw_list=KEYWORDS,
        timeframe=TIMEFRAME,
        geo=GEO,
    )

    df = pytrends.interest_over_time().reset_index()
    records = normalize_records(df)

    payload = {
        "source": "google_trends",
        "domain": "user_interest",
        "entity_type": "search_interest_timeseries_snapshot",
        "target_region": "Ho Chi Minh City",
        "geo": GEO,
        "timeframe": TIMEFRAME,
        "keywords": KEYWORDS,
        "collected_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "payload": records,
    }
    return payload


def daterange_chunks(start_date: datetime, end_date: datetime, window_days: int):
    current_start = start_date
    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=window_days - 1), end_date)
        yield current_start, current_end
        current_start = current_end + timedelta(days=1)


def collect_daily_12m(pytrends: TrendReq) -> dict:
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30 * DAILY_MONTHS_BACK)

    all_frames = []

    for chunk_start, chunk_end in daterange_chunks(start_date, end_date, DAILY_WINDOW_DAYS):
        timeframe = (
            f"{chunk_start.strftime('%Y-%m-%d')} "
            f"{chunk_end.strftime('%Y-%m-%d')}"
        )
        logger.info("Collecting Google Trends chunk: %s", timeframe)

        pytrends.build_payload(
            kw_list=KEYWORDS,
            timeframe=timeframe,
            geo=GEO,
        )

        df = pytrends.interest_over_time().reset_index()

        if df.empty:
            logger.warning("Empty chunk returned for timeframe=%s", timeframe)
            continue

        all_frames.append(df)

    if not all_frames:
        payload = {
            "source": "google_trends",
            "domain": "user_interest",
            "entity_type": "search_interest_timeseries_daily_12m",
            "target_region": "Ho Chi Minh City",
            "geo": GEO,
            "months_back": DAILY_MONTHS_BACK,
            "window_days": DAILY_WINDOW_DAYS,
            "keywords": KEYWORDS,
            "collected_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
            "payload": [],
        }
        return payload

    merged = pd.concat(all_frames, ignore_index=True)

    # Chuẩn hóa tên cột ngày
    if "date" in merged.columns:
        merged["date"] = pd.to_datetime(merged["date"])
        merged = merged.sort_values("date")
        merged = merged.drop_duplicates(subset=["date"], keep="last")

    records = normalize_records(merged)

    payload = {
        "source": "google_trends",
        "domain": "user_interest",
        "entity_type": "search_interest_timeseries_daily_12m",
        "target_region": "Ho Chi Minh City",
        "geo": GEO,
        "months_back": DAILY_MONTHS_BACK,
        "window_days": DAILY_WINDOW_DAYS,
        "keywords": KEYWORDS,
        "collected_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "payload": records,
    }
    return payload


def main() -> None:
    pytrends = TrendReq(hl="vi-VN", tz=420)

    client = create_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    now = datetime.now(timezone.utc)
    dt = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%dT%H%M%SZ")

    # 1) Snapshot tổng quát
    snapshot_payload = collect_snapshot(pytrends)
    snapshot_object = (
        f"bronze/user_interest/google_trends/"
        f"dt={dt}/snapshot_{ts}.json"
    )
    upload_json_bytes(client, MINIO_BUCKET, snapshot_object, snapshot_payload)
    logger.info("Uploaded snapshot to s3://%s/%s", MINIO_BUCKET, snapshot_object)

    # 2) Daily 12 months gần nhất
    daily_payload = collect_daily_12m(pytrends)
    daily_object = (
        f"bronze/user_interest/google_trends/"
        f"dt={dt}/daily_12m_{ts}.json"
    )
    upload_json_bytes(client, MINIO_BUCKET, daily_object, daily_payload)
    logger.info("Uploaded daily 12m to s3://%s/%s", MINIO_BUCKET, daily_object)


if __name__ == "__main__":
    main()