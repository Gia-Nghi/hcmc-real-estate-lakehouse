import os

CHOTOT_LIST_URL = (
    "https://gateway.chotot.com/v1/public/ad-listing"
    "?cg=1000&region_v2=13000&st=s%2Ck&source=listing&limit={limit}"
)
CHOTOT_DETAIL_URL = "https://gateway.chotot.com/v1/public/ad-listing/{list_id}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "chotot_raw")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
LOOP_SLEEP_SECONDS = int(os.getenv("LOOP_SLEEP_SECONDS", "900"))
MODE = os.getenv("MODE", "once")  # mock | once | loop