# import os

# KEYWORDS = [
#     "bất động sản tphcm",
#     "mua nhà tphcm",
#     "căn hộ tphcm",
#     "chung cư tphcm",
#     "đất nền tphcm",
# ]

# TIMEFRAME = os.getenv("GT_TIMEFRAME", "today 12-m")
# GEO = os.getenv("GT_GEO", "VN")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")

import os

KEYWORDS = [
    "bất động sản tphcm",
    "mua nhà tphcm",
    "căn hộ tphcm",
    "chung cư tphcm",
    "đất nền tphcm",
]

# Snapshot tổng quát
TIMEFRAME = os.getenv("GT_TIMEFRAME", "today 12-m")
GEO = os.getenv("GT_GEO", "VN")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")

# Daily collection
DAILY_MONTHS_BACK = int(os.getenv("GT_DAILY_MONTHS_BACK", "12"))
DAILY_WINDOW_DAYS = int(os.getenv("GT_DAILY_WINDOW_DAYS", "30"))