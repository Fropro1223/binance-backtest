import os
from pathlib import Path

# Base Paths (Absolute paths are safer)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Binance Data Source
BINANCE_VISION_BASE_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades"
BINANCE_FAPI_BASE_URL = "https://fapi.binance.com"

# Processing Settings
TIMEFRAMES = ["1s", "5s", "10s", "15s", "30s", "45s"]
TARGET_DAYS = 90  # Number of days to look back

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
