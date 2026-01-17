import os
from pathlib import Path

# Base Paths (Absolute paths are safer)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# NEW DATA STRUCTURE (Outside Repo)
# /Users/firat/Algo/backtest_data
DATA_DIR = PROJECT_ROOT.parent / "backtest_data"
RAW_DATA_DIR = DATA_DIR / "raw"  # Stores symbol/week.parquet
META_DIR = DATA_DIR / "meta"     # Stores manifest.json

# Legacy support aliases (redirected)
# Note: PROCESSED_DATA_DIR pointing to RAW_DATA_DIR might be confusing 
# but prevents errors where code looks for it.
PROCESSED_DATA_DIR = RAW_DATA_DIR 

# Binance Data Source
BINANCE_VISION_BASE_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades"
BINANCE_FAPI_BASE_URL = "https://fapi.binance.com"

# Processing Settings
TIMEFRAMES = ["5s", "10s", "15s", "30s", "45s", "1m"]
TARGET_DAYS = 90  # Number of days to look back

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)
