import logging
import sys
from datetime import datetime, timedelta
from typing import List, Generator

def setup_logging(name: str = "binance_pipeline") -> logging.Logger:
    """Configures and returns a logger with stdout handler."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

def generate_date_range(start_date: datetime, end_date: datetime) -> Generator[datetime, None, None]:
    """Yields dates between start_date and end_date (inclusive)."""
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)

def get_date_from_string(date_str: str) -> datetime:
    """Parses YYYY-MM-DD string to datetime."""
    return datetime.strptime(date_str, "%Y-%m-%d")

def format_date_to_string(date_obj: datetime) -> str:
    """Formats datetime to YYYY-MM-DD string."""
    return date_obj.strftime("%Y-%m-%d")
