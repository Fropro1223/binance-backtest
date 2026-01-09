import requests
import zipfile
import shutil
from pathlib import Path
from tqdm import tqdm
from typing import List, Optional
import time

from src import config
from src.utils import setup_logging

logger = setup_logging("downloader")

def get_params_usdt_futures_symbols() -> List[str]:
    """Fetches all active USDT-Margined Futures symbols."""
    url = f"{config.BINANCE_FAPI_BASE_URL}/fapi/v1/exchangeInfo"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        symbols = []
        for s in data['symbols']:
            if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING':
                symbols.append(s['symbol'])
        
        logger.info(f"Found {len(symbols)} active USDT-M Perpetual symbols.")
        return symbols
    except Exception as e:
        logger.error(f"Failed to fetch symbols: {e}")
        raise

def download_file(url: str, local_path: Path) -> bool:
    """Downloads a file from URL to local_path with progress bar."""
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            if r.status_code == 404:
                # Some days might be missing for new pairs, warn but don't fail hard
                logger.warning(f"File not found (404): {url}")
                return False
            r.raise_for_status()
            
            total_size = int(r.headers.get('content-length', 0))
            block_size = 1024 * 1024 # 1MB

            with open(local_path, 'wb') as f, tqdm(
                desc=local_path.name,
                total=total_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for data in r.iter_content(block_size):
                    size = f.write(data)
                    bar.update(size)
        return True
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        if local_path.exists():
            local_path.unlink()
        return False

def extract_zip(zip_path: Path, extract_to: Path) -> Optional[Path]:
    """Extracts zip file and returns path to the extracted CSV."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # We expect typically one CSV file inside
            csv_filename = zip_ref.namelist()[0]
            zip_ref.extract(csv_filename, extract_to)
            return extract_to / csv_filename
    except Exception as e:
        logger.error(f"Failed to extract {zip_path}: {e}")
        return None
    finally:
        # Cleanup zip file to save space
        if zip_path.exists():
            zip_path.unlink()

def construct_binance_vision_url(symbol: str, date_str: str) -> str:
    """
    Constructs the download URL for a specific symbol and date.
    Format: https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-01.zip
    """
    return f"{config.BINANCE_VISION_BASE_URL}/{symbol}/{symbol}-aggTrades-{date_str}.zip"
