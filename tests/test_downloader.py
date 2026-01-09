import pytest
from pathlib import Path
from src import downloader, config

def test_config_paths():
    assert config.DATA_DIR.name == "data"
    assert config.RAW_DATA_DIR.exists()

def test_construct_url():
    url = downloader.construct_binance_vision_url("BTCUSDT", "2024-01-01")
    expected = "https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-01.zip"
    assert url == expected

@pytest.mark.integration
def test_fetch_symbols():
    """Real network request to check if API is reachable and returns symbols."""
    symbols = downloader.get_params_usdt_futures_symbols()
    assert isinstance(symbols, list)
    assert len(symbols) > 0
    assert "BTCUSDT" in symbols
    # Ensure no garbage symbols
    for s in symbols:
        assert isinstance(s, str)
        assert s.isupper()
