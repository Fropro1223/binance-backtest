# Binance Futures Data Pipeline

This project automates the downloading, processing, and resampling of Binance Futures aggTrades data.

## Features
- **Automated Direct Download**: Fetches daily aggTrades zip files from `data.binance.vision`.
- **High Performance Processing**: Uses `Polars` for fast CSV parsing and aggregation.
- **Resampling**: Converts raw tick data to 1s OHLCV, then resamples to 5s, 10s, 15s, 30s, 45s.
- **Storage**: Saves outputs in compressed Parquet format.
- **Efficiency**: Processes day-by-day to minimize disk usage (deletes raw CSVs after processing).

## Setup

1. **Prerequisites**: Python 3.9+ (Python 3.14 via venv recommended).
2. **Install Dependencies**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

## Usage

### Run Full Pipeline (All Symbols, Last 90 Days)
```bash
python -m src.main
```
*Note: This will take a long time and download terabytes of data. Ensure you have stable internet.*

### Run for Specific Symbol and Duration
```bash
python -m src.main --symbol BTCUSDT --days 5
```

### Dry Run (Test)
```bash
python -m src.main --symbol ethusdt --days 1
```

## Output
Processed files are saved in `data/processed/` as Parquet files:
- `BTCUSDT_1s.parquet`
- `BTCUSDT_5s.parquet`
- ...

## Testing
Run unit tests:
```bash
pytest tests/
```
