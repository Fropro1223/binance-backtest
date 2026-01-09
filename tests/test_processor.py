import pytest
import polars as pl
from pathlib import Path
from src import processor

@pytest.fixture
def dummy_csv(tmp_path):
    # Create a dummy CSV with known data
    # id, price, qty, first_id, last_id, time, is_buyer_maker
    # time 1000 = 1s, 2000 = 2s
    # We want: 
    # T=1000ms: prices 100, 105, 95, 102. Vol 1+1+1+1=4. High 105, Low 95, Open 100, Close 102.
    # T=2000ms: price 110. Vol 2.
    # T=3000ms: no trades
    # T=4000ms: price 100
    
    csv_content = """1,100,1,1,1,1000,true
2,105,1,2,2,1000,true
3,95,1,3,3,1000,false
4,102,1,4,4,1000,false
5,110,2,5,5,2000,true
6,100,1,6,6,4000,true
"""
    p = tmp_path / "test_agg.csv"
    p.write_text(csv_content)
    return p

def test_process_ohlcv(dummy_csv):
    df = processor.process_single_day(dummy_csv)
    assert not df.is_empty()
    assert df.height == 3 # 1000, 2000, 4000. 3000 is missing (sparse)
    
    # Check first row (1000ms)
    row0 = df.row(0, named=True)
    # 1000ms -> datetime 1970-01-01 00:00:01
    assert row0['open'] == 100.0
    assert row0['high'] == 105.0
    assert row0['low'] == 95.0
    assert row0['close'] == 102.0
    assert row0['volume'] == 4.0
    
    # Check second row (2000ms)
    row1 = df.row(1, named=True)
    assert row1['close'] == 110.0
    assert row1['volume'] == 2.0

def test_resample(dummy_csv):
    df_1s = processor.process_single_day(dummy_csv)
    
    # Resample to 2s
    # 0-2s bucket should include 1000ms. 2000ms is exactly on boundary usually start of next bin?
    # Polars group_by_dynamic: "every" window.
    # 1s timestamps: 1970-01-01 00:00:01, ...:02, ...:04
    # If we resample to 5s. All should be in first bin (00:00:00 - 00:00:05).
    
    df_5s = processor.resample_from_1s(df_1s, "5s")
    assert df_5s.height == 1
    
    row = df_5s.row(0, named=True)
    assert row['high'] == 110.0 # From t=2000
    assert row['low'] == 95.0   # From t=1000
    assert row['volume'] == 7.0 # 4+2+1
    assert row['close'] == 100.0 # From t=4000
