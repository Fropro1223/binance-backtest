import os
import sys
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sheets import log_analysis_to_sheet

def force_sync_headers():
    # Mock data to trigger header creation
    data = {
        'strategy_name': '[SHORT] PUMP EMA:None Pump:1.0% TP:8.0% SL:9.0% TSL:OFF M:0.8',
        'win_rate': 50.0,
        'total_trades': 100,
        'total_pnl': 500.0,
        'tf_breakdown': {
            '5s': {'trades': 10, 'pnl': 100},
            '10s': {'trades': 10, 'pnl': 100},
            '1m': {'trades': 10, 'pnl': 100},
        },
        'weekly_stats': [
            {'label': '12/01-18/01', 'week_num': 3, 'trades': 20, 'pnl': 200},
            {'label': '05/01-11/01', 'week_num': 2, 'trades': 20, 'pnl': 200},
            {'label': '29/12-04/01', 'week_num': 1, 'trades': 20, 'pnl': 200},
        ],
        'total_days': 90
    }
    
    print("🔄 Forcing header sync with sample data...")
    log_analysis_to_sheet(data)
    print("✅ Header sync complete.")

if __name__ == "__main__":
    force_sync_headers()
