"""
Google Sheets Integration for Backtest Results (sheets.py)
==========================================================

Bu modül, backtest sonuçlarını Google Sheets'e loglayan ana fonksiyonu içerir.
Gelecekteki agentlar için kritik yapı bilgileri aşağıda detaylı olarak açıklanmıştır.

KRITIK KURALLAR (USER DIRECTIVE):
1. CREDENTIALS: ~/Algo/credentials/google_service_account.json kullanılır
2. MASTER SHEET ID: 1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM
3. WORKSHEET: "backtest1" (Ana çalışma sayfası)
4. VERI YAZIMI: Her zaman Row 3'e INSERT yapılır (en yeni veri en üstte)

SHEET YAPISI (SÜTUN DÜZENİ):
================================================================================
| Sütun | Index | Başlık (Row 1) | Alt Başlık (Row 2) | İçerik Türü |
|-------|-------|----------------|-------------------|-------------|
| A     | 1     | Timestamp      | -                 | Datetime    |
| B     | 2     | Strategy       | -                 | String      |
| C     | 3     | Side           | Side              | SHORT/LONG  |
| D     | 4     | Cond           | Cond              | pump/dump   |
| E     | 5     | Threshold%     | Threshold%        | Float       |
| F     | 6     | EMA            | EMA               | Emoji+Text  |
| G     | 7     | TP%            | TP%               | Int (1-10)  |
| H     | 8     | SL%            | SL%               | Int (1-10)  |
| I     | 9     | TSL%           | TSL%              | Int/OFF     |
| J     | 10    | Maru           | Maru              | Float       |
| K     | 11    | Days           | Days              | Int (90)    |
| L     | 12    | Results        | Win Rate          | Float (0-1) |
| M     | 13    | -              | Trades            | Int         |
| N     | 14    | -              | PnL ($)           | Float (Gross)|
| O     | 15    | -              | Commission        | Float       |
| P     | 16    | -              | Net PnL           | Float (Net) |
|-------|-------|----------------|-------------------|-------------|
| Q-AB  | 17-28 | Timeframes     | 5s, 10s... 1m     | Mixed       |
|-------|-------|----------------|-------------------|-------------|
| AA+   | 26+   | DD/MM-DD/MM    | Trades            | Int         |
| AB+   | 27+   | Wnn            | PnL               | Float       |
================================================================================

HAFTALIK VERI YAPISI:
- Başlangıç: Sütun AA (index 26)
- Her hafta 2 sütun: Trades (tek) ve PnL (çift)
- Label formatı: "DD/MM-DD/MM" (örn: "12/01-19/01")
- Hafta numarası: "Wnn" formatında (örn: "W03")
- Anchor: Pazar günü saat 03:00 (Europe/Istanbul)

DROPDOWN (DATA VALIDATION) SÜTUNLARI:
- C (Side): SHORT, LONG
- D (Cond): pump, dump
- E (Threshold%): 3.0, 2.5, 2.0, 1.5, 1.0, -1.0, -1.5, -2.0, -2.5, -3.0
- F (EMA): ⚪ none, 🔴🔴🔴 all_bear, 🟢🟢🟢 all_bull, ...
- G (TP%): 10, 9, 8, 7, 6, 5, 4, 3, 2, 1
- H (SL%): 10, 9, 8, 7, 6, 5, 4, 3, 2, 1
- I (TSL%): 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, OFF
- J (Maru): 0.9, 0.8, 0.7, 0.6, 0.5

CONDITIONAL FORMATTING:
- L (Win Rate): Gradient (Red → White → Green)
- M (Trades): Gradient (White → Blue)
- N (PnL): 
    - Background: Gradient (Red @ negative → White @ 0 → Green @ positive)
    - Text Color: Green (>0), Red (<0) ← CF Rule ile eklendi
- O (Commission): Text Color (Black)
- P (Net PnL):
    - Background: Gradient (Red @ negative → White @ 0 → Green @ positive)
    - Text Color: Green (>0), Red (<0)
- G (TP%): Gradient (Light Green → Dark Green)
- H (SL%): Gradient (Light Red → Dark Red)


YARDIMCI SCRIPTLER:
- clear_cf.py: Tüm CF kurallarını temizler
- restore_visuals.py: Gradyan formatlarını uygular
- fix_dropdowns.py: Data validation kurallarını düzeltir
- repair_headers.py: Row 1-2 başlık yapısını onarır
- fix_text_color.py: Yazı renklerini siyaha ayarlar
"""

import sys
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime
import re

# =============================================================================
# CONFIGURATION
# =============================================================================

DEFAULT_CREDS_PATH = os.path.expanduser("~/Algo/credentials/google_service_account.json")
MASTER_SHEET_ID = "1iQ2-1KN6kwyA3TGgKHTxzN5YCGxJe2ZHPk9V9mtt5WM"
TARGET_WORKSHEET = "backtest1"

# Master Column Structure for Parameters (C-K, indices 2-10)
PARAM_COLS = {
    2: {"header": "Side", "options": ["SHORT", "LONG"]},
    3: {"header": "Cond", "options": ["pump", "dump"]},
    4: {"header": "Threshold%", "options": ["3.0", "2.5", "2.0", "1.5", "1.0", "-1.0", "-1.5", "-2.0", "-2.5", "-3.0"]},
    5: {"header": "EMA", "options": ["⚪ none", "🔴🔴🔴 all_bear", "🟢🟢🟢 all_bull", "🔴🔴 big_bear", "🟢🟢 big_bull", "🔴 small_bear", "🟢 small_bull", "🔴🔴🟢 big_bear_small_bull", "🟢🟢🔴 big_bull_small_bear"]},
    6: {"header": "TP%", "options": [str(x) for x in range(10, 0, -1)]},
    7: {"header": "SL%", "options": [str(x) for x in range(10, 0, -1)]},
    8: {"header": "TSL%", "options": [str(x) for x in range(10, 0, -1)] + ["OFF"]},
    9: {"header": "Maru", "options": ["0.9", "0.8", "0.7", "0.6", "0.5"]},
    10: {"header": "Days", "options": []}
}

# Timeframe column mapping (1-indexed column numbers) - Shifted by 2 due to Commission/Net PnL
TF_COLS = {'5s': 17, '10s': 19, '15s': 21, '30s': 23, '45s': 25, '1m': 27}

# Results columns (1-indexed)
METRICS_START_COL = 12  # L = Win Rate, M = Trades, N = PnL, O = Commission, P = Net PnL

COMMISSION_RATE = 0.005  # Binde 5 (5 / 1000)

# Weekly data starts at column AA (1-indexed = 27, 0-indexed = 26)
WEEKLY_START_COL = 27

# =============================================================================
# MAIN LOGGING FUNCTION
# =============================================================================

def log_analysis_to_sheet(data, json_path=None):
    """
    Backtest sonuçlarını Google Sheets'e loglar.
    
    Args:
        data (dict): Backtest özet verileri. Aşağıdaki anahtarları içermelidir:
            - strategy_name (str): Strateji adı (regex ile parse edilir)
            - win_rate (float): Kazanma oranı (0-100 arası)
            - total_trades (int): Toplam işlem sayısı
            - total_pnl (float): Toplam kar/zarar ($)
            - tf_breakdown (dict): Timeframe bazlı istatistikler
            - weekly_stats (list): Haftalık istatistikler
            - total_days (int): Backtest süresi (gün)
        
        json_path (str, optional): Kullanılmıyor (legacy parametre)
    
    Returns:
        None
    
    Side Effects:
        1. Row 3'e yeni satır INSERT eder
        2. Gerekirse yeni haftalık sütun başlıkları ekler
    
    Raises:
        Exception: Google Sheets API hatası durumunda
    
    Example:
        >>> data = {
        ...     'strategy_name': '[SHORT] PUMP EMA:None Pump:2.0% TP:8% SL:4% TSL:OFF M:0.8',
        ...     'win_rate': 52.5,
        ...     'total_trades': 1234,
        ...     'total_pnl': 567.89,
        ...     'tf_breakdown': {'5s': {'trades': 100, 'pnl': 50.0}, ...},
        ...     'weekly_stats': [{'label': '12/01-19/01', 'week_num': 3, 'trades': 50, 'pnl': 25.0}],
        ...     'total_days': 90
        ... }
        >>> log_analysis_to_sheet(data)
        ✅ Logged row to Row 3.
    """
    try:
        # ===== 1. AUTHENTICATION =====
        creds_path = DEFAULT_CREDS_PATH
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
        client = gspread.authorize(creds)
        
        # ===== 2. OPEN SHEET =====
        manual_sheet_id = os.environ.get('MANUAL_SHEET_ID')
        sheet_id = manual_sheet_id if manual_sheet_id else MASTER_SHEET_ID
        
        sheet = client.open_by_key(sheet_id)
        ws = sheet.worksheet(TARGET_WORKSHEET)
        
        # ===== 3. PREPARE ROW DATA =====
        row_data = {i: "" for i in range(1, 150)}  # Pre-initialize all columns
        
        # A: Timestamp
        row_data[1] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # B: Strategy Name
        row_data[2] = data.get('strategy_name', 'Unknown')
        
        # ===== 4. PARSE STRATEGY STRING =====
        strategy_str = data.get('strategy_name', '')
        
        # EMA emoji mapping
        ema_emoji_map = {
            "big_bull": "🟢🟢", "big_bear": "🔴🔴", 
            "all_bull": "🟢🟢🟢", "all_bear": "🔴🔴🔴",
            "small_bull": "🟢", "small_bear": "🔴", 
            "none": "⚪",
            "big_bull_small_bear": "🟢🟢🔴", 
            "big_bear_small_bull": "🔴🔴🟢"
        }
        
        # C: Side
        if "[SHORT]" in strategy_str: 
            row_data[3] = "SHORT"
        elif "[LONG]" in strategy_str: 
            row_data[3] = "LONG"
        
        # D: Condition
        if "PUMP" in strategy_str.upper(): 
            row_data[4] = "pump"
        elif "DUMP" in strategy_str.upper(): 
            row_data[4] = "dump"
        
        # F: EMA (with emoji)
        ema_match = re.search(r'EMA:(\S+)', strategy_str)
        ema_raw = (ema_match.group(1).lower() if ema_match else "none")
        # Normalize EMA variations
        ema_raw = ema_raw.replace("big_bull_small_bull", "all_bull")\
                         .replace("big_bear_small_bear", "all_bear")\
                         .replace("small_bull_big_bull", "all_bull")\
                         .replace("small_bear_big_bear", "all_bear")\
                         .replace("small_bull_big_bear", "big_bear_small_bull")\
                         .replace("small_bear_big_bull", "big_bull_small_bear")
        emoji = ema_emoji_map.get(ema_raw, "")
        row_data[6] = f"{emoji} {ema_raw}" if emoji else ema_raw

        # E: Threshold (Pump or Dump value)
        pump_match = re.search(r'Pump:(\d+\.?\d*)%', strategy_str)
        dump_match = re.search(r'Dump:(\d+\.?\d*)%', strategy_str)
        if pump_match: 
            row_data[5] = f"{float(pump_match.group(1)):.1f}"
        elif dump_match: 
            row_data[5] = f"{-float(dump_match.group(1)):.1f}"

        # G: TP%
        tp_match = re.search(r'TP:(\d+\.?\d*)%?', strategy_str)
        row_data[7] = tp_match.group(1) if tp_match else ""
        
        # H: SL%
        sl_match = re.search(r'SL:(\d+\.?\d*)%?', strategy_str)
        row_data[8] = sl_match.group(1) if sl_match else ""
        
        # I: TSL%
        tsl_match = re.search(r'TSL:(\d+\.?\d*|OFF)', strategy_str)
        row_data[9] = tsl_match.group(1) if tsl_match else "OFF"
        
        # J: Marubozu threshold
        maru_match = re.search(r'M:(\d+\.?\d*)', strategy_str)
        row_data[10] = maru_match.group(1) if maru_match else ""
        
        # K: Days
        row_data[11] = str(data.get('total_days', 90))

        # ===== 5. METRICS (L-P) =====
        pnl = float(data.get('total_pnl', 0))
        trades_count = int(data.get('total_trades', 0))
        bet_size = float(data.get('bet_size', 7.0)) # Default to 7.0 as used in run_mega_batch.py
        
        # Commission calculation: Trades * BetSize * COMMISSION_RATE * 2 (Entry + Exit)
        commission = trades_count * bet_size * COMMISSION_RATE * 2 
        net_pnl = pnl - commission

        row_data[12] = float(data.get('win_rate', 0)) / 100.0  # L: Win Rate (0-1 range)
        row_data[13] = trades_count                            # M: Trades
        row_data[14] = round(pnl, 2)                           # N: PnL (Gross)
        row_data[15] = round(commission, 2)                    # O: Commission
        row_data[16] = round(net_pnl, 2)                       # P: Net PnL

        # ===== 6. TIMEFRAME BREAKDOWN (Q-AB) =====
        tf_breakdown = data.get('tf_breakdown', {})
        for tf, col in TF_COLS.items():
            stats = tf_breakdown.get(tf, {})
            row_data[col] = int(stats.get('trades', 0))
            row_data[col + 1] = float(stats.get('pnl', 0.0))

        # ===== 7. WEEKLY STATS - DISABLED =====
        # Haftalık veri kaydı şu anda devre dışı.
        # Tekrar etkinleştirmek için bu bölümü uncomment yapın.
        #
        # weekly_stats = data.get('weekly_stats', [])
        # new_cols_group = []
        # ... (kod kaldırıldı)
        
        # ===== 8. INSERT ROW =====


        # ===== 9. INSERT ROW =====
        max_idx = max(row_data.keys())
        final_values = [""] * max_idx
        for k, v in row_data.items():
            final_values[k - 1] = v
            
        ws.insert_row(final_values, index=3, value_input_option='USER_ENTERED')
        
        print(f"✅ Logged row to Row 3.")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Failed to log: {e}")


def apply_sheet_formatting(ws):
    """
    Worksheet formatlamasını uygular.
    
    NOT: Bu fonksiyon şu anda devre dışı bırakılmıştır.
    Formatlama için ayrı yardımcı scriptler kullanılmalıdır:
    - restore_visuals.py: Gradyan formatları
    - fix_text_color.py: Yazı renkleri
    - fix_dropdowns.py: Data validation
    
    Args:
        ws: gspread.Worksheet object
    
    Returns:
        None
    """
    pass  # Formatlama devre dışı - ayrı scriptler kullanın
