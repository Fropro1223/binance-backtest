#!/usr/bin/env python3
"""
Modular Backtest Runner
=======================
CLI destekli backtest sistemi. Strateji parametrelerini komut satırından alır.

Kullanım:
    python main.py --strategy pump_short --tp 4 --sl 2
    python main.py --strategy ema_pump --tp 8 --sl 3 --side LONG --pump 2
    python main.py --help
"""

import sys
import os
import argparse

# Add current directory to path
sys.path.append(os.getcwd())

from conditions.ema_chain import EmaChainConditions
from conditions.marubozu_pump import MarubozuConditions
from conditions.pump_short import PumpShortStrategy
from conditions.vectorized_strategy import VectorizedStrategy
from actions import evaluate_action
from backtest_framework import BacktestEngine
import pandas as pd

# Use local processed data
DATA_ROOT = os.path.join(os.getcwd(), "data", "processed")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Modüler Backtest Sistemi",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Örnekler:
  python main.py --strategy pump_short --tp 4 --sl 2
  python main.py --strategy ema_pump --tp 8 --sl 3 --side LONG
  python main.py --strategy ema_pump --tp 6 --sl 4 --pump 3 --bet 10
        """
    )
    
    parser.add_argument('--strategy', '-s', type=str, default='vectorized',
                        choices=['pump_short', 'ema_pump', 'vectorized'],
                        help='Strateji seçimi: vectorized (hızlı), pump_short, ema_pump (varsayılan: vectorized)')
    
    parser.add_argument('--tp', type=float, default=4.0,
                        help='Take Profit yüzdesi (örn: 4 = %%4, varsayılan: 4)')
    
    parser.add_argument('--sl', type=float, default=2.0,
                        help='Stop Loss yüzdesi (örn: 2 = %%2, varsayılan: 2)')
    
    parser.add_argument('--side', type=str, default='SHORT',
                        choices=['LONG', 'SHORT'],
                        help='Pozisyon yönü: LONG veya SHORT (varsayılan: SHORT)')
    
    parser.add_argument('--pump', type=float, default=2.0,
                        help='Pump threshold yüzdesi (varsayılan: 2)')
    
    parser.add_argument('--marubozu', type=float, default=0.80,
                        help='Marubozu eşik değeri 0-1 arası (varsayılan: 0.80)')
    
    parser.add_argument('--bet', type=float, default=7.0,
                        help='Pozisyon büyüklüğü USD (varsayılan: 7)')
    
    parser.add_argument('--max-pos', type=int, default=1,
                        help='Maksimum eşzamanlı pozisyon (varsayılan: 1)')
    
    parser.add_argument('--avg-thresh', type=float, default=0.0,
                        help='Ortalama eşik yüzdesi (pyramid için, varsayılan: 0)')
    
    parser.add_argument('--no-sheets', action='store_true',
                        help='Google Sheets loglamayı devre dışı bırak')
    
    parser.add_argument('--serial', action='store_true',
                        help='Paralel yerine seri işleme (debug için)')
    
    parser.add_argument('--tf', type=str, default=None,
                        help='Timeframe filtresi (örn: 45s, 30s, 15s). Belirtilmezse tümü kullanılır.')
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    if not os.path.exists(DATA_ROOT):
        print(f"❌ Data path not found: {DATA_ROOT}")
        return

    # Initialize Engine
    engine = BacktestEngine(data_dir=DATA_ROOT)
    
    # Convert percentages to decimals
    TP_PCT = args.tp / 100.0
    SL_PCT = args.sl / 100.0
    PUMP_THRESHOLD = args.pump / 100.0
    MARUBOZU_THRESHOLD = args.marubozu
    BET_SIZE = args.bet
    MAX_POSITIONS = args.max_pos
    AVG_THRESHOLD = args.avg_thresh / 100.0
    SIDE = args.side
    
    # Build strategy name for logging (includes pump level)
    STRATEGY_NAME_LOG = f"{args.strategy} [{SIDE}] TP:{args.tp}% SL:{args.sl}% Pump:{args.pump}%"
    
    # === STRATEGY SELECTION ===
    if args.strategy == 'vectorized':
        # FAST: Vectorized EMA + Pump + Marubozu (Polars/Turbo mode)
        SELECTED_CONDITIONS = VectorizedStrategy
        SELECTED_ACTION = None
        check_current_candle = False
    elif args.strategy == 'pump_short':
        # Simple pump-based strategy
        SELECTED_CONDITIONS = PumpShortStrategy
        SELECTED_ACTION = None  # PumpShortStrategy handles entry internally
        check_current_candle = True
    else:  # ema_pump
        # EMA Chain + Marubozu Conditions with Actions (SLOW - row by row)
        SELECTED_CONDITIONS = [EmaChainConditions, MarubozuConditions]
        SELECTED_ACTION = evaluate_action
        check_current_candle = False

    print("=" * 50)
    print("🚀 MODULAR BACKTEST SYSTEM")
    print("=" * 50)
    print(f"Strategy:    {args.strategy}")
    print(f"Side:        {SIDE}")
    print(f"TP:          {args.tp}%")
    print(f"SL:          {args.sl}%")
    print(f"Pump:        {args.pump}%")
    print(f"Bet Size:    ${BET_SIZE}")
    print(f"Max Pos:     {MAX_POSITIONS}")
    print(f"Parallel:    {not args.serial}")
    print("=" * 50)
    
    # Run backtest
    results = engine.run(
        SELECTED_CONDITIONS, 
        action_func=SELECTED_ACTION,
        max_positions=MAX_POSITIONS,
        avg_threshold=AVG_THRESHOLD,
        pump_threshold=PUMP_THRESHOLD, 
        marubozu_threshold=MARUBOZU_THRESHOLD, 
        tp=TP_PCT, 
        sl=SL_PCT, 
        bet_size=BET_SIZE,
        side=SIDE,
        parallel=not args.serial,
        check_current_candle=check_current_candle,
        tf_filter=args.tf
    )
    
    if results.empty:
        print("\n❌ No trades generated.")
        return

    # === ANALYSIS ===
    print("\n📊 RESULTS")
    print("-" * 50)
    
    total_trades = len(results)
    wins = len(results[results['pnl_usd'] > 0])
    losses = len(results[results['pnl_usd'] <= 0])
    win_rate = (wins / total_trades) * 100
    
    total_pnl = results['pnl_usd'].sum()
    avg_pnl = results['pnl_usd'].mean()
    
    print(f"Total Trades: {total_trades}")
    print(f"Win Rate:     {win_rate:.2f}% ({wins} W / {losses} L)")
    print(f"Total PnL:    ${total_pnl:.2f}")
    print(f"Avg PnL:      ${avg_pnl:.4f}")
    
    print("\n🏆 Top 5 Winners (by symbol):")
    print(results.groupby('symbol')['pnl_usd'].sum().sort_values(ascending=False).head(5).to_string())
    
    print("\n💀 Top 5 Losers (by symbol):")
    print(results.groupby('symbol')['pnl_usd'].sum().sort_values(ascending=False).tail(5).to_string())
    print("-" * 50)

    # Save Results
    results_csv = "backtest_results_pump.csv"
    print(f"\n💾 Saving results to {results_csv}...")
    results.to_csv(results_csv, index=False)
    
    # === WEEKLY BREAKDOWN ===
    print("📅 Calculating Weekly Stats (Sunday 03:00 - Sunday 03:00)...")
    overall_date_range = ""
    weekly_stats = []
    
    try:
        results['entry_time'] = pd.to_datetime(results['entry_time'])
        results = results.sort_values('entry_time')
        
        start_date = results['entry_time'].min().strftime('%d.%m')
        end_date = results['entry_time'].max().strftime('%d.%m')
        overall_date_range = f"({start_date}-{end_date})"
        
        # TradingView style: Week starts Sunday 03:00 UTC+3
        # Shift times by -3 hours so that Sunday 03:00 becomes Sunday 00:00
        # Then use W-SUN (week ending Sunday) for grouping
        shifted = results.copy()
        shifted['week_time'] = results['entry_time'] - pd.Timedelta(hours=3)
        
        weekly_groups = shifted.set_index('week_time').resample('W-SUN')
        
        for week_end, group in weekly_groups:
            # week_end is the shifted Sunday 00:00, so real week is Sunday 03:00
            real_week_end = week_end + pd.Timedelta(hours=3)
            real_week_start = real_week_end - pd.Timedelta(days=7)
            
            w_start = real_week_start.strftime('%d.%m')
            w_end_str = real_week_end.strftime('%d.%m')
            label = f"{w_start}-{w_end_str}"
            
            if group.empty:
                continue
            
            weekly_stats.append({
                'label': label,
                'trades': len(group),
                'pnl': group['pnl_usd'].sum()
            })
        
        # Reverse order: newest week first
        weekly_stats = weekly_stats[::-1]
            
    except Exception as e:
        print(f"⚠️ Error calculating weekly stats: {e}")

    # === GOOGLE SHEETS LOGGING ===
    if not args.no_sheets:
        print("☁️  Logging Analysis to Google Sheets...")
        
        summary_data = {
            'strategy_name': STRATEGY_NAME_LOG,
            'tp_pct': TP_PCT,
            'sl_pct': SL_PCT,
            'max_pos': MAX_POSITIONS,
            'avg_thresh': AVG_THRESHOLD,
            'bet_size': BET_SIZE,
            'total_trades': total_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'best_trade': results['pnl_usd'].max(),
            'worst_trade': results['pnl_usd'].min(),
            'date_range': overall_date_range,
            'weekly_stats': weekly_stats
        }
        
        try:
            from sheets import log_analysis_to_sheet
            log_analysis_to_sheet(summary_data)
        except Exception as e:
            print(f"⚠️ Sheets logging failed: {e}")
    else:
        print("⏭️  Skipping Google Sheets (--no-sheets)")

    print("\n✅ Backtest complete!")


if __name__ == "__main__":
    main()
