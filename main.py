"""
Binance Backtest Ana Çalıştırıcı (main.py)
==========================================
Bu dosya backtest süreçlerini başlatır, parametreleri yönetir ve sonuçları Google Sheets'e loglar.

GELECEKTEKİ AGENTLAR İÇİN KRİTİK KURALLAR (USER DIRECTIVE):
1. SEQUENTIAL BATCHING: Toplu deneyler (loop) Google Sheets API limitleri için MUTLAKA tek tek (sequential) çalıştırılmalıdır.
2. INTERNAL PARALLELISM: Tek bir backtest çalışırken pairlar MUTLAKA 8 çekirdek (CPU core) ile paralel işlenmelidir.
3. STRATEGY CHOICE: Her zaman 'vectorized' stratejisi kullanılmalıdır.
4. DATA LOGIC: Strateji hesaplamalarında her zaman Pandas kullanılmalıdır.
5. SHEETS LOGGING: Strateji ismi (VECTORIZED) Sheets loglarındaki isim sütununa yazılmamalıdır (yer kaplamaması için).

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

from conditions.vectorized_strategy import VectorizedStrategy
from backtest_framework import BacktestEngine
import pandas as pd
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
    
    parser.add_argument('--cond', type=str, default='pump',
                        choices=['pump', 'dump'],
                        help='Giriş koşulu: pump (yükseliş) veya dump (düşüş)')
    
    parser.add_argument('--pump', type=float, default=2.0,
                        help='Pump threshold yüzdesi (varsayılan: 2)')
    
    parser.add_argument('--dump', type=float, default=2.0,
                        help='Dump threshold yüzdesi (varsayılan: 2)')
    
    parser.add_argument('--tsl', type=float, default=0.0,
                        help='Trailing Stop Loss yüzdesi (örn: 1 = %1, 0 = kapalı, varsayılan: 0)')
    
    parser.add_argument('--marubozu', type=float, default=0.80,
                        help='Marubozu eşik değeri 0-1 arası (varsayılan: 0.80)')
    
    parser.add_argument('--bet', type=float, default=7.0,
                        help='Pozisyon büyüklüğü USD (varsayılan: 7)')
    
    parser.add_argument('--workers', type=int, default=8,
                        help='Paralel işlem için çekirdek sayısı (varsayılan: 8)')
    
    parser.add_argument('--max-pos', type=int, default=1,
                        help='Maksimum eşzamanlı pozisyon (varsayılan: 1)')
    
    parser.add_argument('--avg-thresh', type=float, default=0.0,
                        help='Ortalama eşik yüzdesi (pyramid için, varsayılan: 0)')
    
    parser.add_argument('--no-sheets', action='store_true',
                        help='Google Sheets loglamayı devre dışı bırak')
    
    parser.add_argument('--serial', action='store_true',
                        help='Paralel yerine seri işleme (debug için)')
    
    parser.add_argument('--tf', type=str, default=None,
                        help='Timeframe filtresi (orn: 45s, 30s, 15s). Belirtilmezse tümü kullanılır.')

    parser.add_argument('--ema', type=str, default='none', 
                        help='EMA: all_bull, all_bear, small_bull, small_bear, big_bull, big_bear, big_bear_small_bull, etc. (varsayılan: none)')
    
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
    DUMP_THRESHOLD = args.dump / 100.0
    MARUBOZU_THRESHOLD = args.marubozu
    BET_SIZE = args.bet
    MAX_POSITIONS = args.max_pos
    AVG_THRESHOLD = args.avg_thresh / 100.0
    SIDE = args.side
    
    # Build strategy name for logging
    ema_str = f"EMA:{args.ema.title()}"
    maru_str = f"M:{args.marubozu}"
    target_str = f"TP:{args.tp}% SL:{args.sl}%"
    tsl_str = f"TSL:{args.tsl}%" if args.tsl > 0 else "TSL:OFF"
    
    # Show only the threshold being USED
    if args.cond == "pump":
        cond_val_str = f"Pump:{args.pump}%"
    else:
        cond_val_str = f"Dump:{args.dump}%"

    STRATEGY_NAME_LOG = f"[{args.side}] {args.cond.upper()} {ema_str} {cond_val_str} {target_str} {tsl_str} {maru_str}"
    
    # If using specific EMA logic hardcoded in strategy, we might want to append it.
    # For now, this covers the CLI args.
    
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
        cond=args.cond,
        max_positions=MAX_POSITIONS,
        avg_threshold=AVG_THRESHOLD,
        pump_threshold=PUMP_THRESHOLD, 
        dump_threshold=DUMP_THRESHOLD,
        marubozu_threshold=MARUBOZU_THRESHOLD, 
        tp=TP_PCT, 
        sl=SL_PCT, 
        tsl=args.tsl / 100.0,
        bet_size=BET_SIZE,
        side=SIDE,
        ema=args.ema,
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
    
    # Weekly stats skipped
    weekly_stats = []

    # === GOOGLE SHEETS LOGGING ===
    if not args.no_sheets:
        print("☁️  Logging Analysis to Google Sheets...")
        
        # Calculate missing variables
        # Convert entry_time to datetime if it's string
        if not results.empty:
            results['entry_time'] = pd.to_datetime(results['entry_time'])
            overall_date_range = f"{results['entry_time'].min().strftime('%Y-%m-%d')} to {results['entry_time'].max().strftime('%Y-%m-%d')}"
        else:
            overall_date_range = "N/A"
        tf_breakdown = results.groupby('symbol').agg({'pnl_usd': 'sum'}).to_dict() if not results.empty else {}
        
        # Calculate actual days from data
        actual_days = (results['entry_time'].max() - results['entry_time'].min()).days + 1 if not results.empty else 0
        
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
            'weekly_stats': weekly_stats,
            'tf_breakdown': tf_breakdown,
            'total_days': actual_days
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
