import pandas as pd
import numpy as np
import os
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from dataclasses import dataclass
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

@dataclass
class Trade:
    symbol: str
    entry_time: str
    exit_time: str
    type: str # 'TP', 'SL', 'EXIT'
    entry_price: float
    exit_price: float
    pnl_percent: float
    pnl_usd: float
    duration_min: float
    pump_percent: float = 0.0
    level: int = 1  # Pyramid level (1=first position, 2=second, etc.)

class Strategy(ABC):
    """
    Abstract Base Class for all strategies.
    """
    def __init__(self, bet_size: float = 7.0):
        self.bet_size = bet_size
        self.trades: List[Trade] = []

    @abstractmethod
    def on_candle(self, timestamp, open, high, low, close) -> Optional[Dict]:
        """
        Called for every candle.
        Returns a dict with order details if an entry is triggered, else None.
        Dict format: {'type': 'SHORT'|'LONG', 'tp': float, 'sl': float}
        """
        pass

# Top-level function for multiprocessing (must be picklable)
def process_single_pair(args):
    """
    Worker function to process a single parquet file.
    args: (filepath, strategy_classes, check_current_candle, strategy_kwargs)
    Note: strategy_classes can be a single class or a list of classes.
    """
    filepath, strategy_classes, check_current_candle, strategy_kwargs = args
    
    # Extract action_func if provided
    action_func = strategy_kwargs.pop('action_func', None)
    
    try:
        path = args[0] if isinstance(args, tuple) else args
        # Check if dir
        import pathlib
        import polars as pl
        p = pathlib.Path(filepath)
        
        if p.is_dir():
             # Load directory
             df = pl.scan_parquet(str(p / "*.parquet")).collect().sort("ts_1s").to_pandas()
        else:
             df = pd.read_parquet(filepath)

        if df.empty: return []

        symbol = os.path.basename(filepath).replace('.parquet', '')
        
        # --- MULTI-CONDITION SUPPORT ---
        # Ensure strategy_classes is a list
        if not isinstance(strategy_classes, list):
            strategy_classes = [strategy_classes]
            
        # Instantiate all strategy (condition) classes
        # We use the FIRST one as the "primary" for getting Bet Size etc.
        strategies = []
        for cls in strategy_classes:
            st = cls(**strategy_kwargs)
            # OPTIMIZATION: Data Preparation Hook
            # Har bir stratejiye tüm veriyi gönderip "prep_data" (varsa) çalıştırıyoruz.
            # Bu sayede indikatörleri loop'tan önce toplu hesaplayabilirler.
            if hasattr(st, 'prep_data'):
                st.prep_data(df)
            strategies.append(st)

            
        primary_strategy = strategies[0] # Use this for bet_size access
        
        opens = df['open'].values
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values
        
        if 'open_time' in df.columns:
            times = df['open_time'].values
        elif 'ts_1s' in df.columns:
            times = df['ts_1s'].values
        else:
            times = df.index.values
        
        in_position = False
        position_side = None # 'LONG' or 'SHORT'
        entry_price = 0.0
        tp_price = 0.0
        sl_price = 0.0
        entry_time = None
        entry_pump = 0.0
        
        completed_trades = []
        
        for i in range(len(closes)):
            current_time = times[i]
            
            if in_position:
                exit_price = 0.0
                exit_type = ""
                
                # SHORT EXIT LOGIC
                if position_side == 'SHORT':
                    if highs[i] >= sl_price:
                        exit_price = sl_price
                        exit_type = "SL"
                    elif lows[i] <= tp_price:
                        exit_price = tp_price
                        exit_type = "TP"
                
                # LONG EXIT LOGIC
                elif position_side == 'LONG':
                    if lows[i] <= sl_price:
                        exit_price = sl_price
                        exit_type = "SL"
                    elif highs[i] >= tp_price:
                        exit_price = tp_price
                        exit_type = "TP"
                    
                if exit_type:
                    if position_side == 'SHORT':
                        pnl_pct = (entry_price - exit_price) / entry_price
                    else: # LONG
                        pnl_pct = (exit_price - entry_price) / entry_price
                        
                    pnl_usd = primary_strategy.bet_size * pnl_pct
                    
                    completed_trades.append(Trade(
                        symbol=symbol,
                        entry_time=str(entry_time),
                        exit_time=str(current_time),
                        type=exit_type,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        pnl_percent=pnl_pct,
                        pnl_usd=pnl_usd,
                        duration_min=0,
                        pump_percent=entry_pump
                    ))
                    in_position = False
                    position_side = None
                    continue
            
            if not in_position:
                # 1. Update State (ALL Conditions)
                # We collect 'conditions' dict from ALL strategies and merge them
                merged_conditions = {}
                
                for strategy in strategies:
                    # Run on_candle for indicator updates
                    # (Some might return trade dicts, but we ignore them if using action_func)
                    strategy.on_candle(
                        timestamp=current_time,
                        open=opens[i],
                        high=highs[i],
                        low=lows[i],
                        close=closes[i]
                    )
                    
                    # Merge conditions if it has them
                    if hasattr(strategy, 'conditions'):
                        merged_conditions.update(strategy.conditions)
                
                # Create a "Composite" object to pass to actions
                # It behaves like a strategy instance but has merged conditions
                class CompositeState:
                    def __init__(self, conds, base_strat):
                        self.conditions = conds
                        self.tp = getattr(base_strat, 'tp', 0.04)
                        self.sl = getattr(base_strat, 'sl', 0.04)
                
                composite_state = CompositeState(merged_conditions, primary_strategy)
                
                # 2. Evaluate Decision (Actions)
                decision = None
                if action_func:
                    candle_data = {
                        'timestamp': current_time,
                        'open': opens[i],
                        'high': highs[i],
                        'low': lows[i],
                        'close': closes[i]
                    }
                    decision = action_func(composite_state, candle_data)
                
                if decision:
                    action = decision.get('action')
                    if action in ['SHORT', 'LONG']:
                        entry_price = decision['entry_price']
                        tp_price = decision['tp']
                        sl_price = decision['sl']
                        entry_time = decision.get('timestamp', current_time)
                        entry_pump = decision.get('pump_percent', 0.0)
                        should_check_current = decision.get('check_current_candle', check_current_candle)

                        # Validate TP/SL vs Entry to avoid instant triggers if logic is bad
                        # (Optional check could go here)

                        instant_exit = False
                        
                        if should_check_current:
                            if action == 'SHORT':
                                if highs[i] >= sl_price:
                                    instant_exit = True
                                    exit_price = sl_price
                                    exit_type = "SL_INSTANT"
                                    pnl_pct = (entry_price - sl_price) / entry_price
                                elif lows[i] <= tp_price:
                                    instant_exit = True
                                    exit_price = tp_price
                                    exit_type = "TP_INSTANT"
                                    pnl_pct = (entry_price - tp_price) / entry_price
                            elif action == 'LONG':
                                if lows[i] <= sl_price:
                                    instant_exit = True
                                    exit_price = sl_price
                                    exit_type = "SL_INSTANT"
                                    pnl_pct = (sl_price - entry_price) / entry_price
                                elif highs[i] >= tp_price:
                                    instant_exit = True
                                    exit_price = tp_price
                                    exit_type = "TP_INSTANT"
        return completed_trades

        # Dead code below this line (removed/skipped)
        curr_idx = 0
        max_idx = len(df)
        
        tp_pct = strategy.tp
        sl_pct = strategy.sl
        bet_size = 7.0 # Fixed for now or passed via args
        
        while curr_idx < max_idx:
            # 1. Find Next Signal
            # Slice the signal array
            future_signals = arr_signals[curr_idx:]
            
            # Find indices where True
            true_indices = np.where(future_signals)[0]
            
            if len(true_indices) == 0:
                break # No more signals
            
            # Global Index of next signal
            entry_idx = curr_idx + true_indices[0]
            
            # Execute Entry
            entry_price = arr_close[entry_idx]
            entry_time = arr_time[entry_idx]
            
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)
            
            # 2. Find Exit (Scan forward from distinct entry_idx + 1)
            # We look for: Low <= TP  OR  High >= SL
            
            # Optimization: We only check slice [entry_idx + 1 : ]
            search_slice_high = arr_high[entry_idx+1:]
            search_slice_low = arr_low[entry_idx+1:]
            
            # Find first occurrence indices
            # SL Hit: High >= SL
            sl_hits = np.where(search_slice_high >= sl_price)[0]
            
            # TP Hit: Low <= TP
            tp_hits = np.where(search_slice_low <= tp_price)[0]
            
            first_sl_idx = sl_hits[0] if len(sl_hits) > 0 else 999999999
            first_tp_idx = tp_hits[0] if len(tp_hits) > 0 else 999999999
            
            if first_sl_idx == 999999999 and first_tp_idx == 999999999:
                # Never exits (End of Data)
                # Force close at end? Or ignore?
                break
                
            # Compare which happened first
            if first_sl_idx <= first_tp_idx:
                # SL Hit
                local_exit_idx = first_sl_idx
                exit_type = "SL"
                exit_price = sl_price
                pnl_pct = (entry_price - exit_price) / entry_price # Short logic
            else:
                # TP Hit
                local_exit_idx = first_tp_idx
                exit_type = "TP"
                exit_price = tp_price
                pnl_pct = (entry_price - exit_price) / entry_price # Short logic
                
            # Global Exit Index
            real_exit_idx = (entry_idx + 1) + local_exit_idx
            exit_time = arr_time[real_exit_idx]
            
            pnl_usd = bet_size * pnl_pct
            
            completed_trades.append(Trade(
                symbol=os.path.basename(filepath).replace('.parquet',''),
                entry_time=str(entry_time),
                exit_time=str(exit_time),
                type=exit_type,
                entry_price=entry_price,
                exit_price=exit_price,
                pnl_percent=pnl_pct,
                pnl_usd=pnl_usd,
                duration_min=0
            ))
            
            # Resume search AFTER the exit
            curr_idx = real_exit_idx + 1
            
        return completed_trades

    except Exception as e:
        print(f"Error Polars {filepath}: {e}")
        import traceback
        traceback.print_exc()
        return []

class BacktestEngine:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        
    def _extract_base_pair(self, symbol: str) -> str:
        """Extract base pair without timeframe suffix. e.g. BTCUSDT_30s -> BTCUSDT"""
        parts = symbol.split('_')
        return parts[0] if parts else symbol
        
    def run(self, strategy_class, max_positions=10, avg_threshold=0.10, parallel=True, workers=None, check_current_candle=True, tf_filter=None, **strategy_kwargs) -> pd.DataFrame:
        from pathlib import Path
        base_path = Path(self.data_dir)
        raw_path = base_path / "raw"
        
        files = [] # This will store paths to "TF Dataset" directories (e.g. raw/BTCUSDT/5s)
        
        if raw_path.exists():
             # New Mode: Iterate symbol directories
             # Structure: raw/SYMBOL/TF/*.parquet
             
             # 1. Get all symbols
             symbols = [d for d in raw_path.iterdir() if d.is_dir()]
             
             for sym_dir in symbols:
                 # 2. Get all TFs in symbol dir
                 tfs = [d for d in sym_dir.iterdir() if d.is_dir()]
                 
                 for tf_dir in tfs:
                     tf_name = tf_dir.name # e.g. "5s", "1m"
                     
                     if tf_filter and tf_name != tf_filter:
                         continue
                         
                     # Add this dataset directory to processing list
                     files.append(str(tf_dir))
                     
             if tf_filter:
                 print(f"📂 Timeframe filter applied: {tf_filter}")
        else:
             # Legacy Mode
             files = [os.path.join(self.data_dir, f) for f in os.listdir(self.data_dir) if f.endswith('.parquet')]
             if tf_filter:
                files = [f for f in files if f"_{tf_filter}.parquet" in f]
                print(f"📂 Timeframe filter: {tf_filter} ({len(files)} files)")
        
        if not files:
            print("❌ No data files found.")
            return pd.DataFrame()


        import multiprocessing
        from concurrent.futures import ProcessPoolExecutor, as_completed
        
        num_workers = workers if workers else 8  # Always use 8 cores by default
        
        # Determine Worker Function
        worker_func = process_single_pair
        desc = "Standard"
        
        # Check if strategy supports Polars Turbo Mode
        if hasattr(strategy_class, 'process_file'):
            worker_func = process_single_pair_polars
            desc = "🚀 TURBO (Polars)"
        
        print(f"🚀 Running Backtest on {len(files)} pairs ({desc})...")
        if parallel:
             print(f"⚡ Parallel Mode: {num_workers} workers")
        else:
             print(f"🐌 Serial Mode")
             
        print(f"📊 Pyramid Mode: max_positions={max_positions}, avg_threshold={avg_threshold*100:.0f}%")
        
        all_trades = []
        
        if parallel:
            # Prepare arguments for each worker
            tasks = [(f, strategy_class, check_current_candle, strategy_kwargs) for f in files]
            
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                # Submit all tasks
                futures = [executor.submit(worker_func, t) for t in tasks]
                results = []
                
                total = len(tasks)
                print(f"⏳ Processing {total} pairs...", flush=True)
                
                for i, f in enumerate(as_completed(futures)):
                    try:
                        res = f.result()
                        results.append(res)
                    except Exception as e:
                        print(f"❌ Worker Error: {e}")
                    
                    # Print progress every 20 items
                    if (i + 1) % 20 == 0:
                        pct = ((i + 1) / total) * 100
                        print(f"   👉 Progress: {i + 1}/{total} ({pct:.1f}%)", flush=True)
                
            for res in results:
                all_trades.extend(res)
        else:
            # Serial fallback
            for filepath in files:
                res = worker_func((filepath, strategy_class, check_current_candle, strategy_kwargs))
                all_trades.extend(res)
                
        print(f"📊 Raw trades before pyramid filter: {len(all_trades)}")
        
        # Post-process: Apply pyramid strategy (Global reduction step)
        if max_positions > 1:
             print("Applying global pyramid filter...")
             all_trades = self._apply_pyramid_strategy(all_trades, max_positions, avg_threshold)
        
        print(f"✅ Final trades: {len(all_trades)}")
        df = pd.DataFrame([t.__dict__ for t in all_trades])
        # Reorder columns to put 'level' second (after symbol)
        if not df.empty and 'level' in df.columns:
            cols = ['symbol', 'level'] + [c for c in df.columns if c not in ['symbol', 'level']]
            df = df[cols]
        return df
    
    def _apply_pyramid_strategy(self, trades: List[Trade], max_positions: int, avg_threshold: float) -> List[Trade]:
        """
        Pyramid/Averaging strategy:
        - Allow up to max_positions per base pair
        - 2nd+ entry only if entry_price > avg_price * (1 + threshold)
        - Each position closes independently on its own TP/SL
        """
        if not trades:
            return []
        
        # Sort by entry time, then exit time for deterministic ordering
        trades_sorted = sorted(trades, key=lambda t: (pd.Timestamp(t.entry_time), pd.Timestamp(t.exit_time), t.symbol))
        
        # Track open positions per base pair
        # open_positions[base_pair] = [{'entry_price': x, 'exit_time': ts}, ...]
        open_positions = {}
        filtered = []
        
        for trade in trades_sorted:
            base_pair = self._extract_base_pair(trade.symbol)
            entry_ts = pd.Timestamp(trade.entry_time)
            exit_ts = pd.Timestamp(trade.exit_time)
            entry_price = trade.entry_price
            
            # Initialize if needed
            if base_pair not in open_positions:
                open_positions[base_pair] = []
            
            # Clean up closed positions (exit_time <= current entry_time)
            open_positions[base_pair] = [
                pos for pos in open_positions[base_pair] 
                if pos['exit_time'] > entry_ts
            ]
            
            current_count = len(open_positions[base_pair])
            
            # Check if we can add this position
            can_add = False
            
            if current_count == 0:
                # First position - always allowed
                can_add = True
            elif current_count < max_positions:
                # Calculate current average
                avg_price = sum(pos['entry_price'] for pos in open_positions[base_pair]) / current_count
                required_price = avg_price * (1 + avg_threshold)
                
                # Only add if price is above threshold
                if entry_price > required_price:
                    can_add = True
            
            if can_add:
                # Set the level (how many positions are already open + 1)
                trade.level = current_count + 1
                filtered.append(trade)
                open_positions[base_pair].append({
                    'entry_price': entry_price,
                    'exit_time': exit_ts
                })
        
        return filtered

    
# Old serial method removed/replaced by top-level process_single_pair

import polars as pl
from datetime import timedelta

def process_single_pair_polars(args):
    """
    Turbo Worker using Polars for everything.
    args: (filepath, strategy_class, check_current_candle, strategy_kwargs)
    """
    filepath, strategy_class, check_current_candle, strategy_kwargs = args
    
    try:
        # 1. Instantiate Strategy
        strategy = strategy_class(**strategy_kwargs)
        
        # 2. Load Data (Handle Directory or File)
        import pathlib
        import polars as pl
        p = pathlib.Path(filepath)
        
        df = None
        if p.is_dir():
            # Load directory -> Merge -> Pandas (VectorizedStrategy expects Pandas)
            # Ensure unique and sorted
            df_source = pl.scan_parquet(str(p / "*.parquet")).collect().unique(subset=["ts_1s"]).sort("ts_1s").to_pandas()
            if hasattr(strategy, 'process_data'):
                df = strategy.process_data(df_source)
            else:
                # If strategy only has process_file but we have a dir, we can't help unless we save temp?
                # Or strategy supports dir?
                if hasattr(strategy, 'process_file'):
                     df = strategy.process_file(filepath)
        else:
            # File
            if hasattr(strategy, 'process_file'):
                df = strategy.process_file(filepath)

        if df is None or df.is_empty(): return []
        
        if df is None or df.is_empty(): return []
        
        # Add row number/index for iteration
        df = df.with_row_count("index")
        
        # Convert to Numpy for fast filtering/jumping
        # We need: index, low, high, open_time, entry_signal, close
        arr_signals = df['entry_signal'].to_numpy()
        arr_high = df['high'].to_numpy()
        arr_low = df['low'].to_numpy()
        arr_close = df['close'].to_numpy()
        
        # Handle Time Column
        if 'open_time' in df.columns:
            arr_time = df['open_time'].to_list()
        elif 'ts_1s' in df.columns:
            arr_time = df['ts_1s'].to_list()
        else:
            # Fallback for unexpected schema
            arr_time = [str(x) for x in range(len(df))]
 
        
        completed_trades = []
        curr_idx = 0
        max_idx = len(df)
        
        tp_pct = strategy.tp
        sl_pct = strategy.sl
        bet_size = getattr(strategy, 'bet_size', 7.0)
        
        while curr_idx < max_idx:
            # 1. Find Next Signal
            future_signals = arr_signals[curr_idx:]
            true_indices = np.where(future_signals)[0]
            
            if len(true_indices) == 0:
                break # No more signals
            
            # Global Index of next signal
            entry_idx = curr_idx + true_indices[0]
            
            # Execute Entry
            entry_price = arr_close[entry_idx]
            entry_time = arr_time[entry_idx]
            
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)
            
            # 2. Find Exit
            # Scan slice: [entry_idx + 1 : ]
            if entry_idx + 1 >= max_idx:
                break
                
            search_slice_high = arr_high[entry_idx+1:]
            search_slice_low = arr_low[entry_idx+1:]
            
            # SL Hit: High >= SL
            sl_hits = np.where(search_slice_high >= sl_price)[0]
            # TP Hit: Low <= TP
            tp_hits = np.where(search_slice_low <= tp_price)[0]
            
            first_sl_idx = sl_hits[0] if len(sl_hits) > 0 else 999999999
            first_tp_idx = tp_hits[0] if len(tp_hits) > 0 else 999999999
            
            if first_sl_idx == 999999999 and first_tp_idx == 999999999:
                break # Never exits
                
            if first_sl_idx <= first_tp_idx:
                local_exit_idx = first_sl_idx
                exit_type = "SL"
                exit_price = sl_price
                pnl_pct = (entry_price - exit_price) / entry_price # SHORT logic
            else:
                local_exit_idx = first_tp_idx
                exit_type = "TP"
                exit_price = tp_price
                pnl_pct = (entry_price - exit_price) / entry_price # SHORT logic
                
            real_exit_idx = (entry_idx + 1) + local_exit_idx
            exit_time = arr_time[real_exit_idx]
            
            pnl_usd = bet_size * pnl_pct
            
            completed_trades.append(Trade(
                symbol=os.path.basename(filepath).replace('.parquet',''),
                entry_time=str(entry_time),
                exit_time=str(exit_time),
                type=exit_type,
                entry_price=entry_price,
                exit_price=exit_price,
                pnl_percent=pnl_pct,
                pnl_usd=pnl_usd,
                duration_min=0,
                pump_percent=0.0 # Can extract if needed
            ))
            
            # Jump to after exit
            curr_idx = real_exit_idx + 1
            
        return completed_trades

    except Exception as e:
        print(f"Error Polars {filepath}: {e}")
        return []


