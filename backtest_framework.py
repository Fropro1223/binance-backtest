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
    args: (filepath, strategy_class, check_current_candle, strategy_kwargs)
    """
    filepath, strategy_class, check_current_candle, strategy_kwargs = args
    
    # Extract action_func if provided (it should be passed in strategy_kwargs or handled separately)
    # Since we pack everything into strategy_kwargs in run(), we need to pop it to avoid init errors
    # if the Strategy class doesn't expect it.
    action_func = strategy_kwargs.pop('action_func', None)
    
    try:
        df = pd.read_parquet(filepath)
        if df.empty: return []

        symbol = os.path.basename(filepath).replace('.parquet', '')
        strategy = strategy_class(**strategy_kwargs)
        
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
                        
                    pnl_usd = strategy.bet_size * pnl_pct
                    
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
                # 1. Update State (Conditions)
                # strategy here is actually the "Conditions" instance
                strategy.on_candle(
                    timestamp=current_time,
                    open=opens[i],
                    high=highs[i],
                    low=lows[i],
                    close=closes[i]
                )
                
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
                    decision = action_func(strategy, candle_data)
                
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
                                    pnl_pct = (tp_price - entry_price) / entry_price

                            if instant_exit:
                                completed_trades.append(Trade(
                                    symbol=symbol,
                                    entry_time=str(entry_time),
                                    exit_time=str(current_time),
                                    type=exit_type,
                                    entry_price=entry_price,
                                    exit_price=exit_price,
                                    pnl_percent=pnl_pct,
                                    pnl_usd=strategy.bet_size * pnl_pct,
                                    duration_min=0,
                                    pump_percent=entry_pump
                                ))
                        
                        if not instant_exit:
                            in_position = True
                            position_side = action
                        
        return completed_trades
    except Exception as e:
        # print(f"Error processing {filepath}: {e}")
        return []

class BacktestEngine:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        
    def _extract_base_pair(self, symbol: str) -> str:
        """Extract base pair without timeframe suffix. e.g. BTCUSDT_30s -> BTCUSDT"""
        parts = symbol.split('_')
        return parts[0] if parts else symbol
        
    def run(self, strategy_class, max_positions=10, avg_threshold=0.10, parallel=True, workers=None, check_current_candle=True, **strategy_kwargs) -> pd.DataFrame:
        files = [os.path.join(self.data_dir, f) for f in os.listdir(self.data_dir) if f.endswith('.parquet')]
        if not files:
            print("❌ No data files found.")
            return pd.DataFrame()

        import multiprocessing
        from concurrent.futures import ProcessPoolExecutor
        
        num_workers = workers if workers else os.cpu_count()
        
        print(f"🚀 Running Backtest on {len(files)} pairs...")
        if parallel:
             print(f"⚡ Parallel Mode: {num_workers} workers")
        else:
             print(f"🐌 Serial Mode")
             
        print(f"📊 Pyramid Mode: max_positions={max_positions}, avg_threshold={avg_threshold*100:.0f}%")
        
        all_trades = []
        
        if parallel:
            # Prepare arguments for each worker
            # We pass strategy_kwargs as a dict to be unpacked in the worker
            tasks = [(f, strategy_class, check_current_candle, strategy_kwargs) for f in files]
            
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                results = list(executor.map(process_single_pair, tasks))
                
            for res in results:
                all_trades.extend(res)
        else:
            # Serial fallback
            for filepath in files:
                # We reuse the static logic for serial as well
                res = process_single_pair((filepath, strategy_class, check_current_candle, strategy_kwargs))
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

