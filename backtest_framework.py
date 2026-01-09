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

class BacktestEngine:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        
    def _extract_base_pair(self, symbol: str) -> str:
        """Extract base pair without timeframe suffix. e.g. BTCUSDT_30s -> BTCUSDT"""
        parts = symbol.split('_')
        return parts[0] if parts else symbol
        
    def run(self, strategy_class, max_positions=10, avg_threshold=0.10, **strategy_kwargs) -> pd.DataFrame:
        files = [os.path.join(self.data_dir, f) for f in os.listdir(self.data_dir) if f.endswith('.parquet')]
        if not files:
            print("❌ No data files found.")
            return pd.DataFrame()

        print(f"🚀 Running Backtest on {len(files)} pairs...")
        print(f"📊 Pyramid Mode: max_positions={max_positions}, avg_threshold={avg_threshold*100:.0f}%")
        
        all_trades = []
        
        # Process each symbol independently (memory efficient)
        for filepath in files:
            symbol = os.path.basename(filepath).replace('.parquet', '')
            try:
                trades = self._process_symbol(filepath, symbol, strategy_class, **strategy_kwargs)
                all_trades.extend(trades)
            except Exception as e:
                pass
                
        print(f"📊 Raw trades before pyramid filter: {len(all_trades)}")
        
        # Post-process: Apply pyramid strategy
        all_trades = self._apply_pyramid_strategy(all_trades, max_positions, avg_threshold)
        
        print(f"✅ Final trades after pyramid: {len(all_trades)}")
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

    def _process_symbol(self, filepath, symbol, strategy_class, **kwargs):
        df = pd.read_parquet(filepath)
        if df.empty: return []
        
        strategy = strategy_class(**kwargs)
        
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
        entry_price = 0.0
        tp_price = 0.0
        sl_price = 0.0
        entry_time = None
        
        completed_trades = []
        
        for i in range(len(closes)):
            current_time = times[i]
            
            if in_position:
                exit_price = 0.0
                exit_type = ""
                
                if highs[i] >= sl_price:
                    exit_price = sl_price
                    exit_type = "SL"
                elif lows[i] <= tp_price:
                    exit_price = tp_price
                    exit_type = "TP"
                    
                if exit_type:
                    pnl_pct = (entry_price - exit_price) / entry_price
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
                        duration_min=0
                    ))
                    in_position = False
                    continue
            
            if not in_position:
                decision = strategy.on_candle(
                    timestamp=current_time,
                    open=opens[i],
                    high=highs[i],
                    low=lows[i],
                    close=closes[i]
                )
                
                if decision and decision.get('action') == 'SHORT':
                    entry_price = decision['entry_price']
                    tp_price = decision['tp']
                    sl_price = decision['sl']
                    entry_time = current_time
                    
                    if highs[i] >= sl_price:
                        pnl_pct = (entry_price - sl_price) / entry_price
                        completed_trades.append(Trade(
                            symbol=symbol,
                            entry_time=str(entry_time),
                            exit_time=str(current_time),
                            type="SL_INSTANT",
                            entry_price=entry_price,
                            exit_price=sl_price,
                            pnl_percent=pnl_pct,
                            pnl_usd=strategy.bet_size * pnl_pct,
                            duration_min=0
                        ))
                    elif lows[i] <= tp_price:
                        pnl_pct = (entry_price - tp_price) / entry_price
                        completed_trades.append(Trade(
                            symbol=symbol,
                            entry_time=str(entry_time),
                            exit_time=str(current_time),
                            type="TP_INSTANT",
                            entry_price=entry_price,
                            exit_price=tp_price,
                            pnl_percent=pnl_pct,
                            pnl_usd=strategy.bet_size * pnl_pct,
                            duration_min=0
                        ))
                    else:
                        in_position = True
                        
        return completed_trades
