#!/bin/bash
# run_all_tfs.sh
# Runs backtest for all timeframes sequentially

TIMEFRAMES=("5s" "10s" "15s" "30s" "45s" "1m")

echo "=========================================="
echo "🚀 STARTING MULTI-TIMEFRAME BACKTEST"
echo "Strategy: Pump 2%, Marubozu 0.80, TP 2%, SL 2%"
echo "=========================================="

for tf in "${TIMEFRAMES[@]}"
do
    echo ""
    echo "------------------------------------------"
    echo "⏳ Running for Timeframe: $tf"
    echo "------------------------------------------"
    
    /Users/firat/Algo/binance-backtest/.venv/bin/python3 main.py \
        --strategy vectorized \
        --side LONG \
        --tp 2 \
        --sl 2 \
        --pump 2 \
        --tf "$tf"
        
    echo "✅ Completed $tf"
    sleep 2
done

echo ""
echo "=========================================="
echo "🏁 ALL TIMEFRAMES COMPLETED"
echo "=========================================="
