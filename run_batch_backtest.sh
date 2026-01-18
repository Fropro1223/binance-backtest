#!/bin/bash
TFS=("5s" "10s" "15s" "30s" "45s" "1m")
for tf in "${TFS[@]}"; do
    echo "========================================"
    echo "Running backtest for TF: $tf"
    echo "========================================"
    .venv/bin/python main.py --side SHORT --ema bull --pump 3 --tp 3 --sl 3 --marubozu 0.8 --tf "$tf"
done

echo "========================================"
echo "Consolidating results..."
echo "========================================"
.venv/bin/python log_timeframe_breakdown.py
