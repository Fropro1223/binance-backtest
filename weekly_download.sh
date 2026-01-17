#!/bin/bash
# =============================================================================
# WEEKLY DATA DOWNLOAD SCRIPT
# =============================================================================
# Bu script her Pazar gece 03:00'te çalışarak son 7 günün verilerini indirir.
# Cron job ile otomatik çalıştırılır.
# =============================================================================

set -e

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
DATA_DIR="$PROJECT_DIR/data"
VENV_DIR="$PROJECT_DIR/.venv"
LOG_FILE="$PROJECT_DIR/weekly_download.log"

# Logging
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=========================================="
log "Starting Weekly Data Download"
log "=========================================="

# Activate virtual environment
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
    log "Virtual environment activated"
else
    log "ERROR: Virtual environment not found at $VENV_DIR"
    exit 1
fi

# Change to project directory (src is here, not in data/)
cd "$PROJECT_DIR"

# Run Data Manager (Handles Manifest & Logic automatically)
log "Running Data Manager..."
python -m src.data_manager 2>&1 | tee -a "$LOG_FILE"

log "=========================================="
log "Weekly Data Download Complete"
log "=========================================="

# Optional: Deactivate venv
deactivate 2>/dev/null || true
