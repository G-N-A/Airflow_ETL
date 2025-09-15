#!/bin/bash

# =============================================
# Synthetic E-commerce Data Stream Runner
# Streams synthetic data to SQL Server
# =============================================

set -euo pipefail

# ----------------------------
# Utility logging
# ----------------------------
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Graceful shutdown on Ctrl+C
trap 'log "🛑 Stream interrupted by user"; exit 130' INT

# ----------------------------
# Resolve paths
# ----------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SRC_DIR="$PROJECT_ROOT/src"

# ----------------------------
# Configuration
# ----------------------------
DB_USER="sa"
DB_PASS="Gova#ss123"
DB_NAME="ecom_db"
SERVER="localhost:1433"
WEBHOOK_URL="http://localhost:5000/webhook"

INTERVAL_MINUTES=5       # <- change interval as needed
ORDERS_PER_INTERVAL=50
CLICKS_PER_INTERVAL=100

DB_URL="mssql+pyodbc://${DB_USER}:$(python3 -c "import urllib.parse; print(urllib.parse.quote_plus('${DB_PASS}'))")@${SERVER}/${DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"

# ----------------------------
# Display config (mask password)
# ----------------------------
log "🚀 Starting Synthetic E-commerce Data Stream..."
echo "----------------------------------------------"
echo " Database   : ${DB_NAME}"
echo " User       : ${DB_USER}"
echo " Server     : ${SERVER}"
echo " Interval   : ${INTERVAL_MINUTES} min"
echo " Orders     : ${ORDERS_PER_INTERVAL}"
echo " Clicks     : ${CLICKS_PER_INTERVAL}"
echo " Webhook    : ${WEBHOOK_URL}"
echo "----------------------------------------------"
echo ""

# ----------------------------
# Pre-checks
# ----------------------------
if ! command -v python3 &>/dev/null; then
  log "❌ Python3 not found in PATH"
  exit 1
fi

if ! python3 -c "import pyodbc" &>/dev/null; then
  log "❌ Python module 'pyodbc' is not installed"
  exit 1
fi

if [ ! -f "$SRC_DIR/synth_ecom_mssql_stream.py" ]; then
  log "❌ Script not found: $SRC_DIR/synth_ecom_mssql_stream.py"
  exit 1
fi

if [ -d "$PROJECT_ROOT/airflow_venv" ]; then
  log "🔧 Activating virtual environment..."
  source "$PROJECT_ROOT/airflow_venv/bin/activate"
fi

# ----------------------------
# DB connection check
# ----------------------------
log "🔍 Checking database connection..."
if ! python3 - <<EOF
import pyodbc, sys
try:
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=${DB_NAME};UID=${DB_USER};PWD=${DB_PASS};TrustServerCertificate=yes",
        timeout=5
    )
    conn.close()
    print("✅ Database connection successful")
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    sys.exit(1)
EOF
then
  exit 1
fi

# ----------------------------
# Start streaming
# ----------------------------
log "🔄 Starting synthetic data stream..."
echo "Press Ctrl+C to stop"
echo ""

python3 "$SRC_DIR/synth_ecom_mssql_stream.py" \
  --db-url "${DB_URL}" \
  --webhook-url "${WEBHOOK_URL}" \
  --interval-minutes "${INTERVAL_MINUTES}" \
  --orders-per-interval "${ORDERS_PER_INTERVAL}" \
  --clicks-per-interval "${CLICKS_PER_INTERVAL}"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  log "✅ Synthetic data streaming completed successfully!"
else
  log "❌ Synthetic data streaming failed! Exit code: $EXIT_CODE"
  exit $EXIT_CODE
fi