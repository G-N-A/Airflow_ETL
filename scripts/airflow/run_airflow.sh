#!/bin/bash

# Robust Airflow launcher:
# - Stops existing processes
# - Sets env
# - Cleans stale PIDs
# - Initializes DB
# - Ensures admin user
# - Restarts webserver & scheduler in background with logs

set -euo pipefail

PROJECT_ROOT="/media/softsuave/DATA-HDD/DataEngineering/Apache_Airflow"
export AIRFLOW_HOME="$PROJECT_ROOT"
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_HOME/dags"
export AIRFLOW__CORE__LOG_FILENAME_TEMPLATE="dag_id={{ ti.dag_id }}/run_id={{ ts_nodash }}/task_id={{ ti.task_id }}/{% if ti.map_index >= 0 %}map_index={{ ti.map_index }}/{% endif %}attempt={{ try_number }}.log"
LOG_DIR="$PROJECT_ROOT/logs/airflow"
mkdir -p "$LOG_DIR"

# Trap to cleanup on exit
trap 'echo "⚠️ Script aborted unexpectedly"; exit 1' INT TERM

# Activate virtual environment
if [ -d "$PROJECT_ROOT/airflow_venv" ]; then
  echo "🔧 Activating virtual environment..."
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/airflow_venv/bin/activate"
fi

AIRFLOW_BIN="$(command -v airflow)"
if [ -z "$AIRFLOW_BIN" ]; then
  echo "❌ Airflow binary not found! Make sure it's installed in venv."
  exit 1
fi

# Helper to kill process by pattern if running
kill_if_running() {
  local pattern="$1"
  local pids
  pids=$(pgrep -f "$pattern" || true)
  if [ -n "$pids" ]; then
    echo "🛑 Stopping $pattern (PIDs: $pids)"
    kill $pids || true
    sleep 2
    local pids2
    pids2=$(pgrep -f "$pattern" || true)
    if [ -n "$pids2" ]; then
      echo "⚠️  Forcing kill for $pattern"
      kill -9 $pids2 || true
    fi
  fi
}

echo "🚫 Stopping existing Airflow processes if any..."
kill_if_running "airflow webserver"
kill_if_running "airflow scheduler"

# Clean potential stale PID files
find "$AIRFLOW_HOME" -type f -name "*.pid" -delete 2>/dev/null || true

# Initialize/upgrade DB
echo "🗄️  Initializing Airflow DB..."
$AIRFLOW_BIN db check || $AIRFLOW_BIN db upgrade || $AIRFLOW_BIN db init

# Ensure admin user exists (idempotent)
if ! $AIRFLOW_BIN users list --output json | grep -q '"username": "admin"'; then
  echo "👤 Creating default admin user (admin/admin)"
  $AIRFLOW_BIN users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true
fi

# Start webserver
echo "🚀 Starting Airflow Webserver on :8085"
nohup "$AIRFLOW_BIN" webserver -p 8085 > "$LOG_DIR/webserver.out" 2>&1 &

# Start scheduler
echo "⏱️  Starting Airflow Scheduler"
nohup "$AIRFLOW_BIN" scheduler > "$LOG_DIR/scheduler.out" 2>&1 &

sleep 3

echo "✅ Airflow started successfully!"
echo "🌐 UI: http://localhost:8085"
echo "📄 Logs stored in: $LOG_DIR"
echo "📦 AIRFLOW_HOME: $AIRFLOW_HOME"
