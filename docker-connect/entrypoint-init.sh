#!/usr/bin/env bash
set -euo pipefail

# This wrapper:
# 1) attempts to install connectors via confluent-hub (if available)
# 2) if install fails, copies any local jars found under /tmp/connect-plugins-local to Connect plugin dir
# 3) waits for Kafka to be reachable and runs the create-topics helper
# 4) waits for ksqlDB to be reachable and POSTs all .ksql files (via /scripts/run_ksql_files.sh)
# 5) execs the original Confluent/Connect run script so connect runs as normal

CONNECT_PLUGIN_DIR="/usr/share/confluent-hub-components"
LOCAL_PLUGINS_DIR="/tmp/connect-plugins-local"

log() { echo "[init] $*"; }

# 1) try install via confluent-hub (if present)
if command -v confluent-hub >/dev/null 2>&1; then
  log "Confluent Hub CLI found. Trying automatic install (this may fail in restricted environments)."
  # example connector; you can change name/version if desired
  set +e
  confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:2.2.0
  RC=$?
  set -e
  if [ $RC -eq 0 ]; then
    log "Confluent Hub install succeeded."
  else
    log "Confluent Hub install failed (exit $RC). Will try local jars (if present)."
  fi
else
  log "Confluent Hub CLI not available in image. Will try local jars (if present)."
fi

# 2) copy local jar(s) as fallback (if present)
if [ -d "${LOCAL_PLUGINS_DIR}" ] && [ "$(ls -A ${LOCAL_PLUGINS_DIR} 2>/dev/null || true)" ]; then
  log "Found local plugin files under ${LOCAL_PLUGINS_DIR}. Copying into ${CONNECT_PLUGIN_DIR}."
  # ensure plugin dir exists (it does)
  # copy as root then change ownership
  cp -r "${LOCAL_PLUGINS_DIR}/." "${CONNECT_PLUGIN_DIR}/" || true
  log "Local connectors copied."
else
  log "No local connectors found under ${LOCAL_PLUGINS_DIR}."
fi

# 3) start background process that will create Kafka topics and run ksql files BEFORE starting Connect
# We will wait for Kafka, then run create-topics; then wait for ksql and run ksql files.

# helper: wait for TCP port
wait_for_tcp() {
  local host=$1; local port=$2; local timeout=${3:-120}
  log "Waiting for TCP ${host}:${port} (timeout ${timeout}s)..."
  local start=$(date +%s)
  while true; do
    if nc -z "$host" "$port" >/dev/null 2>&1; then
      log "Host ${host}:${port} is reachable."
      return 0
    fi
    now=$(date +%s)
    if [ $((now - start)) -ge "$timeout" ]; then
      log "Timeout waiting for ${host}:${port}"
      return 1
    fi
    sleep 2
  done
}

background_init() {
  {
    # Wait for Kafka broker
    if wait_for_tcp "kafka" 9092 120; then
      log "Kafka reachable. Running create-topics script."
      /tmp/create-topics.sh || log "create-topics script failed (exit $?)."
    else
      log "Kafka not reachable - skipping create-topics (will continue)."
    fi

    # Wait for ksqlDB server and then run ksql files
    if wait_for_tcp "ksqldb-server" 8088 120; then
      log "ksqlDB reachable. Running ksql files via /tmp/run_ksql_files.sh"
      /tmp/run_ksql_files.sh || log "run_ksql_files.sh failed."
    else
      log "ksqlDB not reachable - skipping run of .ksql files."
    fi

    # WAIT a few seconds for the ksql objects to stabilize
    sleep 5

    # Now create MongoDB collections and indexes (before connectors start)
    if command -v python3 >/dev/null 2>&1; then
      log "Creating MongoDB collections and indexes using Python script."
      python3 /tmp/create-mongo-collections.py || log "create-mongo-collections.py failed."
    else
      log "python3 not found in container; skipping MongoDB collection creation. Make sure to create collections manually before connectors run."
    fi

    # After MongoDB collections exist, create MongoDB sink connectors
    sleep 2
    log "Creating MongoDB sink connectors..."

    CONNECT_URL="http://localhost:8083/connectors"

    if [ -f "/tmp/connectors/mongo-sink-vehicle_latest.json" ]; then
      log "Posting mongo-sink-vehicle_latest.json"
      # allow failures but log them
      curl -s -X POST -H "Content-Type: application/json" \
        --data @/tmp/connectors/mongo-sink-vehicle_latest.json \
        "${CONNECT_URL}" || log "Failed to post vehicle_latest connector"
    else
      log "mongo-sink-vehicle_latest.json not found under /tmp/connectors/"
    fi

    if [ -f "/tmp/connectors/mongo-sink-telemetry_raw.json" ]; then
      log "Posting mongo-sink-telemetry_raw.json"
      curl -s -X POST -H "Content-Type: application/json" \
        --data @/tmp/connectors/mongo-sink-telemetry_raw.json \
        "${CONNECT_URL}" || log "Failed to post telemetry_raw connector"
    else
      log "mongo-sink-telemetry_raw.json not found under /tmp/connectors/"
    fi

    log "Init tasks finished."
  } &
}


# start background init tasks
background_init

# finally exec the original container CMD (start connect) - this stays in foreground
exec "$@"
