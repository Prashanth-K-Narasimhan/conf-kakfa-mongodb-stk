#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="kafka:9092"   # use internal docker network hostnames when running inside container
KAFKA_IMAGE="confluentinc/cp-kafka:7.4.1"

log() { echo "[create-topics] $*"; }

create_topic_cmd() {
  local topic=$1
  shift
  if command -v kafka-topics >/dev/null 2>&1; then
    # run using local kafka-topics bin
    kafka-topics --bootstrap-server "${BOOTSTRAP}" --create --topic "${topic}" "$@"
  elif command -v docker >/dev/null 2>&1; then
    # fallback: run a transient Docker container on host (usehost network) - works from host not inside container
    echo "kafka-topics not available; fallback to docker run (host)."
    docker run --rm --network host ${KAFKA_IMAGE} kafka-topics --bootstrap-server localhost:29092 --create --topic "${topic}" "$@"
  else
    echo "No kafka-topics binary and no docker available to create topic ${topic}. Skipping."
    return 1
  fi
}

log "Creating topic: telemetry (raw)"
create_topic_cmd "telemetry" --partitions 1 --replication-factor 1 || true

log "Creating topic: telemetry_raw (stream)"
create_topic_cmd "telemetry_raw" --partitions 1 --replication-factor 1 || true

log "Creating topic: telemetry_raw (raw audit stream - optional sink to Mongo)"
create_topic_cmd "telemetry_raw" --partitions 1 --replication-factor 1 || true

log "Creating topic: vehicle_latest (table backing topic, compacted)"
# set cleanup.policy=compact for table topic
if command -v kafka-topics >/dev/null 2>&1; then
  kafka-topics --bootstrap-server "${BOOTSTRAP}" --create --topic vehicle_latest --partitions 1 --replication-factor 1 --config cleanup.policy=compact || true
else
  # fallback: create then alter config
  docker run --rm --network host ${KAFKA_IMAGE} kafka-topics --bootstrap-server localhost:29092 --create --topic vehicle_latest --partitions 1 --replication-factor 1 || true
  docker run --rm --network host ${KAFKA_IMAGE} kafka-configs --bootstrap-server localhost:29092 --entity-type topics --entity-name vehicle_latest --alter --add-config cleanup.policy=compact || true
fi

log "Done."
