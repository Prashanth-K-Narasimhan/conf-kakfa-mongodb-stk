#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="localhost:29092"
KAFKA_IMAGE="confluentinc/cp-kafka:7.4.1"

echo "Creating topic: telemetry (raw ingest)"
docker run --rm --network host ${KAFKA_IMAGE} \
  kafka-topics --bootstrap-server ${BOOTSTRAP} \
  --create --topic telemetry --partitions 1 --replication-factor 1 || true

echo "Creating topic: telemetry_raw (normalized/enriched history)"
docker run --rm --network host ${KAFKA_IMAGE} \
  kafka-topics --bootstrap-server ${BOOTSTRAP} \
  --create --topic telemetry_raw --partitions 1 --replication-factor 1 || true

echo "Creating topic: vehicle_latest (table backing topic, compacted)"
docker run --rm --network host ${KAFKA_IMAGE} \
  kafka-topics --bootstrap-server ${BOOTSTRAP} \
  --create --topic vehicle_latest --partitions 1 --replication-factor 1 || true

# set cleanup.policy=compact for vehicle_latest (make it compacted)
docker run --rm --network host ${KAFKA_IMAGE} \
  kafka-configs --bootstrap-server ${BOOTSTRAP} --entity-type topics --entity-name vehicle_latest --alter --add-config cleanup.policy=compact || true

echo "Done. Current topics:"
docker run --rm --network host ${KAFKA_IMAGE} kafka-topics --bootstrap-server ${BOOTSTRAP} --list
