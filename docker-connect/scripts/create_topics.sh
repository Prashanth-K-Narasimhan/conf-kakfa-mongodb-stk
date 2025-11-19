#!/usr/bin/env bash
# scripts/create-topics.sh
# Pre-create required Kafka topics for dev (Docker Desktop).
# Usage: bash scripts/create-topics.sh
set -euo pipefail

BOOTSTRAP="localhost:29092"
KAFKA_IMAGE="confluentinc/cp-kafka:7.4.1"

echo "Creating topic: telemetry (raw)"
docker run --rm --network host ${KAFKA_IMAGE} \
  kafka-topics --bootstrap-server ${BOOTSTRAP} \
  --create --topic telemetry --partitions 1 --replication-factor 1 || true

echo "Creating topic: telemetry_transformed (stream)"
docker run --rm --network host ${KAFKA_IMAGE} \
  kafka-topics --bootstrap-server ${BOOTSTRAP} \
  --create --topic telemetry_transformed --partitions 1 --replication-factor 1 || true

echo "Creating topic: telemetry_raw (raw audit stream - optional sink to Mongo)"
docker run --rm --network host ${KAFKA_IMAGE} \
  kafka-topics --bootstrap-server ${BOOTSTRAP} \
  --create --topic telemetry_raw --partitions 1 --replication-factor 1 || true

echo "Creating topic: vehicle_latest (table backing topic, compacted)"
docker run --rm --network host ${KAFKA_IMAGE} \
  kafka-topics --bootstrap-server ${BOOTSTRAP} \
  --create --topic vehicle_latest --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact || true

echo "Done. Current topics:"
docker run --rm --network host ${KAFKA_IMAGE} kafka-topics --bootstrap-server ${BOOTSTRAP} --list
