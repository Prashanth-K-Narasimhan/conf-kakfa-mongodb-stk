#!/bin/bash
podman run --rm --network host confluentinc/cp-kafka:7.4.11 kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1