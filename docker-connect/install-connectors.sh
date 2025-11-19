#!/bin/bash
set -e

# This script tries to install the MongoDB connector via confluent-hub.
# In many dev environments the Hub install can fail. You can instead
# manually download the connector and put it under ./docker-connect/connect-plugins

if command -v confluent-hub >/dev/null 2>&1; then
  echo "Confluent Hub found. Attempting to install mongodb/kafka-connect-mongodb:2.2.0"
  confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:2.2.0 || {
    echo "confluent-hub install failed. You can manually download and drop jars into ./docker-connect/connect-plugins"
  }
else
  echo "confluent-hub not available in build environment. Skipping automatic install."
  echo "Please download the MongoDB Kafka Connector package from Confluent Hub"
  echo "and extract/copy its contents into ./docker-connect/connect-plugins before starting the stack."
fi
