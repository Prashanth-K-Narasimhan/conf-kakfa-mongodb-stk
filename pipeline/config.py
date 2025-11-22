"""
Central config with environment overrides.
"""
import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
KSQL_URL = os.environ.get("KSQL_URL", "http://localhost:8088")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "telemetry_db")

KSQ_DIR = os.environ.get("KSQL_DIR", "pipeline/ksql")
CONNECTORS_DIR = os.environ.get("CONNECTORS_DIR", "pipeline/connectors")

PRODUCER_RUN_SECONDS = int(os.environ.get("PRODUCER_RUN_SECONDS", "30"))
PRODUCER_NUM_DEVICES = int(os.environ.get("PRODUCER_NUM_DEVICES", "3"))
PRODUCER_RATE = float(os.environ.get("PRODUCER_RATE", "1.0"))
PRODUCER_MISS_RATE = float(os.environ.get("PRODUCER_MISS_RATE", "0.1"))
PRODUCER_TOPIC = os.environ.get("PRODUCER_TOPIC", "telemetry_raw")
PRODUCER_BOOTSTRAP = os.environ.get("PRODUCER_BOOTSTRAP", KAFKA_BOOTSTRAP)

KSQL_TABLE_MATERIALIZE_TIMEOUT = int(os.environ.get("KSQL_TABLE_MATERIALIZE_TIMEOUT", "90"))
