import os

# Infra
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KSQL_URL = os.getenv("KSQL_URL", "http://localhost:8088")
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "telemetry_db")

# dirs
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KSQL_DIR = os.getenv("KSQL_DIR", os.path.join(BASE_DIR, "ksql"))
CONNECTORS_DIR = os.getenv("CONNECTORS_DIR", os.path.join(BASE_DIR, "connectors"))

# Producer settings
PRODUCER_BOOTSTRAP = os.getenv("PRODUCER_BOOTSTRAP", KAFKA_BOOTSTRAP)
PRODUCER_RUN_SECONDS = int(os.getenv("PRODUCER_RUN_SECONDS", "30"))
PRODUCER_NUM_DEVICES = int(os.getenv("PRODUCER_NUM_DEVICES", "3"))
PRODUCER_RATE = float(os.getenv("PRODUCER_RATE", "1.0"))
PRODUCER_TOPIC = os.getenv("PRODUCER_TOPIC", "telemetry_raw")
