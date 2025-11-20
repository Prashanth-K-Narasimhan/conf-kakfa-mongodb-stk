#!/usr/bin/env python3
"""
setup_pipeline.py

Host-run pipeline bootstrap:
 - create kafka topics (idempotent)
 - create mongo collections with validators and indexes (idempotent)
 - post ksql files
 - register connectors
"""

import os
import json
import time
import sys
import requests
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
from pymongo import MongoClient, errors

# config from env with sane defaults for local dev
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
KSQL_URL = os.environ.get("KSQL_URL", "http://localhost:8088/ksql")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083/connectors")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "telemetry_db")

# Detailed validators (the ones you pasted)
RAW_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["did", "timestamp", "name"],
        "properties": {
            "did": {"bsonType": "string"},
            "timestamp": {"bsonType": ["long", "int", "date"]},
            "name": {"bsonType": "string"},
            "int_value": {"bsonType": ["int", "null"]},
            "float_value": {"bsonType": ["double", "null"]},
            "string_value": {"bsonType": ["string", "null"]},
            "eventTime": {"bsonType": ["string", "null"]},
            "receivedTime": {"bsonType": ["string", "null"]},
            "ingest_rowtime": {"bsonType": ["date", "null"]}
        }
    }
}

LATEST_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["did"],
        "properties": {
            "did": {"bsonType": "string"},
            "odo": {"bsonType": ["double", "int", "null"]},
            "soc": {"bsonType": ["int", "null"], "minimum": 0, "maximum": 100},
            "speed": {"bsonType": ["int", "null"], "minimum": 0},
            "ignition": {"bsonType": ["int", "null"]},
            "last_event_ts": {"bsonType": ["long", "int", "date", "null"]}
        }
    }
}

def wait_for_kafka(timeout=120):
    start = time.time()
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=2000)
            admin.close()
            return True
        except Exception as e:
            if time.time() - start > timeout:
                print(f"[wait_for_kafka] Timeout waiting for Kafka at {KAFKA_BOOTSTRAP}: {e}", file=sys.stderr)
                return False
            time.sleep(1)

def create_kafka_topics():
    print("[kafka] Creating topics (if missing)...")
    if not wait_for_kafka(60):
        raise RuntimeError("Kafka not reachable; aborting topic creation")

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    topics = [
        NewTopic(name="telemetry", num_partitions=1, replication_factor=1),
        NewTopic(name="telemetry_raw", num_partitions=1, replication_factor=1),
        NewTopic(name="vehicle_latest", num_partitions=1, replication_factor=1),
    ]
    try:
        admin.create_topics(new_topics=topics, validate_only=False)
        print("[kafka] Topic create request sent.")
    except TopicAlreadyExistsError:
        print("[kafka] Some topics already existed. OK.")
    except KafkaError as e:
        # kafka-python aggregates errors; show and continue (idempotent)
        print(f"[kafka] KafkaError while creating topics: {e}")
    finally:
        admin.close()

    # Try to set cleanup.policy=compact for vehicle_latest using kafka-configs via docker CLI is flaky.
    # Best to set it via kafka-configs outside this script or ensure ksql creates compaction. We'll attempt via Admin API below only if supported.
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        # some kafka-python versions provide incremental_alter_configs, else skip silently
        # Not all environments need this; you can set compaction via broker tool if required.
        print("[kafka] Note: ensure vehicle_latest has cleanup.policy=compact (may need kafka-configs).")
        admin.close()
    except Exception:
        pass

def wait_for_mongo(timeout=120):
    start = time.time()
    while True:
        try:
            client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=2000)
            client.admin.command('ping')
            client.close()
            return True
        except Exception as e:
            if time.time() - start > timeout:
                print(f"[mongo] Timeout waiting for MongoDB at {MONGO_URL}: {e}", file=sys.stderr)
                return False
            time.sleep(1)

def setup_mongo():
    print(f"[mongo] Setting up collections on {MONGO_URL}/{DB_NAME}")
    if not wait_for_mongo(60):
        raise RuntimeError("MongoDB not reachable; aborting")

    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]

    # telemetry_raw
    if "telemetry_raw" in db.list_collection_names():
        print("[mongo] telemetry_raw exists; updating validator if possible.")
        try:
            db.command({"collMod": "telemetry_raw", "validator": RAW_VALIDATOR, "validationLevel": "moderate"})
            print("[mongo] telemetry_raw validator updated.")
        except Exception as e:
            print(f"[mongo] collMod for telemetry_raw failed (nonfatal): {e}")
    else:
        db.create_collection("telemetry_raw", validator=RAW_VALIDATOR, validationLevel="moderate")
        print("[mongo] telemetry_raw created with validator.")

    # index on telemetry_raw
    try:
        db.telemetry_raw.create_index([("did", 1), ("timestamp", 1)])
        print("[mongo] telemetry_raw index created/ensured.")
    except Exception as e:
        print(f"[mongo] Failed to create index on telemetry_raw: {e}")

    # vehicle_latest
    if "vehicle_latest" in db.list_collection_names():
        print("[mongo] vehicle_latest exists; updating validator if possible.")
        try:
            db.command({"collMod": "vehicle_latest", "validator": LATEST_VALIDATOR, "validationLevel": "moderate"})
            print("[mongo] vehicle_latest validator updated.")
        except Exception as e:
            print(f"[mongo] collMod for vehicle_latest failed (nonfatal): {e}")
    else:
        db.create_collection("vehicle_latest", validator=LATEST_VALIDATOR, validationLevel="moderate")
        print("[mongo] vehicle_latest created with validator.")

    # unique index on did
    try:
        db.vehicle_latest.create_index("did", unique=True)
        print("[mongo] vehicle_latest unique index on did created/ensured.")
    except Exception as e:
        print(f"[mongo] Failed to create unique index on vehicle_latest.did: {e}")

    client.close()

def apply_ksql_files():
    print("[ksql] Applying .ksql files from ./ksql")
    if not os.path.isdir("ksql"):
        print("[ksql] ksql folder not found; skipping")
        return

    for fname in sorted(os.listdir("ksql")):
        if not fname.endswith(".ksql"):
            continue
        full = os.path.join("ksql", fname)
        print(f"[ksql] Running {full}")
        with open(full, "r", encoding="utf-8") as fh:
            ksql_text = fh.read().strip()
        if not ksql_text:
            continue
        payload = {"ksql": ksql_text, "streamsProperties": {}}
        resp = requests.post(KSQL_URL, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
        if resp.status_code not in (200, 201):
            print(f"[ksql] Warning: ksql returned {resp.status_code} for {fname}: {resp.text}")
        else:
            print(f"[ksql] {fname} applied.")

def register_connectors():
    print("[connect] Registering connectors from ./connectors")
    if not os.path.isdir("connectors"):
        print("[connect] connectors folder not found; skipping")
        return

    for fname in sorted(os.listdir("connectors")):
        if not fname.endswith(".json"):
            continue
        full = os.path.join("connectors", fname)
        print(f"[connect] Posting {full}")
        with open(full, "r", encoding="utf-8") as fh:
            cfg = fh.read()
        resp = requests.post(CONNECT_URL, headers={"Content-Type": "application/json"}, data=cfg)
        if resp.status_code not in (200, 201):
            print(f"[connect] connector post returned {resp.status_code}: {resp.text}")
        else:
            print(f"[connect] {fname} posted.")

def main():
    print("=== pipeline setup start ===")
    create_kafka_topics()
    setup_mongo()
    # small wait for KSQL to be ready to accept commands
    print("[main] sleeping 3s before ksql apply")
    time.sleep(3)
    apply_ksql_files()
    register_connectors()
    print("=== pipeline setup complete ===")

if __name__ == "__main__":
    main()
