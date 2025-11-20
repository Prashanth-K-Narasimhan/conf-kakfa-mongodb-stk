#!/usr/bin/env python3
"""
Idempotent setup script for pipeline.

Usage:
  python setup_pipeline.py          # create resources, skip existing objects
  python setup_pipeline.py --replace   # replace existing KSQL objects and connectors (drop+create)

Run from repository root so relative paths ./ksql and ./connectors resolve.
"""

import os
import re
import json
import time
import argparse
import requests
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
from pymongo import MongoClient

# Default configurable endpoints (env overrides)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
KSQL_URL = os.environ.get("KSQL_URL", "http://localhost:8088")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "telemetry_db")
KSQ_DIR = os.environ.get("KSQL_DIR", "ksql")
CONNECTORS_DIR = os.environ.get("CONNECTORS_DIR", "connectors")

# validators (use your approved validators)
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


# ------------------ Helpers ------------------
def wait_for_kafka(timeout=60):
    start = time.time()
    while True:
        try:
            a = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=2000)
            a.close()
            return True
        except Exception:
            if time.time() - start > timeout:
                return False
            time.sleep(1)

def wait_for_mongo(timeout=60):
    start = time.time()
    from pymongo import MongoClient
    while True:
        try:
            client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            client.close()
            return True
        except Exception:
            if time.time() - start > timeout:
                return False
            time.sleep(1)

def ksql_endpoint(path="/ksql"):
    return KSQL_URL.rstrip("/") + path

def ksql_show(kind):
    """Return uppercase names list for SHOW STREAMS/TABLES"""
    url = ksql_endpoint("/ksql")
    payload = {"ksql": f"SHOW {kind};", "streamsProperties": {}}
    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()
    names = []
    for item in r.json():
        key = kind.lower()
        if key in item:
            for ent in item[key]:
                names.append(ent.get("name","").upper())
    return names

def ksql_post(statement, streams_props=None):
    url = ksql_endpoint("/ksql")
    payload = {"ksql": statement, "streamsProperties": streams_props or {}}
    r = requests.post(url, json=payload, timeout=60)
    return r

def parse_first_create(ksql_text):
    m = re.search(r"CREATE\s+(STREAM|TABLE)\s+([A-Za-z0-9_]+)", ksql_text, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper(), m.group(2).upper()
    return None, None

def drop_ksql(obj_type, name):
    stmt = f"DROP {obj_type} {name};"
    return ksql_post(stmt)

# ------------------ Kafka topics ------------------
def create_kafka_topics():
    if not wait_for_kafka():
        raise RuntimeError("Kafka not reachable at " + KAFKA_BOOTSTRAP)

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        existing = set(admin.list_topics())
        desired = {
            "telemetry": NewTopic("telemetry", num_partitions=1, replication_factor=1),
            "telemetry_raw": NewTopic("telemetry_raw", num_partitions=1, replication_factor=1),
            "vehicle_latest": NewTopic("vehicle_latest", num_partitions=1, replication_factor=1),
        }

        to_create = [t for name, t in desired.items() if name not in existing]
        if not to_create:
            print("[kafka] topics already present")
            return

        try:
            admin.create_topics(new_topics=to_create, validate_only=False)
            created_names = []
            for nt in to_create:
                # robustly get topic name - handle .topic or .name depending on kafka-python version
                name = getattr(nt, "topic", None) or getattr(nt, "name", None)
                created_names.append(name or "<unknown>")
            print("[kafka] created:", ", ".join(created_names))
        except TopicAlreadyExistsError:
            # another client created them in the meantime - that's fine
            print("[kafka] some topics already existed (race). OK.")
        except Exception as e:
            print("[kafka] create topics exception:", e)
    finally:
        admin.close()

# ------------------ Mongo ------------------
def setup_mongo():
    if not wait_for_mongo():
        raise RuntimeError("Mongo not reachable at " + MONGO_URL)
    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]

    # telemetry_raw
    if "telemetry_raw" in db.list_collection_names():
        try:
            db.command({"collMod": "telemetry_raw", "validator": RAW_VALIDATOR, "validationLevel": "moderate"})
            print("[mongo] telemetry_raw validator updated")
        except Exception as e:
            print("[mongo] telemetry_raw collMod failed (non-fatal):", e)
    else:
        db.create_collection("telemetry_raw", validator=RAW_VALIDATOR, validationLevel="moderate")
        print("[mongo] telemetry_raw created")

    db.telemetry_raw.create_index([("did",1),("timestamp",1)])
    print("[mongo] telemetry_raw index ensured")

    # vehicle_latest
    if "vehicle_latest" in db.list_collection_names():
        try:
            db.command({"collMod": "vehicle_latest", "validator": LATEST_VALIDATOR, "validationLevel": "moderate"})
            print("[mongo] vehicle_latest validator updated")
        except Exception as e:
            print("[mongo] vehicle_latest collMod failed (non-fatal):", e)
    else:
        db.create_collection("vehicle_latest", validator=LATEST_VALIDATOR, validationLevel="moderate")
        print("[mongo] vehicle_latest created")

    try:
        db.vehicle_latest.create_index("did", unique=True)
        print("[mongo] vehicle_latest unique index ensured")
    except Exception as e:
        print("[mongo] vehicle_latest unique index creation failed (non-fatal):", e)

    client.close()

# ------------------ KSQL apply ------------------
def apply_ksql_files(replace=False):
    if not os.path.isdir(KSQ_DIR):
        print("[ksql] no ksql dir; skipping")
        return

    # prefetch
    try:
        streams = ksql_show("STREAMS")
    except Exception:
        streams = []
    try:
        tables = ksql_show("TABLES")
    except Exception:
        tables = []

    for fname in sorted(os.listdir(KSQ_DIR)):
        if not fname.endswith(".ksql"):
            continue
        path = os.path.join(KSQ_DIR, fname)
        with open(path, "r", encoding="utf-8") as fh:
            ksql_text = fh.read().strip()
        if not ksql_text:
            continue

        # skip runtime push queries
        # if re.search(r"\bSELECT\b.*\bEMIT\s+CHANGES\b", ksql_text, flags=re.IGNORECASE | re.DOTALL):
        #     print(f"[ksql] skipping runtime query file {fname}")
        #     continue

        obj_type, obj_name = parse_first_create(ksql_text)
        if obj_type and obj_name:
            if obj_type == "STREAM" and obj_name in streams:
                if replace:
                    print(f"[ksql] dropping existing stream {obj_name}")
                    drop_ksql("STREAM", obj_name)
                else:
                    print(f"[ksql] stream {obj_name} exists; skipping (use --replace to force)")
                    continue
            if obj_type == "TABLE" and obj_name in tables:
                if replace:
                    print(f"[ksql] dropping existing table {obj_name}")
                    drop_ksql("TABLE", obj_name)
                else:
                    print(f"[ksql] table {obj_name} exists; skipping (use --replace to force)")
                    continue

        print(f"[ksql] applying {fname}")
        # When creating CTAS/streams we want to ensure reading from earliest (for test runs)
        streams_props = {"ksql.streams.auto.offset.reset": "earliest"}
        r = ksql_post(ksql_text, streams_props=streams_props)
        if r.status_code not in (200,201):
            print("[ksql] warning, non-2xx response:", r.status_code, r.text)

def ksql_table_exists(table_name):
    try:
        tables = ksql_show("TABLES")
        return table_name.upper() in tables
    except Exception:
        return False

def wait_for_table_materialization(table_name, timeout=60):
    # Wait until ksql tells us the table exists and the backing topic has at least one message
    start = time.time()
    while True:
        if not ksql_table_exists(table_name):
            if time.time() - start > timeout:
                print(f"[ksql] timeout waiting for table {table_name} to exist")
                return False
            time.sleep(1)
            continue

        # check Kafka topic end offset for vehicle_latest
        # quick hack: use kafka-python consumer to get end offsets
        from kafka import KafkaConsumer, TopicPartition
        try:
            tp = TopicPartition(table_name, 0)
            c = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, enable_auto_commit=False)
            c.poll(timeout_ms=100)  # ensure init
            end = c.end_offsets([tp]).get(tp, 0)
            c.close()
            if end > 0:
                print(f"[ksql] topic {table_name} has {end} records; materialized.")
                return True
        except Exception as e:
            # keep waiting
            pass

        if time.time() - start > timeout:
            print(f"[ksql] timeout waiting for table {table_name} to be materialized")
            return False
        time.sleep(1)

# ------------------ Connectors ------------------
def register_connectors(replace=False):
    if not os.path.isdir(CONNECTORS_DIR):
        print("[connect] connectors dir not found; skipping")
        return

    for fname in sorted(os.listdir(CONNECTORS_DIR)):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(CONNECTORS_DIR, fname)
        with open(path, "r", encoding="utf-8") as fh:
            cfg_text = fh.read()
        try:
            cfg = json.loads(cfg_text)
        except Exception as e:
            print("[connect] invalid JSON in", fname, e)
            continue

        conn_name = cfg.get("name") or os.path.splitext(fname)[0]
        conn_url = f"{CONNECT_URL.rstrip('/')}/connectors/{conn_name}"

        if replace:
            try:
                r = requests.get(conn_url, timeout=5)
                if r.status_code == 200:
                    print(f"[connect] deleting existing connector {conn_name}")
                    requests.delete(conn_url, timeout=5)
            except Exception:
                pass

        # create
        post_url = f"{CONNECT_URL.rstrip('/')}/connectors"
        r2 = requests.post(post_url, headers={"Content-Type":"application/json"}, data=json.dumps(cfg), timeout=15)
        if r2.status_code in (200,201):
            print(f"[connect] {conn_name} created")
        else:
            print(f"[connect] failed creating {conn_name}: {r2.status_code} {r2.text}")

# ------------------ Main ------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--replace", action="store_true", help="Replace existing KSQL objects/connectors instead of skipping")
    args = p.parse_args()

    print("=== pipeline setup start ===")
    create_kafka_topics()
    setup_mongo()
    time.sleep(2)
    apply_ksql_files(replace=args.replace)
    wait_for_table_materialization("vehicle_latest", timeout=90)
    register_connectors(replace=args.replace)
    print("=== pipeline setup complete ===")

if __name__ == "__main__":
    main()
