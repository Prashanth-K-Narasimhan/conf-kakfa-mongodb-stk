#!/usr/bin/env python3
"""
create_pipeline.py

Idempotent creation script for pipeline.

This version starts a lightweight background producer thread after topics are created.
Purpose:
 - Kick-start traffic into the `telemetry` topic so ksqlDB has data to materialize CTAS/TABLEs.
 - Producer runs for PRODUCER_RUN_SECONDS (default 30) or until materialization completes.

Usage:
  python create_pipeline.py                # create resources, skip existing objects
  python create_pipeline.py --replace      # replace existing KSQL objects and connectors (drop+create)

Notes:
 - This is intentionally conservative and non-blocking: producer runs in a daemon thread and is stopped
   after table materialization wait completes (or after the configured duration).
 - You can configure behavior via environment variables:
     PRODUCER_RUN_SECONDS  (default: 30)
     PRODUCER_NUM_DEVICES  (default: 3)
     PRODUCER_RATE         (ticks per second, default: 1.0)
     PRODUCER_MISS_RATE    (probability a metric is missing, default: 0.1)
     PRODUCER_BOOTSTRAP    (Kafka bootstrap, default matches KAFKA_BOOTSTRAP)
     PRODUCER_TOPIC        (topic to publish to, default 'telemetry')
"""

import os
import re
import json
import time
import random
import threading
import argparse
import requests
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
from kafka import KafkaProducer, TopicPartition, KafkaConsumer
from pymongo import MongoClient

# Path note (original uploaded file path for reference if needed)
ORIGINAL_CREATE_PATH = "/mnt/data/create_pipeline.py"

# Default configurable endpoints (env overrides)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
KSQL_URL = os.environ.get("KSQL_URL", "http://localhost:8088")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "telemetry_db")
KSQ_DIR = os.environ.get("KSQL_DIR", "ksql")
CONNECTORS_DIR = os.environ.get("CONNECTORS_DIR", "connectors")

# Producer config (background producer)
PRODUCER_RUN_SECONDS = int(os.environ.get("PRODUCER_RUN_SECONDS", "30"))
PRODUCER_NUM_DEVICES = int(os.environ.get("PRODUCER_NUM_DEVICES", "3"))
PRODUCER_RATE = float(os.environ.get("PRODUCER_RATE", "1.0"))  # ticks per second
PRODUCER_MISS_RATE = float(os.environ.get("PRODUCER_MISS_RATE", "0.1"))
PRODUCER_BOOTSTRAP = os.environ.get("PRODUCER_BOOTSTRAP", KAFKA_BOOTSTRAP)
PRODUCER_TOPIC = os.environ.get("PRODUCER_TOPIC", "telemetry")

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

# ------------------ Background producer (lightweight) ------------------

class DeviceState:
    def __init__(self, did, odo_start=100.0):
        self.did = str(did)
        self.odo = float(odo_start)

    def bump_odometer(self):
        # small deterministic-ish increment
        self.odo += random.uniform(0.01, 0.5)
        self.odo = round(self.odo, 2)
        return self.odo

def iso_now(ts_ms=None):
    from datetime import datetime, timezone
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()

def mk_metric_message(did: str, name: str, event_ts_ms: int, odo_value=None):
    base = {
        "did": str(did),
        "timestamp": int(event_ts_ms),
        "name": name,
        "eventTime": iso_now(event_ts_ms),
        "receivedTime": iso_now()
    }

    if name == "odometer":
        base["float_value"] = float(odo_value) if odo_value is not None else round(random.uniform(100.0, 200.0), 2)
    elif name == "soc":
        # state-of-charge: 0-100 integer
        base["int_value"] = random.randint(10, 100)
    elif name == "speed":
        base["int_value"] = random.randint(20, 120)
    elif name == "ignition_status":
        base["int_value"] = random.choice([0, 1])
    else:
        base["int_value"] = None
        base["float_value"] = None

    # strip None
    return {k: v for k, v in base.items() if v is not None}

def background_producer(stop_event: threading.Event, num_devices=3, rate=1.0, miss_rate=0.1, bootstrap=PRODUCER_BOOTSTRAP, topic=PRODUCER_TOPIC):
    """
    Lightweight producer that emits bursts of per-device metrics at `rate` ticks/sec.
    Each tick emits up to 4 metrics per device (odometer, soc, speed, ignition_status).
    """
    devices = [DeviceState(did=str(1000 + i), odo_start=100.0 + i*5.0) for i in range(num_devices)]
    p = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=5
    )

    tick_interval = 1.0 / max(0.001, rate)
    try:
        tick = 0
        while not stop_event.is_set():
            tick += 1
            msgs = []
            for dev in devices:
                event_ts = int(time.time() * 1000) - random.randint(0, 500)
                odo = dev.bump_odometer()

                metrics = [
                    ("odometer", {"float_value": odo}),
                    ("soc", {}),
                    ("speed", {}),
                    ("ignition_status", {})
                ]
                for name, _meta in metrics:
                    if random.random() < miss_rate:
                        continue
                    msg = mk_metric_message(dev.did, name, event_ts, odo_value=odo if name == "odometer" else None)
                    # send
                    try:
                        p.send(topic, key=dev.did, value=msg)
                    except Exception as e:
                        # non-fatal for background producer
                        print("[producer] send error:", e)
            # flush occasionally
            try:
                p.flush(timeout=2)
            except Exception:
                pass

            # interval wait
            for _ in range(int(max(1, tick_interval * 10))):
                if stop_event.is_set():
                    break
                time.sleep(tick_interval / max(1, int(tick_interval * 10)))
    finally:
        try:
            p.flush(timeout=2)
        except Exception:
            pass
        try:
            p.close()
        except Exception:
            pass

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
        from kafka import KafkaConsumer, TopicPartition
        try:
            tp = TopicPartition(table_name, 0)
            c = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, enable_auto_commit=False)
            c.poll(timeout_ms=100)
            end = c.end_offsets([tp]).get(tp, 0)
            c.close()
            if end > 0:
                print(f"[ksql] topic {table_name} has {end} records; materialized.")
                return True
        except Exception:
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
    p.add_argument("--no-produce", action="store_true", help="Do not start the background producer")
    args = p.parse_args()

    print("=== pipeline create start ===")
    create_kafka_topics()
    setup_mongo()

    # Start background producer if allowed
    producer_stop = threading.Event()
    producer_thread = None
    if not args.no_produce:
        print(f"[producer] starting background producer for up to {PRODUCER_RUN_SECONDS}s "
              f"(devices={PRODUCER_NUM_DEVICES}, rate={PRODUCER_RATE}/s, miss_rate={PRODUCER_MISS_RATE})")
        producer_thread = threading.Thread(
            target=background_producer,
            args=(producer_stop, PRODUCER_NUM_DEVICES, PRODUCER_RATE, PRODUCER_MISS_RATE, PRODUCER_BOOTSTRAP, PRODUCER_TOPIC),
            daemon=True,
            name="bg-producer"
        )
        producer_thread.start()
        # give the producer a brief warmup so topics get some messages before KSQL is applied
        time.sleep(min(3, PRODUCER_RUN_SECONDS))

    # Apply KSQL
    time.sleep(0.5)
    apply_ksql_files(replace=args.replace)

    # Wait for table to materialize - while producer is running
    materialized = wait_for_table_materialization("vehicle_latest", timeout=90)

    # Stop background producer if it was started
    if producer_thread and producer_thread.is_alive():
        print("[producer] stopping background producer")
        producer_stop.set()
        producer_thread.join(timeout=5)

    # Register connectors after KSQL objects are applied
    register_connectors(replace=args.replace)
    print("=== pipeline create complete ===")

if __name__ == "__main__":
    main()
