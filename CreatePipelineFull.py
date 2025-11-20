#!/usr/bin/env python3
"""
CreatePipelineFull.py

Full end-to-end create + verification script.

Usage:
  python CreatePipelineFull.py            # create resources, start producer, verify, stop
  python CreatePipelineFull.py --replace  # replace existing KSQL objects and connectors

Notes:
 - Run from repository root so that relative folders (ksql/, connectors/, connect-plugins/) align with docker-compose mounts.
 - Designed to be simple and robust for local dev on Docker Desktop (WSL2).
"""

from __future__ import annotations
import os
import re
import json
import time
import random
import threading
import argparse
import requests
from typing import Optional, Tuple

# Third-party libs (ensure virtualenv has these installed)
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import TopicAlreadyExistsError
from pymongo import MongoClient

# ---------------- Config (env overrides) ----------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
KSQL_URL = os.environ.get("KSQL_URL", "http://localhost:8088")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "telemetry_db")

KSQ_DIR = os.environ.get("KSQL_DIR", "ksql")
CONNECTORS_DIR = os.environ.get("CONNECTORS_DIR", "connectors")

# Background producer defaults
PRODUCER_RUN_SECONDS = int(os.environ.get("PRODUCER_RUN_SECONDS", "30"))
PRODUCER_NUM_DEVICES = int(os.environ.get("PRODUCER_NUM_DEVICES", "3"))
PRODUCER_RATE = float(os.environ.get("PRODUCER_RATE", "1.0"))
PRODUCER_MISS_RATE = float(os.environ.get("PRODUCER_MISS_RATE", "0.1"))
PRODUCER_TOPIC = os.environ.get("PRODUCER_TOPIC", "telemetry")
PRODUCER_BOOTSTRAP = os.environ.get("PRODUCER_BOOTSTRAP", KAFKA_BOOTSTRAP)

# KSQL/Connect timeouts
KSQL_TABLE_MATERIALIZE_TIMEOUT = int(os.environ.get("KSQL_TABLE_MATERIALIZE_TIMEOUT", "90"))

# validators for mongo collections (keeps original intent)
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

# ---------------- Utilities ----------------
def log(*parts):
    print("[{}]".format(time.strftime("%H:%M:%S")), *parts)

# ---------------- Kafka topic creation ----------------
def wait_for_kafka(timeout=60) -> bool:
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

def create_kafka_topics() -> None:
    if not wait_for_kafka(timeout=30):
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
            log("kafka topics already present")
            return
        try:
            admin.create_topics(new_topics=to_create, validate_only=False)
            created = []
            for nt in to_create:
                name = getattr(nt, "topic", None) or getattr(nt, "name", None) or "<unknown>"
                created.append(name)
            log("kafka created topics:", ", ".join(created))
        except TopicAlreadyExistsError:
            log("kafka some topics already existed (race). OK.")
        except Exception as e:
            log("kafka create topics exception:", e)
    finally:
        admin.close()

# ---------------- Mongo setup ----------------
def wait_for_mongo(timeout=60) -> bool:
    start = time.time()
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

def setup_mongo() -> None:
    if not wait_for_mongo(timeout=30):
        raise RuntimeError("Mongo not reachable at " + MONGO_URL)
    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]

    # telemetry_raw
    if "telemetry_raw" in db.list_collection_names():
        try:
            db.command({"collMod": "telemetry_raw", "validator": RAW_VALIDATOR, "validationLevel": "moderate"})
            log("mongo telemetry_raw validator updated")
        except Exception as e:
            log("mongo telemetry_raw collMod non-fatal:", e)
    else:
        db.create_collection("telemetry_raw", validator=RAW_VALIDATOR, validationLevel="moderate")
        log("mongo telemetry_raw created")
    db.telemetry_raw.create_index([("did", 1), ("timestamp", 1)])
    log("mongo telemetry_raw index ensured")

    # vehicle_latest
    if "vehicle_latest" in db.list_collection_names():
        try:
            db.command({"collMod": "vehicle_latest", "validator": LATEST_VALIDATOR, "validationLevel": "moderate"})
            log("mongo vehicle_latest validator updated")
        except Exception as e:
            log("mongo vehicle_latest collMod non-fatal:", e)
    else:
        db.create_collection("vehicle_latest", validator=LATEST_VALIDATOR, validationLevel="moderate")
        log("mongo vehicle_latest created")
    try:
        db.vehicle_latest.create_index("did", unique=True)
        log("mongo vehicle_latest unique index ensured")
    except Exception as e:
        log("mongo vehicle_latest unique index non-fatal:", e)

    client.close()

# ---------------- KSQL helpers ----------------
def ksql_endpoint(path="/ksql") -> str:
    return KSQL_URL.rstrip("/") + path

def ksql_post(statement: str, streams_props: Optional[dict] = None) -> requests.Response:
    url = ksql_endpoint("/ksql")
    payload = {"ksql": statement, "streamsProperties": streams_props or {}}
    r = requests.post(url, json=payload, timeout=60)
    return r

def ksql_show(kind: str) -> list:
    url = ksql_endpoint("/ksql")
    payload = {"ksql": f"SHOW {kind};", "streamsProperties": {}}
    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()
    names = []
    for item in r.json():
        key = kind.lower()
        if key in item:
            for ent in item[key]:
                names.append(ent.get("name", "").upper())
    return names

def parse_first_create(ksql_text: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(r"CREATE\s+(STREAM|TABLE)\s+([A-Za-z0-9_]+)", ksql_text, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper(), m.group(2).upper()
    return None, None

def drop_ksql(obj_type: str, name: str) -> requests.Response:
    stmt = f"DROP {obj_type} {name};"
    return ksql_post(stmt)

def apply_ksql_files(replace: bool = False) -> None:
    if not os.path.isdir(KSQ_DIR):
        log("ksql dir not present; skipping ksql apply")
        return
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
                    log("ksql dropping existing stream", obj_name)
                    drop_ksql("STREAM", obj_name)
                else:
                    log("ksql stream exists, skipping:", obj_name)
                    continue
            if obj_type == "TABLE" and obj_name in tables:
                if replace:
                    log("ksql dropping existing table", obj_name)
                    drop_ksql("TABLE", obj_name)
                else:
                    log("ksql table exists, skipping:", obj_name)
                    continue

        log("ksql applying", fname)
        streams_props = {"ksql.streams.auto.offset.reset": "earliest"}
        r = ksql_post(ksql_text, streams_props=streams_props)
        if r.status_code not in (200, 201):
            log("ksql warning:", r.status_code, r.text)

def ksql_table_exists(table_name: str) -> bool:
    try:
        tables = ksql_show("TABLES")
        return table_name.upper() in tables
    except Exception:
        return False

def wait_for_table_materialization(table_name: str, timeout: int = KSQL_TABLE_MATERIALIZE_TIMEOUT) -> bool:
    start = time.time()
    while True:
        if not ksql_table_exists(table_name):
            if time.time() - start > timeout:
                log("ksql timeout waiting for table to exist:", table_name)
                return False
            time.sleep(1)
            continue
        # check backing topic end offsets
        try:
            tp = TopicPartition(table_name, 0)
            c = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, enable_auto_commit=False)
            c.poll(timeout_ms=100)
            end = c.end_offsets([tp]).get(tp, 0)
            c.close()
            if end > 0:
                log("ksql topic", table_name, "has", end, "records; materialized.")
                return True
        except Exception:
            pass
        if time.time() - start > timeout:
            log("ksql timeout waiting for table", table_name, "to be materialized")
            return False
        time.sleep(1)

# ---------------- Background producer ----------------
class DeviceState:
    def __init__(self, did: str, odo_start: float = 100.0):
        self.did = str(did)
        self.odo = float(odo_start)

    def bump_odometer(self) -> float:
        self.odo += random.uniform(0.01, 0.5)
        self.odo = round(self.odo, 2)
        return self.odo

def iso_now(ts_ms: Optional[int] = None) -> str:
    from datetime import datetime, timezone
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()

def mk_metric_message(did: str, name: str, event_ts_ms: int, odo_value: Optional[float] = None):
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
        base["int_value"] = random.randint(10, 100)
    elif name == "speed":
        base["int_value"] = random.randint(20, 120)
    elif name == "ignition_status":
        base["int_value"] = random.choice([0, 1])
    else:
        base["int_value"] = None
        base["float_value"] = None
    return {k: v for k, v in base.items() if v is not None}

def background_producer(stop_event: threading.Event,
                        num_devices: int = PRODUCER_NUM_DEVICES,
                        rate: float = PRODUCER_RATE,
                        miss_rate: float = PRODUCER_MISS_RATE,
                        bootstrap: str = PRODUCER_BOOTSTRAP,
                        topic: str = PRODUCER_TOPIC) -> None:
    devices = [DeviceState(did=str(1000 + i), odo_start=100.0 + i * 5.0) for i in range(num_devices)]
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
                    try:
                        p.send(topic, key=dev.did, value=msg)
                    except Exception as e:
                        log("producer send error:", e)
            try:
                p.flush(timeout=2)
            except Exception:
                pass
            # sleep with responsiveness to stop_event
            waited = 0.0
            while waited < tick_interval:
                if stop_event.is_set():
                    break
                sleep_chunk = min(0.1, tick_interval - waited)
                time.sleep(sleep_chunk)
                waited += sleep_chunk
    finally:
        try:
            p.flush(timeout=2)
        except Exception:
            pass
        try:
            p.close()
        except Exception:
            pass
        log("producer thread exiting")

# ---------------- KSQL streaming sample ----------------
def ksql_stream_sample(limit: int = 5, stream_name: str = "TELEMETRY_RAW") -> None:
    """Run a short push query and print rows. Requires producers to be active."""
    sql = f"SELECT * FROM {stream_name} EMIT CHANGES LIMIT {limit};"
    url = KSQL_URL.rstrip("/") + "/query-stream"
    log("ksql streaming sample SQL:", sql)
    try:
        r = requests.post(url, json={"sql": sql}, stream=True, timeout=(5, None))
    except Exception as e:
        log("ksql stream request failed:", e)
        return
    count = 0
    try:
        for raw in r.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                doc = json.loads(raw)
            except Exception:
                continue
            if isinstance(doc, dict) and "row" in doc:
                cols = doc["row"].get("columns", [])
                log(f"ksql stream row: {cols}")
                count += 1
                if count >= limit:
                    break
    finally:
        try:
            r.close()
        except Exception:
            pass
    log("ksql stream sample collected", count, "rows")

# ---------------- Connectors registration ----------------
def register_connectors(replace: bool = False) -> None:
    if not os.path.isdir(CONNECTORS_DIR):
        log("connectors dir missing; skipping connector registration")
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
            log("connect config invalid JSON:", fname, e)
            continue
        conn_name = cfg.get("name") or os.path.splitext(fname)[0]
        conn_url = f"{CONNECT_URL.rstrip('/')}/connectors/{conn_name}"
        if replace:
            try:
                r = requests.get(conn_url, timeout=5)
                if r.status_code == 200:
                    log("connect deleting existing connector", conn_name)
                    requests.delete(conn_url, timeout=5)
            except Exception:
                pass
        post_url = f"{CONNECT_URL.rstrip('/')}/connectors"
        try:
            r2 = requests.post(post_url, headers={"Content-Type": "application/json"}, data=json.dumps(cfg), timeout=15)
            if r2.status_code in (200, 201):
                log("connect created", conn_name)
            else:
                log("connect create failed", conn_name, r2.status_code, r2.text[:200])
        except Exception as e:
            log("connect create exception for", conn_name, e)

# ---------------- Mongo quick stats ----------------
def mongo_stats() -> Tuple[int, int]:
    try:
        client = MongoClient(MONGO_URL)
        db = client[DB_NAME]
        raw_count = db.telemetry_raw.estimated_document_count()
        latest_count = db.vehicle_latest.estimated_document_count()
        client.close()
        return raw_count, latest_count
    except Exception as e:
        log("mongo stats error:", e)
        return 0, 0

# ---------------- Main ----------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--replace", action="store_true", help="Replace existing KSQL objects/connectors")
    args = p.parse_args()

    log("pipeline create start")
    create_kafka_topics()
    setup_mongo()

    # Start background producer
    stop_event = threading.Event()
    producer_thread = threading.Thread(
        target=background_producer,
        args=(stop_event, PRODUCER_NUM_DEVICES, PRODUCER_RATE, PRODUCER_MISS_RATE, PRODUCER_BOOTSTRAP, PRODUCER_TOPIC),
        daemon=True,
        name="bg-producer"
    )
    producer_thread.start()
    log(f"started background producer (devices={PRODUCER_NUM_DEVICES}, rate={PRODUCER_RATE}/s)")

    # Warmup so some data exists before KSQL applied
    time.sleep(min(3, PRODUCER_RUN_SECONDS))

    # Apply KSQL objects
    apply_ksql_files(replace=args.replace)

    # Wait for table materialization (vehicle_latest)
    materialized = wait_for_table_materialization("vehicle_latest", timeout=KSQL_TABLE_MATERIALIZE_TIMEOUT)
    if not materialized:
        log("WARNING: vehicle_latest did not materialize within timeout")

    # While producer still running, sample the transformed stream
    log("sampling TELEMETRY_RAW stream while producer is active")
    ksql_stream_sample(limit=5, stream_name="TELEMETRY_RAW")

    # Stop the producer
    if producer_thread.is_alive():
        log("stopping background producer")
        stop_event.set()
        producer_thread.join(timeout=5)

    # Register connectors (post-KSQL)
    register_connectors(replace=args.replace)

    # Print quick Mongo stats
    raw_count, latest_count = mongo_stats()
    log(f"mongo telemetry_raw_count={raw_count}, vehicle_latest_count={latest_count}")

    log("pipeline create complete")

if __name__ == "__main__":
    main()
