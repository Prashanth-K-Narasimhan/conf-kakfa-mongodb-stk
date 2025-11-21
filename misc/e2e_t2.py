#!/usr/bin/env python3
"""
Simple E2E test: produce -> ksqlDB -> MongoDB

Behavior:
  1. Produce a small set of sample telemetry messages to Kafka topic 'telemetry'.
  2. Confirm topic has messages.
  3. Poll KSQL pull query against VEHICLE_LATEST for a DID until it appears (timeout).
  4. Check MongoDB collections telemetry_raw and vehicle_latest for expected documents.

Run from repository root (PowerShell):
  python .\tests\e2e_test.py
"""

import os
import time
import json
import sys
import requests
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient

# Config (env overrides)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
KSQL_URL = os.environ.get("KSQL_URL", "http://localhost:8088")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "telemetry_db")
TEST_DID = os.environ.get("TEST_DID", "981")

# Sample messages (one device, a few metrics)
SAMPLE_MESSAGES = [
    {"did": TEST_DID, "timestamp": 1761559635000, "name": "odometer", "float_value": 101.50, "eventTime": "2025-10-27T10:07:15.000Z", "receivedTime": "2025-10-27T10:07:15.875Z"},
    {"did": TEST_DID, "timestamp": 1761559635234, "name": "soc", "int_value": 100, "eventTime": "2025-10-27T10:07:15.234Z", "receivedTime": "2025-10-27T10:07:16.875Z"},
    {"did": TEST_DID, "timestamp": 1761559636061, "name": "speed", "int_value": 25, "eventTime": "2025-10-27T10:07:16.061Z", "receivedTime": "2025-10-27T10:07:16.567Z"},
    {"did": TEST_DID, "timestamp": 1761559636985, "name": "ignition_status", "int_value": 1, "eventTime": "2025-10-27T10:07:16.985Z", "receivedTime": "2025-10-27T10:07:17.324Z"},
]

# Timeouts
TOPIC_CHECK_TIMEOUT = 10   # seconds
TABLE_POLL_TIMEOUT = 30    # seconds
TABLE_POLL_INTERVAL = 2    # seconds

def produce_messages():
    print("[produce] connecting to kafka:", KAFKA_BOOTSTRAP)
    p = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8")
    )
    for rec in SAMPLE_MESSAGES:
        key = rec["did"]
        p.send("telemetry", key=key, value=rec)
        print(f"[produce] sent key={key} name={rec['name']} ts={rec['timestamp']}")
    p.flush()
    p.close()
    print("[produce] done")

def topic_has_messages(topic="telemetry", timeout=TOPIC_CHECK_TIMEOUT):
    print(f"[consume] checking topic {topic} for messages (timeout {timeout}s)...")
    end = time.time() + timeout
    c = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000
    )
    seen = 0
    try:
        while time.time() < end:
            try:
                msg = next(c)
                seen += 1
                # print a short preview
                try:
                    val = json.loads(msg.value.decode("utf-8"))
                    print("[consume] got message:", val.get("name"))
                except Exception:
                    print("[consume] got raw message")
                if seen >= 1:
                    break
            except StopIteration:
                time.sleep(0.5)
    finally:
        c.close()
    print(f"[consume] consumed {seen} messages")
    return seen > 0

def ksql_pull_query_table(did, timeout=TABLE_POLL_TIMEOUT, interval=TABLE_POLL_INTERVAL):
    """
    Poll a pull query against VEHICLE_LATEST to get the latest state for did.
    Returns the parsed row (list or dict) or None.
    """
    stmt = f"SELECT DID, ODO, SOC, SPEED, IGNITION, LAST_EVENT_TS FROM VEHICLE_LATEST WHERE DID = '{did}';"
    url = KSQL_URL.rstrip("/") + "/query"
    headers = {"Content-Type": "application/vnd.ksql.v1+json"}
    start = time.time()
    last_text = None
    while time.time() - start < timeout:
        try:
            r = requests.post(url, json={"ksql": stmt, "streamsProperties": {}}, headers=headers, timeout=10)
            last_text = r.text
            if r.status_code != 200:
                # sometimes ksql returns 200 with no rows - handle gracefully
                # print short debug if it's an error
                print("[ksql] non-200 response:", r.status_code, r.text[:200])
            else:
                # try parse JSON response
                try:
                    parsed = r.json()
                    # look for row items
                    for item in parsed:
                        if isinstance(item, dict) and "row" in item:
                            cols = item["row"].get("columns", [])
                            return cols
                        # some responses may return arrays
                        if isinstance(item, list) and item:
                            return item[0]
                except Exception:
                    # fallback: try to find a JSON array inside text
                    try:
                        start_idx = r.text.find('[')
                        end_idx = r.text.rfind(']')
                        if start_idx != -1 and end_idx != -1:
                            arr = json.loads(r.text[start_idx:end_idx+1])
                            if arr:
                                return arr[0]
                    except Exception:
                        pass
        except Exception as e:
            print("[ksql] request error:", e)
        time.sleep(interval)
    print("[ksql] timed out waiting for VEHICLE_LATEST row; last response snippet:", (last_text or "")[:300])
    return None

def check_mongo_for_did(did):
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        raw_count = db.telemetry_raw.count_documents({"did": did})
        latest_doc = db.vehicle_latest.find_one({"did": did})
        client.close()
        return raw_count, latest_doc
    except Exception as e:
        print("[mongo] error connecting:", e)
        return 0, None

def main():
    print("=== e2e test start ===")
    produce_messages()

    # Quick check the topic has messages
    if not topic_has_messages("telemetry", timeout=TOPIC_CHECK_TIMEOUT):
        print("FAIL: no messages observed on topic 'telemetry'")
        sys.exit(2)

    # Small grace for KSQL/connect to process messages
    print("[main] sleeping 2s before KSQL checks")
    time.sleep(2)

    # Poll table for DID
    row = ksql_pull_query_table(TEST_DID)
    if row:
        print("[main] VEHICLE_LATEST row (pull query):", row)
    else:
        print("[main] VEHICLE_LATEST pull query did not return a row")

    # Check MongoDB
    raw_count, latest_doc = check_mongo_for_did(TEST_DID)
    print(f"[mongo] telemetry_raw_count= {raw_count}")
    print(f"[mongo] vehicle_latest_doc_present= {bool(latest_doc)}")

    ok_all = True
    if raw_count == 0:
        print("FAIL: telemetry_raw has no documents for did=", TEST_DID)
        ok_all = False
    if not latest_doc:
        print("FAIL: vehicle_latest document not found for did=", TEST_DID)
        ok_all = False

    if ok_all:
        print("=== e2e test PASSED ===")
        sys.exit(0)
    else:
        print("=== e2e test FAILED ===")
        sys.exit(3)

if __name__ == "__main__":
    main()
