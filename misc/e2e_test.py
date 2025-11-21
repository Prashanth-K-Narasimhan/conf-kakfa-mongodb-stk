#!/usr/bin/env python3
"""
e2e_test.py

End-to-end test:
 - produce sample telemetry messages to Kafka topic "telemetry"
 - verify messages appear on the telemetry topic
 - run a KSQL stream query (LIMIT) and a pull query against VEHICLE_LATEST
 - verify MongoDB collections telemetry_raw and vehicle_latest have expected data

Configure via env vars:
  KAFKA_BOOTSTRAP (default localhost:29092)
  KSQL_URL       (default http://localhost:8088)
  CONNECT_URL    (default http://localhost:8083)
  MONGO_URI      (default mongodb://localhost:27017)
  DB_NAME        (default telemetry_db)
"""

import os
import time
import json
import sys
import requests
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
from requests.exceptions import ReadTimeout, ConnectionError

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
KSQL_URL = os.environ.get("KSQL_URL", "http://localhost:8088")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "telemetry_db")
TEST_DEVICE = os.environ.get("TEST_DID", "981")

SAMPLE_MESSAGES = [
    {"did":"981","timestamp":1761559635000,"name":"odometer","value_type":"0","float_value":101.50,"string_value":"","int_value":"0","eventTime":"2025-10-27T10:07:15.000Z","receivedTime":"2025-10-27T10:07:15.875Z"},
    {"did":"981","timestamp":1761559635234,"name":"soc","value_type":"1","float_value":0,"string_value":"","int_value":100,"eventTime":"2025-10-27T10:07:15.234Z","receivedTime":"2025-10-27T10:07:16.875Z"},
    {"did":"981","timestamp":1761559636061,"name":"speed","value_type":"1","float_value":0,"string_value":"","int_value":25,"eventTime":"2025-10-27T10:07:16.061Z","receivedTime":"2025-10-27T10:07:16.567Z"},
    {"did":"981","timestamp":1761559636985,"name":"ignition_status","value_type":"1","float_value":0,"string_value":"","int_value":1,"eventTime":"2025-10-27T10:07:16.985Z","receivedTime":"2025-10-27T10:07:17.324Z"},
]

def produce_messages():
    print("[produce] connecting to kafka:", KAFKA_BOOTSTRAP)
    p = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8")
    )
    for rec in SAMPLE_MESSAGES:
        key = rec["did"]
        p.send("telemetry", key=key, value=rec)
        print(f"[produce] sent key={key} name={rec['name']} ts={rec['timestamp']}")
    p.flush()
    p.close()
    print("[produce] done")

def consume_sample_check(expected_count=8, timeout=20):
    """
    Light check that the telemetry topic has messages.
    We create a consumer and read up to expected_count messages or until timeout.
    """
    print("[consume] checking telemetry topic (short consume)...")
    c = KafkaConsumer(
        "telemetry",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000
    )
    collected = []
    start = time.time()
    while time.time() - start < timeout and len(collected) < expected_count:
        try:
            msg = next(c)
            try:
                val = json.loads(msg.value.decode("utf-8"))
                collected.append(val)
                print("[consume] got message:", val.get("name"))
            except Exception:
                collected.append(msg.value)
        except StopIteration:
            time.sleep(0.5)
    c.close()
    print(f"[consume] consumed {len(collected)} messages")
    return len(collected) > 0

def ksql_post(ksql_stmt, endpoint="/ksql", timeout=20):
    url = KSQL_URL.rstrip("/") + endpoint
    payload = {"ksql": ksql_stmt, "streamsProperties": {}}
    headers = {"Content-Type": "application/vnd.ksql.v1+json"}
    r = requests.post(url, json=payload, headers=headers, timeout=timeout)
    return r

def query_ksql_stream(limit=10, ksql_sql=None):
    """
    Run a push (EMIT CHANGES) KSQL query and return up to `limit` rows.
    Uses /query-stream and streams the response, then stops after `limit`.
    """
    if ksql_sql is None:
        ksql_sql = "SELECT * FROM TELEMETRY_STREAM EMIT CHANGES LIMIT {};".format(limit)

    url = KSQL_URL.rstrip("/") + "/query-stream"
    headers = {"Content-Type": "application/json"}  # or 'application/vnd.ksql.v1+json'
    payload = {"sql": ksql_sql}

    print("[ksql] POST", ksql_sql)
    # connect timeout short, read timeout None (don't timeout on chunked stream)
    try:
        r = requests.post(url, json=payload, headers=headers, stream=True, timeout=(5, None))
    except Exception as e:
        print("[ksql] POST failed:", e)
        return []

    rows = []
    try:
        # iter_lines() yields bytes for each chunk/line. decode and parse JSON.
        for raw in r.iter_lines(decode_unicode=True):
            if raw is None or raw == "" or raw.strip() == "":
                continue
            # server sometimes sends metadata objects, skip those without 'row'
            try:
                doc = json.loads(raw)
            except Exception:
                # not JSON or partial chunk; skip
                continue

            # push query row format: {"row": {"columns": [...]} }
            if isinstance(doc, dict) and "row" in doc and "columns" in doc["row"]:
                cols = doc["row"]["columns"]
                rows.append(cols)
                # stop when limit reached
                if len(rows) >= limit:
                    break
            # some messages may be control/heartbeat; ignore
    except (ReadTimeout, ConnectionError) as e:
        # If the server hangs between rows, a ReadTimeout may be raised only if you set a read timeout.
        # With timeout=(5, None) this usually won't occur. If it does, we handle gracefully.
        print("[ksql] stream read error (non-fatal):", e)
    finally:
        try:
            r.close()
        except Exception:
            pass

    print("[ksql] stream rows:", len(rows))
    return rows

def query_vehicle_latest(did, timeout=20):
    """
    Pull query against VEHICLE_LATEST (table). Returns parsed row or None.
    """
    stmt = f"SELECT DID, ODO, SOC, SPEED, IGNITION, LAST_EVENT_TS FROM VEHICLE_LATEST WHERE DID = '{did}';"
    print("[ksql] POST SELECT", stmt)
    # /query endpoint is used for pull queries as well
    r = ksql_post(stmt, endpoint="/query", timeout=10)
    if r.status_code != 200:
        # some setups return non-json; try to parse fallback
        print("[ksql] non-200 response:", r.status_code, r.text)
        return None
    # ksql sometimes returns lines where the first line is a json metadata object and next line is an array row
    txt = r.text.strip()
    try:
        parsed = r.json()
        # If JSON parse succeeds and is list, attempt to extract data rows
        # Typical successful pull returns first metadata then a JSON array line; we'll search array-like content
        for item in parsed:
            if isinstance(item, dict) and "row" in item:
                cols = item["row"].get("columns", [])
                return cols
        # fallback: if top-level is an array-of-arrays, return first array row
        for item in parsed:
            if isinstance(item, list):
                return item
    except Exception:
        # fall back to best-effort parse: search for first '[' followed by ']' and json.load it
        try:
            start = txt.find('[')
            end = txt.rfind(']')
            if start != -1 and end != -1 and end > start:
                arr = json.loads(txt[start:end+1])
                return arr
        except Exception:
            pass
    return None

def check_mongo(did):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    raw_count = db.telemetry_raw.count_documents({"did": did})
    latest_doc = db.vehicle_latest.find_one({"did": did})
    client.close()
    return raw_count, latest_doc

def wait_for_condition(check_fn, timeout=30, interval=2, desc="condition"):
    start = time.time()
    while time.time() - start < timeout:
        ok, info = check_fn()
        if ok:
            return True, info
        time.sleep(interval)
    return False, info

def main():
    print("=== e2e test start ===")
    produce_messages()

    # quick check that topic has messages
    topic_ok = consume_sample_check()
    if not topic_ok:
        print("FAIL: no messages observed on topic 'telemetry'")
        sys.exit(2)

    # allow time for KSQL/connect to process - a short wait + retries follows
    print("[main] sleeping 3s before KSQL/connect checks")
    time.sleep(3)

    # Try to get a snapshot of stream (non-blocking)
    stream_rows = query_ksql_stream(limit=10)
    if not stream_rows:
        print("[main] Warning: no rows returned from TELEMETRY_STREAM streaming query (it may be fine).")

    # Repeatedly poll pull query for the table to materialize
    def table_ready_check():
        row = query_vehicle_latest(TEST_DEVICE)
        if row:
            return True, row
        return False, None

    ok, row = wait_for_condition(table_ready_check, timeout=30, interval=3, desc="vehicle_latest materialized")
    if ok:
        print("[main] VEHICLE_LATEST row:", row)
    else:
        print("[main] ksql did not return rows for VEHICLE_LATEST - it may not be materialized yet")

    # Check Mongo
    print("[mongo] connecting:", MONGO_URI)
    raw_count, latest_doc = check_mongo(TEST_DEVICE)
    print(f"[mongo] telemetry_raw_count= {raw_count}")
    print(f"[mongo] vehicle_latest_doc= {bool(latest_doc)}")

    ok_all = True
    if raw_count == 0:
        print("FAIL: telemetry_raw has no documents for did=", TEST_DEVICE)
        ok_all = False
    if not latest_doc:
        print("WARN: vehicle_latest document not found for did=", TEST_DEVICE)
        ok_all = False

    if ok_all:
        print("=== e2e test PASSED ===")
        sys.exit(0)
    else:
        print("=== e2e test FAILED ===")
        sys.exit(3)

if __name__ == "__main__":
    main()
