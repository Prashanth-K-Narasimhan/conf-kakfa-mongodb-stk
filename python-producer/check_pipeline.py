#!/usr/bin/env python3
"""
check_pipeline.py
Checks Kafka topics, ksql streams/tables, MongoDB collections/indexes, and Connectors.
Exits 0 on success, 1 if any expected item is missing.
"""

import sys, time, json, os
import requests
from kafka import KafkaAdminClient
from pymongo import MongoClient
from tabulate import tabulate

# Config (change via env if needed)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
KSQL_URL = os.environ.get("KSQL_URL", "http://localhost:8088/ksql")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083/connectors")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "telemetry_db")

EXPECTED_TOPICS = ["telemetry", "telemetry_raw", "vehicle_latest"]
EXPECTED_STREAMS = ["TELEMETRY_STREAM"]  # KSQL returns uppercase names
EXPECTED_TABLES = ["VEHICLE_LATEST"]
EXPECTED_COLLECTIONS = ["telemetry_raw", "vehicle_latest"]
EXPECTED_CONNECTORS = ["mongo-sink-vehicle_latest", "mongo-sink-telemetry_raw"]  # adjust to your filenames

def check_kafka_topics():
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=5000)
        topics = set(admin.list_topics())
        admin.close()
    except Exception as e:
        return False, f"Kafka unreachable at {KAFKA_BOOTSTRAP}: {e}"
    missing = [t for t in EXPECTED_TOPICS if t not in topics]
    return (len(missing) == 0, missing if missing else "ok")

def ksql_request(ksql_text):
    payload = {"ksql": ksql_text, "streamsProperties": {}}
    r = requests.post(KSQL_URL, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()

def check_ksql_objects():
    try:
        streams = ksql_request("SHOW STREAMS;")
        tables = ksql_request("SHOW TABLES;")
        print(streams)
        print(tables)
        # The response format may be a list with statements result; extract names
        streams_list = []
        tables_list = []
        for item in streams:
            if "streams" in item:
                for s in item["streams"]:
                    streams_list.append(s.get("name","").upper())
        for item in tables:
            if "tables" in item:
                for t in item["tables"]:
                    tables_list.append(t.get("name","").upper())
    except Exception as e:
        return False, f"ksql error: {e}"
    missing_streams = [s for s in EXPECTED_STREAMS if s not in streams_list]
    missing_tables = [t for t in EXPECTED_TABLES if t not in tables_list]
    return (len(missing_streams) == 0 and len(missing_tables) == 0, {"streams_missing": missing_streams, "tables_missing": missing_tables})

def check_connectors():
    try:
        r = requests.get(CONNECT_URL, timeout=5)
        r.raise_for_status()
        connectors = r.json()
    except Exception as e:
        return False, f"connect REST unreachable at {CONNECT_URL}: {e}"
    missing = [c for c in EXPECTED_CONNECTORS if c not in connectors]
    return (len(missing) == 0, missing if missing else "ok")

def check_mongo():
    try:
        client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[DB_NAME]
        cols = db.list_collection_names()
    except Exception as e:
        return False, f"mongo unreachable at {MONGO_URL}: {e}"
    missing = [c for c in EXPECTED_COLLECTIONS if c not in cols]
    idx_issues = []
    if "vehicle_latest" in cols:
        try:
            idxs = db.vehicle_latest.index_information()
            # look for unique index on 'did'
            found = any(("did" in ",".join([k[0] for k in idxs[i]['key']]) or ('key' in idxs[i] and any(k[0]=='did' for k in idxs[i]['key']))) and idxs[i].get("unique", False) for i in idxs)
            if not found:
                idx_issues.append("vehicle_latest.missing_unique_did_index")
        except Exception as e:
            idx_issues.append(f"index_check_error:{e}")
    client.close()
    return (len(missing) == 0 and len(idx_issues) == 0, {"missing_collections": missing, "index_issues": idx_issues})

def main():
    results = []
    all_ok = True

    kak, msg = check_kafka_topics()
    results.append(["Kafka topics", "OK" if kak else "MISSING", msg])
    all_ok = all_ok and kak

    ksk, kmsg = check_ksql_objects()
    results.append(["ksql streams/tables", "OK" if ksk else "MISSING", kmsg])
    all_ok = all_ok and ksk

    conk, conmsg = check_connectors()
    results.append(["connect connectors", "OK" if conk else "MISSING", conmsg])
    all_ok = all_ok and conk

    mok, momsg = check_mongo()
    results.append(["mongo collections/indexes", "OK" if mok else "MISSING", momsg])
    all_ok = all_ok and mok

    print("\nPipeline check results:\n")
    print(tabulate(results, headers=["Component", "Status", "Details"], tablefmt="github"))
    print()
    if all_ok:
        print("ALL CHECKS PASSED ✅")
        sys.exit(0)
    else:
        print("SOME CHECKS FAILED ❌ - inspect 'Details' above")
        sys.exit(1)

if __name__ == "__main__":
    main()
