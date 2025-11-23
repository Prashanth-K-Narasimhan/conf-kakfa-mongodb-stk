#!/usr/bin/env python3
"""
Destroy pipeline resources created by the setup script.

Usage examples:
  # remove everything (topics, ksql streams/tables, connectors, mongo collections)
  python destroy_pipeline.py --all

  # remove just connectors and ksql objects
  python destroy_pipeline.py --connectors --ksql

  # remove everything except vehicle_latest topic (keep list accepts comma-separated names)
  python destroy_pipeline.py --all --keep vehicle_latest

  # remove only the kafka topics
  python destroy_pipeline.py --topics
"""

import os
import json
import time
import argparse
import requests
from kafka.admin import KafkaAdminClient
from pymongo import MongoClient

# Config (same defaults as setup)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
KSQL_URL = os.environ.get("KSQL_URL", "http://localhost:8088")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "telemetry_db")

# resources names (change if you used different names)
TOPICS = ["telemetry_raw", "telemetry_normalized", "vehicle_latest_state"]
KSQL_STREAMS = ["TELEMETRY_NORMALIZED", "TELEMETRY_RAW"]
KSQL_TABLES = ["VEHICLE_LATEST_STATE"]
CONNECTOR_NAMES = ["mongo-sink-vehicle-latest-state", "mongo-sink-telemetry-history"]
MONGO_COLLECTIONS = ["mongo_telemetry_history", "mongo_vehicle_latest_state"]

def ksql_endpoint(path="/ksql"):
    return KSQL_URL.rstrip("/") + path

def ksql_show(kind):
    try:
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
    except Exception:
        return []

def ksql_post(statement):
    url = ksql_endpoint("/ksql")
    r = requests.post(url, json={"ksql": statement, "streamsProperties": {}}, timeout=10)
    return r

def drop_ksql(obj_type, name):
    try:
        stmt = f"DROP {obj_type} {name};"
        r = ksql_post(stmt)
        print(f"[ksql] drop {obj_type} {name} -> {r.status_code}")
    except Exception as e:
        print("[ksql] drop error", e)

def delete_kafka_topics(topics, keep=[]):
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    to_delete = [t for t in topics if t not in keep]
    if not to_delete:
        print("[kafka] nothing to delete")
        return
    try:
        admin.delete_topics(to_delete)
        print("[kafka] delete topics request sent:", to_delete)
    except Exception as e:
        print("[kafka] delete topics failed:", e)
    admin.close()

def delete_connectors(names, keep=[]):
    base = CONNECT_URL.rstrip("/") + "/connectors"
    for n in names:
        if n in keep:
            print(f"[connect] keep connector {n}")
            continue
        try:
            r = requests.delete(f"{base}/{n}", timeout=5)
            if r.status_code in (200,204):
                print(f"[connect] deleted {n}")
            else:
                print(f"[connect] delete {n} -> {r.status_code} {r.text}")
        except Exception as e:
            print(f"[connect] delete error for {n} -> {e}")

def delete_ksql_objects(remove_streams=False, remove_tables=False, keep=None, ordered_names=None):
    if keep is None:
        keep = set()
    else:
        keep = set(keep)

    streams = set(ksql_show("STREAMS") or [])
    tables = set(ksql_show("TABLES") or [])

    def _drop(kind, name):
        try:
            drop_ksql(kind, name)
            print(f"[ksql] dropped {kind.lower()} {name}")
        except Exception as e:
            print(f"[ksql] error dropping {kind.lower()} {name}:", e)

    if ordered_names:
        for name in ordered_names:
            if name in keep:
                print(f"[ksql] keep {name}")
                continue
            is_s = name in streams
            is_t = name in tables
            if is_s and remove_streams:
                _drop("STREAM", name)
                if is_t and remove_tables:  # if same name exists as table too
                    _drop("TABLE", name)
                continue
            if is_t and remove_tables:
                _drop("TABLE", name)
                continue
            # found but not allowed by flags, or not found at all
            if is_s and not remove_streams:
                print(f"[ksql] found stream {name} but remove_streams is False; skipping")
            elif is_t and not remove_tables:
                print(f"[ksql] found table {name} but remove_tables is False; skipping")
            else:
                print(f"[ksql] {name} not found; skipping")
    else:
        if remove_streams:
            for s in sorted(streams):
                if s in keep:
                    print(f"[ksql] keep stream {s}")
                    continue
                _drop("STREAM", s)
        if remove_tables:
            for t in sorted(tables):
                if t in keep:
                    print(f"[ksql] keep table {t}")
                    continue
                _drop("TABLE", t)


def drop_mongo_collections(coll_names, keep=[]):
    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]
    for c in coll_names:
        if c in keep:
            print(f"[mongo] keep collection {c}")
            continue
        try:
            if c in db.list_collection_names():
                db.drop_collection(c)
                print(f"[mongo] dropped collection {c}")
            else:
                print(f"[mongo] collection {c} not present")
        except Exception as e:
            print("[mongo] error dropping", c, e)
    client.close()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--topics", action="store_true", help="Delete kafka topics")
    p.add_argument("--ksql", action="store_true", help="Delete ksql streams/tables")
    p.add_argument("--connectors", action="store_true", help="Delete connectors")
    p.add_argument("--mongo", action="store_true", help="Drop mongo collections")
    p.add_argument("--all", action="store_true", help="Delete all resources")
    p.add_argument("--keep", type=str, default="", help="Comma-separated names to keep (topics, connectors, ksql names or collections)")
    args = p.parse_args()

    keep = [x.strip() for x in args.keep.split(",") if x.strip()]

    # If --all set, flip all deletes on
    remove_topics = args.all or args.topics
    remove_ksql = args.all or args.ksql
    remove_connectors = args.all or args.connectors
    remove_mongo = args.all or args.mongo

    print("Destroy plan:",
          f"topics={remove_topics}, ksql={remove_ksql}, connectors={remove_connectors}, mongo={remove_mongo}, keep={keep}")

    if remove_connectors:
        delete_connectors(CONNECTOR_NAMES, keep=keep)

    if remove_ksql:
        # we choose to drop streams and tables listed for this pipeline
        # keep list expects uppercase names for ksql objects
        upper_keep = [k.upper() for k in keep]
        delete_streams = True
        delete_tables = True
        delete_ksql_objects(remove_streams=delete_streams, remove_tables=delete_tables, keep=upper_keep, ordered_names=KSQL_TABLES + KSQL_STREAMS)

    if remove_topics:
        delete_kafka_topics(TOPICS, keep=keep)

    if remove_mongo:
        drop_mongo_collections(MONGO_COLLECTIONS, keep=keep)

    print("Destroy process finished.")

if __name__ == "__main__":
    main()
