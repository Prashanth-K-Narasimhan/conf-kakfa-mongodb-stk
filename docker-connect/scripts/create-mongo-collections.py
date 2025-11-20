#!/usr/bin/env python3
"""
create-mongo-collections.py

Idempotent creation of MongoDB database, collections, validators and indexes.

This script is intended to run inside the Connect container (or any container),
connecting to the mongodb service name at mongodb:27017.

It will:
- create database telemetry_db
- create telemetry_raw collection (history) with a light validator and non-unique index on did
- create vehicle_latest collection with schema validator and unique index on did
- exit successfully if things already exist
"""

import sys
import time
from pymongo import MongoClient, errors

MONGO_URI = "mongodb://mongodb:27017"
DB_NAME = "telemetry_db"
RAW_COLL = "telemetry_raw"
LATEST_COLL = "vehicle_latest"

# JSON Schema validators for the collections
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

def wait_for_mongo(uri, timeout=120):
    start = time.time()
    while True:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            # trigger server selection
            client.admin.command('ping')
            client.close()
            return True
        except Exception as e:
            if time.time() - start > timeout:
                print(f"[create-mongo] Timeout waiting for MongoDB at {uri}: {e}", file=sys.stderr)
                return False
            time.sleep(2)

def create_collections_and_indexes():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # telemetry_raw (history)
    if RAW_COLL in db.list_collection_names():
        print(f"[create-mongo] Collection {RAW_COLL} already exists.")
        # try to update validator via collMod
        try:
            cmd = {
                "collMod": RAW_COLL,
                "validator": RAW_VALIDATOR,
                "validationLevel": "moderate"
            }
            db.command(cmd)
            print(f"[create-mongo] Updated validator for {RAW_COLL}.")
        except Exception as e:
            print(f"[create-mongo] Could not update validator for {RAW_COLL}: {e}")
    else:
        print(f"[create-mongo] Creating collection {RAW_COLL} with validator.")
        db.create_collection(RAW_COLL, validator=RAW_VALIDATOR, validationLevel="moderate")
        print(f"[create-mongo] Created {RAW_COLL}.")

    # telemetry_raw index (non-unique index on did and timestamp)
    try:
        db[RAW_COLL].create_index([("did", 1), ("timestamp", 1)])
        print(f"[create-mongo] Created index on {RAW_COLL} (did, timestamp).")
    except Exception as e:
        print(f"[create-mongo] Failed to create index on {RAW_COLL}: {e}")

    # vehicle_latest (state table)
    if LATEST_COLL in db.list_collection_names():
        print(f"[create-mongo] Collection {LATEST_COLL} already exists.")
        try:
            cmd = {
                "collMod": LATEST_COLL,
                "validator": LATEST_VALIDATOR,
                "validationLevel": "moderate"
            }
            db.command(cmd)
            print(f"[create-mongo] Updated validator for {LATEST_COLL}.")
        except Exception as e:
            print(f"[create-mongo] Could not update validator for {LATEST_COLL}: {e}")
    else:
        print(f"[create-mongo] Creating collection {LATEST_COLL} with validator.")
        db.create_collection(LATEST_COLL, validator=LATEST_VALIDATOR, validationLevel="moderate")
        print(f"[create-mongo] Created {LATEST_COLL}.")

    # vehicle_latest unique index on did
    try:
        db[LATEST_COLL].create_index("did", unique=True)
        print(f"[create-mongo] Created unique index on {LATEST_COLL}.did")
    except Exception as e:
        print(f"[create-mongo] Failed to create unique index on {LATEST_COLL}: {e}")

    client.close()

def main():
    print("[create-mongo] Waiting for MongoDB to be reachable...")
    ok = wait_for_mongo(MONGO_URI, timeout=120)
    if not ok:
        print("[create-mongo] MongoDB not reachable. Exiting with error.", file=sys.stderr)
        sys.exit(1)
    print("[create-mongo] MongoDB reachable. Proceeding to create collections/indexes.")
    create_collections_and_indexes()
    print("[create-mongo] Done.")

if __name__ == "__main__":
    main()
