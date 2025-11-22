from pymongo import MongoClient
from pipeline.config import MONGO_URL, DB_NAME
import time

def log(*parts):
    import time as _t
    print("[{}]".format(_t.strftime("%H:%M:%S")), *parts)

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
    if "telemetry_raw" not in db.list_collection_names():
        db.create_collection("telemetry_raw")
        log("mongo telemetry_raw created")
    else:
        log("mongo telemetry_raw exists")
    try:
        db.telemetry_raw.create_index([("DID", 1), ("TIMESTAMP", 1)])
        log("mongo telemetry_raw index ensured")
    except Exception as e:
        log("mongo telemetry_raw index non-fatal:", e)
    if "vehicle_latest" not in db.list_collection_names():
        db.create_collection("vehicle_latest")
        log("mongo vehicle_latest created")
    else:
        log("mongo vehicle_latest exists")
    try:
        db.vehicle_latest.create_index([("DID", 1)], name="idx_vehicle_latest_DID", unique=False)
        log("mongo vehicle_latest index ensured")
    except Exception as e:
        log("mongo vehicle_latest index non-fatal:", e)
    client.close()

def mongo_stats() -> tuple:
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
