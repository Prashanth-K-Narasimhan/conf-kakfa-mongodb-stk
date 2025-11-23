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
    if "mongo_telemetry_history" not in db.list_collection_names():
        db.create_collection("mongo_telemetry_history")
        log("mongo telemetry_history created")
    else:
        log("mongo telemetry_history exists")
    try:
        db.mongo_telemetry_history.create_index([("did", 1), ("timestamp", 1)])
        log("mongo telemetry_history index ensured")
    except Exception as e:
        log("mongo telemetry_history index non-fatal:", e)
    if "mongo_vehicle_latest_state" not in db.list_collection_names():
        db.create_collection("mongo_vehicle_latest_state")
        log("mongo vehicle_latest created")
    else:
        log("mongo vehicle_latest exists")
    try:
        db.mongo_vehicle_latest_state.create_index([("did", 1)], name="idx_vehicle_latest_state_DID", unique=False)
        log("mongo vehicle_latest index ensured")
    except Exception as e:
        log("mongo vehicle_latest index non-fatal:", e)
    client.close()

def mongo_stats() -> tuple:
    try:
        client = MongoClient(MONGO_URL)
        db = client[DB_NAME]
        raw_count = db.telemetry_history.estimated_document_count()
        latest_count = db.vehicle_latest.estimated_document_count()
        client.close()
        return raw_count, latest_count
    except Exception as e:
        log("mongo stats error:", e)
        return 0, 0
