from pymongo import MongoClient
from pipeline.config import MONGO_URL

def init_db():
    # Just standard pymongo stuff
    try:
        client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=2000)
        client.server_info() # Trigger connection check
    except Exception as e:
        print(f"Mongo check failed: {e}")
        return

    db = client["telemetry_db"]
    
    # Create collections/indexes lazily
    # Raw history
    col = db["mongo_telemetry_history"]
    col.create_index([("did", 1), ("sensor_name", 1), ("timestamp", 1)])
    
    # Latest state
    col_latest = db["mongo_vehicle_latest_state"]
    col_latest.create_index("did")
    
    print("Mongo indexes ensured.")
    client.close()