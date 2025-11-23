import os
import json
import requests
from pipeline.config import CONNECTORS_DIR, CONNECT_URL

def register_all(replace=False):
    if not os.path.exists(CONNECTORS_DIR):
        return

    url = CONNECT_URL.rstrip("/") + "/connectors"
    
    for f in sorted(os.listdir(CONNECTORS_DIR)):
        if not f.endswith(".json"): 
            continue
            
        path = os.path.join(CONNECTORS_DIR, f)
        with open(path, 'r') as fh:
            try:
                config = json.load(fh)
            except:
                print(f"Skipping {f}, invalid JSON")
                continue

        name = config.get("name", f.replace(".json", ""))
        
        if replace:
            # Delete if exists
            try:
                requests.delete(f"{url}/{name}")
            except: 
                pass

        # Check if running
        r = requests.get(f"{url}/{name}")
        if r.status_code == 200:
            print(f"Connector {name} already exists.")
            continue
            
        # Create
        print(f"Creating connector {name}...")
        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, json=config, headers=headers)
        
        if resp.status_code in [200, 201]:
            print("  -> OK")
        else:
            print(f"  -> Failed: {resp.text}")