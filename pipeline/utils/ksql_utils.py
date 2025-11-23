import os
import requests
import json
from pipeline.config import KSQL_URL, KSQL_DIR

def _ksql_exec(ksql_str, prop_override=None):
    url = f"{KSQL_URL}/ksql"
    headers = {"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"}
    
    props = {"ksql.streams.auto.offset.reset": "earliest"}
    if prop_override:
        props.update(prop_override)

    payload = {
        "ksql": ksql_str,
        "streamsProperties": props
    }
    
    r = requests.post(url, json=payload, headers=headers)
    return r

def run_migrations(replace=False):
    if not os.path.exists(KSQL_DIR):
        print("No KSQL directory found, skipping.")
        return

    # Naive approach: just try to run them. If replace is True, try DROP first.
    # Order matters, so we sort by filename.
    files = sorted([f for f in os.listdir(KSQL_DIR) if f.endswith('.ksql')])
    
    for f in files:
        with open(os.path.join(KSQL_DIR, f), 'r') as fh:
            content = fh.read()
            
        # Clean comments/newlines
        statements = [s.strip() for s in content.split(';') if s.strip()]
        
        print(f"Processing {f}...")
        
        for stmt in statements:
            # Quick hack to get object name for DROP
            # Assumes format "CREATE STREAM NAME ..."
            if replace and "CREATE" in stmt.upper():
                parts = stmt.split()
                try:
                    # index 0=CREATE, 1=TYPE, 2=NAME
                    obj_type = parts[1]
                    obj_name = parts[2]
                    print(f"  Attempting drop of {obj_name}...")
                    _ksql_exec(f"DROP {obj_type} IF EXISTS {obj_name} DELETE TOPIC;")
                except:
                    pass # regex parsing sql is hard, skip if fails

            resp = _ksql_exec(stmt + ";")
            if resp.status_code != 200:
                # 400 usually means it already exists
                if "already exists" in resp.text:
                    print("  -> Exists, skipping.")
                else:
                    print(f"  -> Error: {resp.text}")
            else:
                print("  -> Success")