import os
import json
import requests
from pipeline.config import CONNECTORS_DIR, CONNECT_URL

def log(*parts):
    import time as _t
    print("[{}]".format(_t.strftime("%H:%M:%S")), *parts)

def register_connectors(replace: bool = False) -> None:
    if not os.path.isdir(CONNECTORS_DIR):
        log("connectors dir missing; skipping connector registration")
        return
    for fname in sorted(os.listdir(CONNECTORS_DIR)):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(CONNECTORS_DIR, fname)
        with open(path, "r", encoding="utf-8") as fh:
            cfg_text = fh.read()
        try:
            cfg = json.loads(cfg_text)
        except Exception as e:
            log("connect config invalid JSON:", fname, e)
            continue
        conn_name = cfg.get("name") or os.path.splitext(fname)[0]
        conn_url = f"{CONNECT_URL.rstrip('/')}/connectors/{conn_name}"
        if replace:
            try:
                r = requests.get(conn_url, timeout=5)
                if r.status_code == 200:
                    log("connect deleting existing connector", conn_name)
                    requests.delete(conn_url, timeout=5)
            except Exception:
                pass
        post_url = f"{CONNECT_URL.rstrip('/')}/connectors"
        try:
            r2 = requests.post(post_url, headers={"Content-Type": "application/json"}, data=json.dumps(cfg), timeout=15)
            if r2.status_code in (200, 201):
                log("connect created", conn_name)
            else:
                log("connect create failed", conn_name, r2.status_code, r2.text[:1000])
        except Exception as e:
            log("connect create exception for", conn_name, e)
            
if __name__ == "__main__":
    register_connectors()