import os
import re
import requests
import time
from typing import Optional, Tuple
from pipeline.config import KSQL_URL, KSQ_DIR, KSQL_TABLE_MATERIALIZE_TIMEOUT
from pipeline.kafka_utils import topic_has_messages, log as klog

def log(*parts):
    import time as _t
    print("[{}]".format(_t.strftime("%H:%M:%S")), *parts)

def ksql_endpoint(path="/ksql") -> str:
    return KSQL_URL.rstrip("/") + path

def ksql_post(statement: str, streams_props: Optional[dict] = None) -> requests.Response:
    url = ksql_endpoint("/ksql")
    payload = {"ksql": statement, "streamsProperties": streams_props or {}}
    r = requests.post(url, json=payload, timeout=60)
    return r

def ksql_show(kind: str) -> list:
    url = ksql_endpoint("/ksql")
    payload = {"ksql": f"SHOW {kind};", "streamsProperties": {}}
    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()
    names = []
    for item in r.json():
        key = kind.lower()
        if key in item:
            for ent in item[key]:
                names.append(ent.get("name", "").upper())
    return names

def parse_first_create(ksql_text: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(r"CREATE\s+(STREAM|TABLE)\s+([A-Za-z0-9_]+)", ksql_text, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper(), m.group(2).upper()
    return None, None

def drop_ksql(obj_type: str, name: str) -> requests.Response:
    stmt = f"DROP {obj_type} {name};"
    return ksql_post(stmt)

def apply_ksql_files(replace: bool = False) -> None:
    if not os.path.isdir(KSQ_DIR):
        log("ksql dir not present; skipping ksql apply")
        return
    try:
        streams = ksql_show("STREAMS")
    except Exception:
        streams = []
    try:
        tables = ksql_show("TABLES")
    except Exception:
        tables = []
    earliest_props = {"ksql.streams.auto.offset.reset": "earliest"}
    for fname in sorted(os.listdir(KSQ_DIR)):
        if not fname.endswith(".ksql"):
            continue
        path = os.path.join(KSQ_DIR, fname)
        with open(path, "r", encoding="utf-8") as fh:
            ksql_text = fh.read().strip()
        if not ksql_text:
            continue
        obj_type, obj_name = parse_first_create(ksql_text)
        if replace and obj_name:
            if obj_type == "STREAM" and obj_name in streams:
                log("ksql dropping existing stream", obj_name)
                drop_ksql("STREAM", obj_name)
            if obj_type == "TABLE" and obj_name in tables:
                log("ksql dropping existing table", obj_name)
                drop_ksql("TABLE", obj_name)
        log("ksql applying", fname)
        payload = {"ksql": ksql_text, "streamsProperties": earliest_props}
        r = requests.post(ksql_endpoint("/ksql"), json=payload, timeout=60)
        if r.status_code not in (200, 201):
            log("ksql warning:", r.status_code, r.text)

def ksql_table_exists(table_name: str) -> bool:
    try:
        tables = ksql_show("TABLES")
        return table_name.upper() in tables
    except Exception:
        return False

def wait_for_table_materialization(table_name: str, timeout: int = KSQL_TABLE_MATERIALIZE_TIMEOUT) -> bool:
    start = time.time()
    while True:
        if not ksql_table_exists(table_name):
            if time.time() - start > timeout:
                log("ksql timeout waiting for table to exist:", table_name)
                return False
            time.sleep(1)
            continue
        # now check if backing topic has messages. KSQL names backing topics in uppercase by default
        backing_topic = table_name.upper()
        try:
            has = topic_has_messages(backing_topic)
            if has:
                log("ksql table materialized and backing topic has messages:", table_name)
                return True
            if time.time() - start > timeout:
                log("ksql timeout waiting for backing topic messages:", backing_topic)
                return False
            time.sleep(1)
        except Exception:
            # on exception, return True once KSQL reports table exists
            return True

def ksql_server_url():
    """
    Returns the KSQL server URL. This keeps the main orchestrator clean and
    allows you to override via env variable KSQLDB_URL if needed.
    """
    import os
    return os.environ.get("KSQLDB_URL", "http://localhost:8088")

if __name__ == "__main__":
    apply_ksql_files()