import json
import requests
from pipeline.config import KSQL_URL

def log(*parts):
    import time as _t
    print("[{}]".format(_t.strftime("%H:%M:%S")), *parts)

def ksql_stream_sample(limit: int = 5, stream_name: str = "TELEMETRY_RAW") -> None:
    sql = f"SELECT * FROM {stream_name} EMIT CHANGES LIMIT {limit};"
    url = KSQL_URL.rstrip("/") + "/query-stream"
    log("ksql streaming sample SQL:", sql)
    try:
        r = requests.post(url, json={"sql": sql}, stream=True, timeout=(5, None))
    except Exception as e:
        log("ksql stream request failed:", e)
        return
    count = 0
    try:
        for raw in r.iter_lines(decode_unicode=True):
            if not raw:
                continue
            try:
                doc = json.loads(raw)
            except Exception:
                continue
            if isinstance(doc, dict) and "row" in doc:
                cols = doc["row"].get("columns", [])
                log(f"ksql stream row: {cols}")
                count += 1
                if count >= limit:
                    break
    finally:
        try:
            r.close()
        except Exception:
            pass
    log("ksql stream sample collected", count, "rows")

if __name__ == "__main__":
    ksql_stream_sample()