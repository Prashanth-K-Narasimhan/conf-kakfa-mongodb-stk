#!/usr/bin/env python3
"""
ksql_query.py

Run ad-hoc KSQL statements and queries against a ksqlDB server.

Usage examples:
  # simple statement (CREATE STREAM / TABLE / DROP / etc.)
  python ksql_query.py -e "CREATE STREAM X WITH (...);"

  # run a pull / push query (non-streaming)
  python ksql_query.py -e "SELECT * FROM MY_TABLE LIMIT 5;" 

  # run a streaming query (EMIT CHANGES) and stream results to stdout
  python ksql_query.py --stream -e "SELECT * FROM TELEMETRY_RAW EMIT CHANGES LIMIT 5;"

  # read SQL from file
  python ksql_query.py -f my_query.ksql

  # using a custom KSQL URL
  python ksql_query.py --ksql http://localhost:8088 -e "SHOW STREAMS;"
"""
import os
import sys
import argparse
import requests
import json
import time
from typing import Optional

DEFAULT_KSQL = os.environ.get("KSQL_URL", "http://localhost:8088")
HEADERS_V1 = {"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"}
HEADERS_JSON = {"Content-Type": "application/json"}

def log(*parts):
    print(*parts, file=sys.stderr)

def post_ksql_statement(ksql_url: str, statement: str, streams_props: Optional[dict] = None, timeout: int = 60):
    payload = {"ksql": statement, "streamsProperties": streams_props or {}}
    url = ksql_url.rstrip("/") + "/ksql"
    r = requests.post(url, headers=HEADERS_V1, json=payload, timeout=timeout)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        # print server body for debugging
        log("HTTP error:", r.status_code)
        try:
            log(r.text)
        except Exception:
            pass
        raise
    return r.json()

def post_query(ksql_url: str, sql: str, timeout: int = 30):
    """
    POST to /query (pull queries, returns JSON)
    """
    url = ksql_url.rstrip("/") + "/query"
    payload = {"sql": sql}
    r = requests.post(url, headers=HEADERS_JSON, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()

def post_query_stream(ksql_url: str, sql: str, timeout: int = 5):
    """
    POST to /query-stream for streaming queries (EMIT CHANGES).
    This returns a streaming response where each line is a JSON event.
    """
    url = ksql_url.rstrip("/") + "/query-stream"
    payload = {"sql": sql}
    # timeout is a tuple (connect, read) by requests; set read to None for indefinite
    r = requests.post(url, headers=HEADERS_JSON, json=payload, stream=True, timeout=(5, None))
    r.raise_for_status()
    return r

def pretty_print(obj):
    try:
        print(json.dumps(obj, indent=2, ensure_ascii=False))
    except Exception:
        print(obj)

def run_stream(ksql_url: str, sql: str, limit: Optional[int] = None):
    """
    Stream results from /query-stream. If limit provided (int), stop after that many rows.
    """
    log("Streaming query:", sql)
    resp = post_query_stream(ksql_url, sql)
    count = 0
    try:
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            # server may send keepalive or other control lines
            try:
                obj = json.loads(raw)
            except Exception:
                # print raw for debugging
                log("raw:", raw)
                continue
            # try to extract rows (ksql returns {"row": {"columns": [...]}} for each result row)
            if isinstance(obj, dict) and "row" in obj:
                cols = obj["row"].get("columns", [])
                print(json.dumps(cols, ensure_ascii=False))
                count += 1
                if limit and count >= limit:
                    break
            else:
                # print any other server messages (errors, heartbeats)
                pretty_print(obj)
    finally:
        try:
            resp.close()
        except Exception:
            pass
    log("stream finished, rows:", count)

def main():
    p = argparse.ArgumentParser(description="Run adhoc KSQL statements/queries")
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("-e", "--execute", help="SQL string to execute")
    g.add_argument("-f", "--file", help="Read SQL from file (can contain multiple statements)")
    p.add_argument("--ksql", help="ksqlDB REST URL", default=DEFAULT_KSQL)
    p.add_argument("--stream", help="Use streaming endpoint (/query-stream). Use for EMIT CHANGES", action="store_true")
    p.add_argument("--show-response", help="Print raw response JSON for statements", action="store_true")
    p.add_argument("--limit", type=int, help="Limit rows when streaming (simple client-side limit)")
    p.add_argument("--earliest", help="For statements, set ksql.streams.auto.offset.reset=earliest", action="store_true")
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds for non-stream requests")
    args = p.parse_args()

    if not args.execute and not args.file:
        # try read from stdin
        if not sys.stdin.isatty():
            sql = sys.stdin.read().strip()
        else:
            p.print_help()
            sys.exit(1)
    else:
        if args.execute:
            sql = args.execute
        else:
            sql = open(args.file, "r", encoding="utf-8").read()

    ksql_url = args.ksql
    # naive check to decide which endpoint: if streaming flag or contains EMIT CHANGES -> stream
    use_stream = args.stream or ("EMIT CHANGES" in sql.upper())
    try:
        if use_stream:
            run_stream(ksql_url, sql, limit=args.limit)
            return
        # For non-streaming: decide whether this is a KSQL DDL/statement vs pull query
        # If the SQL starts with SHOW / DESCRIBE / CREATE / DROP / SET / UNSET / TERMINATE / REGISTER / RUN etc,
        # the /ksql endpoint is the right one. Otherwise /query (pull queries) may be appropriate.
        first = sql.strip().split(None, 1)[0].upper()
        statement_types = {"SHOW", "DESCRIBE", "CREATE", "DROP", "SET", "UNSET", "TERMINATE", "REGISTER", "RUN", "INSERT"}
        if first in statement_types:
            streams_props = {"ksql.streams.auto.offset.reset": "earliest"} if args.earliest else {}
            out = post_ksql_statement(ksql_url, sql, streams_props=streams_props, timeout=args.timeout)
            if args.show_response:
                pretty_print(out)
            else:
                # response is usually a list of maps, try to give a compact useful view
                if isinstance(out, list):
                    for item in out:
                        pretty_print(item)
                else:
                    pretty_print(out)
        else:
            # treat as a pull query using /query
            out = post_query(ksql_url, sql, timeout=args.timeout)
            pretty_print(out)
    except requests.HTTPError as e:
        log("HTTP error:", e)
        sys.exit(2)
    except requests.RequestException as e:
        log("Request error:", e)
        sys.exit(3)
    except KeyboardInterrupt:
        log("Interrupted")
        sys.exit(4)

if __name__ == "__main__":
    main()
