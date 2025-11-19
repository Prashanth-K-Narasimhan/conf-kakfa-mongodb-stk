#!/usr/bin/env bash
set -euo pipefail

KSQL_URL="http://ksqldb-server:8088/ksql"
KSQLDIR="/ksql"

if [ ! -d "${KSQLDIR}" ]; then
  echo "Directory ${KSQLDIR} not found. Create your .ksql files there."
  exit 1
fi

for f in $(ls "${KSQLDIR}"/*.ksql 2>/dev/null | sort); do
  echo "==== Posting ${f} ===="
  # read file content (preserve quotes/newlines by using jq -Rs)
  PAYLOAD=$(jq -R -s -c '{"ksql":., "streamsProperties": {}}' < "${f}")
  # send
  HTTP_CODE=$(curl -s -o /tmp/ksql_resp.json -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "${PAYLOAD}" ${KSQL_URL} || true)
  if [ "${HTTP_CODE}" != "200" ] && [ "${HTTP_CODE}" != "201" ]; then
    echo "ERROR: HTTP ${HTTP_CODE} while posting ${f}. Response:"
    cat /tmp/ksql_resp.json
    exit 2
  fi
  echo "Posted ${f} OK"
done

echo "All .ksql files posted."
