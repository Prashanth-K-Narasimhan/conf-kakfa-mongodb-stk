#!/usr/bin/env bash
# scripts/run_ksql_files.sh
# Usage: bash scripts/run_ksql_files.sh
set -euo pipefail

KSQL_URL="http://localhost:8088/ksql"
KSQLDIR="./ksql"

if [ ! -d "${KSQLDIR}" ]; then
  echo "Directory ${KSQLDIR} not found. Create your .ksql files there."
  exit 1
fi

for f in $(ls "${KSQLDIR}"/*.ksql 2>/dev/null | sort); do
  echo "==== Running ${f} ===="
  CONTENT=$(sed ':a;N;$!ba;s/"/\\"/g' "${f}" | awk '{printf "%s\\n", $0 }')
  # Use curl to POST the file as a single ksql payload
  RESP=$(cat <<JSON | curl -s -w "\n%{http_code}" -o /tmp/ksql_resp.json -X POST -H "Content-Type: application/json" ${KSQL_URL} -d @-
{"ksql":"${CONTENT}","streamsProperties": {}}
JSON
  HTTP_CODE=$(tail -n1 <<< "${RESP}")
  if [ "${HTTP_CODE}" != "200" ] && [ "${HTTP_CODE}" != "201" ]; then
    echo "ERROR: ksql returned HTTP ${HTTP_CODE}. Response below:"
    cat /tmp/ksql_resp.json
    exit 2
  fi
  echo "OK"
done

echo "All .ksql files processed."
