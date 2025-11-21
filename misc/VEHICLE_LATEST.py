import requests
stmt = "SELECT DID, ODO, SOC, SPEED, IGNITION, LAST_EVENT_TS FROM VEHICLE_LATEST WHERE DID = '981';"
url = "http://localhost:8088/query"
headers = {"Content-Type":"application/vnd.ksql.v1+json"}
r = requests.post(url, json={"ksql": stmt, "streamsProperties": {}}, headers=headers, timeout=10)
print(r.status_code, r.text)
# parse r.json() for rows...
