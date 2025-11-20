Alright Prashanth, here is your **updated, precise, one-liner cheat sheet** with the exact container names from your setup.

Keep this as your copy paste arsenal during the demo. Everything is mapped cleanly to:

• kafka → `conf-kakfa-mongodb-stk-kafka-1`
• connect → `conf-kakfa-mongodb-stk-connect-1`
• mongodb → `conf-kakfa-mongodb-stk-mongodb-1`
• ksqldb → `conf-kakfa-mongodb-stk-ksqldb-server-1`

No fluff, no broken commands, all one line.

==============================

### 1. Kafka one-liners

==============================

List topics:

```powershell
docker exec -it conf-kakfa-mongodb-stk-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

Describe topic telemetry_raw:

```powershell
docker exec -it conf-kakfa-mongodb-stk-kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic telemetry_raw
```

Consume 10 messages from telemetry_raw:

```powershell
docker exec -it conf-kakfa-mongodb-stk-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic telemetry_raw --from-beginning --max-messages 10
```

Consume 10 messages from vehicle_latest:

```powershell
docker exec -it conf-kakfa-mongodb-stk-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic vehicle_latest --from-beginning --max-messages 10
```

==============================

### 2. Kafka Connect one-liners

==============================

List connector plugins:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8083/connector-plugins" | ConvertTo-Json -Depth 5
```

List active connectors:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8083/connectors"
```

Connector status:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8083/connectors/mongo-sink-telemetry-raw/status" | ConvertTo-Json -Depth 6
```

Get connector config:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8083/connectors/mongo-sink-telemetry-raw" | ConvertTo-Json -Depth 6
```

Delete connector:

```powershell
Invoke-RestMethod -Method Delete -Uri "http://localhost:8083/connectors/mongo-sink-telemetry-raw"
```

Create connector:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors" -ContentType "application/json" -Body (Get-Content -Raw .\connectors\mongodb-sink-telemetry-raw.json)
```

Connect logs:

```powershell
docker logs -f conf-kakfa-mongodb-stk-connect-1
```

==============================

### 3. MongoDB one-liners

==============================

Enter Mongo shell:

```powershell
docker exec -it conf-kakfa-mongodb-stk-mongodb-1 mongosh
```

Inside mongosh:

Show DBs:

```javascript
show dbs
```

Use DB:

```javascript
use telemetry_db
```

Collections:

```javascript
show collections
```

Get 10 documents from telemetry_raw:

```javascript
db.telemetry_raw.find().limit(10)
```

Get 10 documents from vehicle_latest:

```javascript
db.vehicle_latest.find().limit(10)
```

Count documents:

```javascript
db.telemetry_raw.countDocuments()
```

==============================

### 4. ksqlDB one-liners

==============================

Open ksql prompt:

```powershell
docker exec -it conf-kakfa-mongodb-stk-ksqldb-server-1 ksql http://localhost:8088
```

Inside ksql:

```sql
SHOW STREAMS;
SHOW TABLES;
SELECT * FROM TELEMETRY_RAW EMIT CHANGES LIMIT 10;
SELECT * FROM VEHICLE_LATEST EMIT CHANGES LIMIT 10;
```

==============================

### 5. Useful container ops during demo

==============================

List containers:

```powershell
docker ps
```

Restart Connect:

```powershell
docker restart conf-kakfa-mongodb-stk-connect-1
```

Restart Kafka:

```powershell
docker restart conf-kakfa-mongodb-stk-kafka-1
```

Restart Mongo:

```powershell
docker restart conf-kakfa-mongodb-stk-mongodb-1
```

mongodb://localhost:27017

==============================
