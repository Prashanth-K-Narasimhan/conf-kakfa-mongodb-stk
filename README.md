# Real-Time Telemetry Streaming Pipeline
*A practical engineering report built from real debugging, real design choices, and real system behavior*

## 1. Use Case and Expected Outcome
The starting point was two raw telemetry JSON samples. Each record represented a single metric emitted by a vehicle device.

```json
{
  "did": "ABC123",
  "name": "odometer",
  "float_value": 12345.6,
  "eventTime": "2024-10-04T11:22:33Z"
}
```

Odometer arrived alone.  
SOC arrived alone.  
Speed, ignition, and everything else arrived in separate events.  
There was no single event that carried the *state* of the vehicle.

The pipeline needed to produce two parallel outputs:

- A **raw history store** where every telemetry event remains preserved exactly as received.
- A **latest state store** where each `did` has **one continuously updated document** representing the freshest odometer, SOC, speed, ignition, and timestamp.

The objective was to make downstream services *not* reconstruct state manually.  
The system had to be reproducible, clean, and easy to observe end-to-end.

## 2. Tech Stack
A tightly integrated real-time stack:

- **Apache Kafka** for ingestion and log persistence  
- **ksqlDB** for real-time parsing and state aggregation  
- **Kafka Connect** for data movement  
- **MongoDB** as both the audit store and the state store  
- **Python** for orchestration, automation, and data generation  
- **Docker** as the execution environment  

The pipeline closely mirrors how real IoT, telematics, and EV platforms operate.

## 3. Design and Approach
Telemetry arrives as fragments.  
The pipeline stitches meaning out of those fragments.

### 3.1 Data Flow Overview
```
Raw Telemetry → Kafka → ksqlDB Stream → ksqlDB Table → Kafka Topic → MongoDB
```

### 3.2 Why Docker
- Identical environment on any machine  
- No dependency conflicts  
- Easy teardown and rebuild  
- Ideal for interview demonstrations  

### 3.3 Stream Definition
Raw data needed structure before aggregation.

```sql
CREATE STREAM vehicle_raw (
  did VARCHAR,
  name VARCHAR,
  float_value DOUBLE,
  int_value BIGINT,
  string_value VARCHAR,
  eventTime VARCHAR
) WITH (KAFKA_TOPIC='vehicle_raw', VALUE_FORMAT='JSON');
```

### 3.4 Table Definition
The table merged all events into a unified state.

```sql
CREATE TABLE vehicle_latest AS
SELECT
  did,
  LATEST_BY_OFFSET(CASE WHEN name='odometer' THEN float_value END) AS odo,
  LATEST_BY_OFFSET(CASE WHEN name='soc' THEN int_value END) AS soc,
  LATEST_BY_OFFSET(CASE WHEN name='speed' THEN int_value END) AS speed,
  LATEST_BY_OFFSET(CASE WHEN name='ignition_status' THEN int_value END) AS ignition,
  LATEST_BY_OFFSET(eventTime) AS eventTime
FROM vehicle_raw
GROUP BY did;
```

Each update refined the state of one device.  
Each metric stayed fresh without overwriting unrelated fields.

## 4. Key Implementation Details

### Explicit Topic Creation
Avoided auto-create to ensure topic name stability.

### MongoDB Document ID Strategy
Absolutely crucial.  
Mapping DID to `_id` guaranteed proper updates.

```json
"document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy",
"document.id.strategy.partial.value.projection.list": "did",
"writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy"
```

### Early Data Injection
ksqlDB tables do not materialize without data.  
A temporary Python producer ensured the table formed instantly.

```python
producer.send("vehicle_raw", sample_event)
```

## 5. Runbook: Starting the Pipeline

### Step 1: Clone the Repo
```bash
git clone https://github.com/.../conf-kafka-mongodb-stk
cd conf-kafka-mongodb-stk
```

### Step 2: Build the Connect Image
```bash
docker build -t kafka-mongo-connect:latest .
```

### Step 3: Launch the Stack
```bash
docker compose up -d
```

### Step 4: Initialize Everything
```bash
python create_pipeline_full_v3.py
```

This script handles:
- Topic creation  
- KSQL stream and table creation  
- Temporary event generation  
- Connector registration  
- Health checks  

### Step 5: Validate the Flow

#### ksqlDB
```sql
SELECT * FROM vehicle_latest EMIT CHANGES LIMIT 5;
```

#### MongoDB
```js
db.vehicle_latest.find().pretty()
db.raw_events.find().pretty()
```

Watching the state update in real time is the moment everything clicks.

## 6. Troubleshooting (Verbose, Real-world Experience)

### When Kafka Connect Ignored the Mongo Plugin
Everything looked correct, yet Connect claimed the plugin didn’t exist.  
The logs revealed the truth:

```
ClassNotFoundException: com.mongodb...
```

The cause was simple:  
The JAR was on the host, not inside the container.

Rebuilding the image fixed the entire chain.  
This moment clarified how Connect isolates plugins and why placement matters.

### When the ksqlDB Table Stayed Empty
The schema was fine.  
The SQL was fine.  
But the table refused to populate.

The issue was timing.  
ksqlDB requires real stream data before the table materializes.  
Once early events were injected before table creation, the table came alive instantly.

### When MongoDB Inserted Instead of Updating
Without a document ID strategy, MongoDB had no way to decide what to update.  
It defaulted to inserts.

Using DID as `_id` flipped MongoDB into proper update mode.

### When Networking Broke Everything Silently
Containers use:

```
kafka:9092
```

Host tools use:

```
localhost:29092
```

Mixing these leads to:
- empty tables  
- stalled consumers  
- silent ingestion failures  

Understanding Docker networking removed an entire category of “ghost bugs.”

## 7. Common Errors and Fixes (Explained, Not Listed)

### PowerShell Curl Corrupting JSON
PowerShell aliases `curl` differently.  
It silently malformed JSON payloads for connectors.

Switching to:

```
curl.exe
```

made connector creation work flawlessly.

### Forgetting Topic Creation
With auto-create disabled, forgetting to create topics caused silent write failures.  
Creating topics explicitly resolved everything.

### Table Created Before Data
Creating the table too early meant it stayed blank forever.  
Injecting sample events first fixed this consistently.

### Duplicate MongoDB Documents
Incorrect write strategies led to endless inserts.  
Choosing the correct ID mapping solved the entire behavior.

## 8. Final Working State
Once everything aligned, the pipeline behaved like a compact IoT backend:

- `vehicle_raw` captured every telemetry fragment  
- `vehicle_latest` emitted a clean, merged, always-updated state  
- MongoDB stored:
  - full audit history  
  - one authoritative document per device  

A final verification:

```sql
SELECT did, odo, soc, speed, ignition FROM vehicle_latest EMIT CHANGES;
```

MongoDB reflected the exact same truth.  
Everything moved in real time, cleanly and predictably.

## 9. Lessons Learned

- Topic discipline prevents unpredictable downstream behavior  
- ksqlDB tables need early data to form meaningful state  
- Kafka Connect plugin management is its own skill  
- Docker networking can silently break ingestion if misunderstood  
- MongoDB write strategy dictates the entire behavior of state storage  
- Automation keeps the pipeline consistent and production-friendly  
- Observing data flow end to end teaches more than theory ever could  

This pipeline didn’t just work.  
It clarified how real-time systems *think*, *flow*, and *fail* — and how to design them with intention.

