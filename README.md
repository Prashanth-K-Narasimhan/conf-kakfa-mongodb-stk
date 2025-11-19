# podman-kakfa-mongodb-stk
    This repo boots a local Confluent Kafka + ksqlDB + Kafka Connect + MongoDB stack using Podman.

# Tech Stack

    - Confluent Kafka (using KSQL/KStreams for real-time data aggregation)
    - MongoDB as the target database for data loading

# Objective

    Provide a step-by-step explanation of the end-to-end data flow, covering:

    - How the incoming payload is streamed through Kafka.
    - How the data is aggregated using KSQL or KStreams.
    - How the processed data is loaded into the target MongoDB database.
    - Identification of key columns used for grouping and aggregation.
    - Interpretation of the signals or events within the source payload.
    - Feel free to contact us in case of any queries.

# Diagram

    Producer (Python) → telemetry (Kafka topic)
                          │
                          ▼
            CREATE STREAM telemetry_stream
                          │
                          ▼
        CREATE STREAM telemetry_raw AS SELECT ...
                          │
                          ▼
      CREATE TABLE vehicle_latest AS SELECT ... GROUP BY did

    Kafka Connect →  
      telemetry_raw      → MongoDB.telemetry_raw  (history)
      vehicle_latest     → MongoDB.vehicle_latest (latest state)

# Commands

        docker compose down
        docker compose build --no-cache
        docker compose up -d
        docker compose down --volumes
        docker compose ps
        docker compose logs -f connect
        docker compose logs -f connect | Select-String "ERROR"
        docker network ls
        docker exec -it <container-name> bash

        $jarUrl = "https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/2.0.1/mongo-kafka-connect-2.0.1.jar"
        Invoke-WebRequest -Uri $jarUrl -OutFile docker-connect\connect-plugins\mongo-kafka-connect-2.0.1.jar

        # run from your project folder that has docker-compose.yml
        docker compose up -d

        # check running services
        docker compose ps

        # view container logs
        docker compose logs -f

You have one raw ingest topic:

telemetry — incoming, raw events (one message per metric reading). Producer writes here.

ksql will create two derived topics:

telemetry_raw — a stream that contains flattened/enriched rows (one row per incoming event, possibly with added fields like low_soc or ingest_ts). Useful for sinking raw events to Mongo (audit trail).

vehicle_latest — the backing topic for a TABLE of the latest metrics per did. This is a compacted topic containing one up-to-date row per device (perfect for dashboards, lookups and upserts into Mongo).

How they plug in:

Producer → telemetry (raw)

ksql reads telemetry → writes telemetry_raw (stream) and updates vehicle_latest (table topic)

Kafka Connect reads telemetry_raw or vehicle_latest and writes to MongoDB (choose raw stream for full audit or table for one doc per device)


    podman run --rm --network host confluentinc/cp-kafka:7.4.1 kafka-topics --bootstrap-server localhost:29092 --create --topic telemetry --partitions 3 --replication-factor 1
    podman run --rm --network host confluentinc/cp-kafka:7.4.1 kafka-topics --bootstrap-server localhost:29092 --create --topic telemetry_raw --partitions 3 --replication-factor 1
    podman run --rm --network host confluentinc/cp-kafka:7.4.1 kafka-topics --bootstrap-server localhost:29092 --create --topic vehicle_latest --partitions 3 --replication-factor 1 --config cleanup.policy=compact


EXAMPLES:

Produce messages:
docker run --rm -it --network host confluentinc/cp-kafka:7.4.1 `
  kafka-console-producer --bootstrap-server localhost:29092 --topic telemetry


Paste JSON lines.

Consume messages:
docker run --rm -it --network host confluentinc/cp-kafka:7.4.1 `
  kafka-console-consumer --bootstrap-server localhost:29092 --topic telemetry --from-beginning

List topics:
docker run --rm --network host confluentinc/cp-kafka:7.4.1 `
  kafka-topics --bootstrap-server localhost:29092 --list


run a single tick (one call across all devices)

python python-producer\producer_simulator.py --bootstrap localhost:29092 --topic telemetry --num-devices 3 --once


run continuous at 1 tick per second

python python-producer\producer_simulator.py --num-devices 5 --rate 1.0


run for a fixed duration, e.g., 60 seconds

python python-producer\producer_simulator.py --num-devices 10 --rate 2.0 --duration 60


run for a fixed number of ticks, e.g., 100 ticks

python python-producer\producer_simulator.py --num-devices 3 --count 100 --rate 2.0


Adjust --miss-rate to simulate sensor dropouts. Lower miss rates mean more metrics per tick.


