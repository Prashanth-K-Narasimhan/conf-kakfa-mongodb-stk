Nice — tidy wrap-up. Below you get:

1. the exact `requirements.txt` content to add to your repo so you can install the same Python deps locally,
2. one-by-one shell/PowerShell commands to fully tear down, rebuild and bring the updated stack up (with notes/warnings), and
3. a short checklist of verification steps to confirm everything ran correctly.

I kept this practical and copy-paste-ready.

---

# 1) `requirements.txt` (place at project root)

Save this file as `requirements.txt`:

```
pymongo==4.6.0
confluent-kafka==2.2.0
```

Notes:

* `pymongo` is needed by `create-mongo-collections.py` inside the Connect image (and locally if you want to run the script yourself).
* `confluent-kafka` is the Python producer dependency (matches earlier producer scripts).
* If you already have a local virtualenv, run `pip install -r requirements.txt`.

---

# 2) Commands to redeploy the whole system (step-by-step)

> Important safety note: `docker compose down -v` will remove compose-created volumes (including MongoDB data and Kafka data). Back up anything you need before running the destructive commands.

Run these from your project root.

### A — Make sure repo files are saved and scripts are executable

```bash
# On Linux / WSL / Git Bash
chmod +x docker-connect/entrypoint-init.sh \
         scripts/create-topics.sh \
         scripts/run_ksql_files.sh \
         create-mongo-collections.py

# On Windows PowerShell (run as Admin if needed)
icacls docker-connect\entrypoint-init.sh /grant "%USERNAME%:RX"
icacls scripts\create-topics.sh /grant "%USERNAME%:RX"
icacls scripts\run_ksql_files.sh /grant "%USERNAME%:RX"
```

### B — (Optional) Install local Python deps so you can run the Mongo creation script from host if you want to test it locally

```bash
python -m venv .venv
# Linux/WSL
source .venv/bin/activate
# Windows PowerShell
# .\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

### C — Stop and remove current compose stack

```bash
# stop containers
docker compose down
```

### D — (Optional but recommended) Remove volumes (wipe Kafka & Mongo data for a clean run)

```bash
# WARNING: this deletes Kafka topics and MongoDB data created by compose
docker compose down -v
```

If you want to be less destructive, skip `-v` (keeps volumes).

### E — Build the updated Connect image (or full stack)

If you changed the Dockerfile for `connect`, rebuild that service. You can build only connect or the whole compose set.

```bash
# build only the connect service (faster)
docker compose build --no-cache connect

# or build everything
docker compose build --no-cache
```

### F — Start everything

```bash
docker compose up -d
```

### G — Tail logs and watch the init sequence

```bash
# tail connect logs (our init script logs will appear here)
docker compose logs -f connect
```

Watch for lines like:

* `[init] Kafka reachable. Running create-topics script.`
* `[create-mongo] MongoDB reachable. Proceeding to create collections/indexes.`
* `Posted mongo-sink-vehicle_latest.json` (or connector posted messages)
* `Init tasks finished.`

### H — Confirm stack is healthy

```bash
docker compose ps
```

---

# 3) Quick verification commands (after stack is up)

### Check topics

```bash
docker run --rm --network host confluentinc/cp-kafka:7.4.1 \
  kafka-topics --bootstrap-server localhost:29092 --list
```

### Describe the `vehicle_latest` topic (should exist and be compacted)

```bash
docker run --rm --network host confluentinc/cp-kafka:7.4.1 \
  kafka-topics --bootstrap-server localhost:29092 --describe --topic vehicle_latest
```

### Check ksql server is reachable

```bash
curl http://localhost:8088/info
```

### Check Connect REST + connector list

```bash
curl http://localhost:8083/connectors | jq .
```

Check status for each:

```bash
curl http://localhost:8083/connectors/mongo-sink-vehicle-latest/status | jq .
curl http://localhost:8083/connectors/mongo-sink-telemetry-raw/status | jq .
```

### Verify Mongo collections & indexes were created

```bash
# find mongodb container name first (or use compose service name)
docker ps -f name=mongodb

# list collections and indexes
docker exec -it $(docker ps -q -f name=mongodb) mongosh telemetry_db --eval 'print("collections:", db.getCollectionNames()); print("vehicle_latest indexes:"); printjson(db.vehicle_latest.getIndexes()); print("telemetry_raw indexes:"); printjson(db.telemetry_raw.getIndexes());'
```

### Quick smoke test: publish a sample message and tail telemetry_raw

Publish one of your uploaded sample files:

```bash
# Linux/WSL example (use the path present in your environment)
cat /mnt/data/use_case1.json | docker run --rm -i --network host confluentinc/cp-kafka:7.4.1 \
  kafka-console-producer --bootstrap-server localhost:29092 --topic telemetry
```

Then check `telemetry_raw` topic messages:

```bash
docker run --rm --network host confluentinc/cp-kafka:7.4.1 \
  kafka-console-consumer --bootstrap-server localhost:29092 --topic telemetry_raw --from-beginning --max-messages 5
```

And check Mongo wrote documents:

```bash
docker exec -it $(docker ps -q -f name=mongodb) mongosh telemetry_db --eval 'db.telemetry_raw.find().limit(3).pretty(); db.vehicle_latest.find().limit(3).pretty();'
```

---

# 4) Troubleshooting tips (quick)

* If `connect` logs show `plugin class not found`, make sure your local JAR is present in `docker-connect/connect-plugins` and that Docker Compose mounts that folder into the container (and that you `docker compose build` after changing Dockerfile).
* If `entrypoint-init.sh` fails due to missing `jq` or `nc` or `python3`, rebuild connect image ensuring apt installs were added in Dockerfile.
* If KSQL errors about transaction topics / RF, ensure your Kafka broker env has replication factor settings set to `1` for dev (we already added those earlier).
* If connectors fail to register, check Connect REST (curl) and connect logs for errors. Posting connectors inside the init script uses `curl` to `http://localhost:8083/connectors` — ensure Connect is running on that port when init tries to post. The script sleeps a bit after posting ksql files; tweak `sleep` durations if needed.

---

# Final note — automation status

With the Dockerfile, `entrypoint-init.sh`, `create-mongo-collections.py`, `.ksql` files and connector JSON copied in, and the `create-topics.sh` + `run_ksql_files.sh` scripts in place, the container will now:

1. ensure connectors jars are present (hub or local fallback),
2. create required Kafka topics,
3. push all `.ksql` files into ksqlDB,
4. create Mongo collections + validators + indexes,
5. register the Mongo sink connectors,
6. then start the normal Connect process.

So yes — this will be fully automated when you rebuild and start the stack using the commands above.

---

If you want I can now:

* print the exact `requirements.txt` file into the project (I already gave content) and show a final one-line script to run the whole redeploy flow, or
* generate a short PowerShell script that runs the same sequence for Windows users.

Which one next?
