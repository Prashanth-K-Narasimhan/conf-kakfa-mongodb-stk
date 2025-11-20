Nice — Windows + Docker Desktop + PowerShell noted. I’ll keep PowerShell-native commands, tips specific to WSL/Windows gotchas, and exactly what to run from your repo root so nothing breaks.

I’ll also include the local path to your current `create_pipeline.py` upload so you can reference it quickly: `/mnt/data/create_pipeline.py`

---

# Before you run anything (quick checklist)

1. Docker Desktop running, backend = WSL2.
2. You run PowerShell **from the repository root** (where `docker-compose.yml`, `create_pipeline.py`, `connect-plugins/`, `connectors/`, `ksql/` live).
3. Python available in PATH (run `python --version`). If using a venv activate it first:

   ```powershell
   .\.venv\Scripts\Activate.ps1   # or your venv path
   ```
4. Ensure the connector jar is in the host folder mapped by compose. Example expected host folder:

   ```
   .\connect-plugins\mongo-kafka-connect-1.11.2-all.jar
   ```

If that’s all good, run the commands below.

---

# PowerShell commands to bring the stack up and validate (copy-paste)

## 1) From repo root: start Docker Compose

If your compose file is at repo root:

```powershell
docker compose up -d
```

If it’s in a subfolder (adjust):

```powershell
# run from repo root but point to the compose file
docker compose -f .\docker\docker-compose.yml up -d
```

Give the services ~20–40s to initialize.

## 2) Confirm containers are healthy

```powershell
docker compose ps
```

Look for `connect`, `ksqldb`, `zookeeper`, `kafka`, `mongo` (names will depend on your compose service names).

## 3) Check the plugin jar is visible inside the Connect container

Find Connect container name (example shows how to extract it):

```powershell
# find the container id or name for the connect service
docker compose ps --services
# then inspect logs or run ls inside connect container:
docker compose exec connect bash -c "ls -la /usr/share/confluent-hub-components || true"
```

Expected: you should see `mongo-kafka-connect-1.11.2-all.jar` listed.

If it’s missing on the host, ensure the host folder path used in `docker-compose.yml` matches where the jar actually is (host path must be relative to where you run `docker compose`).

## 4) Run the Python create script (host)

From repo root:

```powershell
python .\create_pipeline.py --replace
```

This will:

* create topics
* create Mongo collections and validators
* apply KSQL files from `.\ksql\`
* register connectors from `.\connectors\`

Watch the output for failures. If connectors fail to register, the script will print the Connect REST response body.

## 5) Inspect Connect logs if connector registration failed

```powershell
docker compose logs connect --tail 200
```

Search for ClassNotFound, NoClassDefFoundError, or plugin loading errors. Example useful command:

```powershell
docker compose logs connect --tail 200 | Select-String -Pattern "ClassNotFound|NoClassDefFoundError|ERROR|Exception" -Context 0,3
```

## 6) Quick smoke tests

* Check Kafka topics exist (you must have `kafka-python` installed or use `kafka-topics` cli if available):

```powershell
python - <<'PY'
from kafka import KafkaAdminClient
print(KafkaAdminClient(bootstrap_servers="localhost:29092").list_topics())
PY
```

* Check KSQL REST is reachable:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8088/ksql" -Body (@{ksql="SHOW STREAMS;"; streamsProperties=@{}} | ConvertTo-Json) -ContentType "application/json"
```

* Check Connect REST lists connectors:

```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connectors"
```

* Check Mongo reachable:

```powershell
# from PowerShell, using mongo cli if installed:
# mongo --eval "db.getMongo().getDBNames()"
# or use Python quick check:
python - <<'PY'
from pymongo import MongoClient
c=MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=3000)
print(c.list_database_names())
PY
```

## 7) Run the tests

```powershell
python .\tests\e2e_test.py
```

It will produce step-by-step output and exit with code indicating pass/fail.

---

# If something fails — the exact outputs I need (paste raw text)

If things fail, copy-paste these outputs (no redaction):

1. `docker compose ps`
2. `docker compose logs connect --tail 300`
3. `docker compose logs ksqldb --tail 300`
4. Output of `python .\create_pipeline.py --replace` (full console)
5. Output of `python .\tests\e2e_test.py` (full console)
6. Result of `Invoke-RestMethod -Uri "http://localhost:8083/connectors"` (or the PowerShell output)

That will let me pinpoint the problem immediately.

---

# Windows-specific gotchas I’ll watch for

1. **Path separators & compose mount relative path** — when you run `docker compose` from repo root, `./connect-plugins` in compose should map to `.\connect-plugins` on Windows; confirm the compose file uses relative UNIX-style paths (Docker Desktop handles them). If you see permission errors or the jar not appearing, it’s usually because compose was run from a different folder or the path in compose is incorrect relative to the current working directory.

2. **File share permissions** — Docker Desktop needs to be allowed to access the drive. If the mounted host directory is empty inside the container, ensure Docker Desktop has file-sharing access to your drive.

3. **Line endings / executable bits** — shell scripts inside containers may need LF endings. But this only matters if you edit scripts on Windows and then run them inside Linux containers. Python files are fine.

4. **WSL vs Windows host network quirks** — using `localhost:29092` is correct for a host process (PowerShell) to connect to Kafka when Docker publishes that port. If you get connection refused, paste `docker compose ps` and we’ll check listeners.

---

# Quick sanity: path to your create script (so you can open it quickly)

`/mnt/data/create_pipeline.py`

(If you want me to auto-generate a PowerShell wrapper that sets the correct env vars and runs the create script and then tails logs, I can paste the full script next. Say “Make PS wrapper” and I’ll output the complete file.)

---

Ready when you are — run the steps above in PowerShell and paste any failing outputs here. We’ll go fix the root cause fast.
