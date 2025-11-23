# Windows PowerShell example (works if docker network name is conf-kafka-mongodb-stk_default)
docker run --rm -it --network conf-kafka-mongodb-stk_default confluentinc/ksqldb-cli:0.28.2 ksql http://ksqldb-server:8088
