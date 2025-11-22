"""
Orchestrator helpers for sensible startup ordering and health checks.

Provides:
- wait_for_kafka_bootstrap
- ensure_topics_exist
- wait_for_topic_messages
- wait_for_ksql_table_materialization
- wait_for_connectors
- wait_for_mongo
"""

import time
import logging
from typing import List, Optional
import requests
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


logger = logging.getLogger("pipeline.orchestrator")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(asctime)s] [orch] %(message)s", "%H:%M:%S"))
logger.addHandler(handler)


class Orchestrator:
    def __init__(
        self,
        kafka_bootstrap: str = "kafka:9092",
        ksql_url: str = "http://localhost:8088",
        mongo_url: str = "mongodb://mongodb:27017",
        check_interval: float = 2.0,
    ):
        self.kafka_bootstrap = kafka_bootstrap
        self.ksql_url = ksql_url.rstrip("/")
        self.mongo_url = mongo_url
        self.check_interval = check_interval

    # ----------------------------------------------------------------------
    # Kafka Health
    # ----------------------------------------------------------------------
    def wait_for_kafka_bootstrap(self, timeout: int = 60) -> bool:
        """
        Wait until Kafka bootstrap responds via AdminClient.
        """
        logger.info(f"Waiting for Kafka at {self.kafka_bootstrap} (timeout {timeout}s)...")
        start = time.time()

        while time.time() - start < timeout:
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=self.kafka_bootstrap,
                    request_timeout_ms=5000,
                )
                # list_topics() without unsupported timeout argument
                _ = admin.list_topics()
                admin.close()
                logger.info("Kafka bootstrap reachable")
                return True

            except Exception as e:
                logger.info(f"Kafka not ready yet: {e}")

            time.sleep(self.check_interval)

        logger.error("Kafka bootstrap timed out")
        return False

    # ----------------------------------------------------------------------
    # Topic existence
    # ----------------------------------------------------------------------
    def ensure_topics_exist(
        self,
        topics: List[str],
        partitions: int = 1,
        replication_factor: int = 1,
        create_if_missing: bool = True,
        timeout: int = 30,
    ) -> bool:
        """
        Ensure that topics exist on the broker.
        """
        logger.info(f"Ensuring topics exist: {topics}")

        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.kafka_bootstrap,
                request_timeout_ms=10000,
            )
        except Exception as e:
            logger.error(f"Failed to create Kafka AdminClient: {e}")
            return False

        start = time.time()

        while time.time() - start < timeout:
            try:
                existing = set(admin.list_topics())
                missing = [t for t in topics if t not in existing]

                if not missing:
                    logger.info("All required topics present")
                    admin.close()
                    return True

                if create_if_missing:
                    new_topics = [
                        NewTopic(
                            name=t,
                            num_partitions=partitions,
                            replication_factor=replication_factor,
                        )
                        for t in missing
                    ]
                    try:
                        admin.create_topics(new_topics=new_topics, validate_only=False)
                        logger.info(f"Created topics: {missing}")
                    except Exception as e:
                        logger.info(f"Topic create attempt failed (retrying): {e}")
                else:
                    logger.info(f"Missing required topics: {missing}")

            except Exception as e:
                logger.info(f"Topic existence check failed: {e}")

            time.sleep(self.check_interval)

        admin.close()
        logger.error("Topic existence check timed out")
        return False

    # ----------------------------------------------------------------------
    # Topic message readiness
    # ----------------------------------------------------------------------
    def wait_for_topic_messages(
        self,
        topic: str,
        min_messages: int = 1,
        partitions: Optional[List[int]] = None,
        timeout: int = 30,
    ) -> bool:
        """
        Wait until topic has min_messages available across partitions.
        Uses end_offsets to check quickly without consuming.
        """
        logger.info(
            f"Waiting for at least {min_messages} message(s) on topic {topic} (timeout {timeout}s)"
        )
        start = time.time()

        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_bootstrap,
                enable_auto_commit=False,
            )
        except Exception as e:
            logger.error(f"KafkaConsumer failed: {e}")
            return False

        while time.time() - start < timeout:
            try:
                pf = consumer.partitions_for_topic(topic)
                if not pf:
                    logger.info(f"Topic {topic} not visible yet.")
                    time.sleep(self.check_interval)
                    continue

                tps = [TopicPartition(topic, p) for p in sorted(pf)]
                end_offsets = consumer.end_offsets(tps)
                total = sum(end_offsets.get(tp, 0) for tp in tps)

                logger.info(f"Topic {topic}: total messages = {total}")

                if total >= min_messages:
                    consumer.close()
                    logger.info(f"Topic {topic} has sufficient messages.")
                    return True

            except Exception as e:
                logger.info(f"Error checking offsets: {e}")

            time.sleep(self.check_interval)

        consumer.close()
        logger.error(f"Timeout waiting for messages on topic {topic}")
        return False

    # ----------------------------------------------------------------------
    # ksqlDB health + table materialization
    # ----------------------------------------------------------------------
    def ksql_post(self, path: str, payload: dict, timeout: int = 10):
        url = f"{self.ksql_url}/{path.lstrip('/')}"
        headers = {"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"}
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def wait_for_ksql_table_materialization(
        self,
        table_name: str,
        backing_topic_hint: Optional[str] = None,
        timeout: int = 60,
    ) -> bool:
        """
        Poll DESCRIBE EXTENDED and optionally backing_topic offsets until materialized.
        """
        logger.info(
            f"Waiting for ksqlDB table {table_name} materialization (timeout {timeout}s)"
        )
        start = time.time()

        while time.time() - start < timeout:
            try:
                payload = {
                    "ksql": f"DESCRIBE EXTENDED {table_name};",
                    "streamsProperties": {},
                }
                out = self.ksql_post("/ksql", payload, timeout=10)

                if backing_topic_hint:
                    if self.wait_for_topic_messages(backing_topic_hint, min_messages=1, timeout=10):
                        logger.info(f"Backing topic {backing_topic_hint} has data.")
                        return True

                logger.info(f"DESCRIBE EXTENDED returned for {table_name}.")
                return True

            except Exception as e:
                logger.info(f"ksqlDB describe not ready: {e}")

            time.sleep(self.check_interval)

        logger.error(f"Timeout waiting for table {table_name} materialization")
        return False

    # ----------------------------------------------------------------------
    # Connectors
    # ----------------------------------------------------------------------
    def wait_for_connectors(
        self,
        connect_url: str = "http://connect:8083",
        connectors: Optional[List[str]] = None,
        timeout: int = 30,
    ) -> bool:
        logger.info(f"Waiting for connectors @ {connect_url}")

        start = time.time()

        while time.time() - start < timeout:
            try:
                r = requests.get(f"{connect_url}/connectors", timeout=5)
                r.raise_for_status()

                installed = r.json()
                logger.info(f"Connectors installed: {installed}")

                if connectors:
                    missing = [c for c in connectors if c not in installed]
                    if missing:
                        logger.info(f"Missing connectors: {missing}")
                        time.sleep(self.check_interval)
                        continue

                    # Ensure each connector + tasks are running
                    for c in connectors:
                        st = requests.get(f"{connect_url}/connectors/{c}/status", timeout=5).json()
                        cstate = st.get("connector", {}).get("state")
                        tstates = [t.get("state") for t in st.get("tasks", [])]

                        if cstate != "RUNNING" or any(t != "RUNNING" for t in tstates):
                            logger.info(f"Connector {c} not running yet: {cstate}/{tstates}")
                            break
                    else:
                        logger.info("All connectors RUNNING.")
                        return True
                else:
                    logger.info("Connect worker reachable")
                    return True

            except Exception as e:
                logger.info(f"Connect not ready: {e}")

            time.sleep(self.check_interval)

        logger.error("Timeout waiting for connectors")
        return False

    # ----------------------------------------------------------------------
    # MongoDB readiness
    # ----------------------------------------------------------------------
    def wait_for_mongo(self, timeout: int = 30) -> bool:
        logger.info(f"Waiting for MongoDB at {self.mongo_url} (timeout {timeout}s)")
        start = time.time()

        while time.time() - start < timeout:
            try:
                client = MongoClient(self.mongo_url, serverSelectionTimeoutMS=3000)
                client.server_info()
                client.close()
                logger.info("MongoDB reachable")
                return True

            except Exception as e:
                logger.info(f"MongoDB not ready: {e}")

            time.sleep(self.check_interval)

        logger.error("Timeout waiting for MongoDB")
        return False
    
    
    def ksql_server_url():
        """
        Get the ksqlDB server URL from environment or default.
        """
        import os

        return os.environ.get("KSQL_URL", "http://localhost:8088") 