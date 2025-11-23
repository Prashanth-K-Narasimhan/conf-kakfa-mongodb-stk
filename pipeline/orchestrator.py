"""
Orchestrator helpers for sensible startup ordering and health checks.
This fixed file ensures all methods are inside the Orchestrator class (no indentation mistakes)
and provides a simple, reliable wait_for_topic_messages implementation.
"""

import time
import logging
from typing import List, Optional
import requests
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient


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

    # -----------------------------------------------------------
    # Kafka Health
    # -----------------------------------------------------------
    def wait_for_kafka_bootstrap(self, timeout: int = 60) -> bool:
        logger.info(f"Waiting for Kafka at {self.kafka_bootstrap} (timeout {timeout}s)...")
        start = time.time()

        while time.time() - start < timeout:
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=self.kafka_bootstrap,
                    request_timeout_ms=5000,
                )
                _ = admin.list_topics()
                admin.close()
                logger.info("Kafka bootstrap reachable")
                return True

            except Exception as e:
                logger.info(f"Kafka not ready yet: {e}")

            time.sleep(self.check_interval)

        logger.error("Kafka bootstrap timed out")
        return False

    # -----------------------------------------------------------
    # Topic existence
    # -----------------------------------------------------------
    def ensure_topics_exist(
        self,
        topics: List[str],
        partitions: int = 1,
        replication_factor: int = 1,
        create_if_missing: bool = True,
        timeout: int = 30,
    ) -> bool:
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
                        NewTopic(name=t, num_partitions=partitions, replication_factor=replication_factor)
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

    # -----------------------------------------------------------
    # Topic message readiness (simple)
    # -----------------------------------------------------------
    def wait_for_topic_messages(
        self,
        topic: str,
        min_messages: int = 1,
        timeout: int = 30,
    ) -> bool:
        """
        Simplest reliable check: return True if the topic has at least `min_messages` messages.
        The function will wait up to `timeout` seconds for the topic to become visible.
        """
        logger.info(f"Checking if topic '{topic}' has {min_messages}+ messages (timeout {timeout}s)")
        start = time.time()

        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_bootstrap,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                consumer_timeout_ms=2000,
            )
        except Exception as e:
            logger.error(f"KafkaConsumer failed: {e}")
            return False

        try:
            while time.time() - start < timeout:
                try:
                    parts = consumer.partitions_for_topic(topic)
                    if not parts:
                        # Topic not visible yet — retry until timeout
                        time.sleep(0.5)
                        continue

                    tps = [TopicPartition(topic, p) for p in sorted(parts)]
                    end_offsets = consumer.end_offsets(tps)
                    total = sum(end_offsets.values())

                    logger.info(f"Topic '{topic}' message count = {total}")
                    return total >= min_messages

                except Exception as e:
                    logger.info(f"Error while checking offsets for '{topic}': {e}")
                    time.sleep(0.5)

            logger.error(f"Timeout: topic '{topic}' has no messages")
            return False

        finally:
            try:
                consumer.close()
            except Exception:
                pass

    # -----------------------------------------------------------
    # KSQL REST support
    # -----------------------------------------------------------
    def ksql_post(self, path: str, payload: dict, timeout: int = 10):
        url = f"{self.ksql_url}/{path.lstrip('/')}"
        headers = {"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"}
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    # -----------------------------------------------------------
    # ksql table materialization (more correct)
    # -----------------------------------------------------------
    def wait_for_ksql_table_materialization(
        self,
        table_name: str,
        backing_topic_hint: Optional[str] = None,
        timeout: int = 60,
    ) -> bool:
        """
        A table is considered materialized ONLY when:
        1. DESCRIBE EXTENDED works, AND
        2. Backing topic has at least 1 message (if provided)
        """
        logger.info(f"Waiting for ksqlDB table {table_name} materialization (timeout {timeout}s)")
        start = time.time()

        while time.time() - start < timeout:
            try:
                payload = {
                    "ksql": f"DESCRIBE {table_name} EXTENDED;",
                    "streamsProperties": {},
                }
                _ = self.ksql_post("/ksql", payload, timeout=10)

                if backing_topic_hint:
                    if self.wait_for_topic_messages(backing_topic_hint, min_messages=1, timeout=10):
                        logger.info(f"Table {table_name} materialized (backing topic has data)")
                        return True
                else:
                    logger.info(f"DESCRIBE EXTENDED returned for {table_name}")
                    return True

            except Exception as e:
                logger.info(f"ksqlDB describe not ready: {e}")

            time.sleep(self.check_interval)

        logger.error(f"Timeout waiting for table {table_name} materialization")
        return False

    # -----------------------------------------------------------
    # Connectors
    # -----------------------------------------------------------
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
                        time.sleep(self.check_interval)
                        continue

                    # Ensure each connector + task is RUNNING
                    all_good = True
                    for c in connectors:
                        st = requests.get(f"{connect_url}/connectors/{c}/status", timeout=5).json()
                        cstate = st.get("connector", {}).get("state")
                        tstates = [t.get("state") for t in st.get("tasks", [])]

                        if cstate != "RUNNING" or any(t != "RUNNING" for t in tstates):
                            all_good = False
                            break

                    if all_good:
                        logger.info("All connectors RUNNING")
                        return True

                else:
                    logger.info("Connect worker reachable")
                    return True

            except Exception as e:
                logger.info(f"Connect not ready: {e}")

            time.sleep(self.check_interval)

        logger.error("Timeout waiting for connectors")
        return False

    # -----------------------------------------------------------
    # MongoDB readiness
    # -----------------------------------------------------------
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


# -----------------------------------------------------------
# Correct global helper
# -----------------------------------------------------------
def ksql_server_url():
    import os
    return os.environ.get("KSQL_URL", "http://localhost:8088")
