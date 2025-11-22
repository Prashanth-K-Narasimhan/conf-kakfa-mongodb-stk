import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaConsumer, TopicPartition
from typing import List, Tuple
from pipeline.config import KAFKA_BOOTSTRAP

def log(*parts):
    import time as _t
    print("[{}]".format(_t.strftime("%H:%M:%S")), *parts)

def wait_for_kafka(timeout=60) -> bool:
    start = time.time()
    while True:
        try:
            a = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=2000)
            a.close()
            return True
        except Exception:
            if time.time() - start > timeout:
                return False
            time.sleep(1)

def create_kafka_topics(topics: List[Tuple[str, int, int]] = None) -> None:
    if topics is None:
        topics = [("telemetry", 1, 1), ("telemetry_raw", 1, 1), ("vehicle_latest", 1, 1)]
    if not wait_for_kafka(timeout=30):
        raise RuntimeError("Kafka not reachable at " + KAFKA_BOOTSTRAP)
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        existing = set(admin.list_topics())
        desired = {}
        for name, partitions, rf in topics:
            desired[name] = NewTopic(name, num_partitions=partitions, replication_factor=rf)
        to_create = [t for name, t in desired.items() if name not in existing]
        if not to_create:
            log("kafka topics already present")
            return
        try:
            admin.create_topics(new_topics=to_create, validate_only=False)
            created = []
            for nt in to_create:
                name = getattr(nt, "topic", None) or getattr(nt, "name", None) or "<unknown>"
                created.append(name)
            log("kafka created topics:", ", ".join(created))
        except TopicAlreadyExistsError:
            log("kafka some topics already existed. OK.")
        except Exception as e:
            log("kafka create topics exception:", e)
    finally:
        admin.close()

def topic_has_messages(topic: str, partition: int = 0, bootstrap: str = None) -> bool:
    bs = bootstrap or KAFKA_BOOTSTRAP
    try:
        tp = TopicPartition(topic, partition)
        c = KafkaConsumer(bootstrap_servers=bs, enable_auto_commit=False)
        c.poll(timeout_ms=100)
        end = c.end_offsets([tp]).get(tp, 0)
        c.close()
        return end > 0
    except Exception:
        return False
