import time
import requests
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import KafkaError

# Simple health checks. 

def check_kafka(bootstrap, timeout=45):
    start = time.time()
    while (time.time() - start) < timeout:
        try:
            client = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=3000)
            client.close()
            return True
        except Exception:
            time.sleep(2)
    return False

def wait_for_data(topic, bootstrap=None, min_msgs=1, timeout=30):
    """
    Blocks until we see actual data in the topic. 
    Useful to prevent KSQL/Connect from failing on empty topics.
    """
    if not bootstrap:
        from pipeline.config import KAFKA_BOOTSTRAP
        bootstrap = KAFKA_BOOTSTRAP

    print(f"[orch] Waiting for data in '{topic}'...")
    start = time.time()
    
    # Consumer might fail if broker isn't fully up, so retry connection too
    while (time.time() - start) < timeout:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=1000
            )
            
            # Poll quickly
            results = consumer.poll(timeout_ms=1000)
            if results:
                consumer.close()
                return True
            
            consumer.close()
        except Exception as e:
            # ignore connection errors while spinning up
            pass
            
        time.sleep(1)
    
    print(f"[orch] WARN: Timed out waiting for data in {topic}")
    return False