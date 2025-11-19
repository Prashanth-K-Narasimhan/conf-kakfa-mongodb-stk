# producer.py
"""Simple Kafka producer for telemetry messages."""

import json
import time
from confluent_kafka import Producer


config = {
    "bootstrap.servers": "localhost:29092",
    "acks": "all",
    "linger.ms": 0,
}

producer = Producer(config)


def delivery_report(err, msg):
    """Delivery callback invoked by confluent-kafka on message delivery."""
    if err is not None:
        print("Delivery failed:", err)
    else:
        print("Delivered to", msg.topic(), "offset", msg.offset())


def send(msg):
    """Produce a single JSON message to the telemetry topic."""
    producer.produce(
        topic="telemetry",
        value=json.dumps(msg).encode("utf-8"),
        on_delivery=delivery_report,
    )
    producer.flush()


if __name__ == "__main__":
    example = {
        "did": "981",
        "timestamp": int(time.time() * 1000),
        "name": "soc",
        "int_value": 95,
        "eventTime": "2025-11-19T12:00:00Z",
    }
    send(example)
    print("sent:", example)
