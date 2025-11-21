#!/usr/bin/env python3
"""
producer_simulator.py

Simulate vehicle telemetry messages and produce to Kafka topic 'telemetry'.

Message schema (one metric per message):
{
  "did": "981",
  "timestamp": 1700000000000,         # epoch ms (event time)
  "name": "soc",                      # metric name: odometer | soc | speed | ignition_status
  "float_value": 123.45,              # for odometer etc. (nullable)
  "int_value": 95,                    # for soc, speed, ignition (nullable)
  "eventTime": "2025-11-19T12:00:00Z",
  "receivedTime": "2025-11-19T12:00:01Z"
}
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

# --- helpers ---------------------------------------------------------------

def now_ms():
    return int(time.time() * 1000)

def iso_now(ts_ms=None):
    if ts_ms is None:
        ts_ms = now_ms()
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()

def mk_msg(did: str, name: str, event_ts_ms: int):
    """
    Create a metric message for device `did`, metric `name` and event timestamp.
    Returns JSON-serializable dict.
    """
    base = {
        "did": str(did),
        "timestamp": int(event_ts_ms),
        "name": name,
        "eventTime": iso_now(event_ts_ms),
        "receivedTime": iso_now()  # producer ingestion time
    }

    if name == "odometer":
        # monotonic increase per device is handled at caller side
        base["float_value"] = round(random.uniform(50.0, 200.0), 2)
        base["int_value"] = None
    elif name == "soc":
        base["int_value"] = random.randint(10, 100)  # percentage
        base["float_value"] = None
    elif name == "speed":
        base["int_value"] = random.randint(0, 120)  # km/h
        base["float_value"] = None
    elif name == "ignition_status":
        base["int_value"] = random.choice([0, 1])
        base["float_value"] = None
    else:
        # generic fallback
        base["int_value"] = None
        base["float_value"] = None

    # remove explicit None fields for compactness
    return {k: v for k, v in base.items() if v is not None}

# --- simulator -------------------------------------------------------------

class DeviceState:
    def __init__(self, did, odo_start=100.0):
        self.did = str(did)
        self.odo = float(odo_start)

    def bump_odometer(self):
        # small random increment
        self.odo += random.uniform(0.01, 0.5)
        self.odo = round(self.odo, 2)
        return self.odo

def create_producer(bootstrap):
    conf = {
        "bootstrap.servers": bootstrap,
        # you can add linger.ms, acks etc if needed
    }
    return Producer(conf)

def publish_messages(p: Producer, topic: str, messages):
    """
    messages: list of dicts
    sends each message with key=did for partitioning
    """
    for m in messages:
        key = m.get("did")
        p.produce(topic, json.dumps(m).encode("utf-8"), key=str(key))
    p.flush()

def simulate_once(devices, topic, p, miss_rate=0.1):
    """
    Simulate a single tick for all devices.
    For each device we emit 0..4 metrics where each metric has a chance to be missing.
    """
    out = []
    for dev in devices:
        # event time slightly jittered around now
        event_ts = now_ms() - random.randint(0, 500)  # up to 500ms skew

        # ensure odometer monotonic per device
        odo = dev.bump_odometer()

        # metrics set. We will decide which ones to emit (simulate missing)
        metrics = [
            ("odometer", {"float_value": odo}),
            ("soc", {}),
            ("speed", {}),
            ("ignition_status", {})
        ]

        for name, _meta in metrics:
            if random.random() < miss_rate:
                # simulate missing reading
                continue
            msg = mk_msg(dev.did, name, event_ts)
            # override odometer value with deterministic one
            if name == "odometer":
                msg["float_value"] = odo
            out.append(msg)

    if out:
        publish_messages(p, topic, out)
    return out

# --- CLI ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Telemetry producer simulator")
    parser.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="telemetry", help="Kafka topic to publish")
    parser.add_argument("--num-devices", type=int, default=3, help="Number of devices to simulate")
    parser.add_argument("--rate", type=float, default=1.0, help="Ticks per second (each tick produces for all devices)")
    parser.add_argument("--duration", type=int, default=0, help="Duration in seconds to run (0 = run forever)")
    parser.add_argument("--count", type=int, default=0, help="Number of ticks to run and exit (overrides duration if >0)")
    parser.add_argument("--miss-rate", type=float, default=0.1, help="Probability a metric is missing for a device (0..1)")
    parser.add_argument("--once", action="store_true", help="Emit one tick and exit")
    args = parser.parse_args()

    # create device states
    devices = [DeviceState(did=str(1000 + i), odo_start=100.0 + i*5.0) for i in range(args.num_devices)]

    p = create_producer(args.bootstrap)

    if args.once:
        produced = simulate_once(devices, args.topic, p, miss_rate=args.miss_rate)
        print("Produced", len(produced), "messages")
        return

    tick = 0
    start = time.time()
    try:
        while True:
            tick += 1
            produced = simulate_once(devices, args.topic, p, miss_rate=args.miss_rate)
            if produced:
                # quick console summary
                print(f"[{datetime.utcnow().isoformat()}] tick {tick} produced {len(produced)} messages (devices {len(devices)})")
            else:
                print(f"[{datetime.utcnow().isoformat()}] tick {tick} produced 0 messages")

            # termination conditions
            if args.count and tick >= args.count:
                break
            if args.duration and (time.time() - start) >= args.duration:
                break

            time.sleep(1.0 / max(0.001, args.rate))
    except KeyboardInterrupt:
        print("Stopped by user")

if __name__ == "__main__":
    main()
