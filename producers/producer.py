#!/usr/bin/env python3
"""
producer.py

Simple telemetry producer that emits messages matching the KSQL schema keys exactly.

Emitted JSON keys:
  DID, TIMESTAMP, NAME, VALUE_TYPE, FLOAT_VALUE, STRING_VALUE, INT_VALUE, EVENTTIME, RECEIVEDTIME

Behavior:
 - Simulates `num_devices` devices with sequential DIDs starting at 1000.
 - Emits odometer (float, VALUE_TYPE "0"), soc/speed/ignition_status (integers, VALUE_TYPE "1").
 - EVENTTIME is the sensor event timestamp (a few ms before RECEIVEDTIME).
 - RECEIVEDTIME is the producer's current time in ISO format.
"""

import time
import json
import random
import argparse
from kafka import KafkaProducer

def iso_now(ts_ms=None):
    from datetime import datetime, timezone
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    return datetime.fromtimestamp(ts_ms/1000.0, tz=timezone.utc).isoformat()

class DeviceState:
    def __init__(self, did:int, odo_start:float=100.0):
        self.did = str(did)
        self.odo = float(odo_start)

    def bump_odometer(self) -> float:
        self.odo += random.uniform(0.01, 0.5)
        self.odo = round(self.odo, 2)
        return self.odo

def mk_message(device: DeviceState, metric_name: str, now_ms: int):
    """
    Build a message using EXACT key names that match KSQL stream schema.
    - VALUE_TYPE: "0" for odometer (float), "1" otherwise
    - EVENTTIME: a few milliseconds older than RECEIVEDTIME
    - RECEIVEDTIME: now ISO
    """
    # determine value_type: odometer -> "0", others -> "1"
    value_type = "0" if metric_name == "odometer" else "1"

    # event time a little earlier (sensor time)
    event_ts = now_ms - random.randint(0, 500)

    msg = {
        "DID": device.did,
        "TIMESTAMP": int(event_ts),
        "NAME": metric_name,
        "VALUE_TYPE": value_type,
        # use both float/int keys but only populate relevant one
        "FLOAT_VALUE": None,
        "STRING_VALUE": "",
        "INT_VALUE": None,
        "EVENTTIME": iso_now(event_ts),
        "RECEIVEDTIME": iso_now(now_ms)
    }

    if metric_name == "odometer":
        msg["FLOAT_VALUE"] = float(device.bump_odometer())
        msg["INT_VALUE"] = None
    elif metric_name == "soc":
        msg["INT_VALUE"] = random.randint(10, 100)
    elif metric_name == "speed":
        msg["INT_VALUE"] = random.randint(20, 120)
    elif metric_name == "ignition_status":
        msg["INT_VALUE"] = random.choice([0, 1])
    else:
        # generic case
        msg["STRING_VALUE"] = ""
    # Clean keys: remove None values to match earlier samples (optional)
    clean = {k: v for k, v in msg.items() if v is not None}
    return clean


def run_producer(bootstrap: str, topic: str, num_devices: int, rate: float, duration: float):
    """
    Run a simple fixed-run producer.
    :param bootstrap: kafka bootstrap (host:port)
    :param topic: topic name
    :param num_devices: number of device simulations
    :param rate: ticks per second (each tick generates up to 4 metrics per device)
    :param duration: seconds to run; if None, runs forever
    """
    devices = [DeviceState(did=1000 + i, odo_start=100.0 + i*5.0) for i in range(num_devices)]
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=5
    )

    tick_interval = 1.0 / max(0.001, rate)
    infinite = (duration == 0)
    end_time = time.time() + duration if not infinite else None

    print()
    print("Starting producer. Press CTRL+C to stop...")
    print()

    try:
        while True:
            now_ms = int(time.time() * 1000)

            # send messages for each device
            for dev in devices:
                metrics = ["odometer", "soc", "speed", "ignition_status"]
                for name in metrics:
                    msg = mk_message(dev, name, now_ms)
                    producer.send(topic, key=dev.did, value=msg)

            producer.flush()

            # countdown display
            if not infinite:
                remaining = int(end_time - time.time())
                if remaining <= 0:
                    print("\nDuration reached, stopping producer.")
                    break
                print(f"\rTime left: {remaining:2d}s", end="", flush=True)

            # sleep while checking for ctrl+c
            slept = 0.0
            while slept < tick_interval:
                time.sleep(min(0.1, tick_interval - slept))
                slept += min(0.1, tick_interval - slept)

    except KeyboardInterrupt:
        print("\n\nUser interrupted producer (Ctrl+C)")

    finally:
        print("\nFinalizing producer…")
        try:
            producer.flush(timeout=2)
        except:
            pass
        try:
            producer.close()
        except:
            pass
        print("Producer stopped cleanly.")


def main():
    p = argparse.ArgumentParser()
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap")
    p.add_argument("--topic", default="telemetry", help="Kafka topic")
    p.add_argument("--devices", type=int, default=3, help="Number of devices to simulate")
    p.add_argument("--rate", type=float, default=1.0, help="Ticks per second")
    p.add_argument("--duration", type=float, default=30.0, help="Seconds to run. 0 = run forever.")
    args = p.parse_args()

    print(f"[producer] bootstrap={args.bootstrap}, topic={args.topic}, devices={args.devices}")
    run_producer(args.bootstrap, args.topic, args.devices, args.rate, args.duration)
    print("[producer] finished")



if __name__ == "__main__":
    main()
