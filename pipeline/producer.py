#!/usr/bin/env python3
"""
Producer module for telemetry pipeline.

This module exposes:
- background_producer_corrected(stop_event, stats, stats_lock, num_devices, rate, miss_rate, bootstrap, topic, once)
    The worker function suitable as a Thread target. It updates stats["sent"] for acknowledged sends.

- start_producer(num_devices, rate, miss_rate, bootstrap, topic, daemon=True)
    Convenience wrapper that starts the worker and returns (stop_event, thread, stats, stats_lock).

It also supports running as a standalone script (CLI) with --once mode or continuous mode.
"""

import argparse
import threading
import time
import random
import json
from typing import Optional, Tuple

# Try to import pipeline.config defaults; fall back to sane defaults if not present.
try:
    from pipeline.config import (
        PRODUCER_NUM_DEVICES,
        PRODUCER_RATE,
        PRODUCER_MISS_RATE,
        PRODUCER_BOOTSTRAP,
        PRODUCER_TOPIC,
    )
except Exception:
    PRODUCER_NUM_DEVICES = 3
    PRODUCER_RATE = 1.0
    PRODUCER_MISS_RATE = 0.0
    PRODUCER_BOOTSTRAP = "localhost:29092"
    PRODUCER_TOPIC = "telemetry_raw"

try:
    from kafka import KafkaProducer
except Exception as e:
    raise RuntimeError("kafka-python is required. Install with: pip install kafka-python") from e


def log(*parts):
    import time as _t
    print("[{}]".format(_t.strftime("%H:%M:%S")), *parts)


class DeviceState:
    def __init__(self, did: str, odo_start: float = 100.0):
        self.did = str(did)
        self.odo = float(odo_start)

    def bump_odometer(self) -> float:
        self.odo += random.uniform(0.01, 0.5)
        self.odo = round(self.odo, 2)
        return self.odo


def iso_now(ts_ms: Optional[int] = None) -> str:
    from datetime import datetime, timezone
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def mk_metric_message(did: str, name: str, event_ts_ms: int, odo_value: Optional[float] = None):
    """Construct message matching TELEMETRY_RAW schema."""
    value_type = "0" if name == "odometer" else "1"
    event_ts = int(event_ts_ms)
    received_ts_ms = int(time.time() * 1000)

    payload = {
        "did": str(did),
        "timestamp": int(event_ts),
        "name": name,
        "value_type": value_type,
        "float_value": None,
        "string_value": "",
        "int_value": None,
        "eventTime": iso_now(event_ts),
        "receivedTime": iso_now(received_ts_ms),
    }

    if name == "odometer":
        payload["float_value"] = float(odo_value) if odo_value is not None else float(round(random.uniform(100.0, 200.0), 2))
    elif name == "soc":
        payload["int_value"] = str(random.randint(10, 100))
    elif name == "speed":
        payload["int_value"] = str(random.randint(20, 120))
    elif name == "ignition_status":
        payload["int_value"] = str(random.choice([0, 1]))

    return {k: v for k, v in payload.items() if v is not None}


def background_producer_corrected(
    stop_event: threading.Event,
    stats: dict,
    stats_lock: threading.Lock,
    num_devices: int = PRODUCER_NUM_DEVICES,
    rate: float = PRODUCER_RATE,
    miss_rate: float = PRODUCER_MISS_RATE,
    bootstrap: str = PRODUCER_BOOTSTRAP,
    topic: str = PRODUCER_TOPIC,
    once: bool = False,
) -> None:
    """
    Background producer loop.
    - stop_event: threading.Event to request shutdown
    - stats: dict which will be updated with key 'sent' (acked count)
    - stats_lock: threading.Lock to serialize updates to stats
    - once: if True, send a single batch (one tick) and return
    """
    # defensive casts: allow floats/strings in config
    num_devices = int(num_devices)
    rate = float(rate)
    miss_rate = float(miss_rate)

    devices = [DeviceState(did=str(1000 + i), odo_start=100.0 + i * 5.0) for i in range(num_devices)]
    p = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=5,
    )

    def on_send_success(_):
        with stats_lock:
            stats["sent"] = stats.get("sent", 0) + 1

    def on_send_error(excp):
        log("producer send error (callback):", excp)

    tick_interval = 1.0 / max(0.001, rate)

    try:
        while not stop_event.is_set():
            now_ms = int(time.time() * 1000)
            for dev in devices:
                odo_val = dev.bump_odometer()
                for name in ["odometer", "soc", "speed", "ignition_status"]:
                    if random.random() < miss_rate:
                        continue
                    msg = mk_metric_message(dev.did, name, now_ms, odo_value=odo_val)
                    try:
                        fut = p.send(topic, key=dev.did, value=msg)
                        fut.add_callback(on_send_success)
                        fut.add_errback(on_send_error)
                    except Exception as e:
                        log("producer send immediate error:", e)

            # ensure in-flight messages are flushed, especially for --once
            try:
                p.flush(timeout=2)
            except Exception:
                pass

            if once:
                break

            waited = 0.0
            while waited < tick_interval and not stop_event.is_set():
                sleep_chunk = min(0.1, tick_interval - waited)
                time.sleep(sleep_chunk)
                waited += sleep_chunk

    finally:
        try:
            p.flush(timeout=2)
        except Exception:
            pass
        try:
            p.close()
        except Exception:
            pass
        log("producer thread exiting")


def start_producer(
    num_devices: int = PRODUCER_NUM_DEVICES,
    rate: float = PRODUCER_RATE,
    miss_rate: float = PRODUCER_MISS_RATE,
    bootstrap: str = PRODUCER_BOOTSTRAP,
    topic: str = PRODUCER_TOPIC,
    daemon: bool = True,
) -> Tuple[threading.Event, threading.Thread, dict, threading.Lock]:
    """
    Start the producer in a background thread and return (stop_event, thread, stats, stats_lock).
    Stats will contain 'sent' = number of acknowledged messages.
    """
    stop_event = threading.Event()
    stats = {"sent": 0}
    stats_lock = threading.Lock()
    thr = threading.Thread(
        target=background_producer_corrected,
        args=(stop_event, stats, stats_lock, num_devices, rate, miss_rate, bootstrap, topic, False),
        daemon=daemon,
        name="bg-producer",
    )
    thr.start()
    return stop_event, thr, stats, stats_lock


def run_cli():
    parser = argparse.ArgumentParser(description="Standalone telemetry Kafka producer")
    parser.add_argument("--bootstrap", default=PRODUCER_BOOTSTRAP)
    parser.add_argument("--topic", default=PRODUCER_TOPIC)
    parser.add_argument("--num-devices", type=int, default=PRODUCER_NUM_DEVICES)
    parser.add_argument("--rate", type=float, default=PRODUCER_RATE)
    parser.add_argument("--miss-rate", type=float, default=PRODUCER_MISS_RATE)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    stop_event = threading.Event()
    stats = {"sent": 0}
    stats_lock = threading.Lock()

    thr = threading.Thread(
        target=background_producer_corrected,
        args=(stop_event, stats, stats_lock, args.num_devices, args.rate, args.miss_rate, args.bootstrap, args.topic, args.once),
        daemon=True,
        name="bg-producer-cli",
    )
    thr.start()

    try:
        if args.once:
            # wait for one-shot worker to exit
            while thr.is_alive():
                thr.join(timeout=0.5)
        else:
            log("producer running — press Ctrl+C to stop")
            while not stop_event.is_set():
                time.sleep(0.5)
    except KeyboardInterrupt:
        log("KeyboardInterrupt received — stopping producer...")
        stop_event.set()
        thr.join(timeout=5.0)
        log("producer stopped")

    with stats_lock:
        total_sent = stats.get("sent", 0)

    log(f"Total acknowledged messages sent: {total_sent}")
    # easy to capture in scripts
    print(total_sent)


if __name__ == "__main__":
    run_cli()
