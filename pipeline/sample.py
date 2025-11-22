"""
Simple debug-only producer that prints generated messages to the console.
No Kafka dependency. This is only for inspecting the payload format
before sending via the real producer.
"""

import json
import random
import time
from datetime import datetime, timezone


def iso_now(ts_ms=None):
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def print_message(msg):
    """
    Pretty-print a message to console.
    """
    print("------------------------------------------------")
    print("Outgoing message:")
    print(json.dumps(msg, indent=4))
    print("------------------------------------------------")


def mk_debug_message(did, name, event_ts_ms, odo_value=None):
    """
    Same schema as TELEMETRY_RAW but without Kafka.
    """
    value_type = "0" if name == "odometer" else "1"
    event_ts = int(event_ts_ms)
    received_ts_ms = int(time.time() * 1000)

    payload = {
        "did": str(did),
        "timestamp": event_ts,
        "name": name,
        "value_type": value_type,
        "float_value": None,
        "string_value": "",
        "int_value": None,
        "eventTime": iso_now(event_ts),
        "receivedTime": iso_now(received_ts_ms),
    }

    if name == "odometer":
        payload["float_value"] = float(odo_value) if odo_value is not None else round(random.uniform(100.0, 200.0), 2)
    elif name == "soc":
        payload["int_value"] = str(random.randint(10, 100))
    elif name == "speed":
        payload["int_value"] = str(random.randint(20, 120))
    elif name == "ignition_status":
        payload["int_value"] = str(random.choice([0, 1]))

    return {k: v for k, v in payload.items() if v is not None}


def send_and_print_messages():
    """
    Hard-coded caller:
    - 3 devices
    - 1 tick of messages
    - odometer, soc, speed, ignition_status
    - prints each message cleanly
    """

    num_devices = 3
    device_start_odo = 100.0

    devices = []
    for i in range(num_devices):
        devices.append({
            "did": str(1000 + i),
            "odo": device_start_odo + (i * 5.0)
        })

    now_ms = int(time.time() * 1000)
    metrics = ["odometer", "soc", "speed", "ignition_status"]

    print("### Debug producer — printing messages only (no Kafka) ###\n")

    for dev in devices:
        # bump odometer
        dev["odo"] += random.uniform(0.01, 0.5)
        dev["odo"] = round(dev["odo"], 2)

        for name in metrics:
            msg = mk_debug_message(
                did=dev["did"],
                name=name,
                event_ts_ms=now_ms,
                odo_value=dev["odo"]
            )
            print_message(msg)

    print("\n### End of debug batch ###")


if __name__ == "__main__":
    # hard-coded call
    send_and_print_messages()
