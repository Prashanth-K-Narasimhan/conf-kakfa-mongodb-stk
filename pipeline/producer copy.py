import threading
import time
import random
import json
from kafka import KafkaProducer
from typing import Optional
from pipeline.config import PRODUCER_NUM_DEVICES, PRODUCER_RATE, PRODUCER_MISS_RATE, PRODUCER_BOOTSTRAP, PRODUCER_TOPIC

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
    import time as _t
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()

def mk_metric_message_uppercase(did: str, name: str, event_ts_ms: int, odo_value: Optional[float] = None):
    value_type = "0" if name == "odometer" else "1"
    event_ts = int(event_ts_ms)
    received_ts_ms = int(time.time() * 1000)
    payload = {
        "DID": str(did),
        "TIMESTAMP": int(event_ts),
        "NAME": name,
        "VALUE_TYPE": value_type,
        "FLOAT_VALUE": None,
        "STRING_VALUE": "",
        "INT_VALUE": None,
        "EVENTTIME": iso_now(event_ts),
        "RECEIVEDTIME": iso_now(received_ts_ms)
    }
    if name == "odometer":
        payload["FLOAT_VALUE"] = float(odo_value) if odo_value is not None else float(round(random.uniform(100.0, 200.0), 2))
        payload.pop("INT_VALUE", None)
    elif name == "soc":
        payload["INT_VALUE"] = random.randint(10, 100)
        payload.pop("FLOAT_VALUE", None)
    elif name == "speed":
        payload["INT_VALUE"] = random.randint(20, 120)
        payload.pop("FLOAT_VALUE", None)
    elif name == "ignition_status":
        payload["INT_VALUE"] = random.choice([0, 1])
        payload.pop("FLOAT_VALUE", None)
    clean = {k: v for k, v in payload.items() if v is not None}
    return clean

def background_producer_corrected(stop_event: threading.Event,
                                  num_devices: int = PRODUCER_NUM_DEVICES,
                                  rate: float = PRODUCER_RATE,
                                  miss_rate: float = PRODUCER_MISS_RATE,
                                  bootstrap: str = PRODUCER_BOOTSTRAP,
                                  topic: str = PRODUCER_TOPIC) -> None:
    devices = [DeviceState(did=str(1000 + i), odo_start=100.0 + i * 5.0) for i in range(num_devices)]
    p = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=5
    )
    tick_interval = 1.0 / max(0.001, rate)
    try:
        while not stop_event.is_set():
            now_ms = int(time.time() * 1000)
            for dev in devices:
                odo_val = dev.bump_odometer()
                metrics = ["odometer", "soc", "speed", "ignition_status"]
                for name in metrics:
                    if random.random() < miss_rate:
                        continue
                    msg = mk_metric_message_uppercase(dev.did, name, now_ms, odo_value=odo_val)
                    try:
                        p.send(topic, key=dev.did, value=msg)
                    except Exception as e:
                        log("producer send error:", e)
            try:
                p.flush(timeout=2)
            except Exception:
                pass
            waited = 0.0
            while waited < tick_interval:
                if stop_event.is_set():
                    break
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
