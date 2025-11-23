import time
import json
import random
import threading
from kafka import KafkaProducer
from pipeline.config import (
    PRODUCER_BOOTSTRAP, PRODUCER_TOPIC, 
    PRODUCER_NUM_DEVICES, PRODUCER_RATE
)

class DeviceState:
    def __init__(self, did):
        self.did = str(did)
        self.odo = 1000.0
        self.speed = 0.0
        self.soc = 100.0
        self.ignition = 1 # Start with ignition ON
        self.last_ts = 0
    
    def tick(self):
        """
        Simulate one time-step of vehicle physics.
        """
        # 1. Handle Time (Ensure at least 1ms diff from last event)
        now = int(time.time() * 1000)
        if now <= self.last_ts:
            now = self.last_ts + 1
        self.last_ts = now

        # 2. Handle Ignition (95% chance to stay in current state)
        if random.random() > 0.99: 
            self.ignition = 1 - self.ignition # Toggle 0/1

        # 3. Handle Physics based on Ignition
        if self.ignition == 1:
            # Speed: Accelerate or decelerate gradually (-5 to +5 km/h change)
            delta_speed = random.uniform(-5, 5)
            self.speed = max(0.0, min(180.0, self.speed + delta_speed))
            
            # Odometer: Increase based on speed (approximation for 1 tick)
            # Assuming tick is roughly representation of 1 second of travel for simulation speed
            dist_km = (self.speed / 3600.0) * 1.0 
            self.odo += dist_km
            
            # SoC: Drain battery (0.01% to 0.1% per tick)
            self.soc -= random.uniform(0.01, 0.1)
            if self.soc <= 0:
                self.soc = 100.0 # Simualte full recharge
        else:
            self.speed = 0.0
            # SoC stays constant or drops very slowly (phantom drain)
            self.soc -= 0.001

        return now

def _get_producer():
    return KafkaProducer(
        bootstrap_servers=PRODUCER_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8'),
        linger_ms=10 
    )

def _generate_payload(device, timestamp, metric_type):
    # quick helper for ISO time
    def iso(ts): 
        from datetime import datetime, timezone
        return datetime.fromtimestamp(ts/1000, tz=timezone.utc).isoformat()

    base = {
        "did": device.did,
        "timestamp": timestamp,
        "eventTime": iso(timestamp),
        "receivedTime": iso(int(time.time() * 1000))
    }

    if metric_type == "odometer":
        base.update({"name": "odometer", "value_type": "0", "float_value": round(device.odo, 2)})
    elif metric_type == "soc":
        base.update({"name": "soc", "value_type": "1", "int_value": str(int(device.soc))})
    elif metric_type == "speed":
        base.update({"name": "speed", "value_type": "1", "int_value": str(int(device.speed))})
    elif metric_type == "ignition_status":
        base.update({"name": "ignition_status", "value_type": "1", "int_value": str(device.ignition)})
        
    return base

def worker(stop_event, num_devices, rate):
    producer = _get_producer()
    
    # Initialize state for all devices
    devices = [DeviceState(f"100{i}") for i in range(num_devices)]
    
    interval = 1.0 / rate
    print(f"[producer] Starting loop. Target rate: {rate} msg/s")
    
    while not stop_event.is_set():
        start_time = time.time()
        
        for dev in devices:
            # Update physical state and get a strictly unique timestamp
            ts = dev.tick()
            
            # Pick a random metric to emit for this tick
            # weighted to prefer odometer/speed
            mtype = random.choices(
                ["odometer", "speed", "soc", "ignition_status"],
                weights=[0.4, 0.4, 0.1, 0.1],
                k=1
            )[0]

            msg = _generate_payload(dev, ts, mtype)
            
            try:
                producer.send(PRODUCER_TOPIC, key=dev.did, value=msg)
            except Exception as e:
                print(f"[producer] Error sending: {e}")

        # sleep remainder of interval to match rate
        elapsed = time.time() - start_time
        if elapsed < interval:
            time.sleep(interval - elapsed)
            
    producer.flush()
    producer.close()
    print("[producer] Stopped.")

def start_producer(stop_event):
    t = threading.Thread(
        target=worker, 
        args=(stop_event, PRODUCER_NUM_DEVICES, PRODUCER_RATE),
        daemon=True
    )
    t.start()
    return t
