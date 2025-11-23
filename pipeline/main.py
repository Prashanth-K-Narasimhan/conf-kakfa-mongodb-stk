import argparse
import signal
import threading
import time
import sys
import os

# internal modules
from pipeline import config
from pipeline.utils import kafka_utils
from pipeline.utils import mongo_utils
from pipeline.utils import ksql_utils
from pipeline.utils import connectors_utils
from pipeline.utils import orchestrator
from pipeline.producer import start_producer

def get_args():
    p = argparse.ArgumentParser()
    p.add_argument("--replace", action="store_true", help="Force recreate KSQL/Connectors (destructive)")
    p.add_argument("--demo-mode", action="store_true", help="Wait for Enter key between stages")
    return p.parse_args()

def pause_if_needed(args, step_name):
    # Useful for demos so the text doesn't fly by too fast
    if args.demo_mode:
        try:
            input(f"\n[STEP: {step_name}] Press Enter to continue...")
        except (KeyboardInterrupt, EOFError):
            print("\nAborted.")
            sys.exit(0)

def main():
    args = get_args()
    
    stop_event = threading.Event()
    producer_thread = None

    def cleanup(signum, frame):
        print("\n Shutting down...")
        stop_event.set()
        # Let the thread finish its current loop
        if producer_thread and producer_thread.is_alive():
            producer_thread.join(timeout=5)
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    print(">>> Starting Telemetry Pipeline")

    # 1. Infra Check
    pause_if_needed(args, "Check Infrastructure")
    print("Creating Topics and Checking Kafka connectivity...")
    
    if not orchestrator.check_kafka(config.KAFKA_BOOTSTRAP):
        print(f"CRITICAL: Kafka not reachable at {config.KAFKA_BOOTSTRAP}")
        sys.exit(1)
    
    # Ensure topics exist before we start dumping data
    kafka_utils.ensure_topics(["telemetry_raw", "telemetry_normalized", "vehicle_latest_state"])

    # 2. Mongo
    pause_if_needed(args, "Setup MongoDB")
    print("Initializing Mongo indexes...")
    mongo_utils.init_db()

    # 3. KSQL
    pause_if_needed(args, "Apply KSQL")
    print("Applying KSQL streams/tables...")
    ksql_utils.run_migrations(replace=args.replace)

    # 4. Producer
    pause_if_needed(args, "Start Producer")
    print(f"Running background producer ({config.PRODUCER_NUM_DEVICES} devices)...")
    producer_thread = start_producer(stop_event)
    
    # Wait for at least one message to hit the topic before moving on..
    orchestrator.wait_for_data(config.PRODUCER_TOPIC)

    # 5. Connectors
    # Last step because they depend on topics + KSQL tables existing
    pause_if_needed(args, "Register Connectors")
    print("Registering Connectors...")
    connectors_utils.register_all(replace=args.replace)

    print(">>> Pipeline is up.")
    
    # Keep main alive so the producer thread doesn't die
    if args.demo_mode:
        print("[INFO] Producer running. Ctrl+C to stop.")
        
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup(None, None)

if __name__ == "__main__":
    main()