"""
Refactored entrypoint for telemetry-pipeline.

This file splits the long procedural main() into smaller functions to improve
readability and maintainability while preserving the original behavior.

Default behavior: run producer once (one-shot). Use --continuous to run producer continuously.
Place this file as your project entrypoint (replace existing main.py or use it side-by-side).
"""

import argparse
import signal
import threading
import time
import os
import sys
from typing import Optional, Tuple

from pipeline.orchestrator import Orchestrator
from pipeline.kafka_utils import create_kafka_topics, log as klog
from pipeline.mongo_utils import setup_mongo, mongo_stats, log as mlog
from pipeline.ksql_utils import apply_ksql_files, ksql_server_url, ksql_table_exists
from pipeline.connectors_utils import register_connectors

# single producer module (both standalone and embedded behavior)
from pipeline.producer import start_producer, background_producer_corrected


from pipeline.config import (
    PRODUCER_NUM_DEVICES,
    PRODUCER_RATE,
    PRODUCER_MISS_RATE,
    PRODUCER_BOOTSTRAP,
    PRODUCER_TOPIC,
    PRODUCER_RUN_SECONDS,
    CONNECT_URL,
    MONGO_URL
)

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Telemetry pipeline orchestrator")
    p.add_argument("--replace", action="store_true", help="Replace existing KSQL objects/connectors")
    p.add_argument("--no-producer", action="store_true", help="Do not start the embedded producer")
    p.add_argument("--no-ksql", action="store_true", help="Do not apply KSQL files")
    p.add_argument("--no-connectors", action="store_true", help="Do not register connectors")
    p.add_argument("--no-mongo-setup", action="store_true", help="Do not run mongo setup")
    p.add_argument("--skip-orch", action="store_true", help="Skip orchestrator checks (fast but brittle)")
    p.add_argument("--producer-once", action="store_true", help="Run the producer in one-shot mode (send single batch) and exit")
    p.add_argument("--step-mode", action="store_true", help="Pause for user input before each step")
    return p.parse_args()


def build_orchestrator() -> Orchestrator:
    return Orchestrator(
        kafka_bootstrap=PRODUCER_BOOTSTRAP or "kafka:9092",
        ksql_url=ksql_server_url(),
        mongo_url=MONGO_URL or "mongodb://mongodb:27017",
        check_interval=2.0,
    )


def wait_for_kafka_and_topics(orchestrator: Orchestrator, skip_orch: bool) -> bool:
    """Ensure Kafka is up and required topics exist. Returns True if OK, False if failing critical checks."""
    if skip_orch:
        klog("Skipping orchestrator Kafka/topic checks (dev mode)")
        return True

    if not orchestrator.wait_for_kafka_bootstrap(timeout=45):
        klog("Kafka not ready - aborting")
        return False

    essential_topics = ["telemetry_normalized", "telemetry_raw", "vehicle_latest_state"]
    orchestrator.ensure_topics_exist(essential_topics, partitions=1, replication_factor=1, create_if_missing=True, timeout=30)
    return True


def setup_mongo_if_requested(no_mongo_setup: bool) -> None:
    if no_mongo_setup:
        mlog("skipping mongo setup as requested")
    else:
        mlog("setting up mongo")
        setup_mongo()


def start_producer_mode(args: argparse.Namespace, orchestrator: Orchestrator) -> Tuple[Optional[threading.Event], Optional[threading.Thread], Optional[dict], Optional[threading.Lock]]:
    """
    Start the producer either in one-shot (default) or continuous mode.
    Returns (stop_event, thread, stats, stats_lock). For one-shot, stats will contain the one-shot result.
    """
    stop_event = None
    producer_thread = None
    producer_stats = None
    producer_stats_lock = None

    if args.no_producer:
        klog("producer disabled via flag")
        return stop_event, producer_thread, producer_stats, producer_stats_lock

    if args.producer_once:
        # one-shot worker: run background_producer_corrected with once=True and capture stats
        one_shot_stats = {"sent": 0}
        one_shot_lock = threading.Lock()
        one_shot_thread = threading.Thread(
            target=background_producer_corrected,
            args=(threading.Event(), one_shot_stats, one_shot_lock, PRODUCER_NUM_DEVICES, PRODUCER_RATE, PRODUCER_MISS_RATE, PRODUCER_BOOTSTRAP, PRODUCER_TOPIC, True),
            daemon=True,
            name="bg-producer-one-shot",
        )
        one_shot_thread.start()
        klog(f"started one-shot producer (devices={PRODUCER_NUM_DEVICES}, rate={PRODUCER_RATE}/s)")

        if not args.skip_orch:
            orchestrator.wait_for_topic_messages(PRODUCER_TOPIC, min_messages=1, timeout=20)

        # Wait for the one-shot to finish (bounded by run seconds)
        one_shot_thread.join(timeout=PRODUCER_RUN_SECONDS + 5)
        with one_shot_lock:
            klog(f"one-shot producer acked messages: {one_shot_stats.get('sent', 0)}")

        # no continuous producer started; return one-shot handles for visibility
        return stop_event, one_shot_thread, one_shot_stats, one_shot_lock

    # Continuous mode using start_producer wrapper
    stop_event, producer_thread, producer_stats, producer_stats_lock = start_producer(
        num_devices=PRODUCER_NUM_DEVICES,
        rate=PRODUCER_RATE,
        miss_rate=PRODUCER_MISS_RATE,
        bootstrap=PRODUCER_BOOTSTRAP,
        topic=PRODUCER_TOPIC,
        daemon=True,
    )
    klog(f"started background producer (devices={PRODUCER_NUM_DEVICES}, rate={PRODUCER_RATE}/s)")

    if not args.skip_orch:
        orchestrator.wait_for_topic_messages(PRODUCER_TOPIC, min_messages=1, timeout=20)

    # small warmup window
    time.sleep(min(3, PRODUCER_RUN_SECONDS))
    return stop_event, producer_thread, producer_stats, producer_stats_lock


def apply_ksql(args: argparse.Namespace, orchestrator: Orchestrator) -> None:
    if args.no_ksql:
        klog("ksql apply skipped by flag")
        return

    apply_ksql_files(replace=args.replace)
    
    klog("ksql stream / table creation done!")

        
        
def sample_ksql(args: argparse.Namespace, orchestrator: Orchestrator) -> None:
    if ksql_table_exists("VEHICLE_LATEST_STATE"):
        table_ok = True
        if not args.skip_orch:
            table_ok = orchestrator.wait_for_ksql_table_materialization("VEHICLE_LATEST_STATE", backing_topic_hint="vehicle_latest_state ", timeout=40)
        if not table_ok:
            klog("WARNING: vehicle_latest did not materialize within timeout")


def stop_producer_and_log(stop_event: Optional[threading.Event], producer_thread: Optional[threading.Thread], producer_stats: Optional[dict], producer_stats_lock: Optional[threading.Lock]) -> None:
    if producer_thread and producer_thread.is_alive():
        klog("stopping background producer")
        if stop_event:
            stop_event.set()
        producer_thread.join(timeout=5)

    if producer_stats is not None and producer_stats_lock is not None:
        with producer_stats_lock:
            klog(f"producer acked messages: {producer_stats.get('sent', 0)}")
    else:
        klog("producer stats not available (producer may have been skipped or used one-shot)")


def register_connectors_when_ready(args: argparse.Namespace, orchestrator: Orchestrator) -> None:
    # case 1: user disabled connectors entirely
    if args.no_connectors:
        klog("connectors registration skipped by flag")
        return

    klog("registering connectors..")
    register_connectors(replace=args.replace)
    return



def step_pause(args: argparse.Namespace, step_name: str = None) -> None:
    """
    Pause for user input only if step-mode is enabled.
    step_name is used to make the prompt clearer.
    """
    if getattr(args, "step_mode", False):
        prompt = f"[{step_name}] Press Enter to continue..." if step_name else "Press Enter to continue..."
        try:
            input(prompt)
        except (KeyboardInterrupt, EOFError):
            # User aborted — exit with non-zero
            print("\nAborted by user.")
            sys.exit(130)
    

def main():
    args = parse_args()
    
    # Track producer handles (None until created)
    stop_event = None
    producer_thread = None
    producer_stats = None
    producer_stats_lock = None
    
    def handle_interrupt(signum, frame):
        print("\n Interrupt received! Shutting down gracefully...")

        # Stop producer thread if it was started
        if stop_event and producer_thread:
            try:
                stop_producer_and_log(stop_event, producer_thread, producer_stats, producer_stats_lock)
            except Exception as e:
                print(f"Error stopping producer: {e}")

        # Anything else you want to clean up:
        # e.g., close orchestrator, cleanup mongo, kill connectors etc.

        sys.exit(130)  # 130 = standard exit code for SIGINT

    # Register interrupt handler
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt) 

    try:
        # ---- original flow (kept logs and order) ----
        klog("pipeline starting...")

        step_pause(args, "create_kafka_topics ?")
        create_kafka_topics()

        orchestrator = build_orchestrator()

        if not wait_for_kafka_and_topics(orchestrator, args.skip_orch):
            # Keep the original behavior: exit if waiting failed
            sys.exit(2)

        step_pause(args, "setup monogoDB collections?")
        setup_mongo_if_requested(args.no_mongo_setup)


        step_pause(args, "setup ksql streams and tables ?")
        apply_ksql(args, orchestrator)


        step_pause(args, "Start running producer ?")
        stop_event, producer_thread, producer_stats, producer_stats_lock = start_producer_mode(args, orchestrator)


        step_pause(args, "register mongoDB - Kafka connectors ?")
        register_connectors_when_ready(args, orchestrator)       
        

        # Stop producer gracefully before connector registration
        step_pause(args, "stop running producer ? (will be stopping the producer on interrupt also.. ) ")
        stop_producer_and_log(stop_event, producer_thread, producer_stats, producer_stats_lock)
        
        
        klog("pipeline create complete!!")
        
    except KeyboardInterrupt:
        # Fallback in case signal handler didn't catch
        handle_interrupt(None, None)

    except Exception as e:
        print(f"Unexpected error: {e}")
        handle_interrupt(None, None)


if __name__ == "__main__":
    main()
