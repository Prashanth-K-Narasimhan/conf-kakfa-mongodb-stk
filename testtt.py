# Write the updated main_refactored.py with default-once and --continuous flag
code = r'''"""
Refactored entrypoint for telemetry-pipeline.

This file splits the long procedural main() into smaller functions to improve
readability and maintainability while preserving the original behavior.

Default behavior: run producer once (one-shot). Use --continuous to run producer continuously.
Place this file as your project entrypoint (replace existing main.py or use it side-by-side).
"""

import argparse
import threading
import time
import os
import sys
from typing import Optional, Tuple

from pipeline.orchestrator import Orchestrator
from pipeline.kafka_utils import create_kafka_topics, log as klog
from pipeline.mongo_utils import setup_mongo, mongo_stats, log as mlog
from pipeline.ksql_utils import apply_ksql_files, wait_for_table_materialization, ksql_server_url
from pipeline.connectors import register_connectors

# single producer module (both standalone and embedded behavior)
from pipeline.producer import start_producer, background_producer_corrected

from pipeline.sample import ksql_stream_sample
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
    # Default behavior is one-shot. Use --continuous to run continuously.
    p.add_argument("--continuous", action="store_true", help="Run the producer continuously (default: run once)")
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

    essential_topics = [PRODUCER_TOPIC]
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

    if not args.continuous:
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


def apply_ksql_and_sample(args: argparse.Namespace, orchestrator: Orchestrator) -> None:
    if args.no_ksql:
        klog("ksql apply skipped by flag")
        return

    apply_ksql_files(replace=args.replace)

    table_ok = True
    if not args.skip_orch:
        table_ok = orchestrator.wait_for_ksql_table_materialization("VEHICLE_LATEST_STATE", backing_topic_hint="vehicle.latest.state", timeout=40)
    if not table_ok:
        klog("WARNING: vehicle_latest did not materialize within timeout")

    klog("sampling TELEMETRY_RAW stream while producer is active")
    ksql_stream_sample(limit=5, stream_name="TELEMETRY_RAW")

    klog("sampling TELEMETRY_NORMALIZED stream while producer is active")
    ksql_stream_sample(limit=5, stream_name="TELEMETRY_NORMALIZED")


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
    if args.no_connectors:
        klog("connectors registration skipped by flag")
        return

    if args.skip_orch or orchestrator.wait_for_ksql_table_materialization("VEHICLE_LATEST_STATE", backing_topic_hint="vehicle.latest.state", timeout=15):
        register_connectors(replace=args.replace)
        if not args.skip_orch:
            orchestrator.wait_for_connectors(connect_url=CONNECT_URL, connectors=["mongo-sink-telemetry-history", "mongo-sink-vehicle-latest"], timeout=20)
    else:
        klog("Skipping connector registration due to ksql table not ready")


def main():
    args = parse_args()

    klog("pipeline create start")
    create_kafka_topics()

    orchestrator = build_orchestrator()

    if not wait_for_kafka_and_topics(orchestrator, args.skip_orch):
        sys.exit(2)

    setup_mongo_if_requested(args.no_mongo_setup)

    stop_event, producer_thread, producer_stats, producer_stats_lock = start_producer_mode(args, orchestrator)

    apply_ksql_and_sample(args, orchestrator)

    # Stop producer gracefully before connector registration
    stop_producer_and_log(stop_event, producer_thread, producer_stats, producer_stats_lock)

    register_connectors_when_ready(args, orchestrator)

    raw_count, latest_count = mongo_stats()
    mlog(f"mongo telemetry_history_count={raw_count}, vehicle_latest_count={latest_count}")

    klog("pipeline create complete")


if __name__ == "__main__":
    main()
'''
with open('./pipeline/main_refactored.py', 'w', encoding='utf-8') as f:
    f.write(code)
'./pipeline/main_refactored.py'

