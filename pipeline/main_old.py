"""
Entrypoint for telemetry-pipeline installed as a console script.
This version orchestrates reliable startup ordering using pipeline.orchestrator.Orchestrator
"""

import argparse
import threading
import time
import os
import sys
from pipeline.orchestrator import Orchestrator
from pipeline.kafka_utils import create_kafka_topics, log as klog
from pipeline.mongo_utils import setup_mongo, mongo_stats, log as mlog
from pipeline.ksql_utils import apply_ksql_files, wait_for_table_materialization, ksql_server_url
from pipeline.connectors import register_connectors

# import single producer module (both standalone and embedded behavior)
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


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--replace", action="store_true", help="Replace existing KSQL objects/connectors")
    p.add_argument("--no-producer", action="store_true", help="Do not start the embedded producer")
    p.add_argument("--no-ksql", action="store_true", help="Do not apply KSQL files")
    p.add_argument("--no-connectors", action="store_true", help="Do not register connectors")
    p.add_argument("--no-mongo-setup", action="store_true", help="Do not run mongo setup")
    p.add_argument("--skip-orch", action="store_true", help="Skip orchestrator checks (fast but brittle)")
    p.add_argument("--producer-once", action="store_true", help="Run the producer in one-shot mode (send single batch) and exit")
    args = p.parse_args()

    # create basic kafka topics always (this is quick, idempotent)
    klog("pipeline create start")
    create_kafka_topics()

    orchestrator = Orchestrator(kafka_bootstrap=PRODUCER_BOOTSTRAP or "kafka:9092",
                                ksql_url=ksql_server_url(),
                                mongo_url=MONGO_URL or "mongodb://mongodb:27017",
                                check_interval=2.0)

    # Orchestration: Skip if user explicitly asked to skip (for dev)
    if not args.skip_orch:
        # 1) Wait for Kafka to be healthy
        if not orchestrator.wait_for_kafka_bootstrap(timeout=45):
            klog("Kafka not ready - aborting")
            sys.exit(2)

        # 2) Ensure our key topics exist (create if missing)
        essential_topics = [PRODUCER_TOPIC]
        orchestrator.ensure_topics_exist(essential_topics, partitions=1, replication_factor=1, create_if_missing=True, timeout=30)

    # 3) Mongo (optional)
    if not args.no_mongo_setup:
        mlog("setting up mongo")
        setup_mongo()
    else:
        mlog("skipping mongo setup as requested")

    # Producer startup: use the single pipeline.producer module
    stop_event = None
    producer_thread = None
    producer_stats = None
    producer_stats_lock = None

    if not args.no_producer:
        if args.producer_once:
            # run a one-shot worker directly and capture stats
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
            one_shot_thread.join(timeout=PRODUCER_RUN_SECONDS + 5)
            with one_shot_lock:
                klog(f"one-shot producer acked messages: {one_shot_stats.get('sent', 0)}")
            # one-shot complete; we don't start a continuous producer
        else:
            # continuous mode using start_producer wrapper
            stop_event, producer_thread, producer_stats, producer_stats_lock = start_producer(
                num_devices=PRODUCER_NUM_DEVICES,
                rate=PRODUCER_RATE,
                miss_rate=PRODUCER_MISS_RATE,
                bootstrap=PRODUCER_BOOTSTRAP,
                topic=PRODUCER_TOPIC,
                daemon=True,
            )
            klog(f"started background producer (devices={PRODUCER_NUM_DEVICES}, rate={PRODUCER_RATE}/s)")
            # wait a bit so producer can emit first messages
            if not args.skip_orch:
                orchestrator.wait_for_topic_messages(PRODUCER_TOPIC, min_messages=1, timeout=20)
            time.sleep(min(3, PRODUCER_RUN_SECONDS))
    else:
        klog("producer disabled via flag")

    # Apply ksql only after producer has started (so EMIT CHANGES streaming queries pick up messages)
    if not args.no_ksql:
        apply_ksql_files(replace=args.replace)
        # Wait for the vehicle_latest materialization (backing topic named in KSQL file)
        table_ok = True
        if not args.skip_orch:
            table_ok = orchestrator.wait_for_ksql_table_materialization("VEHICLE_LATEST_STATE", backing_topic_hint="vehicle.latest.state", timeout=40)
        if not table_ok:
            klog("WARNING: vehicle_latest did not materialize within timeout")
        klog("sampling TELEMETRY_RAW stream while producer is active")
        ksql_stream_sample(limit=5, stream_name="TELEMETRY_RAW")
        klog("sampling TELEMETRY_NORMALIZED stream while producer is active")
        ksql_stream_sample(limit=5, stream_name="TELEMETRY_NORMALIZED")
    else:
        klog("ksql apply skipped by flag")

    # Stop producer gracefully
    if producer_thread and producer_thread.is_alive():
        klog("stopping background producer")
        if stop_event:
            stop_event.set()
        producer_thread.join(timeout=5)

    # Print acked stats if available
    if producer_stats is not None and producer_stats_lock is not None:
        with producer_stats_lock:
            klog(f"producer acked messages: {producer_stats.get('sent', 0)}")
    else:
        klog("producer stats not available (producer may have been skipped or used one-shot)")

    # Register connectors only after ksql table present
    if not args.no_connectors:
        if args.skip_orch or orchestrator.wait_for_ksql_table_materialization("VEHICLE_LATEST_STATE", backing_topic_hint="vehicle.latest.state", timeout=15):
            register_connectors(replace=args.replace)
            # ensure connectors are up
            if not args.skip_orch:
                orchestrator.wait_for_connectors(connect_url=CONNECT_URL, connectors=["mongo-sink-telemetry-history", "mongo-sink-vehicle-latest"], timeout=20)
        else:
            klog("Skipping connector registration due to ksql table not ready")
    else:
        klog("connectors registration skipped by flag")

    raw_count, latest_count = mongo_stats()
    mlog(f"mongo telemetry_history_count={raw_count}, vehicle_latest_count={latest_count}")

    klog("pipeline create complete")


if __name__ == "__main__":
    main()
