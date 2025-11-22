"""
Entrypoint for telemetry-pipeline installed as a console script.

"""

import argparse
import threading
import time

from pipeline.kafka_utils import create_kafka_topics, log as klog
from pipeline.mongo_utils import setup_mongo, mongo_stats, log as mlog
from pipeline.ksql_utils import apply_ksql_files, wait_for_table_materialization
from pipeline.connectors import register_connectors
from pipeline.producer import background_producer_corrected
from pipeline.sample import ksql_stream_sample
from pipeline.config import (
    PRODUCER_NUM_DEVICES,
    PRODUCER_RATE,
    PRODUCER_MISS_RATE,
    PRODUCER_BOOTSTRAP,
    PRODUCER_TOPIC,
    PRODUCER_RUN_SECONDS,
)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--replace", action="store_true", help="Replace existing KSQL objects/connectors")
    p.add_argument("--no-producer", action="store_true", help="Do not start the embedded producer")
    p.add_argument("--no-ksql", action="store_true", help="Do not apply KSQL files")
    p.add_argument("--no-connectors", action="store_true", help="Do not register connectors")
    p.add_argument("--no-mongo-setup", action="store_true", help="Do not run mongo setup")
    args = p.parse_args()

    klog("pipeline create start")
    create_kafka_topics()

    if not args.no_mongo_setup:
        mlog("setting up mongo")
        setup_mongo()
    else:
        mlog("skipping mongo setup as requested")

    stop_event = threading.Event()
    producer_thread = None
    if not args.no_producer:
        producer_thread = threading.Thread(
            target=background_producer_corrected,
            args=(stop_event, PRODUCER_NUM_DEVICES, PRODUCER_RATE, PRODUCER_MISS_RATE, PRODUCER_BOOTSTRAP, PRODUCER_TOPIC),
            daemon=True,
            name="bg-producer"
        )
        producer_thread.start()
        klog(f"started background producer (devices={PRODUCER_NUM_DEVICES}, rate={PRODUCER_RATE}/s)")
        time.sleep(min(3, PRODUCER_RUN_SECONDS))
    else:
        klog("producer disabled via flag")

    if not args.no_ksql:
        apply_ksql_files(replace=args.replace)
        materialized = wait_for_table_materialization("vehicle_latest")
        if not materialized:
            klog("WARNING: vehicle_latest did not materialize within timeout")
        klog("sampling TELEMETRY_RAW stream while producer is active")
        ksql_stream_sample(limit=5, stream_name="TELEMETRY_RAW")
    else:
        klog("ksql apply skipped by flag")

    if producer_thread and producer_thread.is_alive():
        klog("stopping background producer")
        stop_event.set()
        producer_thread.join(timeout=5)

    if not args.no_connectors:
        register_connectors(replace=args.replace)
    else:
        klog("connectors registration skipped by flag")

    raw_count, latest_count = mongo_stats()
    mlog(f"mongo telemetry_raw_count={raw_count}, vehicle_latest_count={latest_count}")

    klog("pipeline create complete")


if __name__ == "__main__":
    main()
