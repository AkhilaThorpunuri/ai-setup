import json
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from ingest_service import (
    KAFKA_TOPIC,
    insert_minio,
    insert_mongodb,
    insert_postgres,
    kafka_common_config,
    load_job_state,
    save_job_state,
)


def ensure_topic():
    admin = AdminClient(kafka_common_config())
    for _ in range(15):
        try:
            meta = admin.list_topics(timeout=5.0)
            if KAFKA_TOPIC not in meta.topics:
                fs = admin.create_topics([NewTopic(KAFKA_TOPIC, num_partitions=1, replication_factor=1)])
                for _, f in fs.items():
                    f.result()
                print(f"[worker] Created Kafka topic: {KAFKA_TOPIC}", flush=True)
            return
        except Exception as exc:
            print(f"[worker] Waiting for Kafka ({exc})...", flush=True)
            time.sleep(2)


def process_message(payload: dict):
    job_id = payload["job_id"]
    store = payload["store"]
    target = payload["target"]
    rows = int(payload["rows"])

    state = load_job_state(job_id) or dict(payload)
    state["status"] = "processing"
    state["started_at"] = datetime.now(timezone.utc).isoformat()
    save_job_state(job_id, state)

    try:
        if store == "postgres":
            inserted = insert_postgres(target, rows)
            state["inserted_count"] = len(inserted)
            state["sample"] = inserted[:3]
        elif store == "mongodb":
            inserted = insert_mongodb(target, rows)
            state["inserted_count"] = len(inserted)
            state["sample"] = inserted[:3]
        elif store == "minio":
            res = insert_minio(target, rows)
            state["inserted_count"] = 1
            state["rows_in_csv"] = res["rows_written"]
            state["object_key"] = res["object_key"]
            state["sample"] = res["sample"]
        else:
            raise ValueError(f"Unknown store: {store}")

        state["status"] = "completed"
        state["completed_at"] = datetime.now(timezone.utc).isoformat()
        save_job_state(job_id, state)
        print(f"[worker] Completed job {job_id}: {store} -> {target} ({rows} rows)", flush=True)
    except Exception as exc:
        state["status"] = "failed"
        state["error"] = str(exc)
        state["completed_at"] = datetime.now(timezone.utc).isoformat()
        save_job_state(job_id, state)
        print(f"[worker] Failed job {job_id}: {exc}", flush=True)


def main():
    ensure_topic()
    conf = {
        **kafka_common_config(),
        "group.id": "tinitiate-data-workers",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    print(f"[worker] Subscribed to Kafka topic '{KAFKA_TOPIC}'...", flush=True)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[worker] Kafka error: {msg.error()}", flush=True)
                continue
            payload = json.loads(msg.value().decode("utf-8"))
            process_message(payload)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

