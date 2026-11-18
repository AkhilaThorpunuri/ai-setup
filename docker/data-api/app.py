import json
import uuid
from datetime import datetime, timezone
from typing import Literal

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ingest_service import (
    KAFKA_TOPIC,
    build_report,
    kafka_common_config,
    load_job_state,
    save_job_state,
)

app = FastAPI(title="Tinitiate AI Data Control, Kafka Queue & Reporting API")
_producer: Producer | None = None


def get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer(kafka_common_config())
    return _producer


class PostDataRequest(BaseModel):
    store: Literal["postgres", "mongodb", "minio"]
    target: str = Field(..., min_length=1, max_length=120)
    rows: int = Field(..., ge=1, le=500)


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/report")
def get_report():
    return build_report()


@app.get("/api/jobs/{job_id}")
def get_job_status(job_id: str):
    state = load_job_state(job_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return state


@app.post("/api/post-data")
def post_data(req: PostDataRequest):
    if req.store == "postgres":
        allowed = {"categories", "customers", "products", "orders", "order_items", "payments", "inventory"}
        if req.target not in allowed:
            raise HTTPException(status_code=400, detail=f"Unsupported PostgreSQL table '{req.target}'.")

    job_id = uuid.uuid4().hex[:12]
    payload = {
        "job_id": job_id,
        "store": req.store,
        "target": req.target,
        "rows": req.rows,
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }

    job_record = {
        **payload,
        "status": "queued",
        "kafka_topic": KAFKA_TOPIC,
    }
    save_job_state(job_id, job_record)

    delivery_info = {}

    def _on_delivery(err, msg):
        if err is not None:
            delivery_info["error"] = str(err)
        else:
            delivery_info["partition"] = msg.partition()
            delivery_info["offset"] = msg.offset()

    producer = get_producer()
    producer.produce(
        KAFKA_TOPIC,
        key=job_id.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
        callback=_on_delivery,
    )
    producer.flush(timeout=10.0)

    if "error" in delivery_info:
        job_record["status"] = "failed"
        job_record["error"] = delivery_info["error"]
        save_job_state(job_id, job_record)
        raise HTTPException(status_code=500, detail=f"Kafka publish failed: {delivery_info['error']}")

    job_record["kafka_partition"] = delivery_info.get("partition")
    job_record["kafka_offset"] = delivery_info.get("offset")
    save_job_state(job_id, job_record)
    return job_record
