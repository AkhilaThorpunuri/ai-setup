import csv
import io
import json
import os
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import boto3
import psycopg2
from confluent_kafka.admin import AdminClient
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB = os.environ.get("POSTGRES_DB", "ai_setup")
PG_USER = os.environ.get("POSTGRES_USER", "ai_admin")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "ai_admin_pw")

MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://ai_admin:ai_admin_pw@mongodb:27017/ai_setup?authSource=admin",
)
MONGO_DB = os.environ.get("MONGO_DB", "ai_setup")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "ai_admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "ai_admin_pw")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "ai-source-data")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_USER = os.environ.get("KAFKA_USER", "ai_admin")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "ai_admin_pw")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "tinitiate-data-ingest")

STATE_DIR = Path(os.environ.get("JOB_STATE_DIR", "/state"))

FIRST_NAMES = ["Aarav", "Priya", "Rohan", "Ananya", "Vikram", "Neha", "Karthik", "Sneha"]
LAST_NAMES = ["Sharma", "Patel", "Reddy", "Nair", "Iyer", "Gupta", "Mehta", "Verma"]
CITIES = ["Bengaluru", "Hyderabad", "Mumbai", "Chennai", "Pune", "Singapore", "Austin"]
WAREHOUSES = ["WH-BLR-01", "WH-HYD-01", "WH-MUM-01", "WH-MAA-01", "WH-DEL-01", "WH-SGP-01"]
ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "netbanking", "wallet", "cod"]
PAYMENT_STATUSES = ["pending", "completed", "failed", "refunded"]


def kafka_common_config() -> Dict[str, Any]:
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": KAFKA_USER,
        "sasl.password": KAFKA_PASSWORD,
    }


def save_job_state(job_id: str, data: Dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_DIR / f"{job_id}.json.tmp"
    dest = STATE_DIR / f"{job_id}.json"
    tmp.write_text(json.dumps(data, default=str), encoding="utf-8")
    tmp.replace(dest)


def load_job_state(job_id: str) -> Dict[str, Any] | None:
    path = STATE_DIR / f"{job_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def list_recent_jobs(limit: int = 20) -> List[Dict[str, Any]]:
    if not STATE_DIR.exists():
        return []
    files = sorted(
        STATE_DIR.glob("*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )[:limit]
    jobs = []
    for f in files:
        try:
            jobs.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            pass
    return jobs


def pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )


def mongo_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)
    return client[MONGO_DB]


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def insert_postgres(target: str, rows: int) -> List[Dict[str, Any]]:
    allowed = {"categories", "customers", "products", "orders", "order_items", "payments", "inventory"}
    if target not in allowed:
        raise ValueError(f"Unsupported PostgreSQL table '{target}'. Allowed: {sorted(allowed)}")

    now = datetime.now(timezone.utc)
    inserted: List[Dict[str, Any]] = []

    with pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            def next_id(table: str, col: str) -> int:
                cur.execute(f'SELECT COALESCE(MAX("{col}"), 0) + 1 AS nid FROM "{table}";')
                return int(cur.fetchone()["nid"])

            def get_ids(table: str, col: str) -> List[int]:
                cur.execute(f'SELECT "{col}" FROM "{table}" ORDER BY "{col}" DESC LIMIT 50;')
                return [int(r[col]) for r in cur.fetchall()]

            if target == "categories":
                nid = next_id("categories", "category_id")
                for i in range(rows):
                    cid = nid + i
                    name = f"Portal Category {cid}"
                    cur.execute(
                        "INSERT INTO categories (category_id, category_name, parent_category_id, is_deleted, created_at, updated_at) "
                        "VALUES (%s, %s, NULL, FALSE, %s, %s);",
                        (cid, name, now, now),
                    )
                    inserted.append({"category_id": cid, "category_name": name})

            elif target == "customers":
                nid = next_id("customers", "customer_id")
                for i in range(rows):
                    cid = nid + i
                    fn = random.choice(FIRST_NAMES)
                    ln = random.choice(LAST_NAMES)
                    email = f"{fn.lower()}.{ln.lower()}.{cid}.{uuid.uuid4().hex[:6]}@example.com"
                    city = random.choice(CITIES)
                    cur.execute(
                        "INSERT INTO customers (customer_id, first_name, last_name, email, phone, address_line, city, state, country, postal_code, is_deleted, created_at, updated_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, %s, %s);",
                        (cid, fn, ln, email, f"+91-98{random.randint(10000000, 99999999)}", f"{cid} Tech Park Rd", city, "KA", "India", "560001", now, now),
                    )
                    inserted.append({"customer_id": cid, "name": f"{fn} {ln}", "email": email, "city": city})

            elif target == "products":
                cat_ids = get_ids("categories", "category_id")
                if not cat_ids:
                    insert_postgres("categories", 1)
                    cat_ids = get_ids("categories", "category_id")
                nid = next_id("products", "product_id")
                for i in range(rows):
                    pid = nid + i
                    sku = f"SKU-WEB-{pid}-{uuid.uuid4().hex[:5].upper()}"
                    pname = f"Portal Product {pid}"
                    price = round(random.uniform(199.0, 4999.0), 2)
                    cid = random.choice(cat_ids)
                    cur.execute(
                        "INSERT INTO products (product_id, sku, product_name, category_id, price, description, is_deleted, created_at, updated_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, FALSE, %s, %s);",
                        (pid, sku, pname, cid, price, "Queued via Kafka & Website Data Control", now, now),
                    )
                    inserted.append({"product_id": pid, "sku": sku, "product_name": pname, "price": price})

            elif target == "orders":
                cust_ids = get_ids("customers", "customer_id")
                if not cust_ids:
                    insert_postgres("customers", 1)
                    cust_ids = get_ids("customers", "customer_id")
                nid = next_id("orders", "order_id")
                for i in range(rows):
                    oid = nid + i
                    cid = random.choice(cust_ids)
                    status = random.choice(ORDER_STATUSES)
                    amt = round(random.uniform(500.0, 25000.0), 2)
                    cur.execute(
                        "INSERT INTO orders (order_id, customer_id, order_date, status, total_amount, created_at, updated_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s);",
                        (oid, cid, now, status, amt, now, now),
                    )
                    inserted.append({"order_id": oid, "customer_id": cid, "status": status, "total_amount": amt})

            elif target == "order_items":
                order_ids = get_ids("orders", "order_id")
                if not order_ids:
                    insert_postgres("orders", 1)
                    order_ids = get_ids("orders", "order_id")
                prod_ids = get_ids("products", "product_id")
                if not prod_ids:
                    insert_postgres("products", 1)
                    prod_ids = get_ids("products", "product_id")
                nid = next_id("order_items", "order_item_id")
                for i in range(rows):
                    oiid = nid + i
                    oid = random.choice(order_ids)
                    pid = random.choice(prod_ids)
                    qty = random.randint(1, 5)
                    uprice = round(random.uniform(150.0, 2500.0), 2)
                    cur.execute(
                        "INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, created_at, updated_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s);",
                        (oiid, oid, pid, qty, uprice, now, now),
                    )
                    inserted.append({"order_item_id": oiid, "order_id": oid, "product_id": pid, "quantity": qty})

            elif target == "payments":
                order_ids = get_ids("orders", "order_id")
                if not order_ids:
                    insert_postgres("orders", 1)
                    order_ids = get_ids("orders", "order_id")
                nid = next_id("payments", "payment_id")
                for i in range(rows):
                    pay_id = nid + i
                    oid = random.choice(order_ids)
                    amt = round(random.uniform(500.0, 25000.0), 2)
                    method = random.choice(PAYMENT_METHODS)
                    status = random.choice(PAYMENT_STATUSES)
                    cur.execute(
                        "INSERT INTO payments (payment_id, order_id, payment_date, amount, payment_method, status, created_at, updated_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",
                        (pay_id, oid, now, amt, method, status, now, now),
                    )
                    inserted.append({"payment_id": pay_id, "order_id": oid, "amount": amt, "payment_method": method, "status": status})

            elif target == "inventory":
                prod_ids = get_ids("products", "product_id")
                if not prod_ids:
                    insert_postgres("products", 1)
                    prod_ids = get_ids("products", "product_id")
                nid = next_id("inventory", "inventory_id")
                for i in range(rows):
                    iid = nid + i
                    pid = random.choice(prod_ids)
                    wh = f"{random.choice(WAREHOUSES)}-{uuid.uuid4().hex[:4].upper()}"
                    qty = random.randint(10, 500)
                    cur.execute(
                        "INSERT INTO inventory (inventory_id, product_id, warehouse_location, quantity_on_hand, updated_at) "
                        "VALUES (%s, %s, %s, %s, %s);",
                        (iid, pid, wh, qty, now),
                    )
                    inserted.append({"inventory_id": iid, "product_id": pid, "warehouse_location": wh, "quantity_on_hand": qty})

        conn.commit()
    return inserted


def insert_mongodb(target: str, rows: int) -> List[Dict[str, Any]]:
    db = mongo_db()
    coll = db[target]
    now = datetime.now(timezone.utc).isoformat()
    docs = []
    for i in range(rows):
        uid = uuid.uuid4().hex[:8]
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        city = random.choice(CITIES)
        if target == "organizations_100":
            doc = {
                "Index": f"WEB-{uid}",
                "Name": f"{ln} {random.choice(['Logistics', 'Analytics', 'Systems', 'Holdings'])} {i + 1}",
                "City": city,
                "Industry": random.choice(["Technology", "Logistics", "Healthcare", "Banking"]),
                "Employees": random.randint(25, 2500),
                "source": "kafka_ingest_worker",
                "created_at": now,
            }
        elif target == "products_100":
            doc = {
                "Index": f"WEB-{uid}",
                "Name": f"Portal Item {uid}",
                "Category": random.choice(["Electronics", "Office", "Logistics", "Medical"]),
                "Price": round(random.uniform(10.0, 999.0), 2),
                "Currency": "USD",
                "source": "kafka_ingest_worker",
                "created_at": now,
            }
        elif target == "bank_statements":
            doc = {
                "filename": f"portal_statement_{uid}.pdf",
                "source_path": f"file-formats/pdf/portal_statement_{uid}.pdf",
                "page_count": 1,
                "pages": [{"page_number": 1, "text": f"Sample statement generated for {fn} {ln} in {city}."}],
                "full_text": f"Sample statement generated for {fn} {ln} in {city}.",
                "source": "kafka_ingest_worker",
                "created_at": now,
            }
        else:
            doc = {
                "Index": f"WEB-{uid}",
                "First Name": fn,
                "Last Name": ln,
                "City": city,
                "Email": f"{fn.lower()}.{ln.lower()}.{uid}@example.com",
                "source": "kafka_ingest_worker",
                "created_at": now,
            }
        docs.append(doc)

    res = coll.insert_many(docs)
    for d, oid in zip(docs, res.inserted_ids):
        d["_id"] = str(oid)
    return docs


def insert_minio(target: str, rows: int) -> Dict[str, Any]:
    prefix = target.strip("/") or "custom-uploads"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{prefix}/portal_upload_{ts}_{uuid.uuid4().hex[:6]}.csv"

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["record_id", "customer_name", "city", "category", "amount", "created_at"])
    sample = []
    now = datetime.now(timezone.utc).isoformat()
    for _ in range(rows):
        rec = [
            f"MINIO-{uuid.uuid4().hex[:8]}",
            f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            random.choice(CITIES),
            random.choice(["Logistics", "Banking", "Healthcare", "Retail"]),
            round(random.uniform(100.0, 5000.0), 2),
            now,
        ]
        writer.writerow(rec)
        if len(sample) < 3:
            sample.append(dict(zip(["record_id", "customer_name", "city", "category", "amount", "created_at"], rec)))

    s3 = s3_client()
    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    return {"object_key": f"s3://{MINIO_BUCKET}/{key}", "rows_written": rows, "sample": sample}


def build_report() -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "postgres": {"status": "error", "database": PG_DB, "total_rows": 0, "tables": []},
        "mongodb": {"status": "error", "database": MONGO_DB, "total_documents": 0, "collections": []},
        "minio": {"status": "error", "bucket": MINIO_BUCKET, "total_objects": 0, "total_size_kb": 0, "prefixes": []},
        "kafka": {"status": "error", "topic": KAFKA_TOPIC, "broker": KAFKA_BOOTSTRAP, "jobs": []},
    }

    try:
        with pg_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;")
                tables = [r["tablename"] for r in cur.fetchall()]
                table_rows = []
                total = 0
                for tbl in tables:
                    cur.execute(f'SELECT COUNT(*) AS cnt FROM "{tbl}";')
                    cnt = int(cur.fetchone()["cnt"])
                    total += cnt
                    data_type = "ETL Control Metadata (JSONB)" if tbl == "etl_batch_control" else "Structured RDBMS Table"
                    table_rows.append({"name": tbl, "rows": cnt, "data_type": data_type})
                report["postgres"] = {"status": "connected", "database": PG_DB, "total_rows": total, "tables": table_rows}
    except Exception as exc:
        report["postgres"]["error"] = str(exc)

    try:
        db = mongo_db()
        colls = sorted(db.list_collection_names())
        coll_rows = []
        total_docs = 0
        for name in colls:
            cnt = int(db[name].count_documents({}))
            total_docs += cnt
            dtype = "Parsed PDF Documents (JSON)" if "statement" in name or "pdf" in name else "NoSQL JSON Documents"
            coll_rows.append({"name": name, "documents": cnt, "data_type": dtype})
        report["mongodb"] = {"status": "connected", "database": MONGO_DB, "total_documents": total_docs, "collections": coll_rows}
    except Exception as exc:
        report["mongodb"]["error"] = str(exc)

    try:
        s3 = s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        prefix_map: Dict[str, Dict[str, Any]] = {}
        total_objects = 0
        total_size = 0
        for page in paginator.paginate(Bucket=MINIO_BUCKET):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                size = int(obj.get("Size", 0))
                total_objects += 1
                total_size += size
                top = key.split("/")[0] if "/" in key else "(root)"
                ext = os.path.splitext(key)[1].lower().lstrip(".") or "other"
                entry = prefix_map.setdefault(top, {"prefix": top, "objects": 0, "size_bytes": 0, "extensions": set()})
                entry["objects"] += 1
                entry["size_bytes"] += size
                entry["extensions"].add(ext.upper())

        prefixes = [
            {
                "prefix": v["prefix"],
                "objects": v["objects"],
                "size_kb": round(v["size_bytes"] / 1024, 1),
                "data_type": ", ".join(sorted(v["extensions"])),
            }
            for v in sorted(prefix_map.values(), key=lambda x: x["prefix"])
        ]
        report["minio"] = {
            "status": "connected",
            "bucket": MINIO_BUCKET,
            "total_objects": total_objects,
            "total_size_kb": round(total_size / 1024, 1),
            "prefixes": prefixes,
        }
    except Exception as exc:
        report["minio"]["error"] = str(exc)

    try:
        admin = AdminClient(kafka_common_config())
        meta = admin.list_topics(timeout=3.0)
        topic_exists = KAFKA_TOPIC in meta.topics
        jobs = list_recent_jobs(20)
        report["kafka"] = {
            "status": "connected",
            "broker": KAFKA_BOOTSTRAP,
            "topic": KAFKA_TOPIC,
            "topic_ready": topic_exists,
            "recent_job_count": len(jobs),
            "jobs": jobs,
        }
    except Exception as exc:
        report["kafka"]["error"] = str(exc)
        report["kafka"]["jobs"] = list_recent_jobs(20)

    return report

