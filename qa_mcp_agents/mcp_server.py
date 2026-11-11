"""
MCP (Model Context Protocol) Server for Tinitiate AI Setup (with MLflow Tracing).

Exposes tools to query and inspect:
1. PostgreSQL (`tinitiate-ai-postgres` container on localhost:5432, database `ai_setup`)
2. MinIO Object Storage (`tinitiate-ai-minio` container on localhost:9002, bucket `ai-source-data`)

Usage:
  - As a standalone MCP Server (stdio):
      python qa_mcp_agents/mcp_server.py
  - Imported by LangGraph (`ask_langgraph.py`) or CrewAI (`ask_crewai.py`) agents.
"""

import io
import json
import os
import re
from typing import Any, Callable, Dict, List

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

try:
    from mcp.server.fastmcp import FastMCP
    HAS_FASTMCP = True
except ImportError:
    HAS_FASTMCP = False
    FastMCP = None

try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

try:
    import mlflow
    HAS_MLFLOW = True
except ImportError:
    HAS_MLFLOW = False
    mlflow = None  # type: ignore

load_dotenv()

PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
DEFAULT_MLFLOW_URI = f"sqlite:///{os.path.join(PROJECT_ROOT, 'mlflow.db').replace(os.sep, '/')}"
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_MLFLOW_URI)
MLFLOW_EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT_NAME", "Tinitiate-MCP-PostgreSQL-MinIO-QA")


def configure_mlflow():
    if HAS_MLFLOW:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)


def mlflow_trace(name: str, span_type: str = "TOOL") -> Callable:
    """Wraps a function in an MLflow Trace span when mlflow is installed."""
    def decorator(fn: Callable) -> Callable:
        if HAS_MLFLOW and hasattr(mlflow, "trace"):
            return mlflow.trace(name=name, span_type=span_type)(fn)
        return fn
    return decorator


# Connection Configuration (matches tinitiate-ai-onboarding.yml defaults)
PG_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_USER = os.environ.get("POSTGRES_USER", "ai_admin")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "ai_admin_pw")
PG_DB = os.environ.get("POSTGRES_DB", "ai_setup")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9002")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "ai_admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "ai_admin_pw")
MINIO_BUCKET = os.environ.get("SEED_BUCKET", "ai-source-data")

if HAS_FASTMCP:
    mcp = FastMCP("tinitiate-ai-data-server")
else:
    class _DummyMCP:
        def tool(self):
            def decorator(fn):
                return fn
            return decorator
        def run(self):
            print("Please install `mcp` (`pip install mcp`) to run as a stdio server.")
    mcp = _DummyMCP()


def _get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        dbname=PG_DB,
    )


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


# =====================================================================
# 1. POSTGRESQL MCP TOOLS
# =====================================================================

@mcp.tool()
@mlflow_trace(name="mcp_tool.list_db_tables", span_type="TOOL")
def list_db_tables() -> str:
    """
    List all tables in the PostgreSQL database (`ai_setup`) along with their row counts.
    """
    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name;
                """
            )
            tables = [r[0] for r in cur.fetchall()]
            summary = []
            for tbl in tables:
                cur.execute(f'SELECT COUNT(*) FROM "{tbl}";')
                count = cur.fetchone()[0]
                summary.append({"table_name": tbl, "row_count": count})
            return json.dumps(summary, indent=2)
    finally:
        conn.close()


@mcp.tool()
@mlflow_trace(name="mcp_tool.describe_db_table", span_type="TOOL")
def describe_db_table(table_name: str) -> str:
    """
    Return the column names, data types, and 3 sample rows for a PostgreSQL table.
    """
    if not re.match(r"^[a-zA-Z0-9_]+$", table_name):
        return json.dumps({"error": "Invalid table name"})

    conn = _get_pg_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position;
                """,
                (table_name,),
            )
            columns = [dict(r) for r in cur.fetchall()]
            if not columns:
                return json.dumps({"error": f"Table '{table_name}' not found."})

            cur.execute(f'SELECT * FROM "{table_name}" LIMIT 3;')
            sample_rows = [dict(r) for r in cur.fetchall()]
            return json.dumps(
                {"table_name": table_name, "columns": columns, "sample_rows": sample_rows},
                default=str,
                indent=2,
            )
    finally:
        conn.close()


@mcp.tool()
@mlflow_trace(name="mcp_tool.run_sql_query", span_type="TOOL")
def run_sql_query(sql: str) -> str:
    """
    Execute a read-only SELECT query on the PostgreSQL `ai_setup` database and return results as JSON.
    """
    cleaned = sql.strip()
    if not cleaned.upper().startswith(("SELECT", "WITH")):
        return json.dumps({"error": "Only read-only SELECT / WITH queries are permitted."})

    conn = _get_pg_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(cleaned)
            rows = [dict(r) for r in cur.fetchmany(100)]
            return json.dumps(
                {"sql": cleaned, "row_count": len(rows), "rows": rows},
                default=str,
                indent=2,
            )
    except Exception as exc:
        return json.dumps({"error": str(exc), "sql": cleaned}, indent=2)
    finally:
        conn.close()


# =====================================================================
# 2. MINIO OBJECT STORAGE MCP TOOLS
# =====================================================================

@mcp.tool()
@mlflow_trace(name="mcp_tool.list_minio_files", span_type="TOOL")
def list_minio_files(prefix: str = "") -> str:
    """
    List objects stored in the MinIO `ai-source-data` bucket under the given folder prefix.
    """
    s3 = _get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    files: List[Dict[str, Any]] = []
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            files.append(
                {
                    "key": obj["Key"],
                    "size_bytes": obj["Size"],
                }
            )
    return json.dumps(
        {"bucket": MINIO_BUCKET, "prefix": prefix, "total_objects": len(files), "objects": files[:100]},
        indent=2,
    )


@mcp.tool()
@mlflow_trace(name="mcp_tool.read_minio_document", span_type="TOOL")
def read_minio_document(object_key: str, max_chars: int = 6000) -> str:
    """
    Read and extract text content from an object in the MinIO `ai-source-data` bucket.
    Supports Markdown (.md), YAML (.yaml/.yml), CSV (.csv), JSON (.json), HTML (.html), and PDF (.pdf).
    """
    s3 = _get_s3_client()
    try:
        resp = s3.get_object(Bucket=MINIO_BUCKET, Key=object_key)
        raw_bytes = resp["Body"].read()
    except Exception as exc:
        return json.dumps({"error": f"Could not read '{object_key}' from MinIO: {exc}"})

    lower_key = object_key.lower()
    if lower_key.endswith(".pdf"):
        if not HAS_PYPDF:
            return json.dumps({"error": "pypdf is not installed to parse PDF bytes."})
        reader = PdfReader(io.BytesIO(raw_bytes))
        pages_text = []
        for idx, page in enumerate(reader.pages, start=1):
            pages_text.append(f"--- Page {idx} ---\n{page.extract_text() or ''}")
        text = "\n".join(pages_text)
    else:
        text = raw_bytes.decode("utf-8", errors="replace")

    truncated = text[:max_chars]
    return json.dumps(
        {
            "bucket": MINIO_BUCKET,
            "object_key": object_key,
            "total_length": len(text),
            "content": truncated,
        },
        indent=2,
    )


@mcp.tool()
@mlflow_trace(name="mcp_tool.search_minio_documents", span_type="RETRIEVER")
def search_minio_documents(query: str, prefix: str = "business-website-policy-documents/") -> str:
    """
    Search text documents (.md, .yaml, .csv) in the MinIO `ai-source-data` bucket for keywords in `query`.
    Returns the top matching documents and snippets from MinIO.
    """
    s3 = _get_s3_client()
    stopwords = {
        "what", "is", "the", "for", "in", "of", "and", "to", "how", "does", "do",
        "are", "a", "an", "on", "with", "by", "from", "at", "about", "tell", "me",
        "policy", "company", "document", "documents",
    }
    keywords = [
        w.lower()
        for w in re.findall(r"[a-zA-Z0-9]+", query)
        if w.lower() not in stopwords and len(w) > 1
    ]
    if not keywords:
        keywords = [w.lower() for w in re.findall(r"[a-zA-Z0-9]+", query)]

    paginator = s3.get_paginator("list_objects_v2")
    matches = []

    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith((".md", ".yaml", ".yml", ".csv")):
                continue
            if obj["Size"] > 1_500_000 and "business-data-dumps" not in prefix:
                continue

            body = s3.get_object(Bucket=MINIO_BUCKET, Key=key)["Body"].read().decode("utf-8", errors="replace")
            body_lower = body.lower()

            score = sum(1 for kw in keywords if kw in body_lower) + sum(
                2 for kw in keywords if kw in key.lower()
            )
            if score > 0:
                front_matter = ""
                body_content = body
                fm_match = re.match(r"^---\s*\r?\n(.*?)\r?\n---\s*\r?\n(.*)$", body, flags=re.DOTALL)
                if fm_match:
                    front_matter = fm_match.group(1).strip()
                    body_content = fm_match.group(2).strip()

                sections = [s.strip() for s in re.split(r"\r?\n(?=##\s)", body_content) if s.strip()]
                if not sections:
                    sections = [p.strip() for p in re.split(r"\r?\n\s*\r?\n", body_content) if p.strip()]

                best_sec = ""
                best_sec_score = -1
                for sec in sections:
                    s_score = sum(1 for kw in keywords if kw in sec.lower())
                    if s_score > best_sec_score:
                        best_sec_score = s_score
                        best_sec = sec

                snippet = f"{best_sec[:1200]}" if best_sec else body[:1200]
                if front_matter:
                    title_lines = [
                        line for line in front_matter.splitlines()
                        if line.startswith(("doc_id:", "title:", "category:"))
                    ]
                    if title_lines:
                        snippet = "\n".join(title_lines) + "\n\n" + snippet

                matches.append(
                    {
                        "object_key": key,
                        "score": score,
                        "matching_snippet": snippet,
                    }
                )

    matches.sort(key=lambda x: x["score"], reverse=True)
    return json.dumps(
        {
            "bucket": MINIO_BUCKET,
            "query": query,
            "keywords": keywords,
            "top_matches": matches[:5],
        },
        indent=2,
    )


if __name__ == "__main__":
    print("Starting Tinitiate AI Setup MCP Server (PostgreSQL + MinIO)...")
    mcp.run()
