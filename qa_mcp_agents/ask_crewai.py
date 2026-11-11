"""
CrewAI Multi-Agent Question-Answering Script connected to PostgreSQL & MinIO via MCP Tools (Tracked in MLflow).

Agents in the Crew:
  1. `PostgreSQL Database Analyst` — Uses MCP PostgreSQL tools (`list_db_tables`, `describe_db_table`, `run_sql_query`)
     to answer structured questions from `tinitiate-ai-postgres`.
  2. `MinIO Document & Policy Specialist` — Uses MCP MinIO tools (`list_minio_files`, `search_minio_documents`,
     `read_minio_document`) to answer policy & document questions from `tinitiate-ai-minio` (`ai-source-data` bucket).
"""

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv

from ask_langgraph import _build_sql_for_question, _call_llm_if_available
from mcp_server import (
    HAS_MLFLOW,
    MLFLOW_EXPERIMENT_NAME,
    MLFLOW_TRACKING_URI,
    configure_mlflow,
    describe_db_table,
    list_db_tables,
    list_minio_files,
    mlflow,
    mlflow_trace,
    read_minio_document,
    run_sql_query,
    search_minio_documents,
)

try:
    from crewai import Agent, Crew, Process, Task
    from crewai.tools import tool as crewai_tool
    HAS_CREWAI = True
except ImportError:
    HAS_CREWAI = False

load_dotenv()
configure_mlflow()


# =====================================================================
# 1. DEFINE CREWAI ROLES, GOALS & BACKSTORIES
# =====================================================================

@dataclass
class AgentSpec:
    role: str
    goal: str
    backstory: str
    tools: List[str]


POSTGRES_ANALYST_SPEC = AgentSpec(
    role="PostgreSQL Retail & ETL Data Analyst",
    goal="Inspect PostgreSQL tables and run accurate read-only SQL queries via MCP tools to answer data questions.",
    backstory=(
        "You are an expert SQL data analyst managing the `tinitiate-ai-postgres` database (`ai_setup`). "
        "You use MCP tools (`list_db_tables`, `describe_db_table`, `run_sql_query`) to query categories, "
        "products, customers, orders, order_items, payments, inventory, and etl_batch_control."
    ),
    tools=["list_db_tables", "describe_db_table", "run_sql_query"],
)

MINIO_SPECIALIST_SPEC = AgentSpec(
    role="MinIO Policy & Document Research Specialist",
    goal="Search and read company policy documents, YAML profiles, and PDFs stored in the MinIO `ai-source-data` bucket.",
    backstory=(
        "You are a corporate compliance and document researcher managing the `tinitiate-ai-minio` object storage "
        "container (`ai-source-data` bucket). You use MCP tools (`list_minio_files`, `search_minio_documents`, "
        "`read_minio_document`) to retrieve policies for OceanX Logistics, Atlas Retail Bank, and Amerimed Wellness Institute."
    ),
    tools=["list_minio_files", "search_minio_documents", "read_minio_document"],
)


# =====================================================================
# 2. CREWAI AGENT TASKS (TRACED BY MLFLOW)
# =====================================================================

@mlflow_trace(name="crewai.postgres_analyst_agent", span_type="AGENT")
def _run_postgres_analyst_agent(question: str) -> str:
    tables_str = list_db_tables()
    sql = _build_sql_for_question(question, tables_str)
    sql_res = json.loads(run_sql_query(sql))
    return (
        f"[CrewAI Agent 1: {POSTGRES_ANALYST_SPEC.role}]\n"
        f"  • MCP Tools Used: {', '.join(POSTGRES_ANALYST_SPEC.tools)}\n"
        f"  • SQL Executed: `{sql}`\n"
        f"  • PostgreSQL Output:\n{json.dumps(sql_res.get('rows', []), indent=4)}"
    )


@mlflow_trace(name="crewai.minio_specialist_agent", span_type="AGENT")
def _run_minio_specialist_agent(question: str) -> str:
    search_res = json.loads(search_minio_documents(question))
    matches = search_res.get("top_matches", [])
    if matches:
        best = matches[0]
        _ = json.loads(read_minio_document(best["object_key"], max_chars=2000))
        return (
            f"[CrewAI Agent 2: {MINIO_SPECIALIST_SPEC.role}]\n"
            f"  • MCP Tools Used: {', '.join(MINIO_SPECIALIST_SPEC.tools)}\n"
            f"  • Matched MinIO Object: `s3://{search_res.get('bucket')}/{best['object_key']}`\n"
            f"  • Extracted Policy Content:\n{best['matching_snippet']}"
        )
    files_res = json.loads(list_minio_files("business-website-policy-documents/"))
    return (
        f"[CrewAI Agent 2: {MINIO_SPECIALIST_SPEC.role}]\n"
        f"  • Listed {files_res.get('total_objects')} objects in MinIO bucket `ai-source-data`."
    )


@mlflow_trace(name="CrewAI_MCP_Pipeline", span_type="AGENT")
def _execute_crewai_pipeline(question: str) -> tuple[str, List[str]]:
    has_api_key = bool(
        os.environ.get("OPENAI_API_KEY")
        or os.environ.get("ANTHROPIC_API_KEY")
        or os.environ.get("GEMINI_API_KEY")
    )

    if HAS_CREWAI and has_api_key:
        @crewai_tool("list_db_tables")
        def pg_list_tables() -> str:
            """List all tables and row counts in PostgreSQL."""
            return list_db_tables()

        @crewai_tool("describe_db_table")
        def pg_describe_table(table_name: str) -> str:
            """Describe columns and sample rows of a PostgreSQL table."""
            return describe_db_table(table_name)

        @crewai_tool("run_sql_query")
        def pg_run_sql(sql: str) -> str:
            """Run a read-only SELECT query on PostgreSQL."""
            return run_sql_query(sql)

        @crewai_tool("list_minio_files")
        def s3_list_files(prefix: str = "") -> str:
            """List objects in MinIO bucket `ai-source-data`."""
            return list_minio_files(prefix)

        @crewai_tool("search_minio_documents")
        def s3_search_docs(query: str) -> str:
            """Search policy documents in MinIO bucket `ai-source-data`."""
            return search_minio_documents(query)

        @crewai_tool("read_minio_document")
        def s3_read_doc(object_key: str) -> str:
            """Read a document from MinIO bucket `ai-source-data`."""
            return read_minio_document(object_key)

        db_agent = Agent(
            role=POSTGRES_ANALYST_SPEC.role,
            goal=POSTGRES_ANALYST_SPEC.goal,
            backstory=POSTGRES_ANALYST_SPEC.backstory,
            tools=[pg_list_tables, pg_describe_table, pg_run_sql],
            verbose=True,
        )

        minio_agent = Agent(
            role=MINIO_SPECIALIST_SPEC.role,
            goal=MINIO_SPECIALIST_SPEC.goal,
            backstory=MINIO_SPECIALIST_SPEC.backstory,
            tools=[s3_list_files, s3_search_docs, s3_read_doc],
            verbose=True,
        )

        db_task = Task(
            description=f"If relevant to PostgreSQL tables, answer: {question}",
            expected_output="SQL query executed and exact rows returned from PostgreSQL (or 'Not a DB question').",
            agent=db_agent,
        )

        minio_task = Task(
            description=(
                f"Search MinIO `ai-source-data` documents if relevant and combine with DB findings to answer: {question}"
            ),
            expected_output="Final comprehensive answer citing PostgreSQL tables and/or MinIO object keys.",
            agent=minio_agent,
        )

        crew = Crew(
            agents=[db_agent, minio_agent],
            tasks=[db_task, minio_task],
            process=Process.sequential,
            verbose=True,
        )
        return str(crew.kickoff()), [POSTGRES_ANALYST_SPEC.role, MINIO_SPECIALIST_SPEC.role]

    q_lower = question.lower()
    minio_keywords = {
        "oceanx", "atlas", "amerimed", "policy", "policies", "demurrage", "detention",
        "reefer", "reg e", "reg cc", "dispute", "overdraft", "hipaa", "patient",
        "telehealth", "minio", "bucket", "pdf", "website", "document", "shipping",
        "bank", "hospital", "leave", "refund", "customs", "cargo", "financial assistance",
    }
    db_keywords = {
        "order", "orders", "customer", "customers", "product", "products", "category",
        "categories", "payment", "payments", "inventory", "warehouse", "batch", "etl",
        "sql", "table", "database", "revenue", "price", "postgres", "spending", "top",
    }

    use_minio = any(k in q_lower for k in minio_keywords)
    use_db = any(k in q_lower for k in db_keywords) or not use_minio

    report_lines = []
    agents_invoked = []

    if use_db:
        agents_invoked.append(POSTGRES_ANALYST_SPEC.role)
        report_lines.append(_run_postgres_analyst_agent(question))

    if use_minio:
        agents_invoked.append(MINIO_SPECIALIST_SPEC.role)
        report_lines.append(_run_minio_specialist_agent(question))

    combined = "\n\n".join(report_lines)
    llm_summary = _call_llm_if_available(
        "Synthesize the CrewAI agents' MCP tool outputs into a concise answer.",
        f"Question: {question}\n\n{combined}",
    )
    if llm_summary:
        return f"{combined}\n\n[Crew Final Answer]\n{llm_summary}", agents_invoked
    return combined, agents_invoked


def run_crewai_qa(question: str) -> str:
    start_ts = time.perf_counter()
    if HAS_MLFLOW:
        with mlflow.start_run(run_name=f"CrewAI: {question[:45]}"):
            mlflow.set_tag("framework", "CrewAI")
            mlflow.set_tag("protocol", "MCP")
            mlflow.log_param("framework", "CrewAI")
            mlflow.log_param("question", question)

            answer, agents_invoked = _execute_crewai_pipeline(question)
            elapsed_ms = round((time.perf_counter() - start_ts) * 1000, 2)

            mlflow.log_param("agents_invoked", ", ".join(agents_invoked))
            mlflow.log_metric("execution_time_ms", elapsed_ms)
            mlflow.log_metric("agents_count", len(agents_invoked))
            mlflow.log_text(answer, "final_answer.txt")
            mlflow.log_dict(
                {
                    "question": question,
                    "agents_invoked": agents_invoked,
                    "final_answer": answer,
                },
                "crewai_execution_trace.json",
            )
            return answer

    answer, _ = _execute_crewai_pipeline(question)
    return answer


def main():
    parser = argparse.ArgumentParser(description="Ask questions against PostgreSQL and MinIO using CrewAI + MCP.")
    parser.add_argument("--question", "-q", help="Single question to ask the CrewAI agents.")
    parser.add_argument("--interactive", "-i", action="store_true", help="Start an interactive prompt to type questions.")
    args = parser.parse_args()

    if HAS_MLFLOW:
        print(f"[MLflow Tracking Enabled] URI: {MLFLOW_TRACKING_URI} | Experiment: {MLFLOW_EXPERIMENT_NAME}")

    if args.interactive:
        print("=" * 80)
        print("CrewAI + MCP Interactive Q&A (PostgreSQL + MinIO) [Tracked in MLflow]")
        print("Type your question and press Enter (or type 'exit' to quit).")
        print("=" * 80)
        while True:
            try:
                user_q = input("\nAsk a question > ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nExiting.")
                break
            if not user_q or user_q.lower() in ("exit", "quit", "q"):
                print("Exiting.")
                break
            print("-" * 80)
            answer = run_crewai_qa(user_q)
            print(answer)
            print("=" * 80)
        return

    questions = (
        [args.question]
        if args.question
        else [
            "Who are the top 5 customers by total spending in PostgreSQL?",
            "What is the inventory count across each warehouse location in PostgreSQL?",
            "What is Amerimed Wellness Institute policy on financial assistance and charity care?",
        ]
    )

    for idx, q in enumerate(questions, start=1):
        print("=" * 80)
        print(f"CREWAI TASK {idx}: {q}")
        print("-" * 80)
        answer = run_crewai_qa(q)
        print(answer)
        print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
