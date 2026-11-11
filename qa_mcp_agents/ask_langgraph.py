"""
LangGraph Question-Answering Agent connected to PostgreSQL & MinIO via MCP Tools (Tracked in MLflow).

Graph Workflow:
  START -> router_node -> (postgres_mcp_node | minio_mcp_node | both) -> synthesizer_node -> END

Every execution logs:
  - An MLflow Run (params, metrics, tags, and answer artifacts)
  - A full nested MLflow Trace (`router_node`, `postgres_mcp_node`, `minio_mcp_node`, MCP tool calls, `synthesizer_node`)
"""

import argparse
import json
import os
import re
import time
from typing import Any, Dict, List, Literal, TypedDict

from dotenv import load_dotenv

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
    from langgraph.graph import END, START, StateGraph
    HAS_LANGGRAPH = True
except ImportError:
    HAS_LANGGRAPH = False

load_dotenv()
configure_mlflow()


# =====================================================================
# 1. DEFINE LANGGRAPH STATE
# =====================================================================

class QAState(TypedDict):
    question: str
    route: Literal["postgres", "minio", "both"]
    mcp_tool_calls: List[str]
    db_context: str
    minio_context: str
    final_answer: str


# =====================================================================
# 2. HELPER: OPTIONAL LLM CALL (OpenAI / Anthropic / Gemini) + SMART SQL BUILDER
# =====================================================================

def _call_llm_if_available(system_prompt: str, user_prompt: str) -> str | None:
    """Uses OpenAI, Anthropic, or Google Gemini if an API key is present in .env / environment."""
    openai_key = os.environ.get("OPENAI_API_KEY")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    gemini_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")

    if openai_key:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=openai_key)
            resp = client.chat.completions.create(
                model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0,
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            pass

    if anthropic_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=anthropic_key)
            resp = client.messages.create(
                model=os.environ.get("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest"),
                max_tokens=1024,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            return resp.content[0].text.strip()
        except Exception:
            pass

    if gemini_key:
        try:
            from google import genai
            client = genai.Client(api_key=gemini_key)
            resp = client.models.generate_content(
                model=os.environ.get("GEMINI_MODEL", "gemini-2.5-flash"),
                contents=f"{system_prompt}\n\n{user_prompt}",
            )
            return resp.text.strip()
        except Exception:
            pass

    return None


def _build_sql_for_question(question: str, schema_info: str) -> str:
    """Translates natural language question into a PostgreSQL SELECT query (via LLM or schema-aware rules)."""
    llm_sql = _call_llm_if_available(
        system_prompt=(
            "You are a PostgreSQL expert. Given the schema below, return ONLY a valid read-only "
            "PostgreSQL SELECT query (no markdown fences, no commentary).\n\nSchema:\n" + schema_info
        ),
        user_prompt=question,
    )
    if llm_sql:
        cleaned = re.sub(r"^```(?:sql)?\s*|\s*```$", "", llm_sql.strip(), flags=re.IGNORECASE)
        if cleaned.upper().startswith(("SELECT", "WITH")):
            return cleaned

    q = question.lower()
    if "batch" in q or "etl" in q:
        return "SELECT batch_name, batch_type, row_counts, loaded_at FROM etl_batch_control ORDER BY batch_name;"
    if "deliver" in q and "order" in q:
        return (
            "SELECT status, COUNT(*) AS order_count, ROUND(SUM(total_amount), 2) AS total_revenue "
            "FROM orders WHERE status = 'delivered' GROUP BY status;"
        )
    if "status" in q and "order" in q:
        return (
            "SELECT status, COUNT(*) AS order_count, ROUND(SUM(total_amount), 2) AS total_amount "
            "FROM orders GROUP BY status ORDER BY order_count DESC;"
        )
    if "payment" in q:
        return (
            "SELECT payment_method, status, COUNT(*) AS count, ROUND(SUM(amount), 2) AS total_amount "
            "FROM payments GROUP BY payment_method, status ORDER BY total_amount DESC LIMIT 10;"
        )
    if "categor" in q:
        return (
            "SELECT c.category_name, COUNT(p.product_id) AS product_count, ROUND(AVG(p.price), 2) AS avg_price "
            "FROM categories c LEFT JOIN products p ON c.category_id = p.category_id AND NOT p.is_deleted "
            "GROUP BY c.category_name ORDER BY product_count DESC;"
        )
    if "inventory" in q or "warehouse" in q or "stock" in q:
        return (
            "SELECT warehouse_location, COUNT(*) AS stocked_products, SUM(quantity_on_hand) AS total_units "
            "FROM inventory GROUP BY warehouse_location ORDER BY total_units DESC;"
        )
    if "customer" in q:
        return (
            "SELECT c.customer_id, c.first_name || ' ' || c.last_name AS customer_name, c.city, "
            "COUNT(o.order_id) AS total_orders, ROUND(COALESCE(SUM(o.total_amount), 0), 2) AS total_spent "
            "FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id "
            "WHERE NOT c.is_deleted GROUP BY c.customer_id, c.first_name, c.last_name, c.city "
            "ORDER BY total_spent DESC LIMIT 5;"
        )
    if "product" in q:
        return (
            "SELECT p.product_id, p.product_name, p.price, SUM(oi.quantity) AS total_units_sold "
            "FROM products p JOIN order_items oi ON p.product_id = oi.product_id "
            "WHERE NOT p.is_deleted GROUP BY p.product_id, p.product_name, p.price "
            "ORDER BY total_units_sold DESC LIMIT 5;"
        )
    return (
        "SELECT "
        "(SELECT COUNT(*) FROM categories WHERE NOT is_deleted) AS active_categories, "
        "(SELECT COUNT(*) FROM products WHERE NOT is_deleted) AS active_products, "
        "(SELECT COUNT(*) FROM customers WHERE NOT is_deleted) AS active_customers, "
        "(SELECT COUNT(*) FROM orders) AS total_orders, "
        "(SELECT ROUND(SUM(total_amount), 2) FROM orders) AS gross_order_value;"
    )


# =====================================================================
# 3. LANGGRAPH NODES (CALLING MCP TOOLS & TRACED BY MLFLOW)
# =====================================================================

@mlflow_trace(name="langgraph.router_node", span_type="CHAIN")
def router_node(state: QAState) -> Dict[str, Any]:
    """Determines whether the question needs PostgreSQL MCP tools, MinIO MCP tools, or both."""
    q = state["question"].lower()
    db_keywords = {
        "order", "orders", "customer", "customers", "product", "products", "category",
        "categories", "payment", "payments", "inventory", "warehouse", "batch", "etl",
        "sql", "table", "database", "revenue", "price", "postgres", "spending",
    }
    minio_keywords = {
        "oceanx", "atlas", "amerimed", "policy", "policies", "demurrage", "detention",
        "reefer", "reg e", "reg cc", "dispute", "overdraft", "hipaa", "patient",
        "telehealth", "minio", "bucket", "pdf", "website", "document", "shipping",
        "bank", "hospital", "leave", "refund", "customs", "cargo",
    }

    has_db = any(kw in q for kw in db_keywords)
    has_minio = any(kw in q for kw in minio_keywords)

    if has_db and has_minio:
        route: Literal["postgres", "minio", "both"] = "both"
    elif has_minio:
        route = "minio"
    elif has_db:
        route = "postgres"
    else:
        route = "both"

    return {"route": route, "mcp_tool_calls": [f"router_node -> selected route '{route}'"]}


@mlflow_trace(name="langgraph.postgres_mcp_node", span_type="AGENT")
def postgres_mcp_node(state: QAState) -> Dict[str, Any]:
    """Calls MCP PostgreSQL tools (`list_db_tables`, `describe_db_table`, `run_sql_query`)."""
    tool_logs = list(state.get("mcp_tool_calls", []))

    tables_json = list_db_tables()
    tool_logs.append("MCP Tool: list_db_tables()")

    orders_schema = describe_db_table("orders")
    tool_logs.append("MCP Tool: describe_db_table('orders')")

    sql = _build_sql_for_question(state["question"], f"Tables:\n{tables_json}\nOrders Schema:\n{orders_schema}")
    query_result = run_sql_query(sql)
    tool_logs.append(f"MCP Tool: run_sql_query({sql!r})")

    context = json.dumps(
        {
            "tables_overview": json.loads(tables_json),
            "executed_sql": sql,
            "query_result": json.loads(query_result),
        },
        indent=2,
    )
    return {"db_context": context, "mcp_tool_calls": tool_logs}


@mlflow_trace(name="langgraph.minio_mcp_node", span_type="AGENT")
def minio_mcp_node(state: QAState) -> Dict[str, Any]:
    """Calls MCP MinIO tools (`search_minio_documents`, `read_minio_document`, `list_minio_files`)."""
    tool_logs = list(state.get("mcp_tool_calls", []))
    question = state["question"]

    search_res_str = search_minio_documents(question)
    tool_logs.append(f"MCP Tool: search_minio_documents({question!r})")
    search_res = json.loads(search_res_str)

    top_matches = search_res.get("top_matches", [])
    top_doc_content = ""
    if top_matches:
        top_key = top_matches[0]["object_key"]
        doc_str = read_minio_document(top_key, max_chars=4000)
        tool_logs.append(f"MCP Tool: read_minio_document({top_key!r})")
        top_doc_content = json.loads(doc_str).get("content", "")
    else:
        files_str = list_minio_files("business-website-policy-documents/")
        tool_logs.append("MCP Tool: list_minio_files('business-website-policy-documents/')")
        top_doc_content = files_str

    context = json.dumps(
        {
            "bucket": search_res.get("bucket", "ai-source-data"),
            "top_matches": top_matches[:3],
            "top_document_excerpt": top_doc_content[:2500],
        },
        indent=2,
    )
    return {"minio_context": context, "mcp_tool_calls": tool_logs}


@mlflow_trace(name="langgraph.synthesizer_node", span_type="CHAIN")
def synthesizer_node(state: QAState) -> Dict[str, Any]:
    """Combines findings from PostgreSQL MCP tools and/or MinIO MCP tools into a final answer."""
    question = state["question"]
    db_ctx = state.get("db_context", "")
    minio_ctx = state.get("minio_context", "")

    combined_context = ""
    if db_ctx:
        combined_context += f"=== PostgreSQL Database Context ===\n{db_ctx}\n\n"
    if minio_ctx:
        combined_context += f"=== MinIO Object Storage Context ===\n{minio_ctx}\n\n"

    llm_answer = _call_llm_if_available(
        system_prompt=(
            "You are a helpful AI assistant connected to PostgreSQL and MinIO via MCP tools. "
            "Answer the user's question accurately using ONLY the provided tool results, and "
            "cite the exact SQL query or MinIO object key used."
        ),
        user_prompt=f"Question: {question}\n\n{combined_context}",
    )
    if llm_answer:
        return {"final_answer": llm_answer}

    parts = []
    if db_ctx:
        db_data = json.loads(db_ctx)
        sql = db_data.get("executed_sql", "")
        rows = db_data.get("query_result", {}).get("rows", [])
        parts.append(
            f"[PostgreSQL Container (`tinitiate-ai-postgres`)]\n"
            f"• Executed SQL: `{sql}`\n"
            f"• Result ({len(rows)} row(s)): {json.dumps(rows, indent=2)}"
        )

    if minio_ctx:
        m_data = json.loads(minio_ctx)
        matches = m_data.get("top_matches", [])
        if matches:
            top = matches[0]
            parts.append(
                f"[MinIO Container (`tinitiate-ai-minio` -> bucket `{m_data.get('bucket')}`)]\n"
                f"• Top Matching Object Key: `{top['object_key']}`\n"
                f"• Relevant Excerpt:\n{top['matching_snippet']}"
            )
        else:
            parts.append(
                f"[MinIO Container (`tinitiate-ai-minio`)]\n"
                f"• Listed Objects:\n{m_data.get('top_document_excerpt', '')[:600]}"
            )

    return {"final_answer": "\n\n".join(parts)}


# =====================================================================
# 4. BUILD & RUN THE LANGGRAPH WORKFLOW (WITH MLFLOW LOGGING)
# =====================================================================

@mlflow_trace(name="LangGraph_MCP_Pipeline", span_type="AGENT")
def _execute_langgraph_pipeline(question: str) -> QAState:
    initial_state: QAState = {
        "question": question,
        "route": "both",
        "mcp_tool_calls": [],
        "db_context": "",
        "minio_context": "",
        "final_answer": "",
    }

    if HAS_LANGGRAPH:
        workflow = StateGraph(QAState)
        workflow.add_node("router", router_node)
        workflow.add_node("postgres_mcp", postgres_mcp_node)
        workflow.add_node("minio_mcp", minio_mcp_node)
        workflow.add_node("synthesizer", synthesizer_node)

        workflow.add_edge(START, "router")

        def route_decision(state: QAState):
            if state["route"] == "postgres":
                return "postgres_mcp"
            if state["route"] == "minio":
                return "minio_mcp"
            return "both_postgres_first"

        workflow.add_conditional_edges(
            "router",
            route_decision,
            {
                "postgres_mcp": "postgres_mcp",
                "minio_mcp": "minio_mcp",
                "both_postgres_first": "postgres_mcp",
            },
        )

        def after_postgres(state: QAState):
            return "minio_mcp" if state["route"] == "both" else "synthesizer"

        workflow.add_conditional_edges(
            "postgres_mcp",
            after_postgres,
            {"minio_mcp": "minio_mcp", "synthesizer": "synthesizer"},
        )
        workflow.add_edge("minio_mcp", "synthesizer")
        workflow.add_edge("synthesizer", END)

        app = workflow.compile()
        return app.invoke(initial_state)

    state = dict(initial_state)
    state.update(router_node(state))
    if state["route"] in ("postgres", "both"):
        state.update(postgres_mcp_node(state))
    if state["route"] in ("minio", "both"):
        state.update(minio_mcp_node(state))
    state.update(synthesizer_node(state))
    return state  # type: ignore


def run_langgraph_qa(question: str) -> QAState:
    start_ts = time.perf_counter()
    if HAS_MLFLOW:
        with mlflow.start_run(run_name=f"LangGraph: {question[:45]}"):
            mlflow.set_tag("framework", "LangGraph")
            mlflow.set_tag("protocol", "MCP")
            mlflow.log_param("framework", "LangGraph")
            mlflow.log_param("question", question)

            result = _execute_langgraph_pipeline(question)
            elapsed_ms = round((time.perf_counter() - start_ts) * 1000, 2)

            mlflow.log_param("route", result["route"])
            mlflow.log_metric("execution_time_ms", elapsed_ms)
            mlflow.log_metric("mcp_steps_count", len(result["mcp_tool_calls"]))
            mlflow.log_Metric = elapsed_ms
            mlflow.log_text(result["final_answer"], "final_answer.txt")
            mlflow.log_dict(
                {
                    "question": question,
                    "route": result["route"],
                    "mcp_tool_calls": result["mcp_tool_calls"],
                    "final_answer": result["final_answer"],
                },
                "langgraph_execution_trace.json",
            )
            return result

    return _execute_langgraph_pipeline(question)


def main():
    parser = argparse.ArgumentParser(description="Ask questions against PostgreSQL and MinIO using LangGraph + MCP.")
    parser.add_argument("--question", "-q", help="Single question to ask from the command line.")
    parser.add_argument("--interactive", "-i", action="store_true", help="Start an interactive prompt to type questions.")
    args = parser.parse_args()

    if HAS_MLFLOW:
        print(f"[MLflow Tracking Enabled] URI: {MLFLOW_TRACKING_URI} | Experiment: {MLFLOW_EXPERIMENT_NAME}")

    if args.interactive:
        print("=" * 80)
        print("LangGraph + MCP Interactive Q&A (PostgreSQL + MinIO) [Tracked in MLflow]")
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
            result = run_langgraph_qa(user_q)
            print("-" * 80)
            print("LangGraph + MCP Trace:")
            for step in result["mcp_tool_calls"]:
                print(f"  -> {step}")
            print("-" * 80)
            print("ANSWER:")
            print(result["final_answer"])
            print("=" * 80)
        return

    questions = (
        [args.question]
        if args.question
        else [
            "How many orders exist per status in PostgreSQL and what is their total amount?",
            "What is OceanX Logistics policy on demurrage, detention, and free time?",
            "How does Atlas Retail Bank handle Reg E unauthorized electronic fund transfer disputes?",
        ]
    )

    for idx, q in enumerate(questions, start=1):
        print("=" * 80)
        print(f"QUESTION {idx}: {q}")
        print("-" * 80)
        result = run_langgraph_qa(q)
        print("LangGraph + MCP Trace:")
        for step in result["mcp_tool_calls"]:
            print(f"  -> {step}")
        print("-" * 80)
        print("ANSWER:")
        print(result["final_answer"])
        print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
