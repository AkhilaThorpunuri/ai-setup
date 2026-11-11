# MCP + LangGraph & CrewAI Question-Answering (Tracked in MLflow)

This folder contains the **Model Context Protocol (MCP)** server and the **LangGraph** and **CrewAI** question-answering scripts that query both the **PostgreSQL** (`tinitiate-ai-postgres`) and **MinIO** (`tinitiate-ai-minio`) Docker containers.

## Files

- [`mcp_server.py`](mcp_server.py) — FastMCP server exposing PostgreSQL tools (`list_db_tables`, `describe_db_table`, `run_sql_query`) and MinIO tools (`list_minio_files`, `search_minio_documents`, `read_minio_document`), instrumented with `@mlflow.trace`.
- [`ask_langgraph.py`](ask_langgraph.py) — Stateful LangGraph pipeline (`router_node` -> `postgres_mcp_node` / `minio_mcp_node` -> `synthesizer_node`) with MLflow run & trace logging.
- [`ask_crewai.py`](ask_crewai.py) — Multi-agent CrewAI pipeline (`PostgreSQL Retail & ETL Data Analyst` + `MinIO Policy & Document Research Specialist`) with MLflow run & trace logging.

## Quick Start

### 1. Start the Docker Containers & Load Data
```powershell
docker compose -f tinitiate-ai-onboarding.yml up -d
Get-Content ai-development-source-data/rdbms-content/sql/01_schema.sql | docker exec -i tinitiate-ai-postgres psql -U ai_admin -d ai_setup
Get-Content ai-development-source-data/rdbms-content/sql/02_control_table.sql | docker exec -i tinitiate-ai-postgres psql -U ai_admin -d ai_setup
python ai-development-source-data/rdbms-content/loader/load_data.py
```

### 2. Run Q&A with LangGraph or CrewAI
```powershell
# Run built-in demo questions:
python qa_mcp_agents/ask_langgraph.py
python qa_mcp_agents/ask_crewai.py

# Ask a custom question:
python qa_mcp_agents/ask_langgraph.py -q "How many orders exist per status in PostgreSQL?"
python qa_mcp_agents/ask_crewai.py -q "What is OceanX Logistics policy on demurrage and detention?"

# Interactive terminal prompt:
python qa_mcp_agents/ask_langgraph.py -i
python qa_mcp_agents/ask_crewai.py -i
```

### 3. View Runs & Execution Traces in MLflow
```powershell
python -m mlflow ui --backend-store-uri sqlite:///mlflow.db --port 5000
```
Then open **http://localhost:5000** -> **`Tinitiate-MCP-PostgreSQL-MinIO-QA`** -> **Traces** / **Runs**.
