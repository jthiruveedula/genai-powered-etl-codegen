# GenAI-Powered ETL Code Generator

> **Natural language to production code** platform that converts plain English pipeline descriptions into Airflow DAGs, dbt models, PySpark jobs, and BigQuery SQL — complete with unit tests, data quality checks, and CI/CD configuration.

![Python](https://img.shields.io/badge/Python-3.11-blue) ![Airflow](https://img.shields.io/badge/Airflow-2.8-green) ![dbt](https://img.shields.io/badge/dbt-core-orange) ![Streamlit](https://img.shields.io/badge/UI-Streamlit-red)

---

## Problem Statement

Data engineers spend 60-70% of their time on boilerplate: DAG scaffolding, dbt model stubs, PySpark schema definitions, data quality tests. This platform uses LLMs to generate production-ready, best-practice code from natural language specifications, dramatically accelerating the development lifecycle.

---

## System Architecture

```
User Input (Natural Language)
   "Create an Airflow DAG that ingests Salesforce opportunities daily,
    transforms them in BigQuery to calculate win rates by region,
    and loads results to a marketing dashboard table."
         |
         v
+---------------------------+
|  Intent Parser (GPT-4o)  |  -> Extracts: sources, transforms, sinks,
|                           |     schedule, dependencies, quality rules
+----------+----------------+
           |
  +--------+--------+--------+
  |         |                |
  v         v                v
+------+ +------+      +----------+
|Airflow| | dbt  |      | PySpark  |
| DAG  | |Models|      |  Job     |
|Codegen| |Codegen|    |Codegen   |
+--+---+ +--+---+      +----+-----+
   |         |              |
   v         v              v
+--+--+  +---+---+   +------+-----+
|Tests|  |Schema |   |Optimized   |
|+CI/CD| |Tests  |   |SQL+Spark   |
+-----+  +-------+   +------------+
```

---

## Supported Output Types

| Target | Output | Features |
|--------|--------|---------|
| **Apache Airflow** | Full DAG Python file | TaskFlow API, retries, SLAs, sensors |
| **dbt** | models/ + schema.yml | Sources, refs, tests, docs |
| **PySpark** | Spark job + test file | Schema inference, Dataframe transforms |
| **BigQuery SQL** | Optimized SQL | Partitioning hints, CTEs, window functions |
| **Great Expectations** | Expectation suite | Auto-detected data quality rules |
| **GitHub Actions** | CI/CD workflow YAML | Lint, test, deploy pipeline |

---

## Sample Input / Output

### Input (Natural Language)
```
Create a daily pipeline that:
1. Extracts orders from PostgreSQL (orders table, last 24 hours)
2. Joins with customers table to enrich with customer segment
3. Calculates revenue by product_category and customer_segment
4. Loads to BigQuery analytics.daily_revenue_summary
5. Sends Slack alert if total revenue < $10,000
```

### Generated Airflow DAG (excerpt)
```python
@dag(
    dag_id='orders_revenue_pipeline',
    schedule_interval='0 6 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True,
    },
    tags=['orders', 'revenue', 'daily']
)
def orders_revenue_dag():
    
    @task
    def extract_orders(**context) -> dict:
        """Extract last 24 hours of orders from PostgreSQL."""
        # ... generated extraction code
    
    @task
    def transform_revenue(raw_orders: dict) -> dict:
        """Calculate revenue by product_category and customer_segment."""
        # ... generated transformation code
    
    @task
    def load_to_bigquery(revenue_data: dict) -> None:
        """Load results to analytics.daily_revenue_summary."""
        # ... generated loading code
    
    @task
    def validate_revenue_threshold(revenue_data: dict) -> None:
        """Alert if total revenue < $10,000."""
        # ... generated alerting code
    
    extracted = extract_orders()
    transformed = transform_revenue(extracted)
    loaded = load_to_bigquery(transformed)
    validate_revenue_threshold(transformed)
```

---

## Project Structure

```
genai-powered-etl-codegen/
|-- codegen/
|   |-- intent_parser.py         # GPT-4o pipeline intent extraction
|   |-- airflow_generator.py     # Airflow DAG code generation
|   |-- dbt_generator.py         # dbt models + schema.yml generation
|   |-- spark_generator.py       # PySpark job generation
|   |-- bq_sql_generator.py      # BigQuery SQL generation
|   |-- ge_generator.py          # Great Expectations suite generation
|   `-- cicd_generator.py        # GitHub Actions workflow generation
|-- templates/
|   |-- airflow_dag_template.jinja2
|   |-- dbt_model_template.jinja2
|   `-- spark_job_template.jinja2
|-- validation/
|   |-- syntax_validator.py      # Python/SQL syntax checking
|   `-- logic_validator.py       # LLM-based logic review
|-- ui/
|   `-- streamlit_app.py         # Web interface for code generation
|-- api/
|   `-- main.py                  # FastAPI REST endpoint
|-- examples/
|   |-- example_inputs.json      # Sample natural language prompts
|   `-- generated_outputs/       # Sample generated code
`-- README.md
```

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| LLM | GPT-4o + Function Calling |
| Framework | LangChain (structured output) |
| UI | Streamlit |
| API | FastAPI |
| Template Engine | Jinja2 |
| Syntax Validation | Python AST + sqlglot |
| Output Targets | Airflow, dbt, PySpark, BigQuery, GE |

---

## Interview Talking Points

- **Structured output vs free-form generation**: Using LangChain's `with_structured_output()` and Pydantic models to extract pipeline intent before code generation reduces hallucinations dramatically
- **Template-LLM hybrid approach**: Static Jinja2 templates handle boilerplate; LLM fills in domain-specific business logic — better reliability than pure LLM generation
- **Validation pipeline**: Generated code passes through Python AST parser + sqlglot SQL validator before delivery; malformed code triggers regeneration with error context
- **Why this matters for MAANG**: Code generation for data pipelines is a $50B+ opportunity; understanding how to build reliable, validated code generation systems is a top-tier ML Engineering skill

