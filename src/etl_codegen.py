"""GenAI-Powered ETL Code Generator
Uses Gemini to generate PySpark, dbt, and Airflow DAG code from
natural language descriptions and BigQuery schema metadata.
"""

from __future__ import annotations
import json
import re
import logging
from dataclasses import dataclass
from typing import Literal

logger = logging.getLogger(__name__)

TargetFramework = Literal["pyspark", "dbt", "airflow", "bigquery_sql"]


@dataclass
class ETLSpec:
    description: str
    source_schema: dict  # {"table": "...", "columns": [{"name": ..., "type": ...}]}
    target_schema: dict
    framework: TargetFramework
    transformations: list[str] = None
    schedule: str = "@daily"


@dataclass
class GeneratedCode:
    framework: str
    code: str
    explanation: str
    estimated_complexity: str  # low, medium, high
    warnings: list[str]


class ETLCodeGenerator:
    """Generates ETL code from natural language using Gemini."""

    FRAMEWORK_INSTRUCTIONS = {
        "pyspark": "Generate production-grade PySpark code with proper schema enforcement, partitioning, and error handling.",
        "dbt": "Generate a dbt model SQL file with Jinja2 templating, ref() calls, and documentation blocks.",
        "airflow": "Generate an Airflow DAG with proper task dependencies, retries, and GCP operators.",
        "bigquery_sql": "Generate optimized BigQuery SQL with partitioning filters, CTEs, and QUALIFY for deduplication.",
    }

    def __init__(self, project_id: str = "") -> None:
        self.project_id = project_id
        self._model = None

    def _get_model(self):
        if self._model is None:
            import vertexai
            import vertexai.generative_models as genai
            if self.project_id:
                vertexai.init(project=self.project_id)
            self._model = genai.GenerativeModel(
                "gemini-1.5-pro",
                generation_config={"temperature": 0.2, "max_output_tokens": 4096},
            )
        return self._model

    def generate(self, spec: ETLSpec) -> GeneratedCode:
        """Generate ETL code from spec using Gemini."""
        prompt = self._build_prompt(spec)
        try:
            model = self._get_model()
            response = model.generate_content(prompt)
            return self._parse_response(response.text, spec.framework)
        except Exception as e:
            logger.error("Code generation failed: %s", e)
            return GeneratedCode(
                framework=spec.framework,
                code="",
                explanation=f"Generation failed: {e}",
                estimated_complexity="unknown",
                warnings=[str(e)],
            )

    def _build_prompt(self, spec: ETLSpec) -> str:
        fw_instruction = self.FRAMEWORK_INSTRUCTIONS.get(spec.framework, "")
        transformations_str = "\n".join(f"  - {t}" for t in (spec.transformations or []))
        return f"""You are an expert data engineer. {fw_instruction}

Task: {spec.description}

Source Table Schema:
{json.dumps(spec.source_schema, indent=2)}

Target Table Schema:
{json.dumps(spec.target_schema, indent=2)}

Required Transformations:
{transformations_str or '  - Use best practices based on the schemas'}

Schedule: {spec.schedule}

Instructions:
1. Generate complete, runnable {spec.framework} code
2. Include all imports and dependencies
3. Add inline comments for complex logic
4. Handle nulls, duplicates, and data type mismatches
5. Use GCP best practices (partitioning, clustering for BQ; GCS checkpointing for Spark)
6. At the end, add a brief explanation block starting with '## Explanation:'
7. Add any warnings starting with '## Warnings:'

Generate the code now:"""

    def _parse_response(self, raw: str, framework: str) -> GeneratedCode:
        code = raw
        explanation = ""
        warnings = []

        if "## Explanation:" in raw:
            parts = raw.split("## Explanation:")
            code = parts[0].strip()
            rest = parts[1]
            if "## Warnings:" in rest:
                exp_parts = rest.split("## Warnings:")
                explanation = exp_parts[0].strip()
                warnings = [w.strip(" -") for w in exp_parts[1].strip().split("\n") if w.strip()]
            else:
                explanation = rest.strip()

        code = re.sub(r"```[\w]*\n?", "", code).strip()
        complexity = self._estimate_complexity(code)

        return GeneratedCode(
            framework=framework,
            code=code,
            explanation=explanation,
            estimated_complexity=complexity,
            warnings=warnings,
        )

    def _estimate_complexity(self, code: str) -> str:
        lines = len(code.splitlines())
        joins = code.lower().count("join")
        if lines < 50 and joins == 0:
            return "low"
        elif lines < 200 and joins <= 3:
            return "medium"
        return "high"

    def generate_dbt_model(self, source_table: str, transformations: list[str]) -> str:
        """Convenience method for generating a dbt model."""
        spec = ETLSpec(
            description=f"Create a dbt model transforming {source_table}",
            source_schema={"table": source_table},
            target_schema={"table": source_table.replace("stg_", "").replace("raw_", "")},
            framework="dbt",
            transformations=transformations,
        )
        result = self.generate(spec)
        return result.code

    def generate_airflow_dag(
        self,
        dag_id: str,
        tasks: list[dict],
        schedule: str = "0 6 * * *",
    ) -> str:
        """Generate an Airflow DAG with given tasks."""
        spec = ETLSpec(
            description=f"Create Airflow DAG '{dag_id}' with tasks: {[t['name'] for t in tasks]}",
            source_schema={"dag_id": dag_id, "tasks": tasks},
            target_schema={},
            framework="airflow",
            schedule=schedule,
        )
        result = self.generate(spec)
        return result.code

    def explain_existing_code(self, code: str, framework: str) -> str:
        """Use Gemini to explain and document existing ETL code."""
        try:
            model = self._get_model()
            prompt = (
                f"You are a senior data engineer. Explain the following {framework} code "
                "in plain English. Identify: (1) what it does, (2) potential issues, "
                f"(3) optimization opportunities.\n\n```{framework}\n{code}\n```"
            )
            return model.generate_content(prompt).text
        except Exception as e:
            return f"Explanation failed: {e}"

    def schema_to_bq_ddl(self, schema: dict) -> str:
        """Convert a JSON schema dict to BigQuery CREATE TABLE DDL."""
        table = schema.get("table", "my_dataset.my_table")
        columns = schema.get("columns", [])
        col_defs = []
        for col in columns:
            nullable = "" if col.get("required") else ""
            col_defs.append(f"  {col['name']} {col.get('type', 'STRING')} OPTIONS(description='{col.get('description', '')}')") 
        cols_str = ",\n".join(col_defs)
        return f"""CREATE OR REPLACE TABLE `{table}`\n(\n{cols_str}\n)\nPARTITION BY DATE(_PARTITIONTIME)\nCLUSTER BY (id, created_date);"""

