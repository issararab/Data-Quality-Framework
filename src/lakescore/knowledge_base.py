"""Loads the bundled SodaCL knowledge-base CSV into a Delta table for RAG check generation.

The knowledge base is reference material (SodaCL syntax examples), not per-monitored-catalog
data, so it's intentionally centralized in one catalog rather than duplicated per catalog
LakeScore monitors — see `docs/architecture.md`.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from lakescore.catalog._common import ensure_schema_exists, table_exists

# src/lakescore/knowledge_base.py -> src/lakescore -> src -> <repo root>
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_CSV_PATH = _REPO_ROOT / "data" / "knowledge_base" / "sodacl_checks.csv"


def default_knowledge_base_path() -> str:
    """Resolves the repo-relative SodaCL knowledge-base CSV path from this module's own
    location (`Path(__file__)`), independent of the caller's current working directory —
    unlike the previous `os.getcwd()`-relative resolution."""
    return str(_DEFAULT_CSV_PATH)


def create_knowledge_base_table_if_not_exists(
    spark: SparkSession, catalog_name: str, schema_name: str = "data_quality"
) -> None:
    """Creates `catalog_name.schema_name.knowledge_base` if it doesn't already exist."""
    ensure_schema_exists(spark, catalog_name, schema_name)
    if not table_exists(spark, catalog_name, schema_name, "knowledge_base"):
        spark.sql(
            f"""
            CREATE TABLE {catalog_name}.{schema_name}.knowledge_base (
                check_id STRING,
                check_syntax STRING,
                description STRING
            )
            """
        )


def append_data_to_table(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str = "data_quality",
    csv_path: str | None = None,
) -> None:
    """Appends the SodaCL knowledge-base CSV into `catalog_name.schema_name.knowledge_base`.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): Catalog to load the knowledge base into.
        schema_name (str): Schema to load the knowledge base into.
        csv_path (str | None): Explicit path to the CSV; defaults to the file packaged with
            LakeScore (`default_knowledge_base_path()`).
    """
    path = csv_path or default_knowledge_base_path()
    knowledge_base_df = spark.read.format("csv").option("header", "true").load(f"file:{path}")
    knowledge_base_df.write.format("delta").mode("append").saveAsTable(
        f"{catalog_name}.{schema_name}.knowledge_base"
    )
