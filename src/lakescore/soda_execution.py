"""Renders `column_checks` rows into SodaCL YAML and executes them via `soda-core-spark-df`,
feeding the Accuracy dimension's `has_check_passed` metric."""

from __future__ import annotations

import logging
import os
from typing import Any

from pyspark.sql import SparkSession

os.environ.setdefault("SODA_TELEMETRY_ENABLED", "false")

logger = logging.getLogger(__name__)


def generate_soda_checks_yaml(
    spark: SparkSession, catalog_name: str, schema_name: str, list_of_tables: list[str]
) -> list[dict[str, str]]:
    """Reads `catalog_name.data_quality.column_checks` and renders each table's checks into a
    single SodaCL YAML document.

    Parameters:
        spark (SparkSession): Active Spark session.
        catalog_name (str): Catalog owning `data_quality.column_checks`.
        schema_name (str): Schema of the tables being checked.
        list_of_tables (list[str]): Table names to render checks for.

    Returns:
        list[dict[str, str]]: One dict per table with `table_name` (fully qualified) and
            `yaml_content`.
    """
    df = spark.table(f"{catalog_name}.data_quality.column_checks")
    result_list = []

    for table_name in list_of_tables:
        df_filtered = df.filter(
            (df.catalog_name == catalog_name)
            & (df.schema_name == schema_name)
            & (df.table_name == table_name)
        )
        if df_filtered.count() == 0:
            logger.info("No checks found for %s.%s.%s", catalog_name, schema_name, table_name)
            continue

        data = df_filtered.select(
            "catalog_name", "schema_name", "table_name", "check", "tag", "column_name"
        ).collect()

        checks_by_table: dict[str, list[str]] = {}
        for row in data:
            full_table_name = f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}"
            check_clean = row["check"].strip()
            if check_clean.startswith("-"):
                check_clean = check_clean[1:].strip()
            checks_by_table.setdefault(full_table_name, []).append(check_clean)

        for full_table_name, checks in checks_by_table.items():
            yaml_lines = [f"checks for {full_table_name}:"]
            for check in checks:
                check_lines = check.split("\n")
                yaml_lines.append("  - " + check_lines[0])
                yaml_lines.extend("      " + line.strip() for line in check_lines[1:])
            result_list.append(
                {"table_name": full_table_name, "yaml_content": "\n".join(yaml_lines)}
            )

    return result_list


def execute_soda_checks(
    spark: SparkSession, catalog_name: str, schema_name: str, list_of_tables: list[str]
) -> list[dict[str, Any]]:
    """Runs each table's SodaCL checks (via `generate_soda_checks_yaml`) against the live data.

    Returns:
        list[dict[str, Any]]: One dict per table (see `_parse_scan_results`). Tables whose scan
            raised an exception are omitted with a logged warning, not silently dropped.
    """
    from soda.scan import Scan

    soda_check_results = []

    for table_name in list_of_tables:
        logger.info("Running tests for %s.%s.%s", catalog_name, schema_name, table_name)
        soda_checks = generate_soda_checks_yaml(spark, catalog_name, schema_name, [table_name])
        if not soda_checks:
            continue
        yaml_content = soda_checks[0]["yaml_content"]

        try:
            scan = Scan()
            scan.set_scan_definition_name("data_tests")
            scan.set_data_source_name("spark_df")
            scan.add_spark_session(spark)
            scan.add_sodacl_yaml_str(yaml_content)
            scan.execute()

            check_results = _parse_scan_results(
                catalog_name, schema_name, table_name, scan.get_scan_results()
            )
            soda_check_results.append(check_results)
        except Exception as e:
            logger.warning(
                "Error running Soda checks for %s.%s.%s: %s",
                catalog_name,
                schema_name,
                table_name,
                e,
            )

    return soda_check_results


def _parse_scan_results(
    catalog_name: str, schema_name: str, table_name: str, scan_result: dict[str, Any]
) -> dict[str, Any]:
    """Extracts a pass/fail summary from a raw Soda scan result dict."""
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    check_summary = []
    all_checks_passed = True

    for check in scan_result["checks"]:
        is_passed = check["outcome"] == "pass"
        check_summary.append(
            {
                "check_name": check["name"],
                "is_passed": is_passed,
                "check_definition": check["definition"],
                "failed_value_count": check["diagnostics"]["value"],
            }
        )
        if not is_passed:
            all_checks_passed = False

    return {
        "table_name": full_table_name,
        "all_checks_passed": all_checks_passed,
        "check_summary": check_summary,
    }
