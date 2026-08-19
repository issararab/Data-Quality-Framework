"""Shared pytest fixtures.

The `spark` fixture starts a local Spark session with Delta Lake enabled, registering a second
catalog named `lakescore_test` (in addition to the default `spark_catalog`) so integration tests
exercise the same multi-catalog code paths (`USE CATALOG <name>`, `<catalog>.<schema>.<table>`)
LakeScore uses against Unity Catalog in production — this is what the `demo`-hardcoding bug fixed
earlier in the project's history would have been caught by.

Requires a JVM (Java 8/11/17) on PATH. On Windows, PySpark additionally requires `winutils.exe`
on `HADOOP_HOME`; see https://wiki.apache.org/hadoop/WindowsProblems. Tests in `tests/integration/`
are skipped automatically if Spark fails to start.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator

import pytest


@pytest.fixture(scope="session")
def spark() -> Iterator[pyspark.sql.SparkSession]:  # noqa: F821 - forward ref for the docstring only
    pyspark = pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from delta import configure_spark_with_delta_pip

    warehouse_dir = tempfile.mkdtemp(prefix="lakescore-test-warehouse-")
    builder = (
        pyspark.sql.SparkSession.builder.master("local[2]")
        .appName("lakescore-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.sql.catalog.lakescore_test", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.ui.enabled", "false")
    )

    try:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        shutil.rmtree(warehouse_dir, ignore_errors=True)
        pytest.skip(f"Local Spark session could not start: {e}")

    yield session

    session.stop()
    shutil.rmtree(warehouse_dir, ignore_errors=True)


TEST_CATALOG = "lakescore_test"
