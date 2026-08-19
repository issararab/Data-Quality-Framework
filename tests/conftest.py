"""Shared pytest fixtures.

The `spark` fixture starts a local Spark session with Delta Lake enabled as the session
catalog (`spark_catalog`). Tests target `TEST_CATALOG = "spark_catalog"` rather than an
arbitrary custom-named catalog: `DeltaCatalog` only auto-wires its internal delegate when
registered under the special `spark_catalog` name — Spark's catalog manager sets that delegate
itself when initializing the *session* catalog. Registering `DeltaCatalog` a second time under
an arbitrary name (as an earlier version of this fixture did, to more closely mirror
multi-catalog Unity Catalog usage) leaves that delegate unset, and every query against it fails
in the analyzer with `NullPointerException: ... "this.delegate" is null` — a limitation of
plain OSS Delta, not something `lakescore` can work around. All `lakescore` catalog/schema
handling is still fully exercised: every statement is catalog-qualified
(`<catalog>.<schema>.<table>`), so the actual code path under test doesn't care that
`TEST_CATALOG` happens to equal Spark's default catalog name.

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


TEST_CATALOG = "spark_catalog"
