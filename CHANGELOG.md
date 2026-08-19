# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- Rebranded from "Data Quality Framework (DQF)" to **LakeScore**.
- Restructured the repository into an installable `src/lakescore` package with modules split by
  responsibility (`catalog/`, `metadata/`, `quality/`, `generators/`), replacing the flat
  `utils/`/`generators/` layout.
- Renamed the four pipeline notebooks from numeric prefixes (`1_conf_init.py`, ...) to
  descriptive names under `notebooks/`; execution order is now expressed by a Databricks Asset
  Bundle job DAG (`resources/lakescore_job.yml`) instead of filename prefixes.
- Replaced hardcoded per-workspace literals (catalog name, LLM/vector-search endpoint names)
  with `lakescore.config.LakeScoreConfig`, loaded from Databricks widgets or environment
  variables.
- Licensed under MIT; added `LICENSE`.

### Added

- `pyproject.toml` consolidating the dependency versions previously duplicated across per-notebook
  `%pip install` cells, plus a `dev` extra for local tooling.
- Unit and local Spark+Delta integration test suite under `tests/`.
- CI workflow (`.github/workflows/ci.yml`) running ruff, mypy, and pytest.
- `docs/architecture.md` and `docs/quality_dimensions.md`.
- `.pre-commit-config.yaml`, `.editorconfig`, `.gitignore`, `CONTRIBUTING.md`.

### Fixed

- Trailing-space metric dict keys in the dq_summary upsert that produced invalid SQL column
  names.
- `KeyError` when a table's `Table Properties` isn't a string.
- `_fetch_checks_for_table` / `generate_soda_checks_yaml` ignoring the passed `catalog_name` and
  always reading from a hardcoded `demo` catalog, breaking Accuracy scoring for any other
  catalog.
- Unescaped SQL string splicing when writing LLM-generated column descriptions and low-cardinality
  values into `COMMENT`/`TAGS` statements, now centralized in `lakescore.sql_utils`.
- Fragile `os.getcwd()`-relative path resolution for the knowledge-base CSV.
- `lakescore.generators.column_check_rag` requiring a `LAKESCORE_CATALOG_NAME` environment
  variable that nothing set, disconnected from the `catalog_name` widget the calling notebook
  actually uses — `notebooks/generate_checks.py` would fail on import otherwise. Now resolves
  via `LakeScoreConfig.from_widgets`, which reads the widget the notebook already sets (falling
  back to the same environment variable for standalone Model Serving deployment). Also removed
  a hardcoded `llm_model_endpoint="ssbi-openai"` override that shadowed the
  `LAKESCORE_LLM_MODEL_ENDPOINT` environment variable it was meant to respect.
- `catalog._common.ensure_schema_exists`/`table_exists` relying on `USE CATALOG <name>`, a
  Databricks SQL extension not recognized by open-source Apache Spark's SQL parser — broke
  every `catalog/*.py` store function when actually run (a gap the local integration test
  suite never caught until it could run for real on Linux; it silently skipped on the
  Windows dev box this project was largely built on). Every statement is now fully
  catalog-qualified instead of relying on "current catalog" session state, which works on
  both Unity Catalog and plain Spark.
- `catalog.column_checks.add_column_check` writing without `.format("delta")`, unlike every
  other write path in the module — relied on the session's default data source format, which
  Databricks sets to `delta` workspace-wide (masking the gap) but plain Spark does not.
- `tests/integration/`'s local Spark+Delta fixture registering a second, arbitrarily-named
  Delta catalog: `DeltaCatalog` only auto-wires its internal delegate when registered as the
  special `spark_catalog` name, so queries against the second catalog failed with
  `NullPointerException: ... "this.delegate" is null`. Tests now target `spark_catalog`
  directly — `lakescore`'s own catalog-qualification logic is still fully exercised regardless
  of what the catalog happens to be named.
- A regression test asserting that a trailing-space metric key (`"is_fresh "`) raises a SQL
  error — real Spark SQL execution proved this premise false: a trailing space before the next
  token is ordinary whitespace, discarded by the lexer, not part of the identifier. Corrected
  to use a key with an *embedded* space (`"is fresh"`), which does genuinely break the
  generated SQL.

## [0.1.0] - 2026-08-19

Initial GenAI-powered data quality framework: rule-based checks, LLM-generated metadata, and a
RAG pipeline generating SodaCL checks from a vector-indexed knowledge base.
