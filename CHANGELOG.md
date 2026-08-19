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

## [0.1.0] - 2026-08-19

Initial GenAI-powered data quality framework: rule-based checks, LLM-generated metadata, and a
RAG pipeline generating SodaCL checks from a vector-indexed knowledge base.
