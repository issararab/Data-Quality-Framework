# Architecture

## Package map

```
src/lakescore/
├── config.py          LakeScoreConfig: catalog/endpoint/threshold settings, from widgets or env
├── sql_utils.py        escape_sql_string: the one place raw values get escaped for SQL splicing
├── soda_execution.py    renders column_checks -> SodaCL YAML, runs it via soda-core-spark-df
├── knowledge_base.py     loads the bundled SodaCL knowledge-base CSV into a Delta table
├── catalog/             DDL-ensure + CRUD, one module per LakeScore-owned table
│   ├── dq_config.py       per-table thresholds
│   ├── column_checks.py   SodaCL checks + warehouse reconciliation (sync_checks_target_table)
│   └── dq_summary.py      the scored output table
├── metadata/             read-only Unity Catalog metadata + tag mutations
│   ├── tables.py           DESCRIBE DETAIL/HISTORY/EXTENDED -> stewardship/usability signals
│   ├── columns.py          information_schema.columns/column_tags reads + comment writes
│   └── tags.py             the only place ALTER TABLE ... SET/UNSET TAGS is built
├── quality/              dimension-scoring logic
│   ├── freshness.py        threshold parsing + DESCRIBE HISTORY-based freshness check
│   ├── validity.py         schema drift vs. a past Delta version
│   └── cardinality.py      low-cardinality detection + tagging
└── generators/            GenAI chains (functional core / imperative shell)
    ├── prompts.py            prompt templates, reviewed independently of chain code
    ├── _rag_helpers.py       pure/testable retrieval + formatting logic
    ├── column_description.py thin composition root (MLflow "models from code")
    └── column_check_rag.py   thin composition root (MLflow "models from code")
```

`notebooks/` holds thin orchestration scripts that import from `lakescore.*` — they contain
looping/widget logic, not business logic. `resources/lakescore_job.yml` + `databricks.yml`
define the Databricks Asset Bundle job that runs them in order.

## Functional core, imperative shell

Every function under `catalog/`, `metadata/`, and `quality/` takes an explicit
`spark: SparkSession` argument instead of relying on the Databricks-notebook-injected global.
This is what makes `tests/integration/` possible: tests pass in a local Spark+Delta session
instead of requiring a live workspace.

`generators/` follows the same principle at a different boundary. `_rag_helpers.py` holds pure
(or Spark/mock-testable) functions; `column_description.py`/`column_check_rag.py` are thin
composition roots that build real Databricks/LangChain clients and call
`mlflow.models.set_model(...)` at module level, per MLflow's "models from code" pattern — which
requires that side effect to happen at import time, so those two files are deliberately left out
of unit-test coverage, the same way a `main.py` entrypoint would be.

## Data model

```mermaid
erDiagram
    dq_config ||--o{ dq_summary : "scored per table"
    dq_config ||--o{ column_checks : "checks generated per table"
    knowledge_base ||--o{ column_checks : "grounds RAG generation"

    dq_config {
        string catalog_name
        string schema_name
        string table_name
        int low_cardinality_threshold
        string freshness_window
        string validity_window
    }
    column_checks {
        bigint check_id
        string catalog_name
        string schema_name
        string table_name
        string column_name
        string check
        string tag "ai_generated or NULL"
    }
    dq_summary {
        bigint table_id
        string catalog_name
        string schema_name
        string table_name
        boolean table_description
        boolean columns_description
        boolean has_a_valid_owner
        boolean is_fresh
        boolean columns_valid
        boolean has_check_passed
    }
    knowledge_base {
        string check_id
        string check_syntax
        string description
    }
```

`knowledge_base` (and its vector index) is intentionally centralized in one catalog rather than
duplicated per monitored catalog — it's SodaCL reference material, not per-catalog data. See
`LakeScoreConfig.knowledge_base_table`/`vector_search_index`.

## RAG check-generation flow

```mermaid
flowchart LR
    A[Column metadata\n+ comment] --> B[extract_comment_for_retriever]
    B --> C[Vector Search\nsimilarity_search]
    C --> D[retrieve_and_format_docs]
    D --> E[format_context\nvia knowledge_base table]
    E --> F[LLM: SodaCL-grounded\ncheck generation]
    F --> G[column_checks table]
    G --> H[soda_execution.py\nexecutes checks]
    H --> I[dq_summary\nAccuracy score]
```

## Known follow-ups (not done in this restructure)

- **Parameterized queries over manual escaping.** `sql_utils.escape_sql_string` is the correct
  fix for the SQL-splicing risk it addresses, but Databricks' `spark.sql(query, args=...)`
  parameterized-query API is a strictly better long-term fix where it's supported. Its coverage
  across `ALTER TABLE ... COMMENT`/`SET TAGS` statements on your target Databricks Runtime
  wasn't verified as part of this restructure (no live workspace access) — evaluate migrating
  before relying on `escape_sql_string` indefinitely.
- **Databricks Asset Bundle scaffolding is unverified.** `databricks.yml` and
  `resources/lakescore_job.yml` were authored without a live workspace to run
  `databricks bundle validate`/`deploy` against. Review the job cluster spec
  (`spark_version`/`node_type_id`) before deploying.
- **Local integration tests need a JVM + (on Windows) `winutils.exe`.** `tests/integration/`
  starts a real local Spark+Delta session. This was authored and code-reviewed, and unit tests
  (`tests/unit/`, no Spark dependency) were run and pass, but the integration tier itself could
  not be executed in the Windows sandbox this restructure was done in — no network access to
  fetch the Windows Hadoop binaries PySpark requires locally. It runs without this issue in the
  GitHub Actions CI workflow (Linux) and on macOS/Linux dev machines with Java installed.
