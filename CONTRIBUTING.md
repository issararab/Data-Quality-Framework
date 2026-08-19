# Contributing

## Dev setup

```bash
python -m venv .venv
source .venv/bin/activate   # .venv\Scripts\activate on Windows
pip install -e ".[dev]"
pre-commit install
```

## Running checks locally

```bash
ruff check .              # lint
ruff format .              # format
mypy src                    # type check
pytest                       # unit + local Spark/Delta integration tests
```

`tests/integration/` starts a real local Spark session with Delta Lake, so it needs a JVM
(Java 8/11/17) on `PATH`. On Windows, PySpark additionally needs `winutils.exe` on `HADOOP_HOME`
— see https://wiki.apache.org/hadoop/WindowsProblems. If Spark can't start, those tests are
skipped rather than failed; CI runs on Linux, where this isn't an issue.

## Where things live

See [`docs/architecture.md`](docs/architecture.md) for the package map and design rationale
before adding a new module — most new logic belongs in an existing `catalog/`, `metadata/`, or
`quality/` module rather than a new top-level file.

## Conventions

- Every function that talks to Spark takes `spark: SparkSession` as an explicit parameter —
  don't reach for a notebook-injected global.
- Any string that didn't come from a trusted schema/catalog listing (LLM output, raw column
  data) must go through `lakescore.sql_utils.escape_sql_string` before being spliced into a SQL
  statement.
- Prompt text lives in `lakescore/generators/prompts.py`, not inline in chain-assembly code.
- Run `pre-commit run --all-files` before opening a PR — CI runs the same checks.
