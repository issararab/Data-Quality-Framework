"""GenAI chains: column description generation and RAG-grounded SodaCL check generation.

Each chain module is split into a pure, unit-testable "core" (`_rag_helpers.py`, `prompts.py`)
and a thin "composition root" (`column_description.py`, `column_check_rag.py`) that wires
Databricks clients into a chain and, per the MLflow "models from code" pattern, calls
`mlflow.models.set_model(...)` at module level. The composition roots are intentionally left out
of unit-test coverage, the same way a `main.py` entrypoint would be.
"""
